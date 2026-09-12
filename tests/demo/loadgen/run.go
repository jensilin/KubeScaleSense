package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// sink is where generated records go.
//
// Two implementations, for the two shapes the demonstration needs: files for
// the NiFi path, and direct HTTP for measuring the Normalizer without NiFi in
// the way. Behind one interface so the rate and profile logic is written once
// and neither mode can drift from the other.
type sink interface {
	// send delivers one burst of records starting at the given offset.
	send(ctx context.Context, offset, count int) error
	name() string
	close() error
}

// Stats is what a run reports.
type Stats struct {
	Records  atomic.Int64
	Failures atomic.Int64
	Files    atomic.Int64
}

// Run plays a plan against a sink.
//
// The pacing is deliberately simple: a ticker per phase, a burst per tick. It
// does not attempt to correct for a sink that cannot keep up, because that
// would hide the thing worth seeing — if the Normalizer cannot absorb the
// offered rate, the generator falling behind *is* the measurement.
func Run(ctx context.Context, log *slog.Logger, plan Plan, target sink, stats *Stats) error {
	if err := plan.Validate(); err != nil {
		return err
	}

	offset := 0
	for _, phase := range plan.phases() {
		interval := plan.interval(phase.rate)
		log.Info("phase started",
			slog.String("phase", phase.name),
			slog.Float64("records_per_second", phase.rate),
			slog.String("burst_interval", interval.String()),
			slog.Int("burst", plan.Burst),
			slog.String("duration", phase.duration.String()),
			slog.String("sink", target.name()))

		phaseCtx := ctx
		var cancel context.CancelFunc
		if phase.duration > 0 {
			phaseCtx, cancel = context.WithTimeout(ctx, phase.duration)
		}

		err := playPhase(phaseCtx, log, plan, target, stats, &offset, interval)

		if cancel != nil {
			cancel()
		}
		if err != nil {
			return err
		}
		// The parent context ending stops the whole run; a phase's own deadline
		// only ends that phase. A cancelled run is a normal stop — the operator
		// pressed Ctrl-C — so it is not reported as a failure.
		select {
		case <-ctx.Done():
			return nil
		default:
		}
	}
	return nil
}

func playPhase(
	ctx context.Context,
	log *slog.Logger,
	plan Plan,
	target sink,
	stats *Stats,
	offset *int,
	interval time.Duration,
) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		// The record budget is checked before sending, so -records is an exact
		// count rather than "at least".
		if plan.Records > 0 && *offset >= plan.Records {
			return nil
		}

		burst := plan.Burst
		if plan.Records > 0 && *offset+burst > plan.Records {
			burst = plan.Records - *offset
		}

		if err := target.send(ctx, *offset, burst); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			// A send failure is counted and logged, not fatal: a 503 from a
			// saturated pool is the expected answer under load, and a generator
			// that exited on the first one could never drive an overload
			// scenario.
			stats.Failures.Add(1)
			log.Warn("sending records failed", slog.Int("offset", *offset), slog.Any("error", err))
		}
		*offset += burst

		select {
		case <-ticker.C:
		case <-ctx.Done():
			return nil
		}
	}
}

// --- file sink -------------------------------------------------------------

// fileSink writes input files for NiFi's ListFile to pick up.
//
// Each burst becomes one file, written to a temporary name and then renamed
// into place. The rename is the whole point: ListFile would otherwise pick up a
// half-written file and NiFi would split a truncated final record, which looks
// exactly like data corruption and is entirely the writer's fault.
type fileSink struct {
	dir            string
	recordsPerFile int
	pad            int
	stats          *Stats
}

func newFileSink(dir string, plan Plan, stats *Stats) (*fileSink, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("creating the input directory: %w", err)
	}
	return &fileSink{dir: dir, recordsPerFile: plan.RecordsPerFile, pad: plan.Pad, stats: stats}, nil
}

func (s *fileSink) name() string { return "file:" + s.dir }
func (s *fileSink) close() error { return nil }

func (s *fileSink) send(_ context.Context, offset, count int) error {
	// One file per burst of `count` bursts' worth of records, so that
	// records-per-file controls file size independently of the burst size.
	records := count * s.recordsPerFile
	contents := file(offset*s.recordsPerFile, records, s.pad)

	final := filepath.Join(s.dir, fmt.Sprintf("pm-%09d.csv", offset))
	staging := final + ".partial"

	if err := os.WriteFile(staging, []byte(contents), 0o644); err != nil {
		return err
	}
	if err := os.Rename(staging, final); err != nil {
		return err
	}

	s.stats.Files.Add(1)
	s.stats.Records.Add(int64(records))
	return nil
}

// --- http sink -------------------------------------------------------------

// httpSink posts records straight to the Normalizer.
//
// This is the mode that measures the workload itself: it removes NiFi from the
// path, so a flat throughput curve cannot be blamed on the client's concurrency
// (FS-28). Its concurrency is explicit and is what DI-08 holds above the
// replica count.
type httpSink struct {
	url         string
	pad         int
	concurrency int
	client      *http.Client
	stats       *Stats

	// slots bounds in-flight requests. Without it a burst would open one
	// connection per record and the generator would be measuring its own
	// socket exhaustion.
	slots chan struct{}
	wg    sync.WaitGroup
}

func newHTTPSink(url string, plan Plan, concurrency int, timeout time.Duration, stats *Stats) *httpSink {
	return &httpSink{
		url:         url,
		pad:         plan.Pad,
		concurrency: concurrency,
		client: &http.Client{
			Timeout: timeout,
			Transport: &http.Transport{
				MaxIdleConnsPerHost: concurrency,
				IdleConnTimeout:     30 * time.Second,
			},
		},
		stats: stats,
		slots: make(chan struct{}, concurrency),
	}
}

func (s *httpSink) name() string { return "http:" + s.url }

func (s *httpSink) close() error {
	s.wg.Wait()
	s.client.CloseIdleConnections()
	return nil
}

func (s *httpSink) send(ctx context.Context, offset, count int) error {
	for i := range count {
		body := record(offset+i, s.pad)

		select {
		case s.slots <- struct{}{}:
		case <-ctx.Done():
			return ctx.Err()
		}

		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			defer func() { <-s.slots }()

			if err := s.post(ctx, body); err != nil {
				s.stats.Failures.Add(1)
				return
			}
			s.stats.Records.Add(1)
		}()
	}
	return nil
}

func (s *httpSink) post(ctx context.Context, body string) error {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader([]byte(body)))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "text/plain; charset=utf-8")

	response, err := s.client.Do(request)
	if err != nil {
		return err
	}
	defer func() {
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}()

	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status %s", response.Status)
	}
	return nil
}
