package normalizer

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"
)

// WorkDigestHeader carries the proof-of-work digest.
//
// A header and not a body field: the body is the normalized record and must
// stay a pure function of the input (WR-03), while this digest depends on the
// configured round count. Returning it keeps the CPU work observable and
// non-elidable without making a tuning change an output format change.
const WorkDigestHeader = "X-Normalizer-Work-Digest"

// Server is the Normalizer's HTTP surface.
//
// Two planes on two ports: /normalize on the data plane, and /metrics,
// /healthz, /readyz on the admin plane.
type Server struct {
	cfg     Config
	log     *slog.Logger
	metrics *Metrics
	clock   func() time.Time

	// slots is the concurrency limiter. A buffered channel rather than a
	// semaphore type because the receive is selectable, which is what makes the
	// queue timeout and client cancellation expressible in one select.
	slots chan struct{}

	// waiting counts records queued for a slot. Read by the queued gauge and
	// compared against QueueLimit for admission, so it must be exact under
	// concurrency: atomic, not mutex, because it is incremented on every
	// request on the hot path.
	waiting atomic.Int64

	// draining flips once on SIGTERM and never back. Readiness reads it, so the
	// pod leaves the Service's rotation before the listener closes.
	draining atomic.Bool

	// beforeProcess runs just after a slot is taken and before the work starts.
	// It is nil in production and exists so tests can hold a slot open at a
	// known instant. The alternative is asserting concurrency with sleeps,
	// which is how a suite acquires flaky tests that everyone learns to re-run.
	// Set before the first request and not written afterwards.
	beforeProcess func()
}

// NewServer builds the server. It does not listen.
func NewServer(cfg Config, log *slog.Logger, metrics *Metrics, clock func() time.Time) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	if log == nil {
		return nil, errors.New("a logger is required")
	}
	if metrics == nil {
		return nil, errors.New("a metrics registry is required")
	}
	if clock == nil {
		clock = time.Now
	}

	return &Server{
		cfg:     cfg,
		log:     log,
		metrics: metrics,
		clock:   clock,
		slots:   make(chan struct{}, cfg.MaxConcurrent),
	}, nil
}

// DataHandler serves POST /normalize.
func (s *Server) DataHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST /normalize", s.handleNormalize)
	return mux
}

// AdminHandler serves the metrics and probe endpoints.
func (s *Server) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.Handle("GET /metrics", s.metrics.Handler())

	// /healthz reports process liveness only. It stays 200 while draining: a
	// pod that is finishing its in-flight records is working correctly, and
	// restarting it would abandon exactly the requests the drain exists to
	// protect.
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		writePlain(w, http.StatusOK, "ok")
	})

	// /readyz fails the instant SIGTERM arrives, which is what makes the
	// Service withdraw this pod before it stops accepting work (WR-06, D-05).
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.draining.Load() {
			writePlain(w, http.StatusServiceUnavailable, "draining")
			return
		}
		writePlain(w, http.StatusOK, "ok")
	})

	return mux
}

// handleNormalize is the whole workload.
//
// The order of operations matters and is the reason this function is not
// shorter: the body is read before a slot is taken, so a malformed record is
// rejected without consuming capacity; and the queued gauge is incremented
// before the wait, so the pressure signal reflects the wait rather than
// discovering it afterwards.
func (s *Server) handleNormalize(w http.ResponseWriter, r *http.Request) {
	start := s.clock()
	s.metrics.received.Inc()

	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	if err != nil {
		// An oversized or truncated body. Permanent for this record, so it is
		// a 400: retrying will read the same bytes and fail identically.
		s.fail(w, start, OutcomeInvalid, http.StatusBadRequest,
			fmt.Sprintf("could not read the request body within %d bytes: %v", s.cfg.MaxBodyBytes, err))
		return
	}

	// Normalization is pure and cheap, so it runs before admission control.
	// Rejecting a malformed record costs no slot and cannot be starved by a
	// queue, which keeps a batch of bad input from looking like an overload.
	record, err := Normalize(raw)
	if err != nil {
		s.fail(w, start, OutcomeInvalid, http.StatusBadRequest, err.Error())
		return
	}

	acquired, reason := s.acquire(r.Context())
	if !acquired {
		// 503 with Retry-After: this is back-pressure, not a fault. NiFi keeps
		// the record in its durable queue and re-sends it (D-02), which is
		// where a buffer belongs.
		w.Header().Set("Retry-After", "1")
		s.fail(w, start, OutcomeOverloaded, http.StatusServiceUnavailable, reason)
		return
	}
	defer s.release()

	digest := s.process(record)

	body := Encode(record)
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set(WorkDigestHeader, digest)
	w.WriteHeader(http.StatusOK)

	if _, err := w.Write(body); err != nil {
		// The client vanished mid-response. NiFi cannot have recorded a success
		// it never received, so it will retry — and because normalization is
		// pure, the retry produces the same bytes (FS-29). Counted as an error
		// so the duplicate work is visible rather than silent.
		s.metrics.observe(OutcomeError, s.since(start))
		s.log.Warn("writing the response failed after normalization completed",
			slog.String("checksum", record.Checksum), slog.Any("error", err))
		return
	}
	s.metrics.observe(OutcomeNormalized, s.since(start))
}

// acquire waits for a processing slot.
//
// Returns false with an operator-facing reason rather than an error, because
// every rejection here is a normal, expected outcome under load and the reason
// is the response body a pipeline operator will read.
func (s *Server) acquire(ctx context.Context) (bool, string) {
	// Fast path: a slot is free, so this record never queues. Taking it without
	// touching the waiting counter is what makes QueueLimit mean "records
	// allowed to wait" rather than "records allowed at all" — with a limit of
	// zero the pod still runs MaxConcurrent records, it just sheds instead of
	// queueing.
	select {
	case s.slots <- struct{}{}:
		s.metrics.inFlight.Set(float64(len(s.slots)))
		return true, ""
	default:
	}

	// Every slot is busy, so this record has to wait. Admission before
	// queueing: an unbounded queue inside the pod would turn a spike into an
	// OOM kill and lose the in-flight records with it.
	if waiting := s.waiting.Add(1); waiting > int64(s.cfg.QueueLimit) {
		s.waiting.Add(-1)
		return false, fmt.Sprintf("queue is full: %d records are already waiting for one of %d slots",
			waiting-1, s.cfg.MaxConcurrent)
	}
	s.metrics.queued.Set(float64(s.waiting.Load()))

	defer func() {
		s.waiting.Add(-1)
		s.metrics.queued.Set(float64(s.waiting.Load()))
	}()

	// A fresh timer per request is the cost of a selectable timeout; it is
	// stopped promptly so a long queue does not accumulate live timers.
	timer := time.NewTimer(s.cfg.QueueTimeout)
	defer timer.Stop()

	select {
	case s.slots <- struct{}{}:
		s.metrics.inFlight.Set(float64(len(s.slots)))
		return true, ""

	case <-timer.C:
		// Shed rather than wait longer. Holding the record past NiFi's response
		// timeout would have it retried while this copy is still queued, which
		// doubles the load exactly when the pool is saturated (FS-29).
		return false, fmt.Sprintf("timed out after %s waiting for one of %d processing slots",
			s.cfg.QueueTimeout, s.cfg.MaxConcurrent)

	case <-ctx.Done():
		// The client gave up. Nobody is waiting for this answer, so the next
		// slot should go to a record that still has a reader.
		return false, fmt.Sprintf("the client cancelled the request while it was queued: %v", ctx.Err())
	}
}

func (s *Server) release() {
	<-s.slots
	s.metrics.inFlight.Set(float64(len(s.slots)))
}

// process performs the configured work for one record.
func (s *Server) process(record Record) string {
	if s.beforeProcess != nil {
		s.beforeProcess()
	}
	if s.cfg.ProcessingDelay > 0 {
		time.Sleep(s.cfg.ProcessingDelay)
	}
	// Seeded with the checksum, so the work is a function of the record: two
	// pods processing the same retried record do the same work and return the
	// same digest, which is what makes a duplicate detectable in a trace.
	return burn([]byte(record.Checksum), s.cfg.CostRounds)
}

func (s *Server) fail(w http.ResponseWriter, start time.Time, outcome string, status int, message string) {
	s.metrics.observe(outcome, s.since(start))
	writePlain(w, status, message)
}

func (s *Server) since(start time.Time) float64 {
	return s.clock().Sub(start).Seconds()
}

// Drain begins the shutdown sequence and reports when the listener should close.
//
// Split from the actual server shutdown so that the ordering — fail readiness,
// wait for endpoint propagation, then stop accepting — is visible in one place
// and testable without a real listener.
func (s *Server) Drain(ctx context.Context) {
	s.draining.Store(true)
	s.metrics.setReady(false)
	s.log.Info("draining: readiness now fails, waiting for the Service to withdraw this pod",
		slog.String("drain_delay", s.cfg.DrainDelay.String()),
		slog.Int64("in_flight", int64(len(s.slots))),
		slog.Int64("queued", s.waiting.Load()))

	// Endpoint removal is eventually consistent: kube-proxy on another node
	// keeps routing here for a moment after readiness fails. Closing the
	// listener now would turn that race into failed requests, which is the usual
	// cause of "scale-down dropped traffic" (FS-14).
	select {
	case <-time.After(s.cfg.DrainDelay):
	case <-ctx.Done():
	}
}

// Draining reports whether the drain has begun.
func (s *Server) Draining() bool { return s.draining.Load() }

// InFlight and Queued expose the pressure counters for tests and logging.
func (s *Server) InFlight() int { return len(s.slots) }

// Queued reports the records waiting for a slot.
func (s *Server) Queued() int64 { return s.waiting.Load() }

func writePlain(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	// The write error is dropped: the client has gone, and there is no second
	// channel on which to report that fact.
	_, _ = w.Write([]byte(body + "\n"))
}
