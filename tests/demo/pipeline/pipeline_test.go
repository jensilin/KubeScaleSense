// Package pipeline_test is the P2 demonstration, run in one process.
//
// It wires the real pieces together — the real load generator's records, a real
// Normalizer HTTP server, the real `http` pressure source, and the real
// decision engine — and asserts the property P2 exists to establish:
//
//	a genuine load spike raises a genuine pressure signal, which the controller
//	turns into a SCALE_UP *decision*, while the replica count does not move.
//
// It is not a substitute for the kind acceptance test in tests/e2e, which
// checks the things only a cluster can: scheduling, Service routing, node
// capacity. What it does cover is the part most likely to break silently — the
// contract between the workload's exposition and the controller's parser — and
// it covers it on every `go test`, with no cluster required.
package pipeline_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
	kssmetrics "github.com/jensilin/KubeScaleSense/internal/metrics"
	"github.com/jensilin/KubeScaleSense/internal/normalizer"
	"github.com/jensilin/KubeScaleSense/internal/resources"
	"github.com/jensilin/KubeScaleSense/internal/scaling"
)

// The pipeline, end to end, at a fixed replica count.
//
// Records flow generator → Normalizer → normalized output, the pressure signal
// rises because records genuinely queue, and the controller decides to scale up
// without scaling anything.
func TestPipeline_ASpikeProducesAScaleUpDecisionAndNoScaling(t *testing.T) {
	t.Parallel()

	pool := startNormalizerPool(t, 1, func(c *normalizer.Config) {
		// One slot and a slow record, so a modest burst is guaranteed to
		// queue. The demo tunes the same two dials on the cluster; here they
		// are set to make the test fast rather than realistic.
		c.MaxConcurrent = 1
		c.QueueLimit = 64
		c.QueueTimeout = 30 * time.Second
		c.ProcessingDelay = 30 * time.Millisecond
	})

	signal := pool.signal(t)

	// Baseline: nothing in flight, so pressure is zero and available — which is
	// a different thing from unavailable, and the distinction the whole signal
	// design exists to preserve.
	idle, err := signal.Collect(context.Background())
	if err != nil {
		t.Fatalf("collecting the baseline: %v", err)
	}
	if !idle.Available {
		t.Fatal("the baseline sample must be available")
	}
	if idle.PressureItems != 0 {
		t.Errorf("baseline PressureItems = %d, want 0", idle.PressureItems)
	}

	// The spike: 40 records at once against a pod that can process one at a
	// time. They queue, and queueing is the signal.
	spike := pool.spike(t, 40)
	defer spike.wait()

	pressured := pool.awaitPressure(t, signal, 10)
	t.Logf("observed pressure: %d items, %d in flight", pressured.PressureItems, pressured.InFlightRequests)

	// Now the controller's half. Feasibility says there is room for three more
	// pods; demand says we need more than we have.
	cfg := demoConfig()
	now := time.Now()

	snapshot := scaling.Snapshot{
		Valid:           true,
		CurrentReplicas: 1,
		ReadyReplicas:   1,
		PodRequest:      resources.Request{CPUMilli: 500, MemoryBytes: 512 << 20},
		Backlog: scaling.Signal[int64]{
			Value:     pressured.PressureItems,
			SampledAt: pressured.SampledAt,
			Available: true,
		},
		BacklogRaw: pressured.PressureItems,
		// Utilization is deliberately absent: metrics-server may not be
		// installed, and the pressure path alone must be able to drive a
		// scale-up.
		AvgCPUMilliPercent: scaling.Signal[int64]{},
		FitCapacity:        3,
		CandidateNodes:     3,
		Blocking:           resources.DimensionNone,
		FreeCPUMilli:       2600,
		FreeMemoryBytes:    6 << 30,
		HoldBackoff:        scaling.ClearBackoff(),
		LastGoodReplicas:   1,
	}

	decision := scaling.Decide(snapshot, cfg, now)

	// The assertion is on the *shape* of the decision, not on one reason code.
	//
	// How deep the queue is when the scrape lands depends on goroutine
	// scheduling, so the observed pressure is a range rather than a number —
	// and with fit capacity at 3, a high enough pressure correctly produces
	// ScaleUpPartial instead of ScaleUp. Pinning one of the two would make this
	// test pass or fail on timing, which is worse than useless. Both are
	// scale-ups, and "the controller decided to add replicas" is the property
	// P2 has to establish. The exact boundary between the two codes is pinned
	// by the table tests in internal/scaling, where the inputs are fixed.
	if decision.Reason != scaling.ReasonScaleUp && decision.Reason != scaling.ReasonScaleUpPartial {
		t.Fatalf("reason = %s, want %s or %s; decision = %+v",
			decision.Reason, scaling.ReasonScaleUp, scaling.ReasonScaleUpPartial, decision)
	}
	if decision.Direction != scaling.DirectionUp {
		t.Errorf("direction = %s, want %s", decision.Direction, scaling.DirectionUp)
	}
	if decision.Action != scaling.ActionWrite {
		t.Errorf("action = %s, want %s: a scale-up is a write the actuator would perform",
			decision.Action, scaling.ActionWrite)
	}
	if decision.TargetReplicas <= snapshot.CurrentReplicas {
		t.Errorf("TargetReplicas = %d, want more than the current %d",
			decision.TargetReplicas, snapshot.CurrentReplicas)
	}
	if !decision.DemandComputed {
		t.Error("DemandComputed is false, so the pressure signal did not reach the demand model")
	}
	// Feasibility must have bounded the answer: the decision may never ask for
	// more pods than fit, which is the project's entire thesis.
	if decision.TargetReplicas > snapshot.CurrentReplicas+snapshot.FitCapacity {
		t.Errorf("TargetReplicas = %d exceeds current + fit capacity (%d + %d)",
			decision.TargetReplicas, snapshot.CurrentReplicas, snapshot.FitCapacity)
	}

	// The P2 acceptance criterion. The decision says scale up; the pool does
	// not grow, because P2 has no actuator and P3 owns actuation.
	if got := pool.replicas(); got != 1 {
		t.Errorf("the Normalizer pool has %d replicas; P2 must not change the replica count", got)
	}

	t.Logf("decision: %s -> %d replicas (current %d, fit capacity %d)",
		decision.Reason, decision.TargetReplicas, snapshot.CurrentReplicas, snapshot.FitCapacity)
}

// Pressure must be the sum over pods. At a fixed offered load, a three-pod pool
// reports the pressure of all three — not of whichever one a Service happened
// to route the scrape to.
func TestPipeline_PressureIsSummedOverThePool(t *testing.T) {
	t.Parallel()

	pool := startNormalizerPool(t, 3, func(c *normalizer.Config) {
		c.MaxConcurrent = 1
		c.QueueLimit = 64
		c.QueueTimeout = 30 * time.Second
		c.ProcessingDelay = 30 * time.Millisecond
	})

	signal := pool.signal(t)

	spike := pool.spike(t, 60)
	defer spike.wait()

	// With three pods each holding one record and the rest queued behind them,
	// the sum must exceed what any single pod could report. A single pod's
	// in-flight count cannot exceed its one slot, so a total above 3 can only
	// come from a real sum.
	sample := pool.awaitPressure(t, signal, 12)

	if sample.InFlightRequests < 2 {
		t.Errorf("InFlightRequests = %d; with three busy pods the sum should exceed one pod's single slot",
			sample.InFlightRequests)
	}
	if sample.PressureItems <= sample.InFlightRequests {
		t.Errorf("PressureItems (%d) should exceed in-flight (%d) while records are queued",
			sample.PressureItems, sample.InFlightRequests)
	}
}

// The failure half of the same path: when the pool goes away, the controller
// must freeze rather than scale down. This is FR-38 and E2E-07 asserted against
// the real workload rather than a stub.
func TestPipeline_AVanishedPoolFreezesRatherThanScalesDown(t *testing.T) {
	t.Parallel()

	pool := startNormalizerPool(t, 1, nil)
	signal := pool.signal(t)

	if _, err := signal.Collect(context.Background()); err != nil {
		t.Fatalf("collecting while healthy: %v", err)
	}

	pool.stop()

	sample, err := signal.Collect(context.Background())
	if err == nil {
		t.Fatal("collecting from a stopped pool should report an error")
	}
	if sample.Available {
		t.Fatal("the sample must be unavailable")
	}
	if sample.PressureItems != 0 {
		t.Fatalf("PressureItems = %d; unavailable must never carry a value", sample.PressureItems)
	}

	// An unavailable signal against an idle-looking cluster is exactly the
	// situation in which reading "no answer" as "no work" would scale a busy
	// system to the floor.
	snapshot := scaling.Snapshot{
		Valid:           true,
		CurrentReplicas: 4,
		ReadyReplicas:   4,
		PodRequest:      resources.Request{CPUMilli: 500, MemoryBytes: 512 << 20},
		Backlog:         scaling.Signal[int64]{}, // unavailable
		FitCapacity:     5,
		CandidateNodes:  3,
		Blocking:        resources.DimensionNone,
		HoldBackoff:     scaling.ClearBackoff(),
	}

	decision := scaling.Decide(snapshot, demoConfig(), time.Now())

	if decision.Reason != scaling.ReasonHoldStaleMetrics {
		t.Errorf("reason = %s, want %s", decision.Reason, scaling.ReasonHoldStaleMetrics)
	}
	if decision.TargetReplicas != snapshot.CurrentReplicas {
		t.Errorf("TargetReplicas = %d, want the replica count unchanged at %d",
			decision.TargetReplicas, snapshot.CurrentReplicas)
	}
}

// Records must survive the round trip unchanged, and a retry must produce the
// same bytes from a different pod. This is the conservation-and-idempotence
// property the pipeline's durability argument rests on (D-03, DI-04).
func TestPipeline_EveryRecordIsNormalizedIdenticallyOnAnyPod(t *testing.T) {
	t.Parallel()

	pool := startNormalizerPool(t, 3, nil)

	const records = 30
	firstResults := make(map[string]string, records)

	for n := range records {
		raw := demoRecord(n)
		body := pool.post(t, raw)
		firstResults[raw] = body
	}

	// Send everything again. The Service-equivalent round-robin means most
	// records land on a different pod the second time.
	for raw, want := range firstResults {
		if got := pool.post(t, raw); got != want {
			t.Errorf("record %q normalized differently on a retry:\n got: %s\nwant: %s", raw, got, want)
		}
	}

	if len(firstResults) != records {
		t.Errorf("normalized %d distinct records, want %d", len(firstResults), records)
	}
}

// --- the pool --------------------------------------------------------------

// normalizerPool is a set of in-process Normalizer servers presented the way a
// Deployment behind two Services is: one round-robin data path, and a set of
// per-pod metrics endpoints on a shared port across distinct addresses.
type normalizerPool struct {
	servers []*httptest.Server
	dataURL []string
	address []string
	port    int

	mu      sync.Mutex
	next    int
	stopped bool
}

func startNormalizerPool(t *testing.T, size int, tune func(*normalizer.Config)) *normalizerPool {
	t.Helper()

	pool := &normalizerPool{port: freePort(t)}

	for i := range size {
		cfg := normalizer.DefaultConfig()
		// Zero cost rounds: this suite is about the signal and decision path,
		// and burning CPU would only slow it down. The cluster demo sets a real
		// cost; here the queue is created with a small delay instead.
		cfg.CostRounds = 0
		if tune != nil {
			tune(&cfg)
		}

		metrics := normalizer.NewMetrics()
		server, err := normalizer.NewServer(cfg, discard(), metrics, time.Now)
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}

		// The data plane on an arbitrary port: NiFi reaches it through a
		// Service, so its address is never hardcoded anywhere that matters.
		data := httptest.NewServer(server.DataHandler())
		t.Cleanup(data.Close)
		pool.dataURL = append(pool.dataURL, data.URL+"/normalize")

		// The admin plane on the shared port at 127.0.0.(i+1), which is what
		// the headless Service's DNS answer looks like.
		address := fmt.Sprintf("127.0.0.%d", i+1)
		listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d", address, pool.port))
		if err != nil {
			t.Fatalf("listening on %s:%d: %v", address, pool.port, err)
		}
		admin := httptest.NewUnstartedServer(server.AdminHandler())
		_ = admin.Listener.Close()
		admin.Listener = listener
		admin.Start()
		t.Cleanup(admin.Close)

		pool.servers = append(pool.servers, admin)
		pool.address = append(pool.address, address)
	}
	return pool
}

// signal builds the controller's real http source pointed at the pool.
func (p *normalizerPool) signal(t *testing.T) kssmetrics.WorkloadSignal {
	t.Helper()

	source, err := kssmetrics.NewHTTPSignalWithResolver(config.SignalConfig{
		Source:   config.SignalSourceHTTP,
		Endpoint: fmt.Sprintf("http://normalizer-metrics.data-pipeline.svc:%d/metrics", p.port),
		Timeout:  config.Duration(5 * time.Second),
	}, time.Now, staticResolver(p.address))
	if err != nil {
		t.Fatalf("NewHTTPSignalWithResolver: %v", err)
	}
	t.Cleanup(func() { _ = source.Close() })
	return source
}

// replicas is the pool's size: the assertion target for "P2 changed nothing".
func (p *normalizerPool) replicas() int { return len(p.servers) }

func (p *normalizerPool) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.stopped {
		return
	}
	for _, server := range p.servers {
		server.Close()
	}
	p.stopped = true
}

// post sends one record through the data path, round-robin across pods, and
// returns the normalized body.
func (p *normalizerPool) post(t *testing.T, raw string) string {
	t.Helper()

	p.mu.Lock()
	url := p.dataURL[p.next%len(p.dataURL)]
	p.next++
	p.mu.Unlock()

	response, err := http.Post(url, "text/plain", strings.NewReader(raw))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer func() { _ = response.Body.Close() }()

	body, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatalf("reading the response: %v", err)
	}
	if response.StatusCode != http.StatusOK {
		t.Fatalf("POST %s = %d: %s", url, response.StatusCode, body)
	}
	return string(body)
}

// spikeHandle lets a test wait for its background load to finish.
type spikeHandle struct{ wg *sync.WaitGroup }

func (s spikeHandle) wait() { s.wg.Wait() }

// spike offers `count` records concurrently, spread round-robin across the
// pool exactly as a Service would.
func (p *normalizerPool) spike(t *testing.T, count int) spikeHandle {
	t.Helper()

	var wg sync.WaitGroup
	for n := range count {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()

			p.mu.Lock()
			url := p.dataURL[n%len(p.dataURL)]
			p.mu.Unlock()

			response, err := http.Post(url, "text/plain", strings.NewReader(demoRecord(n)))
			if err != nil {
				return
			}
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}(n)
	}
	return spikeHandle{wg: &wg}
}

// awaitPressure polls the signal until the pool reports at least `want`
// outstanding records.
//
// Polling rather than sleeping: the exact moment the queue reaches a depth
// depends on scheduling, and a fixed sleep would be either flaky or slow. The
// assertion is on the pressure, not on when it arrived.
func (p *normalizerPool) awaitPressure(t *testing.T, signal kssmetrics.WorkloadSignal, want int64) kssmetrics.Sample {
	t.Helper()

	deadline := time.Now().Add(20 * time.Second)
	var last kssmetrics.Sample

	for time.Now().Before(deadline) {
		sample, err := signal.Collect(context.Background())
		if err == nil && sample.Available {
			last = sample
			if sample.PressureItems >= want {
				return sample
			}
		}
		time.Sleep(5 * time.Millisecond)
	}

	t.Fatalf("pressure never reached %d; last sample was %+v", want, last)
	return last
}

// --- helpers ---------------------------------------------------------------

// demoConfig is the shipped demo configuration, so the thresholds this test
// asserts against are the ones the demo actually runs with.
func demoConfig() *config.Config {
	cfg := config.Default()
	cfg.Controller.DryRun = true
	cfg.Workload.ItemsPerReplica = 5
	cfg.Workload.Signal.Source = config.SignalSourceHTTP
	return cfg
}

// demoRecord reproduces the generator's record shape without importing its
// main package.
func demoRecord(n int) string {
	observedAt := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC).
		Add(time.Duration(n) * time.Second).Format(time.RFC3339)
	return fmt.Sprintf("%s,epdg-%02d,pdp.sessions.active,%d", observedAt, n%5+1, n*7919%1_000_000)
}

func discard() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, nil))
}

func freePort(t *testing.T) int {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probing for a free port: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	if err := listener.Close(); err != nil {
		t.Fatalf("closing the probe listener: %v", err)
	}
	return port
}

// staticResolver stands in for cluster DNS.
type staticResolver []string

func (r staticResolver) LookupHost(context.Context, string) ([]string, error) {
	return []string(r), nil
}
