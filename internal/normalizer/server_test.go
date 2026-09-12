package normalizer

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestServer_NormalizesAValidRecord(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) { c.CostRounds = 8 })

	response := f.post(referenceRaw)
	if response.Code != http.StatusOK {
		t.Fatalf("status = %d, body = %q", response.Code, response.Body.String())
	}
	if got := response.Body.String(); got != referenceBody {
		t.Errorf("body = %s, want %s", got, referenceBody)
	}
	if got := response.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("Content-Type = %q", got)
	}
	// The work digest proves the CPU cost ran and keeps it out of the body.
	if got := response.Header().Get(WorkDigestHeader); len(got) != 64 {
		t.Errorf("%s = %q, want a 64-character digest", WorkDigestHeader, got)
	}

	f.assertCompleted(OutcomeNormalized, 1)
	f.assertGauge(MetricQueued, 0)
	f.assertGauge(MetricInFlight, 0)
}

// The same record posted twice must come back byte-identical, including when
// the two requests land concurrently. This is the retry-safety property stated
// as an HTTP-level assertion (FS-29, DI-04).
func TestServer_RetriesAreByteIdentical(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) { c.CostRounds = 16 })

	first := f.post(referenceRaw)
	second := f.post(referenceRaw)

	if first.Body.String() != second.Body.String() {
		t.Errorf("a retry produced a different body:\n %s\n %s", first.Body.String(), second.Body.String())
	}
	if first.Header().Get(WorkDigestHeader) != second.Header().Get(WorkDigestHeader) {
		t.Error("a retry produced a different work digest, so the work is not a function of the record")
	}
}

func TestServer_RejectsMalformedRecordsAsPermanent(t *testing.T) {
	t.Parallel()

	f := newFixture(t, nil)

	for _, raw := range []string{"", "not,enough", "2026-03-14T12:00:00Z,n,c,-5", "nope,n,c,1"} {
		response := f.post(raw)
		if response.Code != http.StatusBadRequest {
			t.Errorf("post(%q) status = %d, want 400 so NiFi routes it to failure rather than retrying forever",
				raw, response.Code)
		}
	}
	f.assertCompleted(OutcomeInvalid, 4)
	f.assertCompleted(OutcomeNormalized, 0)
}

// A malformed record must not consume a processing slot: a batch of bad input
// would otherwise look exactly like an overload and distort the pressure
// signal the controller is reading.
func TestServer_MalformedRecordsConsumeNoCapacity(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) {
		c.MaxConcurrent = 1
		c.QueueLimit = 0
	})

	// Occupy the only slot, then post garbage while it is held.
	release := f.occupySlot(t)
	defer release()

	response := f.post("garbage")
	if response.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400: a malformed record should be rejected without waiting for a slot",
			response.Code)
	}
}

func TestServer_RejectsAnOversizedBody(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) { c.MaxBodyBytes = 32 })

	response := f.post(strings.Repeat("x", 64))
	if response.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", response.Code)
	}
	if !strings.Contains(response.Body.String(), "32 bytes") {
		t.Errorf("body = %q, want it to name the limit", response.Body.String())
	}
}

func TestServer_RejectsEverythingButPostOnNormalize(t *testing.T) {
	t.Parallel()

	f := newFixture(t, nil)

	for _, method := range []string{http.MethodGet, http.MethodPut, http.MethodDelete, http.MethodPatch} {
		request := httptest.NewRequest(method, "/normalize", strings.NewReader(referenceRaw))
		response := httptest.NewRecorder()
		f.data.ServeHTTP(response, request)

		if response.Code == http.StatusOK {
			t.Errorf("%s /normalize returned 200; only POST should be routed", method)
		}
	}
}

// Concurrency is the mechanism that turns arrival rate into pressure, so it is
// asserted directly: MaxConcurrent records run at once, and the next one waits.
func TestServer_ProcessesUpToMaxConcurrentAtOnce(t *testing.T) {
	t.Parallel()

	const concurrent = 4

	entered := make(chan struct{}, concurrent+2)
	proceed := make(chan struct{})

	f := newFixture(t, func(c *Config) {
		c.MaxConcurrent = concurrent
		c.QueueLimit = 16
		c.QueueTimeout = 5 * time.Second
	})
	// A hook in place of the CPU cost, so the test controls when a slot frees
	// instead of racing a sleep.
	f.server.beforeProcess = func() {
		entered <- struct{}{}
		<-proceed
	}

	var wg sync.WaitGroup
	for range concurrent + 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.post(referenceRaw)
		}()
	}

	// Exactly MaxConcurrent requests should reach the processing stage.
	for range concurrent {
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatal("timed out waiting for the processing slots to fill")
		}
	}
	select {
	case <-entered:
		t.Fatal("a fifth record entered processing, so the concurrency limit is not enforced")
	case <-time.After(50 * time.Millisecond):
	}

	// The two extra records must be visible as queued: this is the pressure
	// signal the controller scrapes.
	if queued := f.server.Queued(); queued != 2 {
		t.Errorf("Queued() = %d, want 2", queued)
	}
	if inFlight := f.server.InFlight(); inFlight != concurrent {
		t.Errorf("InFlight() = %d, want %d", inFlight, concurrent)
	}

	close(proceed)
	wg.Wait()

	f.assertCompleted(OutcomeNormalized, concurrent+2)
	f.assertGauge(MetricQueued, 0)
	f.assertGauge(MetricInFlight, 0)
}

// Back-pressure: beyond the queue limit the answer is 503, which keeps the
// record in NiFi's durable queue instead of in this pod's memory (D-02).
func TestServer_ShedsLoadBeyondTheQueueLimit(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) {
		c.MaxConcurrent = 1
		c.QueueLimit = 0
		c.QueueTimeout = time.Second
	})

	release := f.occupySlot(t)

	response := f.post(referenceRaw)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", response.Code)
	}
	if got := response.Header().Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want it set so NiFi backs off rather than hot-looping", got)
	}
	if !strings.Contains(response.Body.String(), "queue is full") {
		t.Errorf("body = %q, want it to explain the rejection", response.Body.String())
	}

	release()
	f.assertCompleted(OutcomeOverloaded, 1)
}

// A queue limit of zero must still process MaxConcurrent records: the limit
// bounds waiting, not admission.
func TestServer_AZeroQueueLimitStillProcessesConcurrently(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) {
		c.MaxConcurrent = 2
		c.QueueLimit = 0
	})

	for i := range 2 {
		if response := f.post(referenceRaw); response.Code != http.StatusOK {
			t.Fatalf("request %d: status = %d, want 200", i, response.Code)
		}
	}
}

// A record must not wait indefinitely: holding it past NiFi's response timeout
// would have it retried while this copy is still queued (FS-29).
func TestServer_ShedsARecordThatWaitsTooLong(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) {
		c.MaxConcurrent = 1
		c.QueueLimit = 8
		c.QueueTimeout = 20 * time.Millisecond
	})

	release := f.occupySlot(t)
	defer release()

	response := f.post(referenceRaw)
	if response.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", response.Code)
	}
	if !strings.Contains(response.Body.String(), "timed out") {
		t.Errorf("body = %q, want it to say the record timed out waiting", response.Body.String())
	}
	f.assertCompleted(OutcomeOverloaded, 1)
}

// A client that gives up while queued must free its place immediately rather
// than hold it for the full timeout.
func TestServer_ReleasesAQueuedRecordWhenTheClientCancels(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) {
		c.MaxConcurrent = 1
		c.QueueLimit = 8
		c.QueueTimeout = 10 * time.Second
	})

	release := f.occupySlot(t)
	defer release()

	ctx, cancel := context.WithCancel(context.Background())
	request := httptest.NewRequest(http.MethodPost, "/normalize", strings.NewReader(referenceRaw)).WithContext(ctx)
	response := httptest.NewRecorder()

	done := make(chan struct{})
	go func() {
		defer close(done)
		f.data.ServeHTTP(response, request)
	}()

	// Wait until the record is actually queued, then cancel.
	deadline := time.Now().Add(2 * time.Second)
	for f.server.Queued() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("the record never reached the queue")
		}
		time.Sleep(time.Millisecond)
	}
	cancel()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the handler did not return after the client cancelled; it waited out the queue timeout instead")
	}
	if f.server.Queued() != 0 {
		t.Error("the cancelled record is still counted as queued")
	}
}

func TestServer_HealthAndReadinessEndpoints(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) { c.DrainDelay = 0 })

	if response := f.get("/healthz"); response.Code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", response.Code)
	}
	if response := f.get("/readyz"); response.Code != http.StatusOK {
		t.Errorf("/readyz = %d, want 200", response.Code)
	}
	if response := f.get("/metrics"); response.Code != http.StatusOK {
		t.Errorf("/metrics = %d, want 200", response.Code)
	}

	f.server.Drain(context.Background())

	// Readiness fails so the Service withdraws the pod (WR-06)...
	if response := f.get("/readyz"); response.Code != http.StatusServiceUnavailable {
		t.Errorf("/readyz = %d while draining, want 503", response.Code)
	}
	// ...but liveness must stay up, or the kubelet restarts a pod that is
	// correctly finishing its in-flight records.
	if response := f.get("/healthz"); response.Code != http.StatusOK {
		t.Errorf("/healthz = %d while draining, want 200", response.Code)
	}
	f.assertGauge("normalizer_ready", 0)
}

// The drain must not return before its delay has elapsed: the delay is what
// covers eventually-consistent endpoint removal (FS-14).
func TestServer_DrainWaitsForEndpointPropagation(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) { c.DrainDelay = 80 * time.Millisecond })

	start := time.Now()
	f.server.Drain(context.Background())
	if elapsed := time.Since(start); elapsed < 80*time.Millisecond {
		t.Errorf("Drain returned after %s, want it to wait out the drain delay", elapsed)
	}
	if !f.server.Draining() {
		t.Error("Draining() is false after Drain()")
	}
}

// A drain budget that expires must cut the wait short rather than overrun
// terminationGracePeriodSeconds and be SIGKILLed mid-drain.
func TestServer_DrainRespectsItsBudget(t *testing.T) {
	t.Parallel()

	f := newFixture(t, func(c *Config) { c.DrainDelay = 10 * time.Second })

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()

	start := time.Now()
	f.server.Drain(ctx)
	if elapsed := time.Since(start); elapsed > 2*time.Second {
		t.Errorf("Drain took %s; it should abandon the delay when the budget expires", elapsed)
	}
}

func TestNewServer_RejectsBadArguments(t *testing.T) {
	t.Parallel()

	valid := DefaultConfig()
	log := slog.New(slog.NewJSONHandler(io.Discard, nil))

	if _, err := NewServer(Config{}, log, NewMetrics(), nil); err == nil {
		t.Error("an invalid configuration should not produce a server")
	}
	if _, err := NewServer(valid, nil, NewMetrics(), nil); err == nil {
		t.Error("a nil logger should be rejected")
	}
	if _, err := NewServer(valid, log, nil, nil); err == nil {
		t.Error("nil metrics should be rejected")
	}
	// A nil clock is filled in rather than rejected: time.Now is the only
	// sensible default and requiring it at every call site is noise.
	if _, err := NewServer(valid, log, NewMetrics(), nil); err != nil {
		t.Errorf("a valid configuration was rejected: %v", err)
	}
}

// --- fixture ---------------------------------------------------------------

type fixture struct {
	t       *testing.T
	server  *Server
	metrics *Metrics
	data    http.Handler
	admin   http.Handler
}

func newFixture(t *testing.T, tune func(*Config)) *fixture {
	t.Helper()

	cfg := DefaultConfig()
	// Zero cost rounds by default: the suite is about the HTTP and concurrency
	// behaviour, and burning CPU in a unit test buys nothing but wall time.
	cfg.CostRounds = 0
	if tune != nil {
		tune(&cfg)
	}

	metrics := NewMetrics()
	server, err := NewServer(cfg, slog.New(slog.NewJSONHandler(io.Discard, nil)), metrics, time.Now)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}

	return &fixture{
		t:       t,
		server:  server,
		metrics: metrics,
		data:    server.DataHandler(),
		admin:   server.AdminHandler(),
	}
}

func (f *fixture) post(body string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodPost, "/normalize", strings.NewReader(body))
	response := httptest.NewRecorder()
	f.data.ServeHTTP(response, request)
	return response
}

func (f *fixture) get(path string) *httptest.ResponseRecorder {
	request := httptest.NewRequest(http.MethodGet, path, nil)
	response := httptest.NewRecorder()
	f.admin.ServeHTTP(response, request)
	return response
}

// occupySlot holds one processing slot until the returned function is called,
// which is how the tests create a saturated pod deterministically.
func (f *fixture) occupySlot(t *testing.T) func() {
	t.Helper()

	proceed := make(chan struct{})
	entered := make(chan struct{})
	var entry sync.Once
	f.server.beforeProcess = func() {
		// Only the first record signals; later ones fall straight through,
		// because a receive from a closed channel returns immediately.
		entry.Do(func() { close(entered) })
		<-proceed
	}

	go f.post(referenceRaw)

	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting to occupy a processing slot")
	}

	var once sync.Once
	return func() { once.Do(func() { close(proceed) }) }
}

func (f *fixture) assertCompleted(outcome string, want float64) {
	f.t.Helper()

	got := f.counterValue(MetricCompleted, outcome)
	if got != want {
		f.t.Errorf("%s{outcome=%q} = %v, want %v", MetricCompleted, outcome, got, want)
	}
}

func (f *fixture) counterValue(name, outcome string) float64 {
	f.t.Helper()

	families, err := f.metrics.Registry().Gather()
	if err != nil {
		f.t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == "outcome" && label.GetValue() == outcome {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	f.t.Fatalf("no %s{outcome=%q} in the exposition", name, outcome)
	return 0
}

func (f *fixture) assertGauge(name string, want float64) {
	f.t.Helper()

	families, err := f.metrics.Registry().Gather()
	if err != nil {
		f.t.Fatalf("gathering metrics: %v", err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		if got := family.GetMetric()[0].GetGauge().GetValue(); got != want {
			f.t.Errorf("%s = %v, want %v", name, got, want)
		}
		return
	}
	f.t.Fatalf("no %s in the exposition", name)
}
