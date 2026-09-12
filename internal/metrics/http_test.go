package metrics

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

// exposition renders a Normalizer-shaped /metrics response.
//
// Written as text rather than produced by a prometheus registry on purpose:
// this is the wire format the controller must parse, and generating it with the
// same library that parses it would test the library rather than the contract.
func exposition(queued, inFlight, received, completed float64, latencySum float64, latencyCount int) string {
	return fmt.Sprintf(`# HELP normalizer_requests_queued Records waiting.
# TYPE normalizer_requests_queued gauge
normalizer_requests_queued %g
# HELP normalizer_requests_in_flight Records being normalized.
# TYPE normalizer_requests_in_flight gauge
normalizer_requests_in_flight %g
# HELP normalizer_requests_received_total Records accepted.
# TYPE normalizer_requests_received_total counter
normalizer_requests_received_total %g
# HELP normalizer_requests_completed_total Records completed.
# TYPE normalizer_requests_completed_total counter
normalizer_requests_completed_total{outcome="normalized"} %g
normalizer_requests_completed_total{outcome="invalid"} 0
normalizer_requests_completed_total{outcome="overloaded"} 0
normalizer_requests_completed_total{outcome="error"} 0
# HELP normalizer_request_duration_seconds Request duration.
# TYPE normalizer_request_duration_seconds histogram
normalizer_request_duration_seconds_bucket{outcome="normalized",le="+Inf"} %d
normalizer_request_duration_seconds_sum{outcome="normalized"} %g
normalizer_request_duration_seconds_count{outcome="normalized"} %d
`, queued, inFlight, received, completed, latencyCount, latencySum, latencyCount)
}

func TestHTTPSignal_CollectsQueuedPlusInFlight(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/metrics" {
			t.Errorf("scraped %q, want /metrics", r.URL.Path)
		}
		// FR-37: the controller must never mutate the workload's data path, so
		// the only verb it may use here is GET.
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		fmt.Fprint(w, exposition(7, 4, 120, 109, 3.2, 109))
	}))
	defer server.Close()

	signal := newSignal(t, server.URL+"/metrics")

	sample, err := signal.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if !sample.Available {
		t.Fatal("sample is unavailable")
	}
	// The pressure signal is queued + in-flight: 7 + 4.
	if sample.PressureItems != 11 {
		t.Errorf("PressureItems = %d, want 11", sample.PressureItems)
	}
	if sample.InFlightRequests != 4 {
		t.Errorf("InFlightRequests = %d, want 4", sample.InFlightRequests)
	}
}

// The sum must cover every pod. Scraping one Service-balanced response would
// under-report pressure by a factor of the replica count, which looks like a
// working demo until it matters.
func TestHTTPSignal_SumsOverEveryPod(t *testing.T) {
	t.Parallel()

	cluster := startPodCluster(t, []http.HandlerFunc{
		serveExposition(exposition(3, 4, 10, 8, 1, 8)),
		serveExposition(exposition(0, 4, 10, 8, 1, 8)),
		serveExposition(exposition(9, 2, 10, 8, 1, 8)),
	})

	signal := newSignal(t, cluster.endpoint)
	signal.resolver = staticResolver{addresses: cluster.addresses}

	sample, err := signal.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if got := cluster.scrapes.Load(); got != 3 {
		t.Errorf("scraped %d pods, want 3", got)
	}
	// (3+4) + (0+4) + (9+2)
	if sample.PressureItems != 22 {
		t.Errorf("PressureItems = %d, want 22", sample.PressureItems)
	}
	if sample.InFlightRequests != 10 {
		t.Errorf("InFlightRequests = %d, want 10", sample.InFlightRequests)
	}
}

// A partial sum is the dangerous outcome: it under-reports pressure, and
// under-reported pressure is a scale-down. One unreachable pod must make the
// whole sample unavailable (FR-38).
func TestHTTPSignal_OnePodDownMakesTheWholeSampleUnavailable(t *testing.T) {
	t.Parallel()

	cluster := startPodCluster(t, []http.HandlerFunc{
		serveExposition(exposition(5, 4, 10, 8, 1, 8)),
	})

	signal := newSignal(t, cluster.endpoint)
	// The second pod's address resolves but nothing is listening on it, which
	// is what a pod that has just died looks like for the moment before its
	// endpoint is withdrawn.
	signal.resolver = staticResolver{addresses: append(cluster.addresses, "127.0.0.9")}

	sample, err := signal.Collect(context.Background())
	if err == nil {
		t.Fatal("expected an error naming the unreachable pod")
	}
	if sample.Available {
		t.Error("the sample must be unavailable, never a partial sum")
	}
	if sample.PressureItems != 0 || sample.InFlightRequests != 0 {
		t.Errorf("an unavailable sample must carry no values, got %+v", sample)
	}
}

// Every failure mode IT-16 enumerates must produce unavailable, never zero.
func TestHTTPSignal_EveryFailureIsUnavailableNeverZero(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
		wantMsg string
	}{
		{
			name: "a server error",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusInternalServerError)
			},
			wantMsg: "500",
		},
		{
			name: "an endpoint that is not a Normalizer",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, "# TYPE go_goroutines gauge\ngo_goroutines 7\n")
			},
			wantMsg: "normalizer_requests_queued",
		},
		{
			name: "only half the pressure signal",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, "# TYPE normalizer_requests_queued gauge\nnormalizer_requests_queued 3\n")
			},
			wantMsg: "normalizer_requests_in_flight",
		},
		{
			name: "an unparsable exposition",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, "this is not prometheus text\n{{{\n")
			},
			wantMsg: "parsing the exposition",
		},
		{
			name: "an HTML error page",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				fmt.Fprint(w, "<html><body>502 Bad Gateway</body></html>")
			},
			wantMsg: "parsing the exposition",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			server := httptest.NewServer(tc.handler)
			defer server.Close()

			signal := newSignal(t, server.URL+"/metrics")

			sample, err := signal.Collect(context.Background())
			if err == nil {
				t.Fatal("expected an error")
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err, tc.wantMsg)
			}
			if sample.Available {
				t.Error("the sample must be unavailable")
			}
			if sample.PressureItems != 0 {
				t.Errorf("PressureItems = %d; an unavailable sample must not carry a value", sample.PressureItems)
			}
		})
	}
}

func TestHTTPSignal_AConnectionRefusalIsUnavailable(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	endpoint := server.URL + "/metrics"
	server.Close()

	signal := newSignal(t, endpoint)

	sample, err := signal.Collect(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if sample.Available {
		t.Error("a refused connection must be unavailable, not zero pressure")
	}
}

// A source that blocks past signal.timeout is unavailable, not slow: a scrape
// that hangs would otherwise stall the reconcile loop.
func TestHTTPSignal_ATimeoutIsUnavailable(t *testing.T) {
	t.Parallel()

	release := make(chan struct{})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		fmt.Fprint(w, exposition(1, 1, 1, 1, 1, 1))
	}))

	// Ordering matters: Close waits for the handler to return, and the handler
	// cannot return until the channel is closed. Registering Close first means
	// it runs last, so the release happens before the wait rather than after it.
	defer server.Close()
	defer close(release)

	signal := newSignal(t, server.URL+"/metrics", func(c *config.SignalConfig) {
		c.Timeout = config.Duration(40 * time.Millisecond)
	})

	start := time.Now()
	sample, err := signal.Collect(context.Background())
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a timeout error")
	}
	if sample.Available {
		t.Error("a slow source must be unavailable")
	}
	if elapsed > 2*time.Second {
		t.Errorf("Collect took %s; the timeout is not bounding the scrape", elapsed)
	}
}

// A headless Service with no Ready pods resolves to nothing. That is "no answer
// about the workload", not "no work" — a pool with zero Ready pods is the least
// safe moment to report an idle workload.
func TestHTTPSignal_NoReadyPodsIsUnavailable(t *testing.T) {
	t.Parallel()

	signal := newSignal(t, "http://normalizer-metrics.data-pipeline.svc:8081/metrics")
	signal.resolver = staticResolver{}

	sample, err := signal.Collect(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "no Normalizer pod is Ready") {
		t.Errorf("error = %q, want it to explain that no pod is Ready", err)
	}
	if sample.Available {
		t.Error("the sample must be unavailable")
	}
}

func TestHTTPSignal_AResolutionFailureIsUnavailable(t *testing.T) {
	t.Parallel()

	signal := newSignal(t, "http://normalizer-metrics.data-pipeline.svc:8081/metrics")
	signal.resolver = staticResolver{err: fmt.Errorf("no such host")}

	sample, err := signal.Collect(context.Background())
	if err == nil {
		t.Fatal("expected an error")
	}
	if sample.Available {
		t.Error("the sample must be unavailable")
	}
}

// Freshness is the oldest contributor's: a sum is exactly as current as its
// stalest term, and averaging would hide one frozen pod behind several fresh
// ones (FR-34, DR-14).
func TestHTTPSignal_FreshnessIsTheOldestPods(t *testing.T) {
	t.Parallel()

	fresh := time.Date(2026, 3, 14, 12, 0, 30, 0, time.UTC)
	stale := time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)

	serveAt := func(reported time.Time) http.HandlerFunc {
		return func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Date", reported.Format(http.TimeFormat))
			fmt.Fprint(w, exposition(1, 1, 10, 8, 1, 8))
		}
	}
	cluster := startPodCluster(t, []http.HandlerFunc{serveAt(fresh), serveAt(stale)})

	signal := newSignal(t, cluster.endpoint)
	signal.resolver = staticResolver{addresses: cluster.addresses}

	sample, err := signal.Collect(context.Background())
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if !sample.SampledAt.Equal(stale) {
		t.Errorf("SampledAt = %s, want the oldest contributor %s", sample.SampledAt, stale)
	}
}

// Rates are derived from consecutive scrapes and are observability-only
// (FR-39). The first scrape has no interval, so it reports no rate.
func TestHTTPSignal_DerivesRatesFromConsecutiveScrapes(t *testing.T) {
	t.Parallel()

	var received, completed, latencySum float64
	var latencyCount int

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, exposition(0, 0, received, completed, latencySum, latencyCount))
	}))
	defer server.Close()

	clock := &stepClock{instant: time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)}
	signal := newSignal(t, server.URL+"/metrics")
	signal.clock = clock.now

	received, completed, latencySum, latencyCount = 100, 80, 16, 80
	first, err := signal.Collect(context.Background())
	if err != nil {
		t.Fatalf("first Collect: %v", err)
	}
	if first.RequestRateMilliPerSec != 0 || first.ProcessingRateMilliPerSec != 0 {
		t.Errorf("the first scrape has no interval to divide by, got %+v", first)
	}

	// Ten seconds later: 150 more received, 120 more completed.
	clock.advance(10 * time.Second)
	received, completed, latencySum, latencyCount = 250, 200, 46, 200
	second, err := signal.Collect(context.Background())
	if err != nil {
		t.Fatalf("second Collect: %v", err)
	}

	if second.RequestRateMilliPerSec != 15_000 {
		t.Errorf("RequestRateMilliPerSec = %d, want 15000 (150 records / 10 s)", second.RequestRateMilliPerSec)
	}
	if second.ProcessingRateMilliPerSec != 12_000 {
		t.Errorf("ProcessingRateMilliPerSec = %d, want 12000 (120 records / 10 s)", second.ProcessingRateMilliPerSec)
	}
	// 30 seconds of latency across 120 completions is 250 ms each.
	if second.ProcessingLatencyMillis != 250 {
		t.Errorf("ProcessingLatencyMillis = %d, want 250", second.ProcessingLatencyMillis)
	}
}

// A counter that goes backwards means the pod set changed — a restart, or a
// scale-down that removed a pod's counters from the sum. That interval's delta
// is meaningless and must be skipped, not reported as a negative or a wrapped
// rate.
func TestHTTPSignal_ACounterResetSkipsTheInterval(t *testing.T) {
	t.Parallel()

	var received, completed float64

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, exposition(2, 1, received, completed, 1, int(completed)))
	}))
	defer server.Close()

	clock := &stepClock{instant: time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)}
	signal := newSignal(t, server.URL+"/metrics")
	signal.clock = clock.now

	received, completed = 5000, 4800
	if _, err := signal.Collect(context.Background()); err != nil {
		t.Fatalf("first Collect: %v", err)
	}

	// The pod restarted: counters are back near zero.
	clock.advance(15 * time.Second)
	received, completed = 12, 9
	sample, err := signal.Collect(context.Background())
	if err != nil {
		t.Fatalf("second Collect: %v", err)
	}

	if sample.RequestRateMilliPerSec != 0 || sample.ProcessingRateMilliPerSec != 0 {
		t.Errorf("rates across a counter reset = %+v, want zero rather than a nonsense value", sample)
	}
	// Pressure is a gauge, so it is unaffected by the reset and still reported.
	if !sample.Available || sample.PressureItems != 3 {
		t.Errorf("pressure should still be reported across a reset, got %+v", sample)
	}

	// The next healthy interval must produce a rate again rather than staying
	// poisoned by the reset.
	clock.advance(10 * time.Second)
	received, completed = 112, 99
	recovered, err := signal.Collect(context.Background())
	if err != nil {
		t.Fatalf("third Collect: %v", err)
	}
	if recovered.RequestRateMilliPerSec != 10_000 {
		t.Errorf("RequestRateMilliPerSec = %d, want 10000 after recovery", recovered.RequestRateMilliPerSec)
	}
}

func TestNewHTTPSignal_RejectsAnUnusableEndpoint(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name     string
		endpoint string
	}{
		{name: "empty", endpoint: ""},
		{name: "no scheme", endpoint: "normalizer-metrics:8081/metrics"},
		{name: "wrong scheme", endpoint: "tcp://normalizer-metrics:8081/metrics"},
		{name: "not a URL", endpoint: "http://%zz"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			cfg := config.SignalConfig{
				Source:   config.SignalSourceHTTP,
				Endpoint: tc.endpoint,
				Timeout:  config.Duration(time.Second),
			}
			if _, err := NewHTTPSignal(cfg, time.Now); err == nil {
				t.Errorf("endpoint %q was accepted", tc.endpoint)
			}
		})
	}
}

// The factory must build the http source now that P2 implements it — the P1
// "not implemented" error would freeze a live demo with HoldStaleMetrics.
func TestNewWorkloadSignal_BuildsTheHTTPSource(t *testing.T) {
	t.Parallel()

	signal, err := NewWorkloadSignal(config.SignalConfig{
		Source:   config.SignalSourceHTTP,
		Endpoint: "http://normalizer-metrics.data-pipeline.svc:8081/metrics",
		Timeout:  config.Duration(3 * time.Second),
	}, time.Now)
	if err != nil {
		t.Fatalf("NewWorkloadSignal: %v", err)
	}
	defer func() {
		if err := signal.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	}()

	if signal.Source() != config.SignalSourceHTTP {
		t.Errorf("Source() = %q, want %q", signal.Source(), config.SignalSourceHTTP)
	}
}

// --- helpers ---------------------------------------------------------------

func newSignal(t *testing.T, endpoint string, tune ...func(*config.SignalConfig)) *HTTPSignal {
	t.Helper()

	cfg := config.SignalConfig{
		Source:   config.SignalSourceHTTP,
		Endpoint: endpoint,
		Timeout:  config.Duration(3 * time.Second),
	}
	for _, apply := range tune {
		apply(&cfg)
	}

	signal, err := NewHTTPSignal(cfg, time.Now)
	if err != nil {
		t.Fatalf("NewHTTPSignal(%q): %v", endpoint, err)
	}
	t.Cleanup(func() { _ = signal.Close() })
	return signal
}

// staticResolver stands in for cluster DNS, returning bare addresses exactly as
// LookupHost does for a headless Service.
type staticResolver struct {
	addresses []string
	err       error
}

func (r staticResolver) LookupHost(context.Context, string) ([]string, error) {
	if r.err != nil {
		return nil, r.err
	}
	return r.addresses, nil
}

type stepClock struct {
	instant time.Time
}

func (c *stepClock) now() time.Time          { return c.instant }
func (c *stepClock) advance(d time.Duration) { c.instant = c.instant.Add(d) }

func serveExposition(body string) http.HandlerFunc {
	return func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, body) }
}

// podCluster is a set of servers that present themselves the way a headless
// Service does: distinct addresses, one shared port.
type podCluster struct {
	endpoint  string
	addresses []string
	scrapes   *atomic.Int64
}

// startPodCluster starts one server per handler on distinct loopback addresses
// sharing a single port.
//
// The shared port is what makes this faithful. The source resolves a name to N
// addresses and joins each with the endpoint's port, so servers on N *different*
// ports would exercise a code path that cannot exist in a cluster — and would
// hide the bug where only the first address is ever scraped.
func startPodCluster(t *testing.T, handlers []http.HandlerFunc) podCluster {
	t.Helper()

	port := freePort(t)
	cluster := podCluster{
		endpoint: fmt.Sprintf("http://normalizer-metrics.data-pipeline.svc:%d/metrics", port),
		scrapes:  &atomic.Int64{},
	}

	for i, handler := range handlers {
		// 127.0.0.0/8 is entirely routed to loopback on Linux, so each pod gets
		// its own address without any interface configuration.
		address := fmt.Sprintf("127.0.0.%d", i+1)

		counted := handler
		wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			cluster.scrapes.Add(1)
			counted(w, r)
		})

		listener, err := net.Listen("tcp", net.JoinHostPort(address, strconv.Itoa(port)))
		if err != nil {
			t.Fatalf("listening on %s:%d: %v", address, port, err)
		}

		server := httptest.NewUnstartedServer(wrapped)
		_ = server.Listener.Close()
		server.Listener = listener
		server.Start()
		t.Cleanup(server.Close)

		cluster.addresses = append(cluster.addresses, address)
	}
	return cluster
}

// freePort finds a port that is free on every loopback address the cluster will
// use, by probing them all before any server is started.
func freePort(t *testing.T) int {
	t.Helper()

	probe, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("probing for a free port: %v", err)
	}
	port := probe.Addr().(*net.TCPAddr).Port
	if err := probe.Close(); err != nil {
		t.Fatalf("closing the probe listener: %v", err)
	}
	return port
}
