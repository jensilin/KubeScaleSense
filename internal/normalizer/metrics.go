package normalizer

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metric names. These are a published contract, not an implementation detail:
// the controller's `http` pressure source parses this exposition, so a rename
// here silently freezes the controller with HoldStaleMetrics. The names are
// asserted by a test on both sides.
const (
	MetricQueued    = "normalizer_requests_queued"
	MetricInFlight  = "normalizer_requests_in_flight"
	MetricReceived  = "normalizer_requests_received_total"
	MetricCompleted = "normalizer_requests_completed_total"
	MetricDuration  = "normalizer_request_duration_seconds"
)

// Request outcomes, used as the `outcome` label and kept to a closed set so the
// exposition is aggregatable.
const (
	// OutcomeNormalized is a record that was normalized and returned.
	OutcomeNormalized = "normalized"

	// OutcomeInvalid is a record this service will never accept: 400, and NiFi
	// must not retry it.
	OutcomeInvalid = "invalid"

	// OutcomeOverloaded is back-pressure: 503, and NiFi must retry it. Kept
	// distinct from "error" because it is the healthy response to a spike and
	// alerting on it as a fault would page someone for working as designed.
	OutcomeOverloaded = "overloaded"

	// OutcomeError is an unexpected internal failure: 500, retryable.
	OutcomeError = "error"
)

var outcomes = []string{OutcomeNormalized, OutcomeInvalid, OutcomeOverloaded, OutcomeError}

// Metrics is the Normalizer's exposition.
//
// Deliberately small. Five series answer every question the demonstration asks:
// how much work is outstanding (queued + in-flight — the pressure signal), how
// much arrived, how much completed and how it ended, and how long it took. A
// larger surface would be more to keep consistent and no more informative.
type Metrics struct {
	registry *prometheus.Registry

	queued    prometheus.Gauge
	inFlight  prometheus.Gauge
	received  prometheus.Counter
	completed *prometheus.CounterVec
	duration  *prometheus.HistogramVec
	ready     prometheus.Gauge
}

// NewMetrics registers the exposition on a private registry.
func NewMetrics() *Metrics {
	m := &Metrics{registry: prometheus.NewRegistry()}

	m.queued = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: MetricQueued,
		Help: "Records accepted and waiting for a processing slot. Half of the pressure signal.",
	})
	m.inFlight = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: MetricInFlight,
		Help: "Records being normalized right now. The other half of the pressure signal, and the pod-deletion-cost input.",
	})
	m.received = prometheus.NewCounter(prometheus.CounterOpts{
		Name: MetricReceived,
		Help: "Records accepted by the handler, before admission control.",
	})
	m.completed = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: MetricCompleted,
		Help: "Records that reached a terminal outcome.",
	}, []string{"outcome"})
	m.duration = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: MetricDuration,
		Help: "Seconds from handler entry to response, including time spent queued.",
		// Buckets span a fast normalization (sub-millisecond with zero cost
		// rounds) to a request that waited out most of its queue timeout,
		// because the interesting latency in this service is queue wait.
		Buckets: []float64{0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10},
	}, []string{"outcome"})
	m.ready = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "normalizer_ready",
		Help: "1 while the pod is accepting new records; 0 once it is draining. Makes the scale-down drain visible.",
	})

	m.registry.MustRegister(m.queued, m.inFlight, m.received, m.completed, m.duration, m.ready)

	// Process and Go collectors, because the demonstration's whole subject is
	// resource consumption: goroutine count and heap size are how "the pod is
	// busy" is corroborated independently of our own counters.
	m.registry.MustRegister(
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		collectors.NewGoCollector(),
	)

	// Pre-create every outcome at zero. An outcome that has not happened yet
	// must read zero rather than be absent, or the first 503 of a demo looks
	// like a new metric appearing rather than a threshold being crossed.
	for _, outcome := range outcomes {
		m.completed.WithLabelValues(outcome).Add(0)
	}
	m.ready.Set(1)

	return m
}

// Registry exposes the registry so tests can gather directly.
func (m *Metrics) Registry() *prometheus.Registry { return m.registry }

// Handler serves the exposition.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.registry, promhttp.HandlerOpts{})
}

func (m *Metrics) observe(outcome string, seconds float64) {
	m.completed.WithLabelValues(outcome).Inc()
	m.duration.WithLabelValues(outcome).Observe(seconds)
}

func (m *Metrics) setReady(ready bool) {
	if ready {
		m.ready.Set(1)
		return
	}
	m.ready.Set(0)
}
