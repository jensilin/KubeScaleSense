package metrics

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

// The Normalizer metric names this source consumes.
//
// Duplicated from internal/normalizer rather than imported, deliberately: the
// controller must not depend on the workload's source tree, because the whole
// claim of FR-36 is that any workload exposing these names can be the target. A
// test asserts the two lists agree, which is the right place for that coupling
// — a compile-time dependency would make the controller un-reusable to buy a
// guarantee a test provides just as well.
const (
	normalizerQueued    = "normalizer_requests_queued"
	normalizerInFlight  = "normalizer_requests_in_flight"
	normalizerReceived  = "normalizer_requests_received_total"
	normalizerCompleted = "normalizer_requests_completed_total"
	normalizerDuration  = "normalizer_request_duration_seconds"
)

// maxScrapeBytes bounds one target's exposition. The Go collector makes a
// Normalizer's /metrics a few kilobytes; a megabyte means something is very
// wrong, and reading it into memory on every reconcile would be the wrong
// response to that.
const maxScrapeBytes = 1 << 20

// HTTPSignal scrapes workload pressure from the Normalizer's own metrics
// endpoint (ADR-22, FR-36).
//
// Pressure is queued + in-flight records summed over pods, so the endpoint must
// resolve to *all* of them. A ClusterIP Service would load-balance the scrape to
// one pod and under-report the total by a factor of the replica count — which
// would look like a working demo right up to the moment it mattered. The source
// therefore resolves the endpoint's host and scrapes every address it finds,
// which is what the headless `normalizer-metrics` Service exists to provide.
//
// Rates and latency are derived from consecutive scrapes and are
// observability-only (FR-39); pressure is the only decision input.
type HTTPSignal struct {
	endpoint *url.URL
	timeout  time.Duration
	clock    func() time.Time

	client   *http.Client
	resolver HostResolver

	mu   sync.Mutex
	last scrapeTotals
}

// HostResolver resolves the endpoint's host to one address per pod.
//
// Exported so a test can present a multi-pod headless Service without a DNS
// server. The pod-fan-out is the part of this source most likely to be wrong in
// a way that still passes a single-endpoint test — under-reporting pressure by
// a factor of the replica count — so being able to test it against several real
// servers is worth one exported interface.
type HostResolver interface {
	LookupHost(ctx context.Context, host string) ([]string, error)
}

// scrapeTotals is the previous scrape's counters, kept to derive rates.
type scrapeTotals struct {
	at           time.Time
	received     float64
	completed    float64
	latencySum   float64
	latencyCount float64
	valid        bool
}

// NewHTTPSignal builds the scraping source against cluster DNS.
func NewHTTPSignal(cfg config.SignalConfig, clock func() time.Time) (*HTTPSignal, error) {
	return NewHTTPSignalWithResolver(cfg, clock, net.DefaultResolver)
}

// NewHTTPSignalWithResolver builds the scraping source with an explicit
// resolver.
func NewHTTPSignalWithResolver(
	cfg config.SignalConfig,
	clock func() time.Time,
	resolver HostResolver,
) (*HTTPSignal, error) {
	if resolver == nil {
		return nil, errors.New("a host resolver is required")
	}

	endpoint, err := url.Parse(strings.TrimSpace(cfg.Endpoint))
	if err != nil {
		return nil, fmt.Errorf("workload.signal.endpoint %q is not a valid URL: %w", cfg.Endpoint, err)
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, fmt.Errorf("workload.signal.endpoint %q must use the http or https scheme", cfg.Endpoint)
	}
	if endpoint.Hostname() == "" {
		return nil, fmt.Errorf("workload.signal.endpoint %q has no host", cfg.Endpoint)
	}
	if clock == nil {
		clock = time.Now
	}

	timeout := cfg.Timeout.Duration()
	return &HTTPSignal{
		endpoint: endpoint,
		timeout:  timeout,
		clock:    clock,
		client: &http.Client{
			// The per-request timeout is also enforced by the context, but it is
			// set here as well so that a caller who forgets the context cannot
			// produce a scrape that hangs the reconcile loop.
			Timeout: timeout,
			Transport: &http.Transport{
				// Connections are reused across reconciles: at one scrape per
				// pod per interval, a fresh TCP handshake each time is pure
				// latency, and a slow handshake is indistinguishable from a slow
				// workload.
				MaxIdleConnsPerHost: 8,
				IdleConnTimeout:     90 * time.Second,
			},
		},
		resolver: resolver,
	}, nil
}

// Source implements WorkloadSignal.
func (s *HTTPSignal) Source() string { return config.SignalSourceHTTP }

// Close implements WorkloadSignal.
func (s *HTTPSignal) Close() error {
	s.client.CloseIdleConnections()
	return nil
}

// Collect scrapes every Normalizer pod and sums the result.
//
// Any failure makes the whole sample unavailable. A partial sum is the dangerous
// outcome: it under-reports pressure, and under-reported pressure is a
// scale-down. Reporting "no answer" and freezing is always safe, so the
// all-or-nothing rule is not conservatism for its own sake (FR-38).
func (s *HTTPSignal) Collect(ctx context.Context) (Sample, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	targets, err := s.targets(ctx)
	if err != nil {
		return Unavailable(), err
	}

	var (
		total  scrapeTotals
		queued float64
		flight float64
		oldest time.Time
	)

	for _, target := range targets {
		scraped, err := s.scrape(ctx, target)
		if err != nil {
			return Unavailable(), fmt.Errorf("scraping %s: %w", target, err)
		}

		queued += scraped.queued
		flight += scraped.inFlight
		total.received += scraped.received
		total.completed += scraped.completed
		total.latencySum += scraped.latencySum
		total.latencyCount += scraped.latencyCount

		// Freshness is the oldest contributor's: a sum is exactly as current as
		// its stalest term, and averaging the timestamps would hide one frozen
		// pod behind several fresh ones (DR-14).
		if oldest.IsZero() || scraped.reportedAt.Before(oldest) {
			oldest = scraped.reportedAt
		}
	}

	now := s.clock()
	total.at = now
	total.valid = true

	sample := Sample{
		PressureItems:    int64(queued + flight),
		InFlightRequests: int64(flight),
		SampledAt:        oldest,
		Available:        true,
	}
	if sample.SampledAt.IsZero() {
		sample.SampledAt = now
	}

	s.mu.Lock()
	previous := s.last
	s.last = total
	s.mu.Unlock()

	applyRates(&sample, previous, total)
	return sample, nil
}

// targets resolves the endpoint to one URL per pod.
//
// A literal IP in the endpoint is used as-is; a name is resolved, and every
// address it yields is a target. The addresses are not sorted, because the sum
// does not depend on their order and imposing one would imply a stability the
// DNS response does not have.
func (s *HTTPSignal) targets(ctx context.Context) ([]string, error) {
	host := s.endpoint.Hostname()
	if net.ParseIP(host) != nil {
		return []string{s.endpoint.String()}, nil
	}

	addresses, err := s.resolver.LookupHost(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("resolving %q: %w", host, err)
	}
	if len(addresses) == 0 {
		// An empty answer for a headless Service means no Ready pods. That is
		// genuinely "no answer about the workload's pressure", not "no
		// pressure": a pool with zero Ready pods is the least safe moment to
		// report an idle workload.
		return nil, fmt.Errorf("%q resolved to no addresses, so no Normalizer pod is Ready", host)
	}

	port := s.endpoint.Port()
	targets := make([]string, 0, len(addresses))
	for _, address := range addresses {
		target := *s.endpoint
		if port == "" {
			target.Host = address
		} else {
			target.Host = net.JoinHostPort(address, port)
		}
		targets = append(targets, target.String())
	}
	return targets, nil
}

// podScrape is one pod's contribution.
type podScrape struct {
	queued       float64
	inFlight     float64
	received     float64
	completed    float64
	latencySum   float64
	latencyCount float64
	reportedAt   time.Time
}

func (s *HTTPSignal) scrape(ctx context.Context, target string) (podScrape, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return podScrape{}, err
	}
	// The workload's data path must never see a mutating request from the
	// controller (FR-37). GET on a metrics endpoint is the whole interaction.
	request.Header.Set("Accept", string(expfmt.NewFormat(expfmt.TypeTextPlain)))

	response, err := s.client.Do(request)
	if err != nil {
		return podScrape{}, err
	}
	defer func() {
		// Drain before closing so the connection can be reused; a closed body
		// with bytes outstanding is a discarded connection.
		_, _ = io.Copy(io.Discard, response.Body)
		_ = response.Body.Close()
	}()

	if response.StatusCode != http.StatusOK {
		return podScrape{}, fmt.Errorf("unexpected status %s", response.Status)
	}

	// Legacy name validation: every name this source reads is
	// [a-zA-Z_:][a-zA-Z0-9_:]*, and the stricter scheme means a workload that
	// starts emitting UTF-8 metric names fails the scrape loudly instead of
	// silently contributing nothing to the sum.
	parser := expfmt.NewTextParser(model.LegacyValidation)
	families, err := parser.TextToMetricFamilies(io.LimitReader(response.Body, maxScrapeBytes))
	if err != nil {
		return podScrape{}, fmt.Errorf("parsing the exposition: %w", err)
	}

	scrape := podScrape{reportedAt: reportedAt(response, s.clock())}

	// A missing pressure metric is an error rather than a zero. The likely
	// causes are a renamed metric or the wrong endpoint, and both would
	// otherwise present as a permanently idle workload.
	if scrape.queued, err = gaugeValue(families, normalizerQueued); err != nil {
		return podScrape{}, err
	}
	if scrape.inFlight, err = gaugeValue(families, normalizerInFlight); err != nil {
		return podScrape{}, err
	}

	// The rate inputs are observability-only, so a missing one degrades the
	// derived rate to zero instead of failing the sample.
	scrape.received, _ = counterSum(families, normalizerReceived)
	scrape.completed, _ = counterSum(families, normalizerCompleted)
	scrape.latencySum, scrape.latencyCount = histogramTotals(families, normalizerDuration)

	return scrape, nil
}

// reportedAt is when the pod produced the exposition, per its own Date header.
//
// The source's timestamp rather than ours, so a pod that is reachable but
// frozen — serving a cached response from minutes ago — is detected as stale
// instead of looking perpetually fresh (FR-34, DR-14). Date has one-second
// granularity, which is far finer than the staleness threshold it feeds.
func reportedAt(response *http.Response, fallback time.Time) time.Time {
	parsed, err := http.ParseTime(response.Header.Get("Date"))
	if err != nil {
		return fallback
	}
	return parsed
}

// applyRates derives the observability-only rates from two consecutive scrapes.
func applyRates(sample *Sample, previous, current scrapeTotals) {
	if !previous.valid {
		// The first scrape has no interval to divide by. Zero is honest here in
		// a way it never is for pressure: a rate is a statement about a period,
		// and no period has elapsed yet.
		return
	}
	elapsed := current.at.Sub(previous.at).Seconds()
	if elapsed <= 0 {
		return
	}

	// A decrease means the pod set changed — a restart, or a scale-down that
	// removed a pod's counters from the sum. The delta across that boundary is
	// meaningless, so the interval is skipped rather than reported as a negative
	// rate or, worse, as a huge positive one after an int wrap.
	if current.received < previous.received || current.completed < previous.completed {
		return
	}

	sample.RequestRateMilliPerSec = perSecondMilli(current.received-previous.received, elapsed)
	sample.ProcessingRateMilliPerSec = perSecondMilli(current.completed-previous.completed, elapsed)

	if completed := current.latencyCount - previous.latencyCount; completed > 0 {
		seconds := (current.latencySum - previous.latencySum) / completed
		sample.ProcessingLatencyMillis = int64(seconds * 1000)
	}
}

func perSecondMilli(delta, elapsedSeconds float64) int64 {
	return int64(delta / elapsedSeconds * 1000)
}

func gaugeValue(families map[string]*dto.MetricFamily, name string) (float64, error) {
	family, ok := families[name]
	if !ok || len(family.GetMetric()) == 0 {
		return 0, fmt.Errorf("the exposition has no %s; the endpoint is not a Normalizer or the metric was renamed", name)
	}
	var total float64
	for _, metric := range family.GetMetric() {
		total += metric.GetGauge().GetValue()
	}
	return total, nil
}

func counterSum(families map[string]*dto.MetricFamily, name string) (float64, error) {
	family, ok := families[name]
	if !ok {
		return 0, errors.New("absent")
	}
	var total float64
	for _, metric := range family.GetMetric() {
		total += metric.GetCounter().GetValue()
	}
	return total, nil
}

func histogramTotals(families map[string]*dto.MetricFamily, name string) (sum, count float64) {
	family, ok := families[name]
	if !ok {
		return 0, 0
	}
	for _, metric := range family.GetMetric() {
		histogram := metric.GetHistogram()
		sum += histogram.GetSampleSum()
		count += float64(histogram.GetSampleCount())
	}
	return sum, count
}
