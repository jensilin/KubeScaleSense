package config

import (
	"fmt"
	"net"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"k8s.io/apimachinery/pkg/labels"
)

// FieldError is a single validation failure, named by its YAML key so that the
// message points at the line an operator has to edit.
type FieldError struct {
	// Key is the dotted YAML path, e.g. "scaling.holdBackoff.factor".
	Key string
	// Value is the rejected value, rendered into the message.
	Value any
	// Problem states the constraint in the imperative ("must be at least 5s").
	Problem string
	// Ref cites the requirement the constraint comes from, so the rule can be
	// looked up rather than argued with.
	Ref string
}

func (e FieldError) Error() string {
	var b strings.Builder
	b.WriteString(e.Key)
	b.WriteString(": ")
	if e.Value != nil && e.Value != "" {
		fmt.Fprintf(&b, "%v — ", e.Value)
	}
	b.WriteString(e.Problem)
	if env := EnvVarFor(e.Key); env != "" {
		fmt.Fprintf(&b, " (override with %s)", env)
	}
	if e.Ref != "" {
		fmt.Fprintf(&b, " [%s]", e.Ref)
	}
	return b.String()
}

// ValidationError aggregates every problem found in one pass. Reporting all of
// them at once is deliberate: an operator fixing a mounted ConfigMap pays a pod
// restart per attempt, so revealing one error at a time turns a two-minute edit
// into a twenty-minute loop.
type ValidationError struct {
	Errors []FieldError
}

func (e *ValidationError) Error() string {
	if len(e.Errors) == 1 {
		return "invalid configuration: " + e.Errors[0].Error()
	}
	var b strings.Builder
	fmt.Fprintf(&b, "invalid configuration (%d problems):", len(e.Errors))
	for _, fe := range e.Errors {
		b.WriteString("\n  - ")
		b.WriteString(fe.Error())
	}
	return b.String()
}

// Keys returns the offending keys in sorted order, for tests and for structured
// logging of a startup failure.
func (e *ValidationError) Keys() []string {
	keys := make([]string, 0, len(e.Errors))
	for _, fe := range e.Errors {
		keys = append(keys, fe.Key)
	}
	sort.Strings(keys)
	return keys
}

type validator struct {
	problems []FieldError
}

func (v *validator) add(key string, value any, problem, ref string) {
	v.problems = append(v.problems, FieldError{Key: key, Value: value, Problem: problem, Ref: ref})
}

// Validate checks the configuration and returns a *ValidationError describing
// every problem, or nil if the configuration is safe to run.
//
// Validation is pure: it touches neither the filesystem nor the network, so it
// is fully table-testable. The one environment-dependent check — that a
// synthetic signal file is actually readable — lives in Loader.Load.
func (c *Config) Validate() error {
	v := &validator{}

	c.validateController(v)
	c.validateTarget(v)
	c.validateWorkload(v)
	c.validateResources(v)
	c.validateScaling(v)
	c.validatePending(v)
	c.validateCrossField(v)

	if len(v.problems) == 0 {
		return nil
	}
	return &ValidationError{Errors: v.problems}
}

func (c *Config) validateController(v *validator) {
	if c.Controller.Interval.Duration() < MinInterval {
		v.add("controller.interval", c.Controller.Interval,
			"must be at least "+MinInterval.String()+"; a shorter period adds API and metrics load without improving reaction time",
			"CR-2")
	}
	if err := validateListenAddr(c.Controller.MetricsAddr); err != nil {
		v.add("controller.metricsAddr", c.Controller.MetricsAddr, err.Error(), "CR-2")
	}
	if err := validateListenAddr(c.Controller.HealthAddr); err != nil {
		v.add("controller.healthAddr", c.Controller.HealthAddr, err.Error(), "CR-2")
	}
	if c.Controller.MetricsAddr != "" && c.Controller.MetricsAddr == c.Controller.HealthAddr {
		v.add("controller.healthAddr", c.Controller.HealthAddr,
			"must differ from controller.metricsAddr; both cannot bind the same address", "CR-2")
	}
	if !isOneOf(c.Controller.LogLevel, LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError) {
		v.add("controller.logLevel", c.Controller.LogLevel,
			oneOfMessage(LogLevelDebug, LogLevelInfo, LogLevelWarn, LogLevelError), "CR-2")
	}
	if !isOneOf(c.Controller.LogFormat, LogFormatJSON, LogFormatText) {
		v.add("controller.logFormat", c.Controller.LogFormat,
			oneOfMessage(LogFormatJSON, LogFormatText), "CR-2")
	}

	// controller.dryRun is no longer constrained here.
	//
	// Through P2 this validator refused dryRun: false outright, because the
	// binary had no actuator that could write and a controller that appears to
	// run live while silently changing nothing is the more dangerous of the two
	// failure modes. P3 supplies the write path, so false is now a legal and
	// meaningful value.
	//
	// What has not changed is the default: DefaultDryRun remains true, so live
	// actuation is something an operator turns on deliberately rather than
	// something they get by omission (FR-04).
}

func (c *Config) validateTarget(v *validator) {
	// Required, with no default: see TargetConfig and CR-1.
	if strings.TrimSpace(c.Target.Namespace) == "" {
		v.add("target.namespace", nil, "is required and has no default; set the namespace of the Deployment to scale", "CR-1")
	}
	if strings.TrimSpace(c.Target.Deployment) == "" {
		v.add("target.deployment", nil, "is required and has no default; set the name of the Deployment to scale", "CR-1")
	}
	if c.Target.MinReplicas < 1 {
		v.add("target.minReplicas", c.Target.MinReplicas,
			"must be at least 1; scale-to-zero is not supported in this version", "NG-4")
	}
	if c.Target.MaxReplicas < 1 {
		v.add("target.maxReplicas", c.Target.MaxReplicas, "must be at least 1", "CR-2")
	}
	if c.Target.MinReplicas >= 1 && c.Target.MaxReplicas >= 1 && c.Target.MinReplicas > c.Target.MaxReplicas {
		v.add("target.minReplicas", c.Target.MinReplicas,
			fmt.Sprintf("must not exceed target.maxReplicas (%d)", c.Target.MaxReplicas), "CR-2")
	}
}

func (c *Config) validateWorkload(v *validator) {
	w := c.Workload

	if w.ItemsPerReplica <= 0 {
		v.add("workload.itemsPerReplica", w.ItemsPerReplica,
			"must be greater than 0; it is the divisor converting workload pressure into replicas", "FS-19")
	}
	if w.TargetCPUUtilizationPercent < 1 || w.TargetCPUUtilizationPercent > 100 {
		v.add("workload.targetCPUUtilizationPercent", w.TargetCPUUtilizationPercent,
			"must be between 1 and 100; it is a percentage of the pod's CPU request", "CR-2")
	}
	if w.MetricsStaleAfter.Duration() <= 0 {
		v.add("workload.metricsStaleAfter", w.MetricsStaleAfter, "must be greater than 0", "FR-09")
	}
	if w.PodWarmupPeriod.Duration() < 0 {
		v.add("workload.podWarmupPeriod", w.PodWarmupPeriod, "must not be negative", "FR-28")
	}

	if !isOneOf(w.BacklogSmoothing.Mode, SmoothingModeNone, SmoothingModeEWMA) {
		v.add("workload.backlogSmoothing.mode", w.BacklogSmoothing.Mode,
			oneOfMessage(SmoothingModeNone, SmoothingModeEWMA), "CR-2")
	}
	if w.BacklogSmoothing.Mode == SmoothingModeEWMA &&
		(w.BacklogSmoothing.Alpha <= 0 || w.BacklogSmoothing.Alpha > 1) {
		v.add("workload.backlogSmoothing.alpha", w.BacklogSmoothing.Alpha,
			"must be greater than 0 and at most 1 when mode is \"ewma\"; it is the weight given to the newest sample", "CR-2")
	}

	c.validateSignal(v)
}

func (c *Config) validateSignal(v *validator) {
	s := c.Workload.Signal

	if !isOneOf(s.Source, SignalSourceSynthetic, SignalSourceHTTP, SignalSourceNone) {
		v.add("workload.signal.source", s.Source,
			oneOfMessage(SignalSourceSynthetic, SignalSourceHTTP, SignalSourceNone), "FR-36")
		return
	}

	if s.Timeout.Duration() <= 0 {
		v.add("workload.signal.timeout", s.Timeout,
			"must be greater than 0; a scrape that never times out cannot report the signal as unavailable", "FR-38")
	}

	switch s.Source {
	case SignalSourceHTTP:
		if strings.TrimSpace(s.Endpoint) == "" {
			v.add("workload.signal.endpoint", nil,
				"is required when workload.signal.source is \"http\"; set the Normalizer metrics URL", "FR-36")
			break
		}
		if err := validateHTTPEndpoint(s.Endpoint); err != nil {
			v.add("workload.signal.endpoint", s.Endpoint, err.Error(), "FR-36")
		}
	case SignalSourceSynthetic:
		if strings.TrimSpace(s.SyntheticPath) == "" {
			v.add("workload.signal.syntheticPath", nil,
				"is required when workload.signal.source is \"synthetic\"; set the path of the scripted pressure series", "FR-36")
		}
	}

	// An endpoint configured against a source that never scrapes is almost
	// always a half-finished edit, and silently ignoring it is how a demo ends
	// up reading synthetic numbers while everyone believes it is live.
	if s.Source != SignalSourceHTTP && strings.TrimSpace(s.Endpoint) != "" {
		v.add("workload.signal.endpoint", s.Endpoint,
			fmt.Sprintf("is set but workload.signal.source is %q, so it would never be read; set source to \"http\" or clear the endpoint", s.Source),
			"FR-36")
	}
}

func (c *Config) validateResources(v *validator) {
	r := c.Resources

	if r.PerNodeReserveCPUMilli < 0 {
		v.add("resources.perNodeReserveCPUMilli", r.PerNodeReserveCPUMilli, "must not be negative", "CR-4")
	}
	if r.PerNodeReserveMemoryMiB < 0 {
		v.add("resources.perNodeReserveMemoryMiB", r.PerNodeReserveMemoryMiB, "must not be negative", "CR-4")
	}
	if r.FitCapacityMarginPods < 0 {
		v.add("resources.fitCapacityMarginPods", r.FitCapacityMarginPods,
			"must not be negative; a negative margin would claim more capacity than was computed", "CR-4")
	}
	if err := validateLabelSelector(r.NodeLabelSelector); err != nil {
		v.add("resources.nodeLabelSelector", r.NodeLabelSelector, err.Error(), "CR-2")
	}
}

func (c *Config) validateScaling(v *validator) {
	s := c.Scaling

	if s.ScaleUpCooldown.Duration() < 0 {
		v.add("scaling.scaleUpCooldown", s.ScaleUpCooldown, "must not be negative", "CR-2")
	}
	if s.ScaleDownCooldown.Duration() < 0 {
		v.add("scaling.scaleDownCooldown", s.ScaleDownCooldown, "must not be negative", "CR-2")
	}
	if s.ScaleDownStabilizationWindow.Duration() <= 0 {
		v.add("scaling.scaleDownStabilizationWindow", s.ScaleDownStabilizationWindow,
			"must be greater than 0; without a window a single quiet sample could trigger a scale-down", "FR-29")
	}
	if s.ScaleDownWindowCoverage <= 0 || s.ScaleDownWindowCoverage > 1 {
		v.add("scaling.scaleDownWindowCoverage", s.ScaleDownWindowCoverage,
			"must be greater than 0 and at most 1; it is the fraction of expected samples required before scaling down", "FR-29")
	}
	if s.TolerancePercent < 0 || s.TolerancePercent > 100 {
		v.add("scaling.tolerancePercent", s.TolerancePercent, "must be between 0 and 100", "CR-2")
	}
	if s.MaxScaleUpStep < 1 {
		v.add("scaling.maxScaleUpStep", s.MaxScaleUpStep,
			"must be at least 1, otherwise the controller can never scale up", "FR-03")
	}
	if s.MaxScaleDownStep < 1 {
		v.add("scaling.maxScaleDownStep", s.MaxScaleDownStep,
			"must be at least 1, otherwise the controller can never scale down", "FR-03")
	}
	if s.ExternalChangeTolerance < 0 {
		v.add("scaling.externalChangeTolerance", s.ExternalChangeTolerance, "must not be negative", "FR-31")
	}

	if s.HoldBackoff.Initial.Duration() <= 0 {
		v.add("scaling.holdBackoff.initial", s.HoldBackoff.Initial, "must be greater than 0", "FR-18")
	}
	if s.HoldBackoff.Max.Duration() <= 0 {
		v.add("scaling.holdBackoff.max", s.HoldBackoff.Max, "must be greater than 0", "FR-18")
	}
	if s.HoldBackoff.Initial.Duration() > 0 && s.HoldBackoff.Max.Duration() > 0 &&
		s.HoldBackoff.Max.Duration() < s.HoldBackoff.Initial.Duration() {
		v.add("scaling.holdBackoff.max", s.HoldBackoff.Max,
			fmt.Sprintf("must be at least scaling.holdBackoff.initial (%s)", s.HoldBackoff.Initial), "FR-18")
	}
	if s.HoldBackoff.Factor <= 1 {
		v.add("scaling.holdBackoff.factor", s.HoldBackoff.Factor,
			"must be greater than 1; a factor of 1 or less never backs off", "FR-18")
	}
}

func (c *Config) validatePending(v *validator) {
	p := c.Pending

	if p.PendingPodTimeout.Duration() <= 0 {
		v.add("pending.pendingPodTimeout", p.PendingPodTimeout, "must be greater than 0", "FS-06")
	}
	if p.PodStartupTimeout.Duration() <= 0 {
		v.add("pending.podStartupTimeout", p.PodStartupTimeout, "must be greater than 0", "FR-30")
	}
	if !isOneOf(p.OnPendingTimeout, OnPendingRevert, OnPendingFreeze, OnPendingNone) {
		v.add("pending.onPendingTimeout", p.OnPendingTimeout,
			oneOfMessage(OnPendingRevert, OnPendingFreeze, OnPendingNone), "FS-06")
	}
}

// validateCrossField holds the combinations that are individually legal and
// jointly unsafe. These are the expensive ones to discover at runtime, because
// each produces a controller that looks configured and behaves wrongly.
func (c *Config) validateCrossField(v *validator) {
	interval := c.Controller.Interval.Duration()
	if interval < MinInterval {
		// Already reported; the ratios below would be noise on top of it.
		return
	}

	if c.Workload.MetricsStaleAfter.Duration() > 0 && c.Workload.MetricsStaleAfter.Duration() < interval {
		v.add("workload.metricsStaleAfter", c.Workload.MetricsStaleAfter,
			fmt.Sprintf("must be at least controller.interval (%s); a sample taken once per interval would always be stale on arrival and the controller would never act", interval),
			"FR-09")
	}

	if window := c.Scaling.ScaleDownStabilizationWindow.Duration(); window > 0 && window < interval {
		v.add("scaling.scaleDownStabilizationWindow", c.Scaling.ScaleDownStabilizationWindow,
			fmt.Sprintf("must be at least controller.interval (%s); a window shorter than one sample period cannot be covered", interval),
			"FR-29")
	}

	// How many samples the window holds, and therefore how many missed
	// reconciles the coverage fraction tolerates, is deliberately not validated
	// here. A tight window with high coverage is strict tuning rather than an
	// unsafe configuration — coverage: 1.0 means exactly what it says — and a
	// validator that rejects legal tuning is one operators learn to work
	// around. The observable consequence belongs in a metric, not a startup
	// error.

	if c.Pending.PendingPodTimeout.Duration() > 0 && c.Pending.PendingPodTimeout.Duration() < interval {
		v.add("pending.pendingPodTimeout", c.Pending.PendingPodTimeout,
			fmt.Sprintf("must be at least controller.interval (%s); a pod could otherwise be declared timed out before the loop ever observes it", interval),
			"FS-06")
	}
}

func validateListenAddr(addr string) error {
	if addr == "" {
		// Empty disables the listener, which is a legitimate choice.
		return nil
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("must be a \"host:port\" address such as %q, or empty to disable the listener", ":8080")
	}
	if port == "" {
		return fmt.Errorf("must include a port, such as %q", ":8080")
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 1 || n > 65535 {
		return fmt.Errorf("port %q must be a number between 1 and 65535", port)
	}
	if host != "" && net.ParseIP(host) == nil {
		return fmt.Errorf("host %q must be an IP address or empty to listen on all interfaces", host)
	}
	return nil
}

func validateHTTPEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("is not a valid URL: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("must use the http or https scheme, e.g. %q",
			"http://normalizer-service.data-pipeline.svc.cluster.local:9090/metrics")
	}
	if u.Host == "" {
		return fmt.Errorf("must include a host, e.g. %q",
			"http://normalizer-service.data-pipeline.svc.cluster.local:9090/metrics")
	}
	return nil
}

// validateLabelSelector checks the selector against the real Kubernetes
// grammar.
//
// Phase 0 hand-rolled an equality-only check to avoid pulling API machinery
// into the foundation, and noted that the full grammar belonged to the client
// that would consume the value. That client now exists, so the placeholder is
// replaced with the same parser the candidate-node filter uses. The difference
// is not cosmetic: the hand-rolled version rejected legal selectors such as
// "node-pool in (workers,spot)" and accepted illegal label keys.
func validateLabelSelector(selector string) error {
	if strings.TrimSpace(selector) == "" {
		return nil
	}
	if _, err := labels.Parse(selector); err != nil {
		return fmt.Errorf("is not a valid label selector (%w); expected a form such as %q", err, "node-pool=workers")
	}
	return nil
}

func isOneOf(value string, allowed ...string) bool {
	for _, a := range allowed {
		if value == a {
			return true
		}
	}
	return false
}

func oneOfMessage(allowed ...string) string {
	quoted := make([]string, len(allowed))
	for i, a := range allowed {
		quoted[i] = strconv.Quote(a)
	}
	return "must be one of " + strings.Join(quoted, ", ")
}
