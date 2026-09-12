// Package nififlow_test checks the NiFi flow definition's load-bearing
// settings.
//
// Three of the settings in that file are the difference between a
// demonstration and a misleading graph, and the implementation plan calls them
// deliverables rather than configuration. A deliverable that nothing checks is
// a deliverable that drifts, and all three fail *quietly*: the pipeline keeps
// running and the numbers stop meaning what they appear to.
//
// This test reads the shipped JSON. It cannot prove NiFi accepts the file —
// only a running NiFi can do that, which is what tests/e2e/assert-nifi-flow.sh
// checks against the live REST API at demo time. What it can do is run on every
// commit, with no cluster, and catch the edit that quietly halves the
// concurrent task count.
package nififlow_test

import (
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	flowPath       = "../../../deploy/demo/nifi/flow/normalizer-pipeline.json"
	controllerPath = "../../../config/kubescalesense.yaml"
	demoConfigPath = "../../../deploy/demo/kubescalesense-configmap.yaml"
	normalizerPath = "../../../deploy/normalizer/configmap.yaml"

	invokeHTTP  = "org.apache.nifi.processors.standard.InvokeHTTP"
	nifiVersion = "2.6.7"
)

// The subset of the flow-definition format this test needs. Decoding into a
// narrow struct rather than a map keeps the assertions readable, and unknown
// fields are ignored so a NiFi upgrade that adds keys does not fail here.
type flowDefinition struct {
	FlowContents struct {
		Processors  []processor  `json:"processors"`
		Connections []connection `json:"connections"`
	} `json:"flowContents"`
}

type processor struct {
	Name                             string            `json:"name"`
	Type                             string            `json:"type"`
	Bundle                           bundle            `json:"bundle"`
	Properties                       map[string]string `json:"properties"`
	ConcurrentlySchedulableTaskCount int               `json:"concurrentlySchedulableTaskCount"`
	AutoTerminatedRelationships      []string          `json:"autoTerminatedRelationships"`
	RetriedRelationships             []string          `json:"retriedRelationships"`
	RetryCount                       int               `json:"retryCount"`
	BackoffMechanism                 string            `json:"backoffMechanism"`
	MaxBackoffPeriod                 string            `json:"maxBackoffPeriod"`
}

type bundle struct {
	Group    string `json:"group"`
	Artifact string `json:"artifact"`
	Version  string `json:"version"`
}

type connection struct {
	Name                  string   `json:"name"`
	SelectedRelationships []string `json:"selectedRelationships"`
	Source                endpoint `json:"source"`
	Destination           endpoint `json:"destination"`
}

type endpoint struct {
	Name string `json:"name"`
}

// A-13 / FS-28. Work is *pushed* over HTTP, so throughput is
// min(client concurrency, replica capacity). If NiFi dispatches fewer
// concurrent requests than there are replicas, the controller scales up
// correctly and nothing improves — and the demo shows replicas rising while
// the backlog also rises, which looks exactly like a broken controller.
func TestInvokeHTTP_ConcurrencyExceedsMaxReplicas(t *testing.T) {
	t.Parallel()

	invoke := invokeHTTPProcessor(t)
	maxReplicas := configuredMaxReplicas(t)

	if invoke.ConcurrentlySchedulableTaskCount <= maxReplicas {
		t.Errorf("InvokeHTTP has %d concurrent tasks but maxReplicas is %d.\n"+
			"Throughput is min(client concurrency, replica capacity), so at this setting the pool's "+
			"last %d replicas would sit idle and scaling would change nothing (A-13, FS-28).",
			invoke.ConcurrentlySchedulableTaskCount, maxReplicas,
			maxReplicas-invoke.ConcurrentlySchedulableTaskCount+1)
	}
}

// D-02. Retry on the failure relationships is the entire durability guarantee
// of the v0.2 architecture: there is no queue, no store, and no transaction
// between NiFi and the pods, so a record that fails and is not re-sent is a
// record that is gone.
func TestInvokeHTTP_RetriesTransientFailures(t *testing.T) {
	t.Parallel()

	invoke := invokeHTTPProcessor(t)

	for _, relationship := range []string{"Retry", "Failure"} {
		if !slices.Contains(invoke.RetriedRelationships, relationship) {
			t.Errorf("the %q relationship is not retried (retried: %v).\n"+
				"Without retry there is nothing between a failed request and a lost record: "+
				"the v0.2 design has no queue, no store, and no transaction (D-02).",
				relationship, invoke.RetriedRelationships)
		}
	}
	if invoke.RetryCount < 1 {
		t.Errorf("retryCount = %d, want at least 1", invoke.RetryCount)
	}
	if invoke.MaxBackoffPeriod == "" {
		t.Error("no maxBackoffPeriod: retry must be bounded, or a persistently failing pool is retried forever")
	}
	if invoke.BackoffMechanism == "" {
		t.Error("no backoffMechanism: retry without backoff hammers a pool that is already struggling")
	}
}

// The poison-pill guard. A 400 means the record is malformed and will fail
// identically forever, so it must NOT be retried — retrying it spends the
// pool's capacity on a record that can never succeed.
func TestInvokeHTTP_DoesNotRetryMalformedRecords(t *testing.T) {
	t.Parallel()

	invoke := invokeHTTPProcessor(t)

	if slices.Contains(invoke.RetriedRelationships, "No Retry") {
		t.Error(`the "No Retry" relationship is configured as retried. ` +
			"A 400 from the Normalizer means the record is malformed and will fail identically forever; " +
			"retrying it is an infinite loop that turns one bad line into a pipeline outage.")
	}

	// It must go somewhere visible, though: a record that silently disappears
	// cannot be accounted for at either end of the conservation assertion.
	if !routed(t, "No Retry") {
		t.Error(`the "No Retry" relationship is neither retried nor connected to a sink, ` +
			"so malformed records vanish without trace")
	}
}

// FS-29. If NiFi's read timeout is shorter than the Normalizer's worst-case
// processing time, every slow record is retried while the first attempt is
// still in flight — so the pool does duplicate work exactly under the load
// where it can least afford to.
func TestInvokeHTTP_ReadTimeoutExceedsTheNormalizersWorstCase(t *testing.T) {
	t.Parallel()

	invoke := invokeHTTPProcessor(t)

	readTimeout := nifiSeconds(t, invoke.Properties["Socket Read Timeout"])
	// The Normalizer's worst case is the whole time a record may legitimately
	// be held: waiting for a slot, then being processed.
	queueTimeout := goSeconds(t, normalizerSetting(t, "NORMALIZER_QUEUE_TIMEOUT"))
	processingDelay := goSeconds(t, normalizerSetting(t, "NORMALIZER_PROCESSING_DELAY"))
	worstCase := queueTimeout + processingDelay

	if readTimeout <= worstCase {
		t.Errorf("InvokeHTTP's read timeout is %gs but a record may legitimately take %gs "+
			"(queue timeout %gs + processing delay %gs).\n"+
			"Every slow record would be retried while the first attempt is still being served (FS-29).",
			readTimeout, worstCase, queueTimeout, processingDelay)
	}
}

// The URL must name the Service. A pod name or pod IP would pin the flow to one
// replica, so adding replicas would change nothing and the demonstration would
// be measuring a single pod.
func TestInvokeHTTP_TargetsTheServiceNotAPod(t *testing.T) {
	t.Parallel()

	invoke := invokeHTTPProcessor(t)
	url := invoke.Properties["HTTP URL"]

	if !strings.Contains(url, "normalizer-service") {
		t.Errorf("HTTP URL = %q, want it to target the normalizer-service Service", url)
	}
	if strings.Contains(url, "normalizer-0") || strings.Contains(url, ".pod.") {
		t.Errorf("HTTP URL = %q targets a pod; load must be spread by the Service", url)
	}
	// A literal IPv4 address in the URL would survive a Service rename and
	// break silently on the next cluster.
	for _, part := range strings.Split(url, "/") {
		host := strings.Split(part, ":")[0]
		if looksLikeIPv4(host) {
			t.Errorf("HTTP URL = %q contains a literal IP address", url)
		}
	}
	if invoke.Properties["HTTP Method"] != "POST" {
		t.Errorf("HTTP Method = %q, want POST", invoke.Properties["HTTP Method"])
	}
}

// The flow is version-specific: processor bundles are resolved by coordinate,
// so a flow built against another NiFi will not import into the one the demo
// runs.
func TestFlow_MatchesTheDeployedNiFiVersion(t *testing.T) {
	t.Parallel()

	flow := loadFlow(t)
	if len(flow.FlowContents.Processors) == 0 {
		t.Fatal("the flow has no processors")
	}

	for _, p := range flow.FlowContents.Processors {
		if p.Bundle.Version != nifiVersion {
			t.Errorf("processor %q is bundled against NiFi %s, but the deployment pins %s",
				p.Name, p.Bundle.Version, nifiVersion)
		}
		if p.Bundle.Group != "org.apache.nifi" {
			t.Errorf("processor %q has bundle group %q", p.Name, p.Bundle.Group)
		}
	}
}

// The documented shape of the pipeline: list, fetch, split, post, write.
func TestFlow_HasTheDocumentedPipeline(t *testing.T) {
	t.Parallel()

	flow := loadFlow(t)

	want := []string{
		"org.apache.nifi.processors.standard.ListFile",
		"org.apache.nifi.processors.standard.FetchFile",
		"org.apache.nifi.processors.standard.SplitText",
		invokeHTTP,
		"org.apache.nifi.processors.standard.PutFile",
	}
	for _, wanted := range want {
		found := slices.ContainsFunc(flow.FlowContents.Processors, func(p processor) bool {
			return p.Type == wanted
		})
		if !found {
			t.Errorf("the flow has no %s", wanted)
		}
	}

	// Every record must reach a sink, and the normalized response must reach
	// the output — a flow that posts records and drops the results would pass
	// every other assertion here.
	if !routed(t, "Response") {
		t.Error("InvokeHTTP's Response relationship is not connected: the normalized output would be discarded")
	}
}

// NiFi's own queue is the pipeline's buffer, so back-pressure must be finite:
// the design has no other place for a spike to accumulate (FS-20).
func TestFlow_TheRecordQueueHasBoundedBackPressure(t *testing.T) {
	t.Parallel()

	raw := readFile(t, flowPath)

	var full struct {
		FlowContents struct {
			Connections []struct {
				Name                        string `json:"name"`
				BackPressureObjectThreshold int64  `json:"backPressureObjectThreshold"`
			} `json:"connections"`
		} `json:"flowContents"`
	}
	if err := json.Unmarshal(raw, &full); err != nil {
		t.Fatalf("parsing the flow: %v", err)
	}

	for _, c := range full.FlowContents.Connections {
		if c.BackPressureObjectThreshold <= 0 {
			t.Errorf("connection %q has no back-pressure object threshold; NiFi's queue is this "+
				"pipeline's only buffer and an unbounded one has no overflow behaviour (FS-20)", c.Name)
		}
	}
}

// No credentials anywhere in the flow. The controller has none, the workload
// has none, and a flow definition is exactly the kind of file in which one
// would otherwise be committed by accident (CR-5).
func TestFlow_ContainsNoCredentials(t *testing.T) {
	t.Parallel()

	contents := strings.ToLower(string(readFile(t, flowPath)))

	for _, forbidden := range []string{
		"\"password\"",
		"\"passphrase\"",
		"private key",
		"api key",
		"secret access key",
		"bearer ",
	} {
		if strings.Contains(contents, forbidden) {
			t.Errorf("the flow definition mentions %q; this pipeline must carry no credentials", forbidden)
		}
	}
}

// --- helpers ---------------------------------------------------------------

func loadFlow(t *testing.T) flowDefinition {
	t.Helper()

	var flow flowDefinition
	if err := json.Unmarshal(readFile(t, flowPath), &flow); err != nil {
		t.Fatalf("parsing %s: %v", flowPath, err)
	}
	return flow
}

func invokeHTTPProcessor(t *testing.T) processor {
	t.Helper()

	flow := loadFlow(t)
	for _, p := range flow.FlowContents.Processors {
		if p.Type == invokeHTTP {
			return p
		}
	}
	t.Fatalf("the flow has no %s processor", invokeHTTP)
	return processor{}
}

// routed reports whether a relationship is either connected to a destination or
// auto-terminated deliberately — as opposed to left dangling, which NiFi will
// refuse to start anyway but which is worth catching before demo time.
func routed(t *testing.T, relationship string) bool {
	t.Helper()

	flow := loadFlow(t)
	for _, c := range flow.FlowContents.Connections {
		if slices.Contains(c.SelectedRelationships, relationship) {
			return true
		}
	}
	for _, p := range flow.FlowContents.Processors {
		if p.Type == invokeHTTP && slices.Contains(p.AutoTerminatedRelationships, relationship) {
			return true
		}
	}
	return false
}

// configuredMaxReplicas reads the controller's own configuration, so the
// assertion tracks the setting it protects instead of a copy of it.
//
// Two files carry the value — the shipped default and the demo's ConfigMap —
// and demo-up.sh reads the second. Requiring them to agree means the flow
// assertion cannot be satisfied against one file while the cluster runs the
// other.
func configuredMaxReplicas(t *testing.T) int {
	t.Helper()

	shipped := maxReplicasIn(t, controllerPath, readFile(t, controllerPath))
	demo := maxReplicasIn(t, demoConfigPath, embeddedControllerConfig(t))

	if shipped != demo {
		t.Fatalf("maxReplicas is %d in %s but %d in %s; demo-up.sh reads the second, "+
			"so the flow assertion would be checked against a value the cluster does not run",
			shipped, controllerPath, demo, demoConfigPath)
	}
	return demo
}

func maxReplicasIn(t *testing.T, path string, contents []byte) int {
	t.Helper()

	var cfg struct {
		Target struct {
			MaxReplicas int `yaml:"maxReplicas"`
		} `yaml:"target"`
	}
	if err := yaml.Unmarshal(contents, &cfg); err != nil {
		t.Fatalf("parsing %s: %v", path, err)
	}
	if cfg.Target.MaxReplicas < 1 {
		t.Fatalf("%s has no target.maxReplicas", path)
	}
	return cfg.Target.MaxReplicas
}

// embeddedControllerConfig pulls the controller's YAML out of the demo
// ConfigMap's data key, where it lives as a string.
func embeddedControllerConfig(t *testing.T) []byte {
	t.Helper()

	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal(readFile(t, demoConfigPath), &cm); err != nil {
		t.Fatalf("parsing %s: %v", demoConfigPath, err)
	}
	embedded, ok := cm.Data["kubescalesense.yaml"]
	if !ok {
		t.Fatalf("%s has no kubescalesense.yaml key", demoConfigPath)
	}
	return []byte(embedded)
}

// normalizerSetting reads one value from the Normalizer's ConfigMap, so the
// timeout comparison uses the value the pods actually run with.
func normalizerSetting(t *testing.T, key string) string {
	t.Helper()

	var cm struct {
		Data map[string]string `yaml:"data"`
	}
	if err := yaml.Unmarshal(readFile(t, normalizerPath), &cm); err != nil {
		t.Fatalf("parsing %s: %v", normalizerPath, err)
	}
	value, ok := cm.Data[key]
	if !ok {
		t.Fatalf("%s has no %s", normalizerPath, key)
	}
	return value
}

// nifiSeconds parses NiFi's duration form, "60 secs".
func nifiSeconds(t *testing.T, raw string) float64 {
	t.Helper()

	fields := strings.Fields(strings.TrimSpace(raw))
	if len(fields) != 2 {
		t.Fatalf("%q is not a NiFi duration like %q", raw, "60 secs")
	}
	value, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		t.Fatalf("%q has an unparsable value: %v", raw, err)
	}

	switch unit := strings.ToLower(strings.TrimSuffix(fields[1], "s")); unit {
	case "milli", "millisecond", "ms":
		return value / 1000
	case "sec", "second":
		return value
	case "min", "minute":
		return value * 60
	case "hour":
		return value * 3600
	default:
		t.Fatalf("%q has an unrecognised unit %q", raw, fields[1])
		return 0
	}
}

// goSeconds parses Go's duration form, "10s".
func goSeconds(t *testing.T, raw string) float64 {
	t.Helper()

	parsed, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		t.Fatalf("%q is not a Go duration: %v", raw, err)
	}
	return parsed.Seconds()
}

func looksLikeIPv4(host string) bool {
	octets := strings.Split(host, ".")
	if len(octets) != 4 {
		return false
	}
	for _, octet := range octets {
		if _, err := strconv.Atoi(octet); err != nil {
			return false
		}
	}
	return true
}

func readFile(t *testing.T, path string) []byte {
	t.Helper()

	contents, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return contents
}
