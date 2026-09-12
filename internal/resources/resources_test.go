package resources

import (
	"strings"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

// now is fixed: every freshness check in this package takes the time as a
// parameter, so no test needs to sleep or read the clock.
var now = time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)

// --- UT-14: candidate filter basics ---------------------------------------

func TestEvaluateCandidate_ExclusionBasics(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		node   *corev1.Node
		pods   []*corev1.Pod
		want   string // "" means the node is a candidate
		fatal  string
		expect int64 // expected free slots, when a candidate
	}{
		{
			name:   "healthy worker",
			node:   newNode("w-1", 4000, 8192, 110),
			expect: 110,
		},
		{
			name: "not ready",
			node: newNode("w-1", 4000, 8192, 110, ready(corev1.ConditionFalse, now)),
			want: ExclusionNotReady,
		},
		{
			name: "ready status unknown",
			node: newNode("w-1", 4000, 8192, 110, ready(corev1.ConditionUnknown, now)),
			want: ExclusionNotReady,
		},
		{
			// A node that has never reported has no Ready condition at all.
			// Counting its capacity would mean trusting a node we have never
			// heard from.
			name: "no ready condition at all",
			node: newNode("w-1", 4000, 8192, 110, withoutConditions()),
			want: ExclusionNotReady,
		},
		{
			// Ready=True and nothing has been written to status for half an
			// hour: either the kubelet stopped reporting or our watch desynced,
			// and both mean the free capacity is not ours to count.
			name: "stale heartbeat behind a Ready=True condition",
			node: newNode("w-1", 4000, 8192, 110, ready(corev1.ConditionTrue, now.Add(-30*time.Minute))),
			want: ExclusionStaleHeartbeat,
		},
		{
			// The regression this window exists for. A healthy node's Ready
			// heartbeat is routinely a minute or more old, because the kubelet
			// writes status only on change or every report frequency (5 min);
			// liveness lives in the node Lease. Measured against a live k3s
			// node, a 40 s window excluded the only node in the cluster and
			// drove fit capacity to zero.
			name:   "a healthy node whose status heartbeat is a minute old",
			node:   newNode("w-1", 4000, 8192, 110, ready(corev1.ConditionTrue, now.Add(-time.Minute))),
			expect: 110,
		},
		{
			name:   "heartbeat exactly at the grace period is still fresh",
			node:   newNode("w-1", 4000, 8192, 110, ready(corev1.ConditionTrue, now.Add(-nodeHeartbeatGracePeriod))),
			expect: 110,
		},
		{
			name: "heartbeat one nanosecond past the grace period",
			node: newNode("w-1", 4000, 8192, 110, ready(corev1.ConditionTrue, now.Add(-nodeHeartbeatGracePeriod-1))),
			want: ExclusionStaleHeartbeat,
		},
		{
			name: "cordoned",
			node: newNode("w-1", 4000, 8192, 110, cordoned()),
			want: ExclusionCordoned,
		},
		{
			name: "pod slots exhausted while CPU is free",
			node: newNode("w-1", 4000, 8192, 2),
			pods: []*corev1.Pod{
				newPod("a", "w-1", 10, 10),
				newPod("b", "w-1", 10, 10),
			},
			want: ExclusionPodSlotsFull,
		},
		{
			// Terminal pods have released their slot, so they must not count
			// against it.
			name: "terminal pods do not hold slots",
			node: newNode("w-1", 4000, 8192, 2),
			pods: []*corev1.Pod{
				newPod("a", "w-1", 10, 10, phase(corev1.PodSucceeded)),
				newPod("b", "w-1", 10, 10, phase(corev1.PodFailed)),
			},
			expect: 2,
		},
	}

	tmpl := targetTemplate()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := evaluateCandidate(tt.node, tt.pods, tmpl, testOptions(), now)

			if tt.want == "" {
				if got.excluded {
					t.Fatalf("node should be a candidate, excluded as %q", got.reason)
				}
				if got.freeSlots != tt.expect {
					t.Errorf("freeSlots = %d, want %d", got.freeSlots, tt.expect)
				}
				return
			}
			if !got.excluded {
				t.Fatalf("node should be excluded as %q, got a candidate", tt.want)
			}
			if got.reason != tt.want {
				t.Errorf("exclusion_reason = %q, want %q", got.reason, tt.want)
			}
		})
	}
}

// The check order is part of the contract: the reported reason must be the
// first thing an operator should fix, not an arbitrary one of several.
func TestEvaluateCandidate_ReportsTheFirstFailingCheck(t *testing.T) {
	t.Parallel()

	// Every check fails at once.
	node := newNode("w-1", 4000, 8192, 0,
		ready(corev1.ConditionFalse, now.Add(-time.Hour)),
		cordoned(),
		tainted(corev1.Taint{Key: "dedicated", Value: "other", Effect: corev1.TaintEffectNoSchedule}),
	)

	got := evaluateCandidate(node, nil, targetTemplate(), testOptions(), now)
	if got.reason != ExclusionNotReady {
		t.Errorf("exclusion_reason = %q, want %q: readiness is checked first", got.reason, ExclusionNotReady)
	}
}

// Each exclusion reason is a metric label value, so every one of them must be
// reachable — an unreachable label is a permanently empty dashboard panel.
func TestExclusionReasons_AreAllReachable(t *testing.T) {
	t.Parallel()

	tmpl := targetTemplate()
	restricted := testOptions()
	restricted.NodeLabelSelector = mustSelector(t, "node-pool=workers")

	cases := map[string]func() candidacy{
		ExclusionNotReady: func() candidacy {
			return evaluateCandidate(newNode("n", 4000, 8192, 10, ready(corev1.ConditionFalse, now)), nil, tmpl, testOptions(), now)
		},
		ExclusionStaleHeartbeat: func() candidacy {
			return evaluateCandidate(newNode("n", 4000, 8192, 10, ready(corev1.ConditionTrue, now.Add(-time.Hour))), nil, tmpl, testOptions(), now)
		},
		ExclusionCordoned: func() candidacy {
			return evaluateCandidate(newNode("n", 4000, 8192, 10, cordoned()), nil, tmpl, testOptions(), now)
		},
		ExclusionTaint: func() candidacy {
			return evaluateCandidate(newNode("n", 4000, 8192, 10, tainted(controlPlaneTaint())), nil, tmpl, testOptions(), now)
		},
		ExclusionNodeSelector: func() candidacy {
			return evaluateCandidate(newNode("n", 4000, 8192, 10), nil, templateWithSelector(map[string]string{"disk": "ssd"}), testOptions(), now)
		},
		ExclusionNodeAffinity: func() candidacy {
			return evaluateCandidate(newNode("n", 4000, 8192, 10), nil, templateWithAffinity(requireIn("zone", "eu-north-1a")), testOptions(), now)
		},
		ExclusionOperatorRestricted: func() candidacy {
			return evaluateCandidate(newNode("n", 4000, 8192, 10), nil, tmpl, restricted, now)
		},
		ExclusionPodSlotsFull: func() candidacy {
			return evaluateCandidate(newNode("n", 4000, 8192, 0), nil, tmpl, testOptions(), now)
		},
	}

	for _, reason := range ExclusionReasons {
		build, ok := cases[reason]
		if !ok {
			t.Fatalf("no case produces exclusion_reason %q; a label value nothing can emit is a permanently empty dashboard panel", reason)
		}
		if got := build(); got.reason != reason {
			t.Errorf("case for %q produced %q instead", reason, got.reason)
		}
	}
}

// --- UT-15: toleration semantics -----------------------------------------

func TestToleratesAllTaints(t *testing.T) {
	t.Parallel()

	noSchedule := corev1.Taint{Key: "dedicated", Value: "pipeline", Effect: corev1.TaintEffectNoSchedule}
	noExecute := corev1.Taint{Key: "dedicated", Value: "pipeline", Effect: corev1.TaintEffectNoExecute}
	preferNoSchedule := corev1.Taint{Key: "dedicated", Value: "pipeline", Effect: corev1.TaintEffectPreferNoSchedule}

	tests := []struct {
		name        string
		taints      []corev1.Taint
		tolerations []corev1.Toleration
		want        bool
	}{
		{name: "no taints", want: true},
		{name: "NoSchedule untolerated", taints: []corev1.Taint{noSchedule}, want: false},
		{name: "NoExecute untolerated", taints: []corev1.Taint{noExecute}, want: false},
		{
			// Affects ranking, not admission. Excluding the node here would
			// produce a HOLD on a cluster the scheduler would have used.
			name:   "PreferNoSchedule never excludes",
			taints: []corev1.Taint{preferNoSchedule},
			want:   true,
		},
		{
			name:        "Equal with matching value",
			taints:      []corev1.Taint{noSchedule},
			tolerations: []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "pipeline", Effect: corev1.TaintEffectNoSchedule}},
			want:        true,
		},
		{
			name:        "Equal with the wrong value",
			taints:      []corev1.Taint{noSchedule},
			tolerations: []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpEqual, Value: "other", Effect: corev1.TaintEffectNoSchedule}},
			want:        false,
		},
		{
			// An absent operator defaults to Equal per the Toleration API, so a
			// toleration with no value tolerates only an empty-valued taint.
			name:        "empty operator defaults to Equal",
			taints:      []corev1.Taint{noSchedule},
			tolerations: []corev1.Toleration{{Key: "dedicated", Effect: corev1.TaintEffectNoSchedule}},
			want:        false,
		},
		{
			name:        "Exists ignores the value",
			taints:      []corev1.Taint{noSchedule},
			tolerations: []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
			want:        true,
		},
		{
			name:        "empty key with Exists tolerates everything",
			taints:      []corev1.Taint{noSchedule, noExecute, controlPlaneTaint()},
			tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpExists}},
			want:        true,
		},
		{
			// An empty key without Exists is not the universal toleration; only
			// the Exists form is.
			name:        "empty key with Equal tolerates nothing",
			taints:      []corev1.Taint{noSchedule},
			tolerations: []corev1.Toleration{{Operator: corev1.TolerationOpEqual, Value: "pipeline"}},
			want:        false,
		},
		{
			name:        "empty effect matches every effect",
			taints:      []corev1.Taint{noSchedule, noExecute},
			tolerations: []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists}},
			want:        true,
		},
		{
			name:        "effect mismatch does not tolerate",
			taints:      []corev1.Taint{noSchedule},
			tolerations: []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists, Effect: corev1.TaintEffectNoExecute}},
			want:        false,
		},
		{
			name:        "key mismatch does not tolerate",
			taints:      []corev1.Taint{noSchedule},
			tolerations: []corev1.Toleration{{Key: "other", Operator: corev1.TolerationOpExists}},
			want:        false,
		},
		{
			name:   "all taints must be tolerated, not just one",
			taints: []corev1.Taint{noSchedule, {Key: "special", Effect: corev1.TaintEffectNoSchedule}},
			tolerations: []corev1.Toleration{
				{Key: "dedicated", Operator: corev1.TolerationOpExists},
			},
			want: false,
		},
		{
			// tolerationSeconds governs eviction of running pods, not admission
			// of new ones, so it must not affect the verdict.
			name:        "tolerationSeconds is not consulted",
			taints:      []corev1.Taint{noExecute},
			tolerations: []corev1.Toleration{{Key: "dedicated", Operator: corev1.TolerationOpExists, TolerationSeconds: ptr(int64(0))}},
			want:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := toleratesAllTaints(tt.taints, tt.tolerations); got != tt.want {
				t.Errorf("toleratesAllTaints = %v, want %v", got, tt.want)
			}
		})
	}
}

// The case that motivates the whole check: a kind or k3s control-plane node has
// cores and gigabytes free and can host none of the target's pods. A naive
// cluster-wide sum counts every one of them.
func TestCalculate_ControlPlaneTaintContributesNothing(t *testing.T) {
	t.Parallel()

	cp := newNode("cp-1", 4000, 8192, 110, tainted(controlPlaneTaint()))

	got := Calculate(targetTemplate(), Request{CPUMilli: 500, MemoryBytes: 512 * bytesPerMiB},
		[]*corev1.Node{cp}, nil, testOptions(), now)

	if got.FitCapacity != 0 {
		t.Errorf("FitCapacity = %d, want 0: the only node is untolerated", got.FitCapacity)
	}
	if got.CandidateNodes != 0 {
		t.Errorf("CandidateNodes = %d, want 0", got.CandidateNodes)
	}
	if got.Exclusions[ExclusionTaint] != 1 {
		t.Errorf("Exclusions[%s] = %d, want 1", ExclusionTaint, got.Exclusions[ExclusionTaint])
	}
	// 4000m of free CPU is reported for transparency but contributes no fit.
	if got.FreeCPUMilli != 0 {
		t.Errorf("FreeCPUMilli = %d, want 0: excluded nodes contribute no free capacity", got.FreeCPUMilli)
	}
	if got.Blocking != DimensionNone {
		t.Errorf("Blocking = %q, want %q with no candidate nodes", got.Blocking, DimensionNone)
	}
}

// --- UT-16: selector and affinity ----------------------------------------

func TestMatchesNodeSelector(t *testing.T) {
	t.Parallel()

	nodeLabels := map[string]string{"disk": "ssd", "zone": "eu-north-1a"}

	tests := []struct {
		name     string
		selector map[string]string
		want     bool
	}{
		{name: "empty selector matches", want: true},
		{name: "single match", selector: map[string]string{"disk": "ssd"}, want: true},
		{name: "all terms must match", selector: map[string]string{"disk": "ssd", "zone": "eu-north-1b"}, want: false},
		{name: "missing label", selector: map[string]string{"gpu": "true"}, want: false},
		{name: "wrong value", selector: map[string]string{"disk": "hdd"}, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := matchesNodeSelector(nodeLabels, tt.selector); got != tt.want {
				t.Errorf("matchesNodeSelector = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMatchesRequiredNodeAffinity(t *testing.T) {
	t.Parallel()

	node := newNode("w-1", 4000, 8192, 110, labeled("zone", "eu-north-1a", "cores", "8"))

	tests := []struct {
		name     string
		affinity *corev1.Affinity
		want     bool
	}{
		{name: "no affinity", want: true},
		{name: "In match", affinity: requireIn("zone", "eu-north-1a", "eu-north-1b"), want: true},
		{name: "In miss", affinity: requireIn("zone", "eu-west-1a"), want: false},
		{name: "NotIn on a different value", affinity: requireOp("zone", corev1.NodeSelectorOpNotIn, "eu-west-1a"), want: true},
		{name: "NotIn on our value", affinity: requireOp("zone", corev1.NodeSelectorOpNotIn, "eu-north-1a"), want: false},
		{name: "NotIn on an absent label matches", affinity: requireOp("gpu", corev1.NodeSelectorOpNotIn, "true"), want: true},
		{name: "Exists", affinity: requireOp("zone", corev1.NodeSelectorOpExists), want: true},
		{name: "Exists on an absent label", affinity: requireOp("gpu", corev1.NodeSelectorOpExists), want: false},
		{name: "DoesNotExist", affinity: requireOp("gpu", corev1.NodeSelectorOpDoesNotExist), want: true},
		{name: "DoesNotExist on a present label", affinity: requireOp("zone", corev1.NodeSelectorOpDoesNotExist), want: false},
		{name: "Gt", affinity: requireOp("cores", corev1.NodeSelectorOpGt, "4"), want: true},
		{name: "Gt not satisfied", affinity: requireOp("cores", corev1.NodeSelectorOpGt, "16"), want: false},
		{name: "Lt", affinity: requireOp("cores", corev1.NodeSelectorOpLt, "16"), want: true},
		{name: "Gt on a non-numeric label", affinity: requireOp("zone", corev1.NodeSelectorOpGt, "4"), want: false},
		{name: "Gt with a non-numeric bound", affinity: requireOp("cores", corev1.NodeSelectorOpGt, "many"), want: false},
		{
			// Terms are OR-ed: the second one saves it.
			name: "OR of terms",
			affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "zone", Operator: corev1.NodeSelectorOpIn, Values: []string{"eu-west-1a"}}}},
					{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "cores", Operator: corev1.NodeSelectorOpExists}}},
				}},
			}},
			want: true,
		},
		{
			// Expressions within one term are AND-ed: the second one sinks it.
			name: "AND of expressions",
			affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{NodeSelectorTerms: []corev1.NodeSelectorTerm{
					{MatchExpressions: []corev1.NodeSelectorRequirement{
						{Key: "zone", Operator: corev1.NodeSelectorOpExists},
						{Key: "gpu", Operator: corev1.NodeSelectorOpExists},
					}},
				}},
			}},
			want: false,
		},
		{
			// Preferred affinity ranks nodes; it must not filter them.
			name: "preferred affinity is ignored",
			affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				PreferredDuringSchedulingIgnoredDuringExecution: []corev1.PreferredSchedulingTerm{{
					Weight:     100,
					Preference: corev1.NodeSelectorTerm{MatchExpressions: []corev1.NodeSelectorRequirement{{Key: "gpu", Operator: corev1.NodeSelectorOpExists}}},
				}},
			}},
			want: true,
		},
		{
			name: "empty term list matches",
			affinity: &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
				RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{},
			}},
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := matchesRequiredNodeAffinity(node, tt.affinity); got != tt.want {
				t.Errorf("matchesRequiredNodeAffinity = %v, want %v", got, tt.want)
			}
		})
	}
}

// An unmodelled operator must not silently shrink the candidate set: the error
// is kept in the over-estimate direction, where the Pending watchdog is the
// designated backstop.
func TestMatchesExpression_UnknownOperatorDoesNotExclude(t *testing.T) {
	t.Parallel()

	expr := &corev1.NodeSelectorRequirement{Key: "zone", Operator: corev1.NodeSelectorOperator("Sideways")}
	if !matchesExpression(map[string]string{"zone": "a"}, expr) {
		t.Error("an unmodelled operator must not exclude the node")
	}
}

// respectNodeSelector/respectNodeAffinity/respectTaints exist so an operator
// can disable a predicate the controller models imperfectly.
func TestEvaluateCandidate_RespectSwitchesDisableTheChecks(t *testing.T) {
	t.Parallel()

	node := newNode("w-1", 4000, 8192, 110, tainted(controlPlaneTaint()))
	tmpl := templateWithSelector(map[string]string{"disk": "ssd"})
	tmpl.Affinity = requireIn("zone", "eu-west-1a")

	opts := testOptions()
	opts.RespectTaints = false
	opts.RespectNodeSelector = false
	opts.RespectNodeAffinity = false

	if got := evaluateCandidate(node, nil, tmpl, opts, now); got.excluded {
		t.Errorf("all three predicates are disabled, yet the node was excluded as %q", got.reason)
	}
}

// --- UT-17: effective pod request ----------------------------------------

func TestEffectivePodRequest(t *testing.T) {
	t.Parallel()

	always := corev1.ContainerRestartPolicyAlways

	tests := []struct {
		name    string
		spec    *corev1.PodSpec
		wantCPU int64
		wantMem int64 // MiB
		wantErr string
	}{
		{
			name:    "single container",
			spec:    &corev1.PodSpec{Containers: []corev1.Container{container("app", 500, 512)}},
			wantCPU: 500,
			wantMem: 512,
		},
		{
			name: "multiple containers are summed",
			spec: &corev1.PodSpec{Containers: []corev1.Container{
				container("app", 500, 512),
				container("sidecar", 100, 128),
			}},
			wantCPU: 600,
			wantMem: 640,
		},
		{
			// A native sidecar runs for the pod's whole life, so it adds to the
			// concurrent sum rather than to the init peak.
			name: "restartable init container is a sidecar and adds to the sum",
			spec: &corev1.PodSpec{
				InitContainers: []corev1.Container{withRestartPolicy(container("proxy", 200, 256), &always)},
				Containers:     []corev1.Container{container("app", 500, 512)},
			},
			wantCPU: 700,
			wantMem: 768,
		},
		{
			// Ordinary init containers run sequentially, so the peak is the most
			// demanding one, not their sum.
			name: "non-restartable init containers are a max, not a sum",
			spec: &corev1.PodSpec{
				InitContainers: []corev1.Container{
					container("migrate", 2000, 2048),
					container("warm", 1000, 1024),
				},
				Containers: []corev1.Container{container("app", 500, 512)},
			},
			wantCPU: 2000,
			wantMem: 2048,
		},
		{
			// The init peak includes sidecars declared before it, because those
			// are already running while it executes.
			name: "init peak includes sidecars started before it",
			spec: &corev1.PodSpec{
				InitContainers: []corev1.Container{
					withRestartPolicy(container("proxy", 200, 256), &always),
					container("migrate", 1000, 1024),
				},
				Containers: []corev1.Container{container("app", 500, 512)},
			},
			wantCPU: 1200, // max(500+200, 1000+200)
			wantMem: 1280,
		},
		{
			name: "the steady state wins when it exceeds the init peak",
			spec: &corev1.PodSpec{
				InitContainers: []corev1.Container{container("migrate", 100, 128)},
				Containers:     []corev1.Container{container("app", 500, 512)},
			},
			wantCPU: 500,
			wantMem: 512,
		},
		{
			// The scheduler charges pod overhead when a RuntimeClass declares
			// it, so this calculation must too.
			name: "overhead is added",
			spec: &corev1.PodSpec{
				Containers: []corev1.Container{container("app", 500, 512)},
				Overhead:   resourceList(50, 64),
			},
			wantCPU: 550,
			wantMem: 576,
		},
		{
			name:    "missing CPU request",
			spec:    &corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceMemory: *resource.NewQuantity(512*bytesPerMiB, resource.BinarySI)}}}}},
			wantErr: "no cpu request",
		},
		{
			name:    "missing memory request",
			spec:    &corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{corev1.ResourceCPU: *resource.NewMilliQuantity(500, resource.DecimalSI)}}}}},
			wantErr: "no memory request",
		},
		{
			name:    "no requests at all",
			spec:    &corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}},
			wantErr: "no cpu or memory request",
		},
		{
			name:    "nil spec",
			spec:    nil,
			wantErr: "pod template is empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := EffectivePodRequest(tt.spec)

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected an error containing %q, got request %v", tt.wantErr, got)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error = %q, want it to contain %q", err, tt.wantErr)
				}
				// A zero request is the dangerous answer: it makes a pod
				// schedulable anywhere and fit capacity meaningless.
				if got != (Request{}) {
					t.Errorf("a rejected template must yield no request, got %v", got)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got.CPUMilli != tt.wantCPU {
				t.Errorf("CPUMilli = %d, want %d", got.CPUMilli, tt.wantCPU)
			}
			if want := tt.wantMem * bytesPerMiB; got.MemoryBytes != want {
				t.Errorf("MemoryBytes = %d (%d MiB), want %d (%d MiB)", got.MemoryBytes, got.MemoryMiB(), want, tt.wantMem)
			}
		})
	}
}

// Neighbouring pods are allowed to declare nothing: BestEffort pods are legal.
// They then consume no scheduling budget, which is a documented over-estimate
// of free capacity rather than an error to reject.
func TestPodRequest_AcceptsBestEffortNeighbours(t *testing.T) {
	t.Parallel()

	if got := podRequest(&corev1.PodSpec{Containers: []corev1.Container{{Name: "app"}}}); got != (Request{}) {
		t.Errorf("podRequest = %v, want a zero request", got)
	}
}

func TestRequest_String(t *testing.T) {
	t.Parallel()

	got := Request{CPUMilli: 500, MemoryBytes: 512 * bytesPerMiB}.String()
	if got != "500m/512Mi" {
		t.Errorf("String() = %q, want %q (the form used in the documented event messages)", got, "500m/512Mi")
	}
}

// --- UT-18: per-node free resources --------------------------------------

func TestNodeFree(t *testing.T) {
	t.Parallel()

	opts := testOptions() // reserve 200m / 256 MiB

	tests := []struct {
		name     string
		node     *corev1.Node
		pods     []*corev1.Pod
		wantCPU  int64
		wantMiB  int64
		wantReqC int64
	}{
		{
			name:    "allocatable minus requested minus reserve",
			node:    newNode("w-1", 4000, 8192, 110),
			pods:    []*corev1.Pod{newPod("a", "w-1", 3200, 6144)},
			wantCPU: 600,
			wantMiB: 1792,
		},
		{
			// Negative free must floor at zero rather than turning into credit
			// on another dimension or a negative fit.
			name:    "floored at zero when the reserve overshoots",
			node:    newNode("w-2", 2000, 4096, 110),
			pods:    []*corev1.Pod{newPod("a", "w-2", 1900, 3900)},
			wantCPU: 0,
			wantMiB: 0,
		},
		{
			// Node budget is shared cluster-wide, so a neighbour's pod consumes
			// the same CPU as ours. Counting only our namespace is the single
			// most consequential way to overstate capacity.
			name: "pods of other namespaces are counted",
			node: newNode("w-1", 4000, 8192, 110),
			pods: []*corev1.Pod{
				newPod("ours", "w-1", 500, 512, namespace("data-pipeline")),
				newPod("theirs", "w-1", 2000, 4096, namespace("someone-else")),
			},
			wantCPU:  1300, // 4000 − 2500 − 200
			wantMiB:  3328, // 8192 − 4608 − 256
			wantReqC: 2500,
		},
		{
			// The scheduler has already charged this pod to the node.
			name:    "assigned but still Pending is counted",
			node:    newNode("w-1", 4000, 8192, 110),
			pods:    []*corev1.Pod{newPod("a", "w-1", 1000, 1024, phase(corev1.PodPending))},
			wantCPU: 2800,
			wantMiB: 6912,
		},
		{
			// A terminating pod keeps its budget until it is actually gone.
			// Assuming otherwise double-books the node during a rollout.
			name:    "terminating pods are still counted",
			node:    newNode("w-1", 4000, 8192, 110),
			pods:    []*corev1.Pod{newPod("a", "w-1", 1000, 1024, terminating())},
			wantCPU: 2800,
			wantMiB: 6912,
		},
		{
			name: "Succeeded and Failed are excluded",
			node: newNode("w-1", 4000, 8192, 110),
			pods: []*corev1.Pod{
				newPod("done", "w-1", 1000, 1024, phase(corev1.PodSucceeded)),
				newPod("dead", "w-1", 1000, 1024, phase(corev1.PodFailed)),
			},
			wantCPU: 3800,
			wantMiB: 7936,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			free, requested, allocatable := nodeFree(tt.node, tt.pods, opts)

			if free.CPUMilli != tt.wantCPU {
				t.Errorf("free CPU = %dm, want %dm", free.CPUMilli, tt.wantCPU)
			}
			if want := tt.wantMiB * bytesPerMiB; free.MemoryBytes != want {
				t.Errorf("free memory = %d MiB, want %d MiB", free.MemoryBytes/bytesPerMiB, tt.wantMiB)
			}
			if tt.wantReqC != 0 && requested.CPUMilli != tt.wantReqC {
				t.Errorf("requested CPU = %dm, want %dm", requested.CPUMilli, tt.wantReqC)
			}
			// Allocatable, not capacity: the difference is kube-reserved plus
			// the eviction thresholds, and it is exactly the margin whose
			// absence causes kubelet evictions.
			if allocatable.CPUMilli != cpuMilli(tt.node.Status.Allocatable) {
				t.Errorf("allocatable CPU = %dm, want the node's allocatable", allocatable.CPUMilli)
			}
		})
	}
}

// A negative reserve would manufacture capacity a node does not have —
// allocatable minus a negative reserve exceeds allocatable — so the guarantee
// is that the state is unreachable: config validation rejects it at startup.
func TestNegativeReserveIsRejectedAtStartup(t *testing.T) {
	t.Parallel()

	cfg := config.Default()
	cfg.Target.Namespace, cfg.Target.Deployment = "ns", "d"
	cfg.Workload.Signal.Source = config.SignalSourceNone // no file to read in a unit test
	cfg.Resources.PerNodeReserveCPUMilli = -1000

	err := cfg.Validate()
	if err == nil {
		t.Fatal("a negative perNodeReserveCPUMilli must be rejected at startup")
	}
	if !strings.Contains(err.Error(), "perNodeReserveCPUMilli") {
		t.Errorf("error must name the offending key, got: %v", err)
	}
}

// Pods with no node assignment belong to no node's budget. They will compete
// with ours, which is what the reserve and the margin exist to absorb.
func TestByNode_SkipsUnassignedPods(t *testing.T) {
	t.Parallel()

	index := byNode([]*corev1.Pod{
		newPod("assigned", "w-1", 100, 100),
		newPod("pending", "", 100, 100),
	})

	if len(index) != 1 || len(index["w-1"]) != 1 {
		t.Errorf("byNode = %v, want only the assigned pod indexed", index)
	}
}

// --- UT-19: fit capacity --------------------------------------------------

// The reference cluster from resource-calculation § 7, reproduced number for
// number. If this test and that table ever disagree, one of them is wrong and
// the disagreement is the bug.
func TestCalculate_ReproducesTheReferenceExample(t *testing.T) {
	t.Parallel()

	nodes := []*corev1.Node{
		newNode("cp-1", 4000, 8192, 110, tainted(controlPlaneTaint())),
		newNode("w-1", 4000, 8192, 100),
		newNode("w-2", 4000, 8192, 103),
		newNode("w-3", 2000, 4096, 89),
	}

	pods := []*corev1.Pod{
		// w-1: 3200m / 6144 MiB requested across 4 pods, leaving 96 slots.
		newPod("w1-a", "w-1", 800, 1536),
		newPod("w1-b", "w-1", 800, 1536),
		newPod("w1-c", "w-1", 800, 1536),
		newPod("w1-d", "w-1", 800, 1536),
		// w-2: 1800m / 2048 MiB across 2 pods, leaving 101 slots.
		newPod("w2-a", "w-2", 1000, 1024),
		newPod("w2-b", "w-2", 800, 1024),
		// w-3: 1900m / 3900 MiB in 1 pod, leaving 88 slots.
		newPod("w3-a", "w-3", 1900, 3900),
	}

	got := Calculate(targetTemplate(), Request{CPUMilli: 500, MemoryBytes: 512 * bytesPerMiB},
		nodes, pods, testOptions(), now)

	if got.CandidateNodes != 3 {
		t.Errorf("CandidateNodes = %d, want 3", got.CandidateNodes)
	}
	if got.Exclusions[ExclusionTaint] != 1 {
		t.Errorf("Exclusions[taint] = %d, want 1", got.Exclusions[ExclusionTaint])
	}
	if got.RawFitSum != 5 {
		t.Errorf("RawFitSum = %d, want 5 (1 + 4 + 0)", got.RawFitSum)
	}
	if got.FitCapacity != 4 {
		t.Errorf("FitCapacity = %d, want 4 (5 − fitCapacityMarginPods 1)", got.FitCapacity)
	}
	if got.Blocking != DimensionCPU {
		t.Errorf("Blocking = %q, want %q: CPU is the minimum on every candidate", got.Blocking, DimensionCPU)
	}

	// The headline point of the example: 2600m of free CPU across the
	// candidates is nominally five 500m pods, and only four of them fit.
	if got.FreeCPUMilli != 2600 {
		t.Errorf("FreeCPUMilli = %dm, want 2600m", got.FreeCPUMilli)
	}

	want := map[string]struct {
		free    int64
		freeMiB int64
		slots   int64
		fit     int32
	}{
		"w-1": {free: 600, freeMiB: 1792, slots: 96, fit: 1},
		"w-2": {free: 2000, freeMiB: 5888, slots: 101, fit: 4},
		"w-3": {free: 0, freeMiB: 0, slots: 88, fit: 0},
	}
	if len(got.Nodes) != len(want) {
		t.Fatalf("per-node breakdown has %d entries, want %d", len(got.Nodes), len(want))
	}
	for _, nf := range got.Nodes {
		w, ok := want[nf.Name]
		if !ok {
			t.Errorf("unexpected node %q in the breakdown", nf.Name)
			continue
		}
		if nf.FreeCPUMilli != w.free {
			t.Errorf("%s free CPU = %dm, want %dm", nf.Name, nf.FreeCPUMilli, w.free)
		}
		if nf.FreeMemoryBytes/bytesPerMiB != w.freeMiB {
			t.Errorf("%s free memory = %d MiB, want %d MiB", nf.Name, nf.FreeMemoryBytes/bytesPerMiB, w.freeMiB)
		}
		if nf.FreeSlots != w.slots {
			t.Errorf("%s free slots = %d, want %d", nf.Name, nf.FreeSlots, w.slots)
		}
		if nf.Fit != w.fit {
			t.Errorf("%s fit = %d, want %d", nf.Name, nf.Fit, w.fit)
		}
	}
}

// Fragmentation is the entire reason this package exists: 1500m of free CPU
// spread over five nodes places zero 500m pods when no single node has 500m.
func TestCalculate_FragmentationYieldsZeroFit(t *testing.T) {
	t.Parallel()

	var nodes []*corev1.Node
	var pods []*corev1.Pod
	for _, name := range []string{"n1", "n2", "n3", "n4", "n5"} {
		nodes = append(nodes, newNode(name, 4000, 16384, 110))
		// Leave 300m free after the 200m reserve.
		pods = append(pods, newPod(name+"-filler", name, 3500, 1024))
	}

	opts := testOptions()
	opts.FitCapacityMarginPods = 0 // isolate fragmentation from the margin

	got := Calculate(targetTemplate(), Request{CPUMilli: 500, MemoryBytes: 512 * bytesPerMiB},
		nodes, pods, opts, now)

	if got.FreeCPUMilli != 1500 {
		t.Fatalf("fixture is wrong: FreeCPUMilli = %dm, want 1500m", got.FreeCPUMilli)
	}
	if got.FitCapacity != 0 {
		t.Errorf("FitCapacity = %d, want 0: 1500m of free CPU in 300m pieces places no 500m pod", got.FitCapacity)
	}
	if got.Blocking != DimensionCPU {
		t.Errorf("Blocking = %q, want %q", got.Blocking, DimensionCPU)
	}
}

func TestCalculate_MarginIsSubtractedAfterTheSumAndFlooredAtZero(t *testing.T) {
	t.Parallel()

	// Two nodes, one roomy and one tight: a margin subtracted per node would
	// give a different answer from one subtracted from the total.
	nodes := []*corev1.Node{
		newNode("roomy", 4000, 16384, 110),
		newNode("tight", 1000, 16384, 110),
	}
	req := Request{CPUMilli: 500, MemoryBytes: 512 * bytesPerMiB}

	for _, tc := range []struct {
		margin int32
		want   int32
	}{
		{margin: 0, want: 8}, // roomy 3800/500=7, tight 800/500=1
		{margin: 1, want: 7},
		{margin: 8, want: 0},
		{margin: 99, want: 0}, // floored, never negative
	} {
		opts := testOptions()
		opts.FitCapacityMarginPods = tc.margin

		got := Calculate(targetTemplate(), req, nodes, nil, opts, now)
		if got.FitCapacity != tc.want {
			t.Errorf("margin %d: FitCapacity = %d, want %d", tc.margin, got.FitCapacity, tc.want)
		}
		if got.RawFitSum != 8 {
			t.Errorf("margin %d: RawFitSum = %d, want 8 — the margin must not be baked into the sum", tc.margin, got.RawFitSum)
		}
	}
}

// Determinism is a stated non-functional requirement (NFR-07): the same
// snapshot must produce the same output, including the per-node ordering.
func TestCalculate_IsDeterministicRegardlessOfInputOrder(t *testing.T) {
	t.Parallel()

	a := []*corev1.Node{newNode("w-3", 4000, 8192, 110), newNode("w-1", 4000, 8192, 110), newNode("w-2", 2000, 4096, 110)}
	b := []*corev1.Node{newNode("w-1", 4000, 8192, 110), newNode("w-2", 2000, 4096, 110), newNode("w-3", 4000, 8192, 110)}
	req := Request{CPUMilli: 500, MemoryBytes: 512 * bytesPerMiB}

	first := Calculate(targetTemplate(), req, a, nil, testOptions(), now)
	second := Calculate(targetTemplate(), req, b, nil, testOptions(), now)

	if first.FitCapacity != second.FitCapacity {
		t.Errorf("FitCapacity differs by input order: %d vs %d", first.FitCapacity, second.FitCapacity)
	}
	for i := range first.Nodes {
		if first.Nodes[i].Name != second.Nodes[i].Name {
			t.Errorf("per-node output %d: %q vs %q — nodes must be sorted by name", i, first.Nodes[i].Name, second.Nodes[i].Name)
		}
	}
	if first.Nodes[0].Name != "w-1" {
		t.Errorf("first node = %q, want w-1", first.Nodes[0].Name)
	}
}

func TestCalculate_NoNodes(t *testing.T) {
	t.Parallel()

	got := Calculate(targetTemplate(), Request{CPUMilli: 500, MemoryBytes: 512 * bytesPerMiB}, nil, nil, testOptions(), now)

	if got.FitCapacity != 0 || got.CandidateNodes != 0 || got.TotalNodes != 0 {
		t.Errorf("empty cluster: got %+v, want a zero feasibility", got)
	}
	if got.Blocking != DimensionNone {
		t.Errorf("Blocking = %q, want %q", got.Blocking, DimensionNone)
	}
}

// --- UT-20: blocking dimension -------------------------------------------

func TestFitOnNode_BlockingDimension(t *testing.T) {
	t.Parallel()

	req := Request{CPUMilli: 500, MemoryBytes: 512 * bytesPerMiB}

	tests := []struct {
		name        string
		free        Request
		slots       int64
		wantFit     int32
		wantBinding Dimension
	}{
		{
			name:        "cpu binds",
			free:        Request{CPUMilli: 1000, MemoryBytes: 8192 * bytesPerMiB},
			slots:       50,
			wantFit:     2,
			wantBinding: DimensionCPU,
		},
		{
			name:        "memory binds",
			free:        Request{CPUMilli: 8000, MemoryBytes: 1536 * bytesPerMiB},
			slots:       50,
			wantFit:     3,
			wantBinding: DimensionMemory,
		},
		{
			// Surprising but real on small nodes running many tiny pods: cores
			// free, no slots left.
			name:        "pod slots bind while CPU and memory are free",
			free:        Request{CPUMilli: 8000, MemoryBytes: 8192 * bytesPerMiB},
			slots:       2,
			wantFit:     2,
			wantBinding: DimensionPodSlots,
		},
		{
			// Documented precedence cpu -> memory -> podSlots. A tie at zero is
			// common rather than exotic: the reserve drives both dimensions to
			// zero on a full node.
			name:        "cpu wins a tie with memory",
			free:        Request{CPUMilli: 1000, MemoryBytes: 1024 * bytesPerMiB},
			slots:       50,
			wantFit:     2,
			wantBinding: DimensionCPU,
		},
		{
			name:        "memory wins a tie with pod slots",
			free:        Request{CPUMilli: 8000, MemoryBytes: 1024 * bytesPerMiB},
			slots:       2,
			wantFit:     2,
			wantBinding: DimensionMemory,
		},
		{
			name:        "three-way tie reports cpu",
			free:        Request{CPUMilli: 1000, MemoryBytes: 1024 * bytesPerMiB},
			slots:       2,
			wantFit:     2,
			wantBinding: DimensionCPU,
		},
		{
			name:        "nothing fits",
			free:        Request{CPUMilli: 100, MemoryBytes: 8192 * bytesPerMiB},
			slots:       50,
			wantFit:     0,
			wantBinding: DimensionCPU,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fit, binding := fitOnNode(tt.free, tt.slots, req)
			if fit != tt.wantFit {
				t.Errorf("fit = %d, want %d", fit, tt.wantFit)
			}
			if binding != tt.wantBinding {
				t.Errorf("binding = %q, want %q", binding, tt.wantBinding)
			}
		})
	}
}

// The cluster-level dimension is the one binding on the most candidate nodes:
// the answer an operator can act on. Reporting memory when adding memory would
// change nothing is worse than reporting nothing.
func TestAggregateBinding(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		fits []NodeFit
		want Dimension
	}{
		{name: "no candidates", want: DimensionNone},
		{
			name: "majority wins",
			fits: []NodeFit{{Binding: DimensionCPU}, {Binding: DimensionCPU}, {Binding: DimensionMemory}},
			want: DimensionCPU,
		},
		{
			name: "minority does not win",
			fits: []NodeFit{{Binding: DimensionMemory}, {Binding: DimensionMemory}, {Binding: DimensionCPU}},
			want: DimensionMemory,
		},
		{
			name: "an even split resolves by precedence",
			fits: []NodeFit{{Binding: DimensionMemory}, {Binding: DimensionCPU}},
			want: DimensionCPU,
		},
		{
			name: "pod slots can win outright",
			fits: []NodeFit{{Binding: DimensionPodSlots}, {Binding: DimensionPodSlots}, {Binding: DimensionCPU}},
			want: DimensionPodSlots,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := aggregateBinding(tt.fits); got != tt.want {
				t.Errorf("aggregateBinding = %q, want %q", got, tt.want)
			}
		})
	}
}

// --- Options ---------------------------------------------------------------

func TestOptionsFrom(t *testing.T) {
	t.Parallel()

	cfg := config.Default().Resources
	cfg.NodeLabelSelector = "node-pool in (workers,spot)"

	opts, err := OptionsFrom(cfg)
	if err != nil {
		t.Fatalf("OptionsFrom: %v", err)
	}

	// MiB is converted to bytes once, at startup, rather than on every
	// reconcile.
	if want := int64(256) * bytesPerMiB; opts.PerNodeReserveMemoryBytes != want {
		t.Errorf("PerNodeReserveMemoryBytes = %d, want %d", opts.PerNodeReserveMemoryBytes, want)
	}
	if opts.NodeLabelSelector == nil {
		t.Fatal("NodeLabelSelector must be pre-parsed at startup")
	}
	if !opts.NodeLabelSelector.Matches(labels.Set{"node-pool": "spot"}) {
		t.Error("set-based selector should match node-pool=spot")
	}
}

// An empty selector means "no extra restriction", not "match nothing". Getting
// this backwards empties the candidate set on every default install.
func TestParseNodeLabelSelector_EmptyMeansNoRestriction(t *testing.T) {
	t.Parallel()

	selector, err := ParseNodeLabelSelector("")
	if err != nil {
		t.Fatalf("ParseNodeLabelSelector(\"\"): %v", err)
	}
	if selector != nil {
		t.Error("an empty selector must be nil, which evaluateCandidate reads as no restriction")
	}
}

func TestParseNodeLabelSelector_RejectsGarbage(t *testing.T) {
	t.Parallel()

	if _, err := ParseNodeLabelSelector("=workers"); err == nil {
		t.Error("a malformed selector must fail at startup, not silently empty the candidate set")
	}
}

// --- helpers ---------------------------------------------------------------

type nodeOpt func(*corev1.Node)

// newNode builds a Ready node with the given allocatable CPU (millicores),
// memory (MiB), and pod slots.
func newNode(name string, cpuMilli, memoryMiB, podSlots int64, opts ...nodeOpt) *corev1.Node {
	node := &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Status: corev1.NodeStatus{
			Allocatable: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(memoryMiB*bytesPerMiB, resource.BinarySI),
				corev1.ResourcePods:   *resource.NewQuantity(podSlots, resource.DecimalSI),
			},
			Conditions: []corev1.NodeCondition{{
				Type:              corev1.NodeReady,
				Status:            corev1.ConditionTrue,
				LastHeartbeatTime: metav1.NewTime(now),
			}},
		},
	}
	for _, opt := range opts {
		opt(node)
	}
	return node
}

func ready(status corev1.ConditionStatus, heartbeat time.Time) nodeOpt {
	return func(n *corev1.Node) {
		n.Status.Conditions = []corev1.NodeCondition{{
			Type:              corev1.NodeReady,
			Status:            status,
			LastHeartbeatTime: metav1.NewTime(heartbeat),
		}}
	}
}

func withoutConditions() nodeOpt {
	return func(n *corev1.Node) { n.Status.Conditions = nil }
}

func cordoned() nodeOpt {
	return func(n *corev1.Node) { n.Spec.Unschedulable = true }
}

func tainted(taints ...corev1.Taint) nodeOpt {
	return func(n *corev1.Node) { n.Spec.Taints = append(n.Spec.Taints, taints...) }
}

func labeled(kv ...string) nodeOpt {
	return func(n *corev1.Node) {
		if n.Labels == nil {
			n.Labels = map[string]string{}
		}
		for i := 0; i+1 < len(kv); i += 2 {
			n.Labels[kv[i]] = kv[i+1]
		}
	}
}

func controlPlaneTaint() corev1.Taint {
	return corev1.Taint{Key: "node-role.kubernetes.io/control-plane", Effect: corev1.TaintEffectNoSchedule}
}

type podOpt func(*corev1.Pod)

// newPod builds a Running pod assigned to nodeName, requesting the given CPU
// (millicores) and memory (MiB).
func newPod(name, nodeName string, cpuMilli, memoryMiB int64, opts ...podOpt) *corev1.Pod {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "default"},
		Spec: corev1.PodSpec{
			NodeName:   nodeName,
			Containers: []corev1.Container{container("app", cpuMilli, memoryMiB)},
		},
		Status: corev1.PodStatus{Phase: corev1.PodRunning},
	}
	for _, opt := range opts {
		opt(pod)
	}
	return pod
}

func phase(p corev1.PodPhase) podOpt {
	return func(pod *corev1.Pod) { pod.Status.Phase = p }
}

func namespace(ns string) podOpt {
	return func(pod *corev1.Pod) { pod.Namespace = ns }
}

func terminating() podOpt {
	return func(pod *corev1.Pod) {
		ts := metav1.NewTime(now.Add(-time.Second))
		pod.DeletionTimestamp = &ts
	}
}

func container(name string, cpuMilli, memoryMiB int64) corev1.Container {
	return corev1.Container{
		Name:      name,
		Resources: corev1.ResourceRequirements{Requests: resourceList(cpuMilli, memoryMiB)},
	}
}

func withRestartPolicy(c corev1.Container, policy *corev1.ContainerRestartPolicy) corev1.Container {
	c.RestartPolicy = policy
	return c
}

func resourceList(cpuMilli, memoryMiB int64) corev1.ResourceList {
	return corev1.ResourceList{
		corev1.ResourceCPU:    *resource.NewMilliQuantity(cpuMilli, resource.DecimalSI),
		corev1.ResourceMemory: *resource.NewQuantity(memoryMiB*bytesPerMiB, resource.BinarySI),
	}
}

// targetTemplate is the reference target: 500m / 512 MiB, no tolerations, no
// selector, no affinity.
func targetTemplate() *corev1.PodSpec {
	return &corev1.PodSpec{Containers: []corev1.Container{container("normalizer", 500, 512)}}
}

func templateWithSelector(selector map[string]string) *corev1.PodSpec {
	spec := targetTemplate()
	spec.NodeSelector = selector
	return spec
}

func templateWithAffinity(affinity *corev1.Affinity) *corev1.PodSpec {
	spec := targetTemplate()
	spec.Affinity = affinity
	return spec
}

func requireIn(key string, values ...string) *corev1.Affinity {
	return requireOp(key, corev1.NodeSelectorOpIn, values...)
}

func requireOp(key string, op corev1.NodeSelectorOperator, values ...string) *corev1.Affinity {
	return &corev1.Affinity{NodeAffinity: &corev1.NodeAffinity{
		RequiredDuringSchedulingIgnoredDuringExecution: &corev1.NodeSelector{
			NodeSelectorTerms: []corev1.NodeSelectorTerm{{
				MatchExpressions: []corev1.NodeSelectorRequirement{{Key: key, Operator: op, Values: values}},
			}},
		},
	}}
}

func testOptions() Options {
	return Options{
		PerNodeReserveCPUMilli:    200,
		PerNodeReserveMemoryBytes: 256 * bytesPerMiB,
		FitCapacityMarginPods:     1,
		RespectNodeSelector:       true,
		RespectNodeAffinity:       true,
		RespectTaints:             true,
	}
}

func mustSelector(t *testing.T, expr string) labels.Selector {
	t.Helper()

	selector, err := ParseNodeLabelSelector(expr)
	if err != nil {
		t.Fatalf("ParseNodeLabelSelector(%q): %v", expr, err)
	}
	return selector
}

func ptr[T any](v T) *T { return &v }
