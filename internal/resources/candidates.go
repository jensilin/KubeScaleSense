package resources

import (
	"fmt"
	"strconv"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
)

// nodeHeartbeatGracePeriod is the window inside which a node's Ready heartbeat
// must have been refreshed for check C2 to pass.
//
// It is deliberately *not* --node-monitor-grace-period (40 s). Since node
// leases went GA the kubelet's liveness heartbeat is the Lease in
// kube-node-lease, renewed every 10 s, and that is what the node controller
// evaluates the grace period against. The kubelet only writes
// node.status.conditions when a condition changes or every
// --node-status-report-frequency, which defaults to 5 minutes. Comparing
// lastHeartbeatTime against 40 s therefore excludes healthy nodes for most of
// every reporting interval: measured against a live k3s node, the Ready
// heartbeat was routinely ~60 s old while its Lease was 5 s old, which drove
// the candidate set — and so the fit capacity — to zero.
//
// Two report periods, so one missed status write is not an exclusion. This
// check is a guard against a *stale view* (a desynced watch, or a kubelet that
// has stopped reporting status altogether), not against node death: when a node
// dies the node controller writes Ready=Unknown within the lease grace period
// and C1 catches it from the same watch. Reading the Lease directly would be
// more precise, but leases are a P4 concern (leader election) and buying
// precision that C1 already provides is not worth widening this phase's read
// set.
const nodeHeartbeatGracePeriod = 10 * time.Minute

// Exclusion reasons, used verbatim as the kss_excluded_nodes{exclusion_reason}
// label values so that a surprising HOLD is explainable from a dashboard
// (resource-calculation § 2).
const (
	ExclusionNotReady           = "notReady"
	ExclusionStaleHeartbeat     = "staleHeartbeat"
	ExclusionCordoned           = "cordoned"
	ExclusionTaint              = "taint"
	ExclusionNodeSelector       = "nodeSelector"
	ExclusionNodeAffinity       = "nodeAffinity"
	ExclusionOperatorRestricted = "operatorRestricted"
	ExclusionPodSlotsFull       = "podSlotsFull"
)

// ExclusionReasons lists every reason in check order. Exported so that the
// metrics registry can pre-create each label value: a series that only appears
// once a node is first excluded is a series that is missing from the dashboard
// at exactly the moment an operator goes looking for it.
var ExclusionReasons = []string{
	ExclusionNotReady,
	ExclusionStaleHeartbeat,
	ExclusionCordoned,
	ExclusionTaint,
	ExclusionNodeSelector,
	ExclusionNodeAffinity,
	ExclusionOperatorRestricted,
	ExclusionPodSlotsFull,
}

// candidacy records the outcome of the checks for one node.
type candidacy struct {
	node      *corev1.Node
	excluded  bool
	reason    string
	freeSlots int64
}

// evaluateCandidate applies checks C1–C8 in the documented order and stops at
// the first failure, so the reported reason is the first thing an operator
// should fix rather than an arbitrary one.
//
// The checks are exactly the modelled subset (resource-calculation § 6). Notably
// absent, and absent on purpose: inter-pod affinity, topology spread, volume
// topology, and quota. Each of those can only cause an over-estimate, which the
// Pending-pod watchdog is the designated backstop for; adding a half-correct
// implementation of any of them would shrink the candidate set unpredictably and
// produce false HOLDs (I-16).
func evaluateCandidate(node *corev1.Node, podsOnNode []*corev1.Pod, tmpl *corev1.PodSpec, cfg Options, now time.Time) candidacy {
	c := candidacy{node: node}

	ready, heartbeat := readyCondition(node)
	switch {
	case !ready:
		return c.exclude(ExclusionNotReady)
	case !heartbeat.IsZero() && now.Sub(heartbeat) > nodeHeartbeatGracePeriod:
		// A node whose kubelet has stopped reporting status at all still
		// advertises Ready=True, and so does a node we are looking at through a
		// watch that silently desynced.
		return c.exclude(ExclusionStaleHeartbeat)
	case node.Spec.Unschedulable:
		return c.exclude(ExclusionCordoned)
	}

	if cfg.RespectTaints && !toleratesAllTaints(node.Spec.Taints, tmpl.Tolerations) {
		// The single most common way a naive capacity calculation over-estimates:
		// a control-plane node has plenty of free CPU and can host none of it.
		return c.exclude(ExclusionTaint)
	}
	if cfg.RespectNodeSelector && !matchesNodeSelector(node.Labels, tmpl.NodeSelector) {
		return c.exclude(ExclusionNodeSelector)
	}
	if cfg.RespectNodeAffinity && !matchesRequiredNodeAffinity(node, tmpl.Affinity) {
		return c.exclude(ExclusionNodeAffinity)
	}
	if cfg.NodeLabelSelector != nil && !cfg.NodeLabelSelector.Matches(labels.Set(node.Labels)) {
		return c.exclude(ExclusionOperatorRestricted)
	}

	c.freeSlots = freeSlots(node, podsOnNode)
	if c.freeSlots <= 0 {
		// Genuinely surprising on small nodes running many tiny pods: cores free,
		// no slots left.
		return c.exclude(ExclusionPodSlotsFull)
	}
	return c
}

func (c candidacy) exclude(reason string) candidacy {
	c.excluded = true
	c.reason = reason
	return c
}

// readyCondition reports whether Ready is True, and when it was last confirmed.
func readyCondition(node *corev1.Node) (ready bool, lastHeartbeat time.Time) {
	for i := range node.Status.Conditions {
		cond := &node.Status.Conditions[i]
		if cond.Type == corev1.NodeReady {
			return cond.Status == corev1.ConditionTrue, cond.LastHeartbeatTime.Time
		}
	}
	// No Ready condition at all — a node that has never reported. Treated as
	// not ready, because the alternative is counting capacity we have never
	// had confirmed.
	return false, time.Time{}
}

// freeSlots is allocatable.pods minus the non-terminal pods already on the node.
func freeSlots(node *corev1.Node, podsOnNode []*corev1.Pod) int64 {
	allocatable := int64(0)
	if q, ok := node.Status.Allocatable[corev1.ResourcePods]; ok {
		allocatable = q.Value()
	}
	used := int64(0)
	for _, pod := range podsOnNode {
		if chargeable(pod) {
			used++
		}
	}
	if free := allocatable - used; free > 0 {
		return free
	}
	return 0
}

// chargeable reports whether a pod still holds its node's resources.
//
// Succeeded and Failed pods have released them. Terminating pods have *not*:
// a pod with a deletionTimestamp keeps its budget until it is actually gone, and
// assuming otherwise opens a window where the controller double-books a node
// during a rollout (resource-calculation § 4).
func chargeable(pod *corev1.Pod) bool {
	switch pod.Status.Phase {
	case corev1.PodSucceeded, corev1.PodFailed:
		return false
	default:
		return true
	}
}

// toleratesAllTaints implements the toleration semantics of ADR-07.
//
// PreferNoSchedule is ignored: it affects node *ranking*, not admission, so
// honouring it would exclude nodes the scheduler would happily use and cause
// false HOLDs.
func toleratesAllTaints(taints []corev1.Taint, tolerations []corev1.Toleration) bool {
	for i := range taints {
		taint := &taints[i]
		if taint.Effect != corev1.TaintEffectNoSchedule && taint.Effect != corev1.TaintEffectNoExecute {
			continue
		}
		if !isTolerated(taint, tolerations) {
			return false
		}
	}
	return true
}

func isTolerated(taint *corev1.Taint, tolerations []corev1.Toleration) bool {
	for i := range tolerations {
		if toleratesTaint(&tolerations[i], taint) {
			return true
		}
	}
	return false
}

// toleratesTaint mirrors the upstream matching rules:
//
//   - an empty effect matches every effect;
//   - an empty key with operator Exists tolerates everything;
//   - operator Exists matches on key alone; Equal additionally requires an
//     equal value.
//
// tolerationSeconds is deliberately not consulted: it governs eviction of
// already-running pods, not admission of new ones.
func toleratesTaint(t *corev1.Toleration, taint *corev1.Taint) bool {
	if t.Effect != "" && t.Effect != taint.Effect {
		return false
	}
	if t.Key == "" {
		return t.Operator == corev1.TolerationOpExists
	}
	if t.Key != taint.Key {
		return false
	}
	switch t.Operator {
	case corev1.TolerationOpExists:
		return true
	case corev1.TolerationOpEqual, "":
		// An empty operator defaults to Equal, per the Toleration API.
		return t.Value == taint.Value
	default:
		return false
	}
}

// matchesNodeSelector treats nodeSelector as an AND of label equalities.
func matchesNodeSelector(nodeLabels, selector map[string]string) bool {
	for key, want := range selector {
		if got, ok := nodeLabels[key]; !ok || got != want {
			return false
		}
	}
	return true
}

// matchesRequiredNodeAffinity evaluates
// requiredDuringSchedulingIgnoredDuringExecution as an OR of nodeSelectorTerms,
// each an AND of matchExpressions.
//
// preferredDuringScheduling is ignored by design (ADR-08): it ranks nodes rather
// than filtering them.
//
// matchFields is also not evaluated. It selects on object fields rather than
// labels — in practice metadata.name — and is outside the documented
// label-based subset. Like every other unmodelled predicate it can only cause an
// over-estimate, which the watchdog covers.
func matchesRequiredNodeAffinity(node *corev1.Node, affinity *corev1.Affinity) bool {
	if affinity == nil || affinity.NodeAffinity == nil {
		return true
	}
	required := affinity.NodeAffinity.RequiredDuringSchedulingIgnoredDuringExecution
	if required == nil || len(required.NodeSelectorTerms) == 0 {
		return true
	}
	for i := range required.NodeSelectorTerms {
		if matchesTerm(node, &required.NodeSelectorTerms[i]) {
			return true
		}
	}
	return false
}

func matchesTerm(node *corev1.Node, term *corev1.NodeSelectorTerm) bool {
	// An empty term matches nothing, which is how the API server treats it.
	if len(term.MatchExpressions) == 0 && len(term.MatchFields) == 0 {
		return false
	}
	for i := range term.MatchExpressions {
		if !matchesExpression(node.Labels, &term.MatchExpressions[i]) {
			return false
		}
	}
	return true
}

func matchesExpression(nodeLabels map[string]string, expr *corev1.NodeSelectorRequirement) bool {
	value, present := nodeLabels[expr.Key]

	switch expr.Operator {
	case corev1.NodeSelectorOpExists:
		return present
	case corev1.NodeSelectorOpDoesNotExist:
		return !present
	case corev1.NodeSelectorOpIn:
		return present && containsString(expr.Values, value)
	case corev1.NodeSelectorOpNotIn:
		return !present || !containsString(expr.Values, value)
	case corev1.NodeSelectorOpGt, corev1.NodeSelectorOpLt:
		return compareNumeric(expr.Operator, value, present, expr.Values)
	default:
		// An operator we do not model. Refusing the node would be the
		// conservative choice for scheduling, but here it would silently shrink
		// capacity; accepting it keeps the error in the documented
		// over-estimate direction, where the watchdog is the backstop.
		return true
	}
}

func compareNumeric(op corev1.NodeSelectorOperator, value string, present bool, values []string) bool {
	if !present || len(values) != 1 {
		return false
	}
	have, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return false
	}
	want, err := strconv.ParseInt(values[0], 10, 64)
	if err != nil {
		return false
	}
	if op == corev1.NodeSelectorOpGt {
		return have > want
	}
	return have < want
}

func containsString(haystack []string, needle string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}

// ParseNodeLabelSelector turns the resources.nodeLabelSelector setting into a
// matcher. An empty string means "no extra restriction" rather than "match
// nothing", which is the difference between the default config working and the
// candidate set being empty on every cluster.
func ParseNodeLabelSelector(expr string) (labels.Selector, error) {
	if expr == "" {
		return nil, nil
	}
	selector, err := labels.Parse(expr)
	if err != nil {
		return nil, fmt.Errorf("resources.nodeLabelSelector %q is not a valid label selector: %w", expr, err)
	}
	return selector, nil
}
