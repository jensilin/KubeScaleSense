package resources

import (
	"sort"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"

	"github.com/jensilin/KubeScaleSense/internal/config"
)

// Options is the tuning half of the calculation, derived from the resources
// block of the configuration. It is a separate type from config.ResourcesConfig
// because the label selector is pre-parsed and the memory reserve is converted
// to bytes once, at startup, rather than on every reconcile.
type Options struct {
	PerNodeReserveCPUMilli    int64
	PerNodeReserveMemoryBytes int64
	FitCapacityMarginPods     int32
	RespectNodeSelector       bool
	RespectNodeAffinity       bool
	RespectTaints             bool
	NodeLabelSelector         labels.Selector
}

// OptionsFrom converts validated configuration into Options. The selector is
// parsed here so that a malformed expression fails at startup rather than
// silently emptying the candidate set on the first reconcile.
func OptionsFrom(cfg config.ResourcesConfig) (Options, error) {
	selector, err := ParseNodeLabelSelector(cfg.NodeLabelSelector)
	if err != nil {
		return Options{}, err
	}
	return Options{
		PerNodeReserveCPUMilli:    cfg.PerNodeReserveCPUMilli,
		PerNodeReserveMemoryBytes: cfg.PerNodeReserveMemoryMiB * bytesPerMiB,
		FitCapacityMarginPods:     int32(cfg.FitCapacityMarginPods),
		RespectNodeSelector:       cfg.RespectNodeSelector,
		RespectNodeAffinity:       cfg.RespectNodeAffinity,
		RespectTaints:             cfg.RespectTaints,
		NodeLabelSelector:         selector,
	}, nil
}

// NodeFit is the per-node result. Exported because the per-node breakdown is
// the fragmentation evidence: an operator seeing F=0 across a cluster with
// cores to spare needs to see *where* the room is to understand why none of it
// is usable.
type NodeFit struct {
	Name              string
	AllocatableCPU    int64
	AllocatableMemory int64
	RequestedCPU      int64
	RequestedMemory   int64
	FreeCPUMilli      int64
	FreeMemoryBytes   int64
	FreeSlots         int64
	Fit               int32
	Binding           Dimension
}

// Feasibility is the resource half of the decision snapshot
// (architecture § 6). Everything here is derived from cached objects and pure
// arithmetic, so it is reproducible from a fixture.
type Feasibility struct {
	PodRequest Request

	// FitCapacity is F: the additional pods placeable now, after the margin.
	// This is the value the feasibility gate consumes and the headline
	// resource-awareness metric.
	FitCapacity int32

	// RawFitSum is Σ fit(n) before the margin is subtracted, kept so that the
	// margin's effect is visible rather than baked in.
	RawFitSum int32

	CandidateNodes int32
	TotalNodes     int32
	Exclusions     map[string]int32

	// Blocking is the dimension that bound the count. It is what turns
	// "insufficient resources" into "add CPU".
	Blocking Dimension

	// Aggregate free resources over candidate nodes. Reporting only: using
	// these as a decision input would reintroduce exactly the fragmentation
	// error this package exists to eliminate (I-2).
	FreeCPUMilli    int64
	FreeMemoryBytes int64

	Nodes []NodeFit
}

// Calculate runs steps 1, 3, and 4 over a cached node and pod list, having been
// given the target's already-validated effective pod request.
//
// The pod list is expected to be cluster-scoped. Summing only the target
// namespace would be a serious over-estimate on a shared cluster, because node
// budget is shared cluster-wide (resource-calculation § 4).
//
// Determinism: nodes are sorted by name before the walk, so two identical
// snapshots produce byte-identical per-node output as well as the same F
// (NFR-07).
func Calculate(template *corev1.PodSpec, podRequest Request, nodes []*corev1.Node, pods []*corev1.Pod, opts Options, now time.Time) Feasibility {
	index := byNode(pods)

	result := Feasibility{
		PodRequest: podRequest,
		TotalNodes: int32(len(nodes)),
		Exclusions: make(map[string]int32),
		Blocking:   DimensionNone,
	}

	for _, node := range sortedByName(nodes) {
		podsOnNode := index[node.Name]

		verdict := evaluateCandidate(node, podsOnNode, template, opts, now)
		if verdict.excluded {
			result.Exclusions[verdict.reason]++
			continue
		}

		free, requested, allocatable := nodeFree(node, podsOnNode, opts)
		fit, binding := fitOnNode(free, verdict.freeSlots, podRequest)

		result.CandidateNodes++
		result.RawFitSum += fit
		result.FreeCPUMilli += free.CPUMilli
		result.FreeMemoryBytes += free.MemoryBytes
		result.Nodes = append(result.Nodes, NodeFit{
			Name:              node.Name,
			AllocatableCPU:    allocatable.CPUMilli,
			AllocatableMemory: allocatable.MemoryBytes,
			RequestedCPU:      requested.CPUMilli,
			RequestedMemory:   requested.MemoryBytes,
			FreeCPUMilli:      free.CPUMilli,
			FreeMemoryBytes:   free.MemoryBytes,
			FreeSlots:         verdict.freeSlots,
			Fit:               fit,
			Binding:           binding,
		})
	}

	// The global margin is pessimism about the model itself: unmodelled
	// predicates, in-flight scheduling by other actors, and rounding. It is the
	// last line of defence before the Pending watchdog, and it is subtracted
	// after the per-node sum so that it cannot be absorbed by a single roomy
	// node (resource-calculation § 5).
	if capacity := result.RawFitSum - opts.FitCapacityMarginPods; capacity > 0 {
		result.FitCapacity = capacity
	}

	result.Blocking = aggregateBinding(result.Nodes)
	return result
}

// byNode indexes pods by their assigned node.
//
// Assigned-but-Pending pods are included deliberately: a pod with spec.nodeName
// set is already charged to that node by the scheduler even before it starts.
// Unassigned Pending pods are charged to no node, yet they will compete with
// ours — one of the races perNodeReserve and fitCapacityMarginPods exist to
// absorb (resource-calculation § 8).
func byNode(pods []*corev1.Pod) map[string][]*corev1.Pod {
	index := make(map[string][]*corev1.Pod)
	for _, pod := range pods {
		if pod.Spec.NodeName == "" {
			continue
		}
		index[pod.Spec.NodeName] = append(index[pod.Spec.NodeName], pod)
	}
	return index
}

// nodeFree computes step 3 for one node: allocatable minus the requests already
// charged to it minus the configured reserve, floored at zero.
//
// allocatable is used rather than capacity throughout. allocatable is capacity
// minus kube-reserved, system-reserved, and the eviction thresholds — the
// scheduler's real budget. The difference is often 10–15 % of a node and is
// exactly the margin whose absence causes kubelet evictions (ADR-05).
func nodeFree(node *corev1.Node, podsOnNode []*corev1.Pod, opts Options) (free Request, requested Request, allocatable Request) {
	allocatable = Request{
		CPUMilli:    cpuMilli(node.Status.Allocatable),
		MemoryBytes: memoryBytes(node.Status.Allocatable),
	}

	for _, pod := range podsOnNode {
		if !chargeable(pod) {
			continue
		}
		r := podRequest(&pod.Spec)
		requested.CPUMilli += r.CPUMilli
		requested.MemoryBytes += r.MemoryBytes
	}

	free = Request{
		CPUMilli:    floorZero(allocatable.CPUMilli - requested.CPUMilli - opts.PerNodeReserveCPUMilli),
		MemoryBytes: floorZero(allocatable.MemoryBytes - requested.MemoryBytes - opts.PerNodeReserveMemoryBytes),
	}
	return free, requested, allocatable
}

// fitOnNode is step 4 for one node: the minimum across dimensions, because the
// scheduler requires all of them to fit simultaneously.
//
// Floor division, because a pod is indivisible: 900m free with a 500m request
// is one pod, not 1.8.
func fitOnNode(free Request, freeSlots int64, req Request) (fit int32, binding Dimension) {
	cpuFit := free.CPUMilli / req.CPUMilli
	memoryFit := free.MemoryBytes / req.MemoryBytes

	counts := map[Dimension]int64{
		DimensionCPU:      cpuFit,
		DimensionMemory:   memoryFit,
		DimensionPodSlots: freeSlots,
	}

	lowest := cpuFit
	for _, dim := range dimensionPrecedence {
		if counts[dim] < lowest {
			lowest = counts[dim]
		}
	}
	// Ties resolve by the documented precedence, which is why this second pass
	// walks dimensionPrecedence rather than the map.
	binding = DimensionCPU
	for _, dim := range dimensionPrecedence {
		if counts[dim] == lowest {
			binding = dim
			break
		}
	}

	if lowest < 0 {
		lowest = 0
	}
	return int32(lowest), binding
}

// aggregateBinding picks the dimension to report for the cluster as a whole.
//
// The rule is "the dimension that binds on the most candidate nodes", with the
// cpu -> memory -> podSlots precedence breaking ties. In the reference example
// CPU binds on all three candidate nodes and cpu is reported, which is the
// answer an operator can act on: adding memory would change nothing.
func aggregateBinding(fits []NodeFit) Dimension {
	if len(fits) == 0 {
		return DimensionNone
	}
	tally := make(map[Dimension]int, len(dimensionPrecedence))
	for i := range fits {
		tally[fits[i].Binding]++
	}
	best := DimensionNone
	bestCount := 0
	for _, dim := range dimensionPrecedence {
		if tally[dim] > bestCount {
			best, bestCount = dim, tally[dim]
		}
	}
	if bestCount == 0 {
		return DimensionNone
	}
	return best
}

func floorZero(v int64) int64 {
	if v > 0 {
		return v
	}
	return 0
}

func sortedByName(nodes []*corev1.Node) []*corev1.Node {
	out := make([]*corev1.Node, len(nodes))
	copy(out, nodes)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
