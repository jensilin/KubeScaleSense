package resources

import (
	"fmt"

	corev1 "k8s.io/api/core/v1"
)

// bytesPerMiB converts the MiB-denominated reserve in the configuration to the
// bytes that Kubernetes quantities are measured in.
const bytesPerMiB int64 = 1024 * 1024

// Request is the scheduler-visible resource cost of one pod, in the units the
// two dimensions are naturally measured in: CPU in millicores, memory in bytes.
// Integers throughout, so that identical inputs produce identical decisions at
// exact boundaries (NFR-07, DR-15).
type Request struct {
	CPUMilli    int64
	MemoryBytes int64
}

// MemoryMiB renders the memory request for logs and events, where bytes are
// unreadable. It truncates, so it is display-only and never a decision input.
func (r Request) MemoryMiB() int64 {
	return r.MemoryBytes / bytesPerMiB
}

// String matches the "500m/512Mi" form used in the documented event messages
// (architecture § 8.2).
func (r Request) String() string {
	return fmt.Sprintf("%dm/%dMi", r.CPUMilli, r.MemoryMiB())
}

// Dimension names a resource dimension of the fit calculation. It is a distinct
// type because the value is reported to operators — as the
// kss_fit_capacity_blocking_dimension label and in every log line — and turning
// "insufficient resources" into "add CPU" is the whole point of tracking it.
type Dimension string

// The modelled dimensions. v0.1 models CPU, memory, and pod slots only;
// extended resources and ephemeral storage are NG-9 (resource-calculation § 3).
const (
	DimensionCPU      Dimension = "cpu"
	DimensionMemory   Dimension = "memory"
	DimensionPodSlots Dimension = "podSlots"
	DimensionNone     Dimension = "none"
)

// dimensionPrecedence is the documented tie-break order for the binding
// dimension: cpu, then memory, then pod slots (UT-20). A tie is common rather
// than exotic — a node whose free CPU and free memory are both driven to zero
// by the reserve ties at fit zero in both dimensions — so the order needs to be
// specified rather than left to map iteration.
var dimensionPrecedence = []Dimension{DimensionCPU, DimensionMemory, DimensionPodSlots}

// EffectivePodRequest computes the request the scheduler charges for one pod of
// this template, per resource-calculation § 3:
//
//	effective(r) = max( containers(r) + sidecars(r), initPeak(r) ) + overhead(r)
//
// A missing or zero request in either dimension is a hard error rather than a
// zero: a zero-request pod is schedulable anywhere, which makes fit capacity
// meaningless and hands the safety question back to the kubelet's eviction
// logic — the opposite of this project's purpose (A-03, CR-2, UT-17).
func EffectivePodRequest(spec *corev1.PodSpec) (Request, error) {
	if spec == nil {
		return Request{}, fmt.Errorf("pod template is empty: cannot determine the per-pod resource request")
	}

	req := podRequest(spec)

	var missing []string
	if req.CPUMilli <= 0 {
		missing = append(missing, "cpu")
	}
	if req.MemoryBytes <= 0 {
		missing = append(missing, "memory")
	}
	if len(missing) > 0 {
		return Request{}, fmt.Errorf(
			"target pod template declares no %s request; fit capacity cannot be estimated for a pod that "+
				"is schedulable anywhere, so add resources.requests to every container (A-03)",
			joinAnd(missing))
	}
	return req, nil
}

// podRequest is the same arithmetic without the validation, used to charge
// *other* pods against their node in step 3. Neighbouring pods are allowed to
// declare nothing — BestEffort pods are legal and common — and they then
// consume no scheduling budget, which resource-calculation § 4.1 records as a
// known over-estimation of free capacity rather than an error to reject.
func podRequest(spec *corev1.PodSpec) Request {
	var appCPU, appMemory int64
	for i := range spec.Containers {
		rl := spec.Containers[i].Resources.Requests
		appCPU += cpuMilli(rl)
		appMemory += memoryBytes(rl)
	}

	// Init containers are walked in declaration order because the peak of a
	// non-restartable init container includes the sidecars started *before* it.
	var sidecarCPU, sidecarMemory int64
	var initPeakCPU, initPeakMemory int64
	for i := range spec.InitContainers {
		c := &spec.InitContainers[i]
		cpu := cpuMilli(c.Resources.Requests)
		memory := memoryBytes(c.Resources.Requests)

		if isRestartable(c) {
			// A native sidecar runs for the pod's whole lifetime, so it adds to
			// the concurrent sum. Ignoring it under-counts every replica by
			// exactly the sidecar's size.
			sidecarCPU += cpu
			sidecarMemory += memory
			continue
		}
		// Ordinary init containers run sequentially, so the pod's peak is the
		// most demanding phase rather than the sum of all of them.
		initPeakCPU = maxInt64(initPeakCPU, cpu+sidecarCPU)
		initPeakMemory = maxInt64(initPeakMemory, memory+sidecarMemory)
	}

	// spec.overhead is charged by the scheduler when a RuntimeClass declares it,
	// so it must be charged here too.
	overheadCPU := cpuMilli(spec.Overhead)
	overheadMemory := memoryBytes(spec.Overhead)

	return Request{
		CPUMilli:    maxInt64(appCPU+sidecarCPU, initPeakCPU) + overheadCPU,
		MemoryBytes: maxInt64(appMemory+sidecarMemory, initPeakMemory) + overheadMemory,
	}
}

func isRestartable(c *corev1.Container) bool {
	return c.RestartPolicy != nil && *c.RestartPolicy == corev1.ContainerRestartPolicyAlways
}

func cpuMilli(rl corev1.ResourceList) int64 {
	q, ok := rl[corev1.ResourceCPU]
	if !ok {
		return 0
	}
	return q.MilliValue()
}

func memoryBytes(rl corev1.ResourceList) int64 {
	q, ok := rl[corev1.ResourceMemory]
	if !ok {
		return 0
	}
	return q.Value()
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func joinAnd(items []string) string {
	switch len(items) {
	case 0:
		return ""
	case 1:
		return items[0]
	default:
		out := items[0]
		for _, item := range items[1 : len(items)-1] {
			out += ", " + item
		}
		return out + " or " + items[len(items)-1]
	}
}
