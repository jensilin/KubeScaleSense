package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
	"net/http"
	"slices"
	"strconv"
	"strings"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// DeletionCostAnnotation is read by the ReplicaSet controller when it picks
// which pods a scale-down removes: lower cost goes first, among pods that are
// otherwise equal. A preference, not a guarantee — a pod that is not Ready is
// removed before cost is considered at all (DR-11).
const DeletionCostAnnotation = "controller.kubernetes.io/pod-deletion-cost"

// deletionCostUserAgent is the third write client's identity in the audit log.
const deletionCostUserAgent = "kubescalesense/p3-deletion-cost"

// maxPatchBytes bounds the body the transport reads to validate. The one patch
// it permits is under a hundred bytes.
const maxPatchBytes = 4 << 10

// ErrNotDeletionCostPatch is returned when the pod client attempts anything
// other than setting the deletion-cost annotation on a pod in the target's
// namespace.
var ErrNotDeletionCostPatch = fmt.Errorf(
	"kubescalesense: the pod client may only set the pod-deletion-cost annotation")

// PodsPath returns the API path of the pods collection in a namespace.
func PodsPath(namespace string) string {
	return fmt.Sprintf("/api/v1/namespaces/%s/pods", namespace)
}

// NewDeletionCostRESTConfig builds the REST configuration for the pod client.
//
// RBAC cannot narrow `patch` on pods to one field: the Role that permits this
// annotation equally permits rewriting a pod's labels, which would orphan it
// from its ReplicaSet and make the ReplicaSet create a replacement — a replica
// change through the back door. So the bound lives in the transport, which
// reads every request body and permits exactly one shape: a JSON merge patch
// that sets this one annotation to an integer, on one pod, in one namespace.
func NewDeletionCostRESTConfig(kubeconfigPath, namespace string) (*rest.Config, error) {
	cfg, err := restConfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}

	cfg.UserAgent = deletionCostUserAgent

	collection := PodsPath(namespace) + "/"
	cfg.Wrap(func(base http.RoundTripper) http.RoundTripper {
		return deletionCostTransport{base: base, collection: collection}
	})

	return cfg, nil
}

// deletionCostTransport permits PATCH of a single pod with a body that sets the
// deletion-cost annotation and nothing else. No reads, no other verbs, no
// subresources.
type deletionCostTransport struct {
	base       http.RoundTripper
	collection string
}

// RoundTrip implements http.RoundTripper.
func (t deletionCostTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	refuse := func(why string) (*http.Response, error) {
		return nil, fmt.Errorf("%w (attempted %s %s: %s)", ErrNotDeletionCostPatch, req.Method, req.URL.Path, why)
	}

	if req.Method != http.MethodPatch {
		return refuse("only PATCH is permitted")
	}
	name, ok := strings.CutPrefix(req.URL.Path, t.collection)
	if !ok || name == "" || strings.Contains(name, "/") {
		return refuse("only a pod in " + strings.TrimSuffix(t.collection, "/") + " may be patched, and no subresource")
	}
	if mediaType := req.Header.Get("Content-Type"); mediaType != string(types.MergePatchType) {
		return refuse("the patch must be a JSON merge patch, got " + mediaType)
	}
	if req.Body == nil {
		return refuse("the patch has no body")
	}

	body, err := io.ReadAll(io.LimitReader(req.Body, maxPatchBytes+1))
	_ = req.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading the patch body to validate it: %w", err)
	}
	if len(body) > maxPatchBytes {
		return refuse("the patch body is too large to be a deletion-cost patch")
	}
	if err := validateDeletionCostPatch(body); err != nil {
		return refuse(err.Error())
	}

	// The body was consumed to inspect it; the request that goes on carries
	// the same bytes.
	forwarded := req.Clone(req.Context())
	forwarded.Body = io.NopCloser(bytes.NewReader(body))
	forwarded.ContentLength = int64(len(body))
	forwarded.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	return t.base.RoundTrip(forwarded)
}

// validateDeletionCostPatch accepts exactly
// {"metadata":{"annotations":{"controller.kubernetes.io/pod-deletion-cost":"<int32>"}}}.
func validateDeletionCostPatch(body []byte) error {
	var patch map[string]json.RawMessage
	if err := json.Unmarshal(body, &patch); err != nil {
		return fmt.Errorf("the patch is not a JSON object: %w", err)
	}
	if len(patch) != 1 || patch["metadata"] == nil {
		return fmt.Errorf("the patch touches %s; only metadata is permitted", keys(patch))
	}

	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(patch["metadata"], &metadata); err != nil {
		return fmt.Errorf("metadata is not an object: %w", err)
	}
	if len(metadata) != 1 || metadata["annotations"] == nil {
		return fmt.Errorf("the patch touches metadata %s; only annotations are permitted", keys(metadata))
	}

	var annotations map[string]*string
	if err := json.Unmarshal(metadata["annotations"], &annotations); err != nil {
		return fmt.Errorf("annotations is not an object of strings: %w", err)
	}
	value, ok := annotations[DeletionCostAnnotation]
	if len(annotations) != 1 || !ok {
		return fmt.Errorf("the patch sets annotations %s; only %s is permitted", keys(annotations),
			DeletionCostAnnotation)
	}
	if value == nil {
		return fmt.Errorf("the patch removes %s; it may only set it", DeletionCostAnnotation)
	}
	if _, err := strconv.ParseInt(*value, 10, 32); err != nil {
		return fmt.Errorf("%s must be a 32-bit integer, got %q", DeletionCostAnnotation, *value)
	}
	return nil
}

// deletionCostPatch is the one body the transport permits.
func deletionCostPatch(cost int32) []byte {
	patch := map[string]any{
		"metadata": map[string]any{
			"annotations": map[string]string{DeletionCostAnnotation: strconv.FormatInt(int64(cost), 10)},
		},
	}
	body, _ := json.Marshal(patch)
	return body
}

// PodCost is one pod's deletion cost and whether it differs from the value the
// pod already carries.
type PodCost struct {
	Pod     string
	Cost    int32
	Changed bool
}

// DeletionCosts derives a deletion cost for each of the target's Ready pods
// from its own outstanding work: the busiest pod is the most expensive to
// remove, because its queued and in-flight records are the ones a termination
// has to drain.
//
// All or nothing. A Ready pod with no per-pod value fails the whole set,
// because costs from two different moments — some refreshed now, the rest left
// from an earlier scale-down — would order pods by something other than how
// busy they are. Pods that are not Ready or are terminating are skipped: the
// ReplicaSet controller removes those first whatever their cost.
func DeletionCosts(pods []*corev1.Pod, workByPodIP map[string]int64) ([]PodCost, error) {
	if workByPodIP == nil {
		return nil, errors.New("the workload signal does not report work per pod")
	}

	var costs []PodCost
	for _, pod := range pods {
		if pod.DeletionTimestamp != nil || !isReady(pod) {
			continue
		}
		work, ok := workByPodIP[pod.Status.PodIP]
		if pod.Status.PodIP == "" || !ok {
			return nil, fmt.Errorf("pod %s (IP %q) is Ready but was not scraped, so its work is unknown",
				pod.Name, pod.Status.PodIP)
		}
		cost := int32(min(max(work, 0), int64(math.MaxInt32)))
		costs = append(costs, PodCost{
			Pod:     pod.Name,
			Cost:    cost,
			Changed: pod.Annotations[DeletionCostAnnotation] != strconv.FormatInt(int64(cost), 10),
		})
	}
	if len(costs) == 0 {
		return nil, errors.New("the target has no Ready pods to order")
	}
	return costs, nil
}

// podPatcher is the slice of client-go's PodInterface the writer uses.
type podPatcher interface {
	Patch(ctx context.Context, name string, pt types.PatchType, data []byte, opts metav1.PatchOptions,
		subresources ...string) (*corev1.Pod, error)
}

// DeletionCostWriter sets pod-deletion-cost on pods of one namespace.
type DeletionCostWriter struct {
	pods      podPatcher
	namespace string
}

// NewDeletionCostWriter binds a writer to the target's namespace.
func NewDeletionCostWriter(client kubernetes.Interface, namespace string) *DeletionCostWriter {
	return &DeletionCostWriter{pods: client.CoreV1().Pods(namespace), namespace: namespace}
}

// SetDeletionCost sets one pod's deletion cost.
func (w *DeletionCostWriter) SetDeletionCost(ctx context.Context, pod string, cost int32) error {
	// The third mutating client-go call in the repository. A merge patch of
	// one annotation: it leaves every other field of the pod as it was, and
	// the transport refuses any body that says otherwise.
	if _, err := w.pods.Patch(ctx, pod, types.MergePatchType, deletionCostPatch(cost), metav1.PatchOptions{}); err != nil {
		return fmt.Errorf("setting %s=%d on pod %s/%s: %w", DeletionCostAnnotation, cost, w.namespace, pod, err)
	}
	return nil
}

func keys[V any](m map[string]V) []string {
	return slices.Sorted(maps.Keys(m))
}
