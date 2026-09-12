package kubernetes

import (
	"fmt"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
)

// StaticReader is a ClusterReader backed by in-memory objects.
//
// It is what lets the controller's reconcile loop — gates, history, backoff,
// and the whole decision trace — be tested exhaustively with no cluster, no
// fake clientset, and no informer machinery. Reproducing "node cordoned, one
// pod Pending, metrics stale, backoff armed" as a live cluster state is slow
// and flaky; here it is a struct literal.
//
// It also has a legitimate non-test use: pointed at objects loaded from a
// kubectl dump, it computes the fit capacity of a cluster offline.
type StaticReader struct {
	Synced         bool
	NodeList       []*corev1.Node
	PodList        []*corev1.Pod
	DeploymentList []*appsv1.Deployment
	ReplicaSetList []*appsv1.ReplicaSet
	HPAList        []*autoscalingv2.HorizontalPodAutoscaler

	// Err, when set, is returned by every accessor. It exists so that the
	// ErrorAPIFailure path — the one that must never take an action in either
	// direction — is reachable in a test.
	Err error
}

// HasSynced implements ClusterReader.
func (r *StaticReader) HasSynced() bool { return r.Synced }

// Nodes implements ClusterReader.
func (r *StaticReader) Nodes() ([]*corev1.Node, error) {
	return r.NodeList, r.Err
}

// Pods implements ClusterReader.
func (r *StaticReader) Pods() ([]*corev1.Pod, error) {
	return r.PodList, r.Err
}

// PodsInNamespace implements ClusterReader.
func (r *StaticReader) PodsInNamespace(namespace string) ([]*corev1.Pod, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	out := make([]*corev1.Pod, 0, len(r.PodList))
	for _, pod := range r.PodList {
		if pod.Namespace == namespace {
			out = append(out, pod)
		}
	}
	return out, nil
}

// Deployment implements ClusterReader.
func (r *StaticReader) Deployment(namespace, name string) (*appsv1.Deployment, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	for _, deployment := range r.DeploymentList {
		if deployment.Namespace == namespace && deployment.Name == name {
			return deployment, nil
		}
	}
	return nil, fmt.Errorf("Deployment %s/%s not found", namespace, name)
}

// ReplicaSets implements ClusterReader.
func (r *StaticReader) ReplicaSets(namespace string) ([]*appsv1.ReplicaSet, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	out := make([]*appsv1.ReplicaSet, 0, len(r.ReplicaSetList))
	for _, rs := range r.ReplicaSetList {
		if rs.Namespace == namespace {
			out = append(out, rs)
		}
	}
	return out, nil
}

// HorizontalPodAutoscalers implements ClusterReader.
func (r *StaticReader) HorizontalPodAutoscalers(namespace string) ([]*autoscalingv2.HorizontalPodAutoscaler, error) {
	if r.Err != nil {
		return nil, r.Err
	}
	out := make([]*autoscalingv2.HorizontalPodAutoscaler, 0, len(r.HPAList))
	for _, hpa := range r.HPAList {
		if hpa.Namespace == namespace {
			out = append(out, hpa)
		}
	}
	return out, nil
}
