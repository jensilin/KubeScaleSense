package kubernetes

import (
	"fmt"
	"strconv"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

// revisionAnnotation is how the Deployment controller marks which ReplicaSet
// generation a ReplicaSet belongs to.
const revisionAnnotation = "deployment.kubernetes.io/revision"

// TargetState is everything the decision needs to know about the scaled
// workload and its pods.
type TargetState struct {
	Deployment *appsv1.Deployment

	CurrentReplicas     int32
	ReadyReplicas       int32
	AvailableReplicas   int32
	UnavailableReplicas int32
	UpdatedReplicas     int32

	// RolloutInProgress means the Deployment is mid-update, during which a
	// scale-up would itself be surged and the fit estimate we gate on would not
	// be the resource cost the Deployment controller actually incurs (DR-08).
	RolloutInProgress bool

	// CurrentReplicaSet is the generation of pods the target is converging on.
	// Nil when no ReplicaSet is owned yet, which happens briefly after a
	// Deployment is created.
	CurrentReplicaSet *appsv1.ReplicaSet

	// Pods are the pods of CurrentReplicaSet only. Restricting to the current
	// generation is what stops a doomed pod of a superseded ReplicaSet from
	// being blamed on a healthy new one during a rollout.
	Pods []*corev1.Pod

	// PendingPods are ours and unschedulable — the L3 observation that checks
	// whether the L2 feasibility estimate was right.
	PendingPods      int32
	OldestPendingAge time.Duration

	// UnhealthyPods are scheduled but not becoming Ready. They hold real
	// cluster capacity while processing nothing, so they must block scale-ups
	// rather than encourage them (DR-06).
	UnhealthyPods   int32
	UnhealthyPodAge time.Duration
}

// ObserveTarget assembles the target's state from the caches.
//
// Everything here is observation. The Deployment is read and never written, and
// this function is the only place that interprets its status fields, so the
// rollout and health definitions live in exactly one place.
func ObserveTarget(reader ClusterReader, namespace, name string, now time.Time) (TargetState, error) {
	deployment, err := reader.Deployment(namespace, name)
	if err != nil {
		return TargetState{}, err
	}

	state := TargetState{
		Deployment:          deployment,
		CurrentReplicas:     specReplicas(deployment),
		ReadyReplicas:       deployment.Status.ReadyReplicas,
		AvailableReplicas:   deployment.Status.AvailableReplicas,
		UnavailableReplicas: deployment.Status.UnavailableReplicas,
		UpdatedReplicas:     deployment.Status.UpdatedReplicas,
	}

	// Two independent signs of an in-flight rollout: the controller has not yet
	// observed the current spec, or it has but not all replicas are updated.
	state.RolloutInProgress = deployment.Generation != deployment.Status.ObservedGeneration ||
		deployment.Status.UpdatedReplicas != state.CurrentReplicas

	replicaSets, err := reader.ReplicaSets(namespace)
	if err != nil {
		return TargetState{}, err
	}
	state.CurrentReplicaSet = currentReplicaSet(deployment, replicaSets)

	if state.CurrentReplicaSet != nil {
		pods, err := reader.PodsInNamespace(namespace)
		if err != nil {
			return TargetState{}, err
		}
		state.Pods = podsOwnedBy(pods, state.CurrentReplicaSet)
		state.summarisePodHealth(now)
	}

	return state, nil
}

// summarisePodHealth classifies the target's pods into pending and unhealthy.
func (s *TargetState) summarisePodHealth(now time.Time) {
	for _, pod := range s.Pods {
		if pod.DeletionTimestamp != nil {
			// A pod on its way out is neither pending nor unhealthy; it is
			// finished. Counting it would make every scale-down look like a
			// health problem.
			continue
		}

		scheduled, scheduledAt := scheduledCondition(pod)

		switch {
		case !scheduled && pod.Status.Phase == corev1.PodPending:
			s.PendingPods++
			if age := now.Sub(pendingSince(pod)); age > s.OldestPendingAge {
				s.OldestPendingAge = age
			}

		case scheduled && !isReady(pod) && pod.Status.Phase != corev1.PodSucceeded:
			// Includes the normal case of a pod that started seconds ago. The
			// guard compares the age against podStartupTimeout, so a healthy
			// start is not mistaken for a failure.
			s.UnhealthyPods++
			since := scheduledAt
			if since.IsZero() {
				since = pod.CreationTimestamp.Time
			}
			if age := now.Sub(since); age > s.UnhealthyPodAge {
				s.UnhealthyPodAge = age
			}
		}
	}
}

// specReplicas reads the desired replica count, defaulting to 1 as the API does
// when the field is unset.
func specReplicas(deployment *appsv1.Deployment) int32 {
	if deployment.Spec.Replicas == nil {
		return 1
	}
	return *deployment.Spec.Replicas
}

// currentReplicaSet picks the target's newest ReplicaSet.
//
// The revision annotation is the authoritative ordering — it is what the
// Deployment controller itself uses — with creation time as the tie-break for
// the window before the annotation is set.
func currentReplicaSet(deployment *appsv1.Deployment, sets []*appsv1.ReplicaSet) *appsv1.ReplicaSet {
	var best *appsv1.ReplicaSet
	bestRevision := int64(-1)

	for _, rs := range sets {
		if !isOwnedBy(rs.OwnerReferences, deployment.UID) {
			continue
		}
		revision := revisionOf(rs)
		switch {
		case best == nil,
			revision > bestRevision,
			revision == bestRevision && rs.CreationTimestamp.After(best.CreationTimestamp.Time):
			best, bestRevision = rs, revision
		}
	}
	return best
}

func revisionOf(rs *appsv1.ReplicaSet) int64 {
	raw, ok := rs.Annotations[revisionAnnotation]
	if !ok {
		return -1
	}
	revision, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return -1
	}
	return revision
}

func podsOwnedBy(pods []*corev1.Pod, rs *appsv1.ReplicaSet) []*corev1.Pod {
	owned := make([]*corev1.Pod, 0, len(pods))
	for _, pod := range pods {
		if isOwnedBy(pod.OwnerReferences, rs.UID) {
			owned = append(owned, pod)
		}
	}
	return owned
}

// isOwnedBy matches on UID rather than name, so a ReplicaSet or pod left behind
// by a deleted-and-recreated Deployment of the same name is not adopted.
func isOwnedBy(refs []metav1.OwnerReference, uid types.UID) bool {
	for i := range refs {
		if refs[i].UID == uid {
			return true
		}
	}
	return false
}

// scheduledCondition reports whether the pod has been assigned to a node, and
// when that happened.
func scheduledCondition(pod *corev1.Pod) (bool, time.Time) {
	for i := range pod.Status.Conditions {
		cond := &pod.Status.Conditions[i]
		if cond.Type == corev1.PodScheduled {
			return cond.Status == corev1.ConditionTrue, cond.LastTransitionTime.Time
		}
	}
	// No condition yet. spec.nodeName is the fallback truth: the scheduler sets
	// it at the moment of placement.
	return pod.Spec.NodeName != "", time.Time{}
}

func isReady(pod *corev1.Pod) bool {
	for i := range pod.Status.Conditions {
		cond := &pod.Status.Conditions[i]
		if cond.Type == corev1.PodReady {
			return cond.Status == corev1.ConditionTrue
		}
	}
	return false
}

// pendingSince is when the pod started waiting for a node.
func pendingSince(pod *corev1.Pod) time.Time {
	if _, at := scheduledCondition(pod); !at.IsZero() {
		return at
	}
	return pod.CreationTimestamp.Time
}

// ConflictingHPA returns the first HorizontalPodAutoscaler that targets the
// Deployment, or nil.
//
// Detection rather than coexistence is the whole policy. Whichever controller
// writes last would win, so there is no safe way to share a replica count and
// the only correct response is to stop (FR-19, I-12).
func ConflictingHPA(hpas []*autoscalingv2.HorizontalPodAutoscaler, deploymentName string) *autoscalingv2.HorizontalPodAutoscaler {
	for _, hpa := range hpas {
		ref := hpa.Spec.ScaleTargetRef
		if ref.Kind == "Deployment" && ref.Name == deploymentName {
			return hpa
		}
	}
	return nil
}

// DescribeHPA renders an HPA for an operator-facing message.
func DescribeHPA(hpa *autoscalingv2.HorizontalPodAutoscaler) string {
	if hpa == nil {
		return ""
	}
	return fmt.Sprintf("%s/%s", hpa.Namespace, hpa.Name)
}
