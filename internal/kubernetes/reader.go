package kubernetes

import (
	"context"
	"fmt"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	appsv1listers "k8s.io/client-go/listers/apps/v1"
	autoscalingv2listers "k8s.io/client-go/listers/autoscaling/v2"
	corev1listers "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/tools/cache"
)

// resyncPeriod forces a periodic re-list even when no watch event arrives.
//
// This is the backstop for a watch that has silently desynced — the informer
// believes it is current while the cluster has moved on, which would make every
// fit calculation confidently wrong. Ten minutes is long enough to be cheap and
// short enough that a desync cannot persist through a whole demo.
const resyncPeriod = 10 * time.Minute

// ClusterReader is the controller's entire view of Kubernetes.
//
// Every method reads. There is no Update, Patch, Create, Delete, or Scale here,
// and adding one would be the visible, reviewable moment at which Phase 1's
// central safety property was given up — which is exactly why the interface is
// this narrow rather than simply wrapping the clientset.
type ClusterReader interface {
	// HasSynced reports whether every cache has completed its initial list.
	// Deciding from a half-populated cache would under-count requested
	// resources and over-state capacity, so the controller refuses to decide
	// until this is true (IT-01).
	HasSynced() bool

	// Nodes returns every node, including ones the target cannot use. The
	// filtering is internal/resources' job, and it needs to count exclusions.
	Nodes() ([]*corev1.Node, error)

	// Pods returns pods across all namespaces, because node budget is shared
	// cluster-wide: pods of other namespaces consume the same allocatable.
	Pods() ([]*corev1.Pod, error)

	// PodsInNamespace returns the pods of one namespace, used to find the
	// target's own pods.
	PodsInNamespace(namespace string) ([]*corev1.Pod, error)

	// Deployment returns the target.
	Deployment(namespace, name string) (*appsv1.Deployment, error)

	// ReplicaSets returns the namespace's ReplicaSets, needed to tell the
	// target's *current* generation of pods from a superseded one. Without it a
	// doomed pod of an old ReplicaSet would be attributed to the new one
	// (DR-09).
	ReplicaSets(namespace string) ([]*appsv1.ReplicaSet, error)

	// HorizontalPodAutoscalers returns the namespace's HPAs, for conflict
	// detection. Two controllers writing one replica count is unwinnable, so
	// the only safe response is to detect it and refuse (FR-19, I-12).
	HorizontalPodAutoscalers(namespace string) ([]*autoscalingv2.HorizontalPodAutoscaler, error)
}

// InformerReader implements ClusterReader over shared informer caches.
type InformerReader struct {
	clusterFactory   informers.SharedInformerFactory
	namespaceFactory informers.SharedInformerFactory

	nodes       corev1listers.NodeLister
	pods        corev1listers.PodLister
	deployments appsv1listers.DeploymentLister
	replicaSets appsv1listers.ReplicaSetLister
	hpas        autoscalingv2listers.HorizontalPodAutoscalerLister

	syncs []cache.InformerSynced
}

// NewInformerReader constructs the informers and their listers.
//
// Nodes and pods are cluster-scoped; the Deployment, ReplicaSets, and HPAs are
// confined to the target namespace. That split is deliberate and matches the
// RBAC exactly: a cluster-wide Deployment watch would need a permission the
// controller has no use for.
func NewInformerReader(clients *Clients, namespace string) *InformerReader {
	clusterFactory := informers.NewSharedInformerFactory(clients.Core, resyncPeriod)
	namespaceFactory := informers.NewSharedInformerFactoryWithOptions(
		clients.Core, resyncPeriod, informers.WithNamespace(namespace))

	nodeInformer := clusterFactory.Core().V1().Nodes()
	podInformer := clusterFactory.Core().V1().Pods()
	deploymentInformer := namespaceFactory.Apps().V1().Deployments()
	replicaSetInformer := namespaceFactory.Apps().V1().ReplicaSets()
	hpaInformer := namespaceFactory.Autoscaling().V2().HorizontalPodAutoscalers()

	return &InformerReader{
		clusterFactory:   clusterFactory,
		namespaceFactory: namespaceFactory,
		nodes:            nodeInformer.Lister(),
		pods:             podInformer.Lister(),
		deployments:      deploymentInformer.Lister(),
		replicaSets:      replicaSetInformer.Lister(),
		hpas:             hpaInformer.Lister(),
		syncs: []cache.InformerSynced{
			nodeInformer.Informer().HasSynced,
			podInformer.Informer().HasSynced,
			deploymentInformer.Informer().HasSynced,
			replicaSetInformer.Informer().HasSynced,
			hpaInformer.Informer().HasSynced,
		},
	}
}

// Start launches the informers and blocks until every cache has synced or the
// budget expires.
//
// A sync timeout is a startup failure rather than a degraded mode: a controller
// that proceeds with empty caches would compute a fit capacity from no nodes and
// no pods, and "the cluster is empty" is the most dangerous possible wrong
// answer for a resource estimator.
func (r *InformerReader) Start(ctx context.Context, timeout time.Duration) error {
	r.clusterFactory.Start(ctx.Done())
	r.namespaceFactory.Start(ctx.Done())

	syncCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	if !cache.WaitForCacheSync(syncCtx.Done(), r.syncs...) {
		return fmt.Errorf("informer caches did not sync within %s: the controller will not decide from a partial view of the cluster", timeout)
	}
	return nil
}

// HasSynced implements ClusterReader.
func (r *InformerReader) HasSynced() bool {
	for _, synced := range r.syncs {
		if !synced() {
			return false
		}
	}
	return true
}

// Nodes implements ClusterReader.
func (r *InformerReader) Nodes() ([]*corev1.Node, error) {
	nodes, err := r.nodes.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("listing nodes from cache: %w", err)
	}
	return nodes, nil
}

// Pods implements ClusterReader.
func (r *InformerReader) Pods() ([]*corev1.Pod, error) {
	pods, err := r.pods.List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("listing pods from cache: %w", err)
	}
	return pods, nil
}

// PodsInNamespace implements ClusterReader.
func (r *InformerReader) PodsInNamespace(namespace string) ([]*corev1.Pod, error) {
	pods, err := r.pods.Pods(namespace).List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("listing pods in namespace %q from cache: %w", namespace, err)
	}
	return pods, nil
}

// Deployment implements ClusterReader.
func (r *InformerReader) Deployment(namespace, name string) (*appsv1.Deployment, error) {
	deployment, err := r.deployments.Deployments(namespace).Get(name)
	if err != nil {
		return nil, fmt.Errorf("reading Deployment %s/%s from cache: %w", namespace, name, err)
	}
	return deployment, nil
}

// ReplicaSets implements ClusterReader.
func (r *InformerReader) ReplicaSets(namespace string) ([]*appsv1.ReplicaSet, error) {
	sets, err := r.replicaSets.ReplicaSets(namespace).List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("listing ReplicaSets in namespace %q from cache: %w", namespace, err)
	}
	return sets, nil
}

// HorizontalPodAutoscalers implements ClusterReader.
func (r *InformerReader) HorizontalPodAutoscalers(namespace string) ([]*autoscalingv2.HorizontalPodAutoscaler, error) {
	hpas, err := r.hpas.HorizontalPodAutoscalers(namespace).List(labels.Everything())
	if err != nil {
		return nil, fmt.Errorf("listing HorizontalPodAutoscalers in namespace %q from cache: %w", namespace, err)
	}
	return hpas, nil
}
