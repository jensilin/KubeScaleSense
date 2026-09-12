package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	autoscalingv2 "k8s.io/api/autoscaling/v2"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/watch"
	corefake "k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsfake "k8s.io/metrics/pkg/client/clientset/versioned/fake"
)

var now = time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)

// --- the read-only guarantee ---------------------------------------------

// Every HTTP method that could change cluster state must be refused at the
// transport, which is the layer no future call site can bypass.
func TestReadOnlyTransport_RefusesMutatingMethods(t *testing.T) {
	t.Parallel()

	allowed := map[string]bool{
		http.MethodGet:     true, // includes watches: a watch is a GET with ?watch=true
		http.MethodHead:    true,
		http.MethodOptions: true,
	}

	methods := []string{
		http.MethodGet, http.MethodHead, http.MethodOptions,
		http.MethodPost, http.MethodPut, http.MethodPatch, http.MethodDelete,
		http.MethodConnect, http.MethodTrace,
		"SCALE", // not a real method; proves the guard allowlists rather than denylists
	}

	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			t.Parallel()

			base := &countingRoundTripper{}
			transport := readOnlyTransport{base: base}

			req, err := http.NewRequest(method, "https://example.invalid/api/v1/nodes", nil)
			if err != nil {
				t.Fatal(err)
			}

			_, err = transport.RoundTrip(req)

			if allowed[method] {
				if err != nil {
					t.Errorf("%s must be permitted, got %v", method, err)
				}
				if base.count() != 1 {
					t.Errorf("%s should have reached the base transport", method)
				}
				return
			}

			if !errors.Is(err, ErrReadOnly) {
				t.Errorf("%s must be refused with ErrReadOnly, got %v", method, err)
			}
			if base.count() != 0 {
				t.Errorf("%s reached the base transport; the request left the process", method)
			}
			// The message must name the attempt, or a 403-looking failure in a
			// log is unattributable.
			if err != nil && !strings.Contains(err.Error(), method) {
				t.Errorf("error should name the attempted method, got %v", err)
			}
		})
	}
}

// The end-to-end proof: a real clientset built from NewReadOnlyRESTConfig
// cannot mutate, and the counting server confirms that no non-GET request ever
// left the process.
//
// This is the test that makes the phase's central safety claim checkable rather
// than merely documented, and it needs no cluster.
func TestReadOnlyClientset_HasNoWritePath(t *testing.T) {
	t.Parallel()

	server, seen := apiServerStub(t)
	clients := clientsFor(t, server.URL)

	ctx := context.Background()

	// Reads work.
	if _, err := clients.Core.CoreV1().Nodes().List(ctx, metav1.ListOptions{}); err != nil {
		t.Fatalf("listing nodes must succeed: %v", err)
	}
	if _, err := clients.Core.AppsV1().Deployments("data-pipeline").Get(ctx, "normalizer", metav1.GetOptions{}); err != nil {
		t.Fatalf("getting the Deployment must succeed: %v", err)
	}

	// Every write the later phases will need, attempted now.
	writes := map[string]func() error{
		"update deployment": func() error {
			_, err := clients.Core.AppsV1().Deployments("data-pipeline").
				Update(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "normalizer", Namespace: "data-pipeline"}}, metav1.UpdateOptions{})
			return err
		},
		"update the scale subresource": func() error {
			_, err := clients.Core.AppsV1().Deployments("data-pipeline").
				UpdateScale(ctx, "normalizer", scaleTo(4), metav1.UpdateOptions{})
			return err
		},
		"patch deployment": func() error {
			_, err := clients.Core.AppsV1().Deployments("data-pipeline").
				Patch(ctx, "normalizer", types.MergePatchType, []byte(`{"spec":{"replicas":4}}`), metav1.PatchOptions{})
			return err
		},
		"patch a pod annotation": func() error {
			_, err := clients.Core.CoreV1().Pods("data-pipeline").
				Patch(ctx, "normalizer-abc", types.MergePatchType, []byte(`{"metadata":{"annotations":{}}}`), metav1.PatchOptions{})
			return err
		},
		"create a pod": func() error {
			_, err := clients.Core.CoreV1().Pods("data-pipeline").
				Create(ctx, &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "rogue"}}, metav1.CreateOptions{})
			return err
		},
		"delete a pod": func() error {
			return clients.Core.CoreV1().Pods("data-pipeline").Delete(ctx, "normalizer-abc", metav1.DeleteOptions{})
		},
		"delete a collection of pods": func() error {
			return clients.Core.CoreV1().Pods("data-pipeline").DeleteCollection(ctx, metav1.DeleteOptions{}, metav1.ListOptions{})
		},
		"evict via a subresource": func() error {
			_, err := clients.Core.CoreV1().Nodes().
				Patch(ctx, "w-1", types.MergePatchType, []byte(`{"spec":{"unschedulable":true}}`), metav1.PatchOptions{})
			return err
		},
		"create an event": func() error {
			_, err := clients.Core.CoreV1().Events("data-pipeline").
				Create(ctx, &corev1.Event{ObjectMeta: metav1.ObjectMeta{Name: "e"}}, metav1.CreateOptions{})
			return err
		},
		"create a lease": func() error {
			_, err := clients.Core.CoordinationV1().Leases("kubescalesense").
				Create(ctx, leaseNamed("kubescalesense"), metav1.CreateOptions{})
			return err
		},
	}

	for name, attempt := range writes {
		t.Run(name, func(t *testing.T) {
			err := attempt()
			if err == nil {
				t.Fatalf("%s succeeded; Phase 1 must have no write path", name)
			}
			if !errors.Is(err, ErrReadOnly) {
				t.Errorf("%s failed with %v, want ErrReadOnly", name, err)
			}
		})
	}

	// The strongest form of the assertion: whatever the errors said, nothing
	// but reads ever reached the network.
	for _, method := range seen.methods() {
		switch method {
		case http.MethodGet, http.MethodHead, http.MethodOptions:
		default:
			t.Errorf("the API server received a %s request; Phase 1 must only read", method)
		}
	}
}

// A metrics client built from the same config is equally incapable of writing.
func TestReadOnlyClientset_MetricsClientIsAlsoReadOnly(t *testing.T) {
	t.Parallel()

	server, _ := apiServerStub(t)
	clients := clientsFor(t, server.URL)

	if _, err := clients.Metrics.MetricsV1beta1().PodMetricses("data-pipeline").
		List(context.Background(), metav1.ListOptions{}); err != nil {
		t.Fatalf("listing pod metrics must succeed: %v", err)
	}
}

func TestNewReadOnlyRESTConfig_SetsAUserAgent(t *testing.T) {
	t.Parallel()

	cfg, err := NewReadOnlyRESTConfig(writeKubeconfig(t, "https://example.invalid"))
	if err != nil {
		t.Fatalf("NewReadOnlyRESTConfig: %v", err)
	}
	// "Who scaled my Deployment?" is answered from the audit log, and the
	// default Go user agent answers it with the binary name only.
	if cfg.UserAgent != userAgent {
		t.Errorf("UserAgent = %q, want %q", cfg.UserAgent, userAgent)
	}
}

func TestNewReadOnlyRESTConfig_ReportsAnUnreadableKubeconfig(t *testing.T) {
	t.Parallel()

	_, err := NewReadOnlyRESTConfig(filepath.Join(t.TempDir(), "absent.yaml"))
	if err == nil {
		t.Fatal("a missing kubeconfig must be reported")
	}
	if !strings.Contains(err.Error(), "kubeconfig") {
		t.Errorf("error should mention the kubeconfig, got %v", err)
	}
}

// --- ObserveTarget --------------------------------------------------------

func TestObserveTarget(t *testing.T) {
	t.Parallel()

	t.Run("steady state", func(t *testing.T) {
		t.Parallel()

		deployment := newDeployment(4, withStatus(4, 4, 4, 4), atGeneration(3, 3))
		rs := newReplicaSet("normalizer-1", deployment, 1)
		reader := &StaticReader{
			Synced:         true,
			DeploymentList: []*appsv1.Deployment{deployment},
			ReplicaSetList: []*appsv1.ReplicaSet{rs},
			PodList: []*corev1.Pod{
				ownedPod("p1", rs, running()),
				ownedPod("p2", rs, running()),
			},
		}

		state, err := ObserveTarget(reader, "data-pipeline", "normalizer", now)
		if err != nil {
			t.Fatalf("ObserveTarget: %v", err)
		}
		if state.CurrentReplicas != 4 || state.ReadyReplicas != 4 {
			t.Errorf("replicas = %d/%d, want 4/4", state.ReadyReplicas, state.CurrentReplicas)
		}
		if state.RolloutInProgress {
			t.Error("no rollout is in progress")
		}
		if len(state.Pods) != 2 {
			t.Errorf("Pods = %d, want 2", len(state.Pods))
		}
		if state.PendingPods != 0 || state.UnhealthyPods != 0 {
			t.Errorf("PendingPods = %d, UnhealthyPods = %d, want 0/0", state.PendingPods, state.UnhealthyPods)
		}
	})

	// An unset spec.replicas defaults to 1, exactly as the API does. Reading it
	// as 0 would make the controller believe the workload was scaled to zero.
	t.Run("unset spec.replicas defaults to one", func(t *testing.T) {
		t.Parallel()

		deployment := newDeployment(4, atGeneration(1, 1))
		deployment.Spec.Replicas = nil

		reader := &StaticReader{Synced: true, DeploymentList: []*appsv1.Deployment{deployment}}

		state, err := ObserveTarget(reader, "data-pipeline", "normalizer", now)
		if err != nil {
			t.Fatalf("ObserveTarget: %v", err)
		}
		if state.CurrentReplicas != 1 {
			t.Errorf("CurrentReplicas = %d, want 1", state.CurrentReplicas)
		}
	})

	t.Run("rollout detection", func(t *testing.T) {
		t.Parallel()

		tests := []struct {
			name       string
			deployment *appsv1.Deployment
			want       bool
		}{
			{
				name:       "spec not yet observed",
				deployment: newDeployment(4, withStatus(4, 4, 4, 4), atGeneration(4, 3)),
				want:       true,
			},
			{
				name:       "observed but not all replicas updated",
				deployment: newDeployment(4, withStatus(4, 4, 4, 2), atGeneration(3, 3)),
				want:       true,
			},
			{
				name:       "settled",
				deployment: newDeployment(4, withStatus(4, 4, 4, 4), atGeneration(3, 3)),
				want:       false,
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				reader := &StaticReader{Synced: true, DeploymentList: []*appsv1.Deployment{tt.deployment}}

				state, err := ObserveTarget(reader, "data-pipeline", "normalizer", now)
				if err != nil {
					t.Fatalf("ObserveTarget: %v", err)
				}
				if state.RolloutInProgress != tt.want {
					t.Errorf("RolloutInProgress = %v, want %v", state.RolloutInProgress, tt.want)
				}
			})
		}
	})

	// DR-09: during a rollout, a doomed pod of the superseded ReplicaSet must
	// not be blamed on the healthy new one.
	t.Run("only the current ReplicaSet's pods are counted", func(t *testing.T) {
		t.Parallel()

		deployment := newDeployment(2, withStatus(2, 2, 2, 2), atGeneration(2, 2))
		old := newReplicaSet("normalizer-1", deployment, 1)
		current := newReplicaSet("normalizer-2", deployment, 2)

		reader := &StaticReader{
			Synced:         true,
			DeploymentList: []*appsv1.Deployment{deployment},
			ReplicaSetList: []*appsv1.ReplicaSet{old, current},
			PodList: []*corev1.Pod{
				ownedPod("old-broken", old, pending(now.Add(-time.Hour))),
				ownedPod("new-1", current, running()),
			},
		}

		state, err := ObserveTarget(reader, "data-pipeline", "normalizer", now)
		if err != nil {
			t.Fatalf("ObserveTarget: %v", err)
		}
		if state.CurrentReplicaSet == nil || state.CurrentReplicaSet.Name != "normalizer-2" {
			t.Fatalf("CurrentReplicaSet = %v, want normalizer-2", state.CurrentReplicaSet)
		}
		if len(state.Pods) != 1 || state.Pods[0].Name != "new-1" {
			t.Errorf("Pods = %v, want only new-1", podNames(state.Pods))
		}
		if state.PendingPods != 0 {
			t.Errorf("PendingPods = %d, want 0: the Pending pod belongs to the superseded ReplicaSet", state.PendingPods)
		}
	})

	// Ownership is matched on UID, so a ReplicaSet left behind by a
	// deleted-and-recreated Deployment of the same name is not adopted.
	t.Run("ownership is matched by UID, not name", func(t *testing.T) {
		t.Parallel()

		deployment := newDeployment(2, withStatus(2, 2, 2, 2), atGeneration(1, 1))

		impostorOwner := newDeployment(2, atGeneration(1, 1))
		impostorOwner.UID = "a-previous-incarnation"
		orphan := newReplicaSet("normalizer-old", impostorOwner, 1)

		reader := &StaticReader{
			Synced:         true,
			DeploymentList: []*appsv1.Deployment{deployment},
			ReplicaSetList: []*appsv1.ReplicaSet{orphan},
		}

		state, err := ObserveTarget(reader, "data-pipeline", "normalizer", now)
		if err != nil {
			t.Fatalf("ObserveTarget: %v", err)
		}
		if state.CurrentReplicaSet != nil {
			t.Errorf("CurrentReplicaSet = %q, want nil: it belongs to a previous Deployment UID", state.CurrentReplicaSet.Name)
		}
	})

	t.Run("the newest revision wins", func(t *testing.T) {
		t.Parallel()

		deployment := newDeployment(2, atGeneration(1, 1))
		first := newReplicaSet("rs-1", deployment, 1)
		tenth := newReplicaSet("rs-10", deployment, 10)
		second := newReplicaSet("rs-2", deployment, 2)

		// Revision 10 must beat revision 2, which string ordering would get
		// wrong.
		reader := &StaticReader{
			Synced:         true,
			DeploymentList: []*appsv1.Deployment{deployment},
			ReplicaSetList: []*appsv1.ReplicaSet{first, tenth, second},
		}

		state, err := ObserveTarget(reader, "data-pipeline", "normalizer", now)
		if err != nil {
			t.Fatalf("ObserveTarget: %v", err)
		}
		if state.CurrentReplicaSet.Name != "rs-10" {
			t.Errorf("CurrentReplicaSet = %q, want rs-10", state.CurrentReplicaSet.Name)
		}
	})

	// Before the Deployment controller sets the revision annotation, creation
	// time is the tie-break.
	t.Run("creation time breaks a revision tie", func(t *testing.T) {
		t.Parallel()

		deployment := newDeployment(2, atGeneration(1, 1))
		older := newReplicaSet("rs-older", deployment, -1)
		older.CreationTimestamp = metav1.NewTime(now.Add(-time.Hour))
		newer := newReplicaSet("rs-newer", deployment, -1)
		newer.CreationTimestamp = metav1.NewTime(now)

		reader := &StaticReader{
			Synced:         true,
			DeploymentList: []*appsv1.Deployment{deployment},
			ReplicaSetList: []*appsv1.ReplicaSet{older, newer},
		}

		state, err := ObserveTarget(reader, "data-pipeline", "normalizer", now)
		if err != nil {
			t.Fatalf("ObserveTarget: %v", err)
		}
		if state.CurrentReplicaSet.Name != "rs-newer" {
			t.Errorf("CurrentReplicaSet = %q, want rs-newer", state.CurrentReplicaSet.Name)
		}
	})

	t.Run("pod health classification", func(t *testing.T) {
		t.Parallel()

		deployment := newDeployment(5, withStatus(5, 2, 2, 5), atGeneration(1, 1))
		rs := newReplicaSet("normalizer-1", deployment, 1)

		reader := &StaticReader{
			Synced:         true,
			DeploymentList: []*appsv1.Deployment{deployment},
			ReplicaSetList: []*appsv1.ReplicaSet{rs},
			PodList: []*corev1.Pod{
				ownedPod("healthy", rs, running()),
				ownedPod("unschedulable", rs, pending(now.Add(-3*time.Minute))),
				ownedPod("unschedulable-longer", rs, pending(now.Add(-10*time.Minute))),
				ownedPod("crashlooping", rs, scheduledNotReady(now.Add(-7*time.Minute))),
				// Terminating is neither pending nor unhealthy; it is
				// finished. Counting it would make every scale-down look like
				// a health problem.
				ownedPod("going-away", rs, running(), deleting()),
			},
		}

		state, err := ObserveTarget(reader, "data-pipeline", "normalizer", now)
		if err != nil {
			t.Fatalf("ObserveTarget: %v", err)
		}
		if state.PendingPods != 2 {
			t.Errorf("PendingPods = %d, want 2", state.PendingPods)
		}
		if state.OldestPendingAge != 10*time.Minute {
			t.Errorf("OldestPendingAge = %v, want 10m (the oldest, not the newest)", state.OldestPendingAge)
		}
		if state.UnhealthyPods != 1 {
			t.Errorf("UnhealthyPods = %d, want 1", state.UnhealthyPods)
		}
		if state.UnhealthyPodAge != 7*time.Minute {
			t.Errorf("UnhealthyPodAge = %v, want 7m", state.UnhealthyPodAge)
		}
	})

	// A pod that started seconds ago is also "scheduled but not Ready". The
	// guard compares the age against podStartupTimeout, so the classification
	// must report the age rather than pre-judging it.
	t.Run("a freshly started pod is counted with its true age", func(t *testing.T) {
		t.Parallel()

		deployment := newDeployment(1, atGeneration(1, 1))
		rs := newReplicaSet("normalizer-1", deployment, 1)

		reader := &StaticReader{
			Synced:         true,
			DeploymentList: []*appsv1.Deployment{deployment},
			ReplicaSetList: []*appsv1.ReplicaSet{rs},
			PodList:        []*corev1.Pod{ownedPod("starting", rs, scheduledNotReady(now.Add(-2*time.Second)))},
		}

		state, err := ObserveTarget(reader, "data-pipeline", "normalizer", now)
		if err != nil {
			t.Fatalf("ObserveTarget: %v", err)
		}
		if state.UnhealthyPods != 1 || state.UnhealthyPodAge != 2*time.Second {
			t.Errorf("UnhealthyPods = %d at %v, want 1 at 2s", state.UnhealthyPods, state.UnhealthyPodAge)
		}
	})

	t.Run("a missing Deployment is an error", func(t *testing.T) {
		t.Parallel()

		if _, err := ObserveTarget(&StaticReader{Synced: true}, "data-pipeline", "normalizer", now); err == nil {
			t.Error("a missing target must be reported")
		}
	})

	t.Run("a reader failure propagates", func(t *testing.T) {
		t.Parallel()

		wantErr := errors.New("cache on fire")
		reader := &StaticReader{Synced: true, Err: wantErr}

		if _, err := ObserveTarget(reader, "data-pipeline", "normalizer", now); !errors.Is(err, wantErr) {
			t.Errorf("error = %v, want it to wrap %v", err, wantErr)
		}
	})
}

// --- HPA conflict detection ----------------------------------------------

func TestConflictingHPA(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		hpas []*autoscalingv2.HorizontalPodAutoscaler
		want string
	}{
		{name: "none", want: ""},
		{
			name: "targets our Deployment",
			hpas: []*autoscalingv2.HorizontalPodAutoscaler{hpa("competitor", "Deployment", "normalizer")},
			want: "competitor",
		},
		{
			name: "targets a different Deployment",
			hpas: []*autoscalingv2.HorizontalPodAutoscaler{hpa("other", "Deployment", "something-else")},
			want: "",
		},
		{
			// A StatefulSet of the same name is not our target.
			name: "targets a different kind",
			hpas: []*autoscalingv2.HorizontalPodAutoscaler{hpa("sts", "StatefulSet", "normalizer")},
			want: "",
		},
		{
			name: "one of several",
			hpas: []*autoscalingv2.HorizontalPodAutoscaler{
				hpa("other", "Deployment", "something-else"),
				hpa("competitor", "Deployment", "normalizer"),
			},
			want: "competitor",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got := ConflictingHPA(tt.hpas, "normalizer")
			switch {
			case tt.want == "":
				if got != nil {
					t.Errorf("ConflictingHPA = %q, want nil", got.Name)
				}
			case got == nil:
				t.Errorf("ConflictingHPA = nil, want %q", tt.want)
			case got.Name != tt.want:
				t.Errorf("ConflictingHPA = %q, want %q", got.Name, tt.want)
			}
		})
	}
}

func TestDescribeHPA(t *testing.T) {
	t.Parallel()

	if got := DescribeHPA(nil); got != "" {
		t.Errorf("DescribeHPA(nil) = %q, want empty", got)
	}
	if got := DescribeHPA(hpa("competitor", "Deployment", "normalizer")); got != "data-pipeline/competitor" {
		t.Errorf("DescribeHPA = %q, want data-pipeline/competitor", got)
	}
}

// --- MetricsReader --------------------------------------------------------

func TestMetricsReader_ListPodMetrics(t *testing.T) {
	t.Parallel()

	t.Run("returns the samples", func(t *testing.T) {
		t.Parallel()

		// Served from a reactor rather than the fake's object tracker: the
		// subject here is this package's adapter, and a reactor states the
		// API's response exactly.
		client := metricsfake.NewSimpleClientset()
		client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, &metricsv1beta1.PodMetricsList{
				Items: []metricsv1beta1.PodMetrics{{
					ObjectMeta: metav1.ObjectMeta{Name: "p1", Namespace: "data-pipeline"},
				}},
			}, nil
		})

		got, err := NewMetricsReader(client).ListPodMetrics(context.Background(), "data-pipeline")
		if err != nil {
			t.Fatalf("ListPodMetrics: %v", err)
		}
		if len(got) != 1 || got[0].Name != "p1" {
			t.Errorf("got %d samples, want 1 named p1", len(got))
		}
	})

	// metrics.k8s.io is an aggregated API that may simply not be installed.
	// That is an expected state, not a fault: the caller degrades to
	// pressure-only scaling and loses the utilization safety net.
	t.Run("an uninstalled metrics API is an empty result", func(t *testing.T) {
		t.Parallel()

		for _, tc := range []struct {
			name string
			err  error
		}{
			{name: "not found", err: apierrors.NewNotFound(schema.GroupResource{Group: "metrics.k8s.io", Resource: "pods"}, "")},
			{name: "service unavailable", err: apierrors.NewServiceUnavailable("no endpoints available for metrics-server")},
		} {
			t.Run(tc.name, func(t *testing.T) {
				t.Parallel()

				client := metricsfake.NewSimpleClientset()
				client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
					return true, nil, tc.err
				})

				got, err := NewMetricsReader(client).ListPodMetrics(context.Background(), "data-pipeline")
				if err != nil {
					t.Errorf("error = %v, want nil: an absent metrics API is expected, not fatal", err)
				}
				if got != nil {
					t.Errorf("got %v, want no samples", got)
				}
			})
		}
	})

	t.Run("a real failure is reported", func(t *testing.T) {
		t.Parallel()

		client := metricsfake.NewSimpleClientset()
		client.PrependReactor("list", "pods", func(k8stesting.Action) (bool, runtime.Object, error) {
			return true, nil, apierrors.NewForbidden(schema.GroupResource{Group: "metrics.k8s.io", Resource: "pods"}, "", errors.New("no permission"))
		})

		if _, err := NewMetricsReader(client).ListPodMetrics(context.Background(), "data-pipeline"); err == nil {
			t.Error("a Forbidden response must be reported: it means the RBAC is wrong, which is actionable")
		}
	})
}

// --- InformerReader -------------------------------------------------------

// The informer wiring is exercised against a fake clientset: the point is that
// the five caches sync and that each lister is bound to the resource it claims,
// which is a mistake no amount of unit-testing the callers would catch.
func TestInformerReader_SyncsAndReads(t *testing.T) {
	t.Parallel()

	deployment := newDeployment(3, withStatus(3, 3, 3, 3), atGeneration(1, 1))
	rs := newReplicaSet("normalizer-1", deployment, 1)

	core := corefake.NewSimpleClientset(
		node("w-1"),
		node("w-2"),
		ownedPod("ours", rs, running()),
		podIn("kube-system", "coredns"),
		deployment,
		rs,
		hpa("competitor", "Deployment", "normalizer"),
	)

	reader := NewInformerReader(&Clients{Core: core}, "data-pipeline")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := reader.Start(ctx, 30*time.Second); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if !reader.HasSynced() {
		t.Fatal("HasSynced must be true after a successful Start")
	}

	nodes, err := reader.Nodes()
	if err != nil {
		t.Fatalf("Nodes: %v", err)
	}
	if len(nodes) != 2 {
		t.Errorf("Nodes = %d, want 2", len(nodes))
	}

	// Cluster-scoped, deliberately: node budget is shared, so pods of other
	// namespaces must be visible.
	pods, err := reader.Pods()
	if err != nil {
		t.Fatalf("Pods: %v", err)
	}
	if len(pods) != 2 {
		t.Errorf("Pods = %d, want 2 including the kube-system pod", len(pods))
	}

	ours, err := reader.PodsInNamespace("data-pipeline")
	if err != nil {
		t.Fatalf("PodsInNamespace: %v", err)
	}
	if len(ours) != 1 || ours[0].Name != "ours" {
		t.Errorf("PodsInNamespace = %v, want [ours]", podNames(ours))
	}

	if _, err := reader.Deployment("data-pipeline", "normalizer"); err != nil {
		t.Errorf("Deployment: %v", err)
	}
	if sets, err := reader.ReplicaSets("data-pipeline"); err != nil || len(sets) != 1 {
		t.Errorf("ReplicaSets = %d, %v; want 1, nil", len(sets), err)
	}
	if hpas, err := reader.HorizontalPodAutoscalers("data-pipeline"); err != nil || len(hpas) != 1 {
		t.Errorf("HorizontalPodAutoscalers = %d, %v; want 1, nil", len(hpas), err)
	}
}

func TestInformerReader_MissingDeploymentIsAnError(t *testing.T) {
	t.Parallel()

	reader := NewInformerReader(&Clients{Core: corefake.NewSimpleClientset()}, "data-pipeline")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := reader.Start(ctx, 30*time.Second); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if _, err := reader.Deployment("data-pipeline", "normalizer"); err == nil {
		t.Error("a missing Deployment must be an error, not a nil object")
	}
}

// A sync timeout is a startup failure rather than a degraded mode: deciding
// from empty caches would compute the fit capacity of an apparently empty
// cluster, the most dangerous possible wrong answer for a resource estimator.
func TestInformerReader_SyncTimeoutIsAFailure(t *testing.T) {
	t.Parallel()

	core := corefake.NewSimpleClientset()
	core.PrependWatchReactor("*", func(k8stesting.Action) (bool, watch.Interface, error) {
		return true, nil, errors.New("watch refused")
	})
	core.PrependReactor("list", "*", func(k8stesting.Action) (bool, runtime.Object, error) {
		// Never succeeds, so the caches never sync.
		return true, nil, errors.New("list refused")
	})

	reader := NewInformerReader(&Clients{Core: core}, "data-pipeline")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	err := reader.Start(ctx, 100*time.Millisecond)
	if err == nil {
		t.Fatal("an unsynced cache must fail startup")
	}
	if !strings.Contains(err.Error(), "partial view") {
		t.Errorf("error should explain why this is fatal, got: %v", err)
	}
}

// --- StaticReader ---------------------------------------------------------

func TestStaticReader(t *testing.T) {
	t.Parallel()

	deployment := newDeployment(2, atGeneration(1, 1))
	reader := &StaticReader{
		Synced:         true,
		NodeList:       []*corev1.Node{node("w-1")},
		PodList:        []*corev1.Pod{podIn("data-pipeline", "ours"), podIn("other", "theirs")},
		DeploymentList: []*appsv1.Deployment{deployment},
		ReplicaSetList: []*appsv1.ReplicaSet{newReplicaSet("rs", deployment, 1), replicaSetIn("other", "theirs")},
		HPAList:        []*autoscalingv2.HorizontalPodAutoscaler{hpa("ours", "Deployment", "normalizer"), hpaIn("other", "theirs")},
	}

	if !reader.HasSynced() {
		t.Error("HasSynced should reflect the configured value")
	}
	if pods, _ := reader.PodsInNamespace("data-pipeline"); len(pods) != 1 {
		t.Errorf("PodsInNamespace = %d, want 1", len(pods))
	}
	if all, _ := reader.Pods(); len(all) != 2 {
		t.Errorf("Pods = %d, want 2", len(all))
	}
	if sets, _ := reader.ReplicaSets("data-pipeline"); len(sets) != 1 {
		t.Errorf("ReplicaSets = %d, want 1", len(sets))
	}
	if hpas, _ := reader.HorizontalPodAutoscalers("data-pipeline"); len(hpas) != 1 {
		t.Errorf("HorizontalPodAutoscalers = %d, want 1", len(hpas))
	}

	// Err exists so the ErrorAPIFailure path — the one that must never act in
	// either direction — is reachable in a test.
	reader.Err = errors.New("boom")
	for name, call := range map[string]func() error{
		"Nodes":           func() error { _, err := reader.Nodes(); return err },
		"Pods":            func() error { _, err := reader.Pods(); return err },
		"PodsInNamespace": func() error { _, err := reader.PodsInNamespace("data-pipeline"); return err },
		"Deployment":      func() error { _, err := reader.Deployment("data-pipeline", "normalizer"); return err },
		"ReplicaSets":     func() error { _, err := reader.ReplicaSets("data-pipeline"); return err },
		"HPAs":            func() error { _, err := reader.HorizontalPodAutoscalers("data-pipeline"); return err },
	} {
		if err := call(); !errors.Is(err, reader.Err) {
			t.Errorf("%s returned %v, want the injected error", name, err)
		}
	}
}

// StaticReader must satisfy the same interface the controller consumes,
// otherwise the offline tests would be testing a different contract.
func TestStaticReader_ImplementsClusterReader(t *testing.T) {
	t.Parallel()

	var _ ClusterReader = (*StaticReader)(nil)
	var _ ClusterReader = (*InformerReader)(nil)
}

// --- helpers ---------------------------------------------------------------

type countingRoundTripper struct {
	mu       sync.Mutex
	requests []string
}

func (c *countingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	c.mu.Lock()
	c.requests = append(c.requests, req.Method)
	c.mu.Unlock()

	return &http.Response{
		StatusCode: http.StatusOK,
		Body:       http.NoBody,
		Header:     http.Header{},
		Request:    req,
	}, nil
}

func (c *countingRoundTripper) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.requests)
}

func (c *countingRoundTripper) methods() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]string(nil), c.requests...)
}

// apiServerStub answers reads with empty-but-valid objects and records the
// method of every request that reaches it.
func apiServerStub(t *testing.T) (*httptest.Server, *countingRoundTripper) {
	t.Helper()

	seen := &countingRoundTripper{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		seen.mu.Lock()
		seen.requests = append(seen.requests, r.Method)
		seen.mu.Unlock()

		// An empty-but-valid body. No `kind` is set deliberately: the typed
		// client decodes into the object it asked for, and a `kind: Status`
		// body would be interpreted as an API error instead.
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprint(w, `{"metadata":{},"items":[]}`)
	}))
	t.Cleanup(server.Close)

	return server, seen
}

func clientsFor(t *testing.T, serverURL string) *Clients {
	t.Helper()

	cfg, err := NewReadOnlyRESTConfig(writeKubeconfig(t, serverURL))
	if err != nil {
		t.Fatalf("NewReadOnlyRESTConfig: %v", err)
	}
	clients, err := NewClients(cfg)
	if err != nil {
		t.Fatalf("NewClients: %v", err)
	}
	return clients
}

func writeKubeconfig(t *testing.T, serverURL string) string {
	t.Helper()

	body := fmt.Sprintf(`apiVersion: v1
kind: Config
current-context: test
clusters:
- name: test
  cluster:
    server: %s
contexts:
- name: test
  context:
    cluster: test
    user: test
users:
- name: test
  user: {}
`, serverURL)

	path := filepath.Join(t.TempDir(), "kubeconfig")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

type deploymentOpt func(*appsv1.Deployment)

func newDeployment(replicas int32, opts ...deploymentOpt) *appsv1.Deployment {
	d := &appsv1.Deployment{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "normalizer",
			Namespace: "data-pipeline",
			UID:       "deployment-uid",
		},
		Spec: appsv1.DeploymentSpec{Replicas: &replicas},
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

func withStatus(replicas, ready, available, updated int32) deploymentOpt {
	return func(d *appsv1.Deployment) {
		d.Status.Replicas = replicas
		d.Status.ReadyReplicas = ready
		d.Status.AvailableReplicas = available
		d.Status.UpdatedReplicas = updated
	}
}

func atGeneration(generation, observed int64) deploymentOpt {
	return func(d *appsv1.Deployment) {
		d.Generation = generation
		d.Status.ObservedGeneration = observed
	}
}

func newReplicaSet(name string, owner *appsv1.Deployment, revision int64) *appsv1.ReplicaSet {
	rs := &appsv1.ReplicaSet{
		ObjectMeta: metav1.ObjectMeta{
			Name:            name,
			Namespace:       owner.Namespace,
			UID:             types.UID(name + "-uid"),
			OwnerReferences: []metav1.OwnerReference{{UID: owner.UID, Kind: "Deployment", Name: owner.Name}},
		},
	}
	if revision >= 0 {
		rs.Annotations = map[string]string{revisionAnnotation: fmt.Sprint(revision)}
	}
	return rs
}

func replicaSetIn(namespace, name string) *appsv1.ReplicaSet {
	return &appsv1.ReplicaSet{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
}

type podOpt func(*corev1.Pod)

func ownedPod(name string, rs *appsv1.ReplicaSet, opts ...podOpt) *corev1.Pod {
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:              name,
			Namespace:         rs.Namespace,
			CreationTimestamp: metav1.NewTime(now.Add(-time.Hour)),
			OwnerReferences:   []metav1.OwnerReference{{UID: rs.UID, Kind: "ReplicaSet", Name: rs.Name}},
		},
	}
	for _, opt := range opts {
		opt(p)
	}
	return p
}

func podIn(namespace, name string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace},
		Status:     corev1.PodStatus{Phase: corev1.PodRunning},
	}
}

func running() podOpt {
	return func(p *corev1.Pod) {
		p.Spec.NodeName = "w-1"
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(now.Add(-time.Hour))},
			{Type: corev1.PodReady, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(now.Add(-time.Hour))},
		}
	}
}

func pending(since time.Time) podOpt {
	return func(p *corev1.Pod) {
		p.Status.Phase = corev1.PodPending
		p.CreationTimestamp = metav1.NewTime(since)
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionFalse, Reason: "Unschedulable", LastTransitionTime: metav1.NewTime(since)},
		}
	}
}

func scheduledNotReady(since time.Time) podOpt {
	return func(p *corev1.Pod) {
		p.Spec.NodeName = "w-1"
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{
			{Type: corev1.PodScheduled, Status: corev1.ConditionTrue, LastTransitionTime: metav1.NewTime(since)},
			{Type: corev1.PodReady, Status: corev1.ConditionFalse, LastTransitionTime: metav1.NewTime(since)},
		}
	}
}

func deleting() podOpt {
	return func(p *corev1.Pod) {
		ts := metav1.NewTime(now.Add(-time.Second))
		p.DeletionTimestamp = &ts
	}
}

func node(name string) *corev1.Node {
	return &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: name}}
}

func hpa(name, kind, target string) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "data-pipeline"},
		Spec: autoscalingv2.HorizontalPodAutoscalerSpec{
			ScaleTargetRef: autoscalingv2.CrossVersionObjectReference{Kind: kind, Name: target},
		},
	}
}

func hpaIn(namespace, name string) *autoscalingv2.HorizontalPodAutoscaler {
	return &autoscalingv2.HorizontalPodAutoscaler{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: namespace}}
}

func podNames(pods []*corev1.Pod) []string {
	names := make([]string, 0, len(pods))
	for _, pod := range pods {
		names = append(names, pod.Name)
	}
	return names
}

// scaleTo builds the Scale object P3 will submit, so that the write P1 must not
// be able to perform is attempted here in its real form.
func scaleTo(replicas int32) *autoscalingv1.Scale {
	return &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: "normalizer", Namespace: "data-pipeline"},
		Spec:       autoscalingv1.ScaleSpec{Replicas: replicas},
	}
}

func leaseNamed(name string) *coordinationv1.Lease {
	return &coordinationv1.Lease{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "kubescalesense"}}
}
