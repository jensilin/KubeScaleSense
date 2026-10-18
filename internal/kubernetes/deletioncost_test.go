package kubernetes

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// The pod client's tests. RBAC grants `patch` on pods, which on its own would
// permit relabelling a pod out of its ReplicaSet; the claim here is that the
// transport narrows that to one annotation, whatever the code above it sends.

func TestDeletionCostTransport_PermitsOnlyTheAnnotationPatch(t *testing.T) {
	t.Parallel()

	const (
		merge     = string(types.MergePatchType)
		strategic = string(types.StrategicMergePatchType)
	)
	pod := PodsPath(testNamespace) + "/normalizer-abc"
	valid := `{"metadata":{"annotations":{"controller.kubernetes.io/pod-deletion-cost":"7"}}}`

	for _, tc := range []struct {
		name        string
		method      string
		path        string
		contentType string
		body        string
		allowed     bool
		why         string
	}{
		{"set the deletion cost", http.MethodPatch, pod, merge, valid, true, "the one write this client exists for"},
		{"set a negative cost", http.MethodPatch, pod, merge,
			`{"metadata":{"annotations":{"controller.kubernetes.io/pod-deletion-cost":"-3"}}}`, true,
			"the annotation is a signed 32-bit integer"},

		{"relabel the pod", http.MethodPatch, pod, merge, `{"metadata":{"labels":{"app":"other"}}}`, false,
			"a pod relabelled out of its ReplicaSet is replaced — a replica change through the back door"},
		{"set the cost and a label", http.MethodPatch, pod, merge,
			`{"metadata":{"labels":{"app":"x"},"annotations":{"controller.kubernetes.io/pod-deletion-cost":"1"}}}`,
			false, "nothing may ride along with the annotation"},
		{"set another annotation", http.MethodPatch, pod, merge,
			`{"metadata":{"annotations":{"controller.kubernetes.io/pod-deletion-cost":"1","other":"x"}}}`,
			false, "one annotation"},
		{"remove the cost", http.MethodPatch, pod, merge,
			`{"metadata":{"annotations":{"controller.kubernetes.io/pod-deletion-cost":null}}}`, false,
			"the client sets the cost, it never removes it"},
		{"set a non-integer cost", http.MethodPatch, pod, merge,
			`{"metadata":{"annotations":{"controller.kubernetes.io/pod-deletion-cost":"cheap"}}}`, false,
			"the ReplicaSet controller ignores a malformed cost, silently"},
		{"patch the spec", http.MethodPatch, pod, merge, `{"spec":{"activeDeadlineSeconds":1}}`, false,
			"would kill the pod"},
		{"a strategic merge patch", http.MethodPatch, pod, strategic, valid, false,
			"strategic merge has directives ($patch, $retainKeys) a body check would have to understand"},
		{"patch a subresource", http.MethodPatch, pod + "/status", merge, valid, false, "no subresources"},
		{"patch a pod elsewhere", http.MethodPatch, PodsPath("kube-system") + "/coredns", merge, valid, false,
			"the target's namespace only"},
		{"patch the collection", http.MethodPatch, PodsPath(testNamespace), merge, valid, false, "one pod at a time"},
		{"read a pod", http.MethodGet, pod, "", "", false, "this client is not an observer"},
		{"delete a pod", http.MethodDelete, pod, "", "", false, "the controller never deletes pods (I-11)"},
		{"evict a pod", http.MethodPost, pod + "/eviction", "application/json", "{}", false, "nor evicts them"},
		{"replace a pod", http.MethodPut, pod, "application/json", "{}", false, "PATCH only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			base := &bodyRecorder{}
			transport := deletionCostTransport{base: base, collection: PodsPath(testNamespace) + "/"}

			var body io.Reader
			if tc.body != "" {
				body = strings.NewReader(tc.body)
			}
			request, err := http.NewRequestWithContext(context.Background(), tc.method, "https://api"+tc.path, body)
			if err != nil {
				t.Fatalf("building the request: %v", err)
			}
			if tc.contentType != "" {
				request.Header.Set("Content-Type", tc.contentType)
			}

			_, err = transport.RoundTrip(request)

			switch {
			case tc.allowed && err != nil:
				t.Errorf("refused (%v), but it must be allowed: %s", err, tc.why)
			case tc.allowed && base.body() != tc.body:
				t.Errorf("forwarded body %q, want the original %q", base.body(), tc.body)
			case !tc.allowed && !errors.Is(err, ErrNotDeletionCostPatch):
				t.Errorf("returned %v, want ErrNotDeletionCostPatch: %s", err, tc.why)
			case !tc.allowed && base.calls() != 0:
				t.Error("reached the underlying transport despite being refused")
			}
		})
	}
}

// End to end through the configuration main builds.
func TestDeletionCostWriter_SendsTheOnePermittedPatch(t *testing.T) {
	t.Parallel()

	var (
		mu       sync.Mutex
		received []string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		received = append(received, r.Method+" "+r.URL.Path+" "+r.Header.Get("Content-Type")+" "+string(body))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"apiVersion":"v1","kind":"Pod","metadata":{"name":"normalizer-abc"}}`))
	}))
	defer server.Close()

	cfg, err := NewDeletionCostRESTConfig(writeKubeconfig(t, server.URL), testNamespace)
	if err != nil {
		t.Fatalf("NewDeletionCostRESTConfig: %v", err)
	}
	if cfg.UserAgent != deletionCostUserAgent {
		t.Errorf("UserAgent = %q, want %q", cfg.UserAgent, deletionCostUserAgent)
	}
	cfg.ContentType = "application/json"
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("NewForConfig: %v", err)
	}

	if err := NewDeletionCostWriter(client, testNamespace).SetDeletionCost(context.Background(), "normalizer-abc", 42); err != nil {
		t.Fatalf("SetDeletionCost: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := "PATCH /api/v1/namespaces/data-pipeline/pods/normalizer-abc application/merge-patch+json " +
		`{"metadata":{"annotations":{"controller.kubernetes.io/pod-deletion-cost":"42"}}}`
	if len(received) != 1 || received[0] != want {
		t.Errorf("the API server received %q, want exactly [%q]", received, want)
	}
}

func TestDeletionCosts(t *testing.T) {
	t.Parallel()

	ready := func(name, ip string, annotations map[string]string) *corev1.Pod {
		return &corev1.Pod{
			ObjectMeta: metav1.ObjectMeta{Name: name, Annotations: annotations},
			Status: corev1.PodStatus{
				PodIP:      ip,
				Conditions: []corev1.PodCondition{{Type: corev1.PodReady, Status: corev1.ConditionTrue}},
			},
		}
	}
	notReady := ready("starting", "10.0.0.9", nil)
	notReady.Status.Conditions[0].Status = corev1.ConditionFalse
	terminating := ready("leaving", "10.0.0.8", nil)
	terminating.DeletionTimestamp = &metav1.Time{}

	t.Run("each Ready pod costs its own work", func(t *testing.T) {
		t.Parallel()

		pods := []*corev1.Pod{
			ready("busy", "10.0.0.1", nil),
			ready("idle", "10.0.0.2", map[string]string{DeletionCostAnnotation: "0"}),
			notReady, terminating,
		}
		got, err := DeletionCosts(pods, map[string]int64{"10.0.0.1": 30, "10.0.0.2": 0})
		if err != nil {
			t.Fatalf("DeletionCosts: %v", err)
		}
		want := []PodCost{{Pod: "busy", Cost: 30, Changed: true}, {Pod: "idle", Cost: 0, Changed: false}}
		if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
			t.Errorf("got %+v, want %+v — not-Ready and terminating pods are the ReplicaSet controller's "+
				"first choice regardless, so they are skipped", got, want)
		}
	})

	t.Run("work beyond int32 is clamped, not wrapped", func(t *testing.T) {
		t.Parallel()

		got, err := DeletionCosts([]*corev1.Pod{ready("huge", "10.0.0.1", nil)}, map[string]int64{"10.0.0.1": 1 << 40})
		if err != nil || got[0].Cost != 1<<31-1 {
			t.Errorf("got %+v, %v; a wrapped value would make the busiest pod the cheapest", got, err)
		}
	})

	for _, tc := range []struct {
		name string
		pods []*corev1.Pod
		work map[string]int64
	}{
		{"a Ready pod was not scraped", []*corev1.Pod{ready("a", "10.0.0.1", nil), ready("b", "10.0.0.2", nil)},
			map[string]int64{"10.0.0.1": 1}},
		{"a Ready pod has no IP", []*corev1.Pod{ready("a", "", nil)}, map[string]int64{"": 1}},
		{"the source has no per-pod view", []*corev1.Pod{ready("a", "10.0.0.1", nil)}, nil},
		{"no pod is Ready", []*corev1.Pod{notReady}, map[string]int64{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			if got, err := DeletionCosts(tc.pods, tc.work); err == nil {
				t.Errorf("got %+v; an incomplete view must refresh nothing", got)
			}
		})
	}
}

// bodyRecorder is a base transport that keeps what reached it.
type bodyRecorder struct {
	mu       sync.Mutex
	n        int
	lastBody string
}

func (b *bodyRecorder) RoundTrip(req *http.Request) (*http.Response, error) {
	var body []byte
	if req.Body != nil {
		body, _ = io.ReadAll(req.Body)
	}
	b.mu.Lock()
	b.n++
	b.lastBody = string(body)
	b.mu.Unlock()
	return &http.Response{StatusCode: http.StatusOK, Body: http.NoBody, Request: req}, nil
}

func (b *bodyRecorder) calls() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.n
}

func (b *bodyRecorder) body() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.lastBody
}
