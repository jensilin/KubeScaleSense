package kubernetes

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/rest"
)

// The actuation client's tests. The claim being checked is narrow and precise:
// this client can set the replica count of one Deployment, and there is no
// request it can make that does anything else.

const (
	testNamespace  = "data-pipeline"
	testDeployment = "normalizer"
)

func TestScalePath_IsTheSubresourceNotTheObject(t *testing.T) {
	t.Parallel()

	got := ScalePath(testNamespace, testDeployment)
	want := "/apis/apps/v1/namespaces/data-pipeline/deployments/normalizer/scale"

	if got != want {
		t.Errorf("ScalePath = %q, want %q", got, want)
	}
}

// The transport is the layer that holds even if the code above it is wrong, so
// it is asserted method by method and path by path.
func TestScaleOnlyTransport_PermitsExactlyOneWritablePath(t *testing.T) {
	t.Parallel()

	const (
		scalePath      = "/apis/apps/v1/namespaces/data-pipeline/deployments/normalizer/scale"
		deploymentPath = "/apis/apps/v1/namespaces/data-pipeline/deployments/normalizer"
		otherScalePath = "/apis/apps/v1/namespaces/data-pipeline/deployments/nifi/scale"
		podPath        = "/api/v1/namespaces/data-pipeline/pods/normalizer-abc"
	)

	for _, tc := range []struct {
		name    string
		method  string
		path    string
		allowed bool
		why     string
	}{
		{"read the scale subresource", http.MethodGet, scalePath, true,
			"the precondition for a safe write is a live read"},
		{"read anything else", http.MethodGet, podPath, true,
			"reads cannot change anything, and client-go needs discovery"},
		{"discovery", http.MethodOptions, "/apis", true, "client-go negotiates the API version"},

		{"write the scale subresource", http.MethodPut, scalePath, true,
			"this is the one mutation the project performs"},
		{"patch the scale subresource", http.MethodPatch, scalePath, true,
			"the subresource's other legitimate write form"},

		{"write the Deployment itself", http.MethodPut, deploymentPath, false,
			"would be able to rewrite the pod template (FS-21)"},
		{"patch the Deployment itself", http.MethodPatch, deploymentPath, false,
			"would be able to rewrite the pod template (FS-21)"},
		{"scale a different Deployment", http.MethodPut, otherScalePath, false,
			"one target, fixed at wiring time (I-15)"},
		{"delete a pod", http.MethodDelete, podPath, false,
			"the controller never deletes pods (I-11)"},
		{"create anything", http.MethodPost, podPath, false,
			"there is nothing the actuator legitimately creates"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			base := &countingRoundTripper{}
			transport := scaleOnlyTransport{base: base, allowed: scalePath}

			request, err := http.NewRequestWithContext(context.Background(), tc.method, "https://api"+tc.path, nil)
			if err != nil {
				t.Fatalf("building the request: %v", err)
			}

			_, err = transport.RoundTrip(request)

			switch {
			case tc.allowed && err != nil:
				t.Errorf("%s %s was refused (%v), but it must be allowed: %s", tc.method, tc.path, err, tc.why)
			case tc.allowed && base.count() != 1:
				t.Errorf("%s %s did not reach the underlying transport", tc.method, tc.path)
			case !tc.allowed && err == nil:
				t.Errorf("%s %s was allowed through, but it must be refused: %s", tc.method, tc.path, tc.why)
			case !tc.allowed && !errors.Is(err, ErrNotScaleSubresource):
				t.Errorf("%s %s failed with %v, want ErrNotScaleSubresource", tc.method, tc.path, err)
			case !tc.allowed && base.count() != 0:
				t.Errorf("%s %s reached the underlying transport despite being refused", tc.method, tc.path)
			}
		})
	}
}

// The two transports must not be confused for one another. The observation
// client refuses every write, including to the scale path; the actuation client
// refuses every write except that one.
func TestTheTwoTransportsDifferOnlyOnTheScalePath(t *testing.T) {
	t.Parallel()

	scalePath := ScalePath(testNamespace, testDeployment)

	request, err := http.NewRequestWithContext(context.Background(), http.MethodPut, "https://api"+scalePath, nil)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}

	readOnly := readOnlyTransport{base: &countingRoundTripper{}}
	if _, err := readOnly.RoundTrip(request); !errors.Is(err, ErrReadOnly) {
		t.Errorf("the observation transport allowed a write to the scale path (%v); enabling actuation must "+
			"not relax the read path", err)
	}

	actuation := scaleOnlyTransport{base: &countingRoundTripper{}, allowed: scalePath}
	if _, err := actuation.RoundTrip(request); err != nil {
		t.Errorf("the actuation transport refused its own scale path: %v", err)
	}
}

// Whether the write is really conditional is the difference between optimistic
// concurrency and a blind overwrite, so it is asserted on the object that goes
// to the API server rather than inferred from the code.
func TestScaleWriter_SendsTheResourceVersionPrecondition(t *testing.T) {
	t.Parallel()

	scaler := &recordingScaler{replicas: 2, resourceVersion: "4077"}
	writer := &ScaleWriter{scaler: scaler, namespace: testNamespace, deployment: testDeployment}

	state, err := writer.Current(context.Background())
	if err != nil {
		t.Fatalf("Current: %v", err)
	}
	if state.Replicas != 2 || state.ResourceVersion != "4077" {
		t.Fatalf("Current = %+v, want 2 replicas at version 4077", state)
	}

	if err := writer.Write(context.Background(), 6, state.ResourceVersion); err != nil {
		t.Fatalf("Write: %v", err)
	}

	if scaler.updated == nil {
		t.Fatal("Write performed no update")
	}
	if got := scaler.updated.Spec.Replicas; got != 6 {
		t.Errorf("wrote %d replicas, want 6", got)
	}
	if got := scaler.updated.ResourceVersion; got != "4077" {
		t.Errorf("wrote resourceVersion %q, want %q: without it the update is unconditional (FR-05)",
			got, "4077")
	}
	if got := scaler.updated.Name; got != testDeployment {
		t.Errorf("wrote against %q, want %q", got, testDeployment)
	}
	if got := scaler.updated.Namespace; got != testNamespace {
		t.Errorf("wrote in namespace %q, want %q", got, testNamespace)
	}
}

// An empty precondition is refused rather than sent. client-go treats an empty
// resourceVersion as "overwrite whatever is there", which is the one thing
// FR-05 exists to prevent — and an actuator bug that lost the version would
// otherwise degrade silently into a blind write.
func TestScaleWriter_RefusesToWriteWithoutAPrecondition(t *testing.T) {
	t.Parallel()

	scaler := &recordingScaler{replicas: 2, resourceVersion: "1"}
	writer := &ScaleWriter{scaler: scaler, namespace: testNamespace, deployment: testDeployment}

	if err := writer.Write(context.Background(), 6, ""); err == nil {
		t.Fatal("Write accepted an empty resourceVersion")
	}
	if scaler.updated != nil {
		t.Errorf("an unconditional write reached the API: %+v", scaler.updated)
	}
}

// The Scale object carries replicas and nothing else. This is why the write
// cannot touch the pod template even by accident: there is no field for it on
// the object being sent.
func TestScaleWriter_TheWrittenObjectCarriesOnlyTheReplicaCount(t *testing.T) {
	t.Parallel()

	scaler := &recordingScaler{replicas: 2, resourceVersion: "1"}
	writer := &ScaleWriter{scaler: scaler, namespace: testNamespace, deployment: testDeployment}

	if err := writer.Write(context.Background(), 3, "1"); err != nil {
		t.Fatalf("Write: %v", err)
	}

	sent := scaler.updated
	if len(sent.Labels) != 0 || len(sent.Annotations) != 0 {
		t.Errorf("the scale object carries metadata the actuator did not intend to set: %+v", sent.ObjectMeta)
	}
	if len(sent.OwnerReferences) != 0 {
		t.Error("the scale object carries owner references")
	}
	// autoscalingv1.Scale has exactly one spec field. Asserted so that a future
	// change of write mechanism — to an unstructured patch, say — has to
	// restate the claim rather than inherit it.
	if sent.Spec.Replicas != 3 {
		t.Errorf("Spec.Replicas = %d, want 3", sent.Spec.Replicas)
	}
}

// Errors must arrive at the caller in a form the standard helpers recognise,
// because the actuator's whole failure policy is a switch over them.
func TestScaleWriter_PreservesAPIErrorClassification(t *testing.T) {
	t.Parallel()

	deployments := schema.GroupResource{Group: "apps", Resource: "deployments"}

	for _, tc := range []struct {
		name  string
		err   error
		check func(error) bool
	}{
		{"conflict", apierrors.NewConflict(deployments, testDeployment, errors.New("modified")),
			apierrors.IsConflict},
		{"forbidden", apierrors.NewForbidden(deployments, testDeployment, errors.New("no scale verb")),
			apierrors.IsForbidden},
		{"not found", apierrors.NewNotFound(deployments, testDeployment), apierrors.IsNotFound},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			scaler := &recordingScaler{replicas: 2, resourceVersion: "1", updateErr: tc.err}
			writer := &ScaleWriter{scaler: scaler, namespace: testNamespace, deployment: testDeployment}

			err := writer.Write(context.Background(), 3, "1")
			if err == nil {
				t.Fatal("Write succeeded despite the injected error")
			}
			if !tc.check(err) {
				t.Errorf("the wrapped error is no longer recognisable as %s: %v", tc.name, err)
			}
		})
	}
}

func TestNewScaleRESTConfig_InstallsTheGuardAndTheUserAgent(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	kubeconfig := writeKubeconfig(t, server.URL)

	cfg, err := NewScaleRESTConfig(kubeconfig, testNamespace, testDeployment)
	if err != nil {
		t.Fatalf("NewScaleRESTConfig: %v", err)
	}

	if cfg.UserAgent != scaleUserAgent {
		t.Errorf("UserAgent = %q, want %q so that a replica change is attributable in the audit log",
			cfg.UserAgent, scaleUserAgent)
	}

	transport, err := rest.TransportFor(cfg)
	if err != nil {
		t.Fatalf("TransportFor: %v", err)
	}

	// A write to a path outside the target must be refused by the transport the
	// configuration actually produces, not merely by the type asserted above.
	request, err := http.NewRequestWithContext(context.Background(), http.MethodDelete,
		server.URL+"/api/v1/namespaces/data-pipeline/pods/normalizer-abc", nil)
	if err != nil {
		t.Fatalf("building the request: %v", err)
	}
	if _, err := transport.RoundTrip(request); !errors.Is(err, ErrNotScaleSubresource) {
		t.Errorf("the configured transport allowed a pod deletion: %v", err)
	}
}

// --- helpers --------------------------------------------------------------

// recordingScaler stands in for client-go's DeploymentInterface, keeping the
// object that was sent so the test can assert on it.
type recordingScaler struct {
	replicas        int32
	resourceVersion string

	getErr    error
	updateErr error

	updated *autoscalingv1.Scale
}

func (r *recordingScaler) GetScale(_ context.Context, name string, _ metav1.GetOptions) (*autoscalingv1.Scale, error) {
	if r.getErr != nil {
		return nil, r.getErr
	}
	return &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{Name: name, ResourceVersion: r.resourceVersion},
		Spec:       autoscalingv1.ScaleSpec{Replicas: r.replicas},
	}, nil
}

func (r *recordingScaler) UpdateScale(
	_ context.Context, _ string, scale *autoscalingv1.Scale, _ metav1.UpdateOptions,
) (*autoscalingv1.Scale, error) {
	if r.updateErr != nil {
		return nil, r.updateErr
	}
	r.updated = scale
	r.replicas = scale.Spec.Replicas
	return scale, nil
}
