package kubernetes

import (
	"context"
	"fmt"
	"net/http"

	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// scaleUserAgent identifies the one client in this binary that can write, so
// that an audit log entry for a replica change names the thing that made it.
// The observation client keeps its own agent, which makes the two separable in
// the API server's logs: reads come from the observer, and every write in the
// audit trail should carry this agent and no other.
const scaleUserAgent = "kubescalesense/p3-actuator"

// ErrNotScaleSubresource is returned when a mutating request targets anything
// other than the configured target's scale subresource.
//
// Like ErrReadOnly it is a value rather than a message, so the safety test can
// assert on identity and keep passing only while the guard itself is present.
var ErrNotScaleSubresource = fmt.Errorf(
	"kubescalesense: the actuation client may write only the configured target's scale subresource")

// ScalePath returns the API path of a Deployment's scale subresource.
//
// Exported because it is the exact string the transport guard compares against,
// and a guard whose expected value is computed in two places is a guard with a
// way of disagreeing with itself.
func ScalePath(namespace, deployment string) string {
	return fmt.Sprintf("/apis/apps/v1/namespaces/%s/deployments/%s/scale", namespace, deployment)
}

// NewScaleRESTConfig builds the REST configuration for the actuation client.
//
// P1 and P2 installed a transport that refused every mutating method, and the
// arrival of a write path is the moment that guarantee could quietly become
// "the client can write anything". It does not: this transport permits exactly
// two mutating methods against exactly one URL path, so the blast radius of an
// actuation bug is bounded by the transport rather than by the correctness of
// the code above it.
//
// Reads stay on the read-only client. Nothing in the observation path is routed
// through here, which is why a bug in the reconcile loop cannot turn an
// observation into a write.
func NewScaleRESTConfig(kubeconfigPath, namespace, deployment string) (*rest.Config, error) {
	cfg, err := restConfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}

	cfg.UserAgent = scaleUserAgent

	allowed := ScalePath(namespace, deployment)
	cfg.Wrap(func(base http.RoundTripper) http.RoundTripper {
		return scaleOnlyTransport{base: base, allowed: allowed}
	})

	return cfg, nil
}

// scaleOnlyTransport permits reads anywhere and writes to one path.
//
// GET, HEAD, and OPTIONS are allowed unrestricted because client-go needs them
// for discovery and none of them can change anything. PUT and PATCH are the two
// methods the scale subresource accepts, and they are allowed only against the
// configured target. POST and DELETE are refused outright: there is nothing the
// actuator legitimately creates or deletes, and the permission to delete pods is
// one the controller deliberately does not hold (I-11).
type scaleOnlyTransport struct {
	base    http.RoundTripper
	allowed string
}

// RoundTrip implements http.RoundTripper.
func (t scaleOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return t.base.RoundTrip(req)

	case http.MethodPut, http.MethodPatch:
		if req.URL.Path == t.allowed {
			return t.base.RoundTrip(req)
		}
		return nil, fmt.Errorf("%w (attempted %s %s; the only writable path is %s)",
			ErrNotScaleSubresource, req.Method, req.URL.Path, t.allowed)

	default:
		return nil, fmt.Errorf("%w (attempted %s %s)", ErrNotScaleSubresource, req.Method, req.URL.Path)
	}
}

// ScaleState is the target's replica count together with the version it was
// read at.
//
// The two travel together on purpose. A replica count without the version it
// was observed at cannot be written back safely, and making the pair the unit of
// currency means a caller cannot accidentally write a decision computed from one
// version against a cluster that has moved to another (FR-05).
type ScaleState struct {
	Replicas        int32
	ResourceVersion string
}

// deploymentScaler is the slice of client-go's DeploymentInterface that the
// writer uses: two calls, both on the scale subresource.
//
// Narrowed to an interface rather than holding the clientset because the full
// DeploymentInterface also carries Update, Patch, and Delete for the Deployment
// itself — including the pod template. Nothing here can reach them.
type deploymentScaler interface {
	GetScale(ctx context.Context, deploymentName string, opts metav1.GetOptions) (*autoscalingv1.Scale, error)
	UpdateScale(ctx context.Context, deploymentName string, scale *autoscalingv1.Scale,
		opts metav1.UpdateOptions) (*autoscalingv1.Scale, error)
}

// ScaleWriter is the only type in this repository that can change cluster state.
//
// It is deliberately small and deliberately alone. Everything it can do is
// visible in two methods: read the replica count, and set the replica count of
// one named Deployment. It cannot touch the pod template, create or delete a
// pod, or address any other object — not because the code above it is careful,
// but because there is no method here that would express it.
type ScaleWriter struct {
	scaler     deploymentScaler
	namespace  string
	deployment string
}

// NewScaleWriter binds a writer to one Deployment.
//
// The namespace and name are fixed at construction, so "which Deployment does
// this scale?" is answered once, at wiring time, rather than by every call site.
// A writer built for one target cannot be pointed at another.
func NewScaleWriter(client kubernetes.Interface, namespace, deployment string) *ScaleWriter {
	return &ScaleWriter{
		scaler:     client.AppsV1().Deployments(namespace),
		namespace:  namespace,
		deployment: deployment,
	}
}

// Target names the Deployment this writer is bound to, for logs and errors.
func (w *ScaleWriter) Target() string { return w.namespace + "/" + w.deployment }

// Current reads the target's live replica count and the version it was read at.
//
// Deliberately a live read rather than an informer lookup. The cache is the
// right source for deciding — it is cheap and a few seconds of lag does not
// change what the right replica count is — but it is the wrong source for the
// precondition on a write, because a stale resourceVersion is precisely the
// thing the precondition exists to catch.
func (w *ScaleWriter) Current(ctx context.Context) (ScaleState, error) {
	scale, err := w.scaler.GetScale(ctx, w.deployment, metav1.GetOptions{})
	if err != nil {
		return ScaleState{}, fmt.Errorf("reading the scale subresource of %s: %w", w.Target(), err)
	}
	return ScaleState{
		Replicas:        scale.Spec.Replicas,
		ResourceVersion: scale.ResourceVersion,
	}, nil
}

// Write sets the replica count, conditional on resourceVersion.
//
// The version is required rather than optional. client-go treats an empty
// resourceVersion as "overwrite whatever is there", which is exactly the
// unconditional write that FR-05 forbids — and an optional precondition is one
// that a future caller will omit by accident, on the one code path where it
// matters most.
func (w *ScaleWriter) Write(ctx context.Context, replicas int32, resourceVersion string) error {
	if resourceVersion == "" {
		return fmt.Errorf(
			"refusing to set the replica count of %s without a resourceVersion precondition: "+
				"an unconditional write would silently overwrite a newer value (FR-05)", w.Target())
	}

	scale := &autoscalingv1.Scale{
		ObjectMeta: metav1.ObjectMeta{
			Name:            w.deployment,
			Namespace:       w.namespace,
			ResourceVersion: resourceVersion,
		},
		Spec: autoscalingv1.ScaleSpec{Replicas: replicas},
	}

	// The one mutating client-go call in the repository. It addresses
	// deployments/scale, so it can change spec.replicas and nothing else: the
	// pod template is not part of this object and cannot be reached through it
	// (FS-21).
	if _, err := w.scaler.UpdateScale(ctx, w.deployment, scale, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("setting the replica count of %s to %d: %w", w.Target(), replicas, err)
	}
	return nil
}
