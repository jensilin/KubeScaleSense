package kubernetes

import (
	"context"
	"fmt"
	"net/http"
	"os"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	metricsv1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metricsclient "k8s.io/metrics/pkg/client/clientset/versioned"

	"k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// userAgent identifies the controller in API server audit logs. Worth setting
// explicitly: "who scaled my Deployment?" is the first question an operator
// asks, and the default Go user agent answers it with the binary name only.
const userAgent = "kubescalesense/p1-observer"

// ErrReadOnly is returned when something attempts a mutating request.
//
// It is a distinct error value rather than a string so that the safety test can
// assert on identity: a test that matched on message text would pass after
// someone reworded the message while removing the guard.
var ErrReadOnly = fmt.Errorf("kubescalesense: the Phase 1 Kubernetes client is read-only and refuses mutating requests")

// NewReadOnlyRESTConfig builds a REST configuration whose transport physically
// cannot mutate cluster state.
//
// Resolution order, chosen so that the same binary works in a Pod and on a
// laptop without a configuration switch:
//
//	explicit path   the -kubeconfig flag, for local observation runs
//	in-cluster      the projected service account token, when running as a Pod
//	default rules   $KUBECONFIG or ~/.kube/config
func NewReadOnlyRESTConfig(kubeconfigPath string) (*rest.Config, error) {
	cfg, err := restConfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}

	cfg.UserAgent = userAgent

	// The guard is installed on the transport rather than checked at each call
	// site, because a call site can be forgotten and a transport cannot be
	// bypassed by adding new code above it.
	cfg.Wrap(func(base http.RoundTripper) http.RoundTripper {
		return readOnlyTransport{base: base}
	})

	return cfg, nil
}

func restConfig(kubeconfigPath string) (*rest.Config, error) {
	if kubeconfigPath != "" {
		cfg, err := clientcmd.BuildConfigFromFlags("", kubeconfigPath)
		if err != nil {
			return nil, fmt.Errorf("loading kubeconfig %q: %w", kubeconfigPath, err)
		}
		return cfg, nil
	}

	if inCluster() {
		cfg, err := rest.InClusterConfig()
		if err != nil {
			return nil, fmt.Errorf("loading in-cluster configuration: %w", err)
		}
		return cfg, nil
	}

	rules := clientcmd.NewDefaultClientConfigLoadingRules()
	cfg, err := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(rules, &clientcmd.ConfigOverrides{}).ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("loading kubeconfig from the default rules (%s): %w", rules.GetLoadingPrecedence(), err)
	}
	return cfg, nil
}

func inCluster() bool {
	_, err := os.Stat("/var/run/secrets/kubernetes.io/serviceaccount/token")
	return err == nil
}

// readOnlyTransport refuses any HTTP method that could change cluster state.
//
// GET covers everything Phase 1 needs, including watches — a watch is a GET
// with ?watch=true. HEAD and OPTIONS are permitted because client-go uses them
// for discovery and neither can mutate.
type readOnlyTransport struct {
	base http.RoundTripper
}

// RoundTrip implements http.RoundTripper.
func (t readOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	switch req.Method {
	case http.MethodGet, http.MethodHead, http.MethodOptions:
		return t.base.RoundTrip(req)
	default:
		return nil, fmt.Errorf("%w (attempted %s %s)", ErrReadOnly, req.Method, req.URL.Path)
	}
}

// Clients bundles the two clientsets the observer needs.
//
// They are kept unexported inside this package's constructors and never handed
// to the controller, which receives only the ClusterReader interface.
type Clients struct {
	Core    kubernetes.Interface
	Metrics metricsclient.Interface
}

// NewClients builds both clientsets from a read-only REST configuration.
func NewClients(cfg *rest.Config) (*Clients, error) {
	core, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building the Kubernetes clientset: %w", err)
	}
	metrics, err := metricsclient.NewForConfig(cfg)
	if err != nil {
		return nil, fmt.Errorf("building the metrics.k8s.io clientset: %w", err)
	}
	return &Clients{Core: core, Metrics: metrics}, nil
}

// MetricsReader adapts the metrics clientset to the narrow lister interface
// internal/metrics consumes.
type MetricsReader struct {
	client metricsclient.Interface
}

// NewMetricsReader wires a reader to the metrics clientset.
func NewMetricsReader(client metricsclient.Interface) *MetricsReader {
	return &MetricsReader{client: client}
}

// ListPodMetrics reads the current pod utilization samples for a namespace.
//
// metrics.k8s.io is an aggregated API that may simply not be installed. That is
// an expected state rather than a fault: the caller degrades to pressure-only
// scaling and loses the utilization safety net, which is why the not-found case
// is reported as an empty result rather than an error.
func (m *MetricsReader) ListPodMetrics(ctx context.Context, namespace string) ([]metricsv1beta1.PodMetrics, error) {
	list, err := m.client.MetricsV1beta1().PodMetricses(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		if errors.IsNotFound(err) || errors.IsServiceUnavailable(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("listing pod metrics in namespace %q: %w", namespace, err)
	}
	return list.Items, nil
}
