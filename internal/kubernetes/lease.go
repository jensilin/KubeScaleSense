package kubernetes

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/apimachinery/pkg/util/validation"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
)

// leaseUserAgent is the fourth write client's identity in the audit log.
const leaseUserAgent = "kubescalesense/p4-lease"

// leasePrefix names the lease after the target rather than after this
// installation. Two controllers pointed at the same Deployment then contend for
// one lease even when they were installed separately, so "one writer per
// target" holds across installations and not only across replicas of one.
const leasePrefix = "kubescalesense-"

// maxLeaseBodyBytes bounds the create body the transport reads to validate. A
// Lease the elector creates is a few hundred bytes.
const maxLeaseBodyBytes = 16 << 10

// The lease timing kube-controller-manager and kube-scheduler ship with. A
// leader that cannot renew for RenewDeadline stops leading, and a successor
// can acquire only once LeaseDuration has passed since the last renewal, so
// the two are never leading at once unless clocks drift by more than the gap.
const (
	DefaultLeaseDuration = 15 * time.Second
	DefaultRenewDeadline = 10 * time.Second
	DefaultRetryPeriod   = 2 * time.Second
)

// ErrNotLeaseAccess is returned when the lease client attempts anything other
// than reading, creating, or renewing the controller's own lease.
var ErrNotLeaseAccess = fmt.Errorf(
	"kubescalesense: the lease client may only read, create, and renew the controller's own lease")

// LeaseName returns the name of the lease that elects the writer for a target.
func LeaseName(deployment string) string {
	return leasePrefix + deployment
}

// LeasesPath returns the API path of the leases collection in a namespace.
func LeasesPath(namespace string) string {
	return fmt.Sprintf("/apis/coordination.k8s.io/v1/namespaces/%s/leases", namespace)
}

// LeasePath returns the API path of one lease.
func LeasePath(namespace, name string) string {
	return LeasesPath(namespace) + "/" + name
}

// NewLeaseRESTConfig builds the REST configuration for the lease client.
//
// RBAC cannot narrow `create` to one object name, so the Role that lets this
// controller create its own lease equally lets it create any other lease in the
// namespace — including one another component elects with, which it could then
// hold and starve. The bound therefore lives in the transport, as it does for
// the pod client: reads and renewals of the one lease path, and a create only
// when the body names that lease.
//
// JSON is pinned so that the create body can be read. A protobuf body would be
// refused rather than waved through.
func NewLeaseRESTConfig(kubeconfigPath, namespace, name string) (*rest.Config, error) {
	cfg, err := restConfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}

	cfg.UserAgent = leaseUserAgent
	cfg.ContentType = "application/json"

	collection, item := LeasesPath(namespace), LeasePath(namespace, name)
	cfg.Wrap(func(base http.RoundTripper) http.RoundTripper {
		return leaseTransport{base: base, collection: collection, item: item, name: name}
	})

	return cfg, nil
}

// leaseTransport permits GET and PUT of one lease, and a POST that creates it.
// No list, no watch, no delete, and nothing outside coordination.k8s.io.
type leaseTransport struct {
	base       http.RoundTripper
	collection string
	item       string
	name       string
}

// RoundTrip implements http.RoundTripper.
func (t leaseTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	refuse := func(why string) (*http.Response, error) {
		return nil, fmt.Errorf("%w (attempted %s %s: %s)", ErrNotLeaseAccess, req.Method, req.URL.Path, why)
	}

	switch {
	case req.URL.Path == t.item && (req.Method == http.MethodGet || req.Method == http.MethodPut):
		// The API server rejects a PUT whose body names a different object than
		// its path, so the path is the whole bound for a renewal.
		return t.base.RoundTrip(req)
	case req.URL.Path == t.collection && req.Method == http.MethodPost:
	default:
		return refuse("only GET and PUT of " + t.item + ", and POST of that lease to " + t.collection + ", are permitted")
	}

	if mediaType := req.Header.Get("Content-Type"); mediaType != "application/json" {
		return refuse("the lease must be sent as JSON so its name can be checked, got " + mediaType)
	}
	if req.Body == nil {
		return refuse("the create has no body")
	}
	body, err := io.ReadAll(io.LimitReader(req.Body, maxLeaseBodyBytes+1))
	_ = req.Body.Close()
	if err != nil {
		return nil, fmt.Errorf("reading the lease body to validate it: %w", err)
	}
	if len(body) > maxLeaseBodyBytes {
		return refuse("the body is too large to be a lease")
	}

	var lease struct {
		Metadata struct {
			Name         string `json:"name"`
			GenerateName string `json:"generateName"`
		} `json:"metadata"`
	}
	if err := json.Unmarshal(body, &lease); err != nil {
		return refuse("the body is not a JSON object: " + err.Error())
	}
	if lease.Metadata.Name != t.name || lease.Metadata.GenerateName != "" {
		return refuse(fmt.Sprintf("only the lease %q may be created, the body names %q", t.name, lease.Metadata.Name))
	}

	forwarded := req.Clone(req.Context())
	forwarded.Body = io.NopCloser(bytes.NewReader(body))
	forwarded.ContentLength = int64(len(body))
	forwarded.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	return t.base.RoundTrip(forwarded)
}

// ElectionConfig configures an Election. The durations default to the
// kube-controller-manager values when zero.
type ElectionConfig struct {
	Client    kubernetes.Interface
	Namespace string
	Name      string
	Identity  string

	LeaseDuration time.Duration
	RenewDeadline time.Duration
	RetryPeriod   time.Duration

	// OnStartedLeading runs in its own goroutine once the lease is held; ctx
	// is cancelled the moment it is not.
	OnStartedLeading func(ctx context.Context)
	// OnStoppedLeading runs when Run returns, whether or not this process ever
	// led.
	OnStoppedLeading func()
	// OnNewLeader reports each change of holder, this process included.
	OnNewLeader func(identity string)
}

// Election elects the one process that may write for a target.
//
// A thin wrapper so that client-go's election package is imported here and
// nowhere else: the lock it drives creates and updates a Lease, and every
// object this repository can write is reached from internal/kubernetes.
type Election struct {
	elector *leaderelection.LeaderElector
}

// NewElection builds an election over one lease.
//
// The lease is released on cancellation (ReleaseOnCancel), so a clean shutdown
// hands over in one retry period instead of one lease duration. That is only
// safe if everything the lease guards has stopped before Run's context is
// cancelled; the caller owns that ordering.
func NewElection(c ElectionConfig) (*Election, error) {
	if c.Client == nil {
		return nil, fmt.Errorf("leader election: a client is required")
	}
	if c.Identity == "" {
		return nil, fmt.Errorf("leader election: an identity is required")
	}
	if problems := validation.IsDNS1123Subdomain(c.Name); len(problems) > 0 {
		// Checked here because the API server would reject the create and the
		// elector would retry forever, leaving a controller that never leads
		// and never says why.
		return nil, fmt.Errorf("leader election: lease name %q is not a valid object name: %s",
			c.Name, strings.Join(problems, "; "))
	}

	leaseDuration := orDefault(c.LeaseDuration, DefaultLeaseDuration)
	renewDeadline := orDefault(c.RenewDeadline, DefaultRenewDeadline)
	retryPeriod := orDefault(c.RetryPeriod, DefaultRetryPeriod)

	onStarted := c.OnStartedLeading
	if onStarted == nil {
		onStarted = func(context.Context) {}
	}
	onStopped := c.OnStoppedLeading
	if onStopped == nil {
		onStopped = func() {}
	}

	elector, err := leaderelection.NewLeaderElector(leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta:  metav1.ObjectMeta{Namespace: c.Namespace, Name: c.Name},
			Client:     c.Client.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{Identity: c.Identity},
		},
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		ReleaseOnCancel: true,
		Name:            c.Name,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: onStarted,
			OnStoppedLeading: onStopped,
			OnNewLeader:      c.OnNewLeader,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("leader election: %w", err)
	}
	return &Election{elector: elector}, nil
}

// Run campaigns for the lease and holds it until ctx is cancelled or a renewal
// fails. It returns when this process no longer holds the lease.
func (e *Election) Run(ctx context.Context) { e.elector.Run(ctx) }

// IsLeader reports whether this process held the lease at its last renewal.
func (e *Election) IsLeader() bool { return e.elector.IsLeader() }

// Leader returns the identity of the last observed holder.
func (e *Election) Leader() string { return e.elector.GetLeader() }

// ElectionIdentity names this process in the lease.
//
// The pod name, so `kubectl get lease` points at a pod an operator can look
// up, plus a random suffix: a restarted container keeps its pod name, and two
// processes must never be able to present the same identity.
func ElectionIdentity() string {
	return reportingInstance() + "_" + string(uuid.NewUUID())
}

func orDefault(d, fallback time.Duration) time.Duration {
	if d > 0 {
		return d
	}
	return fallback
}
