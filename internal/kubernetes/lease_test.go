package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	corefake "k8s.io/client-go/kubernetes/fake"
)

// The lease client's tests. The claim: it can read, create, and renew the one
// lease that elects the writer for the target, and there is no request it can
// make that touches any other object.

const testLease = "kubescalesense-normalizer"

// Short enough that an election test finishes in about a second, and still
// inside the elector's own constraints (RenewDeadline > 1.2 × RetryPeriod).
const (
	testLeaseDuration = time.Second
	testRenewDeadline = 600 * time.Millisecond
	testRetryPeriod   = 100 * time.Millisecond
)

// The tests that assert a release stop the leader between renewals, never
// during one. A renewal abandoned by cancellation can still be applied by the
// server after release has re-read the lease, and then the release's update is
// rejected as stale and the lease is left to expire. A one-second retry period
// leaves a wide gap to stop in.
const (
	releaseLeaseDuration = 5 * time.Second
	releaseRenewDeadline = 2 * time.Second
	releaseRetryPeriod   = time.Second
)

func TestLeaseName_IsKeyedOnTheTarget(t *testing.T) {
	t.Parallel()

	if got := LeaseName(testDeployment); got != testLease {
		t.Errorf("LeaseName = %q, want %q", got, testLease)
	}
	if got, want := LeasePath(testNamespace, testLease),
		"/apis/coordination.k8s.io/v1/namespaces/data-pipeline/leases/kubescalesense-normalizer"; got != want {
		t.Errorf("LeasePath = %q, want %q", got, want)
	}
}

func TestLeaseTransport_PermitsOnlyTheOneLease(t *testing.T) {
	t.Parallel()

	collection, item := LeasesPath(testNamespace), LeasePath(testNamespace, testLease)
	ownLease := `{"metadata":{"name":"` + testLease + `","namespace":"data-pipeline"}}`

	for _, tc := range []struct {
		name        string
		method      string
		path        string
		contentType string
		body        string
		allowed     bool
		why         string
	}{
		{"read the lease", http.MethodGet, item, "", "", true, "the elector reads the holder before every renewal"},
		{"renew the lease", http.MethodPut, item, "application/json", ownLease, true, "renewal and release are updates"},
		{"create the lease", http.MethodPost, collection, "application/json", ownLease, true,
			"the first candidate creates it"},

		{"create another lease", http.MethodPost, collection, "application/json",
			`{"metadata":{"name":"kube-scheduler"}}`, false, "RBAC cannot narrow create by name, so the body is checked"},
		{"create a generated name", http.MethodPost, collection, "application/json",
			`{"metadata":{"generateName":"` + testLease + `"}}`, false, "the name must be exactly the lease's"},
		{"create as protobuf", http.MethodPost, collection, "application/vnd.kubernetes.protobuf", ownLease, false,
			"a body the guard cannot read is refused, not trusted"},
		{"create with no JSON object", http.MethodPost, collection, "application/json", `[]`, false,
			"an unparsable body names no lease"},
		{"list leases", http.MethodGet, collection, "", "", false, "the client is not an observer of other leases"},
		{"read another lease", http.MethodGet, LeasePath(testNamespace, "kube-scheduler"), "", "", false,
			"one lease, one path"},
		{"renew another lease", http.MethodPut, LeasePath(testNamespace, "kube-scheduler"), "application/json",
			`{"metadata":{"name":"kube-scheduler"}}`, false, "holding someone else's lease would starve them"},
		{"the lease in another namespace", http.MethodPut, LeasePath("kube-system", testLease), "application/json",
			ownLease, false, "the lease lives in the target's namespace"},
		{"patch the lease", http.MethodPatch, item, "application/merge-patch+json", `{}`, false,
			"client-go's lease lock never patches"},
		{"delete the lease", http.MethodDelete, item, "", "", false, "release is an update, never a delete"},
		{"write the scale subresource", http.MethodPut, ScalePath(testNamespace, testDeployment), "application/json",
			`{}`, false, "replica changes belong to the actuation client"},
		{"create an event", http.MethodPost, EventsPath(testNamespace), "application/json", `{}`, false,
			"announcements belong to the events client"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			base := &countingRoundTripper{}
			transport := leaseTransport{base: base, collection: collection, item: item, name: testLease}

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
				t.Errorf("%s %s was refused (%v), but it must be allowed: %s", tc.method, tc.path, err, tc.why)
			case tc.allowed && base.count() != 1:
				t.Errorf("%s %s did not reach the underlying transport", tc.method, tc.path)
			case !tc.allowed && !errors.Is(err, ErrNotLeaseAccess):
				t.Errorf("%s %s returned %v, want ErrNotLeaseAccess: %s", tc.method, tc.path, err, tc.why)
			case !tc.allowed && base.count() != 0:
				t.Errorf("%s %s reached the underlying transport despite being refused", tc.method, tc.path)
			}
		})
	}
}

// End to end through the configuration main builds: the real clientset, the
// real transport chain, client-go's own elector, and an API server stand-in
// that keeps one lease. It proves the elector's request shapes are the ones the
// guard permits — a guard that refused them would be a controller that never
// leads.
func TestElection_AcquiresAndReleasesThroughTheGuardedClient(t *testing.T) {
	t.Parallel()

	api := newLeaseServer()
	server := httptest.NewServer(api)
	defer server.Close()

	cfg, err := NewLeaseRESTConfig(writeKubeconfig(t, server.URL), testNamespace, testLease)
	if err != nil {
		t.Fatalf("NewLeaseRESTConfig: %v", err)
	}
	if cfg.UserAgent != leaseUserAgent {
		t.Errorf("UserAgent = %q, want %q", cfg.UserAgent, leaseUserAgent)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("NewForConfig: %v", err)
	}

	started := make(chan struct{})
	election, err := NewElection(ElectionConfig{
		Client: client, Namespace: testNamespace, Name: testLease, Identity: "controller-a",
		LeaseDuration: releaseLeaseDuration, RenewDeadline: releaseRenewDeadline, RetryPeriod: releaseRetryPeriod,
		OnStartedLeading: func(context.Context) { close(started) },
	})
	if err != nil {
		t.Fatalf("NewElection: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { election.Run(ctx); close(done) }()

	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatalf("the election never led; requests seen: %v", api.seen())
	}
	if !election.IsLeader() || election.Leader() != "controller-a" {
		t.Errorf("IsLeader = %v, Leader = %q after acquiring", election.IsLeader(), election.Leader())
	}

	api.betweenRenewals(t)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancellation")
	}

	for _, r := range api.seen() {
		switch r {
		case "GET " + LeasePath(testNamespace, testLease),
			"POST " + LeasesPath(testNamespace),
			"PUT " + LeasePath(testNamespace, testLease):
		default:
			t.Errorf("the API server received %s, which is not a lease request", r)
		}
	}

	// ReleaseOnCancel: the holder is cleared and the lease made to expire at
	// once, so a standby takes over in one retry period instead of waiting out
	// the lease.
	lease := api.current(t)
	if holder := deref(lease.Spec.HolderIdentity); holder != "" {
		t.Errorf("after a clean stop the lease is still held by %q", holder)
	}
	if got := deref32(lease.Spec.LeaseDurationSeconds); got != 1 {
		t.Errorf("after a clean stop leaseDurationSeconds = %d, want 1", got)
	}
}

// Only one of two candidates leads, and a clean stop hands over without
// waiting for the lease to expire.
func TestElection_OneLeaderAndACleanHandover(t *testing.T) {
	t.Parallel()

	api := newLeaseServer()
	client := guardedLeaseClient(t, api)
	timings := ElectionConfig{
		LeaseDuration: releaseLeaseDuration, RenewDeadline: releaseRenewDeadline, RetryPeriod: releaseRetryPeriod,
	}

	a := startCandidateWith(t, client, "controller-a", timings)
	waitFor(t, "controller-a to lead", a.leading)

	b := startCandidateWith(t, client, "controller-b", timings)
	waitFor(t, "the standby to observe controller-a", func() bool { return b.election.Leader() == "controller-a" })
	if b.leading() {
		t.Fatal("two candidates lead at once")
	}

	api.betweenRenewals(t)
	stoppedAt := time.Now()
	a.stop(t)
	if holder := deref(api.current(t).Spec.HolderIdentity); holder != "" {
		t.Fatalf("after controller-a stopped the lease is still held by %q", holder)
	}
	waitFor(t, "controller-b to take over", b.leading)
	if took := time.Since(stoppedAt); took >= releaseLeaseDuration {
		t.Errorf("handover took %v, at least the lease duration; the lease was not released", took)
	}
	b.stop(t)
}

// IT-09's lease-loss half at unit level: when another identity takes the lease
// the holder's leading context is cancelled within the renew deadline.
func TestElection_LosingTheLeaseCancelsTheLeader(t *testing.T) {
	t.Parallel()

	api := newLeaseServer()
	a := startCandidate(t, guardedLeaseClient(t, api), "controller-a")
	waitFor(t, "controller-a to lead", a.leading)

	api.seize(t, "intruder")

	select {
	case <-a.leaderCtx().Done():
	case <-time.After(testRenewDeadline + 2*time.Second):
		t.Fatal("the leading context was not cancelled after the lease was taken")
	}
	select {
	case <-a.stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("OnStoppedLeading did not run after the lease was lost")
	}
	if a.election.IsLeader() {
		t.Error("IsLeader is still true after the lease was lost")
	}
	a.stop(t)
}

func TestNewElection_RejectsWhatTheAPIServerWould(t *testing.T) {
	t.Parallel()

	client := corefake.NewClientset()
	for name, c := range map[string]ElectionConfig{
		"a lease name over 253 characters": {
			Client: client, Namespace: testNamespace, Name: LeaseName(strings.Repeat("n", 250)), Identity: "a",
		},
		"an uppercase lease name": {Client: client, Namespace: testNamespace, Name: "KubeScaleSense", Identity: "a"},
		"no identity":             {Client: client, Namespace: testNamespace, Name: testLease},
		"no client":               {Namespace: testNamespace, Name: testLease, Identity: "a"},
	} {
		if _, err := NewElection(c); err == nil {
			t.Errorf("%s was accepted; the elector would retry a doomed create forever", name)
		}
	}
}

func TestElectionIdentity_IsUniquePerProcessStart(t *testing.T) {
	t.Parallel()

	first, second := ElectionIdentity(), ElectionIdentity()
	if first == second {
		t.Errorf("two identities are both %q; a restarted container would impersonate its predecessor", first)
	}
	if !strings.HasPrefix(first, reportingInstance()+"_") {
		t.Errorf("identity %q does not start with the pod name %q", first, reportingInstance())
	}
}

// --- helpers --------------------------------------------------------------

// guardedLeaseClient serves the stand-in over HTTP and returns a client built
// exactly as main builds it, so every election test also exercises the guard.
func guardedLeaseClient(t *testing.T, api *leaseServer) kubernetes.Interface {
	t.Helper()

	server := httptest.NewServer(api)
	t.Cleanup(server.Close)

	cfg, err := NewLeaseRESTConfig(writeKubeconfig(t, server.URL), testNamespace, testLease)
	if err != nil {
		t.Fatalf("NewLeaseRESTConfig: %v", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("NewForConfig: %v", err)
	}
	return client
}

type candidate struct {
	election *Election
	cancel   context.CancelFunc
	done     chan struct{}
	stopped  chan struct{}

	mu  sync.Mutex
	ctx context.Context
}

func startCandidate(t *testing.T, client kubernetes.Interface, identity string) *candidate {
	t.Helper()
	return startCandidateWith(t, client, identity, ElectionConfig{
		LeaseDuration: testLeaseDuration, RenewDeadline: testRenewDeadline, RetryPeriod: testRetryPeriod,
	})
}

// startCandidateWith takes its timings from the durations in timings.
func startCandidateWith(t *testing.T, client kubernetes.Interface, identity string, timings ElectionConfig) *candidate {
	t.Helper()

	c := &candidate{done: make(chan struct{}), stopped: make(chan struct{})}
	election, err := NewElection(ElectionConfig{
		Client: client, Namespace: testNamespace, Name: testLease, Identity: identity,
		LeaseDuration: timings.LeaseDuration, RenewDeadline: timings.RenewDeadline, RetryPeriod: timings.RetryPeriod,
		OnStartedLeading: func(ctx context.Context) {
			c.mu.Lock()
			c.ctx = ctx
			c.mu.Unlock()
		},
		OnStoppedLeading: func() { close(c.stopped) },
	})
	if err != nil {
		t.Fatalf("NewElection(%s): %v", identity, err)
	}
	c.election = election

	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	go func() { election.Run(ctx); close(c.done) }()
	t.Cleanup(cancel)
	return c
}

func (c *candidate) leaderCtx() context.Context {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.ctx
}

func (c *candidate) leading() bool {
	ctx := c.leaderCtx()
	return ctx != nil && ctx.Err() == nil && c.election.IsLeader()
}

func (c *candidate) stop(t *testing.T) {
	t.Helper()
	c.cancel()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the election did not stop after cancellation")
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// leaseServer keeps one lease the way the API server does: 404 until created,
// 409 on a second create, and a new resourceVersion on every write.
type leaseServer struct {
	mu       sync.Mutex
	lease    []byte
	version  int
	writes   int
	requests []string
}

func newLeaseServer() *leaseServer { return &leaseServer{} }

func (s *leaseServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	s.mu.Lock()
	defer s.mu.Unlock()
	s.requests = append(s.requests, r.Method+" "+r.URL.Path)

	w.Header().Set("Content-Type", "application/json")
	switch {
	case r.Method == http.MethodGet && s.lease == nil:
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"NotFound","code":404}`))
	case r.Method == http.MethodGet:
		_, _ = w.Write(s.lease)
	case r.Method == http.MethodPost && s.lease != nil:
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"AlreadyExists",` +
			`"message":"lease already exists","code":409}`))
	case r.Method == http.MethodPut && s.lease == nil:
		w.WriteHeader(http.StatusNotFound)
	case r.Method == http.MethodPost || r.Method == http.MethodPut:
		var lease coordinationv1.Lease
		if err := json.Unmarshal(body, &lease); err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		// Optimistic concurrency, which the elector's renewal relies on: it
		// first tries an update from its cached copy, and only a conflict
		// sends it back to read who holds the lease now.
		if r.Method == http.MethodPut && lease.ResourceVersion != strconv.Itoa(s.version) {
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte(`{"kind":"Status","apiVersion":"v1","status":"Failure","reason":"Conflict",` +
				`"message":"resourceVersion ` + lease.ResourceVersion + ` is stale, the lease is at ` +
				strconv.Itoa(s.version) + `","code":409}`))
			return
		}
		s.version++
		s.writes++
		lease.ResourceVersion = strconv.Itoa(s.version)
		s.lease, _ = json.Marshal(&lease)
		if r.Method == http.MethodPost {
			w.WriteHeader(http.StatusCreated)
		}
		_, _ = w.Write(s.lease)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// seize hands the lease to another identity, as a second controller that won
// it would.
func (s *leaseServer) seize(t *testing.T, holder string) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()

	var lease coordinationv1.Lease
	if err := json.Unmarshal(s.lease, &lease); err != nil {
		t.Fatalf("the stand-in holds no lease to seize: %v", err)
	}
	duration, renewed := int32(60), metav1.NewMicroTime(time.Now())
	lease.Spec.HolderIdentity, lease.Spec.LeaseDurationSeconds, lease.Spec.RenewTime = &holder, &duration, &renewed
	s.version++
	lease.ResourceVersion = strconv.Itoa(s.version)
	s.lease, _ = json.Marshal(&lease)
}

// betweenRenewals returns just after the leader's next renewal has been
// answered, when the next one is a whole retry period away: the moment a stop
// cannot race a renewal in flight.
func (s *leaseServer) betweenRenewals(t *testing.T) {
	t.Helper()
	before := s.writeCount()
	waitFor(t, "the leader's next renewal", func() bool { return s.writeCount() > before })
	time.Sleep(50 * time.Millisecond)
}

func (s *leaseServer) writeCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writes
}

func (s *leaseServer) seen() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.requests...)
}

func (s *leaseServer) current(t *testing.T) coordinationv1.Lease {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	var lease coordinationv1.Lease
	if err := json.Unmarshal(s.lease, &lease); err != nil {
		t.Fatalf("the stand-in holds no lease: %v", err)
	}
	return lease
}

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

func deref32(i *int32) int32 {
	if i == nil {
		return 0
	}
	return *i
}
