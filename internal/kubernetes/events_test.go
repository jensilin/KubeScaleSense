package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
)

// The events client's tests. The claim: it can add an announcement about the
// target in the target's namespace, and there is no request it can make that
// does anything else — including reading.

func TestEventsPath_IsTheNamespacedCollection(t *testing.T) {
	t.Parallel()

	if got, want := EventsPath(testNamespace), "/apis/events.k8s.io/v1/namespaces/data-pipeline/events"; got != want {
		t.Errorf("EventsPath = %q, want %q", got, want)
	}
}

func TestEventsOnlyTransport_PermitsOnlyCreateInTheTargetNamespace(t *testing.T) {
	t.Parallel()

	eventsPath := EventsPath(testNamespace)

	for _, tc := range []struct {
		name    string
		method  string
		path    string
		allowed bool
		why     string
	}{
		{"create an event", http.MethodPost, eventsPath, true, "the one request this client exists to make"},

		{"create an event elsewhere", http.MethodPost, EventsPath("kube-system"), false,
			"events are confined to the target's namespace"},
		{"create a core event", http.MethodPost, "/api/v1/namespaces/data-pipeline/events", false,
			"one API, one path"},
		{"edit an event", http.MethodPatch, eventsPath + "/normalizer.1", false,
			"an announcement is never rewritten"},
		{"delete an event", http.MethodDelete, eventsPath + "/normalizer.1", false,
			"or retracted"},
		{"read events", http.MethodGet, eventsPath, false,
			"this client is not an observer"},
		{"create a pod", http.MethodPost, "/api/v1/namespaces/data-pipeline/pods", false,
			"POST is allowed on one path only"},
		{"write the scale subresource", http.MethodPut, ScalePath(testNamespace, testDeployment), false,
			"replica changes belong to the actuation client"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			base := &countingRoundTripper{}
			transport := eventsOnlyTransport{base: base, allowed: eventsPath}

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
			case !tc.allowed && !errors.Is(err, ErrNotEventCreate):
				t.Errorf("%s %s returned %v, want ErrNotEventCreate: %s", tc.method, tc.path, err, tc.why)
			case !tc.allowed && base.count() != 0:
				t.Errorf("%s %s reached the underlying transport despite being refused", tc.method, tc.path)
			}
		})
	}
}

// End to end through the configuration main builds: the real clientset, the
// real transport chain, and an API server stand-in that records what arrived.
func TestEventWriter_CreatesAnEventRegardingTheTarget(t *testing.T) {
	t.Parallel()

	api := &eventSink{}
	server := httptest.NewServer(api)
	defer server.Close()

	cfg, err := NewEventsRESTConfig(writeKubeconfig(t, server.URL), testNamespace)
	if err != nil {
		t.Fatalf("NewEventsRESTConfig: %v", err)
	}
	if cfg.UserAgent != eventsUserAgent {
		t.Errorf("UserAgent = %q, want %q", cfg.UserAgent, eventsUserAgent)
	}
	// client-go sends built-in types as protobuf by default; JSON keeps the
	// stand-in decodable. The guard looks only at method and path.
	cfg.ContentType = "application/json"
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("NewForConfig: %v", err)
	}

	reader := &StaticReader{Synced: true, DeploymentList: []*appsv1.Deployment{{
		ObjectMeta: metav1.ObjectMeta{
			Name: testDeployment, Namespace: testNamespace, UID: "deployment-uid", ResourceVersion: "42",
		},
	}}}
	writer := NewEventWriter(client, reader, testNamespace, testDeployment)
	writer.now = func() time.Time { return time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC) }

	err = writer.Record(context.Background(), Event{
		Type: EventNormal, Reason: "ScaledUp", Action: "ScaleUp", Note: "2 -> 6",
	})
	if err != nil {
		t.Fatalf("Record: %v", err)
	}

	got := api.only(t)
	if got.method != http.MethodPost || got.path != EventsPath(testNamespace) {
		t.Errorf("request was %s %s, want POST %s", got.method, got.path, EventsPath(testNamespace))
	}

	event := got.event
	if event.Regarding.UID != "deployment-uid" || event.Regarding.Kind != "Deployment" ||
		event.Regarding.Name != testDeployment || event.Regarding.Namespace != testNamespace {
		t.Errorf("regarding = %+v; kubectl describe matches events to the Deployment by UID, so without it "+
			"the event is recorded and never shown", event.Regarding)
	}
	if event.Reason != "ScaledUp" || event.Action != "ScaleUp" || event.Type != EventNormal || event.Note != "2 -> 6" {
		t.Errorf("event = %s/%s/%s %q, want ScaledUp/ScaleUp/Normal %q",
			event.Reason, event.Action, event.Type, event.Note, "2 -> 6")
	}
	if event.ReportingController != reportingController || event.ReportingInstance == "" {
		t.Errorf("reporting %q/%q; the events API rejects an event without both",
			event.ReportingController, event.ReportingInstance)
	}
	if event.EventTime.IsZero() {
		t.Error("eventTime is unset; the events API requires it")
	}
	if !strings.HasPrefix(event.Name, testDeployment+".") {
		t.Errorf("name %q does not follow the <object>.<timestamp> convention", event.Name)
	}
}

func TestEventWriter_FailsWithoutTheTarget(t *testing.T) {
	t.Parallel()

	api := &eventSink{}
	server := httptest.NewServer(api)
	defer server.Close()

	cfg, err := NewEventsRESTConfig(writeKubeconfig(t, server.URL), testNamespace)
	if err != nil {
		t.Fatalf("NewEventsRESTConfig: %v", err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatalf("NewForConfig: %v", err)
	}

	writer := NewEventWriter(client, &StaticReader{Synced: true}, testNamespace, testDeployment)
	if err := writer.Record(context.Background(), Event{Type: EventNormal, Reason: "ScaledUp"}); err == nil {
		t.Error("an event was recorded with no Deployment to regard")
	}
	if n := api.count(); n != 0 {
		t.Errorf("%d requests reached the API server for an event with no target", n)
	}
}

func TestTruncateNote_StaysWithinTheAPILimitOnARuneBoundary(t *testing.T) {
	t.Parallel()

	short := "2 -> 6"
	if got := truncateNote(short); got != short {
		t.Errorf("a short note was changed to %q", got)
	}

	long := strings.Repeat("é", maxNoteBytes) // two bytes each
	got := truncateNote(long)
	if len(got) > maxNoteBytes {
		t.Errorf("truncated note is %d bytes, over the %d byte limit", len(got), maxNoteBytes)
	}
	if !strings.HasSuffix(got, "...") {
		t.Error("a truncated note does not say so")
	}
	if !json.Valid([]byte(`"`+got+`"`)) || strings.ContainsRune(got, '\uFFFD') {
		t.Error("truncation split a multi-byte character")
	}
}

// --- helpers --------------------------------------------------------------

type receivedEvent struct {
	method, path string
	event        eventsv1.Event
}

// eventSink answers like the API server's events endpoint: it echoes the
// created object back, as a real create does.
type eventSink struct {
	mu       sync.Mutex
	received []receivedEvent
}

func (s *eventSink) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)

	var event eventsv1.Event
	_ = json.Unmarshal(body, &event)

	s.mu.Lock()
	s.received = append(s.received, receivedEvent{method: r.Method, path: r.URL.Path, event: event})
	s.mu.Unlock()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	_, _ = w.Write(body)
}

func (s *eventSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.received)
}

func (s *eventSink) only(t *testing.T) receivedEvent {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.received) != 1 {
		t.Fatalf("the API server received %d requests, want exactly 1", len(s.received))
	}
	return s.received[0]
}
