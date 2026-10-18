package kubernetes

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"time"

	corev1 "k8s.io/api/core/v1"
	eventsv1 "k8s.io/api/events/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
)

// eventsUserAgent separates the audit trail's two writers: a replica change
// carries scaleUserAgent, an announcement carries this.
const eventsUserAgent = "kubescalesense/p3-events"

// reportingController is the events.k8s.io reportingController field. The API
// requires a qualified name, and the project's label domain is reused for it.
const reportingController = "kubescalesense.io/controller"

// Event types, as the events API spells them.
const (
	EventNormal  = corev1.EventTypeNormal
	EventWarning = corev1.EventTypeWarning
)

// The events API rejects a note longer than 1 KiB, and a rejected event is an
// announcement lost.
const maxNoteBytes = 1024

// ErrNotEventCreate is returned when the events client attempts anything other
// than creating an event in the target's namespace.
var ErrNotEventCreate = fmt.Errorf(
	"kubescalesense: the events client may only create events in the target's namespace")

// EventsPath returns the API path events are created at in a namespace.
//
// Exported for the same reason as ScalePath: it is the exact string the
// transport guard compares against.
func EventsPath(namespace string) string {
	return fmt.Sprintf("/apis/events.k8s.io/v1/namespaces/%s/events", namespace)
}

// NewEventsRESTConfig builds the REST configuration for the events client.
//
// A third client rather than a wider scale transport, so that each write
// capability is bounded on its own: the scale client still cannot create
// anything, and this one can create one kind of object in one namespace and do
// nothing else — not read, not update, not delete, and certainly not reach a
// Deployment. An event is an annotation for humans; the worst a bug here can do
// is say something misleading in `kubectl describe`.
func NewEventsRESTConfig(kubeconfigPath, namespace string) (*rest.Config, error) {
	cfg, err := restConfig(kubeconfigPath)
	if err != nil {
		return nil, err
	}

	cfg.UserAgent = eventsUserAgent

	allowed := EventsPath(namespace)
	cfg.Wrap(func(base http.RoundTripper) http.RoundTripper {
		return eventsOnlyTransport{base: base, allowed: allowed}
	})

	return cfg, nil
}

// eventsOnlyTransport permits POST to one collection path and nothing else.
//
// Unlike the scale transport it does not even permit reads. The typed events
// client issues none on Create, and a client that cannot read cannot be
// repurposed as a second observer with a different user agent.
type eventsOnlyTransport struct {
	base    http.RoundTripper
	allowed string
}

// RoundTrip implements http.RoundTripper.
func (t eventsOnlyTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Method == http.MethodPost && req.URL.Path == t.allowed {
		return t.base.RoundTrip(req)
	}
	return nil, fmt.Errorf("%w (attempted %s %s; the only permitted request is POST %s)",
		ErrNotEventCreate, req.Method, req.URL.Path, t.allowed)
}

// Event is one announcement about the target Deployment.
type Event struct {
	// Type is EventNormal or EventWarning.
	Type string
	// Reason is the architecture § 8.2 event reason: ScaledUp, ScalingConflict.
	Reason string
	// Action is what the controller did about it: Scale, Revert, Hold.
	Action string
	// Note is the human-readable message `kubectl describe` shows.
	Note string
}

// eventCreator is the slice of client-go's EventInterface the writer uses.
//
// One method. The full interface also carries Update, Patch, and Delete, and an
// announcement has no business editing or retracting another one.
type eventCreator interface {
	Create(ctx context.Context, event *eventsv1.Event, opts metav1.CreateOptions) (*eventsv1.Event, error)
}

// EventWriter creates events regarding one Deployment.
//
// Bound to its target at construction, like ScaleWriter. The Deployment's UID
// is looked up in the informer cache at each write rather than passed in, so a
// caller cannot point an event at some other object; and it is looked up at all
// because `kubectl describe` matches events to an object by UID, so an event
// without one is recorded and then never shown.
type EventWriter struct {
	events     eventCreator
	reader     ClusterReader
	namespace  string
	deployment string
	instance   string
	now        func() time.Time
}

// NewEventWriter binds a writer to one Deployment.
func NewEventWriter(client kubernetes.Interface, reader ClusterReader, namespace, deployment string) *EventWriter {
	return &EventWriter{
		events:     client.EventsV1().Events(namespace),
		reader:     reader,
		namespace:  namespace,
		deployment: deployment,
		instance:   reportingInstance(),
		now:        time.Now,
	}
}

// Record creates one event regarding the target.
func (w *EventWriter) Record(ctx context.Context, e Event) error {
	target, err := w.reader.Deployment(w.namespace, w.deployment)
	if err != nil {
		return fmt.Errorf("resolving %s/%s for an event: %w", w.namespace, w.deployment, err)
	}

	now := w.now()
	event := &eventsv1.Event{
		ObjectMeta: metav1.ObjectMeta{
			// The naming scheme client-go's own recorders use, so our events
			// sort and collide the way every other controller's do.
			Name:      fmt.Sprintf("%s.%x", w.deployment, now.UnixNano()),
			Namespace: w.namespace,
		},
		EventTime:           metav1.NewMicroTime(now),
		ReportingController: reportingController,
		ReportingInstance:   w.instance,
		Action:              e.Action,
		Reason:              e.Reason,
		Type:                e.Type,
		Note:                truncateNote(e.Note),
		Regarding: corev1.ObjectReference{
			APIVersion:      "apps/v1",
			Kind:            "Deployment",
			Namespace:       w.namespace,
			Name:            w.deployment,
			UID:             target.UID,
			ResourceVersion: target.ResourceVersion,
		},
	}

	// The second mutating client-go call in the repository, and the only one
	// that does not address the target: it creates a new Event object, which
	// no other component reads to make a decision.
	if _, err := w.events.Create(ctx, event, metav1.CreateOptions{}); err != nil {
		return fmt.Errorf("creating a %s event on %s/%s: %w", e.Reason, w.namespace, w.deployment, err)
	}
	return nil
}

// reportingInstance names this process in the events it writes. In a Pod the
// hostname is the Pod name, which is the identity an operator can look up.
func reportingInstance() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "kubescalesense"
	}
	if len(host) > 128 {
		host = host[:128]
	}
	return host
}

func truncateNote(note string) string {
	if len(note) <= maxNoteBytes {
		return note
	}
	const ellipsis = "..."
	cut := maxNoteBytes - len(ellipsis)
	// Back off to a rune boundary so the API is not handed invalid UTF-8.
	for cut > 0 && note[cut]&0xC0 == 0x80 {
		cut--
	}
	return note[:cut] + ellipsis
}
