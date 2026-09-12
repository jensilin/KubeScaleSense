package normalizer

import (
	"bytes"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
)

// The reference record used throughout the suite, and its exact normalized
// form.
//
// referenceBody is a golden value: written out literally rather than recomputed
// by the test, because a test that recomputes the implementation's own
// arithmetic cannot detect a change to it. Any edit to the field set, the key
// order, or the checksum's canonical form fails here — which is the point,
// since NiFi's retry safety rests on this output being stable (DI-04, D-03).
const (
	referenceRaw  = "2026-03-14T12:00:00Z,epdg-01,pdp.sessions.active,4821"
	referenceBody = `{"observedAt":"2026-03-14T12:00:00Z","node":"epdg-01","counter":"pdp.sessions.active",` +
		`"value":4821,"checksum":"0b511d44b96962cfbe3c20eb7381c56efeaa59281ade9d29a710427a8721e97b"}`
)

func TestEncode_MatchesTheGoldenBody(t *testing.T) {
	t.Parallel()

	record, err := Normalize([]byte(referenceRaw))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if got := string(Encode(record)); got != referenceBody {
		t.Errorf("the normalized body changed.\n got: %s\nwant: %s", got, referenceBody)
	}
}

func TestNormalize_TheReferenceRecord(t *testing.T) {
	t.Parallel()

	record, err := Normalize([]byte(referenceRaw))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}

	if record.ObservedAt != "2026-03-14T12:00:00Z" {
		t.Errorf("ObservedAt = %q", record.ObservedAt)
	}
	if record.Node != "epdg-01" {
		t.Errorf("Node = %q", record.Node)
	}
	if record.Counter != "pdp.sessions.active" {
		t.Errorf("Counter = %q", record.Counter)
	}
	if record.Value != 4821 {
		t.Errorf("Value = %d", record.Value)
	}
	if len(record.Checksum) != 64 {
		t.Errorf("Checksum = %q, want 64 hex characters", record.Checksum)
	}

	// Key order is part of the contract: encoding/json emits declaration order,
	// and a reordered struct would change every byte downstream.
	body := string(Encode(record))
	wantOrder := []string{`"observedAt"`, `"node"`, `"counter"`, `"value"`, `"checksum"`}
	position := -1
	for _, key := range wantOrder {
		next := strings.Index(body, key)
		if next <= position {
			t.Fatalf("key %s is out of order in %s", key, body)
		}
		position = next
	}
}

// The normalization the service actually performs, case by case. Each row is a
// difference the normalizer is supposed to erase.
func TestNormalize_CanonicalisesItsInput(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		raw  string
		want string
	}{
		{
			name: "an offset timestamp becomes UTC",
			raw:  "2026-03-14T14:00:00+02:00,epdg-01,pdp.sessions.active,4821",
			want: "2026-03-14T12:00:00Z",
		},
		{
			name: "a UTC timestamp is unchanged",
			raw:  referenceRaw,
			want: "2026-03-14T12:00:00Z",
		},
		{
			name: "a negative offset becomes UTC",
			raw:  "2026-03-14T07:00:00-05:00,epdg-01,pdp.sessions.active,4821",
			want: "2026-03-14T12:00:00Z",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			record, err := Normalize([]byte(tc.raw))
			if err != nil {
				t.Fatalf("Normalize: %v", err)
			}
			if record.ObservedAt != tc.want {
				t.Errorf("ObservedAt = %q, want %q", record.ObservedAt, tc.want)
			}
		})
	}

	t.Run("case and padding are folded", func(t *testing.T) {
		t.Parallel()

		record, err := Normalize([]byte("2026-03-14T12:00:00Z, EPDG-01 ,  PDP.Sessions.Active , 4821 "))
		if err != nil {
			t.Fatalf("Normalize: %v", err)
		}
		if record.Node != "epdg-01" || record.Counter != "pdp.sessions.active" || record.Value != 4821 {
			t.Errorf("got %+v", record)
		}
	})
}

// The property the whole pipeline's safety rests on: inputs that differ only in
// ways normalization erases produce byte-identical output, so a NiFi retry is
// harmless (WR-03, D-03, FS-29).
func TestNormalize_IsAPureFunctionOfMeaning(t *testing.T) {
	t.Parallel()

	equivalent := []string{
		referenceRaw,
		"2026-03-14T14:00:00+02:00,epdg-01,pdp.sessions.active,4821",
		" 2026-03-14T12:00:00Z , EPDG-01 , PDP.SESSIONS.ACTIVE , 4821 ",
		"2026-03-14T12:00:00Z,Epdg-01,Pdp.Sessions.Active,4821\n",
	}

	first, err := Normalize([]byte(equivalent[0]))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	want := Encode(first)

	for _, raw := range equivalent[1:] {
		record, err := Normalize([]byte(raw))
		if err != nil {
			t.Fatalf("Normalize(%q): %v", raw, err)
		}
		if got := Encode(record); !bytes.Equal(got, want) {
			t.Errorf("Normalize(%q) =\n  %s\nwant\n  %s", raw, got, want)
		}
	}
}

// Repeated normalization of the same record must be byte-identical, including
// across goroutines: any shared mutable state would show up here.
func TestNormalize_IsStableAcrossRepeatsAndGoroutines(t *testing.T) {
	t.Parallel()

	first, err := Normalize([]byte(referenceRaw))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	want := Encode(first)

	const goroutines = 16
	var wg sync.WaitGroup
	results := make([][]byte, goroutines)

	for i := range goroutines {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			record, err := Normalize([]byte(referenceRaw))
			if err != nil {
				t.Errorf("Normalize: %v", err)
				return
			}
			results[slot] = Encode(record)
		}(i)
	}
	wg.Wait()

	for i, got := range results {
		if !bytes.Equal(got, want) {
			t.Errorf("goroutine %d produced %s, want %s", i, got, want)
		}
	}
}

// Different records must not collide, and in particular the field separator in
// the checksum must prevent ("ab","c") from hashing like ("a","bc").
func TestNormalize_DistinctRecordsGetDistinctChecksums(t *testing.T) {
	t.Parallel()

	raws := []string{
		referenceRaw,
		"2026-03-14T12:00:01Z,epdg-01,pdp.sessions.active,4821", // one second later
		"2026-03-14T12:00:00Z,epdg-02,pdp.sessions.active,4821", // another node
		"2026-03-14T12:00:00Z,epdg-01,pdp.sessions.idle,4821",   // another counter
		"2026-03-14T12:00:00Z,epdg-01,pdp.sessions.active,4822", // another value
		"2026-03-14T12:00:00Z,epdg,01.pdp.sessions.active,4821", // the concatenation trap
	}

	seen := make(map[string]string, len(raws))
	for _, raw := range raws {
		record, err := Normalize([]byte(raw))
		if err != nil {
			t.Fatalf("Normalize(%q): %v", raw, err)
		}
		if previous, clash := seen[record.Checksum]; clash {
			t.Errorf("%q and %q share checksum %s", previous, raw, record.Checksum)
		}
		seen[record.Checksum] = raw
	}
}

// Malformed input must be reported as malformed, not as a transient failure:
// the pipeline retries transient failures forever, so misclassifying a bad
// record turns one bad line into a poison pill.
func TestNormalize_RejectsMalformedInput(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		raw     string
		wantMsg string
	}{
		{name: "empty", raw: "", wantMsg: "empty"},
		{name: "whitespace only", raw: "   \n\t ", wantMsg: "empty"},
		{name: "too few fields", raw: "2026-03-14T12:00:00Z,epdg-01,pdp.sessions.active", wantMsg: "got 3"},
		{name: "too many fields", raw: referenceRaw + ",extra", wantMsg: "got 5"},
		{name: "timestamp not RFC 3339", raw: "14/03/2026 12:00,epdg-01,c,1", wantMsg: "RFC 3339"},
		{name: "timestamp without a zone", raw: "2026-03-14T12:00:00,epdg-01,c,1", wantMsg: "RFC 3339"},
		{name: "empty node", raw: "2026-03-14T12:00:00Z, ,c,1", wantMsg: "node field is empty"},
		{name: "empty counter", raw: "2026-03-14T12:00:00Z,epdg-01, ,1", wantMsg: "counter field is empty"},
		{name: "value not an integer", raw: "2026-03-14T12:00:00Z,epdg-01,c,4.5", wantMsg: "not an integer"},
		{name: "value is a word", raw: "2026-03-14T12:00:00Z,epdg-01,c,many", wantMsg: "not an integer"},
		{name: "negative value", raw: "2026-03-14T12:00:00Z,epdg-01,c,-1", wantMsg: "negative"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			_, err := Normalize([]byte(tc.raw))
			if err == nil {
				t.Fatalf("Normalize(%q) succeeded, want a malformed-record error", tc.raw)
			}
			if !errors.Is(err, ErrMalformed) {
				t.Errorf("error %v does not wrap ErrMalformed, so the pipeline would retry it forever", err)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Errorf("error = %q, want it to mention %q", err, tc.wantMsg)
			}
		})
	}
}

// A large value must survive: PM counters are cumulative and can be large.
func TestNormalize_AcceptsTheFullInt64Range(t *testing.T) {
	t.Parallel()

	record, err := Normalize([]byte("2026-03-14T12:00:00Z,epdg-01,bytes.total,9223372036854775807"))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	if record.Value != 9223372036854775807 {
		t.Errorf("Value = %d", record.Value)
	}

	if _, err := Normalize([]byte("2026-03-14T12:00:00Z,epdg-01,bytes.total,9223372036854775808")); err == nil {
		t.Error("a value beyond int64 should be rejected rather than silently wrapped")
	}
}

func TestEncode_ProducesValidJSONWithoutATrailingNewline(t *testing.T) {
	t.Parallel()

	record, err := Normalize([]byte(referenceRaw))
	if err != nil {
		t.Fatalf("Normalize: %v", err)
	}
	body := Encode(record)

	var decoded Record
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("the response body must be valid JSON: %v", err)
	}
	if decoded != record {
		t.Errorf("round trip changed the record: %+v vs %+v", decoded, record)
	}
	// A trailing newline would make the body differ from its marshalled form,
	// and the byte-identical assertion is on the body.
	if bytes.HasSuffix(body, []byte("\n")) {
		t.Error("the body must not end in a newline")
	}
}

// The CPU cost must be deterministic for a given round count and must actually
// depend on the count, or the dial does nothing.
func TestBurn_IsDeterministicAndCostDependent(t *testing.T) {
	t.Parallel()

	seed := []byte("seed")

	if a, b := burn(seed, 1000), burn(seed, 1000); a != b {
		t.Errorf("burn is not deterministic: %s vs %s", a, b)
	}
	if a, b := burn(seed, 1000), burn(seed, 1001); a == b {
		t.Error("burn returned the same digest for different round counts, so the cost dial is not wired to the work")
	}
	if a, b := burn(seed, 1000), burn([]byte("other"), 1000); a == b {
		t.Error("burn returned the same digest for different seeds")
	}
	// Zero rounds is legal and is what the fast unit tests use.
	if got := burn(seed, 0); len(got) != 64 {
		t.Errorf("burn(seed, 0) = %q, want a 64-character digest", got)
	}
}
