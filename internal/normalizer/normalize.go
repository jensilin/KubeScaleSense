package normalizer

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// The input record format.
//
// A PM mediation record as it arrives from the network element, one per line:
//
//	2026-03-14T12:00:00Z,epdg-01,pdp.sessions.active,4821
//
// Four comma-separated fields: an RFC 3339 timestamp, a node identifier, a
// counter name, and an integer counter value. Deliberately the simplest format
// that still has something to normalize — a timezone to canonicalise, case and
// whitespace to fold, and a value to validate.
const recordFields = 4

// ErrMalformed marks input the Normalizer will never accept, however many times
// it is sent.
//
// The distinction between this and a transient failure is load-bearing for the
// pipeline: NiFi must route malformed records to its failure relationship and
// retry only transient ones. Retrying a malformed record is an infinite loop
// that burns the pool's capacity on a record that cannot succeed — the poison
// pill that turns one bad line into a pipeline outage.
var ErrMalformed = errors.New("malformed record")

// Record is the normalized output.
//
// Field order is the JSON key order, and it is fixed: encoding/json emits struct
// fields in declaration order, so the serialized form is byte-stable without
// needing a canonical-JSON library. Reordering these fields is an output format
// change and would break DI-04's byte-identical assertion.
type Record struct {
	// ObservedAt is the sample timestamp, canonicalised to UTC and RFC 3339.
	// Input may arrive in any offset; two records for the same instant must
	// normalize identically, which is most of the point of normalizing.
	ObservedAt string `json:"observedAt"`

	// Node is lowercased and trimmed, because "EPDG-01" and " epdg-01 " are the
	// same element and a downstream group-by must not think otherwise.
	Node string `json:"node"`

	// Counter is lowercased, trimmed, and dot-separated.
	Counter string `json:"counter"`

	Value int64 `json:"value"`

	// Checksum is the SHA-256 of the canonical form of the four fields above,
	// as lowercase hex.
	//
	// It is computed over the *normalized* values rather than the raw line, so
	// two inputs that differ only in the ways normalization is supposed to erase
	// (offset, case, padding) produce the same checksum. That makes the checksum
	// a statement about the record's meaning rather than about its spelling.
	Checksum string `json:"checksum"`
}

// Normalize converts one raw record into its normalized form.
//
// This is the pure function WR-03 requires: same input, same output, forever,
// on any pod, regardless of configuration. The configured processing cost
// deliberately does not appear here — it is applied by the caller and affects
// how long normalization takes, never what it produces. Folding the cost into
// the result would make a tuning change an output format change, and would break
// the byte-identical guarantee that NiFi's retry depends on.
func Normalize(raw []byte) (Record, error) {
	line := strings.TrimSpace(string(raw))
	if line == "" {
		return Record{}, fmt.Errorf("%w: the record is empty", ErrMalformed)
	}

	fields := strings.Split(line, ",")
	if len(fields) != recordFields {
		return Record{}, fmt.Errorf(
			"%w: expected %d comma-separated fields (timestamp,node,counter,value), got %d",
			ErrMalformed, recordFields, len(fields))
	}

	observedAt, err := parseTimestamp(fields[0])
	if err != nil {
		return Record{}, err
	}
	node, err := parseIdentifier("node", fields[1])
	if err != nil {
		return Record{}, err
	}
	counter, err := parseIdentifier("counter", fields[2])
	if err != nil {
		return Record{}, err
	}
	value, err := parseValue(fields[3])
	if err != nil {
		return Record{}, err
	}

	record := Record{
		ObservedAt: observedAt,
		Node:       node,
		Counter:    counter,
		Value:      value,
	}
	record.Checksum = checksum(record)
	return record, nil
}

// Encode renders a normalized record as the response body.
//
// json.Marshal rather than an encoder, because an encoder appends a newline and
// the body is compared byte for byte. Marshalling a struct with only string and
// int64 fields cannot fail, so the error is discarded rather than propagated as
// an outcome that no test could ever produce.
func Encode(record Record) []byte {
	encoded, _ := json.Marshal(record)
	return encoded
}

// checksum hashes the canonical form of the record's meaningful fields.
//
// The separator is a byte that cannot appear in any of the fields, so
// ("ab","c") and ("a","bc") cannot collide — the classic concatenation bug.
func checksum(r Record) string {
	canonical := strings.Join([]string{
		r.ObservedAt,
		r.Node,
		r.Counter,
		strconv.FormatInt(r.Value, 10),
	}, "\x00")

	sum := sha256.Sum256([]byte(canonical))
	return hex.EncodeToString(sum[:])
}

func parseTimestamp(field string) (string, error) {
	trimmed := strings.TrimSpace(field)
	parsed, err := time.Parse(time.RFC3339, trimmed)
	if err != nil {
		return "", fmt.Errorf("%w: timestamp %q is not RFC 3339, e.g. %q",
			ErrMalformed, trimmed, "2026-03-14T12:00:00Z")
	}
	// UTC before formatting: the offset is presentation, and a normalizer that
	// preserved it would emit two different records for one instant.
	return parsed.UTC().Format(time.RFC3339), nil
}

func parseIdentifier(name, field string) (string, error) {
	trimmed := strings.ToLower(strings.TrimSpace(field))
	if trimmed == "" {
		return "", fmt.Errorf("%w: the %s field is empty", ErrMalformed, name)
	}
	return trimmed, nil
}

func parseValue(field string) (int64, error) {
	trimmed := strings.TrimSpace(field)
	value, err := strconv.ParseInt(trimmed, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("%w: value %q is not an integer", ErrMalformed, trimmed)
	}
	if value < 0 {
		// A negative PM counter is a mediation bug upstream, and passing it
		// through would let it become a negative rate in someone's dashboard.
		return 0, fmt.Errorf("%w: value %d is negative", ErrMalformed, value)
	}
	return value, nil
}

// burn performs a deterministic amount of real CPU work and returns its digest.
//
// Iterated SHA-256 rather than a sleep or a spin loop: it is genuine CPU load
// (so the kubelet and metrics-server see it, and a busy pod really is busy), it
// is exactly reproducible for a given round count, and its cost is linear in
// that count, which is what lets a spike be tuned to demand a known replica
// count.
//
// The digest is returned, and the caller puts it in a response header, so the
// chain of hashes is observable and cannot be optimised away. A discarded result
// would be dead code the compiler is free to delete, and a workload whose work
// the compiler can delete is not a workload.
func burn(seed []byte, rounds int) string {
	digest := sha256.Sum256(seed)
	for i := 0; i < rounds; i++ {
		digest = sha256.Sum256(digest[:])
	}
	return hex.EncodeToString(digest[:])
}
