package main

import (
	"fmt"
	"strings"
	"time"
)

// The record generator.
//
// Deterministic by construction: record n is a pure function of n and the
// generator's settings, with no clock and no randomness. That is what makes a
// scenario repeatable — two runs of the same spike send byte-identical records
// in the same order, so a difference in the controller's reason-code sequence
// is a difference in the controller.

// epoch is the base timestamp. A fixed instant rather than time.Now, so that
// the records a scenario sends today are the records it sent last week.
var epoch = time.Date(2026, 3, 14, 12, 0, 0, 0, time.UTC)

// nodes and counters are small closed sets, so a run produces a realistic mix
// of series rather than one series repeated — the normalized output is then
// something you can actually group by.
var (
	nodes    = []string{"epdg-01", "epdg-02", "epdg-03", "mme-01", "ggsn-01"}
	counters = []string{
		"pdp.sessions.active",
		"pdp.sessions.created",
		"bearer.setup.success",
		"bearer.setup.failure",
		"gtp.bytes.uplink",
		"gtp.bytes.downlink",
	}
)

// record builds record n.
//
// pad appends filler to the counter name, which is how payload size is
// controlled without inventing a field the Normalizer does not accept. The
// filler is deterministic, so padding changes the record's size and its
// checksum but never its reproducibility.
func record(n, pad int) string {
	observedAt := epoch.Add(time.Duration(n) * time.Second).UTC().Format(time.RFC3339)
	node := nodes[n%len(nodes)]
	counter := counters[(n/len(nodes))%len(counters)]
	if pad > 0 {
		counter += "." + strings.Repeat("x", pad)
	}
	// A value that varies per record but stays well inside int64, so a long run
	// cannot start emitting records the Normalizer rejects.
	value := (n*7919 + 1013) % 1_000_000

	return fmt.Sprintf("%s,%s,%s,%d", observedAt, node, counter, value)
}

// file builds the contents of one input file: recordsPerFile records, one per
// line, starting at the given offset.
func file(offset, recordsPerFile, pad int) string {
	var builder strings.Builder
	for i := range recordsPerFile {
		builder.WriteString(record(offset+i, pad))
		builder.WriteByte('\n')
	}
	return builder.String()
}
