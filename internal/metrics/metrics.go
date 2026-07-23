// Package metrics defines knell's Prometheus metrics and the registry that
// serves them. Metrics are package-level singletons registered once at init
// (registration only panics on programmer error: a duplicate or invalid
// name); the exposition prefix is "knell_".
package metrics

import (
	metricslib "github.com/cplieger/metrics/v3"
)

// beatLabel is the label naming the watched beat on per-beat metrics.
const beatLabel = "beat"

// Registry serves every registered metric plus process metrics on /metrics.
var Registry = metricslib.NewRegistry("knell")

// BeatFresh reports per beat whether the last ping is within its deadline
// (1) or the beat is overdue (0). This is the aggregation input for
// multi-observer quorum rules.
var BeatFresh = metricslib.NewLabeledGauge(
	"beat_fresh",
	"Whether the beat's last ping is within its deadline (1 = fresh, 0 = overdue).",
	[]string{beatLabel},
)

// BeatLastSeen is the Unix timestamp of each beat's last accepted ping.
// Until a first ping arrives it carries the process start time (the boot
// baseline the deadline counts from).
var BeatLastSeen = metricslib.NewLabeledGauge(
	"beat_last_seen_timestamp_seconds",
	"Unix timestamp of the beat's last accepted ping (process start until the first ping).",
	[]string{beatLabel},
)

// BeatsReceived counts accepted pings per beat. Unknown beat ids are
// rejected with 404 and deliberately not counted (the id is a label; counting
// arbitrary request paths would let callers mint unbounded series).
var BeatsReceived = metricslib.NewLabeledCounter(
	"beats_received_total",
	"Accepted pings per beat (unknown ids are rejected and not counted).",
	[]string{beatLabel},
)

// NotificationsSent counts webhook notifications delivered, by kind
// (missing, recovered).
var NotificationsSent = metricslib.NewLabeledCounter(
	"notifications_sent_total",
	"Webhook notifications delivered, by kind (missing, recovered).",
	[]string{"kind"},
)

// NotificationsFailed counts webhook notifications that failed after
// retries, by kind. A missing notification that fails is retried on the
// next watch tick; a recovered notification is best-effort.
var NotificationsFailed = metricslib.NewLabeledCounter(
	"notifications_failed_total",
	"Webhook notifications that failed after retries, by kind (missing, recovered).",
	[]string{"kind"},
)

func init() {
	Registry.RegisterLabeledGauge(BeatFresh)
	Registry.RegisterLabeledGauge(BeatLastSeen)
	Registry.RegisterLabeledCounter(BeatsReceived)
	Registry.RegisterLabeledCounter(NotificationsSent)
	Registry.RegisterLabeledCounter(NotificationsFailed)
}
