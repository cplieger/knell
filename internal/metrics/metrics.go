// Package metrics defines knell's Prometheus metrics and the registry that
// serves them. Metrics are package-level singletons registered once at init
// (registration only panics on programmer error: a duplicate or invalid
// name); the exposition prefix is "knell_".
package metrics

import (
	metricslib "github.com/cplieger/metrics/v3"
)

// beatLabel names the watched beat on per-beat metrics; kindLabel names the
// notification kind (missing, recovered, history) on the delivery counters.
const (
	beatLabel = "beat"
	kindLabel = "kind"
)

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

// BeatOutages counts detected outages per beat: exactly one increment for
// every deadline crossing the state machine detects, at detection time and
// independent of notification delivery. An outage whose notice was never
// delivered, or was dropped by a full queue, or was collapsed with others
// into one history message is counted here all the same. This is the only
// series that counts OUTAGES: the notification counters count MESSAGES, and
// one history message can cover several outages.
var BeatOutages = metricslib.NewLabeledCounter(
	"beat_outages_total",
	"Detected outages per beat: one increment per deadline crossing detected, independent of notification delivery.",
	[]string{beatLabel},
)

// NotificationsSent counts webhook notifications delivered, by kind
// (missing, recovered, history). One increment per delivered MESSAGE: a
// history message covering several ended outages counts once, so pair this
// with BeatOutages to reason about outages rather than messages.
var NotificationsSent = metricslib.NewLabeledCounter(
	"notifications_sent_total",
	"Webhook notifications delivered, by kind (missing, recovered, history); one per delivered message.",
	[]string{kindLabel},
)

// NotificationsFailed counts unsuccessful webhook delivery attempts after
// retries and transitions a full queue could not accept, by kind. A closed
// outage that overflows is lost, while an ongoing one stays detectable and
// is queued once a slot opens; either case is accounted once per affected
// outage. A failed missing or history delivery is retried on the next watch
// tick; a recovered notification is best-effort. One increment per failed
// MESSAGE, so a failed history message counts once whatever number of ended
// outages it covered.
var NotificationsFailed = metricslib.NewLabeledCounter(
	"notifications_failed_total",
	"Unsuccessful webhook delivery attempts after retries and transitions a full queue could not accept, by kind (missing, recovered, history).",
	[]string{kindLabel},
)

func init() {
	Registry.RegisterLabeledGauge(BeatFresh)
	Registry.RegisterLabeledGauge(BeatLastSeen)
	Registry.RegisterLabeledCounter(BeatsReceived)
	Registry.RegisterLabeledCounter(BeatOutages)
	Registry.RegisterLabeledCounter(NotificationsSent)
	Registry.RegisterLabeledCounter(NotificationsFailed)
}
