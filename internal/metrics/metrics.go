// Package metrics defines knell's Prometheus metrics and the registry that
// serves them. Metrics are package-level singletons registered once at init
// (registration only panics on programmer error: a duplicate or invalid
// name); the exposition prefix is "knell_".
package metrics

import (
	"strings"

	metricslib "github.com/cplieger/metrics/v3"
)

// beatLabel names the watched beat on per-beat metrics; kindLabel names the
// notification kind (missing, recovered, history) on the delivery counters.
const (
	beatLabel = "beat"
	kindLabel = "kind"
)

// Notification kinds are the legal values of kindLabel on the sent, failed
// and dropped notification counters; dashboards and the KnellNotifyFailing
// alert key on them. They count MESSAGES, so KindHistory moves by one for a
// notice covering any number of ended outages; BeatOutages counts outages.
const (
	KindMissing   = "missing"
	KindRecovered = "recovered"
	KindHistory   = "history"
)

// notificationKinds is the single enumeration of the legal kindLabel values:
// it drives both the zero-minting below and the rendered kind list in the
// three counters' HELP text, so a new kind cannot be minted while the
// exposition metadata still advertises the old set.
var notificationKinds = []string{KindMissing, KindRecovered, KindHistory}

// notificationKindsText renders notificationKinds for HELP strings.
var notificationKindsText = strings.Join(notificationKinds, ", ")

// mintNotificationKinds pre-mints every notification counter series at zero
// for every kind, so an increase() alert sees the very first failure or drop:
// a counter series born at a nonzero value has no earlier sample to diff
// against. Dropped is minted for all three kinds like
// its siblings, so the alert's selector covers the whole set from a cold start
// (only missing and recovered have a drop path today; a failed history send
// keeps its records and retries). Called from init() below, after the
// registrations, so the guarantee cannot be lost by a path that serves
// /metrics without building a Watcher.
func mintNotificationKinds() {
	for _, kind := range notificationKinds {
		NotificationsSent.Add(0, kind)
		NotificationsFailed.Add(0, kind)
		NotificationsDropped.Add(0, kind)
	}
}

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
	"Webhook notifications delivered, by kind ("+notificationKindsText+"); one per delivered message.",
	[]string{kindLabel},
)

// NotificationsFailed counts webhook delivery attempts that failed after
// retries, by kind. That is its only meaning: something was sent and did not
// get through. A failed missing or history delivery is retried on the next
// watch tick; a recovered notification is best-effort. One increment per
// failed MESSAGE, so a failed history message counts once whatever number of
// ended outages it covered. A notification that was never attempted because
// its record was discarded by a full queue is NOT counted here (nothing
// failed and nothing will retry): see NotificationsDropped.
var NotificationsFailed = metricslib.NewLabeledCounter(
	"notifications_failed_total",
	"Webhook delivery attempts that failed after retries, by kind ("+notificationKindsText+"); one per failed message.",
	[]string{kindLabel},
)

// NotificationsDropped counts notifications that will never be delivered
// because their record was discarded when the per-beat queue was full, by
// kind. It is distinct from NotificationsFailed: a failed delivery still has
// its record and retries, while a dropped one has nothing left to retry from,
// so no notice for that transition will ever arrive. Reconstruct the missed
// window from BeatLastSeen; BeatOutages already counted the outage itself at
// detection. A queue-full event that loses nothing — an ongoing outage that
// stays detected and is queued once a slot frees — is not counted here
// either, because nothing was dropped.
var NotificationsDropped = metricslib.NewLabeledCounter(
	"notifications_dropped_total",
	"Notifications that will never be delivered because their record was discarded when the per-beat queue was full, by kind ("+notificationKindsText+"); distinct from a delivery that failed and will retry.",
	[]string{kindLabel},
)

func init() {
	Registry.RegisterLabeledGauge(BeatFresh)
	Registry.RegisterLabeledGauge(BeatLastSeen)
	Registry.RegisterLabeledCounter(BeatsReceived)
	Registry.RegisterLabeledCounter(BeatOutages)
	Registry.RegisterLabeledCounter(NotificationsSent)
	Registry.RegisterLabeledCounter(NotificationsFailed)
	Registry.RegisterLabeledCounter(NotificationsDropped)
	mintNotificationKinds()
}
