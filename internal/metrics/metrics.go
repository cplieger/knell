// Package metrics defines knell's Prometheus metrics and the registry that
// serves them. Metrics are package-level singletons registered once at init
// (registration only panics on programmer error: a duplicate or invalid
// name); the exposition prefix is "knell_".
//
// The registry and the collectors are unexported: the package's edge is
// Handler plus the knell-semantic recording functions below (InitBeat,
// RecordBeat, SetBeatFresh, RecordOutage and the three RecordNotification*
// functions). Callers therefore cannot register, rename or delete a series,
// nor write a raw label position, which is what keeps the metric names, label
// sets and cold-start zero samples the quorum and delivery alerts read in one
// place. Label CARDINALITY is not structurally contained: InitBeat mints a
// per-beat series for whatever id it is handed, so keeping the beat label
// bounded stays the caller's contract (watch.New passes only configured ids;
// unknown ids answer 404 in webapi and never reach here).
package metrics

import (
	"net/http"
	"strings"
	"time"

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
// notice covering any number of ended outages; beatOutages counts outages.
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
		notificationsSent.Add(0, kind)
		notificationsFailed.Add(0, kind)
		notificationsDropped.Add(0, kind)
	}
}

// registry serves every registered metric plus process metrics on /metrics;
// Handler is its only exported view.
var registry = metricslib.NewRegistry("knell")

// beatFresh reports per beat whether the last ping is within its deadline
// (1) or the beat is overdue (0). This is the aggregation input for
// multi-observer quorum rules.
var beatFresh = metricslib.NewLabeledGauge(
	"beat_fresh",
	"Whether the beat's last ping is within its deadline (1 = fresh, 0 = overdue).",
	[]string{beatLabel},
)

// beatLastSeen is the Unix timestamp of each beat's last accepted ping.
// Until a first ping arrives it carries the process start time (the boot
// baseline the deadline counts from).
var beatLastSeen = metricslib.NewLabeledGauge(
	"beat_last_seen_timestamp_seconds",
	"Unix timestamp of the beat's last accepted ping (process start until the first ping).",
	[]string{beatLabel},
)

// beatsReceived counts accepted pings per beat. Unknown beat ids are
// rejected with 404 and deliberately not counted (the id is a label; counting
// arbitrary request paths would let callers mint unbounded series).
var beatsReceived = metricslib.NewLabeledCounter(
	"beats_received_total",
	"Accepted pings per beat (unknown ids are rejected and not counted).",
	[]string{beatLabel},
)

// beatOutages counts detected outages per beat: exactly one increment for
// every deadline crossing the state machine detects, at detection time and
// independent of notification delivery. An outage whose notice was never
// delivered, or was dropped by a full queue, or was collapsed with others
// into one history message is counted here all the same. This is the only
// series that counts OUTAGES: the notification counters count MESSAGES, and
// one history message can cover several outages.
var beatOutages = metricslib.NewLabeledCounter(
	"beat_outages_total",
	"Detected outages per beat: one increment per deadline crossing detected, independent of notification delivery.",
	[]string{beatLabel},
)

// notificationsSent counts webhook notifications delivered, by kind
// (missing, recovered, history). One increment per delivered MESSAGE: a
// history message covering several ended outages counts once, so pair this
// with beatOutages to reason about outages rather than messages.
var notificationsSent = metricslib.NewLabeledCounter(
	"notifications_sent_total",
	"Webhook notifications delivered, by kind ("+notificationKindsText+"); one per delivered message.",
	[]string{kindLabel},
)

// notificationsFailed counts webhook delivery attempts that failed after
// retries, by kind. That is its only meaning: something was sent and did not
// get through. A failed missing or history delivery is retried on the next
// watch tick; a recovered notification is best-effort. One increment per
// failed MESSAGE, so a failed history message counts once whatever number of
// ended outages it covered. A notification that was never attempted because
// its record was discarded by a full queue is NOT counted here (nothing
// failed and nothing will retry): see notificationsDropped.
var notificationsFailed = metricslib.NewLabeledCounter(
	"notifications_failed_total",
	"Webhook delivery attempts that failed after retries, by kind ("+notificationKindsText+"); one per failed message.",
	[]string{kindLabel},
)

// notificationsDropped counts notifications that will never be delivered
// because their record was discarded when the per-beat queue was full, by
// kind. It is distinct from notificationsFailed: a failed delivery still has
// its record and retries, while a dropped one has nothing left to retry from,
// so no notice for that transition will ever arrive. Reconstruct the missed
// window from beatLastSeen; beatOutages already counted the outage itself at
// detection. A queue-full event that loses nothing — an ongoing outage that
// stays detected and is queued once a slot frees — is not counted here
// either, because nothing was dropped.
var notificationsDropped = metricslib.NewLabeledCounter(
	"notifications_dropped_total",
	"Notifications that will never be delivered because their record was discarded when the per-beat queue was full, by kind ("+notificationKindsText+"); distinct from a delivery that failed and will retry.",
	[]string{kindLabel},
)

func init() {
	registry.RegisterLabeledGauge(beatFresh)
	registry.RegisterLabeledGauge(beatLastSeen)
	registry.RegisterLabeledCounter(beatsReceived)
	registry.RegisterLabeledCounter(beatOutages)
	registry.RegisterLabeledCounter(notificationsSent)
	registry.RegisterLabeledCounter(notificationsFailed)
	registry.RegisterLabeledCounter(notificationsDropped)
	mintNotificationKinds()
}

// Handler serves the Prometheus exposition: every registered metric plus
// process metrics. It is the only exported view of the registry, so a
// consumer can scrape the metrics but cannot mint, delete or re-register a
// series.
func Handler() http.Handler {
	return registry.Handler()
}

// InitBeat declares a configured beat at process start: the beat begins fresh
// with start as its last-seen baseline (the boot-armed clock every first
// deadline counts from), and its per-beat counters are pre-minted at zero for
// the same reason mintNotificationKinds pre-mints the notification counters —
// increase() needs an earlier sample to diff against. Only configured ids
// reach here, which is what keeps the beat label bounded.
func InitBeat(id string, start time.Time) {
	beatFresh.Set(1, id)
	beatLastSeen.Set(float64(start.Unix()), id)
	beatsReceived.Add(0, id)
	beatOutages.Add(0, id)
}

// RecordBeat records an accepted ping for id observed at now: the ping is
// counted and the beat is published fresh with its new last-seen timestamp.
// The caller publishes under its own lock so concurrent pings cannot write
// the gauges out of state order.
func RecordBeat(id string, now time.Time) {
	beatsReceived.Inc(id)
	beatFresh.Set(1, id)
	beatLastSeen.Set(float64(now.Unix()), id)
}

// SetBeatFresh publishes the quorum gauge for id: fresh when the last ping is
// within the beat's deadline, overdue otherwise. The freshness boundary
// itself belongs to the watch state machine; this is only the exposition of
// its verdict.
func SetBeatFresh(id string, fresh bool) {
	if fresh {
		beatFresh.Set(1, id)
		return
	}
	beatFresh.Set(0, id)
}

// RecordOutage counts one detected outage for id, at detection time and
// independent of whether its notice is ever delivered. It is the only
// counter that counts OUTAGES rather than messages.
func RecordOutage(id string) {
	beatOutages.Inc(id)
}

// RecordNotificationSent counts one delivered notification message of kind.
func RecordNotificationSent(kind string) {
	notificationsSent.Inc(kind)
}

// RecordNotificationFailed counts one notification message of kind whose
// delivery was attempted and failed after retries. Its record survives, so
// the send retries: this is never the counter for something that was dropped
// unsent.
func RecordNotificationFailed(kind string) {
	notificationsFailed.Inc(kind)
}

// RecordNotificationDropped counts one notification message of kind that will
// never be delivered because the record it would have been built from was
// discarded by a full queue. Nothing retries it.
func RecordNotificationDropped(kind string) {
	notificationsDropped.Inc(kind)
}
