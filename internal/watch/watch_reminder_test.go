package watch

import (
	"context"
	"errors"
	"log/slog"
	"slices"
	"testing"
	"time"

	"github.com/cplieger/go/slogx/capture"
)

func TestMissingNoticeRepeatsWhileTheBeatStaysMissing(t *testing.T) {
	t.Parallel()

	const id = "reminder-cadence-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute, Repeat: 6 * time.Hour})

	clock.Advance(11 * time.Minute)
	w.sweep(t.Context())
	clock.Advance(6*time.Hour - time.Second)
	w.sweep(t.Context())
	if got := n.snapshot(); len(got) != 1 {
		t.Fatalf("calls one second before the repeat interval = %v, want only the first missing notice", got)
	}
	clock.Advance(time.Second)
	w.sweep(t.Context())
	w.sweep(t.Context())
	clock.Advance(6 * time.Hour)
	w.sweep(t.Context())

	want := []call{
		{kind: "missing", id: id, elapsed: 11 * time.Minute},
		{kind: "missing", id: id, elapsed: 6*time.Hour + 11*time.Minute},
		{kind: "missing", id: id, elapsed: 12*time.Hour + 11*time.Minute},
	}
	if got := n.snapshot(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v: one missing notice per repeat interval while the beat stays silent, each reporting the whole silence so far", got, want)
	}
}

func TestPingEndsTheRemindersAndTheNextOutageRestartsTheCadence(t *testing.T) {
	t.Parallel()

	const id = "reminder-recovery-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute, Repeat: time.Hour})

	clock.Advance(11 * time.Minute)
	w.sweep(t.Context())
	clock.Advance(time.Hour)
	w.sweep(t.Context())
	if !recordedBeat(w, id) {
		t.Fatalf("Beat(%s) refused", id)
	}
	drainRecoveries(w)
	clock.Advance(5 * time.Minute)
	w.sweep(t.Context())

	// A second outage: its first notice comes at its own deadline crossing, and
	// its reminder one interval after that notice, not after the first outage's.
	clock.Advance(6 * time.Minute)
	w.sweep(t.Context())
	clock.Advance(time.Hour - time.Second)
	w.sweep(t.Context())
	clock.Advance(time.Second)
	w.sweep(t.Context())

	want := []call{
		{kind: "missing", id: id, elapsed: 11 * time.Minute},
		{kind: "missing", id: id, elapsed: time.Hour + 11*time.Minute},
		{kind: "recovered", id: id, elapsed: time.Hour + 11*time.Minute},
		{kind: "missing", id: id, elapsed: 11 * time.Minute},
		{kind: "missing", id: id, elapsed: time.Hour + 11*time.Minute},
	}
	if got := n.snapshot(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
}

func TestPingDuringAReminderSendStillDeliversTheRecovery(t *testing.T) {
	t.Parallel()

	const id = "reminder-race-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute, Repeat: time.Hour})

	clock.Advance(11 * time.Minute)
	w.sweep(t.Context())
	clock.Advance(time.Hour)
	n.onMissing = func() { w.Beat(id) }
	w.sweep(t.Context())
	n.onMissing = nil
	drainRecoveries(w)
	clock.Advance(5 * time.Minute)
	w.sweep(t.Context())

	want := []call{
		{kind: "missing", id: id, elapsed: 11 * time.Minute},
		{kind: "missing", id: id, elapsed: time.Hour + 11*time.Minute},
		{kind: "recovered", id: id, elapsed: time.Hour + 11*time.Minute},
	}
	if got := n.snapshot(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v: the ping that lands while a reminder is in flight ends the outage, so its recovered notice follows and no reminder comes after it", got, want)
	}
}

func TestFailedReminderRetriesNextSweepAndCountsAsAFailedMissingSend(t *testing.T) {
	// Serial (no t.Parallel): asserts deltas on the package-global notification
	// counters, which the parallel tests also increment.
	const id = "reminder-failure-probe"
	w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute, Repeat: time.Hour})

	clock.Advance(11 * time.Minute)
	w.sweep(t.Context())
	clock.Advance(time.Hour)

	failedBefore := counterValue(t, "knell_notifications_failed_total", "missing")
	sentBefore := counterValue(t, "knell_notifications_sent_total", "missing")
	n.setFail(errors.New("discord down"))
	w.sweep(t.Context())
	if got := counterValue(t, "knell_notifications_failed_total", "missing"); got != failedBefore+1 {
		t.Errorf("failed{missing} = %v after a failed reminder, want %v", got, failedBefore+1)
	}

	n.setFail(nil)
	clock.Advance(15 * time.Second)
	w.sweep(t.Context())
	if got := counterValue(t, "knell_notifications_sent_total", "missing"); got != sentBefore+1 {
		t.Errorf("sent{missing} = %v after the retried reminder, want %v", got, sentBefore+1)
	}
	// The next reminder is due one interval after the one that was delivered.
	clock.Advance(time.Hour - time.Second)
	w.sweep(t.Context())
	clock.Advance(time.Second)
	w.sweep(t.Context())

	want := []call{
		{kind: "missing", id: id, elapsed: 11 * time.Minute},
		{kind: "missing", id: id, elapsed: time.Hour + 11*time.Minute + 15*time.Second},
		{kind: "missing", id: id, elapsed: 2*time.Hour + 11*time.Minute + 15*time.Second},
	}
	if got := n.snapshot(); !slices.Equal(got, want) {
		t.Errorf("calls = %v, want %v", got, want)
	}
}

// TestReminderLogsCarryTheirRetryability pins the retryable attribute the
// KnellNoticeLostForGood log rule reads. A lost reminder loses nothing the
// channel lacks, because the outage's first missing notice was delivered, so
// neither a failed nor an abandoned reminder may log retryable=false.
func TestReminderLogsCarryTheirRetryability(t *testing.T) {
	// Serial (no t.Parallel): capture.Default swaps the process-global slog
	// default.
	const id = "reminder-retryability-probe"
	cases := map[string]struct {
		err   error
		level slog.Level
		msg   string
	}{
		"a failed reminder is retried": {
			err:   errors.New("discord down"),
			level: slog.LevelError,
			msg:   "missing reminder failed",
		},
		"a reminder abandoned at shutdown is not a lost notice": {
			err:   context.Canceled,
			level: slog.LevelInfo,
			msg:   "missing reminder abandoned",
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			w, clock, n := newTestWatcher(Beat{ID: id, Deadline: 10 * time.Minute, Repeat: time.Hour})
			clock.Advance(11 * time.Minute)
			w.sweep(t.Context())
			clock.Advance(time.Hour)
			n.setFail(tc.err)
			rec := capture.Default(t)
			w.sweep(t.Context())
			if got := rec.CountLevel(tc.level, tc.msg); got != 1 {
				t.Fatalf("%q lines at %s = %d, want 1: %v", tc.msg, tc.level, got, rec.Messages())
			}
			if !rec.HasAttr(tc.msg, "retryable", "true") {
				t.Errorf("%q does not carry retryable=true: %v", tc.msg, rec.Records())
			}
		})
	}
}
