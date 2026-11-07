package main

import (
	"errors"
	"net"
	"syscall"
	"testing"
)

// scriptedListener answers Accept with its queued errors, one per call. Only
// Accept is exercised; the embedded nil Listener panics if anything else is.
type scriptedListener struct {
	net.Listener
	errs []error
}

func (l *scriptedListener) Accept() (net.Conn, error) {
	err := l.errs[0]
	l.errs = l.errs[1:]
	return nil, err
}

// TestAcceptErrorCounterCountsFailedAcceptsButNotTheShutdownClose pins the
// listener half of KnellAcceptFailing: a failed accept moves
// knell_listener_accept_errors_total by one, and the close that httpserver's
// shutdown performs does not, or every clean stop would page.
func TestAcceptErrorCounterCountsFailedAcceptsButNotTheShutdownClose(t *testing.T) {
	const series = "knell_listener_accept_errors_total"
	exhausted := &net.OpError{Op: "accept", Net: "tcp", Err: syscall.EMFILE}
	closed := &net.OpError{Op: "accept", Net: "tcp", Err: net.ErrClosed}
	ln := countAcceptErrors(&scriptedListener{errs: []error{exhausted, closed}})

	before := scrapeCounter(t, series)
	if _, err := ln.Accept(); !errors.Is(err, syscall.EMFILE) {
		t.Fatalf("Accept = %v, want the listener's own error passed through unchanged", err)
	}
	if got := scrapeCounter(t, series); got != before+1 {
		t.Errorf("%s after one failed accept = %v, want %v", series, got, before+1)
	}

	if _, err := ln.Accept(); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("Accept after close = %v, want net.ErrClosed passed through", err)
	}
	if got := scrapeCounter(t, series); got != before+1 {
		t.Errorf("%s after the shutdown close = %v, want it unchanged at %v", series, got, before+1)
	}
}

// TestAcceptErrorCounterLeavesASuccessfulAcceptUncounted pins that a served
// connection is not an error.
func TestAcceptErrorCounterLeavesASuccessfulAcceptUncounted(t *testing.T) {
	const series = "knell_listener_accept_errors_total"
	inner, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ln := countAcceptErrors(inner)
	t.Cleanup(func() { _ = ln.Close() })

	dialed := make(chan error, 1)
	go func() {
		c, derr := net.Dial("tcp", inner.Addr().String())
		if derr == nil {
			derr = c.Close()
		}
		dialed <- derr
	}()

	before := scrapeCounter(t, series)
	conn, err := ln.Accept()
	if err != nil {
		t.Fatalf("Accept: %v", err)
	}
	_ = conn.Close()
	if derr := <-dialed; derr != nil {
		t.Fatalf("dial: %v", derr)
	}
	if got := scrapeCounter(t, series); got != before {
		t.Errorf("%s after a served connection = %v, want it unchanged at %v", series, got, before)
	}
}
