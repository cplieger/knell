package main

import (
	"fmt"
	"io"
	"time"

	"github.com/cplieger/go/health/v2/probe"
)

// probeHealth is `knell health`: it exits 0 only when /healthz answers 2xx
// within timeout, so a listener that is bound but no longer accepting fails.
func probeHealth(stderr io.Writer, listenAddr string, timeout time.Duration) int {
	url, err := probe.LocalURL(listenAddr, "/healthz")
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "unhealthy: LISTEN_ADDR %q: %v\n", listenAddr, err)
		return 1
	}
	return probe.Check(stderr, timeout, url)
}
