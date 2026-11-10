package main

import (
	"errors"
	"net"

	"github.com/cplieger/knell/internal/obs"
)

// acceptErrorCounter counts every failed Accept except the close that ends
// serving, so KnellAcceptFailing reads a series instead of net/http's log line.
type acceptErrorCounter struct {
	net.Listener
}

func countAcceptErrors(ln net.Listener) net.Listener {
	return acceptErrorCounter{Listener: ln}
}

func (l acceptErrorCounter) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err != nil && !errors.Is(err, net.ErrClosed) {
		obs.RecordAcceptError()
	}
	return conn, err
}
