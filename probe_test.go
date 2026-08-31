package redis_test

import (
	"net"
	"time"
)

// probeRedis reports whether a TCP listener answers at addr within a short
// timeout. Tests that skip when Redis is absent call this before building a
// client and Ping-ing it: a Ping to an addr with nothing listening does not
// return immediately, it pays go-redis's dial-retry (DialerRetries, 5 by
// default) plus command-retry (MaxRetries) budget first — ~1.7s per call. In an
// environment without a local Redis (for example the RE nightly, whose real
// endpoint is remote), hundreds of such skips add up to minutes of wasted wall
// time. A single short-timeout TCP dial fails in microseconds on
// connection-refused instead.
//
// A nil return only means something is listening; callers keep their Ping so a
// reachable-but-unusable server (auth required, a different service on the port)
// still skips rather than proceeds.
func probeRedis(addr string) error {
	c, err := net.DialTimeout("tcp", addr, 300*time.Millisecond)
	if err != nil {
		return err
	}
	return c.Close()
}
