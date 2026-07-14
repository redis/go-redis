package pool

import (
	"sync"
	"time"
)

// dialLimiter is a token-bucket rate limiter that paces the creation of new
// connections. It is only consulted on the slow path of Get() (when no idle
// connection is available and a new one would otherwise be dialed), so it is
// never in the hot path of an idle-connection hit.
//
// The bucket starts full (burst tokens) and refills at rate tokens per second,
// capped at burst. Allow() consumes a single token if one is available.
// delayUntilNext() reports how long until the next token becomes available so a
// throttled caller can park for that long while it waits for an idle connection
// to be returned instead.
type dialLimiter struct {
	mu     sync.Mutex
	rate   float64 // tokens (connection dials) added per second
	burst  float64 // maximum number of tokens the bucket can hold
	tokens float64
	last   time.Time

	// now is injectable so tests can drive the limiter deterministically.
	now func() time.Time
}

// newDialLimiter returns a token-bucket limiter, or nil if rate limiting is
// disabled (ratePerSec <= 0). When burst <= 0 it defaults to ratePerSec so that
// a full second's worth of dials is allowed to burst before throttling kicks in.
func newDialLimiter(ratePerSec, burst int) *dialLimiter {
	if ratePerSec <= 0 {
		return nil
	}
	b := float64(burst)
	if burst <= 0 {
		b = float64(ratePerSec)
	}
	l := &dialLimiter{
		rate:  float64(ratePerSec),
		burst: b,
		now:   time.Now,
	}
	l.last = l.now()
	l.tokens = b
	return l
}

// refillLocked adds the tokens accrued since the last update. Caller holds mu.
func (l *dialLimiter) refillLocked(now time.Time) {
	elapsed := now.Sub(l.last)
	if elapsed <= 0 {
		return
	}
	l.tokens += elapsed.Seconds() * l.rate
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.last = now
}

// Allow consumes a token and returns true if one was available.
// It is the single gate through which a throttled dial is admitted: when many
// parked callers wake simultaneously, exactly one of them wins the token here
// (the decrement is serialized by mu) and the rest re-park, which is what paces
// creation instead of letting a burst through all at once.
func (l *dialLimiter) Allow() bool {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refillLocked(now)
	if l.tokens >= 1 {
		l.tokens--
		return true
	}
	return false
}

// refund returns a token to the bucket, capped at burst. Used when a caller
// acquired a token but could not proceed (e.g. a min-idle refill that found no
// free pool turn), so the unused token isn't lost to other dialers.
func (l *dialLimiter) refund() {
	l.mu.Lock()
	l.tokens++
	if l.tokens > l.burst {
		l.tokens = l.burst
	}
	l.mu.Unlock()
}

// delayUntilNext returns the duration until at least one token is available.
// It returns 0 if a token is available right now.
func (l *dialLimiter) delayUntilNext() time.Duration {
	now := l.now()
	l.mu.Lock()
	defer l.mu.Unlock()
	l.refillLocked(now)
	if l.tokens >= 1 {
		return 0
	}
	needed := 1 - l.tokens
	return time.Duration(needed / l.rate * float64(time.Second))
}
