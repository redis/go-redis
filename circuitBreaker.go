package redis

import (
	"errors"
	"fmt"
	"math/rand"
	"sync/atomic"
	"time"

	metrics "github.com/bountylabs/go-metrics"
	"github.com/facebookgo/clock"
)

var ErrCircuitBroken = errors.New("circuit broken")

type State string

const (
	Intact  State = "Intact"
	Broken  State = "Broken"
	Partial State = "Partial"
)

//breakerImpl implements the Breaker interface as a self healing circuit over backoff duration.
type breakerImpl struct {
	rng            *rand.Rand
	min            time.Duration
	backoff        time.Duration
	lastBrokenTime int64 // nanos
	clock          clock.Clock
	meterErrs      metrics.Meter
}

func (b *breakerImpl) Break() {
	atomic.StoreInt64(&b.lastBrokenTime, b.clock.Now().UnixNano())
}

func (b *breakerImpl) Repair() {
	//This circuit can only be repaired by waiting backoff time
}

func (b *breakerImpl) Allow() error {
	allow, _ := b.allow()
	if allow {
		return nil
	}
	return ErrCircuitBroken
}

func (b *breakerImpl) ReportResult(result error) {
	b.meterErrs.Mark(1)
	return
}

func (b *breakerImpl) State() State {
	_, state := b.allow()
	return state
}

//Allow returns true if we are more than backoff duration from the last time
//the circuit was broken. If the circuit has been recently broken a percentage
//of requests will be allowed to proceed increasing to 100% as we near backoff
//duration.
func (b *breakerImpl) allow() (bool, State) {

	var (
		now            = b.clock.Now()
		lastBreakNanos = atomic.LoadInt64(&b.lastBrokenTime)
		timeSinceEvent = now.Sub(time.Unix(0, lastBreakNanos))
	)

	//circuit has never been broken
	if lastBreakNanos == 0 {
		return true, Intact
	}

	//circuit was broken but it has been more than backoff since the last break
	//so it has fully repaired
	if timeSinceEvent >= b.backoff {
		return true, Intact
	}

	//The circuit was broken and we havn't reached the min duration meaning the
	//circuit is fully broken
	if timeSinceEvent <= b.min {
		return false, Broken
	}

	//(x*x*x*12.5) => as x goes from:1-2, y goes from 10->100
	x := (float64(timeSinceEvent) / float64(b.backoff)) + 1 //1-2
	allow := x * x * x * 12.5

	// Example: backoff=200s
	//1 second = > 12.6884390625
	//2 second => 12.8787625
	//10 seconds => 14.4703125
	//30 seconds => 19.0109375
	//60 seconds => 27.4625
	//120 seconds => 51.2
	//160 seconds => 72.9
	//180 seconds => 85.7375
	//200 seconds => 100
	return b.rand() < int(allow), Partial
}

func (b *breakerImpl) rand() int {
	if b.rng != nil {
		return b.rng.Intn(100)
	}
	return rand.Intn(100)
}

func NewBreaker(backoff time.Duration) *breakerImpl {
	return &breakerImpl{
		backoff:   backoff,
		clock:     clock.New(),
		min:       time.Second,
		meterErrs: metrics.GetOrRegisterMeter(fmt.Sprintf("%s/errors", "node"), metrics.NewRegistry()),
	}
}

func NewBreakerWithSource(backoff time.Duration, src rand.Source) *breakerImpl {
	return &breakerImpl{
		backoff: backoff,
		rng:     rand.New(src),
		clock:   clock.New(),
		min:     time.Second,
	}
}

type NilBreaker struct{}

func (NilBreaker) Break()       {}
func (NilBreaker) Allow() bool  { return true }
func (NilBreaker) State() State { return Intact }
func (NilBreaker) Repair()      {}
