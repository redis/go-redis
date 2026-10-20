package pubsub

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
)

// dialer runs the shared connection's dials in the background, off the
// owner's lock: the network part — the dial, the handshake, the retries
// and the backoff between them — runs with the lock released, so
// nothing that takes the lock (writes, the read loop's fan-out, Close)
// ever waits for it. Dials are single-flight: a caller finding one in
// flight joins it rather than starting another.
//
// The dialer is driven under the owner's lock (lock, Manager.mu): the
// owner starts or joins a dial with it held (dialLocked), and the dialer
// takes it to pick a retry's target and to hand the outcome back
// (dialPlan's callbacks). The in-flight call is cleared under the same
// hold that installs its outcome, so no caller can observe the gap
// between a finished dial and its installation and start a second one.
type dialer struct {
	lock    sync.Locker
	newConn func(ctx context.Context, addr string) (*pool.Conn, error)

	// retries is the number of extra attempts after a failed one before
	// the failure is reported; retryBackoff paces them (see
	// Config.DialRetryBackoff). minBackoff and maxBackoff bound the
	// owner's reconnect backoff between dials, and retryBackoff's
	// fallback.
	retries      int
	retryBackoff func(attempt int) time.Duration
	minBackoff   time.Duration
	maxBackoff   time.Duration

	// ctx bounds every attempt; cancel aborts the dial in flight when
	// the owner closes.
	ctx    context.Context
	cancel context.CancelFunc

	// call is the dial in flight, nil when none. Guarded by lock.
	call *dialCall
}

// dialPlan is what a new dial needs from its owner, prepared under the
// owner's lock when the dial starts (joiners reuse the plan in flight).
type dialPlan struct {
	// addr is the first attempt's target.
	addr string
	// values supplies every attempt's context values (e.g. what a
	// CredentialsProviderContext reads during the handshake); it carries
	// no cancellation — attempts are bounded by timeout and the dialer's
	// own cancellation only, as the dial may outlive the caller that
	// started it. nil means none.
	values context.Context
	// timeout bounds each attempt; <= 0 leaves only the dialer's own
	// cancellation.
	timeout time.Duration
	// nextAddrLocked (optional) picks a retry's target, with the owner's
	// lock held; nil retries addr.
	nextAddrLocked func() string
	// installLocked receives the outcome — the connection, or the last
	// attempt's error — with the owner's lock held; it owns the
	// connection from then on, and its error is the call's.
	installLocked func(cn *pool.Conn, err error) error
}

// dialCall is one dial in flight, awaited by every caller that started
// or joined it.
type dialCall struct {
	done chan struct{}
	err  error // installLocked's verdict; set before done closes
}

func newDialer(
	lock sync.Locker,
	newConn func(ctx context.Context, addr string) (*pool.Conn, error),
	retries int,
	retryBackoff func(attempt int) time.Duration,
	minBackoff, maxBackoff time.Duration,
) *dialer {
	// Floor the backoff: a dial can fail instantly, and MinRetryBackoff
	// -1 ("never back off") would spin the reconnects hot.
	if minBackoff <= 0 {
		minBackoff = 10 * time.Millisecond
	}
	maxBackoff = max(maxBackoff, minBackoff)
	if retryBackoff == nil {
		retryBackoff = func(attempt int) time.Duration {
			return internal.RetryBackoff(attempt, minBackoff, maxBackoff)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &dialer{
		lock:         lock,
		newConn:      newConn,
		retries:      max(retries, 0),
		retryBackoff: retryBackoff,
		minBackoff:   minBackoff,
		maxBackoff:   maxBackoff,
		ctx:          ctx,
		cancel:       cancel,
	}
}

// dialLocked joins the dial in flight or, with none, starts one from
// plan (called only then). Callers must hold the owner's lock.
func (d *dialer) dialLocked(plan func() dialPlan) *dialCall {
	if d.call != nil {
		return d.call
	}
	call := &dialCall{done: make(chan struct{})}
	d.call = call
	go d.run(call, plan())
	return call
}

// inFlightLocked reports whether a dial is in flight. Callers must hold
// the owner's lock.
func (d *dialer) inFlightLocked() bool {
	return d.call != nil
}

// close aborts the dial in flight, if any, and every later one: its
// attempt fails with the cancellation and the outcome is still handed
// to installLocked. Safe to call with or without the owner's lock.
func (d *dialer) close() {
	d.cancel()
}

// run makes the attempts — the retries re-targeted by nextAddrLocked
// and paced by the backoff — then hands the outcome to installLocked
// and settles the call.
func (d *dialer) run(call *dialCall, plan dialPlan) {
	addr := plan.addr
	var (
		cn  *pool.Conn
		err error
	)
	for attempt := 0; ; attempt++ {
		if attempt > 0 {
			if !d.sleep(max(d.retryBackoff(attempt-1), 0)) {
				break // closed: err is the last attempt's
			}
			if plan.nextAddrLocked != nil {
				d.lock.Lock()
				addr = plan.nextAddrLocked()
				d.lock.Unlock()
			}
		}
		cn, err = d.attempt(plan.values, addr, plan.timeout)
		if err == nil || attempt >= d.retries || d.ctx.Err() != nil {
			break
		}
	}

	// Settle the call under the same hold that installs its outcome, so a
	// caller back under the lock sees a settled call iff it was installed
	// (see settled).
	d.lock.Lock()
	d.call = nil
	call.err = plan.installLocked(cn, err)
	close(call.done)
	d.lock.Unlock()
}

// attempt makes one dial to addr with values' context values (nil:
// none), bounded by timeout (when positive) and by the dialer's
// cancellation.
func (d *dialer) attempt(values context.Context, addr string, timeout time.Duration) (*pool.Conn, error) {
	ctx := d.ctx
	if values != nil {
		var cancel context.CancelFunc
		ctx, cancel = context.WithCancel(context.WithoutCancel(values))
		defer cancel()
		stop := context.AfterFunc(d.ctx, cancel)
		defer stop()
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}
	return d.newConn(ctx, addr)
}

// sleep pauses for d, reporting false if the dialer closed meanwhile.
func (d *dialer) sleep(pause time.Duration) bool {
	t := time.NewTimer(pause)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-d.ctx.Done():
		return false
	}
}

// wait blocks until the call settles, returning its error, or until ctx
// is done. Abandoning the wait does not abandon the dial: the call still
// hands its outcome to its owner. A call settled by the time ctx is done
// reports its outcome, not ctx's error.
func (c *dialCall) wait(ctx context.Context) error {
	select {
	case <-c.done:
		return c.err
	case <-ctx.Done():
		if settled, err := c.settled(); settled {
			return err
		}
		return ctx.Err()
	}
}

// settled reports whether the call has settled and, if so, its error.
// Under the owner's lock it is exact: the call settles under the same
// hold that installs its outcome.
func (c *dialCall) settled() (bool, error) {
	select {
	case <-c.done:
		return true, c.err
	default:
		return false, nil
	}
}
