package redis

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

// multidbFDRouter runs the ordered full-duplex autopipeline on a
// *MultiDBClient.
//
// A MultiDBClient holds no connection of its own; its members do. The router
// keeps one full-duplex child autopipeliner per member — a standalone FD
// engine on a *Client member, a cluster FD router on a *ClusterClient member —
// and sends each command to the ACTIVE member's child. The parent
// AutoPipeliner keeps diversion (blocking, fan-out, HIMPORT, mustDivert), so
// only batchable commands reach here.
//
// MultiDB semantics on the fast path:
//   - Admission: a command rides FD only on a closed-state breaker admission
//     with the failure detector not tripped. Anything else — no active
//     member, a tripped detector, an open circuit, a half-open probe — goes to
//     MultiDB's normal path (core.process), which owns failover and probes.
//   - Accounting: each child reports every command's outcome (fdOutcome) and
//     the router settles it into the member's breaker and the failure
//     detector, as core.process does. The breaker episode and the detector
//     window are read when the outcome lands, not when the command was
//     admitted: a command that outlives a reselect counts into the new window.
//   - Recovery: a child whose connection drops, or whose member cannot be
//     leased, reroutes its replayable work through MultiDB's pipeline path
//     (fdReroute), which re-gates and fails over to the newly active member.
//     A sent NoRetry command is still failed, never re-sent.
//
// Children are built privately (not through the member's cached autopipeline
// getters), so their owner wiring is always present.
type multidbFDRouter struct {
	parent   *AutoPipeliner
	c        *MultiDBClient
	blocking bool
	// childCfg is the base config every child is built from: the parent's,
	// forced to the ordered single-engine full-duplex combo.
	childCfg AutoPipelineOptions

	mu       sync.RWMutex
	closed   bool
	children map[*multidbDatabase]*AutoPipeliner
	// noFD holds members where full duplex does not engage (no pipeline pool,
	// replica routing on a cluster member, or a build error): their commands
	// divert without rebuilding a child each time.
	noFD map[*multidbDatabase]struct{}

	// Async face only: one ordered lane at a time (see submitOrdered). lane is
	// the autopipeliner commands go to now: the active member's FD child, or hd,
	// an ordered half-duplex MultiDB autopipeliner for commands that cannot
	// take the fast path. laneMu serializes lane switches and hd writes; hd is
	// atomic so len can read it without waiting out a lane switch's drain.
	lane   atomic.Pointer[AutoPipeliner]
	laneMu sync.Mutex
	hd     atomic.Pointer[AutoPipeliner]
	hdCfg  AutoPipelineOptions

	// removeHooks holds the members this router registered an onRemove hook
	// on (see childFor), so close can unregister them. Guarded by mu.
	removeHooks map[*multidbDatabase]struct{}
}

func newMultiDBFDRouter(parent *AutoPipeliner, c *MultiDBClient, cfg *AutoPipelineOptions, blocking bool) *multidbFDRouter {
	child := *cfg
	child.FullDuplex = true
	child.Unordered = false
	child.MaxConcurrentBatches = 1
	child.NumShards = 1
	child.contentSharded = false
	child.clusterReprocess = nil
	child.clusterRetryBudget = 0
	hd := *cfg
	hd.FullDuplex = false
	hd.contentSharded = false
	hd.clusterReprocess = nil
	hd.clusterRetryBudget = 0
	return &multidbFDRouter{
		parent:   parent,
		c:        c,
		blocking: blocking,
		childCfg: child,
		children: make(map[*multidbDatabase]*AutoPipeliner),
		noFD:     make(map[*multidbDatabase]struct{}),
		hdCfg:    hd,
	}
}

// submit routes cmd to the active member's FD child, or diverts it to
// MultiDB's normal path. Called from AutoPipeliner.submit after diversion.
func (r *multidbFDRouter) submit(ctx context.Context, cmd Cmder) AutoFuture {
	if !r.blocking {
		return r.submitOrdered(ctx, cmd)
	}
	child, reject := r.target()
	if reject {
		return r.rejectClosed(cmd)
	}
	if child == nil {
		return r.divert(ctx, cmd)
	}
	fut := child.submit(ctx, cmd)
	if fut.batch == completedBatch && errors.Is(cmd.Err(), ErrClosed) {
		// The member was removed or its child closed under us: MultiDB's path
		// routes the command to the live active member.
		cmd.SetErr(nil)
		return r.divert(ctx, cmd)
	}
	return fut
}

// target admits a command: the active member's FD child when the member is
// closed-admitted and the detector has not tripped, nil when the command must
// take MultiDB's normal path, reject when the client is closed.
func (r *multidbFDRouter) target() (child *AutoPipeliner, reject bool) {
	if r.parent.isClosed() {
		return nil, true
	}
	core := r.c.core
	if core.closed.Load() {
		return nil, true
	}
	db, _ := core.activeSnapshot()
	if db == nil || core.shouldFailoverSafely() {
		return nil, false
	}
	ok, res := db.cb.AllowReserve()
	if !ok {
		return nil, false
	}
	if res.Held() {
		// A half-open probe: give the slot back and let MultiDB's path take
		// it, which reserves it again and settles the probe precisely.
		db.cb.ReleaseFor(res)
		return nil, false
	}
	return r.childFor(db)
}

// submitOrdered is the async face's submit. The ordered async face promises
// submit order, so commands go to ONE lane at a time — the active member's FD
// child, or the ordered half-duplex MultiDB autopipeliner (hd) when the fast
// path is not available — and switching lanes first drains the old one: its
// Close completes every accepted command, in order (a dead member's backlog
// reroutes through MultiDB), before any command reaches the new lane. Lane
// switches happen on failover, fallback and breaker transitions; the switch
// stalls submitters for the drain.
//
// Fast path: the lane picked now is the current lane, so submit without
// laneMu. Anything else (a lane switch, a lane closed under the command) takes
// submitLocked, which picks, switches and submits under laneMu, so no caller
// can act on a stale pick.
func (r *multidbFDRouter) submitOrdered(ctx context.Context, cmd Cmder) AutoFuture {
	want, reject := r.target()
	if reject {
		return r.rejectClosed(cmd)
	}
	if want == nil {
		want = r.hd.Load()
	}
	if want != nil && r.lane.Load() == want {
		fut := want.submit(ctx, cmd)
		if fut.batch != completedBatch || !errors.Is(cmd.Err(), ErrClosed) {
			return fut
		}
		// The lane closed under this command (a switch by another caller):
		// pick again under laneMu.
		cmd.SetErr(nil)
	}
	return r.submitLocked(ctx, cmd)
}

// fdLaneAttempts bounds submitLocked's retries when the picked lane keeps
// closing under the command.
const fdLaneAttempts = 4

// submitLocked picks the lane, switches to it and submits cmd, all under
// laneMu. If no lane can take the command (no hd lane could be built, or the
// picked lane kept closing), it drains the current lane, runs cmd on
// MultiDB's normal path and waits for it, still under laneMu: every command
// accepted before it has completed by then, and none after it can start
// first, so the fallback keeps submit order.
func (r *multidbFDRouter) submitLocked(ctx context.Context, cmd Cmder) AutoFuture {
	r.laneMu.Lock()
	defer r.laneMu.Unlock()
	for attempt := 0; ; attempt++ {
		want, reject := r.target()
		if !reject && want == nil {
			want, reject = r.hdLaneLocked()
		}
		if reject {
			return r.rejectClosed(cmd)
		}
		if want == nil || attempt >= fdLaneAttempts {
			// Drain first, then wait for the diverted command itself while
			// laneMu is held: the async divert runs on its own goroutine, and a
			// later command must not reach a new lane before it completes.
			r.drainLaneLocked()
			fut := r.divert(ctx, cmd)
			<-fut.batch.done
			return fut
		}
		if r.lane.Load() != want {
			r.drainLaneLocked()
			r.lane.Store(want)
		}
		fut := want.submit(ctx, cmd)
		if fut.batch != completedBatch || !errors.Is(cmd.Err(), ErrClosed) {
			return fut
		}
		cmd.SetErr(nil)
	}
}

// hdLaneLocked returns the ordered half-duplex lane, building it on first
// use. reject is true when the router is closed. laneMu MUST be held.
func (r *multidbFDRouter) hdLaneLocked() (ap *AutoPipeliner, reject bool) {
	if r.isClosed() {
		return nil, true
	}
	if hd := r.hd.Load(); hd != nil && !hd.IsClosed() {
		return hd, false
	}
	cfg := r.hdCfg
	hd, err := r.c.newMultiDBAutoPipeliner(&cfg, false)
	if err != nil {
		return nil, false
	}
	r.hd.Store(hd)
	return hd, false
}

// drainLaneLocked closes the current lane and waits for its drain: every
// command it accepted completes (or is rerouted through MultiDB) before this
// returns. A closed member child is rebuilt by childFor on its next use; a
// closed hd by hdLaneLocked. laneMu MUST be held.
func (r *multidbFDRouter) drainLaneLocked() {
	cur := r.lane.Load()
	if cur == nil {
		return
	}
	_ = cur.Close()
	_ = cur.WaitClosed()
	r.hd.CompareAndSwap(cur, nil)
	r.lane.Store(nil)
}

func (r *multidbFDRouter) isClosed() bool {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.closed
}

func (r *multidbFDRouter) rejectClosed(cmd Cmder) AutoFuture {
	cmd.SetErr(ErrClosed)
	if !r.blocking {
		cmd.setReady(completedBatch)
	}
	return AutoFuture{cmd: cmd, batch: completedBatch}
}

// divert runs cmd on MultiDB's normal path (MultiDBClient.Process ->
// core.process), outside the pipeline.
func (r *multidbFDRouter) divert(ctx context.Context, cmd Cmder) AutoFuture {
	return AutoFuture{cmd: cmd, batch: r.parent.runOutsidePipeline(ctx, cmd)}
}

// childFor returns db's FD child, building it on first use. It returns
// (nil, false) when the member cannot run full duplex (divert), and
// (nil, true) when the router is closed (reject).
func (r *multidbFDRouter) childFor(db *multidbDatabase) (*AutoPipeliner, bool) {
	r.mu.RLock()
	child, closed := r.children[db], r.closed
	_, noFD := r.noFD[db]
	r.mu.RUnlock()
	if closed {
		return nil, true
	}
	if child != nil && !child.IsClosed() {
		return child, false
	}
	if noFD {
		return nil, false
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, true
	}
	if child := r.children[db]; child != nil && !child.IsClosed() {
		return child, false
	}
	// Drop children of removed members while the write lock is held anyway. A
	// removed member's client is closed, so its child has nothing left to
	// serve; close it off the lock (its drain may take a moment).
	for m, ch := range r.children {
		if m.removed.Load() || ch.IsClosed() {
			delete(r.children, m)
			go func() { _ = ch.Close() }()
		}
	}
	for m := range r.noFD {
		if m.removed.Load() {
			delete(r.noFD, m)
		}
	}
	if db.removed.Load() {
		return nil, false
	}
	if _, ok := r.noFD[db]; ok {
		return nil, false
	}
	child, err := r.newChild(db)
	// Tie the entry (child or noFD mark) to the member's removal before
	// publishing it, under r.mu: the hook takes r.mu, so a removal racing this
	// either finds the entry published, or has already run and register
	// reports false.
	registered := r.registerRemoveLocked(db)
	if err != nil || child == nil {
		if registered {
			r.noFD[db] = struct{}{}
		}
		return nil, false
	}
	if !registered {
		go func() { _ = child.Close() }()
		return nil, false
	}
	r.children[db] = child
	return child, false
}

// registerRemoveLocked registers this router's onRemove hook on db (dropChild,
// which clears both the child and the noFD mark). It reports false when db's
// removal already ran. r.mu MUST be held.
func (r *multidbFDRouter) registerRemoveLocked(db *multidbDatabase) bool {
	if !db.onRemove.register(r.removeHookID(), func() error {
		r.dropChild(db)
		return nil
	}) {
		return false
	}
	if r.removeHooks == nil {
		r.removeHooks = make(map[*multidbDatabase]struct{})
	}
	r.removeHooks[db] = struct{}{}
	return true
}

func (r *multidbFDRouter) removeHookID() string {
	return fmt.Sprintf("multidbFDRouter#%p", r)
}

// dropChild drains and closes db's child when db is removed. It runs before
// the member's client is closed, so the child flushes its accepted commands
// while the member is still open.
func (r *multidbFDRouter) dropChild(db *multidbDatabase) {
	r.mu.Lock()
	ch := r.children[db]
	delete(r.children, db)
	delete(r.noFD, db)
	delete(r.removeHooks, db)
	r.mu.Unlock()
	if ch != nil {
		_ = ch.Close()
		_ = ch.WaitClosed()
	}
}

// newChild builds db's FD child with the owner wiring: outcomes settle into
// db's breaker and the detector, recovery reroutes through MultiDB. It
// returns nil when full duplex does not engage on the member (no pipeline
// pool, replica routing on a cluster member), so that member diverts.
func (r *multidbFDRouter) newChild(db *multidbDatabase) (*AutoPipeliner, error) {
	core := r.c.core
	cfg := r.childCfg
	cfg.fdOutcome = func(ctx context.Context, cmd Cmder, err error) { core.settleFDOutcome(ctx, db, cmd, err) }
	// The engine passes the commands' remaining budget (derived from
	// fdRetryBudget below) as the retry bound, so a rerouted command never
	// runs more than CommandRetries+1 times in total.
	cfg.fdReroute = core.processPipelineRetries
	// Recovery follows MultiDB's retry policy, not the member's MaxRetries
	// (members commonly run with retries off): a dropped session's tail gets
	// re-issued through MultiDB as often as core.process would retry.
	cfg.fdOwnerRetries = true
	cfg.fdRetryBudget = core.opts.CommandRetries

	var (
		child *AutoPipeliner
		err   error
	)
	if db.cc != nil {
		child, err = newAutoPipeliner(db.cc, clusterAutoPipelineOptions(&cfg), r.blocking)
		if err == nil {
			db.cc.installAutoPipelineSharding(child)
			if child.clusterFD != nil {
				// Routing misses (keyless command, topology miss, a node
				// without FD) take MultiDB's path, not db.cc.Process.
				child.clusterFD.ownerDivert = r.divert
			}
		}
	} else {
		child, err = newAutoPipeliner(db.c, &cfg, r.blocking)
	}
	if err != nil {
		return nil, err
	}
	if child.fd == nil && child.clusterFD == nil {
		_ = child.Close()
		return nil, nil
	}
	return child, nil
}

// len sums the backlog of the children and of the half-duplex lane, for
// AutoPipeliner.Len. hd is never one of the children.
func (r *multidbFDRouter) len() int {
	r.mu.RLock()
	total := 0
	for _, ch := range r.children {
		total += ch.Len()
	}
	r.mu.RUnlock()
	if hd := r.hd.Load(); hd != nil {
		total += hd.Len()
	}
	return total
}

// close closes every child and waits for each drain. Called from the parent's
// drain, which MultiDBClient.Close runs before it closes the members, so each
// child flushes its accepted commands while its member is still open.
func (r *multidbFDRouter) close() error {
	r.mu.Lock()
	r.closed = true
	children := make([]*AutoPipeliner, 0, len(r.children))
	for _, ch := range r.children {
		children = append(children, ch)
	}
	r.children = make(map[*multidbDatabase]*AutoPipeliner)
	hooked := make([]*multidbDatabase, 0, len(r.removeHooks))
	for db := range r.removeHooks {
		hooked = append(hooked, db)
	}
	r.removeHooks = nil
	r.mu.Unlock()
	for _, db := range hooked {
		db.onRemove.unregister(r.removeHookID())
	}
	r.laneMu.Lock()
	if hd := r.hd.Swap(nil); hd != nil {
		children = append(children, hd)
	}
	r.lane.Store(nil)
	r.laneMu.Unlock()

	var firstErr error
	for _, ch := range children {
		cerr := ch.Close()
		if werr := ch.WaitClosed(); werr != nil {
			cerr = werr
		}
		if cerr != nil && firstErr == nil {
			firstErr = cerr
		}
	}
	return firstErr
}

// settleFDOutcome records a full-duplex command's final outcome on db, the way
// core.process does for a command it ran: the member's breaker always, the
// failure detector only while db is still the active member. The breaker
// episode and the detector window are the current ones (see multidbFDRouter).
// Runs on the child's reader or engine goroutine: it must not block.
func (c *multidbCore) settleFDOutcome(ctx context.Context, db *multidbDatabase, cmd Cmder, err error) {
	if err != nil && ctx != nil && ctx.Err() != nil {
		// The caller's own context ended: a client-side signal, not a health
		// verdict (core.process skips it the same way). A context error with
		// the caller's context still live (a member-side dial or limiter
		// deadline) is classified like any other error.
		return
	}
	kind := classifyOutcome(err, cmd.readTimeout() == nil)
	if kind == outcomeNeutral {
		return
	}
	if res, ok := db.cb.CurrentClosedReservation(); ok {
		if kind == outcomeSuccess {
			db.cb.RecordSuccessFor(res)
		} else {
			db.cb.RecordFailureFor(res)
		}
	}
	// Sample the detector window BEFORE the active check. A failover switches
	// the active member first and then resets the detector (bumping the
	// generation), so a generation read before a member check that still sees
	// db belongs to db's window: a reset after it voids the write
	// (recordDetector*), and a switch before it fails the check.
	gen := c.detectorGen.Load()
	if cur, _ := c.activeSnapshot(); cur != db {
		return
	}
	if kind == outcomeSuccess {
		c.successSinceFailover.Store(true)
		c.recordDetectorSuccess(gen)
	} else {
		c.recordDetectorFailure(gen, err)
	}
}
