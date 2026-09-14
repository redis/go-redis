package redis

import "sync"

// fdQueue is the full-duplex submit queue: an MPSC hand-off from many submitting
// goroutines to the single writer goroutine.
//
// It replaces a buffered `chan fdReq`. The channel cost, per command:
//
//	producer  lock + 104-byte copy + unlock, plus a goready EVERY time an
//	          arrival found the writer parked
//	writer    lock + 104-byte copy + unlock, ONCE PER COMMAND
//
// At ~450k ops/s that measured 3.41 s in the submit send, 2.65 s in the writer's
// receive and 3.25 s in the accumulate select (19.6% of all CPU between them).
//
// The slice costs one lock and one append per command on the producer side, and
// ONE lock plus ONE bulk copy for the whole wave on the writer side — the
// writer's per-command cost disappears. pushBatch amortises the producer lock
// across k commands as well.
//
// The structural win is not the copy, it is the parking. A channel can only
// express "wake me on the next arrival", so an accumulating writer was woken
// once per command and re-entered its select ~112 times per 250 us window. This
// queue lets the writer say "wake me when the queue holds N more, or not at
// all" (park), so an accumulating writer parks and wakes ONCE per flush. A
// submitter that finds the writer awake, or the depth still short of what the
// writer asked for, does no channel work at all.
//
// Ordering is FIFO, so the full-order (T1) guarantee is unchanged. The one
// semantic loss versus a channel is fairness between submitters blocked on a
// FULL queue: chansend queues blocked senders FIFO, whereas here they re-contend
// for the mutex. Per-goroutine program order is still exact (a goroutine's push
// happens-before its next push), which is what the ordering contract actually
// promises; cross-goroutine arrival order under saturation was never defined.
type fdQueue struct {
	mu  sync.Mutex
	buf []fdReq
	max int // bound; mirrors the old channel capacity

	// parked/wakeAt are the writer's standing request: it is asleep and wants a
	// signal once the queue holds at least wakeAt entries. Only a submitter that
	// takes parked from true to false sends the signal, so a wave of arrivals
	// produces exactly one wake.
	parked bool
	wakeAt int

	wake chan struct{} // cap 1: submitter -> parked writer
	room chan struct{} // cap 1: writer -> submitters blocked on a full queue

	closed bool
}

// fdQueueReady is an always-ready receive. The writer selects on it instead of
// the wake channel when park() reported that work is already queued, so the
// "already had work" and "waited for work" paths share one select and one copy
// of the batch-building code.
var fdQueueReady = func() chan struct{} {
	c := make(chan struct{})
	close(c)
	return c
}()

// fdPushResult is the outcome of an enqueue attempt.
type fdPushResult int

const (
	fdPushOK     fdPushResult = iota
	fdPushFull                // bound reached; caller waits on roomCh and retries
	fdPushClosed              // engine shut down; caller fails the command
)

func newFDQueue(max int) *fdQueue {
	if max < 1 {
		max = 1
	}
	return &fdQueue{
		buf:  make([]fdReq, 0, 64), // grows to the live depth, not to max up front
		max:  max,
		wake: make(chan struct{}, 1),
		room: make(chan struct{}, 1),
	}
}

// NIL-QUEUE CONTRACT. Every method below tolerates a nil receiver, and together
// they reproduce the semantics of the nil `chan fdReq` this type replaced: a nil
// channel is never ready, so a receive on it blocks forever and a send on it
// blocks forever. That is not a defensive flourish — fdEngine is constructed as a
// bare struct literal in several tests that exercise session()/attempt() without
// a queue, and those tests depend on the submit arm simply never firing. So:
//
//	wakeCh/roomCh  nil channel  -> that select arm is never ready
//	park           true         -> "parked", and the nil wakeCh never wakes us
//	takeInto       no-op        -> no work ever arrives
//	push           fdPushFull   -> submit waits on the nil roomCh, i.e. forever,
//	                               leaving only its ctx.Done() arms, exactly as a
//	                               blocked send on a nil channel did
func (q *fdQueue) wakeCh() <-chan struct{} {
	if q == nil {
		return nil
	}
	return q.wake
}

func (q *fdQueue) roomCh() <-chan struct{} {
	if q == nil {
		return nil
	}
	return q.room
}

// capacity is the queue bound, 0 for a nil queue.
func (q *fdQueue) capacity() int {
	if q == nil {
		return 0
	}
	return q.max
}

// signalRoom re-arms the cap-1 room signal. Called only by a submitter that just
// woke from a full queue, to chain the wake to the next one waiting.
func (q *fdQueue) signalRoom() {
	if q == nil {
		return
	}
	select {
	case q.room <- struct{}{}:
	default:
	}
}

// push enqueues one command. The wake signal is sent OUTSIDE the mutex: it is a
// non-blocking send on a cap-1 channel, but it can call goready, and holding the
// submit mutex across a scheduler operation would convoy every other submitter
// behind it.
func (q *fdQueue) push(req fdReq) fdPushResult {
	if q == nil {
		return fdPushFull // see the nil-queue contract
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return fdPushClosed
	}
	if len(q.buf) >= q.max {
		q.mu.Unlock()
		return fdPushFull
	}
	q.buf = append(q.buf, req)
	signal := q.parked && len(q.buf) >= q.wakeAt
	if signal {
		q.parked = false // claim the wake: later arrivals in this wave stay silent
	}
	q.mu.Unlock()
	if signal {
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
	return fdPushOK
}

// pushBatch enqueues reqs under a SINGLE lock, for callers that already hold a
// run of commands (a pipeline, or a carry tail being returned to the queue).
// All-or-nothing: a partial enqueue would split a pipeline across two flushes
// and, worse, leave the caller unsure which half it owns.
func (q *fdQueue) pushBatch(reqs []fdReq) fdPushResult {
	if len(reqs) == 0 {
		return fdPushOK
	}
	if q == nil {
		return fdPushFull // see the nil-queue contract
	}
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return fdPushClosed
	}
	if len(q.buf)+len(reqs) > q.max {
		q.mu.Unlock()
		return fdPushFull
	}
	q.buf = append(q.buf, reqs...)
	signal := q.parked && len(q.buf) >= q.wakeAt
	if signal {
		q.parked = false
	}
	q.mu.Unlock()
	if signal {
		select {
		case q.wake <- struct{}{}:
		default:
		}
	}
	return fdPushOK
}

// pushFront returns commands the WRITER already took but could not write this
// round (the MaxBatchBytes cap trips mid-wave) to the head of the queue, so they
// go out first and FIFO order is exact. Writer-only: there is one writer, it
// holds no other lock here, and the commands were already admitted, so this is
// not a new enqueue and is deliberately not bounded by max — total outstanding
// work is unchanged.
func (q *fdQueue) pushFront(reqs []fdReq) {
	// Unreachable on a nil queue: takeInto yields nothing there, so the writer
	// never holds a tail to give back.
	if q == nil || len(reqs) == 0 {
		return
	}
	q.mu.Lock()
	q.buf = append(append(make([]fdReq, 0, len(reqs)+len(q.buf)), reqs...), q.buf...)
	q.mu.Unlock()
}

// takeInto moves up to max queued commands onto dst and returns the extended
// slice: one lock and one bulk copy for the whole wave. Taking everything (the
// common case) leaves nothing to shift down.
//
// The vacated slots are cleared, because the backing array outlives them and a
// stale fdReq pins a Cmder, a context and an apBatch. That is 104 bytes per slot
// of memclr, against the per-command lock and copy the channel charged.
func (q *fdQueue) takeInto(dst []fdReq, max int) []fdReq {
	if q == nil || max <= 0 {
		return dst
	}
	q.mu.Lock()
	n := len(q.buf)
	if n == 0 {
		q.mu.Unlock()
		return dst
	}
	if n > max {
		n = max
	}
	dst = append(dst, q.buf[:n]...)
	rest := copy(q.buf, q.buf[n:])
	clear(q.buf[rest:])
	q.buf = q.buf[:rest]
	q.parked = false // we are awake; stop submitters from signalling
	q.mu.Unlock()
	// Release anyone blocked on a full queue. Non-blocking on a cap-1 channel, so
	// this is a cheap no-op once a signal is already pending.
	select {
	case q.room <- struct{}{}:
	default:
	}
	return dst
}

// park registers the writer's intent to sleep until the queue holds at least
// minDepth commands. It returns false when the queue ALREADY has that many, in
// which case the writer must take instead of waiting — checking under the same
// lock that a submitter needs to signal is what makes the wake lossless.
//
// minDepth > 1 is only safe when the caller ALSO waits on a timer: submitters
// stay silent below the threshold, so a wave that never reaches minDepth must be
// released by something other than a push.
func (q *fdQueue) park(minDepth int) bool {
	if q == nil {
		return true // "parked" on a nil wakeCh: that arm never fires
	}
	if minDepth < 1 {
		minDepth = 1
	}
	q.mu.Lock()
	if q.closed || len(q.buf) >= minDepth {
		q.parked = false
		q.mu.Unlock()
		return false
	}
	q.parked = true
	q.wakeAt = minDepth
	q.mu.Unlock()
	return true
}

// unpark withdraws a standing park request. The writer calls it after any wait
// that did NOT end in a push signal (timer, idle, max-hold, close), so a later
// arrival does not send a wake nobody is listening for.
func (q *fdQueue) unpark() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.parked = false
	q.mu.Unlock()
}

func (q *fdQueue) depth() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	n := len(q.buf)
	q.mu.Unlock()
	return n
}

// drainAll removes and returns everything queued. Used by the shutdown and
// fail-backlog paths, which replace the channel's `for { select { case r := <-ch:
// default: return } }` drains.
func (q *fdQueue) drainAll(dst []fdReq) []fdReq {
	if q == nil {
		return dst
	}
	q.mu.Lock()
	dst = append(dst, q.buf...)
	clear(q.buf)
	q.buf = q.buf[:0]
	q.parked = false
	q.mu.Unlock()
	select {
	case q.room <- struct{}{}:
	default:
	}
	return dst
}

// closeQueue rejects further pushes. The engine's submitMu still orders this
// against in-flight submits exactly as it ordered the old channel drain.
func (q *fdQueue) closeQueue() {
	if q == nil {
		return
	}
	q.mu.Lock()
	q.closed = true
	q.mu.Unlock()
	// Release submitters blocked on a full queue so they observe the closed state.
	select {
	case q.room <- struct{}{}:
	default:
	}
}
