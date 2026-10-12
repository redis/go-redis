package redis

import (
	"context"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

// fdBackpressureEngine is an engine with no writer and a submit queue of
// capacity 10 holding 9 commands, so a 5-command batch cannot be admitted.
func fdBackpressureEngine(t *testing.T) *fdEngine {
	t.Helper()
	apCtx, apCancel := context.WithCancel(context.Background())
	t.Cleanup(apCancel)
	fd := &fdEngine{
		ap:     &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: apCtx},
		client: &Client{baseClient: &baseClient{opt: &Options{}}},
		q:      newFDQueue(10),
	}
	for i := 0; i < 9; i++ {
		fd.q.push(fdReq{cmd: NewStatusCmd(context.Background(), "set", "k", "v"), batch: newAPBatch()})
	}
	return fd
}

// startBlockedBatch submits a 5-command batch that must wait for room and
// returns a func that cancels it and waits for it to return.
func startBlockedBatch(t *testing.T, fd *fdEngine) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	cmds := make([]Cmder, 5)
	for i := range cmds {
		cmds[i] = NewStatusCmd(ctx, "set", "k", "v")
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		if b, err := fd.submitBatch(ctx, cmds); b != nil || err != nil {
			t.Errorf("submitBatch = (%v, %v); want a ctx rejection", b, err)
		}
	}()
	time.Sleep(50 * time.Millisecond) // let it park on the room signal
	return func() {
		cancel()
		<-done
	}
}

// submitBatchParked reports whether the goroutine running submitBatch is
// parked in its select. A spinning loop never parks: it finds the room signal
// it just put back and runs on.
func submitBatchParked(t *testing.T) bool {
	t.Helper()
	buf := make([]byte, 1<<20)
	buf = buf[:runtime.Stack(buf, true)]
	for _, g := range strings.Split(string(buf), "\n\n") {
		if strings.Contains(g, "(*fdEngine).submitBatch(") {
			header, _, _ := strings.Cut(g, "\n")
			return strings.Contains(header, "[select")
		}
	}
	t.Fatal("no goroutine is running submitBatch")
	return false
}

// TestFDSubmitBatchDoesNotSpinWhileBlocked pins that a batch waiting for more
// room than one wake frees does not spin. It used to put the room signal back
// whenever any slot was free, take it straight back, fail to fit, and loop,
// burning a core for as long as the writer was blocked.
func TestFDSubmitBatchDoesNotSpinWhileBlocked(t *testing.T) {
	fd := fdBackpressureEngine(t)
	stop := startBlockedBatch(t, fd)
	defer stop()

	wakeHead(t, fd.q) // one slot free, the batch needs five
	time.Sleep(20 * time.Millisecond)
	running := 0
	const samples = 20
	for i := 0; i < samples; i++ {
		if !submitBatchParked(t) {
			running++
		}
		time.Sleep(5 * time.Millisecond)
	}
	if running > 1 {
		t.Fatalf("the waiting batch was running in %d of %d samples: it is spinning", running, samples)
	}
}

// wakeHead signals the head of the holder line the way a take would.
func wakeHead(t *testing.T, q *fdQueue) {
	t.Helper()
	q.mu.Lock()
	head := q.headWakeLocked()
	q.mu.Unlock()
	if head == nil {
		t.Fatal("no holder is waiting")
	}
	signalHolder(head)
}

// holderCount reports how many batches are waiting with a reservation.
func holderCount(q *fdQueue) int {
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.holders)
}

// waitHolders blocks until n batches hold a reservation.
func waitHolders(t *testing.T, q *fdQueue, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for holderCount(q) != n {
		if time.Now().After(deadline) {
			t.Fatalf("holders = %d, want %d", holderCount(q), n)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestFDSubmitBatchWakesOnlyTheHead pins the direct wake: a take signals the
// head of the holder line on its own channel, and a holder behind it is not
// woken at all, so no wake is shared or passed between holders.
func TestFDSubmitBatchWakesOnlyTheHead(t *testing.T) {
	fd := fdBackpressureEngine(t)
	headID, headWake := fd.q.hold(1) // an earlier holder, standing in for a head
	stop := startBlockedBatch(t, fd) // the batch lines up behind it
	defer stop()
	waitHolders(t, fd.q, 2)

	fd.q.takeInto(nil, 1)
	select {
	case <-headWake:
	case <-time.After(time.Second):
		t.Fatal("a take did not wake the head of the line")
	}
	time.Sleep(20 * time.Millisecond)
	if !submitBatchParked(t) {
		t.Fatal("the batch behind the head was woken by a take meant for the head")
	}

	// The head leaves: the batch becomes the head and is woken to try.
	fd.q.unhold(headID)
	time.Sleep(20 * time.Millisecond)
	if n := holderCount(fd.q); n != 1 {
		t.Fatalf("holders after unhold = %d, want 1", n)
	}
}

// TestFDSubmitBatchHoldersAreFIFO pins that holders are admitted in arrival
// order: small batches that arrive behind a large one cannot take the room
// the writer frees for the large one, even though each alone would fit, and
// each is admitted in turn once it reaches the head of the line.
func TestFDSubmitBatchHoldersAreFIFO(t *testing.T) {
	apCtx, apCancel := context.WithCancel(context.Background())
	defer apCancel()
	fd := &fdEngine{
		ap:     &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: apCtx},
		client: &Client{baseClient: &baseClient{opt: &Options{}}},
		q:      newFDQueue(10),
	}
	for i := 0; i < 10; i++ {
		fd.q.push(fdReq{cmd: NewStatusCmd(context.Background(), "set", "k", "v"), batch: newAPBatch()})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mk := func(n int) []Cmder {
		cmds := make([]Cmder, n)
		for i := range cmds {
			cmds[i] = NewStatusCmd(ctx, "set", "k", "v")
		}
		return cmds
	}
	order := make(chan string, 3)
	submit := func(name string, cmds []Cmder) {
		b, err := fd.submitBatch(ctx, cmds)
		if err != nil || len(b) != len(cmds) {
			t.Errorf("%s: submitBatch = (%d, %v)", name, len(b), err)
		}
		order <- name
	}
	go submit("big", mk(10))
	waitHolders(t, fd.q, 1)
	go submit("small1", mk(1))
	waitHolders(t, fd.q, 2)
	go submit("small2", mk(1))
	waitHolders(t, fd.q, 3)

	// Drain one at a time, as a writer with a one-command wave would. After
	// each take a small batch alone would fit; neither may be admitted.
	for i := 0; i < 10; i++ {
		fd.q.takeInto(nil, 1)
		time.Sleep(5 * time.Millisecond)
		if i < 9 {
			select {
			case name := <-order:
				t.Fatalf("%q admitted after %d takes, ahead of the head of the line", name, i+1)
			default:
			}
		}
	}
	next := func(want string) {
		t.Helper()
		select {
		case got := <-order:
			if got != want {
				t.Fatalf("admitted %q, want %q", got, want)
			}
		case <-time.After(time.Second):
			t.Fatalf("%q was not admitted", want)
		}
	}
	next("big") // 10 live again
	fd.q.takeInto(nil, 1)
	next("small1")
	fd.q.takeInto(nil, 1)
	next("small2")
}

// TestFDSubmitBatchCloseReleasesAllHolders pins that closing the queue wakes
// every holder, not just the head: a holder behind the head has no other
// room signal, and Close must not leave it waiting for its ctx.
func TestFDSubmitBatchCloseReleasesAllHolders(t *testing.T) {
	fd := fdBackpressureEngine(t)
	ctx := context.Background()
	done := make(chan error, 2)
	for _, n := range []int{5, 5} {
		cmds := make([]Cmder, n)
		for i := range cmds {
			cmds[i] = NewStatusCmd(ctx, "set", "k", "v")
		}
		go func() {
			b, err := fd.submitBatch(ctx, cmds)
			if b != nil || err != nil {
				t.Errorf("submitBatch = (%v, %v); want a submit-time rejection", b, err)
			}
			done <- cmds[0].Err()
		}()
	}
	waitHolders(t, fd.q, 2)

	fd.q.closeQueue()
	for i := 0; i < 2; i++ {
		select {
		case err := <-done:
			if err != ErrClosed {
				t.Fatalf("holder error = %v, want ErrClosed", err)
			}
		case <-time.After(time.Second):
			t.Fatal("a holder was not released by closeQueue")
		}
	}
}

// TestFDSubmitBatchReservesRoom pins that a refused batch holds its slots: the
// room the writer frees accumulates for it instead of going to single
// submitters, and the singles resume once the batch is in.
func TestFDSubmitBatchReservesRoom(t *testing.T) {
	fd := fdBackpressureEngine(t) // capacity 10, 9 queued
	ctx := context.Background()
	cmds := make([]Cmder, 5)
	for i := range cmds {
		cmds[i] = NewStatusCmd(ctx, "set", "k", "v")
	}
	type result struct {
		batches []*apBatch
		err     error
	}
	done := make(chan result, 1)
	go func() {
		b, err := fd.submitBatch(ctx, cmds)
		done <- result{b, err}
	}()
	time.Sleep(50 * time.Millisecond)

	// One slot is free, but the batch has reserved five: a single is refused.
	single := fdReq{cmd: NewStatusCmd(ctx, "set", "k", "v"), batch: newAPBatch()}
	if res := fd.q.push(single); res != fdPushFull {
		t.Fatalf("push with a batch waiting = %v; want fdPushFull (slots reserved)", res)
	}

	// The writer takes four. The batch now fits (5 live + 5), and the take
	// wakes it through batchRoom.
	if got := len(fd.q.takeInto(nil, 4)); got != 4 {
		t.Fatalf("takeInto = %d, want 4", got)
	}
	select {
	case r := <-done:
		if r.err != nil || len(r.batches) != 5 {
			t.Fatalf("submitBatch = (%d batches, %v); want 5 batches, nil", len(r.batches), r.err)
		}
	case <-time.After(time.Second):
		t.Fatal("the batch was not admitted after the writer freed its room")
	}
	if d := fd.q.depth(); d != 10 {
		t.Fatalf("depth after admission = %d, want 10", d)
	}
	if fd.q.roomFor(1) {
		t.Fatal("roomFor(1) with a full queue and no reservation = true")
	}

	// The reservation is consumed: once the writer frees a slot, a single fits.
	fd.q.takeInto(nil, 1)
	if res := fd.q.push(single); res != fdPushOK {
		t.Fatalf("push after admission = %v; want fdPushOK (reservation released)", res)
	}
}

// TestFDSubmitBatchReleasesReservationOnCancel pins that a batch that gives up
// gives its slots back and wakes a single its reservation was holding back.
func TestFDSubmitBatchReleasesReservationOnCancel(t *testing.T) {
	fd := fdBackpressureEngine(t)
	stop := startBlockedBatch(t, fd)

	single := fdReq{cmd: NewStatusCmd(context.Background(), "set", "k", "v"), batch: newAPBatch()}
	if res := fd.q.push(single); res != fdPushFull {
		t.Fatalf("push with a batch waiting = %v; want fdPushFull", res)
	}
	stop() // ctx cancelled: the batch is rejected and must unhold

	select {
	case <-fd.q.roomCh():
	case <-time.After(time.Second):
		t.Fatal("unhold did not wake the single the reservation held back")
	}
	if res := fd.q.push(single); res != fdPushOK {
		t.Fatalf("push after the batch gave up = %v; want fdPushOK", res)
	}
}

// TestFDSubmitBatchNotStarvedBySingles is the end-to-end shape of the fix: a
// writer that frees two slots per take, singles that refill every free slot
// as fast as they can, and a batch longer than a take. Without a reservation
// the batch never sees its whole length free at once and waits until its ctx
// expires.
func TestFDSubmitBatchNotStarvedBySingles(t *testing.T) {
	apCtx, apCancel := context.WithCancel(context.Background())
	defer apCancel()
	fd := &fdEngine{
		ap:     &AutoPipeliner{config: &AutoPipelineOptions{}, ctx: apCtx},
		client: &Client{baseClient: &baseClient{opt: &Options{}}},
		q:      newFDQueue(10),
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup

	// The writer: two slots per take, continuously.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			fd.q.takeInto(nil, 2)
			time.Sleep(200 * time.Microsecond)
		}
	}()
	// The singles: refill as submit() does, waiting on room when refused.
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req := fdReq{cmd: NewStatusCmd(context.Background(), "set", "k", "v"), batch: newAPBatch()}
			for {
				select {
				case <-stop:
					return
				default:
				}
				if fd.q.push(req) == fdPushOK {
					continue
				}
				select {
				case <-fd.q.roomCh():
					if fd.q.roomFor(1) {
						fd.q.signalRoom()
					}
				case <-stop:
					return
				}
			}
		}()
	}
	time.Sleep(20 * time.Millisecond) // let the singles saturate the queue

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	cmds := make([]Cmder, 8) // four times a take
	for i := range cmds {
		cmds[i] = NewStatusCmd(ctx, "set", "k", "v")
	}
	batches, err := fd.submitBatch(ctx, cmds)
	close(stop)
	wg.Wait()
	if err != nil || len(batches) != 8 {
		t.Fatalf("submitBatch under single-command load = (%d batches, %v, cmd err %v); want admission",
			len(batches), err, cmds[0].Err())
	}
}
