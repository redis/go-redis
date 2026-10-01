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

	fd.q.signalBatchRoom() // one slot free, the batch needs five
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

// TestFDSubmitBatchPassesOnAWakeItCannotUse pins the wake chain: batchRoom
// holds one signal, and a holder that wakes passes it on while space remains.
// A batch that wakes but does not fit must still pass the wake to a smaller
// batch behind it, which may fit.
func TestFDSubmitBatchPassesOnAWakeItCannotUse(t *testing.T) {
	fd := fdBackpressureEngine(t)
	stop := startBlockedBatch(t, fd)
	defer stop()

	woke := make(chan struct{})
	go func() {
		<-fd.q.batchRoomCh() // another batch waiting behind this one
		close(woke)
	}()
	time.Sleep(50 * time.Millisecond)

	fd.q.signalBatchRoom() // one slot free: the batch wakes first and cannot fit
	select {
	case <-woke:
	case <-time.After(time.Second):
		t.Fatal("the batch kept a wake it could not use; the waiter behind it never woke")
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
