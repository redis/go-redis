package redis

import (
	"context"
	"runtime"
	"strings"
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

	fd.q.signalRoom() // one slot free, the batch needs five
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

// TestFDSubmitBatchPassesOnAWakeItCannotUse pins the wake chain: room holds
// one signal, and a waiter that wakes passes it on while space remains. A
// batch that wakes but does not fit must still pass the wake to a smaller
// waiter behind it, which may fit.
func TestFDSubmitBatchPassesOnAWakeItCannotUse(t *testing.T) {
	fd := fdBackpressureEngine(t)
	stop := startBlockedBatch(t, fd)
	defer stop()

	woke := make(chan struct{})
	go func() {
		<-fd.q.roomCh() // a single command waiting behind the batch
		close(woke)
	}()
	time.Sleep(50 * time.Millisecond)

	fd.q.signalRoom() // one slot free: the batch wakes first and cannot fit
	select {
	case <-woke:
	case <-time.After(time.Second):
		t.Fatal("the batch kept a wake it could not use; the waiter behind it never woke")
	}
}
