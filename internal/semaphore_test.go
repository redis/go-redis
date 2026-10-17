package internal

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

var errSemTestTimeout = errors.New("sem test timeout")

// hammerAcquireReleaseRace races Release against timeout expiry so the
// Acquire select's token case regularly commits around the moment the pooled
// timer fires, pinning the `if !timer.Stop() { <-timer.C }` drain against
// BOTH timer-channel semantics (reviewed on #3942 as a suspected Go 1.23+
// hang, refuted empirically):
//
//   - go.mod >= 1.23 (synchronous channels): Stop returns TRUE for an
//     expired-but-undelivered fire — it aborts the delivery — and returns
//     false only once the value was actually received. With the receiving
//     select on the SAME goroutine, a taken token means the fire was never
//     received, so the drain branch is unreachable and cannot block.
//   - GODEBUG=asynctimerchan=1 (old buffered channels, e.g. a consumer main
//     module on go < 1.23): Stop returns false and the fired value sits
//     buffered; the drain consumes it so the pooled timer is clean for
//     Reset-reuse.
func hammerAcquireReleaseRace(t *testing.T, tryAcquire func() bool, acquire func(context.Context, time.Duration, error) error, release func()) {
	t.Helper()
	ctx := context.Background()
	iters := 200000
	if testing.Short() {
		iters = 2000
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < iters; i++ {
			if !tryAcquire() {
				t.Error("token missing at loop start")
				return
			}
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				release()
			}()
			if err := acquire(ctx, time.Microsecond, errSemTestTimeout); err == nil {
				release()
			}
			wg.Wait()
		}
	}()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		t.Fatal("Acquire hung: timer drained with a blocking receive after Stop returned false " +
			"(Go 1.23+ synchronous timer channels discard undelivered fires)")
	}
}

func TestFIFOSemaphoreAcquireTimerRace(t *testing.T) {
	s := NewFIFOSemaphore(1)
	hammerAcquireReleaseRace(t, s.TryAcquire, s.Acquire, s.Release)
}

func TestFastSemaphoreAcquireTimerRace(t *testing.T) {
	s := NewFastSemaphore(1)
	hammerAcquireReleaseRace(t, s.TryAcquire, s.Acquire, s.Release)
}

// The asynctimerchan=1 variants exercise the OLD buffered-channel semantics a
// consumer main module on go < 1.23 gets (the GODEBUG default follows the
// MAIN module's go directive, not this library's).
func TestFIFOSemaphoreAcquireTimerRaceOldTimerChan(t *testing.T) {
	t.Setenv("GODEBUG", "asynctimerchan=1")
	s := NewFIFOSemaphore(1)
	hammerAcquireReleaseRace(t, s.TryAcquire, s.Acquire, s.Release)
}

func TestFastSemaphoreAcquireTimerRaceOldTimerChan(t *testing.T) {
	t.Setenv("GODEBUG", "asynctimerchan=1")
	s := NewFastSemaphore(1)
	hammerAcquireReleaseRace(t, s.TryAcquire, s.Acquire, s.Release)
}

// hammerStaleTimerFire pins pooled-timer hygiene: every iteration times an
// Acquire out (firing the pooled timer), then immediately runs a
// long-budget Acquire that must win via a released token — a stale fire
// leaking through the pool would surface as an instant spurious timeout.
func hammerStaleTimerFire(t *testing.T, tryAcquire func() bool, acquire func(context.Context, time.Duration, error) error, release func()) {
	t.Helper()
	ctx := context.Background()
	iters := 200
	if testing.Short() {
		iters = 20
	}
	for i := 0; i < iters; i++ {
		if !tryAcquire() {
			t.Fatal("token missing at loop start")
		}
		if err := acquire(ctx, time.Millisecond, errSemTestTimeout); err != errSemTestTimeout {
			t.Fatalf("iter %d: want timeout, got %v", i, err)
		}
		go func() {
			time.Sleep(2 * time.Millisecond)
			release()
		}()
		if err := acquire(ctx, 10*time.Second, errSemTestTimeout); err != nil {
			t.Fatalf("iter %d: spurious timeout from a stale pooled-timer fire: %v", i, err)
		}
		release()
	}
}

func TestFIFOSemaphoreNoStaleTimerFire(t *testing.T) {
	s := NewFIFOSemaphore(1)
	hammerStaleTimerFire(t, s.TryAcquire, s.Acquire, s.Release)
}

func TestFastSemaphoreNoStaleTimerFire(t *testing.T) {
	s := NewFastSemaphore(1)
	hammerStaleTimerFire(t, s.TryAcquire, s.Acquire, s.Release)
}

func TestFIFOSemaphoreNoStaleTimerFireOldTimerChan(t *testing.T) {
	t.Setenv("GODEBUG", "asynctimerchan=1")
	s := NewFIFOSemaphore(1)
	hammerStaleTimerFire(t, s.TryAcquire, s.Acquire, s.Release)
}

func TestFastSemaphoreNoStaleTimerFireOldTimerChan(t *testing.T) {
	t.Setenv("GODEBUG", "asynctimerchan=1")
	s := NewFastSemaphore(1)
	hammerStaleTimerFire(t, s.TryAcquire, s.Acquire, s.Release)
}
