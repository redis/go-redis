package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

// TestConnStateMachine_AwaitAndTransition_WokenByClose verifies that closing a
// connection wakes all goroutines parked in AwaitAndTransition with
// ErrStateMachineClosed instead of leaving them blocked until context expiry.
func TestConnStateMachine_AwaitAndTransition_WokenByClose(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	cn := NewConn(client)
	sm := cn.GetStateMachine()
	sm.Transition(StateInUse)

	const numWaiters = 2
	type result struct {
		state ConnState
		err   error
	}
	results := make(chan result, numWaiters)

	for i := 0; i < numWaiters; i++ {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			state, err := sm.AwaitAndTransition(ctx, ValidFromIdle(), StateUnusable)
			results <- result{state: state, err: err}
		}()
	}

	deadline := time.Now().Add(2 * time.Second)
	for sm.waiterCount.Load() < numWaiters {
		if time.Now().After(deadline) {
			t.Fatalf("only %d/%d waiters enqueued", sm.waiterCount.Load(), numWaiters)
		}
		time.Sleep(time.Millisecond)
	}

	if err := cn.Close(); err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	for i := 0; i < numWaiters; i++ {
		select {
		case res := <-results:
			if !errors.Is(res.err, ErrStateMachineClosed) {
				t.Fatalf("waiter %d: err = %v, want %v", i, res.err, ErrStateMachineClosed)
			}
			if res.state != StateClosed {
				t.Fatalf("waiter %d: state = %s, want %s", i, res.state, StateClosed)
			}
		case <-time.After(time.Second):
			t.Fatalf("waiter %d still parked after Close", i)
		}
	}

	checkWaiterQueueEmpty(t, sm, 0)
}

// TestConnStateMachine_AwaitAndTransition_WokenByTransitionClosed verifies that
// an unconditional Transition(StateClosed) wakes a waiter parked on
// initialization states with ErrStateMachineClosed.
func TestConnStateMachine_AwaitAndTransition_WokenByTransitionClosed(t *testing.T) {
	sm := NewConnStateMachine()
	sm.Transition(StateInUse)

	type result struct {
		state ConnState
		err   error
	}
	done := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		state, err := sm.AwaitAndTransition(ctx, ValidFromCreatedIdleOrUnusable(), StateInitializing)
		done <- result{state: state, err: err}
	}()

	waitForWaiter(t, sm, 0)

	sm.Transition(StateClosed)

	select {
	case res := <-done:
		if !errors.Is(res.err, ErrStateMachineClosed) {
			t.Fatalf("err = %v, want %v", res.err, ErrStateMachineClosed)
		}
		if res.state != StateClosed {
			t.Fatalf("state = %s, want %s", res.state, StateClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter still parked after Transition(StateClosed)")
	}

	checkWaiterQueueEmpty(t, sm, 0)
}

// TestConnStateMachine_AwaitAndTransition_CloseBeforeEnqueue covers Close
// landing between the pre-enqueue StateClosed check and pushing the waiter onto
// the queue. At that moment waiterCount is 0 so Close's notifyWaiters returns
// early; AwaitAndTransition's post-enqueue notifyWaiters must wake the waiter
// with ErrStateMachineClosed.
func TestConnStateMachine_AwaitAndTransition_CloseBeforeEnqueue(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	cn := NewConn(client)
	sm := cn.GetStateMachine()
	sm.Transition(StateInUse)

	// Hold sm.mu so the waiter passes the fast path and the pre-enqueue
	// StateClosed check, then blocks before pushing itself onto sm.waiters.
	sm.mu.Lock()
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		defer cancel()
		_, err := sm.AwaitAndTransition(ctx, ValidFromIdle(), StateUnusable)
		done <- err
	}()
	time.Sleep(30 * time.Millisecond)

	// Close transitions to StateClosed while waiterCount is still 0.
	closeErr := cn.Close()
	sm.mu.Unlock()
	if closeErr != nil {
		t.Fatalf("Close failed: %v", closeErr)
	}

	select {
	case err := <-done:
		if !errors.Is(err, ErrStateMachineClosed) {
			t.Fatalf("waiter enqueued after Close got err = %v, want %v", err, ErrStateMachineClosed)
		}
	case <-time.After(time.Second):
		t.Fatal("waiter still parked after Close before enqueue")
	}

	checkWaiterQueueEmpty(t, sm, 0)
}

// TestConnStateMachine_AwaitAndTransition_CancelRaceWithClose verifies that
// racing context cancellation against Transition(StateClosed) always reports
// either ErrStateMachineClosed or context.Canceled and decrements waiterCount
// exactly once.
func TestConnStateMachine_AwaitAndTransition_CancelRaceWithClose(t *testing.T) {
	for i := 0; i < 500; i++ {
		sm := NewConnStateMachine()
		sm.Transition(StateInUse)

		ctx, cancel := context.WithCancel(context.Background())
		errc := make(chan error, 1)
		go func() {
			_, err := sm.AwaitAndTransition(ctx, ValidFromIdle(), StateUnusable)
			errc <- err
		}()

		waitForWaiter(t, sm, i)

		var wg sync.WaitGroup
		wg.Add(2)
		go func() {
			defer wg.Done()
			cancel()
		}()
		go func() {
			defer wg.Done()
			sm.Transition(StateClosed)
		}()
		wg.Wait()

		err := <-errc
		if !errors.Is(err, ErrStateMachineClosed) && !errors.Is(err, context.Canceled) {
			t.Fatalf("iteration %d: unexpected err = %v", i, err)
		}
		if got := sm.GetState(); got != StateClosed {
			t.Fatalf("iteration %d: state = %s, want %s", i, got, StateClosed)
		}
		checkWaiterQueueEmpty(t, sm, i)
	}
}
