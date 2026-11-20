package pool

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestHandoffPending(t *testing.T) {
	cn := NewConn(nil)
	if cn.HandoffPending() {
		t.Fatal("fresh conn reports a pending handoff")
	}
	if err := cn.MarkForHandoff("new:6379", 1); err != nil {
		t.Fatal(err)
	}
	if !cn.HandoffPending() {
		t.Fatal("marked conn does not report a pending handoff")
	}
	// Queued: ShouldHandoff is cleared but the worker still owns the handoff.
	if err := cn.MarkQueuedForHandoff(); err != nil {
		t.Fatal(err)
	}
	if cn.ShouldHandoff() {
		t.Fatal("test premise: queued conn should have ShouldHandoff=false")
	}
	if !cn.HandoffPending() {
		t.Fatal("queued conn does not report a pending handoff")
	}
	cn.ClearHandoffState()
	if cn.HandoffPending() {
		t.Fatal("conn still pending after ClearHandoffState")
	}
}

// A handoff re-initializes the conn in place while it sits in the pool, and
// initConn leaves it IDLE before OnConnect returns. The reaper must not close
// it in that window, even if its lifetime has expired.
func TestReaperSkipsConnDuringHandoffInit(t *testing.T) {
	p := reaperPool(t, Options{ConnMaxLifetime: time.Hour})
	cn := fillIdle(t, p, 1)[0]
	cn.expiresAt = time.Now().Add(-time.Second)

	entered := make(chan struct{})
	release := make(chan struct{})
	cn.SetInitConnFunc(func(context.Context, *Conn) error {
		// What initConn does: report IDLE, then run OnConnect.
		cn.GetStateMachine().Transition(StateIdle)
		close(entered)
		<-release
		return nil
	})

	if err := cn.MarkForHandoff("new:6379", 1); err != nil {
		t.Fatal(err)
	}
	if err := cn.MarkQueuedForHandoff(); err != nil { // IDLE -> UNUSABLE, as OnPut/claim do
		t.Fatal(err)
	}
	newNet, peer := net.Pipe()
	defer peer.Close()
	initDone := make(chan error, 1)
	go func() { initDone <- cn.SetNetConnAndInitConn(context.Background(), newNet) }()

	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("handoff init never started")
	}
	if s := cn.GetStateMachine().GetState(); s != StateIdle {
		t.Fatalf("test premise: conn should be IDLE during OnConnect, got %v", s)
	}
	if n := p.reapExpiredIdle(context.Background()); n != 0 {
		t.Fatalf("reaped %d conns during handoff init, want 0", n)
	}
	if cn.IsClosed() {
		t.Fatal("conn was closed under the handoff worker")
	}

	close(release)
	if err := <-initDone; err != nil {
		t.Fatal(err)
	}
	cn.ClearHandoffState()

	// Handoff done: the expired conn is now an ordinary reap candidate.
	if n := p.reapExpiredIdle(context.Background()); n != 1 {
		t.Fatalf("reaped %d after the handoff cleared, want 1", n)
	}
}
