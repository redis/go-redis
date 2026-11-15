package pool

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestWaitForUsableIdleChecksReadyBeforeTicker(t *testing.T) {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()
	cn := NewConn(client)
	cn.GetStateMachine().Transition(StateIdle)
	p := &ConnPool{
		closedCh:  make(chan struct{}),
		idleConns: []*Conn{cn},
	}
	if err := p.waitForUsableIdle(context.Background(), time.Now().Add(5*time.Millisecond), false); err != nil {
		t.Fatalf("ready idle connection should not wait for ticker: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := p.waitForUsableIdle(ctx, time.Now().Add(time.Second), false); err != context.Canceled {
		t.Fatalf("cancelled wait returned %v, want context.Canceled", err)
	}
}

func TestUsableIdlePollIntervalRespectsCallerDeadline(t *testing.T) {
	if got := usableIdlePollInterval(context.Background(), time.Second); got != 10*time.Millisecond {
		t.Fatalf("unbounded caller interval = %v, want 10ms", got)
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(30*time.Millisecond))
	defer cancel()
	if got := usableIdlePollInterval(ctx, time.Second); got <= 0 || got > 7500*time.Microsecond {
		t.Fatalf("short-deadline interval = %v, want (0, 7.5ms]", got)
	}
	expiredCtx, expiredCancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer expiredCancel()
	if got := usableIdlePollInterval(expiredCtx, time.Second); got != 0 {
		t.Fatalf("expired caller interval = %v, want 0", got)
	}
}
