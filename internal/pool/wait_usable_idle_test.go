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
