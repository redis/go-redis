package redis

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

func TestContextDeadlinePipelineErrors(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	cn := pool.NewConn(client)
	defer cn.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	c := &baseClient{opt: &Options{Protocol: 2}}
	cmds := []Cmder{NewStringCmd(ctx, "get", "a"), NewStringCmd(ctx, "get", "b")}
	err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		return c.pipelineReadCmds(ctx, cn, rd, cmds)
	})
	// This is the same stamping done by generalProcessPipelineFrom.
	if pipelineErrShouldStamp(err) {
		setCmdsErr(cmds, err)
	}
	t.Logf("pipeline error: %v; command error: %v", err, cmds[0].Err())
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(cmds[0].Err(), context.DeadlineExceeded) || !errors.Is(cmds[1].Err(), context.DeadlineExceeded) {
		t.Fatal("pipeline and command disagree on context deadline")
	}
}

// TestContextDeadlinePubSubKeepsTimeoutConnection: a receive bounded by an
// expired context reports the deadline without touching the shared
// connection (the engine reads it on its own goroutine), while
// isBadConn still retires command connections after a timeout.
func TestContextDeadlinePubSubKeepsTimeoutConnection(t *testing.T) {
	client := NewClient(&Options{Addr: "127.0.0.1:1", Protocol: 2})
	defer client.Close()
	p := newPubSub(client.pubSubManager)
	defer p.Close()

	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	_, err := p.ReceiveTimeout(ctx, time.Second)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected context deadline, got %v", err)
	}
	if !isBadConn(context.DeadlineExceeded, true, "") {
		t.Fatal("plain context errors must still retire connections")
	}
	if !isBadConn(context.DeadlineExceeded, false, "") {
		t.Fatal("command connections must still be retired after a timeout")
	}
}

func TestContextDeadlineClusterPipelineErrors(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	cn := pool.NewConn(client)
	defer cn.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	c := &ClusterClient{opt: &ClusterOptions{}}
	node := &clusterNode{Client: &Client{baseClient: &baseClient{opt: &Options{Protocol: 2}}}}
	cmds := []Cmder{NewStringCmd(ctx, "get", "a"), NewStringCmd(ctx, "get", "b")}
	failed := newCmdsMap()
	err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		return c.pipelineReadCmdsInView(ctx, node, cn, rd, cmds, failed, nil, nil)
	})
	for _, cmd := range cmds {
		if !errors.Is(cmd.Err(), context.DeadlineExceeded) {
			t.Fatalf("command error: %v; pipeline error: %v", cmd.Err(), err)
		}
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("pipeline error: %v", err)
	}
	if len(failed.m) != 0 {
		t.Fatal("context deadline must not requeue commands for retry")
	}
}
