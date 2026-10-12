package redis

import (
	"bufio"
	"context"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/push"
)

type fdPanickingPushHandler struct{}

func (fdPanickingPushHandler) HandlePushNotification(context.Context, push.NotificationHandlerContext, []interface{}) error {
	panic("boom from a push handler")
}

// A push handler that panics while the grouped reader drains a push between
// two replies must not make the session replay the replies the group already
// read. The panic escaped the group's WithReader (only the reply decoder was
// guarded), so the read SET reply was never completed and recovery ran the
// SET again: one write, two executions. The drain now turns the panic into a
// fatal drain error at that position, like a decoder panic.
func TestFullDuplexPushHandlerPanicDoesNotReplayReadReplies(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	var sets atomic.Int64
	var first atomic.Bool
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			poisoned := first.CompareAndSwap(false, true)
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				for {
					args, err := readRESPCommand(rd)
					if err != nil {
						return
					}
					name := ""
					if len(args) > 0 {
						name = strings.ToLower(args[0])
					}
					switch {
					case name == "hello":
						_, _ = io.WriteString(c, "%1\r\n$5\r\nproto\r\n:3\r\n")
					case name == "set":
						sets.Add(1)
						if !poisoned {
							_, _ = io.WriteString(c, "+OK\r\n")
							continue
						}
						// Answer SET and the GET behind it in ONE write, with a
						// push between the replies, so both are in one read group.
						if _, err := readRESPCommand(rd); err != nil {
							return
						}
						_, _ = io.WriteString(c, "+OK\r\n>2\r\n$4\r\nboom\r\n$1\r\nx\r\n$1\r\nv\r\n")
					case name == "get" && len(args) == 2 && args[1] == "slow":
						// Keep the reader busy so SET and the GET behind it are
						// both in flight when it takes its next snapshot.
						time.Sleep(100 * time.Millisecond)
						_, _ = io.WriteString(c, "$-1\r\n")
					case name == "get":
						_, _ = io.WriteString(c, "$1\r\nv\r\n")
					default:
						_, _ = io.WriteString(c, "+OK\r\n")
					}
				}
			}(c)
		}
	}()

	c := NewClient(&Options{
		Addr:             ln.Addr().String(),
		Protocol:         3,
		DisableIdentity:  true,
		PipelinePoolSize: 2,
		PoolSize:         2,
		ReadTimeout:      2 * time.Second,
		MaxRetries:       3,
	})
	defer c.Close()
	if err := c.RegisterPushNotificationHandler("boom", fdPanickingPushHandler{}, false); err != nil {
		t.Fatalf("RegisterPushNotificationHandler: %v", err)
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	ctx := context.Background()
	slow := ap.Get(ctx, "slow")
	time.Sleep(20 * time.Millisecond) // slow is on the wire, the reader waits on it
	set := ap.Set(ctx, "k", "v", 0)
	get := ap.Get(ctx, "x")
	_ = get.Err()
	_ = slow.Err()
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if !first.Load() {
		t.Fatal("the poisoned connection was never used")
	}
	if n := sets.Load(); n != 1 {
		t.Fatalf("SET executed %d times, want 1 (its read reply was replayed after the push panic)", n)
	}
}
