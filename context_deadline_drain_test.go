package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/push"
)

// drainTestServer is a minimal RESP server for DrainOnContextTimeout tests. GET
// replies with the key itself; a key starting with "slow" is answered only after
// slowDelay, optionally preceded by a push frame or split in two halves around
// the delay.
type drainTestServer struct {
	ln        net.Listener
	resp3     bool
	slowDelay time.Duration
	// pushFirst sends a "drain_test" push frame just ahead of a slow reply,
	// after the delay, so only the background drain can see it.
	pushFirst bool
	// splitReply delays only the last element of an MGET reply.
	splitReply bool

	dials atomic.Int32
}

func startDrainTestServer(t *testing.T, s *drainTestServer) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s.ln = ln
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.dials.Add(1)
			go s.serve(c)
		}
	}()
	return ln.Addr().String()
}

func (s *drainTestServer) serve(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	for {
		args, err := readRESPCommand(rd)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		var reply string
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			if !s.resp3 {
				reply = "-ERR unknown command 'hello'\r\n"
			} else {
				reply = "%1\r\n+proto\r\n:3\r\n"
			}
		case "CLIENT":
			reply = "-ERR unknown subcommand\r\n"
		case "GET":
			key := args[1]
			reply = fmt.Sprintf("$%d\r\n%s\r\n", len(key), key)
			if strings.HasPrefix(key, "slow") {
				time.Sleep(s.slowDelay)
				if s.pushFirst {
					reply = ">2\r\n$10\r\ndrain_test\r\n$3\r\nabc\r\n" + reply
				}
			}
		case "MGET":
			// With splitReply, everything up to the last element is sent before
			// the delay, so a timed-out read stops between two complete frames
			// inside the array.
			var b strings.Builder
			fmt.Fprintf(&b, "*%d\r\n", len(args)-1)
			for _, key := range args[1 : len(args)-1] {
				fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(key), key)
			}
			last := args[len(args)-1]
			reply = fmt.Sprintf("$%d\r\n%s\r\n", len(last), last)
			if s.splitReply {
				if _, err := c.Write([]byte(b.String())); err != nil {
					return
				}
				time.Sleep(s.slowDelay)
			} else {
				reply = b.String() + reply
			}
		default:
			reply = "+OK\r\n"
		}
		if _, err := c.Write([]byte(reply)); err != nil {
			return
		}
	}
}

func newDrainTestClient(addr string, protocol int, drain bool, drainTimeout time.Duration) *Client {
	return NewClient(&Options{
		Addr:                       addr,
		Protocol:                   protocol,
		DisableIdentity:            true,
		PoolSize:                   1,
		MaxRetries:                 -1,
		ContextTimeoutEnabled:      true,
		DrainOnContextTimeout:      drain,
		ContextTimeoutDrainTimeout: drainTimeout,
	})
}

// getSlowWithDeadline runs GET slow under a short context deadline and asserts
// it fails with the context error.
func getSlowWithDeadline(t *testing.T, rdb *Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := rdb.Get(ctx, "slow").Err()
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("GET slow err = %v, want context.DeadlineExceeded", err)
	}
}

// getFast asserts that the next command reads its own reply, not the late one.
func getFast(t *testing.T, rdb *Client) {
	t.Helper()
	got, err := rdb.Get(context.Background(), "fast").Result()
	if err != nil || got != "fast" {
		t.Fatalf("GET fast = %q, %v; want \"fast\" (late reply leaked?)", got, err)
	}
}

func TestDrainOnContextTimeoutReusesConn(t *testing.T) {
	for _, protocol := range []int{2, 3} {
		t.Run(fmt.Sprintf("RESP%d", protocol), func(t *testing.T) {
			srv := &drainTestServer{resp3: protocol == 3, slowDelay: 150 * time.Millisecond}
			rdb := newDrainTestClient(startDrainTestServer(t, srv), protocol, true, 2*time.Second)
			defer rdb.Close()

			getFast(t, rdb)
			getSlowWithDeadline(t, rdb)
			// PoolSize is 1 and the drain keeps the turn, so this waits for the
			// drain and must then reuse the drained conn.
			getFast(t, rdb)

			if dials := srv.dials.Load(); dials != 1 {
				t.Fatalf("dials = %d, want 1: drained conn was not reused", dials)
			}
			if stats := rdb.PoolStats(); stats.TotalConns != 1 {
				t.Fatalf("TotalConns = %d, want 1", stats.TotalConns)
			}
		})
	}
}

func TestDrainOnContextTimeoutDisabledClosesConn(t *testing.T) {
	srv := &drainTestServer{slowDelay: 100 * time.Millisecond}
	rdb := newDrainTestClient(startDrainTestServer(t, srv), 2, false, 0)
	defer rdb.Close()

	getFast(t, rdb)
	getSlowWithDeadline(t, rdb)
	getFast(t, rdb)

	if dials := srv.dials.Load(); dials != 2 {
		t.Fatalf("dials = %d, want 2: timed-out conn must be closed by default", dials)
	}
}

func TestDrainOnContextTimeoutBudgetExceededClosesConn(t *testing.T) {
	srv := &drainTestServer{slowDelay: time.Second}
	rdb := newDrainTestClient(startDrainTestServer(t, srv), 2, true, 50*time.Millisecond)
	defer rdb.Close()

	getFast(t, rdb)
	getSlowWithDeadline(t, rdb)
	start := time.Now()
	getFast(t, rdb)
	if waited := time.Since(start); waited > 700*time.Millisecond {
		t.Fatalf("next command waited %v; the drain budget is 50ms", waited)
	}

	if dials := srv.dials.Load(); dials != 2 {
		t.Fatalf("dials = %d, want 2: conn must be closed when the drain times out", dials)
	}
}

func TestDrainOnContextTimeoutPartialReplyClosesConn(t *testing.T) {
	// The first array element arrives before the deadline. The reader stops on a
	// frame boundary, but inside the reply: draining one more frame would look
	// successful while leaving the stream misaligned, so the conn must be closed.
	srv := &drainTestServer{slowDelay: 100 * time.Millisecond, splitReply: true}
	rdb := newDrainTestClient(startDrainTestServer(t, srv), 2, true, 2*time.Second)
	defer rdb.Close()

	getFast(t, rdb)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := rdb.MGet(ctx, "a", "b").Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("MGET err = %v, want context.DeadlineExceeded", err)
	}
	getFast(t, rdb)

	if dials := srv.dials.Load(); dials != 2 {
		t.Fatalf("dials = %d, want 2: a partially read reply must close the conn", dials)
	}
}

type drainTestPushHandler struct{ calls atomic.Int32 }

func (h *drainTestPushHandler) HandlePushNotification(context.Context, push.NotificationHandlerContext, []interface{}) error {
	h.calls.Add(1)
	return nil
}

func TestDrainOnContextTimeoutProcessesPushNotifications(t *testing.T) {
	srv := &drainTestServer{resp3: true, slowDelay: 150 * time.Millisecond, pushFirst: true}
	rdb := newDrainTestClient(startDrainTestServer(t, srv), 3, true, 2*time.Second)
	defer rdb.Close()

	handler := &drainTestPushHandler{}
	if err := rdb.RegisterPushNotificationHandler("drain_test", handler, false); err != nil {
		t.Fatal(err)
	}

	getFast(t, rdb)
	getSlowWithDeadline(t, rdb)
	getFast(t, rdb)

	if calls := handler.calls.Load(); calls != 1 {
		t.Fatalf("push handler calls = %d, want 1: drain must hand pushes to the processor", calls)
	}
	if dials := srv.dials.Load(); dials != 1 {
		t.Fatalf("dials = %d, want 1: drained conn was not reused", dials)
	}
}

func TestDrainOnContextTimeoutCloseDuringDrain(t *testing.T) {
	srv := &drainTestServer{slowDelay: 2 * time.Second}
	rdb := newDrainTestClient(startDrainTestServer(t, srv), 2, true, 5*time.Second)

	getFast(t, rdb)
	getSlowWithDeadline(t, rdb)

	done := make(chan error, 1)
	go func() { done <- rdb.Close() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Close blocked on an in-flight drain")
	}
	// The drain goroutine sees its conn closed and removes it from the closed
	// pool; give it a moment so the race detector observes that path.
	time.Sleep(50 * time.Millisecond)
}
