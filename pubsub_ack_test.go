package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// A real RESP stream with explicit response gates. The peer drains commands,
// while tests decide exactly which response the single receiver sees next.
func newAckTestConnection(t *testing.T) (*pool.Conn, net.Conn) {
	t.Helper()
	client, server := net.Pipe()
	cn := pool.NewConn(client)
	done := make(chan struct{})
	go func() {
		defer close(done)
		rd := proto.NewReader(server)
		for {
			if _, err := rd.ReadReply(); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { _ = cn.Close(); _ = server.Close(); <-done })
	return cn, server
}

func newAckTestPubSub(t *testing.T) (*PubSub, net.Conn) {
	t.Helper()
	cn, peer := newAckTestConnection(t)
	ps := &PubSub{
		opt: &Options{Addr: "pipe", WriteTimeout: time.Second}, cn: cn,
		closeConn: func(c *pool.Conn) error { return c.Close() },
		newConn:   func(context.Context, string, []string) (*pool.Conn, error) { return nil, errors.New("dial disabled") },
	}
	ps.init()
	t.Cleanup(func() { _ = ps.Close() })
	return ps, peer
}

func receiveAckTestFrame(t *testing.T, ps *PubSub, peer net.Conn, kind, channel string) {
	t.Helper()
	written := make(chan error, 1)
	go func() {
		_, err := fmt.Fprintf(peer, "*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n:1\r\n", len(kind), kind, len(channel), channel)
		written <- err
	}()
	_, err := ps.ReceiveTimeout(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
}

func TestPubSubAckReplacementWaitsForOwnReply(t *testing.T) {
	t.Parallel()
	ps, peer := newAckTestPubSub(t)
	first, err := ps.SubscribeWithAck(context.Background(), "news")
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Unsubscribe(context.Background(), "news"); err != nil {
		t.Fatal(err)
	}
	second, err := ps.SubscribeWithAck(context.Background(), "news")
	if err != nil {
		t.Fatal(err)
	}
	receiveAckTestFrame(t, ps, peer, "subscribe", "news")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := first.Wait(ctx); err != nil {
		t.Fatalf("first confirmation: %v", err)
	}
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := second.Wait(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("old ACK confirmed replacement: %v", err)
	}
	receiveAckTestFrame(t, ps, peer, "unsubscribe", "news")
	receiveAckTestFrame(t, ps, peer, "subscribe", "news")
	if err := second.Wait(ctx); err != nil {
		t.Fatalf("replacement confirmation: %v", err)
	}
}

func TestPubSubAckCloseSettlesAllWaiters(t *testing.T) {
	t.Parallel()
	ps, _ := newAckTestPubSub(t)
	ack, err := ps.SubscribeWithAck(context.Background(), "one", "two")
	if err != nil {
		t.Fatal(err)
	}
	if err := ps.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		if err := ack.Wait(ctx); !errors.Is(err, pool.ErrClosed) {
			t.Fatalf("closed operation: %v", err)
		}
	}
}

func TestPubSubAckCancellationDoesNotWithdrawSharedResult(t *testing.T) {
	t.Parallel()
	ps, peer := newAckTestPubSub(t)
	ack, err := ps.SubscribeWithAck(context.Background(), "news", "news", "other")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ack.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled wait: %v", err)
	}
	receiveAckTestFrame(t, ps, peer, "subscribe", "news")
	receiveAckTestFrame(t, ps, peer, "subscribe", "news")
	if err := ack.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("partial batch confirmed: %v", err)
	}
	receiveAckTestFrame(t, ps, peer, "subscribe", "other")
	if err := ack.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPubSubAckOrdinarySubscribeCannotConfirmTrackedOperation(t *testing.T) {
	t.Parallel()
	ps, peer := newAckTestPubSub(t)
	if err := ps.Subscribe(context.Background(), "news"); err != nil {
		t.Fatal(err)
	}
	ack, err := ps.SubscribeWithAck(context.Background(), "news")
	if err != nil {
		t.Fatal(err)
	}
	receiveAckTestFrame(t, ps, peer, "subscribe", "news")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := ack.Wait(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("ordinary ACK confirmed tracked operation: %v", err)
	}
	receiveAckTestFrame(t, ps, peer, "subscribe", "news")
	if err := ack.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPubSubAckWriteFailureRetiresPending(t *testing.T) {
	t.Parallel()
	ps, peer := newAckTestPubSub(t)
	old, err := ps.SubscribeWithAck(context.Background(), "news")
	if err != nil {
		t.Fatal(err)
	}
	_ = peer.Close()
	if _, err := ps.SubscribeWithAck(context.Background(), "news"); err == nil {
		t.Fatal("write to closed peer succeeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := old.Wait(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("retired operation: %v", err)
	}
}

func TestPubSubAckReconnectDoesNotReplayPendingConfirmations(t *testing.T) {
	for _, trigger := range []string{"read", "ping"} {
		t.Run(trigger, func(t *testing.T) {
			t.Parallel()
			ps, oldPeer := newAckTestPubSub(t)
			old, err := ps.SubscribeWithAck(context.Background(), "news")
			if err != nil {
				t.Fatal(err)
			}
			cn, peer := newAckTestConnection(t)
			ps.newConn = func(context.Context, string, []string) (*pool.Conn, error) { return cn, nil }
			_ = oldPeer.Close()
			if trigger == "read" {
				_, err = ps.ReceiveTimeout(context.Background(), time.Second)
			} else {
				err = ps.Ping(context.Background())
			}
			if err == nil {
				t.Fatal("closed connection did not fail")
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := old.Wait(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("old operation did not retire: %v", err)
			}
			next, err := ps.SubscribeWithAck(context.Background(), "news")
			if err != nil {
				t.Fatal(err)
			}
			// The first reply belongs to automatic resubscription, not next.
			receiveAckTestFrame(t, ps, peer, "subscribe", "news")
			canceled, stop := context.WithCancel(context.Background())
			stop()
			if err := next.Wait(canceled); !errors.Is(err, context.Canceled) {
				t.Fatalf("automatic ACK confirmed explicit operation: %v", err)
			}
			receiveAckTestFrame(t, ps, peer, "subscribe", "news")
			if err := next.Wait(ctx); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPubSubAckFailedResubscribeClosesOnce(t *testing.T) {
	t.Parallel()
	ps, peer := newAckTestPubSub(t)
	_ = peer.Close()
	cn := ps.cn
	ps.cn = nil
	ps.channels = map[string]struct{}{"news": {}}
	ps.newConn = func(context.Context, string, []string) (*pool.Conn, error) { return cn, nil }
	closes := 0
	ps.closeConn = func(c *pool.Conn) error {
		closes++
		if closes > 1 {
			t.Fatal("connection cleanup called twice; cluster callback would dereference retired node")
		}
		return c.Close()
	}
	if _, err := ps.SubscribeWithAck(context.Background(), "news"); err == nil {
		t.Fatal("resubscribe to closed peer succeeded")
	}
	if closes != 1 {
		t.Fatalf("close count = %d", closes)
	}
}

func TestPubSubAckRejectedCommandRetiresConfirmation(t *testing.T) {
	t.Parallel()
	ps, peer := newAckTestPubSub(t)
	ack, err := ps.SubscribeWithAck(context.Background(), "news")
	if err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() { _, err := fmt.Fprint(peer, "-NOPERM subscription denied\r\n"); written <- err }()
	_, err = ps.ReceiveTimeout(context.Background(), time.Second)
	if err == nil {
		t.Fatal("expected Redis error")
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ack.Wait(ctx); err == nil || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("rejected operation did not retire: %v", err)
	}
	cn, replacementPeer := newAckTestConnection(t)
	ps.newConn = func(context.Context, string, []string) (*pool.Conn, error) { return cn, nil }
	next, err := ps.SubscribeWithAck(context.Background(), "news")
	if err != nil {
		t.Fatal(err)
	}
	receiveAckTestFrame(t, ps, replacementPeer, "subscribe", "news")
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := next.Wait(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("automatic ACK confirmed replacement: %v", err)
	}
	receiveAckTestFrame(t, ps, replacementPeer, "subscribe", "news")
	if err := next.Wait(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestPubSubAckLegacyRejectionPreservesConnection(t *testing.T) {
	t.Parallel()
	ps, peer := newAckTestPubSub(t)
	if err := ps.Subscribe(context.Background(), "good"); err != nil {
		t.Fatal(err)
	}
	receiveAckTestFrame(t, ps, peer, "subscribe", "good")
	if err := ps.Subscribe(context.Background(), "bad"); err != nil {
		t.Fatal(err)
	}
	written := make(chan error, 1)
	go func() { _, err := fmt.Fprint(peer, "-NOPERM subscription denied\r\n"); written <- err }()
	if _, err := ps.ReceiveTimeout(context.Background(), time.Second); err == nil {
		t.Fatal("expected Redis error")
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	go func() {
		_, err := fmt.Fprint(peer, "*3\r\n$7\r\nmessage\r\n$4\r\ngood\r\n$4\r\ndata\r\n")
		written <- err
	}()
	msg, err := ps.ReceiveTimeout(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if msg, ok := msg.(*Message); !ok || msg.Channel != "good" || msg.Payload != "data" {
		t.Fatalf("unexpected delivery: %#v", msg)
	}
	// An explicit confirmation cannot use the stream's now-ambiguous history.
	if err := ps.Unsubscribe(context.Background(), "bad"); err != nil {
		t.Fatal(err)
	}
	cn, replacementPeer := newAckTestConnection(t)
	ps.newConn = func(context.Context, string, []string) (*pool.Conn, error) { return cn, nil }
	ack, err := ps.SubscribeWithAck(context.Background(), "good")
	if err != nil {
		t.Fatal(err)
	}
	receiveAckTestFrame(t, ps, replacementPeer, "subscribe", "good")
	canceled, stop := context.WithCancel(context.Background())
	stop()
	if err := ack.Wait(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("automatic ACK confirmed operation: %v", err)
	}
	receiveAckTestFrame(t, ps, replacementPeer, "subscribe", "good")
	if err := ack.Wait(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestPubSubAckLegacyReadOnlyRetainsReconnectPolicy(t *testing.T) {
	t.Parallel()
	ps, peer := newAckTestPubSub(t)
	if err := ps.Subscribe(context.Background(), "news"); err != nil {
		t.Fatal(err)
	}
	receiveAckTestFrame(t, ps, peer, "subscribe", "news")
	cn, _ := newAckTestConnection(t)
	reconnected := false
	ps.newConn = func(context.Context, string, []string) (*pool.Conn, error) { reconnected = true; return cn, nil }
	written := make(chan error, 1)
	go func() { _, err := fmt.Fprint(peer, "-READONLY replica\r\n"); written <- err }()
	if _, err := ps.ReceiveTimeout(context.Background(), time.Second); err == nil {
		t.Fatal("expected Redis error")
	}
	if err := <-written; err != nil {
		t.Fatal(err)
	}
	if !reconnected {
		t.Fatal("legacy READONLY reconnect policy was bypassed")
	}
}
