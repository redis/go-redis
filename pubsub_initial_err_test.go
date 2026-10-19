package redis

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestSubscribeInitialDialErrorReachesReceive pins that the dial error of
// the subscribe Client.Subscribe runs — which it cannot return — is
// delivered to the PubSub, so the documented Subscribe-then-Receive
// pattern reports an unreachable address instead of blocking.
func TestSubscribeInitialDialErrorReachesReceive(t *testing.T) {
	// A port nothing listens on.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()

	client := NewClient(&Options{
		Addr:          addr,
		DialerRetries: 1,
		DialTimeout:   200 * time.Millisecond,
	})
	defer client.Close()

	ctx := context.Background()
	for _, tc := range []struct {
		name      string
		subscribe func() *PubSub
	}{
		{"Subscribe", func() *PubSub { return client.Subscribe(ctx, "ch") }},
		{"PSubscribe", func() *PubSub { return client.PSubscribe(ctx, "p*") }},
		{"SSubscribe", func() *PubSub { return client.SSubscribe(ctx, "sch") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ps := tc.subscribe()
			defer ps.Close()

			msg, err := ps.ReceiveTimeout(ctx, 5*time.Second)
			if err == nil {
				t.Fatalf("Receive = %T, want the dial error", msg)
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() && strings.Contains(err.Error(), "receive timeout") {
				t.Fatal("Receive blocked until its timeout: the initial dial error was not delivered")
			}
			var opErr *net.OpError
			if !errors.As(err, &opErr) {
				t.Fatalf("Receive error = %v, want the dial's *net.OpError", err)
			}
		})
	}
}

// TestSubscribeInitialAuthErrorReachesReceiveThenRetries pins that a
// rejected handshake reaches the first Receive, and that delivering it
// keeps the registration: once the server accepts, the reconnect
// subscribes and the confirmation follows the error.
func TestSubscribeInitialAuthErrorReachesReceiveThenRetries(t *testing.T) {
	var reject atomic.Bool
	reject.Store(true)
	addr := listenSubscribeServer(t, &reject)

	client := NewClient(&Options{
		Addr:            addr,
		Password:        "wrong",
		Protocol:        2,
		DisableIdentity: true,
		DialerRetries:   1,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	ctx := context.Background()
	ps := client.Subscribe(ctx, "ch")
	defer ps.Close()

	_, err := ps.ReceiveTimeout(ctx, 5*time.Second)
	if err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("first Receive error = %v, want the WRONGPASS handshake error", err)
	}

	reject.Store(false)
	for {
		ev, err := ps.ReceiveTimeout(ctx, 10*time.Second)
		if err != nil {
			if strings.Contains(err.Error(), "WRONGPASS") {
				continue // a retry that raced the flip; only the first failure is required
			}
			t.Fatalf("Receive after the server accepts: %v, want the reconnect's subscribe confirmation", err)
		}
		sub, ok := ev.(*Subscription)
		if !ok {
			continue
		}
		if sub.Kind != "subscribe" || sub.Channel != "ch" {
			t.Fatalf("confirmation = %+v, want subscribe ch", sub)
		}
		return
	}
}

// TestSubscribeCancelledContextStillSubscribes pins that Client.Subscribe
// with a context already done keeps the channels: Receive reports the
// cancellation, and the dial it started subscribes them.
func TestSubscribeCancelledContextStillSubscribes(t *testing.T) {
	var reject atomic.Bool
	client := NewClient(&Options{
		Addr:            listenSubscribeServer(t, &reject),
		Protocol:        2,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	ps := client.Subscribe(cancelled, "ch")
	defer ps.Close()

	if channels, _, _ := ps.handle.Subscriptions(); len(channels) != 1 || channels[0] != "ch" {
		t.Fatalf("subscriptions = %v, want [ch]", channels)
	}
	// The cancellation surfaces first, unless the local dial settled
	// before the abandoned wait was checked (Subscribe then succeeded).
	ctx := context.Background()
	ev, err := ps.ReceiveTimeout(ctx, 5*time.Second)
	if errors.Is(err, context.Canceled) {
		ev, err = ps.ReceiveTimeout(ctx, 5*time.Second)
	}
	if err != nil {
		t.Fatalf("second Receive: %v, want the subscribe confirmation", err)
	}
	if sub, ok := ev.(*Subscription); !ok || sub.Kind != "subscribe" || sub.Channel != "ch" {
		t.Fatalf("second Receive = %#v, want subscribe ch", ev)
	}
}

// listenSubscribeServer serves a minimal RESP2 server that confirms
// SUBSCRIBE ch, rejecting AUTH while reject is set; it returns its
// address.
func listenSubscribeServer(t *testing.T, reject *atomic.Bool) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			netConn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveTestRESPConn(netConn, func(command string) string {
				switch command {
				case "hello":
					return "-ERR unknown command 'hello'\r\n"
				case "auth":
					if reject.Load() {
						return "-WRONGPASS invalid username-password pair or user is disabled.\r\n"
					}
					return "+OK\r\n"
				case "subscribe":
					return "*3\r\n$9\r\nsubscribe\r\n$2\r\nch\r\n:1\r\n"
				case "ping":
					return "*2\r\n$4\r\npong\r\n$0\r\n\r\n"
				}
				return "+OK\r\n"
			})
		}
	}()
	return ln.Addr().String()
}
