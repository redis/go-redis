package redis

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestPubSubNullEndpointMovingOnIdleConnReconnects pins the timer path of
// a MOVING with a null endpoint through the built-in handler: the
// handler marks the shared pub/sub connection timeS/2 later, from a
// timer, with no frame to follow. With the health check disabled the
// manager must still reconnect (to the same address) and resubscribe.
func TestPubSubNullEndpointMovingOnIdleConnReconnects(t *testing.T) {
	const channel = "moving-wake-ch"
	subscribeReply := fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(channel), channel)
	// MOVING seqID 1, timeS 2 (mark after 1s), null endpoint.
	const moving = ">4\r\n$6\r\nMOVING\r\n:1\r\n:2\r\n_\r\n"

	var subscribes atomic.Int32
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
					return "%0\r\n"
				case "subscribe":
					if subscribes.Add(1) == 1 {
						return subscribeReply + moving
					}
					return subscribeReply
				}
				return "+OK\r\n"
			})
		}
	}()

	client := NewClient(&Options{
		Addr:                      ln.Addr().String(),
		Protocol:                  3,
		MaxRetries:                -1,
		DisableIdentity:           true,
		PubSubHealthCheckInterval: -1,
		MaintNotificationsConfig:  &maintnotifications.Config{Mode: maintnotifications.ModeEnabled},
	})
	t.Cleanup(func() { _ = client.Close() })

	pubsub := client.Subscribe(context.Background(), channel)
	t.Cleanup(func() { _ = pubsub.Close() })

	deadline := time.Now().Add(5 * time.Second)
	for subscribes.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatalf("no resubscribe after the null-endpoint MOVING (subscribes = %d)", subscribes.Load())
		}
		time.Sleep(10 * time.Millisecond)
	}
}
