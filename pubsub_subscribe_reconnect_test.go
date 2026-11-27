package redis

import (
	"context"
	"io"
	"net"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// A Subscribe whose write fails reconnects; the replacement connection must
// be subscribed to the channels of that call, not only to the older ones.
func TestPubSubSubscribeSurvivesReconnect(t *testing.T) {
	firstClosed := make(chan struct{})
	subscribed := make(chan []interface{}, 8)
	var dials atomic.Int32
	dial := func(_ context.Context, _, _ string) (net.Conn, error) {
		first := dials.Add(1) == 1
		client, server := net.Pipe()
		go func() {
			defer server.Close()
			reader := proto.NewReader(server)
			for {
				args, err := reader.ReadSlice()
				if err != nil {
					return
				}
				reply := "+OK\r\n"
				switch args[0] {
				case "hello":
					reply = "-ERR unknown command 'hello'\r\n"
				case "subscribe":
					if first {
						// Drop the connection so the next write on it fails.
						server.Close()
						close(firstClosed)
						return
					}
					subscribed <- args[1:]
					continue
				}
				if _, err := io.WriteString(server, reply); err != nil {
					return
				}
			}
		}()
		return client, nil
	}

	client := NewClient(&Options{
		Addr: "redis:6379", Dialer: dial, MaxRetries: -1, DisableIdentity: true,
		ReadTimeout: time.Second, WriteTimeout: time.Second,
	})
	defer client.Close()

	ctx := context.Background()
	ps := client.Subscribe(ctx, "old")
	defer ps.Close()
	<-firstClosed

	if err := ps.Subscribe(ctx, "new"); err == nil {
		t.Fatal("Subscribe on a dropped connection returned nil error")
	}

	var got []interface{}
	timeout := time.After(time.Second)
	for !slices.Contains(got, interface{}("new")) {
		select {
		case channels := <-subscribed:
			got = append(got, channels...)
		case <-timeout:
			t.Fatalf("replacement connection subscribed to %v, want it to include \"new\"", got)
		}
	}
}
