package redis

import (
	"context"
	"fmt"
	"io"
	"net"
	"slices"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// Sentinel publishes replica reconfiguration as "+slave-reconf-done"; a client
// subscribed to any other name never hears that a replica rejoined.
func TestSentinelFailoverListensForReplicaReconf(t *testing.T) {
	dial := func(_ context.Context, _, _ string) (net.Conn, error) {
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
				case "sentinel":
					reply = "*0\r\n"
					if args[1] == "get-master-addr-by-name" {
						reply = "*2\r\n+127.0.0.1\r\n+6379\r\n"
					}
				case "subscribe":
					reply = ""
					for i, channel := range args[1:] {
						reply += fmt.Sprintf("*3\r\n+subscribe\r\n+%s\r\n:%d\r\n", channel, i+1)
					}
					// Like Sentinel, deliver the event only to its subscribers.
					if slices.Contains(args[1:], interface{}("+slave-reconf-done")) {
						reply += "*3\r\n+message\r\n++slave-reconf-done\r\n+slave 127.0.0.1:6380\r\n"
					}
				}
				if _, err := io.WriteString(server, reply); err != nil {
					return
				}
			}
		}()
		return client, nil
	}

	updates := make(chan struct{}, 8)
	f := &sentinelFailover{
		opt: &FailoverOptions{
			MasterName: "mymaster", Dialer: dial, PoolSize: 1,
			MaxRetries: -1, DisableIdentity: true,
			ReadTimeout: time.Second, WriteTimeout: time.Second,
		},
		sentinelAddrs: []string{"sentinel:26379"},
		onUpdate:      func(context.Context) { updates <- struct{}{} },
	}
	t.Cleanup(func() { _ = f.Close() })

	if _, err := f.MasterAddr(context.Background()); err != nil {
		t.Fatal(err)
	}

	// The first update fires when the listener starts, the second on the event.
	for i := range 2 {
		select {
		case <-updates:
		case <-time.After(2 * time.Second):
			t.Fatalf("got %d topology updates, want 2: +slave-reconf-done was not received", i)
		}
	}
}
