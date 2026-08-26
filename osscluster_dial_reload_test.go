package redis

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// startMockPONGServer speaks enough RESP for Ping() against a ClusterClient
// that uses ClusterSlots (no CLUSTER SLOTS command on the wire).
func startMockPONGServer(t *testing.T) net.Listener {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				r := bufio.NewReader(c)
				for {
					args, err := readRESPCommand(r)
					if err != nil {
						return
					}
					if len(args) == 0 {
						continue
					}
					switch strings.ToUpper(args[0]) {
					case "HELLO":
						_, _ = c.Write([]byte("-ERR unknown command 'hello'\r\n"))
					case "PING":
						_, _ = c.Write([]byte("+PONG\r\n"))
					default:
						_, _ = c.Write([]byte("+OK\r\n"))
					}
				}
			}(conn)
		}
	}()
	return ln
}

func TestClusterClientReloadsStateOnConnectionRefused(t *testing.T) {
	live := startMockPONGServer(t)
	defer live.Close()
	liveAddr := live.Addr().String()

	dead, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen dead: %v", err)
	}
	deadAddr := dead.Addr().String()
	_ = dead.Close()

	var currentAddr atomic.Value
	currentAddr.Store(deadAddr)

	ctx := context.Background()
	client := NewClusterClient(&ClusterOptions{
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{
				Start: 0,
				End:   16383,
				Nodes: []ClusterNode{{Addr: currentAddr.Load().(string)}},
			}}, nil
		},
		MaxRedirects:               3,
		ClusterStateReloadInterval: time.Hour,
		DialTimeout:                200 * time.Millisecond,
		DialerRetries:              1,
		DialerRetryTimeout:         time.Millisecond,
		MinRetryBackoff:            -1,
		MaxRetryBackoff:            -1,
	})
	defer client.Close()

	if err := client.Ping(ctx).Err(); err == nil {
		t.Fatal("expected ping against the closed listener to fail")
	}

	currentAddr.Store(liveAddr)

	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping after topology change: %v", err)
	}
}
