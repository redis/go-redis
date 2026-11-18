package redis

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
)

func TestGetAtReAuthCompletion(t *testing.T) {
	authSeen := make(chan struct{})
	srv := newStubServer(func(args []string) string {
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			return "-ERR unknown command 'hello'\r\n" // use RESP2 AUTH
		case "AUTH":
			if args[len(args)-1] == "rotated" {
				close(authSeen)
			}
			return "+OK\r\n"
		case "PING":
			return "+PONG\r\n"
		default:
			return "+OK\r\n"
		}
	})
	p := &capturingCredentialsProvider{}
	c := NewClient(&Options{
		Protocol: 2, DisableIdentity: true, Dialer: srv.dialer(),
		StreamingCredentialsProvider: p, PoolSize: 1, MaxActiveConns: 1,
		MaxRetries: -1, DialerRetries: -1, PoolTimeout: 5 * time.Second,
	})
	defer c.Close()
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	// Deliver the rotation while checked out. Put schedules re-auth on this socket.
	cn, err := c.connPool.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	p.capturedListener().OnNext(auth.NewBasicCredentials("u", "rotated"))
	c.connPool.Put(ctx, cn)
	select {
	case <-authSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("server did not see rotated AUTH")
	}
	// The server answers AUTH immediately. Do not wait for the worker to finish.
	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("PING overlapping re-auth: %v", err)
	}
}
