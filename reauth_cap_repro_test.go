package redis

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
)

// TestGetAtReAuthCompletion covers the window between a Get's idle scan and
// its MaxActiveConns capacity check: the scan runs while the pool's only
// connection is in use by the re-authentication worker, and the worker
// returns the connection to StateIdle before the Get reaches the capacity
// check. The Get must acquire that connection instead of falling through
// to the dial path and failing with ErrPoolExhausted.
//
// getConn runs the scan and the capacity check back to back with no hook
// in between that a test could synchronize on, and holding the connection
// in use for longer only routes the Get into the re-auth wait path, which
// succeeds with and without the fix. A single attempt therefore hits the
// window only occasionally (~5% of runs on the unfixed code), so one CI
// run would almost always pass even without the fix. Repeating the
// rotation 200 times makes the unfixed code pass every attempt with
// probability well under 1%, while the whole loop still only takes a
// couple of seconds.
func TestGetAtReAuthCompletion(t *testing.T) {
	const attempts = 200

	// Buffered so the handler never blocks on the notification: delaying
	// the AUTH reply would change the timing this test races against.
	authSeen := make(chan string, 1024)
	srv := newStubServer(func(args []string) string {
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			return "-ERR unknown command 'hello'\r\n" // use RESP2 AUTH
		case "AUTH":
			authSeen <- args[len(args)-1]
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

	waitAuth := func(password string) {
		t.Helper()
		timeout := time.After(5 * time.Second)
		for {
			select {
			case seen := <-authSeen:
				if seen == password {
					return
				}
			case <-timeout:
				t.Fatalf("server did not see AUTH for %q", password)
			}
		}
	}

	for i := 0; i < attempts; i++ {
		password := fmt.Sprintf("rotated-%d", i)
		// Deliver the rotation while checked out. Put schedules re-auth
		// on this socket.
		cn, err := c.connPool.Get(ctx)
		if err != nil {
			t.Fatal(err)
		}
		p.capturedListener().OnNext(auth.NewBasicCredentials("u", password))
		c.connPool.Put(ctx, cn)
		waitAuth(password)
		// The server answers AUTH immediately. Do not wait for the worker
		// to finish: this Ping is the Get that races its completion.
		if err := c.Ping(ctx).Err(); err != nil {
			t.Fatalf("attempt %d: PING overlapping re-auth: %v", i, err)
		}
	}
}
