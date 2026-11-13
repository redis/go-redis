package redis_test

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Exercise real failed connections, including a TCP peer that stalls TLS,
// without relying on an external address being unreachable on every host.
func TestDialTimeoutBoundsRetries(t *testing.T) {
	for _, stalledTLS := range []bool{false, true} {
		name := "connection_refused"
		if stalledTLS {
			name = "TLS_timeout"
		}
		t.Run(name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			addr := ln.Addr().String()
			if stalledTLS {
				done := make(chan struct{})
				go func() {
					defer close(done)
					var conns []net.Conn
					defer func() {
						for _, cn := range conns {
							cn.Close()
						}
					}()
					for {
						cn, err := ln.Accept()
						if err != nil {
							return
						}
						conns = append(conns, cn)
					}
				}()
				defer func() { ln.Close(); <-done }()
			} else {
				ln.Close()
			}
			opt := &redis.Options{Addr: addr, DialTimeout: 200 * time.Millisecond}
			if stalledTLS {
				opt.TLSConfig = &tls.Config{InsecureSkipVerify: true}
			}
			client := redis.NewClient(opt)
			defer client.Close()
			start := time.Now()
			err = client.Get(context.Background(), "unreachable").Err()
			elapsed := time.Since(start)
			if err == nil {
				t.Fatal("expected a connection error")
			}
			// Allow scheduling slack while distinguishing one shared timeout from
			// the repeated per-attempt and per-command windows of the regression.
			if elapsed > 600*time.Millisecond {
				t.Fatalf("dial retries exceeded the shared DialTimeout budget: %v", elapsed)
			}
		})
	}
}
