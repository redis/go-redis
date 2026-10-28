package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"testing"
	"time"
)

func TestTLSDialerContext(t *testing.T) {
	for _, kind := range []string{"default", "sentinel"} {
		for _, mode := range []string{"deadline", "cancel", "dial timeout"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				ln, err := net.Listen("tcp", "127.0.0.1:0")
				if err != nil {
					t.Fatal(err)
				}
				started := make(chan struct{})
				release := make(chan struct{})
				serverDone := make(chan struct{})
				var dialDone chan struct{}
				go func() {
					defer close(serverDone)
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					defer conn.Close()
					_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
					var b [1]byte
					if _, err := conn.Read(b[:]); err == nil {
						close(started)
					}
					<-release
				}()
				t.Cleanup(func() {
					close(release)
					_ = ln.Close()
					<-serverDone
					if dialDone != nil {
						<-dialDone
					}
				})

				dialTimeout := 2 * time.Second
				ctx := context.Background()
				cancel := func() {}
				wantErr := context.DeadlineExceeded
				switch mode {
				case "deadline":
					ctx, cancel = context.WithTimeout(ctx, 100*time.Millisecond)
				case "cancel":
					dialTimeout = 0
					ctx, cancel = context.WithCancel(ctx)
					wantErr = context.Canceled
				case "dial timeout":
					dialTimeout = 100 * time.Millisecond
				}
				defer cancel()
				config := &tls.Config{InsecureSkipVerify: true}
				dial := NewDialer(&Options{DialTimeout: dialTimeout, TLSConfig: config})
				if kind == "sentinel" {
					host, port, err := net.SplitHostPort(ln.Addr().String())
					if err != nil {
						t.Fatal(err)
					}
					// Isolate Sentinel discovery from the real TLS dial to the master.
					sentinel := &SentinelClient{baseClient: &baseClient{}}
					sentinel.initHooks(hooks{process: func(_ context.Context, cmd Cmder) error {
						cmd.(*StringSliceCmd).SetVal([]string{host, port})
						return nil
					}})
					dial = masterReplicaDialer(&sentinelFailover{
						opt:        &FailoverOptions{DialTimeout: dialTimeout, TLSConfig: config},
						sentinel:   sentinel,
						masterAddr: ln.Addr().String(),
					}, &Options{DialTimeout: dialTimeout})
				}

				result := make(chan error, 1)
				dialDone = make(chan struct{})
				go func() {
					defer close(dialDone)
					conn, err := dial(ctx, "tcp", ln.Addr().String())
					if err == nil && conn != nil {
						_ = conn.Close()
					}
					result <- err
				}()
				select {
				case <-started:
				case err := <-result:
					t.Fatalf("dial returned before TLS handshake started: %v", err)
				case <-time.After(3 * time.Second):
					t.Fatal("TLS handshake did not start")
				}
				if mode == "cancel" {
					cancel()
				}
				select {
				case err := <-result:
					if !errors.Is(err, wantErr) {
						t.Fatalf("dial error = %v, want %v", err, wantErr)
					}
				case <-time.After(time.Second):
					t.Fatalf("TLS dial did not honor %s", mode)
				}
			})
		}
	}
}
