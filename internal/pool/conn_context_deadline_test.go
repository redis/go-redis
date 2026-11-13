package pool

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestConnContextDeadline(t *testing.T) {
	for _, operation := range []string{"read", "write flush", "write buffer"} {
		for _, tc := range []struct {
			name        string
			ctxTimeout  time.Duration
			ioTimeout   time.Duration
			wantContext bool
		}{
			{"expired context", -time.Second, time.Second, true},
			{"context expires during IO", 10 * time.Millisecond, time.Second, true},
			{"context with no IO timeout", 10 * time.Millisecond, 0, true},
			{"IO timeout first", time.Hour, time.Millisecond, false},
			{"no context deadline", 0, time.Millisecond, false},
			{"deadline updates disabled", -time.Second, -1, false},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				server, client := net.Pipe()
				t.Cleanup(func() { _ = server.Close() })
				cn := NewConn(client)
				t.Cleanup(func() { _ = cn.Close() })
				if tc.ioTimeout < 0 {
					if err := client.SetDeadline(time.Now().Add(-time.Second)); err != nil {
						t.Fatal(err)
					}
				}

				ctx := context.Background()
				if tc.ctxTimeout != 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.ctxTimeout)
					defer cancel()
				}
				var err error
				if operation == "read" {
					err = cn.WithReader(ctx, tc.ioTimeout, func(rd *proto.Reader) error {
						_, err := rd.ReadReply()
						return err
					})
				} else {
					arg := "ping"
					if operation == "write buffer" {
						arg = strings.Repeat("x", cn.bw.Size()+1)
					}
					err = cn.WithWriter(ctx, tc.ioTimeout, func(wr *proto.Writer) error {
						return wr.WriteArgs([]interface{}{arg})
					})
				}
				if tc.wantContext {
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("expected context deadline exceeded, got %v", err)
					}
				} else {
					var netErr net.Error
					if !errors.As(err, &netErr) || !netErr.Timeout() || errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("expected socket timeout, got %v", err)
					}
				}
			})
		}
	}
}

func TestConnContextDeadlinePreservesResult(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		for _, result := range []error{nil, io.EOF, proto.RedisError("ERR test")} {
			t.Run(operation+"/"+fmt.Sprint(result), func(t *testing.T) {
				server, client := net.Pipe()
				defer server.Close()
				cn := NewConn(client)
				defer cn.Close()
				ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
				var err error
				if operation == "read" {
					err = cn.WithReader(ctx, time.Second, func(*proto.Reader) error { return result })
				} else {
					err = cn.WithWriter(ctx, time.Second, func(*proto.Writer) error { return result })
				}
				if err != result {
					t.Fatalf("expected %v, got %v", result, err)
				}
			})
		}
	}
}

func TestConnContextDeadlinePreservesEarlierTimeout(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	cn := NewConn(client)
	defer cn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
	defer cancel()

	var readErr error
	err := cn.WithReader(ctx, time.Millisecond, func(rd *proto.Reader) error {
		_, readErr = rd.ReadReply()
		// The context can end while the callback handles an earlier socket timeout.
		cancel()
		return readErr
	})
	var netErr net.Error
	if err != readErr || !errors.As(err, &netErr) || !netErr.Timeout() || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected original socket timeout, got %v", err)
	}
}

func TestConnContextDeadlineErrorIsNetError(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	cn := NewConn(client)
	defer cn.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()

	err := cn.WithReader(ctx, time.Hour, func(rd *proto.Reader) error {
		_, err := rd.ReadReply()
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expected a context deadline error, got %v", err)
	}
	netErr, ok := err.(net.Error)
	if !ok || !netErr.Timeout() {
		t.Fatalf("expected err.(net.Error) with Timeout() true, got %T %v", err, err)
	}
}

func TestConnContextDeadlineHardRead(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	cn := NewConn(client)
	defer cn.Close()
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err := cn.WithReader(ctx, time.Second, func(*proto.Reader) error {
		return cn.WithReaderHardDeadline(time.Millisecond, func(rd *proto.Reader) error {
			_, err := rd.ReadReply()
			return err
		})
	})
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() || errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hard deadline must return a socket timeout, got %v", err)
	}
}

func TestConnContextDeadlineAfterSocketReplacement(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	cn := NewConn(client)
	defer cn.Close()
	newServer, newClient := net.Pipe()
	defer newServer.Close()
	cn.SetNetConn(newClient)
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		_, err := rd.ReadReply()
		return err
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("new socket deadline: %v", err)
	}
	go func() { _, _ = newServer.Write([]byte("+OK\r\n")) }()
	err = cn.WithReader(context.Background(), time.Second, func(rd *proto.Reader) error {
		reply, err := rd.ReadReply()
		if err == nil && reply != "OK" {
			t.Errorf("new socket reply: %#v", reply)
		}
		return err
	})
	if err != nil {
		t.Fatalf("read after context deadline: %v", err)
	}
}

func TestConnContextDeadlinePreservesWrappedSocketError(t *testing.T) {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	deadline, _ := ctx.Deadline()
	socketErr := &net.OpError{Op: "read", Net: "tcp", Addr: &net.TCPAddr{IP: net.IPv4(10, 0, 0, 1), Port: 6379}, Err: os.ErrDeadlineExceeded}
	original := fmt.Errorf("custom transport: %w", socketErr)
	err := contextDeadlineError(ctx, deadline, original)
	if !errors.Is(err, context.DeadlineExceeded) || !errors.Is(err, original) {
		t.Fatalf("expected context deadline and original transport error, got %v", err)
	}
	var opErr *net.OpError
	if !errors.As(err, &opErr) || opErr != socketErr {
		t.Fatalf("lost socket error: %v", err)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("lost network timeout: %v", err)
	}
	if want := context.DeadlineExceeded.Error() + ": " + original.Error(); err.Error() != want {
		t.Fatalf("error text = %q, want %q", err, want)
	}
}
