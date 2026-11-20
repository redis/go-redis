package redis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// TestPipelineSetupErrorStamped is a regression test for
// https://github.com/redis/go-redis/issues/4310: when connection setup
// (AUTH/SELECT during initConn) fails with a Redis error reply, Pipeline
// and TxPipelined return the error from Exec but every queued command
// keeps a nil error, because no command was ever sent and no reply was
// read. Every queued command must carry the setup error instead.
func TestPipelineSetupErrorStamped(t *testing.T) {
	helloOK := "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n"

	t.Run("wrong password pipeline", func(t *testing.T) {
		stub := newStubServer(func(cmd []string) string {
			switch strings.ToLower(cmd[0]) {
			case "hello", "auth":
				return "-WRONGPASS invalid username-password pair or user is disabled\r\n"
			default:
				return "+OK\r\n"
			}
		})
		rdb := newStubClient(stub, &Options{Password: "wrong", Protocol: 2})
		defer rdb.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pipe := rdb.Pipeline()
		set := pipe.Set(ctx, "k", "v", 0)
		get := pipe.Get(ctx, "k")
		_, err := pipe.Exec(ctx)
		if err == nil {
			t.Fatal("expected Exec to fail on authentication error")
		}
		if set.Err() == nil || get.Err() == nil {
			t.Fatalf("expected queued commands to carry the setup error, got set=%v get=%v (Exec err: %v)", set.Err(), get.Err(), err)
		}
		stub.assertNoDataCommands(t)
	})

	t.Run("select out of range pipeline", func(t *testing.T) {
		stub := newStubServer(func(cmd []string) string {
			switch strings.ToLower(cmd[0]) {
			case "hello":
				return helloOK
			case "select":
				return "-ERR DB index is out of range\r\n"
			default:
				return "+OK\r\n"
			}
		})
		rdb := newStubClient(stub, &Options{Password: "secret", DB: 9999, Protocol: 2})
		defer rdb.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pipe := rdb.Pipeline()
		set := pipe.Set(ctx, "k", "v", 0)
		get := pipe.Get(ctx, "k")
		_, err := pipe.Exec(ctx)
		if err == nil {
			t.Fatal("expected Exec to fail on SELECT error")
		}
		if set.Err() == nil || get.Err() == nil {
			t.Fatalf("expected queued commands to carry the setup error, got set=%v get=%v (Exec err: %v)", set.Err(), get.Err(), err)
		}
		stub.assertNoDataCommands(t)
	})

	t.Run("wrong password tx pipelined", func(t *testing.T) {
		stub := newStubServer(func(cmd []string) string {
			switch strings.ToLower(cmd[0]) {
			case "hello", "auth":
				return "-WRONGPASS invalid username-password pair or user is disabled\r\n"
			default:
				return "+OK\r\n"
			}
		})
		rdb := newStubClient(stub, &Options{Password: "wrong", Protocol: 2})
		defer rdb.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var incr *IntCmd
		_, err := rdb.TxPipelined(ctx, func(p Pipeliner) error {
			incr = p.Incr(ctx, "n")
			return nil
		})
		if err == nil {
			t.Fatal("expected TxPipelined to fail on authentication error")
		}
		if incr.Err() == nil {
			t.Fatalf("expected queued command to carry the setup error (TxPipelined err: %v)", err)
		}
		stub.assertNoDataCommands(t)
	})

	t.Run("wrong password pipeline resp3", func(t *testing.T) {
		stub := newStubServer(func(cmd []string) string {
			switch strings.ToLower(cmd[0]) {
			case "hello", "auth":
				return "-WRONGPASS invalid username-password pair or user is disabled\r\n"
			default:
				return "+OK\r\n"
			}
		})
		rdb := newStubClient(stub, &Options{Password: "wrong", Protocol: 3})
		defer rdb.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pipe := rdb.Pipeline()
		set := pipe.Set(ctx, "k", "v", 0)
		_, err := pipe.Exec(ctx)
		if err == nil {
			t.Fatal("expected Exec to fail on authentication error")
		}
		if set.Err() == nil {
			t.Fatalf("expected queued command to carry the setup error (Exec err: %v)", err)
		}
		stub.assertNoDataCommands(t)
	})

	t.Run("control pipeline succeeds", func(t *testing.T) {
		stub := newStubServer(func(cmd []string) string {
			switch strings.ToLower(cmd[0]) {
			case "hello":
				return helloOK
			case "set":
				return "+OK\r\n"
			case "get":
				return "$1\r\nv\r\n"
			default:
				return "+OK\r\n"
			}
		})
		rdb := newStubClient(stub, &Options{Password: "secret", Protocol: 2})
		defer rdb.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		pipe := rdb.Pipeline()
		set := pipe.Set(ctx, "k", "v", 0)
		get := pipe.Get(ctx, "k")
		_, err := pipe.Exec(ctx)
		if err != nil {
			t.Fatalf("expected Exec to succeed, got %v", err)
		}
		if set.Err() != nil || get.Err() != nil {
			t.Fatalf("expected nil command errors, got set=%v get=%v", set.Err(), get.Err())
		}
		if got := get.Val(); got != "v" {
			t.Fatalf("expected GET to return v, got %q", got)
		}
	})
}

func newStubClient(stub *stubServer, opts *Options) *Client {
	opts.Addr = "stub:6379"
	opts.Dialer = stub.dialer()
	opts.MaxRetries = 0
	opts.DialTimeout = 2 * time.Second
	opts.ReadTimeout = 2 * time.Second
	opts.WriteTimeout = 2 * time.Second
	opts.DisableIdentity = true
	return NewClient(opts)
}

// stubServer is a minimal fake Redis server: it reads RESP arrays and
// answers each command with a scripted reply.
type stubServer struct {
	mu      sync.Mutex
	seen    [][]string
	handler func(cmd []string) string
}

func newStubServer(handler func(cmd []string) string) *stubServer {
	return &stubServer{handler: handler}
}

func (s *stubServer) dialer() func(ctx context.Context, network, addr string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		clientConn, serverConn := net.Pipe()
		go s.serve(serverConn)
		return clientConn, nil
	}
}

func (s *stubServer) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	for {
		cmd, err := readRESPArray(r)
		if err != nil {
			return
		}
		s.mu.Lock()
		s.seen = append(s.seen, cmd)
		s.mu.Unlock()
		if _, err := io.WriteString(conn, s.handler(cmd)); err != nil {
			return
		}
	}
}

func (s *stubServer) assertNoDataCommands(t *testing.T) {
	t.Helper()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cmd := range s.seen {
		switch strings.ToLower(cmd[0]) {
		case "set", "get", "incr", "multi", "exec":
			t.Fatalf("queued command %q reached the server although setup failed (seen: %v)", cmd[0], s.seen)
		}
	}
}

func readRESPArray(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected RESP array header, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		if !strings.HasPrefix(hdr, "$") {
			return nil, fmt.Errorf("expected RESP bulk header, got %q", hdr)
		}
		ln, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, ln+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:ln]))
	}
	return args, nil
}
