package redis

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type txQueueErrorServer struct {
	ln        net.Listener
	execSeen  atomic.Int32
	queueErr  string
	execReply string
	resp3     bool
}

func (s *txQueueErrorServer) Addr() string { return s.ln.Addr().String() }
func (s *txQueueErrorServer) Close() error { return s.ln.Close() }

func (s *txQueueErrorServer) handle(c net.Conn) {
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

		if strings.EqualFold(args[0], "hello") {
			if s.resp3 {
				_, _ = c.Write([]byte("%7\r\n+server\r\n+redis\r\n+version\r\n$5\r\n7.2.0\r\n+proto\r\n:3\r\n+id\r\n:1\r\n+mode\r\n+standalone\r\n+role\r\n+master\r\n+modules\r\n*0\r\n"))
				continue
			}
			_, _ = c.Write([]byte("-ERR unknown command 'hello'\r\n"))
			continue
		}

		if strings.EqualFold(args[0], "multi") {
			_, _ = c.Write([]byte("+OK\r\n"))
			continue
		}

		if strings.EqualFold(args[0], "set") && len(args) > 1 && args[1] == "b" {
			_, _ = c.Write([]byte(s.queueErr))
			continue
		}

		if strings.EqualFold(args[0], "exec") {
			s.execSeen.Add(1)
			_, _ = c.Write([]byte(s.execReply))
			continue
		}

		_, _ = c.Write([]byte(fmt.Sprintf("+%s\r\n", strings.ToUpper(args[0]))))
	}
}

func startTxQueueErrorServer(t *testing.T) *txQueueErrorServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := &txQueueErrorServer{
		ln:        ln,
		queueErr:  "-ERR in transaction context, keys must in same slot\r\n",
		execReply: "-EXECABORT Transaction discarded because of previous errors.\r\n",
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.handle(conn)
		}
	}()
	return s
}

func fakeRESP3PushNotification(notificationType string, args ...string) string {
	buf := &bytes.Buffer{}
	fmt.Fprintf(buf, ">%d\r\n", 1+len(args))
	fmt.Fprintf(buf, "$%d\r\n%s\r\n", len(notificationType), notificationType)
	for _, arg := range args {
		fmt.Fprintf(buf, "$%d\r\n%s\r\n", len(arg), arg)
	}
	return buf.String()
}

func TestTxPipelineExecReturnsQueuedRedisError(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	pipe := client.TxPipeline()
	pipe.Set(ctx, "a", 1, 0)
	pipe.Set(ctx, "b", 1, 0)

	cmds, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error", got)
	}
	if !IsExecAbortError(err) {
		t.Fatalf("Exec() error = %v, want IsExecAbortError to be true after draining EXECABORT", err)
	}
	if len(cmds) != 2 {
		t.Fatalf("Exec() returned %d cmds, want 2", len(cmds))
	}
	for i, cmd := range cmds {
		if cmdErr := cmd.Err(); cmdErr == nil || !strings.Contains(cmdErr.Error(), "ERR in transaction context, keys must in same slot") {
			t.Fatalf("cmd[%d] err = %v, want queued Redis error", i, cmdErr)
		}
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestTxPipelineExecDrainsExecAbortOnStickyConnection(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	err := client.Watch(ctx, func(tx *Tx) error {
		pipe := tx.TxPipeline()
		pipe.Set(ctx, "a", 1, 0)
		pipe.Set(ctx, "b", 1, 0)

		_, err := pipe.Exec(ctx)
		if err == nil {
			t.Fatal("Exec() error = nil, want queued Redis error")
		}
		if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
			t.Fatalf("Exec() error = %q, want queued Redis error", got)
		}
		if !IsExecAbortError(err) {
			t.Fatalf("Exec() error = %v, want IsExecAbortError to be true after draining EXECABORT", err)
		}

		pong, pingErr := tx.Ping(ctx).Result()
		if pingErr != nil {
			t.Fatalf("Ping() error = %v, want nil", pingErr)
		}
		if pong != "PING" {
			t.Fatalf("Ping() = %q, want %q", pong, "PING")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Watch() error = %v, want nil", err)
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestTxPipelineExecPreservesQueuedErrorHelpers(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.queueErr = "-OOM command not allowed when used memory > 'maxmemory'\r\n"
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	pipe := client.TxPipeline()
	pipe.Set(ctx, "a", 1, 0)
	pipe.Set(ctx, "b", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if !IsOOMError(err) {
		t.Fatalf("Exec() error = %v, want IsOOMError to be true", err)
	}
	if !IsExecAbortError(err) {
		t.Fatalf("Exec() error = %v, want IsExecAbortError to be true", err)
	}
}

func TestTxPipelineExecDrainsExecArrayOnStickyConnection(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*1\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	err := client.Watch(ctx, func(tx *Tx) error {
		pipe := tx.TxPipeline()
		pipe.Set(ctx, "a", 1, 0)
		pipe.Set(ctx, "b", 1, 0)

		_, err := pipe.Exec(ctx)
		if err == nil {
			t.Fatal("Exec() error = nil, want queued Redis error")
		}
		if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
			t.Fatalf("Exec() error = %q, want queued Redis error", got)
		}

		pong, pingErr := tx.Ping(ctx).Result()
		if pingErr != nil {
			t.Fatalf("Ping() error = %v, want nil", pingErr)
		}
		if pong != "PING" {
			t.Fatalf("Ping() = %q, want %q", pong, "PING")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Watch() error = %v, want nil", err)
	}
}

func TestTxPipelineExecDrainsExecArrayWithErrorElement(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*2\r\n-WRONGTYPE Operation against a key holding the wrong kind of value\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	err := client.Watch(ctx, func(tx *Tx) error {
		pipe := tx.TxPipeline()
		pipe.Set(ctx, "a", 1, 0)
		pipe.Set(ctx, "b", 1, 0)

		_, err := pipe.Exec(ctx)
		if err == nil {
			t.Fatal("Exec() error = nil, want queued Redis error")
		}
		if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
			t.Fatalf("Exec() error = %q, want queued Redis error", got)
		}

		pong, pingErr := tx.Ping(ctx).Result()
		if pingErr != nil {
			t.Fatalf("Ping() error = %v, want nil", pingErr)
		}
		if pong != "PING" {
			t.Fatalf("Ping() = %q, want %q", pong, "PING")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Watch() error = %v, want nil", err)
	}
}

func TestTxPipelineExecDrainsExecArrayAfterRESP3Push(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.resp3 = true
	srv.execReply = fakeRESP3PushNotification("MOVING", "slot", "123") + "*1\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		Protocol:     3,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	ctx := context.Background()
	err := client.Watch(ctx, func(tx *Tx) error {
		pipe := tx.TxPipeline()
		pipe.Set(ctx, "a", 1, 0)
		pipe.Set(ctx, "b", 1, 0)

		_, err := pipe.Exec(ctx)
		if err == nil {
			t.Fatal("Exec() error = nil, want queued Redis error")
		}
		if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
			t.Fatalf("Exec() error = %q, want queued Redis error", got)
		}

		pong, pingErr := tx.Ping(ctx).Result()
		if pingErr != nil {
			t.Fatalf("Ping() error = %v, want nil", pingErr)
		}
		if pong != "PING" {
			t.Fatalf("Ping() = %q, want %q", pong, "PING")
		}

		return nil
	})
	if err != nil {
		t.Fatalf("Watch() error = %v, want nil", err)
	}
}
