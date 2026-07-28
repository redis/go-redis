package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type txQueueErrorServer struct {
	ln       net.Listener
	execSeen atomic.Int32
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
			_, _ = c.Write([]byte("-ERR unknown command 'hello'\r\n"))
			continue
		}

		if strings.EqualFold(args[0], "multi") {
			_, _ = c.Write([]byte("+OK\r\n"))
			continue
		}

		if strings.EqualFold(args[0], "set") && len(args) > 1 && args[1] == "b" {
			_, _ = c.Write([]byte("-ERR in transaction context, keys must in same slot\r\n"))
			continue
		}

		if strings.EqualFold(args[0], "exec") {
			s.execSeen.Add(1)
			_, _ = c.Write([]byte("-EXECABORT Transaction discarded because of previous errors.\r\n"))
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
	s := &txQueueErrorServer{ln: ln}
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
