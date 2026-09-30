package redis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Replay and recovery tests for full-duplex pipeline batches. Commands are
// parsed with readRESPCommand from internal_maint_notif_test.go.

// fdDropScript is a scripted RESP2 server for replay tests. Data connections
// are numbered in the order their first GET/SET arrives; step decides, per
// command on a connection, whether to reply normally, reply with LOADING, or
// close the connection. It counts executions per command name.
type fdDropScript struct {
	ln    net.Listener
	conns atomic.Int64
	calls sync.Map // name -> *atomic.Int64
	step  func(conn, cmdIdx int, name string) (reply string, drop bool)
}

func (s *fdDropScript) count(name string) int64 {
	v, _ := s.calls.LoadOrStore(name, new(atomic.Int64))
	return v.(*atomic.Int64).Load()
}

func newFDDropScript(t *testing.T, step func(conn, cmdIdx int, name string) (string, bool)) *fdDropScript {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fdDropScript{ln: ln, step: step}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				rd := bufio.NewReader(c)
				conn, idx := -1, 0
				for {
					args, err := readRESPCommand(rd)
					if err != nil {
						return
					}
					name := strings.ToLower(args[0])
					if name == "hello" {
						_, _ = io.WriteString(c, "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n")
						continue
					}
					if name != "get" && name != "set" {
						_, _ = io.WriteString(c, "+OK\r\n")
						continue
					}
					if conn < 0 {
						conn = int(s.conns.Add(1)) - 1
					}
					v, _ := s.calls.LoadOrStore(name, new(atomic.Int64))
					v.(*atomic.Int64).Add(1)
					reply, drop := s.step(conn, idx, name)
					idx++
					if drop {
						return
					}
					if reply == "" {
						if name == "set" {
							reply = "+OK\r\n"
						} else {
							reply = "$-1\r\n"
						}
					}
					if _, err := io.WriteString(c, reply); err != nil {
						return
					}
				}
			}(c)
		}
	}()
	return s
}

// A pipeline holding a NoRetry command must not be replayed as a unit after a
// connection error, as an ordinary pipeline is not. The FD tail recovery
// replayed the commands before the NoRetry one, so here the SET ran twice.
// Such a batch now stays off the pipe.
func TestFDPipelineNoRetryBatchNotReplayed(t *testing.T) {
	srv := newFDDropScript(t, func(conn, idx int, name string) (string, bool) {
		return "", conn == 0 && idx == 0 // lose the first connection on its first command
	})
	ap := fdPipelineTestAP(t, &Options{Addr: srv.ln.Addr().String(), MaxRetries: 3,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})

	ctx := context.Background()
	var buf strings.Builder
	batch := []Cmder{NewStatusCmd(ctx, "set", "k", "v"), NewRawWriteToCmd(ctx, &buf, "get", "x")}
	if err := ap.FDPipelined(ctx, batch); !errors.Is(err, ErrFDPipelineDiverts) {
		t.Fatalf("FDPipelined with a NoRetry command: err=%v, want ErrFDPipelineDiverts", err)
	}
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	_ = pipe.Process(ctx, NewRawWriteToCmd(ctx, &buf, "get", "x"))
	_, _ = pipe.Exec(ctx)
	if n := srv.count("set"); n != 1 {
		t.Fatalf("SET executed %d times; a pipeline holding a NoRetry command must not be replayed", n)
	}
}

// Replays the FD engine spends on a command count against the pipeline
// budget even when that command then FAILS instead of completing. Here, with
// MaxRetries:1, the SET gets LOADING, the connection drops before the GET's
// reply, the GET is replayed and that connection drops too. That is two
// executions of the GET, the whole budget. The failed GET used to carry no
// attempt count, so the pooled re-run ran it a third time.
func TestFDPipelineRetryCountsFailedReplays(t *testing.T) {
	srv := newFDDropScript(t, func(conn, idx int, name string) (string, bool) {
		switch {
		case conn == 0 && name == "set":
			return "-LOADING Redis is loading the dataset in memory\r\n", false
		case conn <= 1 && name == "get":
			return "", true // drop before replying, on the first and the replay connection
		}
		return "", false
	})
	ap := fdPipelineTestAP(t, &Options{Addr: srv.ln.Addr().String(), MaxRetries: 1,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if n := srv.count("get"); n != 2 {
		t.Fatalf("GET executed %d times with MaxRetries:1, want 2 (err=%v)", n, err)
	}
	if err == nil {
		t.Fatal("Exec succeeded, want the failure once the budget is spent")
	}
}
