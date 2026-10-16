package redis

// Tests for TxPipeline queued-command error handling. See issue #3800:
// when Redis (or a proxy) rejects a queued tx command, the EXEC reply may
// be a short/error array or never arrive, so the queued error must be
// surfaced instead of an i/o timeout.

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/push"
)

type txQueueErrorServer struct {
	ln                  net.Listener
	execSeen            atomic.Int32
	unwatchSeen         atomic.Int32
	queueErr            string
	execReply           string
	himportPrepareReply string
	preQueueReply       string
	resp3               bool
	holdAfterExec       bool
	holdAfterQueueErr   bool
	closeAfterQueueErr  bool
	closeOnSetC         bool
	closeAfterExec      bool
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
				_, _ = c.Write([]byte("%0\r\n"))
				continue
			}
			_, _ = c.Write([]byte("-ERR unknown command 'hello'\r\n"))
			continue
		}

		if strings.EqualFold(args[0], "multi") {
			_, _ = c.Write([]byte("+OK\r\n"))
			continue
		}

		if strings.EqualFold(args[0], "himport") && len(args) > 1 {
			switch strings.ToLower(args[1]) {
			case "prepare":
				_, _ = c.Write([]byte(s.himportPrepareReply))
				continue
			case "set":
				_, _ = c.Write([]byte("+QUEUED\r\n"))
				continue
			}
		}

		if strings.EqualFold(args[0], "unwatch") {
			s.unwatchSeen.Add(1)
		}

		if strings.EqualFold(args[0], "set") && len(args) > 1 && args[1] == "b" {
			_, _ = c.Write([]byte(s.queueErr))
			if s.closeAfterQueueErr {
				return
			}
			if s.holdAfterQueueErr {
				select {}
			}
			continue
		}

		if strings.EqualFold(args[0], "set") && len(args) > 1 && args[1] == "a" && s.preQueueReply != "" {
			_, _ = c.Write([]byte(s.preQueueReply))
			s.preQueueReply = ""
			continue
		}

		if strings.EqualFold(args[0], "set") && len(args) > 1 && args[1] == "c" && s.closeOnSetC {
			return
		}

		if strings.EqualFold(args[0], "exec") {
			s.execSeen.Add(1)
			_, _ = c.Write([]byte(s.execReply))
			if s.closeAfterExec {
				return
			}
			if s.holdAfterExec {
				select {}
			}
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
		ln:                  ln,
		queueErr:            "-ERR in transaction context, keys must in same slot\r\n",
		execReply:           "-EXECABORT Transaction discarded because of previous errors.\r\n",
		himportPrepareReply: "+OK\r\n",
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

func filteredHImportCmds(cmds []Cmder, indexes map[int]struct{}) []Cmder {
	filtered := make([]Cmder, len(cmds))
	for i, cmd := range cmds {
		if _, ok := cmd.(himportCmder); !ok {
			filtered[i] = cmd
			continue
		}
		if _, ok := indexes[i]; ok {
			filtered[i] = cmd
		}
	}
	return filtered
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

func TestTxPipelineExecConvertsNilExecReplyToTxFailedErr(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "_\r\n"
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
			t.Fatal("Exec() error = nil, want TxFailedErr")
		}
		if !errors.Is(err, TxFailedErr) {
			t.Fatalf("Exec() error = %v, want errors.Is(err, TxFailedErr)", err)
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

func TestTxPipelineExecPreservesQueuedErrorOnMissingExecReply(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = ""
	srv.holdAfterExec = true
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

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want timeout with queued Redis error context")
	}
	if !strings.Contains(err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error context", err)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("Exec() error = %v, want wrapped timeout error", err)
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestTxPipelineExecPreservesQueuedErrorOnQueuedReplyReadFailure(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.closeOnSetC = true
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
	pipe.Set(ctx, "c", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want timeout with queued Redis error context")
	}
	if !strings.Contains(err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error context", err)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) && !errors.Is(err, io.EOF) {
		t.Fatalf("Exec() error = %v, want wrapped network read error or EOF", err)
	}
	if srv.execSeen.Load() != 0 {
		t.Fatalf("EXEC replies seen = %d, want 0", srv.execSeen.Load())
	}
}

func TestTxPipelineExecQueuedReplyReadFailureStampsWrappedError(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.closeOnSetC = true
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
	cmdC := pipe.Set(ctx, "c", 1, 0)

	cmds, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want wrapped queued+read error")
	}
	// The command whose +QUEUED read hit EOF must carry the wrapped error,
	// not the raw read error, so callers see the queued root cause.
	if cmdC.Err() == nil {
		t.Fatal("cmdC err = nil, want wrapped error")
	}
	if !strings.Contains(cmdC.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("cmdC err = %v, want queued Redis error in wrapped message", cmdC.Err())
	}
	if len(cmds) < 3 {
		t.Fatalf("Exec() returned %d cmds, want >= 3", len(cmds))
	}
	for i, cmd := range cmds {
		if cmd.Err() == nil {
			t.Fatalf("cmd[%d] err = nil, want wrapped queued+read error", i)
		}
	}
}

func TestClusterTxPipelinePreservesQueuedErrorOnMissingExecReply(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = ""
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, 100*time.Millisecond, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if outcome.err == nil {
		t.Fatal("outcome.err = nil, want wrapped timeout with queued Redis error context")
	}
	if !strings.Contains(outcome.err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("outcome.err = %q, want queued Redis error context", outcome.err)
	}
	var netErr net.Error
	if !errors.As(outcome.err, &netErr) || !netErr.Timeout() {
		t.Fatalf("outcome.err = %v, want wrapped timeout error", outcome.err)
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
	if cmds[1].Err() == nil || !strings.Contains(cmds[1].Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("cmd[1] err = %v, want queued Redis error", cmds[1].Err())
	}
	if !outcome.unreadReplies {
		t.Fatal("outcome.unreadReplies = false, want true for missing EXEC reply")
	}
}

func TestClusterTxPipelineQueuedReplyReadFailurePreservesQueuedError(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.closeOnSetC = true
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
		NewStatusCmd(ctx, "set", "c", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, 100*time.Millisecond, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	// The queued root cause must be visible through the wrapper, not just the
	// bare transport error.
	if outcome.err == nil {
		t.Fatal("outcome.err = nil, want wrapped queued+read error")
	}
	if !strings.Contains(outcome.err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("outcome.err = %q, want queued Redis error preserved", outcome.err.Error())
	}
	if !outcome.unreadReplies {
		t.Fatal("outcome.unreadReplies = false, want true for desynced connection")
	}
}

func TestTxPipelineExecErrorIncludesExecAbortContext(t *testing.T) {
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
	pipe := client.TxPipeline()
	pipe.Set(ctx, "a", 1, 0)
	pipe.Set(ctx, "b", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error text", got)
	}
	if got := err.Error(); !strings.Contains(got, "EXECABORT") {
		t.Fatalf("Exec() error = %q, want EXECABORT context", got)
	}
}

func TestClusterTxPipelineReturnsQueuedErrorAfterExecArray(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*2\r\n+OK\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if outcome.err == nil {
		t.Fatal("outcome.err = nil, want queued Redis error")
	}
	if got := outcome.err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("outcome.err = %q, want queued Redis error", got)
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
	if cmds[1].Err() == nil || !strings.Contains(cmds[1].Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("cmd[1] err = %v, want queued Redis error", cmds[1].Err())
	}
}

func TestTxPipelineExecQueuedErrorPreservesHImportAfterBatch(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "-ERR duplicate field name in fieldset\r\n"
	srv.execReply = "*1\r\n-ERR no such fieldset\r\n"
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	client.himport.register("fs", []string{"f1"})

	ctx := context.Background()
	pipe := client.TxPipeline()
	himportSet := pipe.HImportSet(ctx, "k", "fs", "v1")
	pipe.Set(ctx, "b", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error", got)
	}
	if himportSet.Err() == nil || !strings.Contains(himportSet.Err().Error(), "ERR duplicate field name in fieldset") {
		t.Fatalf("HImportSet err = %v, want injected prepare root cause", himportSet.Err())
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestTxPipelineExecAbortSkipsHImportAfterBatchSideEffects(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "+OK\r\n"
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	client.himport.register("fs", []string{"f1"})

	ctx := context.Background()
	pipe := client.TxPipeline()
	himportDiscard := pipe.HImportDiscard(ctx, "fs")
	pipe.Set(ctx, "b", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error", got)
	}
	if himportDiscard.Err() == nil || !strings.Contains(himportDiscard.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("HImportDiscard err = %v, want queued Redis error", himportDiscard.Err())
	}
	if _, ok := client.himport.lookup("fs"); !ok {
		t.Fatal("fieldset fs unexpectedly removed from registry")
	}
}

func TestTxPipelineExecMidDrainFailurePreservesHImportAfterBatch(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "+OK\r\n"
	srv.execReply = "*2\r\n+OK\r\n"
	srv.closeAfterExec = true
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	client.himport.register("fs", []string{"f1"})

	ctx := context.Background()
	pipe := client.TxPipeline()
	himportSet := pipe.HImportSet(ctx, "k", "fs", "v1")
	pipe.Set(ctx, "b", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want mid-drain read error")
	}
	// The queued root cause must be visible through the wrapper.
	if !strings.Contains(err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error preserved", err.Error())
	}
	// HIMPORT SET should have its real reply (the first EXEC array element),
	// not be overwritten with the drain error.
	if himportSet.Err() != nil {
		t.Fatalf("HImportSet err = %v, want nil (successful drained reply)", himportSet.Err())
	}
}

func TestClusterTxPipelineQueuedErrorPreservesHImportAfterBatch(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "-ERR duplicate field name in fieldset\r\n"
	srv.execReply = "*1\r\n-ERR no such fieldset\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	nodeClient.himport.register("fs", []string{"f1"})

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewHImportSetCmd(ctx, "k", "fs", "v1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	injected := nodeClient.himportInjectedCmds(ctx, cn, cmds)
	if len(injected) != 1 {
		t.Fatalf("injected cmds = %d, want 1", len(injected))
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}
	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		if err := nodeClient.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			return err
		}
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		if outcome != nil && len(outcome.himportedIndexes) > 0 {
			nodeClient.himportAfterBatch(cn, injected, filteredHImportCmds(cmds, outcome.himportedIndexes))
		}
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("outcome.err = %v, want queued Redis error", outcome.err)
	}
	himportSet := cmds[0].(*HImportSetCmd)
	if himportSet.Err() == nil || !strings.Contains(himportSet.Err().Error(), "ERR duplicate field name in fieldset") {
		t.Fatalf("HImportSet err = %v, want injected prepare root cause", himportSet.Err())
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestClusterTxPipelineMidDrainFailurePreservesHImportIndexes(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "+OK\r\n"
	srv.execReply = "*2\r\n+OK\r\n"
	srv.closeAfterExec = true
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	nodeClient.himport.register("fs", []string{"f1"})

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewHImportSetCmd(ctx, "k", "fs", "v1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	injected := nodeClient.himportInjectedCmds(ctx, cn, cmds)
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}
	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		if err := nodeClient.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			return err
		}
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		if outcome != nil && len(outcome.himportedIndexes) > 0 {
			nodeClient.himportAfterBatch(cn, injected, filteredHImportCmds(cmds, outcome.himportedIndexes))
		}
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	// Mid-drain failure must carry himportedIndexes so the caller can apply
	// side effects for the HIMPORT reply that was already decoded.
	if len(outcome.himportedIndexes) == 0 {
		t.Fatal("outcome.himportedIndexes = empty, want the drained HIMPORT index")
	}
	if _, ok := outcome.himportedIndexes[0]; !ok {
		t.Fatalf("outcome.himportedIndexes = %v, want index 0", outcome.himportedIndexes)
	}
	// HIMPORT SET should have its real reply (the first EXEC array element),
	// not be overwritten with the drain error.
	if cmds[0].Err() != nil {
		t.Fatalf("HImportSet err = %v, want nil (successful drained reply)", cmds[0].Err())
	}
}

func TestClusterTxPipelineRedirectMidDrainFailurePreservesHImportIndexes(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "+OK\r\n"
	srv.queueErr = "-MOVED 123 127.0.0.1:7001\r\n"
	srv.execReply = "*2\r\n+OK\r\n"
	srv.closeAfterExec = true
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	nodeClient.himport.register("fs", []string{"f1"})

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewHImportSetCmd(ctx, "k", "fs", "v1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	injected := nodeClient.himportInjectedCmds(ctx, cn, cmds)
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}
	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		if err := nodeClient.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			return err
		}
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		if outcome != nil && len(outcome.himportedIndexes) > 0 {
			nodeClient.himportAfterBatch(cn, injected, filteredHImportCmds(cmds, outcome.himportedIndexes))
		}
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if len(outcome.himportedIndexes) == 0 {
		t.Fatal("outcome.himportedIndexes = empty, want the drained HIMPORT index")
	}
	if _, ok := outcome.himportedIndexes[0]; !ok {
		t.Fatalf("outcome.himportedIndexes = %v, want index 0", outcome.himportedIndexes)
	}
	if cmds[0].Err() != nil {
		t.Fatalf("HImportSet err = %v, want nil (successful drained reply)", cmds[0].Err())
	}
}

func TestClusterTxPipelineRedirectMapsExecRepliesToQueuedIndexes(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "+OK\r\n"
	srv.queueErr = "-MOVED 123 127.0.0.1:7001\r\n"
	srv.execReply = "*2\r\n+OK\r\n:1\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	nodeClient.himport.register("fs", []string{"f1"})

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
		NewHImportDiscardCmd(ctx, "fs"),
	}
	injected := nodeClient.himportInjectedCmds(ctx, cn, cmds)
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		if err := nodeClient.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			return err
		}
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		if outcome != nil && len(outcome.himportedIndexes) > 0 {
			nodeClient.himportAfterBatch(cn, injected, filteredHImportCmds(cmds, outcome.himportedIndexes))
		}
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "MOVED 123 127.0.0.1:7001") {
		t.Fatalf("outcome.err = %v, want MOVED root cause", outcome.err)
	}
	if _, ok := outcome.execCmdIndexes[2]; !ok {
		t.Fatalf("outcome.execCmdIndexes = %v, want executed HIMPORT index 2", outcome.execCmdIndexes)
	}
	himportDiscard := cmds[2].(*HImportDiscardCmd)
	if himportDiscard.Err() != nil {
		t.Fatalf("HImportDiscard err = %v, want nil (successful drained reply)", himportDiscard.Err())
	}
	if _, ok := nodeClient.himport.lookup("fs"); ok {
		t.Fatal("fieldset fs unexpectedly kept in registry after successful HIMPORT DISCARD")
	}
}

func TestClusterTxPipelineReturnsFatalAfterRedirectExecArray(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.queueErr = "-MOVED 123 127.0.0.1:7001\r\n"
	srv.execReply = "*1\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "MOVED 123 127.0.0.1:7001") {
		t.Fatalf("outcome.err = %v, want MOVED root cause", outcome.err)
	}
	if outcome.readCount != 1 {
		t.Fatalf("outcome.readCount = %d, want 1", outcome.readCount)
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestClusterTxPipelineRedirectPreservedOnMalformedExecArrayLen(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.queueErr = "-MOVED 123 127.0.0.1:7001\r\n"
	srv.execReply = "*-2\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	// A malformed EXEC array length after a redirect means EXEC was accepted
	// and may have executed queued commands, so retrying could double-apply
	// non-idempotent commands. The outcome must be fatal, not a retry.
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	// The wrapped error must preserve both the MOVED root cause and the
	// protocol error.
	if outcome.err == nil {
		t.Fatal("outcome.err = nil, want wrapped error")
	}
	if !strings.Contains(outcome.err.Error(), "MOVED 123 127.0.0.1:7001") {
		t.Fatalf("outcome.err = %q, want MOVED root cause preserved", outcome.err.Error())
	}
	if !strings.Contains(outcome.err.Error(), "invalid EXEC array length") {
		t.Fatalf("outcome.err = %q, want protocol error preserved", outcome.err.Error())
	}
	// Commands must be stamped with the fatal error.
	for i, cmd := range cmds {
		if cmd.Err() == nil {
			t.Fatalf("cmd[%d] err = nil, want fatal stamped error", i)
		}
	}
}

func TestClusterTxPipelineSuccessPreservesHImportAfterBatch(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*1\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewHImportPrepareCmd(ctx, "fs", "f1"),
	}
	injected := nodeClient.himportInjectedCmds(ctx, cn, cmds)
	if len(injected) != 0 {
		t.Fatalf("injected cmds = %d, want 0", len(injected))
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		if outcome != nil && len(outcome.himportedIndexes) > 0 {
			nodeClient.himportAfterBatch(cn, injected, filteredHImportCmds(cmds, outcome.himportedIndexes))
		}
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txSuccess {
		t.Fatalf("outcome.kind = %v, want txSuccess", outcome.kind)
	}
	if _, ok := nodeClient.himport.lookup("fs"); !ok {
		t.Fatal("fieldset fs missing from registry after successful HIMPORT PREPARE")
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestTxPipelineExecQueuedErrorDiscardedCmdGetsQueuedErr(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*2\r\n+PONG\r\n+OK\r\n"
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
	ping := pipe.Ping(ctx)
	set := pipe.Set(ctx, "b", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error", got)
	}
	if ping.Err() == nil || !strings.Contains(ping.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Ping err = %v, want queued Redis error", ping.Err())
	}
	if set.Err() == nil || !strings.Contains(set.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Set err = %v, want queued Redis error", set.Err())
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestTxPipelineExecQueuedErrorDiscardSkipsRESP3Attrs(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.resp3 = true
	srv.execReply = "*2\r\n|1\r\n+meta\r\n+data\r\n+PONG\r\n+OK\r\n"
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
	pipe := client.TxPipeline()
	ping := pipe.Ping(ctx)
	set := pipe.Set(ctx, "b", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error", got)
	}
	if ping.Err() == nil || !strings.Contains(ping.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Ping err = %v, want queued Redis error", ping.Err())
	}
	if set.Err() == nil || !strings.Contains(set.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Set err = %v, want queued Redis error", set.Err())
	}
}

func TestTxPipelineExecQueuedErrorRejectsNegativeExecArrayLen(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*-2\r\n"
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
	pipe.Ping(ctx)
	pipe.Set(ctx, "b", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want malformed EXEC array error")
	}
	if !strings.Contains(err.Error(), "invalid EXEC array length") {
		t.Fatalf("Exec() error = %q, want invalid EXEC array length", err.Error())
	}
}

func TestTxPipelineExecQueuedErrorNonArrayReplyPreservesQueuedError(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "+BOGUS\r\n"
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
		t.Fatal("Exec() error = nil, want wrapped queued+proto error")
	}
	if !strings.Contains(err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error preserved", err.Error())
	}
	if !strings.Contains(err.Error(), "expected '*'") {
		t.Fatalf("Exec() error = %q, want protocol error preserved", err.Error())
	}
}

func TestClusterTxPipelineQueuedErrorRejectsNegativeExecArrayLen(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*-2\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if outcome.err == nil {
		t.Fatal("outcome.err = nil, want malformed EXEC array error")
	}
	if !strings.Contains(outcome.err.Error(), "invalid EXEC array length") {
		t.Fatalf("outcome.err = %q, want invalid EXEC array length", outcome.err.Error())
	}
}

func TestClusterTxPipelineQueuedErrorNonArrayReplyPreservesQueuedError(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "+BOGUS\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if outcome.err == nil {
		t.Fatal("outcome.err = nil, want wrapped queued+proto error")
	}
	if !strings.Contains(outcome.err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("outcome.err = %q, want queued Redis error preserved", outcome.err.Error())
	}
	if !strings.Contains(outcome.err.Error(), "unexpected EXEC reply") {
		t.Fatalf("outcome.err = %q, want protocol error preserved", outcome.err.Error())
	}
	if !outcome.unreadReplies {
		t.Fatal("outcome.unreadReplies = false, want true for desynced non-array reply")
	}
}

func TestTxPipelineExecQueuedErrorShortArraySkipsUnreadHImportSideEffects(t *testing.T) {
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

	client.himport.register("fs", []string{"f1"})

	ctx := context.Background()
	pipe := client.TxPipeline()
	pipe.Set(ctx, "b", 1, 0)
	himportDiscard := pipe.HImportDiscard(ctx, "fs")

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error", got)
	}
	if _, ok := client.himport.lookup("fs"); !ok {
		t.Fatal("fieldset fs unexpectedly removed from registry")
	}
	if himportDiscard.Err() == nil || !strings.Contains(himportDiscard.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("HImportDiscard err = %v, want queued Redis error", himportDiscard.Err())
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestTxPipelineExecQueuedErrorPartialHImportReadSkipsUnreadSideEffects(t *testing.T) {
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

	client.himport.register("fs", []string{"f1"})

	ctx := context.Background()
	pipe := client.TxPipeline()
	prepare := pipe.HImportPrepare(ctx, "fs", "f1")
	pipe.Set(ctx, "b", 1, 0)
	discard := pipe.HImportDiscard(ctx, "fs")

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error", got)
	}
	if prepare.Err() != nil {
		t.Fatalf("HImportPrepare err = %v, want nil", prepare.Err())
	}
	if _, ok := client.himport.lookup("fs"); !ok {
		t.Fatal("fieldset fs unexpectedly removed from registry")
	}
	if discard.Err() == nil || !strings.Contains(discard.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("HImportDiscard err = %v, want queued Redis error", discard.Err())
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestTxPipelineExecQueuedErrorMixedBatchDiscardsNonHImportReplies(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "-ERR duplicate field name in fieldset\r\n"
	srv.execReply = "*2\r\n+PONG\r\n-ERR no such fieldset\r\n"
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	client.himport.register("fs", []string{"f1"})

	ctx := context.Background()
	pipe := client.TxPipeline()
	incr := pipe.Incr(ctx, "a")
	himportSet := pipe.HImportSet(ctx, "k", "fs", "v1")
	pipe.Set(ctx, "b", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error", got)
	}
	if incr.Err() == nil || !strings.Contains(incr.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Incr err = %v, want queued Redis error", incr.Err())
	}
	if himportSet.Err() == nil || !strings.Contains(himportSet.Err().Error(), "ERR duplicate field name in fieldset") {
		t.Fatalf("HImportSet err = %v, want injected prepare root cause", himportSet.Err())
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestTxPipelineExecQueuedErrorMapsExecRepliesToOriginalIndexes(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "+OK\r\n"
	srv.execReply = "*2\r\n+OK\r\n:1\r\n"
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	client.himport.register("fs", []string{"f1"})

	ctx := context.Background()
	pipe := client.TxPipeline()
	pipe.Set(ctx, "a", 1, 0)
	pipe.Set(ctx, "b", 1, 0)
	himportDiscard := pipe.HImportDiscard(ctx, "fs")

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error", got)
	}
	if himportDiscard.Err() != nil {
		t.Fatalf("HImportDiscard err = %v, want nil (successful drained reply)", himportDiscard.Err())
	}
	if _, ok := client.himport.lookup("fs"); ok {
		t.Fatal("fieldset fs unexpectedly kept in registry after successful HIMPORT DISCARD")
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestTxPipelineQueuedReadMapsExecRepliesDespiteStaleRetryErrors(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{})
	client.himport.register("fs", []string{"f1"})

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
		NewHImportDiscardCmd(ctx, "fs"),
	}
	for _, cmd := range cmds {
		cmd.SetErr(io.EOF)
	}

	replies := strings.NewReader(strings.Join([]string{
		"+OK\r\n",
		"+QUEUED\r\n",
		"-ERR in transaction context, keys must in same slot\r\n",
		"+QUEUED\r\n",
		"*2\r\n",
		"+OK\r\n",
		":1\r\n",
	}, ""))

	rd := proto.NewReader(bufio.NewReader(replies))
	statusCmd := NewStatusCmd(ctx)

	err := client.txPipelineReadQueued(ctx, nil, rd, statusCmd, cmds)
	if err == nil {
		t.Fatal("txPipelineReadQueued() error = nil, want queued Redis error")
	}

	var execArrayErr *txQueuedExecArrayError
	if !errors.As(err, &execArrayErr) {
		t.Fatalf("txPipelineReadQueued() error = %T, want *txQueuedExecArrayError", err)
	}
	if _, ok := execArrayErr.execCmdIndexes[2]; !ok {
		t.Fatalf("execCmdIndexes = %v, want executed HIMPORT index 2", execArrayErr.execCmdIndexes)
	}
	if _, ok := execArrayErr.himportedIndexes[2]; !ok {
		t.Fatalf("himportedIndexes = %v, want drained HIMPORT index 2", execArrayErr.himportedIndexes)
	}

	himportDiscard := cmds[2].(*HImportDiscardCmd)
	if himportDiscard.Err() != nil {
		t.Fatalf("HImportDiscard err = %v, want nil (successful drained reply)", himportDiscard.Err())
	}
}

func TestClusterTxPipelineQueuedErrorShortArraySkipsUnreadHImportSideEffects(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*1\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	nodeClient.himport.register("fs", []string{"f1"})

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "b", "1"),
		NewHImportDiscardCmd(ctx, "fs"),
	}
	injected := nodeClient.himportInjectedCmds(ctx, cn, cmds)
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		if err := nodeClient.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			return err
		}
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		if outcome != nil && len(outcome.himportedIndexes) > 0 {
			nodeClient.himportAfterBatch(cn, injected, filteredHImportCmds(cmds, outcome.himportedIndexes))
		}
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if _, ok := nodeClient.himport.lookup("fs"); !ok {
		t.Fatal("fieldset fs unexpectedly removed from registry")
	}
	himportDiscard := cmds[1].(*HImportDiscardCmd)
	if himportDiscard.Err() == nil || !strings.Contains(himportDiscard.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("HImportDiscard err = %v, want queued Redis error", himportDiscard.Err())
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestClusterTxPipelineQueuedErrorPartialHImportReadPreservesReadSideEffects(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*1\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	nodeClient.himport.register("fs", []string{"f1"})

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewHImportPrepareCmd(ctx, "fs", "f1"),
		NewStatusCmd(ctx, "set", "b", "1"),
		NewHImportDiscardCmd(ctx, "fs"),
	}
	injected := nodeClient.himportInjectedCmds(ctx, cn, cmds)
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		if err := nodeClient.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			return err
		}
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		if outcome != nil && len(outcome.himportedIndexes) > 0 {
			nodeClient.himportAfterBatch(cn, injected, filteredHImportCmds(cmds, outcome.himportedIndexes))
		}
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if outcome.readCount != 1 {
		t.Fatalf("outcome.readCount = %d, want 1", outcome.readCount)
	}
	prepare := cmds[0].(*HImportPrepareCmd)
	if prepare.Err() != nil {
		t.Fatalf("HImportPrepare err = %v, want nil", prepare.Err())
	}
	if _, ok := nodeClient.himport.lookup("fs"); !ok {
		t.Fatal("fieldset fs unexpectedly missing from registry")
	}
	discard := cmds[2].(*HImportDiscardCmd)
	if discard.Err() == nil || !strings.Contains(discard.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("HImportDiscard err = %v, want queued Redis error", discard.Err())
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestClusterTxPipelineQueuedErrorMixedBatchDiscardsNonHImportReplies(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "-ERR duplicate field name in fieldset\r\n"
	srv.execReply = "*2\r\n+PONG\r\n-ERR no such fieldset\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	nodeClient.himport.register("fs", []string{"f1"})

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewIntCmd(ctx, "incr", "a"),
		NewHImportSetCmd(ctx, "k", "fs", "v1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	injected := nodeClient.himportInjectedCmds(ctx, cn, cmds)
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		if err := nodeClient.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			return err
		}
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		if outcome != nil && len(outcome.himportedIndexes) > 0 {
			nodeClient.himportAfterBatch(cn, injected, filteredHImportCmds(cmds, outcome.himportedIndexes))
		}
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("outcome = nil, want txFatal")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if got := outcome.err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("outcome.err = %q, want queued Redis error", got)
	}
	incr := cmds[0].(*IntCmd)
	if incr.Err() == nil || !strings.Contains(incr.Err().Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Incr err = %v, want queued Redis error", incr.Err())
	}
	himportSet := cmds[1].(*HImportSetCmd)
	if himportSet.Err() == nil || !strings.Contains(himportSet.Err().Error(), "ERR duplicate field name in fieldset") {
		t.Fatalf("HImportSet err = %v, want injected prepare root cause", himportSet.Err())
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestClusterTxPipelineQueuedErrorMapsExecRepliesToOriginalIndexes(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "+OK\r\n"
	srv.execReply = "*2\r\n+OK\r\n:1\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	nodeClient.himport.register("fs", []string{"f1"})

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
		NewHImportDiscardCmd(ctx, "fs"),
	}
	injected := nodeClient.himportInjectedCmds(ctx, cn, cmds)
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		for _, ic := range injected {
			if err := writeCmd(wr, ic); err != nil {
				return err
			}
		}
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		if err := nodeClient.himportReadInjectedReplies(ctx, cn, rd, injected); err != nil {
			return err
		}
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		if outcome != nil && len(outcome.himportedIndexes) > 0 {
			nodeClient.himportAfterBatch(cn, injected, filteredHImportCmds(cmds, outcome.himportedIndexes))
		}
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("outcome.err = %v, want queued Redis error", outcome.err)
	}
	himportDiscard := cmds[2].(*HImportDiscardCmd)
	if himportDiscard.Err() != nil {
		t.Fatalf("HImportDiscard err = %v, want nil (successful drained reply)", himportDiscard.Err())
	}
	if _, ok := nodeClient.himport.lookup("fs"); ok {
		t.Fatal("fieldset fs unexpectedly kept in registry after successful HIMPORT DISCARD")
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestClusterTxPipelineRedirectDrainFailureReturnsFatalAfterExecutedReply(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.queueErr = "-MOVED 123 127.0.0.1:7001\r\n"
	srv.execReply = "*2\r\n+OK\r\n"
	srv.holdAfterExec = true
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, 100*time.Millisecond, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	var netErr net.Error
	if !errors.As(outcome.err, &netErr) || !netErr.Timeout() {
		t.Fatalf("outcome.err = %v, want wrapped timeout preserving redirect root cause", outcome.err)
	}
	if !strings.Contains(outcome.err.Error(), "MOVED 123 127.0.0.1:7001") {
		t.Fatalf("outcome.err = %v, want MOVED root cause", outcome.err)
	}
}

func TestClusterTxPipelineRedirectReadFailurePreservesRetry(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.queueErr = "-MOVED 123 127.0.0.1:7001\r\n"
	srv.execReply = ""
	srv.holdAfterExec = true
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, 100*time.Millisecond, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txRetryMoved {
		t.Fatalf("outcome.kind = %v, want txRetryMoved", outcome.kind)
	}
	if outcome.addr != "127.0.0.1:7001" {
		t.Fatalf("outcome.addr = %q, want %q", outcome.addr, "127.0.0.1:7001")
	}
	var netErr net.Error
	if !errors.As(outcome.err, &netErr) || !netErr.Timeout() {
		t.Fatalf("outcome.err = %v, want wrapped timeout preserving redirect outcome", outcome.err)
	}
	if !strings.Contains(outcome.err.Error(), "MOVED 123 127.0.0.1:7001") {
		t.Fatalf("outcome.err = %q, want MOVED root cause context", outcome.err)
	}
}

func TestClusterTxPipelineQueuedErrorDiscardSkipsRESP3Attrs(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.resp3 = true
	srv.queueErr = "-MOVED 123 127.0.0.1:7001\r\n"
	srv.execReply = "*2\r\n|1\r\n+meta\r\n+data\r\n+PONG\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		Protocol:     3,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if got := outcome.err.Error(); !strings.Contains(got, "MOVED 123 127.0.0.1:7001") {
		t.Fatalf("outcome.err = %q, want MOVED root cause", got)
	}
}

func TestClusterTxPipelineQueuedPushProcessorErrorIsFatal(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.resp3 = true
	srv.preQueueReply = fakeRESP3PushNotification("testpush", "payload")
	srv.execReply = "*2\r\n+OK\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		Protocol:     3,
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
		PushNotificationProcessor: erroringProcessor{
			Processor: push.NewProcessor(),
			err:       proto.NewOOMError("OOM custom push processor failure"),
		},
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "c", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, time.Second, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if !outcome.unreadReplies {
		t.Fatal("outcome.unreadReplies = false, want true")
	}
	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "OOM custom push processor failure") {
		t.Fatalf("outcome.err = %v, want custom push processor failure", outcome.err)
	}
	var pushErr *txPushReadError
	if !errors.As(outcome.err, &pushErr) {
		t.Fatalf("outcome.err = %T, want txPushReadError", outcome.err)
	}
	for i, cmd := range cmds {
		if cmd.Err() != nil {
			t.Fatalf("cmd[%d] err = %v, want nil because queue reply was never read", i, cmd.Err())
		}
	}
	if srv.execSeen.Load() != 0 {
		t.Fatalf("EXEC replies seen = %d, want 0 because reader failed before EXEC", srv.execSeen.Load())
	}
}

func TestTxPipelineExecQueuedErrorHImportArrayLenMismatchDoesNotPanic(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.himportPrepareReply = "-ERR duplicate field name in fieldset\r\n"
	srv.execReply = "*3\r\n-ERR no such fieldset\r\n+OK\r\n+OK\r\n"
	defer func() { _ = srv.Close() }()

	client := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  time.Second,
		WriteTimeout: time.Second,
	})
	defer func() { _ = client.Close() }()

	client.himport.register("fs", []string{"f1"})

	ctx := context.Background()
	pipe := client.TxPipeline()
	himportSet := pipe.HImportSet(ctx, "k", "fs", "v1")
	pipe.Set(ctx, "b", 1, 0)

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error", got)
	}
	if himportSet.Err() == nil || !strings.Contains(himportSet.Err().Error(), "ERR duplicate field name in fieldset") {
		t.Fatalf("HImportSet err = %v, want injected prepare root cause", himportSet.Err())
	}
}

func TestTxQueuedReadErrorPreservesHelpersAndBadConn(t *testing.T) {
	err := &txQueuedReadError{
		queuedErr: proto.NewOOMError("OOM command not allowed when used memory > 'maxmemory'"),
		readErr:   &net.OpError{Op: "read", Err: timeoutErrorStub{}},
	}

	if !IsOOMError(err) {
		t.Fatalf("IsOOMError(%v) = false, want true", err)
	}
	if !isBadConn(err, false, "127.0.0.1:6379") {
		t.Fatalf("isBadConn(%v) = false, want true", err)
	}

	forceBad := &txQueuedReadError{
		queuedErr: proto.NewMovedError("MOVED 1 127.0.0.1:7001", "127.0.0.1:7001"),
		readErr:   proto.NewOOMError("OOM custom push processor failure"),
		forceBad:  true,
	}
	if !isBadConn(forceBad, false, "127.0.0.1:6379") {
		t.Fatalf("isBadConn(%v) = false, want true for forced bad conn", forceBad)
	}
}

func TestTxPipelineExecSuccessfulArrayClearsWatchOnClose(t *testing.T) {
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
		_, err := tx.TxPipelined(ctx, func(pipe Pipeliner) error {
			pipe.Set(ctx, "a", 1, 0)
			pipe.Set(ctx, "b", 1, 0)
			return nil
		})
		if err == nil {
			t.Fatal("Exec() error = nil, want queued Redis error")
		}
		if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
			t.Fatalf("Exec() error = %q, want queued Redis error", got)
		}
		return err
	}, "key")
	if err == nil {
		t.Fatal("Watch() error = nil, want queued Redis error")
	}
	if got := err.Error(); !strings.Contains(got, "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Watch() error = %q, want queued Redis error", got)
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
	if srv.unwatchSeen.Load() != 0 {
		t.Fatalf("UNWATCH replies seen = %d, want 0", srv.unwatchSeen.Load())
	}
}

func TestTxPipelineExecPreservesQueuedErrorOnPartialExecArrayDrain(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*2\r\n+OK\r\n"
	srv.holdAfterExec = true
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

	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatal("Exec() error = nil, want timeout with queued Redis error context")
	}
	if !strings.Contains(err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("Exec() error = %q, want queued Redis error context", err)
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("Exec() error = %v, want wrapped timeout error", err)
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

func TestClusterTxPipelinePreservesQueuedErrorOnPartialExecArrayDrain(t *testing.T) {
	srv := startTxQueueErrorServer(t)
	srv.execReply = "*2\r\n+OK\r\n"
	srv.holdAfterExec = true
	defer func() { _ = srv.Close() }()

	ctx := context.Background()
	nodeClient := NewClient(&Options{
		Addr:         srv.Addr(),
		DialTimeout:  time.Second,
		ReadTimeout:  100 * time.Millisecond,
		WriteTimeout: time.Second,
	})
	defer func() { _ = nodeClient.Close() }()

	clusterClient := &ClusterClient{}
	node := &clusterNode{Client: nodeClient}
	node.generation.Store(1)
	cn, err := nodeClient.getConn(ctx)
	if err != nil {
		t.Fatalf("getConn() error = %v", err)
	}
	defer nodeClient.releaseConn(ctx, cn, errTxDirtyConn)

	cmds := []Cmder{
		NewStatusCmd(ctx, "set", "a", "1"),
		NewStatusCmd(ctx, "set", "b", "1"),
	}
	if err := cn.WithWriter(ctx, time.Second, func(wr *proto.Writer) error {
		return writeCmds(wr, wrapMultiExec(ctx, cmds))
	}); err != nil {
		t.Fatalf("writeCmds() error = %v", err)
	}

	var outcome *txOutcome
	if err := cn.WithReader(ctx, 100*time.Millisecond, func(rd *proto.Reader) error {
		outcome = clusterClient.readTxPipelineReplies(ctx, node, cn, rd, cmds, false)
		return nil
	}); err != nil {
		t.Fatalf("WithReader() error = %v", err)
	}
	if outcome == nil {
		t.Fatal("readTxPipelineReplies() outcome = nil")
	}
	if outcome.kind != txFatal {
		t.Fatalf("outcome.kind = %v, want txFatal", outcome.kind)
	}
	if outcome.err == nil {
		t.Fatal("outcome.err = nil, want wrapped timeout with queued Redis error context")
	}
	if !strings.Contains(outcome.err.Error(), "ERR in transaction context, keys must in same slot") {
		t.Fatalf("outcome.err = %q, want queued Redis error context", outcome.err)
	}
	var netErr net.Error
	if !errors.As(outcome.err, &netErr) || !netErr.Timeout() {
		t.Fatalf("outcome.err = %v, want wrapped timeout error", outcome.err)
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
	}
}

type timeoutErrorStub struct{}

func (timeoutErrorStub) Error() string   { return "i/o timeout" }
func (timeoutErrorStub) Timeout() bool   { return true }
func (timeoutErrorStub) Temporary() bool { return false }
