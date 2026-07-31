package redis

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
)

type txQueueErrorServer struct {
	ln                  net.Listener
	execSeen            atomic.Int32
	unwatchSeen         atomic.Int32
	queueErr            string
	execReply           string
	himportPrepareReply string
	resp3               bool
	holdAfterExec       bool
	holdAfterQueueErr   bool
	closeAfterQueueErr  bool
	closeOnSetC         bool
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

		if strings.EqualFold(args[0], "set") && len(args) > 1 && args[1] == "c" && s.closeOnSetC {
			return
		}

		if strings.EqualFold(args[0], "exec") {
			s.execSeen.Add(1)
			_, _ = c.Write([]byte(s.execReply))
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
		if outcome != nil && outcome.himported {
			nodeClient.himportAfterBatch(cn, injected, cmds)
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

func TestClusterTxPipelineReturnsRedirectAfterExecArray(t *testing.T) {
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
	if outcome.kind != txRetryMoved {
		t.Fatalf("outcome.kind = %v, want txRetryMoved", outcome.kind)
	}
	if outcome.addr != "127.0.0.1:7001" {
		t.Fatalf("outcome.addr = %q, want %q", outcome.addr, "127.0.0.1:7001")
	}
	if outcome.err == nil || !strings.Contains(outcome.err.Error(), "MOVED 123 127.0.0.1:7001") {
		t.Fatalf("outcome.err = %v, want MOVED root cause", outcome.err)
	}
	if srv.execSeen.Load() != 1 {
		t.Fatalf("EXEC replies seen = %d, want 1", srv.execSeen.Load())
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
