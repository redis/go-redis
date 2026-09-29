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

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
)

// fdStateServer is a minimal stateful RESP2 server for the FD pipeline tests.
// SET stores, GET reads ($-1 when missing), HELLO gets a minimal map, anything
// else +OK. It can answer the first N SETs with -LOADING (not applied), counts
// SETs, and can delay every GET/SET reply. Commands are parsed with readRESPCommand from
// internal_maint_notif_test.go.
type fdStateServer struct {
	ln       net.Listener
	mu       sync.Mutex
	kv       map[string]string
	loading  atomic.Int64 // SETs still to answer with -LOADING (not applied)
	setCalls atomic.Int64 // SETs received, LOADING and dropped ones included
	// dropFirst closes the connection on the first SET instead of replying,
	// so the client sees the write land and the reply never arrive.
	dropFirst atomic.Bool
	delay     time.Duration
	wg        sync.WaitGroup
	conns     []net.Conn
}

func newFDStateServer(t *testing.T) *fdStateServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fdStateServer{ln: ln, kv: map[string]string{}}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, c)
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(c)
			}()
		}
	}()
	t.Cleanup(s.close)
	return s
}

func (s *fdStateServer) addr() string { return s.ln.Addr().String() }

func (s *fdStateServer) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *fdStateServer) serve(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	for {
		args, err := readRESPCommand(rd)
		if err != nil {
			return
		}
		name := ""
		if len(args) > 0 {
			name = strings.ToLower(args[0])
		}
		var reply string
		switch {
		case name == "hello":
			reply = "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n"
		case name == "set" && len(args) >= 3:
			s.setCalls.Add(1)
			if s.dropFirst.CompareAndSwap(true, false) {
				return // closes c: the reply never arrives
			}
			if s.loading.Add(-1) >= 0 {
				reply = "-LOADING Redis is loading the dataset in memory\r\n"
				break
			}
			s.mu.Lock()
			s.kv[args[1]] = args[2]
			s.mu.Unlock()
			reply = "+OK\r\n"
		case name == "get" && len(args) == 2:
			s.mu.Lock()
			v, ok := s.kv[args[1]]
			s.mu.Unlock()
			if ok {
				reply = "$" + itoa(len(v)) + "\r\n" + v + "\r\n"
			} else {
				reply = "$-1\r\n"
			}
		default:
			reply = "+OK\r\n"
		}
		if (name == "get" || name == "set") && s.delay > 0 {
			time.Sleep(s.delay)
		}
		if _, err := io.WriteString(c, reply); err != nil {
			return
		}
	}
}

func fdPipelineTestAP(t *testing.T, opt *Options) *AutoPipeliner {
	t.Helper()
	opt.Protocol = 2
	opt.DisableIdentity = true
	if opt.PipelinePoolSize == 0 {
		opt.PipelinePoolSize = 2
	}
	c := NewClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	t.Cleanup(func() { _ = ap.Close() })
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	return ap
}

// Pipeline.Exec returns the first command error, redis.Nil included. The FD
// path used to drop Nil and return nil for a pipeline whose only error is a
// missing key.
func TestFDPipelineExecReturnsNil(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})

	ctx := context.Background()
	pipe := ap.Pipeline()
	get := pipe.Get(ctx, "missing")
	if _, err := pipe.Exec(ctx); !errors.Is(err, Nil) {
		t.Fatalf("Exec err=%v, want redis.Nil", err)
	}
	if !errors.Is(get.Err(), Nil) {
		t.Fatalf("get err=%v, want redis.Nil", get.Err())
	}
}

// A retryable reply on the first command retries the whole pipeline, in
// order, as an ordinary pipeline does. The FD path used to retry the SET alone
// on another connection while the GET completed from the first attempt, so
// the GET returned the value from before the SET.
func TestFDPipelineExecRetriesWholeBatchInOrder(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(1)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr(), MaxRetries: 3})

	ctx := context.Background()
	pipe := ap.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if v, err := get.Result(); err != nil || v != "v" {
		t.Fatalf("get: v=%q err=%v, want the value the pipeline's SET wrote", v, err)
	}
	if srv.loading.Load() > 0 {
		t.Fatal("the LOADING reply was never sent; the test did not exercise the retry")
	}
}

// With ContextTimeoutEnabled an ordinary pipeline is bounded by the caller's
// deadline. The FD path used to wait for the replies regardless.
func TestFDPipelineExecHonorsContextTimeout(t *testing.T) {
	srv := newFDStateServer(t)
	srv.delay = time.Second
	ap := fdPipelineTestAP(t, &Options{
		Addr:                  srv.addr(),
		ContextTimeoutEnabled: true,
		ReadTimeout:           5 * time.Second,
		MaxRetries:            -1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	pipe := ap.Pipeline()
	pipe.Get(ctx, "k")
	start := time.Now()
	_, err := pipe.Exec(ctx)
	if el := time.Since(start); el > 700*time.Millisecond {
		t.Fatalf("Exec took %v with a 100ms deadline (err=%v): the deadline was ignored", el, err)
	}
	if err == nil {
		t.Fatal("Exec returned nil although its deadline passed before the reply")
	}
}

// fdPipeHook counts ProcessPipelineHook invocations and passes everything
// through.
type fdPipeHook struct{ pipelines atomic.Int64 }

func (h *fdPipeHook) DialHook(next DialHook) DialHook          { return next }
func (h *fdPipeHook) ProcessHook(next ProcessHook) ProcessHook { return next }
func (h *fdPipeHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		h.pipelines.Add(1)
		return next(ctx, cmds)
	}
}

// An ordinary Pipeline.Exec runs the ProcessPipelineHook chain once. The FD
// path used to skip it, so pipeline-level tracing and metrics hooks never saw
// these pipelines.
func TestFDPipelineExecRunsPipelineHooks(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	h := &fdPipeHook{}
	ap.pipeliner.(*Client).AddHook(h)

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if n := h.pipelines.Load(); n != 1 {
		t.Fatalf("ProcessPipelineHook ran %d times, want 1", n)
	}
}

// fdPipeCountLimiter admits everything and counts Allow calls.
type fdPipeCountLimiter struct{ calls atomic.Int64 }

func (l *fdPipeCountLimiter) Allow() error       { l.calls.Add(1); return nil }
func (l *fdPipeCountLimiter) ReportResult(error) {}

// On a warm connection an ordinary Pipeline.Exec takes ONE Limiter decision
// for the whole batch, so a breaker admits or rejects it as a unit. The FD path
// asked per write chunk (3 decisions for 3 commands at MaxBatchSize 1), so a
// breaker could admit part of a pipeline and reject the rest.
func TestFDPipelineExecOneLimiterDecision(t *testing.T) {
	srv := newFDStateServer(t)
	lim := &fdPipeCountLimiter{}
	c := NewClient(&Options{
		Addr: srv.addr(), Limiter: lim,
		Protocol: 2, DisableIdentity: true, PipelinePoolSize: 2,
	})
	t.Cleanup(func() { _ = c.Close() })
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true, MaxBatchSize: 1})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	t.Cleanup(func() { _ = ap.Close() })
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	ctx := context.Background()
	exec := func() int64 {
		pipe := ap.Pipeline()
		pipe.Set(ctx, "a", "1", 0)
		pipe.Set(ctx, "b", "2", 0)
		pipe.Set(ctx, "c", "3", 0)
		before := lim.calls.Load()
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Exec: %v", err)
		}
		return lim.calls.Load() - before
	}
	exec() // warm: the first Exec also pays for dialing
	for i := 0; i < 3; i++ {
		if n := exec(); n != 1 {
			t.Fatalf("warm Exec %d took %d Limiter decisions, want 1 (as an ordinary pipeline)", i, n)
		}
	}
}

// fdPipelineRecorder counts pipeline-duration records.
type fdPipelineRecorder struct {
	fdOtelRecorder
	pipelines atomic.Int64
	cmdCount  atomic.Int64
}

func (r *fdPipelineRecorder) RecordPipelineOperationDuration(_ context.Context, _ time.Duration, _ string, n, _ int, _ error, _ *pool.Conn, _ int) {
	r.pipelines.Add(1)
	r.cmdCount.Store(int64(n))
}

// With an OTel recorder installed, an ordinary Pipeline.Exec records one
// pipeline duration. The FD path recorded per-command durations only.
func TestFDPipelineExecRecordsPipelineMetric(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	rec := &fdPipelineRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if n := rec.pipelines.Load(); n != 1 {
		t.Fatalf("pipeline duration recorded %d times, want 1", n)
	}
	if n := rec.cmdCount.Load(); n != 2 {
		t.Fatalf("pipeline duration cmdCount %d, want 2", n)
	}
}

// A pipeline holding a NoRetry command is not retried, even when its first
// command gets a retryable reply: an ordinary pipeline stops on
// cmdsContainNoRetry. The FD path re-ran the batch, so a RawWriteTo command
// wrote its reply into the caller's writer twice.
func TestFDPipelineExecDoesNotRetryNoRetryCommands(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(1)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr(), MaxRetries: 3})

	ctx := context.Background()
	var buf strings.Builder
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	_ = pipe.Process(ctx, NewRawWriteToCmd(ctx, &buf, "get", "missing"))
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "LOADING") {
		t.Fatalf("Exec err=%v, want the first command's LOADING (not retried)", err)
	}
	if got, want := buf.String(), "$-1\r\n"; got != want {
		t.Fatalf("NoRetry command wrote %q, want one reply %q", got, want)
	}
}

// A canceled context is refused before any command is written, as an ordinary
// pipeline does: every command carries the context error and nothing runs.
func TestFDPipelineExecRejectsCanceledContext(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pipe := ap.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Exec err=%v, want context.Canceled", err)
	}
	for _, cmd := range []Cmder{set, get} {
		if !errors.Is(cmd.Err(), context.Canceled) {
			t.Fatalf("%s err=%v, want context.Canceled", cmd.Name(), cmd.Err())
		}
	}
	srv.mu.Lock()
	_, wrote := srv.kv["k"]
	srv.mu.Unlock()
	if wrote {
		t.Fatal("the SET reached the server although the context was already canceled")
	}
}

// The whole-batch retry keeps the ordinary pipeline's budget of MaxRetries+1
// executions, counting the FD attempt as the first. The re-run used to start
// with the full budget again, so MaxRetries:1 allowed three executions.
func TestFDPipelineExecRetryKeepsBudget(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(2) // the FD attempt and one retry both see LOADING
	ap := fdPipelineTestAP(t, &Options{
		Addr: srv.addr(), MaxRetries: 1,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond,
	})

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if n := srv.setCalls.Load(); n != 2 {
		t.Fatalf("pipeline executed %d times with MaxRetries:1, want 2 (err=%v)", n, err)
	}
	if err == nil || !strings.Contains(err.Error(), "LOADING") {
		t.Fatalf("Exec err=%v, want LOADING once the budget is spent", err)
	}
}

// Executions the FD engine spends on its own connection-error replay count
// against the same budget. Here the first write is lost (connection closed),
// the engine replays the batch and gets LOADING: with MaxRetries:1 that is
// both allowed executions, so there is no pooled re-run. The re-run used to
// assume the FD engine had issued the batch exactly once.
func TestFDPipelineExecRetryCountsFDReplays(t *testing.T) {
	srv := newFDStateServer(t)
	srv.dropFirst.Store(true)
	srv.loading.Store(1)
	ap := fdPipelineTestAP(t, &Options{
		Addr: srv.addr(), MaxRetries: 1,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond,
	})

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if n := srv.setCalls.Load(); n != 2 {
		t.Fatalf("pipeline executed %d times with MaxRetries:1, want 2 (err=%v)", n, err)
	}
	if err == nil || !strings.Contains(err.Error(), "LOADING") {
		t.Fatalf("Exec err=%v, want LOADING once the budget is spent", err)
	}
	if srv.dropFirst.Load() {
		t.Fatal("the first write was never dropped; the test did not exercise the replay")
	}
}

// A redirect-aware engine (a cluster node child, reachable as the node
// client's cached async autopipeliner) follows MOVED/ASK by re-running one
// command off the pipe. A pipeline must not ride it, or a redirected command
// would run after the rest of its batch. It keeps the ordinary pipeline.
func TestFDPipelineRefusesRedirectAwareEngine(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	ap.fd.redirectAware = true // what the cluster router sets on its children

	ctx := context.Background()
	if err := ap.FDPipelined(ctx, []Cmder{NewStatusCmd(ctx, "set", "k", "v")}); !errors.Is(err, ErrFDPipelineUnavailable) {
		t.Fatalf("FDPipelined on a redirect-aware engine: err=%v, want ErrFDPipelineUnavailable", err)
	}
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if v, err := get.Result(); err != nil || v != "v" {
		t.Fatalf("get: v=%q err=%v", v, err)
	}
}
