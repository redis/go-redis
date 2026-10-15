package redis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
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

// A Limiter does not take the batch off the FD path. The FD writer asks it
// once per write chunk (documented on FDPipelined), so at MaxBatchSize 1 a
// three-command pipeline takes three decisions, where an ordinary pipeline
// takes one per attempt.
func TestFDPipelineWithLimiterStaysOnFD(t *testing.T) {
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
	exec := func() ([]*StatusCmd, int64) {
		pipe := ap.Pipeline()
		cmds := []*StatusCmd{pipe.Set(ctx, "a", "1", 0), pipe.Set(ctx, "b", "2", 0), pipe.Set(ctx, "c", "3", 0)}
		before := lim.calls.Load()
		if _, err := pipe.Exec(ctx); err != nil {
			t.Fatalf("Exec: %v", err)
		}
		return cmds, lim.calls.Load() - before
	}
	exec() // warm: the first Exec also pays for leasing the held connection
	cmds, n := exec()
	for _, cmd := range cmds {
		if !fdRodePipe(cmd) {
			t.Fatal("a Limiter took the pipeline off the FD path")
		}
	}
	if n != int64(len(cmds)) {
		t.Fatalf("%d Limiter decisions for %d single-command chunks, want one per chunk", n, len(cmds))
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

// With several engines, a batch may ride one only if every keyed command in it
// hashes to that engine: an earlier, unawaited command for one of its keys went
// to that key's engine, and the batch must queue behind it. The batch used to
// ride its first command's engine, so a GET for another key could run ahead of
// that key's pending SET. A spanning batch is refused, and Pipeline() runs it
// as an ordinary pipeline.
func TestFDPipelineMultiEngineStaysOnItsKeysEngine(t *testing.T) {
	srv := newFDStateServer(t)
	c := NewClient(&Options{Addr: srv.addr(), Protocol: 2, DisableIdentity: true, PipelinePoolSize: 4})
	t.Cleanup(func() { _ = c.Close() })
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true, NumShards: 4, MaxConcurrentBatches: 1})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	t.Cleanup(func() { _ = ap.Close() })
	if len(ap.fds) != 4 {
		t.Fatalf("want 4 engines, got %d", len(ap.fds))
	}
	// Two keys on different engines.
	a, b := "k0", ""
	for i := 1; b == ""; i++ {
		if k := "k" + itoa(i); ap.fdForKey(k) != ap.fdForKey(a) {
			b = k
		}
	}

	ctx := context.Background()
	span := []Cmder{NewStatusCmd(ctx, "set", a, "1"), NewStringCmd(ctx, "get", b)}
	if err := ap.FDPipelined(ctx, span); !errors.Is(err, ErrFDPipelineSpansEngines) {
		t.Fatalf("batch over two engines: err=%v, want ErrFDPipelineSpansEngines", err)
	}

	// Same engine, plus a keyless command that may ride along: accepted.
	get := NewStringCmd(ctx, "get", a)
	if err := ap.FDPipelined(ctx, []Cmder{NewStatusCmd(ctx, "set", a, "v"), get, NewStatusCmd(ctx, "ping")}); err != nil {
		t.Fatalf("batch on one engine: %v", err)
	}
	if v, err := get.Result(); err != nil || v != "v" {
		t.Fatalf("get %s: v=%q err=%v", a, v, err)
	}

	// Pipeline() runs a spanning batch as an ordinary pipeline.
	pipe := ap.Pipeline()
	pipe.Set(ctx, a, "x", 0)
	pipe.Set(ctx, b, "y", 0)
	gb := pipe.Get(ctx, b)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec over two engines: %v", err)
	}
	if v, err := gb.Result(); err != nil || v != "y" {
		t.Fatalf("get %s: v=%q err=%v", b, v, err)
	}
}

// ===== from autopipeline_fd_pipeline_csc_guard_test.go =====

// An FD pipeline on a client-side-caching client refuses the commands that
// would change the held connection's state (SELECT, AUTH, HELLO with a
// protocol, RESET, CLIENT TRACKING), as an ordinary pipeline does. They leave
// the pipe (runsOutsidePipeline), so Pipeline() falls back to the ordinary
// pipeline, whose CSC guard rejects them, and FDPipelined refuses the batch.
// The held connection stays on DB 0.
func TestFDPipelineRejectsCSCStateCommands(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	c := NewClient(&Options{
		Addr:                  internalTestRedisAddr(),
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 128},
	})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if !c.autopipelineCSCActive() {
		t.Skip("client-side caching did not attach (server lacks CLIENT TRACKING?)")
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	key := "fd-csc-guard:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	plain := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer plain.Close()
	if err := plain.Set(ctx, key, "db0", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	defer plain.Del(ctx, key)

	for _, tc := range []struct {
		args []interface{}
		want error
	}{
		{[]interface{}{"select", 1}, errSelectWithCSC},
		{[]interface{}{"SELECT", 1}, errSelectWithCSC},
		{[]interface{}{"auth", "user", "pass"}, errAuthWithCSC},
		{[]interface{}{"hello", 2}, errHelloWithCSC},
		{[]interface{}{"reset"}, errResetWithCSC},
		{[]interface{}{"client", "tracking", "off"}, errClientTrackingWithCSC},
	} {
		pipe := ap.Pipeline()
		cmd := pipe.Do(ctx, tc.args...)
		if _, err := pipe.Exec(ctx); !errors.Is(err, tc.want) {
			t.Fatalf("Pipeline %v: Exec err=%v, want %v", tc.args, err, tc.want)
		}
		if !errors.Is(cmd.Err(), tc.want) {
			t.Fatalf("Pipeline %v: cmd err=%v, want %v", tc.args, cmd.Err(), tc.want)
		}
		if err := ap.FDPipelined(ctx, []Cmder{NewCmd(ctx, tc.args...)}); !errors.Is(err, ErrFDPipelineDiverts) {
			t.Fatalf("FDPipelined %v: err=%v, want ErrFDPipelineDiverts", tc.args, err)
		}
	}

	// The held connection must still be on DB 0.
	pipe := ap.Pipeline()
	get := pipe.Get(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("GET after the refused SELECT: %v", err)
	}
	if v := get.Val(); v != "db0" {
		t.Fatalf("GET %s = %q on the FD pipe, want db0: the connection changed DB", key, v)
	}
}

// ===== from autopipeline_fd_pipeline_errs_test.go =====

// An FD pipeline whose connection fails after an early Redis error returns the
// transport failure, as an ordinary pipeline does: it tells the caller that
// the later commands have no known result. It used to return the early
// WRONGTYPE.
func TestFDPipelineReturnsTransportErrorOverRedisError(t *testing.T) {
	srv := newFDDropScript(t, func(conn, idx int, name string) (string, bool) {
		if conn == 0 && name == "get" {
			return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n", false
		}
		if conn == 0 {
			return "", true // SET executed, then the connection drops
		}
		return "", false
	})
	ap := fdPipelineTestAP(t, &Options{Addr: srv.ln.Addr().String(), MaxRetries: -1})

	ctx := context.Background()
	pipe := ap.Pipeline()
	get := pipe.Get(ctx, "k")
	pipe.Set(ctx, "k2", "v", 0)
	_, err := pipe.Exec(ctx)
	if err == nil || isRedisError(err) {
		t.Fatalf("Exec err=%v, want the transport failure", err)
	}
	if e := get.Err(); e == nil || !strings.Contains(e.Error(), "WRONGTYPE") {
		t.Fatalf("GET err=%v, want its own WRONGTYPE", e)
	}
}

// A retryable first reply must not re-run the batch when a later reply failed
// with a protocol error, which the engine does not replay because that command
// may already have run. The re-run executed SET twice; an ordinary pipeline
// returns the read error and does not retry.
func TestFDPipelineNoRerunAfterUnreplayedFailure(t *testing.T) {
	srv := newFDDropScript(t, func(conn, idx int, name string) (string, bool) {
		if conn == 0 && name == "get" {
			return "-LOADING Redis is loading the dataset in memory\r\n", false
		}
		if conn == 0 && name == "set" {
			return "?malformed\r\n", false // SET executed, then a protocol fault
		}
		return "", false
	})
	ap := fdPipelineTestAP(t, &Options{Addr: srv.ln.Addr().String(), MaxRetries: 3,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Get(ctx, "k")
	pipe.Set(ctx, "k2", "v", 0)
	_, err := pipe.Exec(ctx)
	if n := srv.count("set"); n != 1 {
		t.Fatalf("SET executed %d times, want 1", n)
	}
	if err == nil || isRedisError(err) {
		t.Fatalf("Exec err=%v, want the protocol failure", err)
	}
}

// ===== from autopipeline_fd_pipeline_hooks_test.go =====

// Pipeline hooks and pipeline metrics on the full-duplex path. A pipeline
// batch runs inside the client's ProcessPipelineHook chain, exactly once, and
// records one pipeline duration, as an ordinary pipeline does, while staying
// on the held connection.

// fdRodePipe reports whether cmd ran as a full-duplex pipeline batch: only
// that path installs a batch the reader stamps with an attempt count.
func fdRodePipe(cmd Cmder) bool {
	var b *apBatch
	switch c := cmd.(type) {
	case *StatusCmd:
		b = c.ready.Load()
	case *StringCmd:
		b = c.ready.Load()
	}
	return b != nil && b.fdAttempts > 0
}

// fdWarm runs one pipeline so the held connection is leased before a test
// measures: the lease's handshake (HELLO) goes through the client's process
// path once, which is connection setup, not a pipelined command.
func fdWarm(t *testing.T, ap *AutoPipeliner) {
	t.Helper()
	pipe := ap.Pipeline()
	pipe.Get(context.Background(), "warm")
	if _, err := pipe.Exec(context.Background()); err != nil && !errors.Is(err, Nil) {
		t.Fatalf("warm-up Exec: %v", err)
	}
}

// fdPipeHooksCounter counts pipeline and per-command hook calls, and records what a
// pipeline hook sees in the last command after next() returns.
type fdPipeHooksCounter struct {
	pipelines, processes atomic.Int64
	afterNext            atomic.Value // string
}

func (h *fdPipeHooksCounter) DialHook(next DialHook) DialHook { return next }
func (h *fdPipeHooksCounter) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		h.processes.Add(1)
		return next(ctx, cmd)
	}
}
func (h *fdPipeHooksCounter) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return func(ctx context.Context, cmds []Cmder) error {
		h.pipelines.Add(1)
		err := next(ctx, cmds)
		if s, ok := cmds[len(cmds)-1].(*StringCmd); ok {
			h.afterNext.Store(s.Val())
		}
		return err
	}
}

func TestFDPipelineHooksWrapTheFDBatch(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	fdWarm(t, ap)
	h := &fdPipeHooksCounter{}
	ap.pipeliner.(*Client).AddHook(h)

	ctx := context.Background()
	pipe := ap.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if !fdRodePipe(set) || !fdRodePipe(get) {
		t.Fatal("a hook took the pipeline off the FD path")
	}
	if n := h.pipelines.Load(); n != 1 {
		t.Fatalf("ProcessPipelineHook ran %d times, want 1", n)
	}
	if n := h.processes.Load(); n != 0 {
		t.Fatalf("ProcessHook ran %d times for a pipeline, want 0 (as an ordinary pipeline)", n)
	}
	if v, _ := h.afterNext.Load().(string); v != "v" {
		t.Fatalf("the hook saw %q after next(), want the GET result", v)
	}
}

func TestFDPipelinedRunsPipelineHooks(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	fdWarm(t, ap)
	h := &fdPipeHooksCounter{}
	ap.pipeliner.(*Client).AddHook(h)

	ctx := context.Background()
	get := NewStringCmd(ctx, "get", "k")
	if err := ap.FDPipelined(ctx, []Cmder{NewStatusCmd(ctx, "set", "k", "v"), get}); err != nil {
		t.Fatalf("FDPipelined: %v", err)
	}
	if !fdRodePipe(get) {
		t.Fatal("FDPipelined did not run on the FD path")
	}
	if h.pipelines.Load() != 1 || h.processes.Load() != 0 {
		t.Fatalf("hooks: pipeline=%d process=%d, want 1 and 0", h.pipelines.Load(), h.processes.Load())
	}
}

// A batch that cannot ride the pipe runs the ordinary pipeline INSIDE the same
// hook chain, so the hooks still run once, not once around the refusal and
// again around the fallback.
func TestFDPipelineHooksRunOnceOnFallback(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	h := &fdPipeHooksCounter{}
	ap.pipeliner.(*Client).AddHook(h)

	ctx := context.Background()
	var buf strings.Builder
	pipe := ap.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	_ = pipe.Process(ctx, NewRawWriteToCmd(ctx, &buf, "get", "k")) // NoRetry: ordinary path
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if fdRodePipe(set) {
		t.Fatal("a batch with a NoRetry command rode the FD path")
	}
	if n := h.pipelines.Load(); n != 1 {
		t.Fatalf("ProcessPipelineHook ran %d times across the fallback, want 1", n)
	}
}

// The whole-batch retry runs inside the hook chain, as generalProcessPipeline's
// retries do, so the hooks see one pipeline.
func TestFDPipelineHooksRunOnceAcrossRetry(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(1)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr(), MaxRetries: 3,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})
	h := &fdPipeHooksCounter{}
	ap.pipeliner.(*Client).AddHook(h)

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if v, err := get.Result(); err != nil || v != "v" {
		t.Fatalf("get: v=%q err=%v", v, err)
	}
	if n := h.pipelines.Load(); n != 1 {
		t.Fatalf("ProcessPipelineHook ran %d times across the retry, want 1", n)
	}
}

// An OTel recorder gets one pipeline duration for an FD batch and no
// per-command durations, as for an ordinary pipeline; the batch stays on the
// FD path. Both entry points.
func TestFDPipelineRecordsPipelineMetricOnFD(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	fdWarm(t, ap)
	rec := &fdPipelineRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if !fdRodePipe(get) {
		t.Fatal("an OTel recorder took the pipeline off the FD path")
	}
	if rec.pipelines.Load() != 1 || rec.cmdCount.Load() != 2 {
		t.Fatalf("pipeline metric: count=%d cmdCount=%d, want 1 and 2", rec.pipelines.Load(), rec.cmdCount.Load())
	}
	if n := rec.opDurations.Load(); n != 0 {
		t.Fatalf("%d per-command durations recorded for a pipeline, want 0", n)
	}

	get2 := NewStringCmd(ctx, "get", "k")
	if err := ap.FDPipelined(ctx, []Cmder{get2}); err != nil {
		t.Fatalf("FDPipelined: %v", err)
	}
	if rec.pipelines.Load() != 2 || rec.opDurations.Load() != 0 {
		t.Fatalf("after FDPipelined: pipelines=%d per-command=%d, want 2 and 0", rec.pipelines.Load(), rec.opDurations.Load())
	}
}

// ===== from autopipeline_fd_pipeline_late_recorder_test.go =====

// fdDurationRecorder records the longest pipeline duration it was given.
type fdDurationRecorder struct {
	fdOtelRecorder
	calls atomic.Int64
	max   atomic.Int64
}

func (r *fdDurationRecorder) RecordPipelineOperationDuration(_ context.Context, d time.Duration, _ string, _, _ int, _ error, _ *pool.Conn, _ int) {
	r.calls.Add(1)
	if int64(d) > r.max.Load() {
		r.max.Store(int64(d))
	}
}

// A recorder installed while an FD pipeline runs finds the operation without
// a start time: the start is read only when a duration callback exists.
// fdPipelineMetrics used to report time.Since(time.Time{}), a duration of
// centuries. It now records no duration for that operation, as an ordinary
// pipeline does (it reads the callback once, at the start).
func TestFDPipelineMetricsSkipsDurationWithoutStart(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	rec := &fdDurationRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	cmds := []Cmder{NewStatusCmd(ctx, "set", "k", "v")}
	ap.fdPipelineMetrics(ctx, time.Time{}, cmds, 1, nil)
	if n := rec.calls.Load(); n != 0 {
		t.Fatalf("recorded %d duration(s) for an operation with no start (max %v)", n, time.Duration(rec.max.Load()))
	}
	ap.fdPipelineMetrics(ctx, time.Now(), cmds, 1, nil)
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("recorded %d duration(s) for an operation with a start, want 1", n)
	}
}

// The pooled re-run continues an FD operation. If that operation began with
// no duration callback (zero start) and a recorder was installed before the
// re-run, generalProcessPipelineFrom started the clock at the re-run: the
// duration covered only the pooled tail while the count included the FD
// attempt. It now records no duration, as the FD path does.
func TestPipelineRetriesAfterSkipsDurationWithoutStart(t *testing.T) {
	srv := newFDStateServer(t)
	c := NewClient(&Options{Addr: srv.addr(), Protocol: 2, DisableIdentity: true})
	defer c.Close()
	rec := &fdDurationRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	cmds := []Cmder{NewStatusCmd(ctx, "set", "k", "v")}
	if err := c.processPipelineRetriesAfter(ctx, cmds, 0, time.Time{}, 1); err != nil {
		t.Fatalf("processPipelineRetriesAfter: %v", err)
	}
	if n := rec.calls.Load(); n != 0 {
		t.Fatalf("recorded %d duration(s) for a continued operation with no start", n)
	}
	cmds = []Cmder{NewStatusCmd(ctx, "set", "k", "v")}
	if err := c.processPipelineRetriesAfter(ctx, cmds, 0, time.Now(), 1); err != nil {
		t.Fatalf("processPipelineRetriesAfter: %v", err)
	}
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("recorded %d duration(s) for a continued operation with a start, want 1", n)
	}
}

// ===== from autopipeline_fd_pipeline_metrics_test.go =====

// processPipelineRetriesAfter continues an operation that already ran, so its
// retries continue the backoff too. It restarted at the first step, so after
// an FD attempt the pooled re-run's later retries slept like early ones. With
// 20 prior attempts the next backoff is at MaxRetryBackoff; the first step is
// at most 3*MinRetryBackoff.
func TestPipelineRetriesAfterContinueBackoff(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(1) // the first run gets LOADING, the retry succeeds
	c := NewClient(&Options{
		Addr: srv.addr(), Protocol: 2, DisableIdentity: true,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: 200 * time.Millisecond,
	})
	defer c.Close()

	ctx := context.Background()
	cmd := NewStatusCmd(ctx, "set", "k", "v")
	start := time.Now()
	if err := c.processPipelineRetriesAfter(ctx, []Cmder{cmd}, 1, start, 20); err != nil {
		t.Fatalf("processPipelineRetriesAfter: %v", err)
	}
	if elapsed := time.Since(start); elapsed < 100*time.Millisecond {
		t.Fatalf("the retry slept %v; after 20 prior attempts the backoff should be at its 200ms cap", elapsed)
	}
}

// A pipeline rejected before admission (its ctx already done) records one
// pipeline metric, as an ordinary pipeline does when withPipelineConn fails.
// The FD path recorded nothing.
func TestFDPipelineRecordsRejectedPipelineMetric(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})
	rec := &fdPipelineRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Exec err=%v, want context.Canceled", err)
	}
	if n := rec.pipelines.Load(); n != 1 {
		t.Fatalf("Pipeline().Exec: pipeline duration recorded %d times, want 1", n)
	}

	cmds := []Cmder{NewStatusCmd(ctx, "set", "k", "v")}
	if err := ap.FDPipelined(ctx, cmds); !errors.Is(err, context.Canceled) {
		t.Fatalf("FDPipelined err=%v, want context.Canceled", err)
	}
	if n := rec.pipelines.Load(); n != 2 {
		t.Fatalf("FDPipelined: pipeline duration recorded %d times in total, want 2", n)
	}
}

// A batch the Close-time flush never runs keeps its own metric: the flush
// marked every batch before running any, so when the carry hit a dead
// endpoint and the fresh queue was failed, fdPipelineExec skipped the failed
// batch's metric and nothing recorded it.
func TestFDShutdownFlushMarksOnlyBatchesItRuns(t *testing.T) {
	ctx := context.Background()
	dead := errors.New("dial tcp: connection refused")
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{MaxRetries: 3}}},
		maxBatch: 8,
		q:        newFDQueue(8),
		runPipeline: func(_ context.Context, cmds []Cmder, _ int) error {
			setCmdsErr(cmds, dead)
			return dead
		},
	}
	carry := fdPipeGroupReqs(ctx, 1)
	fresh := fdPipeGroupReqs(ctx, 1)
	carry[0].batch.fdAttempts, fresh[0].batch.fdAttempts = 1, 1 // as submitBatch stamps them
	fd.q.push(fresh[0])
	fd.shutdownFlush(ctx, carry)

	if !carry[0].batch.fdFlushed {
		t.Fatal("the carried batch ran in the flush but is not marked flushed")
	}
	if carry[0].batch.fdAttempts < 2 {
		t.Fatalf("the carried batch ran again in the flush but reports %d issue(s)", carry[0].batch.fdAttempts)
	}
	if fresh[0].batch.fdFlushed {
		t.Fatal("the fresh batch never ran (the carry hit a dead endpoint) but is marked flushed; its failure would record no metric")
	}
	// Its attempt count must not count an issue that never happened.
	if a := fresh[0].batch.fdAttempts; a != 1 {
		t.Fatalf("the fresh batch never ran but reports %d issues, want 1", a)
	}
}

// Only a pipeline the flush ran in full was measured by the flush. When it ran
// only the unread tail, the FD metric still covers the whole operation.
func TestFDPipelineAllFlushed(t *testing.T) {
	a, b := newAPBatch(), newAPBatch()
	b.fdFlushed = true
	if fdPipelineAllFlushed([]*apBatch{a, b}) {
		t.Fatal("a pipeline with only its tail flushed reported as flushed in full")
	}
	a.fdFlushed = true
	if !fdPipelineAllFlushed([]*apBatch{a, b}) {
		t.Fatal("a pipeline flushed in full reported as not flushed")
	}
	if fdPipelineAllFlushed(nil) {
		t.Fatal("an empty pipeline reported as flushed")
	}
}

// ===== from autopipeline_fd_pipeline_replay_test.go =====

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

// ===== from autopipeline_fd_pipeline_retry_test.go =====

// The whole-batch retry and its interaction with the engine's own replays.

// After a connection-error replay the replay already was the batch's retry, so
// a stale retryable reply on the first command must not trigger a pooled
// re-run on top. Here SET gets LOADING, the connection drops before GET's
// reply, and the engine replays GET. The re-run used to run GET a third time,
// where an ordinary pipeline runs it twice (once, then once in its retry).
func TestFDPipelineNoRerunAfterReplay(t *testing.T) {
	srv := newFDDropScript(t, func(conn, idx int, name string) (string, bool) {
		if conn == 0 {
			if name == "set" {
				return "-LOADING Redis is loading the dataset in memory\r\n", false
			}
			return "", true // GET executed, then the connection drops
		}
		return "", false
	})
	ap := fdPipelineTestAP(t, &Options{Addr: srv.ln.Addr().String(), MaxRetries: 3,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if n := srv.count("get"); n != 2 {
		t.Fatalf("GET executed %d times, want 2 (once, then the engine's replay)", n)
	}
	if err == nil || !strings.Contains(err.Error(), "LOADING") {
		t.Fatalf("Exec err=%v, want the first command's LOADING", err)
	}
}

// The Close-time flush runs a pipelined batch through the pooled pipeline with
// its own retry loop, so the batch must count as already retried: otherwise
// fdPipelineExec saw one issue and a stale LOADING on the first command, and
// ran the batch again after Close.
func TestFDShutdownFlushMarksPipelinedBatchesRetried(t *testing.T) {
	ctx := context.Background()
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{MaxRetries: 3}}},
		maxBatch: 8,
		runPipeline: func(_ context.Context, cmds []Cmder, _ int) error {
			cmds[0].SetErr(proto.RedisError("LOADING Redis is loading the dataset in memory"))
			return nil
		},
	}
	set := NewStatusCmd(ctx, "set", "k", "v")
	b := newAPBatch()
	b.fdAttempts = 1
	fd.shutdownFlush(ctx, []fdReq{{cmd: set, batch: b, attempts: 1, pipelined: true}})

	select {
	case <-b.done:
	case <-time.After(5 * time.Second):
		t.Fatal("the flushed command never completed")
	}
	if b.fdAttempts < 2 {
		t.Fatalf("flushed pipelined batch reports %d issue(s); fdPipelineExec would re-run it after Close", b.fdAttempts)
	}
	if !b.fdFlushed {
		t.Fatal("flushed pipelined batch is not marked flushed; fdPipelineExec would record its pipeline metric twice")
	}
}

// fdFlushProbe is a stub engine whose flush records the chunk sizes it runs.
func fdFlushProbe(maxBatch int) (*fdEngine, *[]int) {
	var chunks []int
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{MaxRetries: 3}}},
		maxBatch: maxBatch,
		runPipeline: func(_ context.Context, cmds []Cmder, _ int) error {
			chunks = append(chunks, len(cmds))
			return nil
		},
	}
	return fd, &chunks
}

// fdPipeGroupReqs builds n pipelined requests of one FD pipeline batch, the
// way submitBatch stamps them, with the given attempt counts.
func fdPipeGroupReqs(ctx context.Context, attempts ...int) []fdReq {
	reqs := make([]fdReq, len(attempts))
	var first *apBatch
	for i, a := range attempts {
		b := newAPBatch()
		if first == nil {
			first = b
		}
		b.fdGroup = first
		reqs[i] = fdReq{cmd: NewStatusCmd(ctx, "set", "k"+itoa(i), "v"), batch: b, attempts: a, pipelined: true}
	}
	return reqs
}

// The Close-time flush runs an FD pipeline batch as ONE pipeline, so its
// whole-batch retry covers all of it. It used to split the batch at
// MaxBatchSize and retry each chunk on its own. Ordinary commands around it are
// still chunked, without being mixed into the pipeline.
func TestFDShutdownFlushKeepsPipelineWhole(t *testing.T) {
	ctx := context.Background()
	fd, chunks := fdFlushProbe(2)
	carry := append(fdPipeGroupReqs(ctx, 1, 1, 1, 1, 1),
		fdReq{cmd: NewStatusCmd(ctx, "get", "a"), batch: newAPBatch(), attempts: 1},
		fdReq{cmd: NewStatusCmd(ctx, "get", "b"), batch: newAPBatch(), attempts: 1})
	fd.shutdownFlush(ctx, carry)
	if got := *chunks; len(got) != 2 || got[0] != 5 || got[1] != 2 {
		t.Fatalf("flush chunks %v, want [5 2]: the 5-command pipeline whole, then the ordinary commands", got)
	}
}

// A pipeline batch whose commands carry different attempt counts (a partial
// replay) is still flushed as one pipeline, not split by attempt count.
func TestFDShutdownFlushKeepsPipelineWholeAcrossAttempts(t *testing.T) {
	ctx := context.Background()
	fd, chunks := fdFlushProbe(8)
	fd.shutdownFlush(ctx, fdPipeGroupReqs(ctx, 2, 2, 1))
	if got := *chunks; len(got) != 1 || got[0] != 3 {
		t.Fatalf("flush chunks %v, want [3]: one pipeline despite mixed attempt counts", got)
	}
}

// A pipeline the writer took only a prefix of, before the session failed, has
// that prefix in the carry and its tail still queued. Close flushes it as one
// pipeline with one retry budget, the smallest of its commands', not as two
// pipelines where the tail gets a full budget of its own.
func TestFDShutdownFlushJoinsPipelineSplitAcrossCarryAndQueue(t *testing.T) {
	ctx := context.Background()
	type run struct{ n, maxRetries int }
	var runs []run
	fd := &fdEngine{
		ap:       &AutoPipeliner{config: &AutoPipelineOptions{}},
		client:   &Client{baseClient: &baseClient{opt: &Options{MaxRetries: 3}}},
		maxBatch: 8,
		q:        newFDQueue(8),
		runPipeline: func(_ context.Context, cmds []Cmder, maxRetries int) error {
			runs = append(runs, run{len(cmds), maxRetries})
			return nil
		},
	}
	reqs := fdPipeGroupReqs(ctx, 2, 2, 1, 1, 1) // prefix replayed once, tail never taken
	if res := fd.q.pushBatch(reqs[2:]); res != fdPushOK {
		t.Fatalf("pushBatch = %v", res)
	}
	fd.shutdownFlush(ctx, reqs[:2])

	want := run{5, fdCarryRemainingRetries(2, 3)}
	if len(runs) != 1 || runs[0] != want {
		t.Fatalf("flush runs %+v, want one %+v: the whole pipeline at the prefix's remaining budget", runs, want)
	}
}

// fdRetryMetricRecorder captures pipeline-duration records.
type fdRetryMetricRecorder struct {
	fdOtelRecorder
	calls    atomic.Int64
	attempts atomic.Int64
	duration atomic.Int64
	lastErr  atomic.Value
}

func (r *fdRetryMetricRecorder) RecordPipelineOperationDuration(_ context.Context, d time.Duration, _ string, _ int, attempts int, err error, _ *pool.Conn, _ int) {
	r.calls.Add(1)
	r.attempts.Store(int64(attempts))
	r.duration.Store(int64(d))
	if err != nil {
		r.lastErr.Store(err)
	}
}

// A whole-batch re-run continues the FD attempt's measurement: one pipeline
// metric, counting the FD attempt and covering its time. The pooled re-run
// used to open its own measurement, recording one attempt and only its own
// duration.
func TestFDPipelineRetryMetricCoversTheWholeOperation(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(1)
	srv.delay = 30 * time.Millisecond
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr(), MaxRetries: 3,
		MinRetryBackoff: time.Millisecond, MaxRetryBackoff: time.Millisecond})
	rec := &fdRetryMetricRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("%d pipeline metrics for one operation, want 1", n)
	}
	if a := rec.attempts.Load(); a != 2 {
		t.Fatalf("attempts=%d, want 2 (the FD attempt and the re-run)", a)
	}
	// Both attempts wait srv.delay, so one operation spans at least two of them.
	if d := time.Duration(rec.duration.Load()); d < 2*srv.delay {
		t.Fatalf("duration %v excludes the FD attempt (each attempt waits %v)", d, srv.delay)
	}
}

// A ctx that ends during the backoff before the re-run still records the
// operation, with the ctx error.
func TestFDPipelineRetryMetricOnBackoffCancel(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loading.Store(1)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr(), MaxRetries: 3,
		MinRetryBackoff: time.Second, MaxRetryBackoff: time.Second})
	rec := &fdRetryMetricRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	pipe := ap.Pipeline()
	pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exec err=%v, want the ctx deadline", err)
	}
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("%d pipeline metrics, want 1 even though ctx ended in the backoff", n)
	}
	if e, _ := rec.lastErr.Load().(error); !errors.Is(e, context.DeadlineExceeded) {
		t.Fatalf("metric error %v, want the ctx deadline", e)
	}
}

// Live recovery fails an FD pipeline as one. When part of it fails (its
// budget is spent, or the error is not retryable), the rest of it must not
// run: neither its members kept for replay nor the tail the writer had not yet
// taken from the queue. It used to send that tail on the next connection, so
// Exec reported a transport error for a pipeline whose later commands ran.
func TestFDRecoveryFailsTheWholePipeline(t *testing.T) {
	ctx := context.Background()
	fd := &fdEngine{
		ap:     &AutoPipeliner{config: &AutoPipelineOptions{}},
		client: &Client{baseClient: &baseClient{opt: &Options{}}},
		q:      newFDQueue(16),
	}
	pipe := fdPipeGroupReqs(ctx, 2, 1, 1, 1, 1) // [0] spent, [1] kept, [2:] still queued
	other := fdPipeGroupReqs(ctx, 1)            // another pipeline, kept
	single := fdReq{cmd: NewStatusCmd(ctx, "get", "s"), batch: newAPBatch(), attempts: 1}
	if res := fd.q.pushBatch(pipe[2:]); res != fdPushOK {
		t.Fatalf("pushBatch = %v", res)
	}
	fd.q.push(single)

	dead := errors.New("read: connection reset by peer")
	keep := fd.failPipelines(pipe[:1], []fdReq{pipe[1], other[0]}, dead)

	if len(keep) != 1 || keep[0].batch != other[0].batch {
		t.Fatalf("kept %d requests, want only the other pipeline's", len(keep))
	}
	for i, r := range pipe {
		select {
		case <-r.batch.done:
		default:
			t.Fatalf("pipeline command %d was not failed with the rest of its pipeline", i)
		}
		if !errors.Is(r.cmd.Err(), dead) {
			t.Fatalf("pipeline command %d: err %v, want %v", i, r.cmd.Err(), dead)
		}
	}
	if d := fd.q.depth(); d != 1 {
		t.Fatalf("queue depth %d after the failure, want 1 (only the unrelated command)", d)
	}
}
