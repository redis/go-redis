package redis

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
)

// countingRecorder is an OTelRecorder that counts what reaches it, per method.
type countingRecorder struct {
	mu     sync.Mutex
	counts map[string]int
	conns  int // connection.count balance
}

func newCountingRecorder() *countingRecorder { return &countingRecorder{counts: map[string]int{}} }

func (r *countingRecorder) bump(k string) {
	r.mu.Lock()
	r.counts[k]++
	r.mu.Unlock()
}

func (r *countingRecorder) get(k string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[k]
}

func (r *countingRecorder) reset() {
	r.mu.Lock()
	r.counts = map[string]int{}
	r.mu.Unlock()
}

func (r *countingRecorder) connCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.conns
}

// Operations are counted by command name ("op:ping"): the handshake commands
// a connection runs (HELLO, CLIENT SETINFO) are recorded too, like any other.
func (r *countingRecorder) RecordOperationDuration(_ context.Context, _ time.Duration, cmd Cmder, _ int, _ error, _ ConnInfo, _ int) {
	r.bump("op:" + cmd.Name())
}

func (r *countingRecorder) RecordPipelineOperationDuration(context.Context, time.Duration, string, int, int, error, ConnInfo, int) {
	r.bump("pipeline")
}
func (r *countingRecorder) RecordConnectionCreateTime(context.Context, time.Duration, ConnInfo) {
	r.bump("create")
}
func (r *countingRecorder) RecordConnectionRelaxedTimeout(context.Context, int, ConnInfo, string, string) {
	r.bump("relaxed")
}
func (r *countingRecorder) RecordConnectionHandoff(context.Context, ConnInfo, string) {
	r.bump("handoff")
}
func (r *countingRecorder) RecordError(context.Context, string, ConnInfo, string, bool, int) {
	r.bump("error")
}
func (r *countingRecorder) RecordMaintenanceNotification(context.Context, ConnInfo, string) {
	r.bump("maint")
}
func (r *countingRecorder) RecordConnectionWaitTime(context.Context, time.Duration, ConnInfo) {
	r.bump("wait")
}
func (r *countingRecorder) RecordConnectionClosed(context.Context, ConnInfo, string, error) {
	r.bump("closed")
}
func (r *countingRecorder) RecordPubSubMessage(context.Context, ConnInfo, string, string, bool) {
	r.bump("pubsub")
}
func (r *countingRecorder) RecordStreamLag(context.Context, time.Duration, ConnInfo, string, string, string) {
	r.bump("lag")
}

func (r *countingRecorder) RecordConnectionCount(_ context.Context, delta int, _ ConnInfo, _ string, _ bool) {
	r.mu.Lock()
	r.conns += delta
	r.mu.Unlock()
}
func (r *countingRecorder) RecordPendingRequests(context.Context, int, ConnInfo, string) {
	r.bump("pending")
}

var _ OTelConnectionCounter = (*countingRecorder)(nil)

// TestOTelRecorderOptionPropagates pins that every options conversion carries
// the per-client recorder, so nodes, shards, sentinels and MultiDB members
// record where their parent does.
func TestOTelRecorderOptionPropagates(t *testing.T) {
	r := newCountingRecorder()
	u := &UniversalOptions{OTelRecorder: r}
	if u.Simple().OTelRecorder != r {
		t.Fatal("UniversalOptions.Simple dropped OTelRecorder")
	}
	if u.Cluster().OTelRecorder != r {
		t.Fatal("UniversalOptions.Cluster dropped OTelRecorder")
	}
	if u.Failover().OTelRecorder != r {
		t.Fatal("UniversalOptions.Failover dropped OTelRecorder")
	}
	if (&ClusterOptions{OTelRecorder: r}).clientOptions().OTelRecorder != r {
		t.Fatal("ClusterOptions.clientOptions dropped OTelRecorder")
	}
	if (&RingOptions{OTelRecorder: r}).clientOptions().OTelRecorder != r {
		t.Fatal("RingOptions.clientOptions dropped OTelRecorder")
	}
	fo := &FailoverOptions{OTelRecorder: r}
	if fo.clientOptions().OTelRecorder != r {
		t.Fatal("FailoverOptions.clientOptions dropped OTelRecorder")
	}
	if fo.sentinelOptions("127.0.0.1:26379").OTelRecorder != r {
		t.Fatal("FailoverOptions.sentinelOptions dropped OTelRecorder")
	}
	if fo.clusterOptions().OTelRecorder != r {
		t.Fatal("FailoverOptions.clusterOptions dropped OTelRecorder")
	}
	if (&Options{OTelRecorder: r}).clone().OTelRecorder != r {
		t.Fatal("Options.clone dropped OTelRecorder")
	}
}

func pipeDialer(context.Context, string, string) (net.Conn, error) {
	c, _ := net.Pipe()
	return c, nil
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cond() {
		t.Fatalf("timed out waiting for %s", what)
	}
}

// TestOTelRecorderPerClientPoolEvents pins that a client's pool events reach
// its own recorder and nothing else: not the global recorder, not another
// client's; that a client without a recorder still reaches the global one; and
// that installing or removing the global recorder neither recounts nor
// disturbs a per-client pool, whose count returns to zero on Close.
func TestOTelRecorderPerClientPoolEvents(t *testing.T) {
	global := newCountingRecorder()
	a, b := newCountingRecorder(), newCountingRecorder()

	newClient := func(rec OTelRecorder) *Client {
		return NewClient(&Options{
			Addr:            "per-client:6379",
			Dialer:          pipeDialer,
			PoolSize:        2,
			MinIdleConns:    2,
			ConnMaxIdleTime: -1,
			OTelRecorder:    rec,
		})
	}
	ca := newClient(a)
	t.Cleanup(func() { _ = ca.Close() })
	cb := newClient(b)
	t.Cleanup(func() { _ = cb.Close() })
	cg := newClient(nil)
	t.Cleanup(func() { _ = cg.Close() })

	waitFor(t, "a's idle conns", func() bool { return a.connCount() == 2 })
	waitFor(t, "b's idle conns", func() bool { return b.connCount() == 2 })
	waitFor(t, "g's idle conns", func() bool { return cg.connPool.Len() == 2 })
	if got := global.connCount(); got != 0 {
		t.Fatalf("global recorder saw %d conns before it was installed", got)
	}

	SetOTelRecorder(global)
	t.Cleanup(func() { SetOTelRecorder(nil) })
	if got := global.connCount(); got != 2 {
		t.Fatalf("global recorder after install: %d conns, want 2 (only the client without a recorder)", got)
	}
	if a.connCount() != 2 || b.connCount() != 2 {
		t.Fatalf("per-client counts changed by the global install: a=%d b=%d", a.connCount(), b.connCount())
	}

	// A conn from a's pool resolves to a's table, g's to the global table.
	cn, err := ca.connPool.Get(context.Background())
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if pool.MetricsOf(cn) != ca.poolMetrics || ca.poolMetrics == nil {
		t.Fatal("conn dialed by a per-client pool does not carry the client's table")
	}
	ca.connPool.Put(context.Background(), cn)
	if cg.poolMetrics != nil {
		t.Fatal("client without a recorder got its own pool table")
	}
	if !otel.Enabled(ca.recorder()) {
		t.Fatal("per-client recorder reports disabled")
	}

	SetOTelRecorder(nil)
	if got := global.connCount(); got != 0 {
		t.Fatalf("global recorder after uninstall: %d conns, want 0", got)
	}
	if a.connCount() != 2 || b.connCount() != 2 {
		t.Fatalf("per-client counts changed by the global uninstall: a=%d b=%d", a.connCount(), b.connCount())
	}

	_ = ca.Close()
	if got := a.connCount(); got != 0 {
		t.Fatalf("a after Close: %d conns, want 0", got)
	}
	if got := b.connCount(); got != 2 {
		t.Fatalf("b after a's Close: %d conns, want 2", got)
	}
}

// TestOTelRecorderPerClientCommands pins the execution-path routing: two
// clients with their own recorders never see each other's commands, errors or
// Pub/Sub sends, and a third client reaches only the global recorder.
func TestOTelRecorderPerClientCommands(t *testing.T) {
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	global := newCountingRecorder()
	a, b := newCountingRecorder(), newCountingRecorder()
	SetOTelRecorder(global)
	t.Cleanup(func() { SetOTelRecorder(nil) })

	ctx := context.Background()
	// One connection per client and no pipeline pool, so after the warm-up
	// below no handshake (its HELLO/CLIENT SETINFO pipeline) adds to the counts.
	newClient := func(rec OTelRecorder) *Client {
		c := NewClient(&Options{Addr: internalTestRedisAddr(), PoolSize: 1, PipelinePoolSize: -1, OTelRecorder: rec})
		t.Cleanup(func() { _ = c.Close() })
		if err := c.Ping(ctx).Err(); err != nil {
			t.Fatalf("warm-up ping: %v", err)
		}
		return c
	}
	ca, cb, cg := newClient(a), newClient(b), newClient(nil)
	if a.get("op:hello") == 0 || b.get("op:hello") == 0 || global.get("op:hello") == 0 {
		t.Fatalf("handshake not routed to each client's recorder: a=%v b=%v global=%v", a.counts, b.counts, global.counts)
	}
	a.reset()
	b.reset()
	global.reset()

	if err := ca.Ping(ctx).Err(); err != nil {
		t.Fatalf("a ping: %v", err)
	}
	if err := ca.Publish(ctx, "per-client", "x").Err(); err != nil {
		t.Fatalf("a publish: %v", err)
	}
	if err := ca.Do(ctx, "no-such-command").Err(); err == nil {
		t.Fatal("a: expected an error")
	}
	if err := cb.Ping(ctx).Err(); err != nil {
		t.Fatalf("b ping: %v", err)
	}
	if err := cg.Ping(ctx).Err(); err != nil {
		t.Fatalf("g ping: %v", err)
	}
	pipe := cb.Pipeline()
	pipe.Ping(ctx)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("b pipeline: %v", err)
	}

	want := map[*countingRecorder]map[string]int{
		a:      {"op:ping": 1, "op:publish": 1, "op:no-such-command": 1, "pubsub": 1, "error": 1, "pipeline": 0},
		b:      {"op:ping": 1, "op:publish": 0, "op:no-such-command": 0, "pubsub": 0, "error": 0, "pipeline": 1},
		global: {"op:ping": 1, "op:publish": 0, "op:no-such-command": 0, "pubsub": 0, "error": 0, "pipeline": 0},
	}
	for rec, counts := range want {
		for k, n := range counts {
			if got := rec.get(k); got != n {
				t.Errorf("%s = %d, want %d (a=%v b=%v global=%v)", k, got, n, a.counts, b.counts, global.counts)
			}
		}
	}
}

// TestOTelRecorderBatchedCommandSamples pins that Pub/Sub "sent" and stream
// lag are recorded once per successful command that runs inside a batch
// (Pipeline, TxPipeline, half-duplex and full-duplex autopipelining), and not
// for a discarded pipeline.
func TestOTelRecorderBatchedCommandSamples(t *testing.T) {
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ctx := context.Background()
	rec := newCountingRecorder()
	c := NewClient(&Options{Addr: internalTestRedisAddr(), OTelRecorder: rec})
	t.Cleanup(func() { _ = c.Close() })

	expect := func(what, key string, want int, run func()) {
		t.Helper()
		rec.reset()
		run()
		if got := rec.get(key); got != want {
			t.Errorf("%s: %s = %d, want %d", what, key, got, want)
		}
	}

	expect("Pipeline Publish", "pubsub", 1, func() {
		p := c.Pipeline()
		p.Publish(ctx, "ch", "x")
		_, _ = p.Exec(ctx)
	})
	expect("Pipeline Publish discarded", "pubsub", 0, func() {
		p := c.Pipeline()
		p.Publish(ctx, "ch", "x")
		p.Discard()
	})
	expect("TxPipeline SPublish", "pubsub", 1, func() {
		p := c.TxPipeline()
		p.SPublish(ctx, "ch", "x")
		_, _ = p.Exec(ctx)
	})

	stream := "otel-batched-samples"
	_ = c.Del(ctx, stream).Err()
	if err := c.XGroupCreateMkStream(ctx, stream, "g", "0").Err(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Del(ctx, stream).Err() })
	readArgs := &XReadGroupArgs{Group: "g", Consumer: "c", Streams: []string{stream, ">"}, Count: 1, Block: -1}
	add := func() { c.XAdd(ctx, &XAddArgs{Stream: stream, Values: []string{"k", "v"}}) }

	expect("Pipeline XReadGroup", "lag", 1, func() {
		add()
		p := c.Pipeline()
		p.XReadGroup(ctx, readArgs)
		_, _ = p.Exec(ctx)
	})

	for _, fd := range []bool{false, true} {
		ap, err := c.AutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: fd})
		if err != nil {
			t.Fatal(err)
		}
		name := map[bool]string{false: "half-duplex", true: "full-duplex"}[fd]
		expect(name+" AutoPipeline Publish", "pubsub", 1, func() {
			if err := ap.Publish(ctx, "ch", "x").Err(); err != nil {
				t.Fatal(err)
			}
		})
		expect(name+" AutoPipeline XReadGroup", "lag", 1, func() {
			add()
			if err := ap.XReadGroup(ctx, readArgs).Err(); err != nil {
				t.Fatal(err)
			}
		})
		expect(name+" AutoPipeline Pipeline Publish", "pubsub", 1, func() {
			p := ap.Pipeline()
			p.Publish(ctx, "ch", "x")
			_, _ = p.Exec(ctx)
		})
		expect(name+" AutoPipeline Pipeline XReadGroup", "lag", 1, func() {
			add()
			p := ap.Pipeline()
			p.XReadGroup(ctx, readArgs)
			_, _ = p.Exec(ctx)
		})
		_ = ap.Close()
	}
}
