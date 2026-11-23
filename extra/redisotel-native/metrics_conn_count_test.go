package redisotel

import (
	"bytes"
	"context"
	"io"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/redis/go-redis/v9"
)

// countKey identifies one db.client.connection.count series.
type countKey struct {
	pool, state string
	pubSub      bool
}

// collectCounts returns the connection.count points on reader whose pool name
// starts with prefix, and checks the exported shape: a non-monotonic Sum.
func collectCounts(t *testing.T, reader *metric.ManualReader, prefix string) map[countKey]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	got := map[countKey]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name != MetricConnectionCount {
				continue
			}
			sum, ok := m.Data.(metricdata.Sum[int64])
			if !ok {
				t.Fatalf("%s is %T, want metricdata.Sum[int64]", m.Name, m.Data)
			}
			if sum.IsMonotonic {
				t.Fatalf("%s is monotonic, want an UpDownCounter", m.Name)
			}
			if m.Unit != "{connection}" {
				t.Fatalf("%s unit = %q, want {connection}", m.Name, m.Unit)
			}
			for _, dp := range sum.DataPoints {
				name, _ := dp.Attributes.Value(AttrDBClientConnectionPoolName)
				if !strings.HasPrefix(name.AsString(), prefix) {
					continue
				}
				state, _ := dp.Attributes.Value(AttrDBClientConnectionState)
				pubSub, _ := dp.Attributes.Value(AttrRedisClientConnectionPubSub)
				got[countKey{name.AsString(), state.AsString(), pubSub.AsBool()}] += dp.Value
			}
		}
	}
	return got
}

// byState sums counts over pools.
func byState(counts map[countKey]int64) (idle, used, pubSub int64) {
	for k, v := range counts {
		switch {
		case k.pubSub:
			pubSub += v
		case k.state == "idle":
			idle += v
		default:
			used += v
		}
	}
	return idle, used, pubSub
}

// memConn answers commands in memory: PING with PONG, HELLO with an error
// (so the client uses RESP2), anything else with OK.
type memConn struct {
	mu    sync.Mutex
	buf   []byte
	ready chan struct{}
	done  chan struct{}
	once  sync.Once
}

func memDialer(context.Context, string, string) (net.Conn, error) {
	return &memConn{ready: make(chan struct{}, 1), done: make(chan struct{})}, nil
}

// commandNames returns the name of each RESP array command in p.
func commandNames(p []byte) []string {
	var names []string
	lines := bytes.Split(p, []byte("\r\n"))
	for i := 0; i < len(lines); i++ {
		if len(lines[i]) == 0 || lines[i][0] != '*' {
			continue
		}
		argc, _ := strconv.Atoi(string(lines[i][1:]))
		if argc > 0 && i+2 < len(lines) {
			names = append(names, strings.ToLower(string(lines[i+2])))
		}
		i += 2 * argc
	}
	return names
}

func (c *memConn) Write(p []byte) (int, error) {
	select {
	case <-c.done:
		return 0, net.ErrClosed
	default:
	}
	c.mu.Lock()
	for _, name := range commandNames(p) {
		switch name {
		case "hello":
			c.buf = append(c.buf, "-ERR unknown command 'hello'\r\n"...)
		case "ping":
			c.buf = append(c.buf, "+PONG\r\n"...)
		default:
			c.buf = append(c.buf, "+OK\r\n"...)
		}
	}
	c.mu.Unlock()
	select {
	case c.ready <- struct{}{}:
	default:
	}
	return len(p), nil
}

func (c *memConn) Read(p []byte) (int, error) {
	for {
		c.mu.Lock()
		if len(c.buf) > 0 {
			n := copy(p, c.buf)
			c.buf = c.buf[:copy(c.buf, c.buf[n:])]
			c.mu.Unlock()
			return n, nil
		}
		c.mu.Unlock()
		select {
		case <-c.ready:
		case <-c.done:
			return 0, io.EOF
		}
	}
}

func (c *memConn) Close() error                     { c.once.Do(func() { close(c.done) }); return nil }
func (c *memConn) LocalAddr() net.Addr              { return &net.TCPAddr{} }
func (c *memConn) RemoteAddr() net.Addr             { return &net.TCPAddr{} }
func (c *memConn) SetDeadline(time.Time) error      { return nil }
func (c *memConn) SetReadDeadline(time.Time) error  { return nil }
func (c *memConn) SetWriteDeadline(time.Time) error { return nil }

func memClientOptions(addr string, rec redis.OTelRecorder) *redis.Options {
	return &redis.Options{
		Addr:             addr,
		Dialer:           memDialer,
		PoolSize:         4,
		MinIdleConns:     3,
		ConnMaxIdleTime:  -1,
		PipelinePoolSize: -1,
		DisableIdentity:  true,
		OTelRecorder:     rec,
	}
}

func waitConns(t *testing.T, c *redis.Client, n uint32) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for c.PoolStats().TotalConns != n && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if got := c.PoolStats().TotalConns; got != n {
		t.Fatalf("pool has %d conns, want %d", got, n)
	}
}

func newMemClient(t *testing.T, addr string, rec redis.OTelRecorder) *redis.Client {
	t.Helper()
	c := redis.NewClient(memClientOptions(addr, rec))
	t.Cleanup(func() { _ = c.Close() })
	waitConns(t, c, 3)
	return c
}

func initOn(t *testing.T, reader *metric.ManualReader, cfg *Config) *ObservabilityInstance {
	t.Helper()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	inst := GetObservabilityInstance()
	if err := inst.Init(cfg.WithEnabled(true).WithMeterProvider(provider)); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = inst.Shutdown() })
	return inst
}

// Each collection reads the current state of the pool: a held connection
// shows as used, and as idle again once it is returned. MinIdleConns dials a
// replacement while the connection is held, so the pool grows to 4.
func TestConnectionCountFollowsPoolState(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	reader := metric.NewManualReader()
	initOn(t, reader, NewConfig())
	c := newMemClient(t, "state:6379", nil)

	ctx := context.Background()
	held := c.Conn()
	if err := held.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	waitConns(t, c, 4)
	if idle, used, _ := byState(collectCounts(t, reader, "state:6379_")); idle != 3 || used != 1 {
		t.Fatalf("while a conn is held: idle=%d used=%d, want 3 and 1", idle, used)
	}
	_ = held.Close()
	if idle, used, _ := byState(collectCounts(t, reader, "state:6379_")); idle != 4 || used != 0 {
		t.Fatalf("after release: idle=%d used=%d, want 4 and 0", idle, used)
	}
}

// A delta-temporality reader gets the change between collections, which the
// SDK computes from the observed absolute values.
func TestConnectionCountDeltaTemporality(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	reader := metric.NewManualReader(metric.WithTemporalitySelector(func(metric.InstrumentKind) metricdata.Temporality {
		return metricdata.DeltaTemporality
	}))
	initOn(t, reader, NewConfig())
	c := newMemClient(t, "delta:6379", nil)
	ctx := context.Background()

	check := func(step string, wantIdle, wantUsed int64) {
		t.Helper()
		if idle, used, _ := byState(collectCounts(t, reader, "delta:6379_")); idle != wantIdle || used != wantUsed {
			t.Fatalf("%s: delta idle=%d used=%d, want %d and %d", step, idle, used, wantIdle, wantUsed)
		}
	}
	check("first collection", 3, 0)
	check("no change", 0, 0)
	held := c.Conn()
	if err := held.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	waitConns(t, c, 4) // MinIdleConns replaced the held conn
	check("conn held", 0, 1)
	_ = held.Close()
	check("conn released", 1, -1)
}

// A client created before Init is counted once Init runs, Shutdown/Init
// cycles give exact values, custom attributes are present, and Close and
// Shutdown leave the client's series at 0.
func TestConnectionCountClientLifecycle(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	const addr = "lifecycle:6390"
	c := newMemClient(t, addr, nil)

	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	inst := GetObservabilityInstance()
	cfg := NewConfig().WithEnabled(true).WithMeterProvider(provider).
		WithCustomAttributes(attribute.String("team", "platform"))

	for cycle := 0; cycle < 5; cycle++ {
		if err := inst.Init(cfg); err != nil {
			t.Fatalf("Init %d: %v", cycle, err)
		}
		counts := collectCounts(t, reader, addr+"_")
		idle, used, pubSub := byState(counts)
		if idle != 3 || used != 0 || pubSub != 0 {
			t.Fatalf("cycle %d after Init: idle=%d used=%d pubsub=%d, want 3, 0, 0 (%v)", cycle, idle, used, pubSub, counts)
		}
		// Like the synchronous counter, a series appears with its first
		// non-zero value: no used or pubsub series yet.
		if len(counts) != 1 {
			t.Fatalf("cycle %d: %d series, want 1 (main idle): %v", cycle, len(counts), counts)
		}
		if err := inst.Shutdown(); err != nil {
			t.Fatalf("Shutdown %d: %v", cycle, err)
		}
		if got := collectCounts(t, reader, addr+"_"); len(got) != 1 || got[countKey{pool: poolName(got), state: "idle"}] != 0 {
			t.Fatalf("cycle %d after Shutdown: %v, want the idle series at 0", cycle, got)
		}
	}

	if err := inst.Init(cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = inst.Shutdown() })
	sets := collectAttrs(t, reader, MetricConnectionCount)
	if len(sets) == 0 {
		t.Fatal("no connection.count series")
	}
	for _, set := range sets {
		requireAttr(t, set, "team", "platform")
		requireAttr(t, set, AttrDBSystemName, DBSystemRedis)
		requireAttr(t, set, AttrServerAddress, "lifecycle")
		requireAttr(t, set, AttrServerPort, "6390")
	}
	_ = c.Close()
	if got := collectCounts(t, reader, addr+"_"); len(got) != 1 || got[countKey{pool: poolName(got), state: "idle"}] != 0 {
		t.Fatalf("after Close: %v, want the idle series at 0", got)
	}
}

// poolName returns the pool name of the first series in counts.
func poolName(counts map[countKey]int64) string {
	for k := range counts {
		return k.pool
	}
	return ""
}

// A client dropped without Close is observed at 0 once it is collected, and
// the recorder does not keep it alive.
func TestConnectionCountDropsCollectedClients(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	reader := metric.NewManualReader()
	initOn(t, reader, NewConfig())
	func() {
		c := redis.NewClient(memClientOptions("dropped:6379", nil))
		waitConns(t, c, 3)
	}()
	if idle, _, _ := byState(collectCounts(t, reader, "dropped:6379_")); idle != 3 {
		t.Fatalf("before GC: idle=%d, want 3", idle)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		runtime.GC()
		got := collectCounts(t, reader, "dropped:6379_")
		if idle, _, _ := byState(got); idle == 0 && len(got) == 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("a collected client is not observed at 0")
}

// A per-client recorder observes its own client's pools, with its own
// attributes, and the global recorder does not.
func TestConnectionCountPerClientRecorder(t *testing.T) {
	inst, reader := initForTest(t, attribute.String("tenant", "global"))
	rec, err := inst.NewRecorder(NewConfig().WithCustomAttributes(attribute.String("tenant", "acme")))
	if err != nil {
		t.Fatalf("NewRecorder: %v", err)
	}
	newMemClient(t, "own:6379", rec)
	newMemClient(t, "shared:6379", nil)

	tenants := map[string]map[string]bool{}
	for _, set := range collectAttrs(t, reader, MetricConnectionCount) {
		name, _ := set.Value(AttrDBClientConnectionPoolName)
		tenant, _ := set.Value("tenant")
		host := strings.SplitN(name.AsString(), ":", 2)[0]
		if tenants[host] == nil {
			tenants[host] = map[string]bool{}
		}
		tenants[host][tenant.AsString()] = true
	}
	if len(tenants["own"]) != 1 || !tenants["own"]["acme"] {
		t.Fatalf("own client observed with tenants %v, want only acme", tenants["own"])
	}
	if len(tenants["shared"]) != 1 || !tenants["shared"]["global"] {
		t.Fatalf("shared client observed with tenants %v, want only global", tenants["shared"])
	}
	if idle, _, _ := byState(collectCounts(t, reader, "own:6379_")); idle != 3 {
		t.Fatalf("per-client idle = %d, want 3", idle)
	}
}

// With only the ConnectionBasic group on, a command allocates as much as
// with metrics off: getting and returning a connection records nothing.
func TestConnectionCountNoWorkOnGetPut(t *testing.T) {
	ctx := context.Background()
	allocs := func(c *redis.Client) float64 {
		return testing.AllocsPerRun(2000, func() { _ = c.Ping(ctx).Err() })
	}
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	off := allocs(newMemClient(t, "allocs-off:6379", nil))

	reader := metric.NewManualReader()
	initOn(t, reader, NewConfig().WithMetricGroups(MetricGroupFlagConnectionBasic))
	c := newMemClient(t, "allocs-on:6379", nil)
	on := allocs(c)
	if on != off {
		t.Fatalf("Ping allocs with connection.count on = %v, off = %v; want equal", on, off)
	}
	want := int64(c.PoolStats().IdleConns)
	if idle, used, _ := byState(collectCounts(t, reader, "allocs-on:6379_")); idle != want || used != 0 {
		t.Fatalf("after Pings: idle=%d used=%d, want %d and 0", idle, used, want)
	}
}

// A consumer that adds up delta points must get back to 0 when a client
// closes and when Shutdown runs, and a cumulative reader must see 0. The SDK
// emits no point for a series its callback stops observing, so closed series
// have to be observed at 0.
func TestConnectionCountDeltaSumReturnsToZero(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	delta := metric.NewManualReader(metric.WithTemporalitySelector(func(metric.InstrumentKind) metricdata.Temporality {
		return metricdata.DeltaTemporality
	}))
	cumulative := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(delta), metric.WithReader(cumulative))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	inst := GetObservabilityInstance()
	cfg := NewConfig().WithEnabled(true).WithMeterProvider(provider)
	if err := inst.Init(cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = inst.Shutdown() })

	sums := map[countKey]int64{}
	check := func(step string, wantIdle, wantUsed int64) {
		t.Helper()
		for k, v := range collectCounts(t, delta, "sum:6379_") {
			sums[k] += v
		}
		cur := collectCounts(t, cumulative, "sum:6379_")
		if idle, used, _ := byState(sums); idle != wantIdle || used != wantUsed {
			t.Fatalf("%s: summed delta idle=%d used=%d, want %d and %d", step, idle, used, wantIdle, wantUsed)
		}
		for k, v := range sums {
			if cur[k] != v {
				t.Fatalf("%s: cumulative %v, want the summed deltas %v", step, cur, sums)
			}
		}
	}

	c := redis.NewClient(memClientOptions("sum:6379", nil))
	waitConns(t, c, 3)
	held := c.Conn()
	if err := held.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	waitConns(t, c, 4)
	check("conn held", 3, 1)
	_ = held.Close()
	_ = c.Close()
	check("after Close", 0, 0)
	check("after Close, no change", 0, 0)

	c2 := redis.NewClient(memClientOptions("sum:6379", nil))
	t.Cleanup(func() { _ = c2.Close() })
	waitConns(t, c2, 3)
	check("second client", 3, 0)
	if err := inst.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	check("after Shutdown", 0, 0)
	if err := inst.Init(cfg); err != nil {
		t.Fatalf("re-Init: %v", err)
	}
	check("after re-Init", 3, 0)
}

// Shutdown and Init back to back, with no collection in between, while a conn
// is held: the used series keeps its exported state across the new recorder,
// so a delta reader still gets the decrement when the conn is released.
func TestConnectionCountDeltaSumAcrossReinit(t *testing.T) {
	resetObservabilityForTest()
	t.Cleanup(resetObservabilityForTest)
	delta := metric.NewManualReader(metric.WithTemporalitySelector(func(metric.InstrumentKind) metricdata.Temporality {
		return metricdata.DeltaTemporality
	}))
	provider := metric.NewMeterProvider(metric.WithReader(delta))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })
	inst := GetObservabilityInstance()
	cfg := NewConfig().WithEnabled(true).WithMeterProvider(provider)
	if err := inst.Init(cfg); err != nil {
		t.Fatalf("Init: %v", err)
	}
	t.Cleanup(func() { _ = inst.Shutdown() })
	c := newMemClient(t, "reinit:6379", nil)

	sums := map[countKey]int64{}
	collect := func() {
		for k, v := range collectCounts(t, delta, "reinit:6379_") {
			sums[k] += v
		}
	}
	held := c.Conn()
	if err := held.Ping(context.Background()).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
	waitConns(t, c, 4)
	collect()
	if err := inst.Shutdown(); err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	if err := inst.Init(cfg); err != nil {
		t.Fatalf("re-Init: %v", err)
	}
	_ = held.Close()
	collect()
	if idle, used, _ := byState(sums); idle != 4 || used != 0 {
		t.Fatalf("summed delta idle=%d used=%d, want 4 and 0", idle, used)
	}
}
