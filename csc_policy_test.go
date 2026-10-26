package redis

import (
	"context"
	"fmt"
	"net"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
)

func cscTestContext(ctx context.Context, cache bool) context.Context {
	if cache {
		return WithCache(ctx)
	}
	return WithoutCache(ctx)
}

type cscPolicyLogs struct {
	mu       sync.Mutex
	messages []string
}

func captureCSCPolicyLogs(t *testing.T) *cscPolicyLogs {
	t.Helper()
	old := internal.Logger.Load()
	logs := new(cscPolicyLogs)
	SetLogger(logs)
	t.Cleanup(func() { SetLogger(old) })
	return logs
}

func (l *cscPolicyLogs) Printf(_ context.Context, format string, args ...interface{}) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.messages = append(l.messages, fmt.Sprintf(format, args...))
}

func (l *cscPolicyLogs) contains(text string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, message := range l.messages {
		if strings.Contains(message, text) {
			return true
		}
	}
	return false
}

// Observe internal command intent at the public hook boundary. These commands
// must stay unmarked even though unsupported marks no longer produce errors.
type cscInternalIntentHook struct {
	leaked atomic.Bool
}

func (*cscInternalIntentHook) DialHook(next DialHook) DialHook { return next }
func (*cscInternalIntentHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

func (h *cscInternalIntentHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if (cmd.Name() == "unwatch" || cmd.Name() == "cluster") && cmd.cacheState().mark != cacheUnmarked {
			h.leaked.Store(true)
		}
		return next(ctx, cmd)
	}
}

type cscPolicyEvent struct {
	conn int
	args []string
}
type cscPolicyServer struct {
	mu     sync.Mutex
	events []cscPolicyEvent
	addr   string
}

func newCSCPolicyServer(t testing.TB, reply func([]string) string) *cscPolicyServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &cscPolicyServer{addr: ln.Addr().String()}
	var mu sync.Mutex
	var conns []net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			cn, e := ln.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			conns = append(conns, cn)
			id := len(conns)
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer cn.Close()
				rd := proto.NewReader(cn)
				for {
					values, e := rd.ReadSlice()
					if e != nil {
						return
					}
					args := make([]string, len(values))
					for i, v := range values {
						args[i] = fmt.Sprint(v)
					}
					s.mu.Lock()
					s.events = append(s.events, cscPolicyEvent{id, args})
					s.mu.Unlock()
					response := ""
					if reply != nil {
						response = reply(args)
					}
					if response == "DROP" {
						return
					}
					if response == "" {
						response = cscPolicyReply(args)
					}
					if _, e := cn.Write([]byte(response)); e != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, cn := range conns {
			_ = cn.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return s
}

func cscPolicyReply(args []string) string {
	switch strings.ToLower(args[0]) {
	case "hello":
		return "%0\r\n"
	case "get":
		return "$1\r\nv\r\n"
	case "touch":
		return ":1\r\n"
	case "mget":
		return "*" + strconv.Itoa(len(args)-1) + "\r\n" + strings.Repeat("$1\r\nv\r\n", len(args)-1)
	case "command":
		return "*0\r\n"
	default:
		return "+OK\r\n"
	}
}

func (s *cscPolicyServer) snapshot() []cscPolicyEvent {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]cscPolicyEvent(nil), s.events...)
}

func (s *cscPolicyServer) count(name string) int {
	n := 0
	for _, e := range s.snapshot() {
		if strings.EqualFold(e.args[0], name) {
			n++
		}
	}
	return n
}

func (s *cscPolicyServer) prefixes() int {
	n := 0
	for _, e := range s.snapshot() {
		if len(e.args) > 1 && strings.EqualFold(e.args[0], "client") && strings.EqualFold(e.args[1], "caching") {
			n++
		}
	}
	return n
}

func (s *cscPolicyServer) assertPairs(t testing.TB) {
	t.Helper()
	last := map[int][]string{}
	for _, e := range s.snapshot() {
		prior := last[e.conn]
		if len(prior) > 1 && strings.EqualFold(prior[0], "client") && strings.EqualFold(prior[1], "caching") {
			if strings.EqualFold(e.args[0], "client") || strings.EqualFold(e.args[0], "ping") {
				t.Errorf("interleaved pair %v -> %v", prior, e.args)
			}
		}
		last[e.conn] = e.args
	}
	for _, args := range last {
		if len(args) > 1 && strings.EqualFold(args[0], "client") && strings.EqualFold(args[1], "caching") {
			t.Errorf("stranded prelude %v", args)
		}
	}
}

func cscPolicyOptions(addr string, mode TrackingMode) *Options {
	return &Options{Addr: addr, Protocol: 3, DisableIdentity: true, MaxRetries: -1, PoolSize: 4, ReadTimeout: time.Second, WriteTimeout: time.Second, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 128, TrackingMode: mode, DrainInterval: time.Hour}, MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}}
}

func policyClient(t testing.TB, opt *Options) *Client {
	t.Helper()
	c := NewClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestCSCPolicyIntent(t *testing.T) {
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		for _, mark := range []int{-1, 0, 1} {
			for _, pred := range []int{-1, 0, 1} {
				t.Run(fmt.Sprintf("%s/mark%d/pred%d", mode, mark, pred), func(t *testing.T) {
					s := newCSCPolicyServer(t, nil)
					opt := cscPolicyOptions(s.addr, mode)
					var calls atomic.Int32
					if pred >= 0 {
						opt.ClientSideCacheConfig.Cacheable = func(name string, keys []string) bool {
							calls.Add(1)
							if name != "get" || !reflect.DeepEqual(keys, []string{"user:42"}) {
								t.Errorf("predicate input %s %v", name, keys)
							}
							return pred == 1
						}
					}
					c := policyClient(t, opt)
					cached := mode != TrackingModeOptIn
					if pred >= 0 {
						cached = pred == 1
					}
					if mark >= 0 {
						cached = mark == 1
					}
					for i := 0; i < 2; i++ {
						var cmd *StringCmd
						if mark < 0 {
							cmd = c.Get(context.Background(), "user:42")
						} else {
							cmd = c.Get(cscTestContext(context.Background(), mark == 1), "user:42")
						}
						if v, e := cmd.Result(); e != nil || v != "v" {
							t.Fatalf("%q %v", v, e)
						}
					}
					want := 2
					if cached {
						want = 1
					}
					if s.count("get") != want {
						t.Fatalf("GETs=%d want%d", s.count("get"), want)
					}
					wantPred := int32(0)
					if pred >= 0 && mark < 0 {
						wantPred = 2
					}
					if calls.Load() != wantPred {
						t.Errorf("predicate calls %d", calls.Load())
					}
					s.assertPairs(t)
					wantPrefixes := 0
					if mode == TrackingModeOptIn && cached {
						wantPrefixes = 1
					}
					if mode == TrackingModeOptOut && !cached {
						wantPrefixes = 2
					}
					if s.prefixes() != wantPrefixes {
						t.Errorf("prefixes %d want %d", s.prefixes(), wantPrefixes)
					}
					for _, e := range s.snapshot() {
						if len(e.args) >= 3 && strings.EqualFold(e.args[0], "client") && strings.EqualFold(e.args[1], "tracking") {
							text := strings.ToLower(strings.Join(e.args, " "))
							if mode == TrackingModeOptIn && !strings.Contains(text, "optin") {
								t.Error(text)
							}
							if mode == TrackingModeOptOut && !strings.Contains(text, "optout") {
								t.Error(text)
							}
						}
					}
				})
			}
		}
	}
}

type cscZeroTokenCache struct {
	Cache
	tokens sync.Map
}

func (z *cscZeroTokenCache) Reserve(key string, keys []string) (uint64, bool) {
	token, fetch := z.Cache.Reserve(key, keys)
	if fetch {
		z.tokens.Store(key, token)
		return 0, true
	}
	return token, false
}

func (z *cscZeroTokenCache) Cancel(key string, token uint64) bool {
	if token == 0 {
		if v, ok := z.tokens.Load(key); ok {
			token = v.(uint64)
		}
	}
	return z.Cache.Cancel(key, token)
}

func (z *cscZeroTokenCache) FulfillOwned(key string, token, id uint64, raw []byte) bool {
	if token == 0 {
		if v, ok := z.tokens.Load(key); ok {
			token = v.(uint64)
		}
	}
	return z.Cache.FulfillOwned(key, token, id, raw)
}

func TestCSCPolicyFrozenAndInvalidConfig(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
	c := policyClient(t, opt)
	opt.ClientSideCacheConfig.TrackingMode = TrackingModeOptOut
	opt.ClientSideCacheConfig.Cacheable = func(string, []string) bool { t.Error("mutated callback used"); return true }
	if err := c.Get(WithCache(context.Background()), "k").Err(); err != nil {
		t.Fatal(err)
	}
	if s.prefixes() != 1 {
		t.Fatal("mode mutated")
	}
	bad := cscPolicyOptions(s.addr, TrackingMode("invalid"))
	invalid := policyClient(t, bad)
	before := len(s.snapshot())
	if err := invalid.Get(WithCache(context.Background()), "invalid").Err(); err == nil {
		t.Fatal("invalid mode accepted")
	}
	if len(s.snapshot()) != before {
		t.Fatal("invalid mode used network")
	}
}

// A mark belongs to the submitted call, not the command's construction context
// or an earlier execution. Nested marks override without modifying their parent.
func TestCSCPolicyContextScope(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	c := policyClient(t, cscPolicyOptions(s.addr, TrackingModeOptIn))
	type contextKey struct{}
	deadline := time.Now().Add(time.Minute)
	parent, cancel := context.WithDeadline(context.WithValue(context.Background(), contextKey{}, "value"), deadline)
	defer cancel()
	cached := WithCache(parent)
	bypassed := WithoutCache(cached)
	contexts := []context.Context{cached, bypassed, WithCache(bypassed), parent}
	for _, ctx := range contexts {
		if d, ok := ctx.Deadline(); !ok || !d.Equal(deadline) || ctx.Value(contextKey{}) != "value" || ctx.Done() != parent.Done() {
			t.Fatal("cache context did not inherit its parent")
		}
	}
	cmd := NewStringCmd(cached, "get", "k")
	for i, ctx := range []context.Context{cached, cached, bypassed, WithCache(bypassed), parent} {
		if err := c.Process(ctx, cmd); err != nil {
			t.Fatal(err)
		}
		if *cmd.cacheState() != (cscCommandState{}) {
			t.Fatal("invocation state survived completion", i)
		}
	}
	if s.count("get") != 3 || s.prefixes() != 1 || c.CSCStats().Hits != 2 {
		t.Fatal("context scope or command reuse changed intent", s.snapshot(), c.CSCStats())
	}
	// Sharing a context does not share mutable command state.
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			if err := c.Get(cached, "k").Err(); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if s.count("get") != 3 || c.CSCStats().Hits != 10 {
		t.Fatal("concurrent context reuse lost its mark", c.CSCStats())
	}
	cancel()
	for _, ctx := range contexts {
		if ctx.Err() != context.Canceled {
			t.Fatal("cache context lost cancellation")
		}
	}
}

type cscCloneHook struct {
	clone Cmder
	after bool
}

func (*cscCloneHook) DialHook(next DialHook) DialHook { return next }

func (*cscCloneHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

func (h *cscCloneHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		capture := h.clone == nil && cmd.Name() == "get"
		if capture && !h.after {
			h.clone = cmd.Clone()
		}
		err := next(ctx, cmd)
		if capture && h.after {
			h.clone = cmd.Clone()
		}
		return err
	}
}

func TestCSCPolicyContextOverridesClone(t *testing.T) {
	for _, originalCache := range []bool{false, true} {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("cache=%t/after=%t", originalCache, after), func(t *testing.T) {
				var reads atomic.Int32
				s := newCSCPolicyServer(t, func(args []string) string {
					if args[0] == "get" {
						return fmt.Sprintf("$1\r\n%d\r\n", reads.Add(1))
					}
					return ""
				})
				c := policyClient(t, cscPolicyOptions(s.addr, TrackingModeOptIn))
				hook := &cscCloneHook{after: after}
				c.AddHook(hook)
				ctx := context.Background()
				if got, err := c.Get(cscTestContext(ctx, originalCache), "k").Result(); err != nil || got != "1" {
					t.Fatal(got, err)
				}
				// The clone is now owned by a new invocation. Its explicit mark
				// must override both the copied mark and any resolved intent.
				if err := c.Process(cscTestContext(ctx, !originalCache), hook.clone); err != nil {
					t.Fatal(err)
				}
				if got := hook.clone.(*StringCmd).Val(); got != "2" {
					t.Fatalf("new invocation reused the clone's cache decision: %q", got)
				}
				wantCached := "2"
				if originalCache {
					wantCached = "1" // Bypass must leave the original entry intact.
				}
				if got, err := c.Get(WithCache(ctx), "k").Result(); err != nil || got != wantCached {
					t.Fatal("new invocation did not apply its cache policy", got, err)
				}
				if reads.Load() != 2 || s.prefixes() != 1 {
					t.Fatal("unexpected fetch or tracking count", s.snapshot())
				}
			})
		}
	}
}

func TestCSCPolicyPipelineCloneIntent(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		for _, mark := range []string{"unmarked", "cache", "bypass"} {
			for _, discard := range []bool{false, true} {
				t.Run(fmt.Sprintf("cluster=%t/mark=%s/discard=%t", cluster, mark, discard), func(t *testing.T) {
					s := newCSCPolicyServer(t, nil)
					var c interface {
						UniversalClient
						CSCStats() CSCStats
					}
					if cluster {
						c = cscPolicyCluster(t, s.addr, s.addr, TrackingModeOptIn, false)
					} else {
						c = policyClient(t, cscPolicyOptions(s.addr, TrackingModeOptIn))
					}
					hook := &cscCloneHook{after: true}
					c.AddHook(hook)
					ctx := context.Background()
					if err := c.Get(WithCache(ctx), "{one}:key").Err(); err != nil {
						t.Fatal(err)
					}
					submitCtx := ctx
					switch mark {
					case "cache":
						submitCtx = WithCache(ctx)
					case "bypass":
						submitCtx = WithoutCache(ctx)
					}
					clones := []Cmder{hook.clone, hook.clone.Clone()}
					pipe := c.Pipeline()
					if err := pipe.BatchProcess(submitCtx, clones...); err != nil {
						t.Fatal(err)
					}
					wantReads := 5 // Initial fill, two pipeline reads, two unmarked reads.
					if discard {
						pipe.Discard()
						wantReads = 3
					} else if _, err := pipe.Exec(ctx); err != nil {
						t.Fatal(err)
					}
					// The public pipeline owns a new invocation, even when it is
					// discarded. Its clones must not carry earlier cache intent
					// into a later unmarked OPTIN call.
					for _, clone := range clones {
						if err := c.Process(ctx, clone); err != nil {
							t.Fatal(err)
						}
					}
					if s.count("get") != wantReads || s.prefixes() != 1 || c.CSCStats().Hits != 0 || c.CSCStats().Entries != 1 {
						t.Fatal("pipeline retained clone intent or changed the cached entry", s.snapshot(), c.CSCStats())
					}
				})
			}
		}
	}
}

func TestCSCPolicyContextPublicPipeline(t *testing.T) {
	logs := captureCSCPolicyLogs(t)
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprintf("cluster=%t", cluster), func(t *testing.T) {
			s := newCSCPolicyServer(t, nil)
			var c interface {
				UniversalClient
				CSCStats() CSCStats
			}
			if cluster {
				c = cscPolicyCluster(t, s.addr, s.addr, TrackingModeOptIn, false)
			} else {
				c = policyClient(t, cscPolicyOptions(s.addr, TrackingModeOptIn))
			}
			ctx := context.Background()
			pipe := c.Pipeline()
			marked := pipe.Get(WithCache(ctx), "{one}:marked")
			bypass := pipe.Get(WithoutCache(ctx), "{one}:bypass")
			normal := pipe.Get(ctx, "{one}:normal")
			if marked.Err() != nil || pipe.Len() != 3 {
				t.Fatal("mark changed pipeline submission", marked.Err(), pipe.Len())
			}
			cmds, err := pipe.Exec(WithCache(ctx))
			if err != nil || len(cmds) != 3 || marked.Val() != "v" || bypass.Val() != "v" || normal.Val() != "v" {
				t.Fatal("context changed pipeline execution", err, len(cmds))
			}
			if s.count("get") != 3 || s.prefixes() != 0 || c.CSCStats().Entries != 0 {
				t.Fatal("pipeline cached or failed to dispatch a command", s.snapshot())
			}
		})
	}
	if logs.contains("cannot cache") {
		t.Fatal("pipeline mark produced a misuse warning")
	}
}

func BenchmarkCSCPolicyHit(b *testing.B) {
	for _, predicate := range []bool{false, true} {
		b.Run(fmt.Sprintf("predicate=%t", predicate), func(b *testing.B) {
			s := newCSCPolicyServer(b, nil)
			opt := cscPolicyOptions(s.addr, TrackingModePlain)
			if predicate {
				opt.ClientSideCacheConfig.Cacheable = func(_ string, keys []string) bool { return len(keys) == 1 }
			}
			c := policyClient(b, opt)
			ctx := context.Background()
			_ = c.Get(ctx, "k").Err()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				_ = c.Get(ctx, "k").Err()
			}
		})
	}
}

func mustCSCWire(t testing.TB, args ...interface{}) []byte {
	t.Helper()
	var b strings.Builder
	if err := writeCmd(proto.NewWriter(&b), NewCmd(context.Background(), args...)); err != nil {
		t.Fatal(err)
	}
	return []byte(b.String())
}

// CSC tracking modes: OPTIN/OPTOUT policy and integration.

func TestCSCPolicyConstructorForwarding(t *testing.T) {
	cfg := &ClientSideCacheConfig{TrackingMode: TrackingModeOptOut, Cacheable: func(string, []string) bool { return false }}
	u := &UniversalOptions{Addrs: []string{"localhost:1"}, ClientSideCacheConfig: cfg}
	for name, opt := range map[string]*Options{"simple": u.Simple(), "cluster": u.Cluster().clientOptions(), "failoverCluster": u.Failover().clusterOptions().clientOptions()} {
		p := cachePolicy(opt.ClientSideCacheConfig)
		if p.mode != TrackingModeOptOut || p.cacheable == nil {
			t.Fatal(name)
		}
	}
}

func TestCSCPolicyBorrowedConnectionMode(t *testing.T) {
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		for _, path := range []string{"direct", "Conn", "EphemeralConn", "Watch"} {
			t.Run(string(mode)+"/"+path, func(t *testing.T) {
				s := newCSCPolicyServer(t, nil)
				opt := cscPolicyOptions(s.addr, mode)
				opt.PoolSize = 1
				c := policyClient(t, opt)
				ctx := context.Background()
				switch path {
				case "Conn", "EphemeralConn":
					var cn *Conn
					if path == "Conn" {
						cn = c.Conn()
					} else {
						cn = c.EphemeralConn()
					}
					if err := cn.Ping(ctx).Err(); err != nil {
						t.Fatal(err)
					}
					if err := cn.Close(); err != nil {
						t.Fatal(err)
					}
				case "Watch":
					if err := c.Watch(ctx, func(tx *Tx) error { return tx.Ping(ctx).Err() }); err != nil {
						t.Fatal(err)
					}
				}
				for i := 0; i < 2; i++ {
					if got, err := c.Get(WithCache(ctx), "k").Result(); got != "v" || err != nil {
						t.Fatal(got, err)
					}
				}
				want := []string{"client", "tracking", "on"}
				if mode != TrackingModePlain {
					want = append(want, string(mode))
				}
				for _, event := range s.snapshot() {
					args := strings.Fields(strings.ToLower(strings.Join(event.args, " ")))
					if len(args) >= 2 && args[0] == "client" && args[1] == "tracking" && !reflect.DeepEqual(args, want) {
						t.Fatalf("borrowed connection handshake %v; want %v", args, want)
					}
				}
				if s.count("get") != 1 || c.CSCStats().Hits != 1 {
					t.Fatal("parent did not reuse tracked cache entry", c.CSCStats())
				}
			})
		}
	}
}

type cscPolicyMarshaledKey struct{ calls atomic.Int32 }

func (k *cscPolicyMarshaledKey) MarshalBinary() ([]byte, error) {
	k.calls.Add(1)
	return []byte("key"), nil
}

func TestCSCPolicyOptOutDoesNotProbeMarshaler(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	c := policyClient(t, cscPolicyOptions(s.addr, TrackingModeOptOut))
	key := &cscPolicyMarshaledKey{}
	if got, err := c.Do(WithoutCache(context.Background()), "get", key).Text(); got != "v" || err != nil {
		t.Fatal(got, err)
	}
	if key.calls.Load() != 1 || s.prefixes() != 0 {
		t.Fatal("unknown wire key was probed or classified", key.calls.Load(), s.snapshot())
	}
}

func TestCSCPolicyOptOutWireKeys(t *testing.T) {
	key := "key"
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprint(cluster), func(t *testing.T) {
			s := newCSCPolicyServer(t, nil)
			var c UniversalClient
			if cluster {
				c = cscPolicyCluster(t, s.addr, s.addr, TrackingModeOptOut, false)
			} else {
				c = policyClient(t, cscPolicyOptions(s.addr, TrackingModeOptOut))
			}
			ctx := context.Background()
			if err := c.Get(WithCache(ctx), key).Err(); err != nil {
				t.Fatal(err)
			}
			for _, arg := range []interface{}{key, []byte(key), &key} {
				before := s.prefixes()
				if got, err := c.Do(WithoutCache(ctx), "get", arg).Text(); got != "v" || err != nil {
					t.Fatal(got, err)
				}
				if s.prefixes() != before+1 {
					t.Fatalf("%T bypass omitted CACHING NO: %v", arg, s.snapshot())
				}
			}
			before := s.count("get")
			if err := c.Get(WithCache(ctx), key).Err(); err != nil {
				t.Fatal(err)
			}
			if s.count("get") != before {
				t.Fatal("bypass removed existing entry")
			}
			s.assertPairs(t)
		})
	}
}

func TestCSCPolicyBorrowedOptOutRead(t *testing.T) {
	for _, warm := range []bool{false, true} {
		t.Run(fmt.Sprint(warm), func(t *testing.T) {
			s := newCSCPolicyServer(t, nil)
			c := policyClient(t, cscPolicyOptions(s.addr, TrackingModeOptOut))
			ctx := context.Background()
			if warm {
				if err := c.Get(ctx, "k").Err(); err != nil {
					t.Fatal(err)
				}
			}
			cn := c.Conn()
			for i := 0; i < 2; i++ {
				if err := cn.Get(ctx, "k").Err(); err != nil {
					t.Fatal(err)
				}
			}
			if err := cn.Close(); err != nil {
				t.Fatal(err)
			}
			if s.prefixes() != 2 {
				t.Fatal("tracked uncached Conn reads omitted NO", s.snapshot())
			}
			// Claiming a sticky connection intentionally revokes its old cache
			// entries because it leaves the parent's drainer (independent of NO).
			// On release the parent must be able to fill and hit again.
			before := s.count("get")
			for i := 0; i < 2; i++ {
				if err := c.Get(ctx, "k").Err(); err != nil {
					t.Fatal(err)
				}
			}
			if s.count("get") != before+1 || c.CSCStats().Hits != 1 {
				t.Fatal("parent failed to regain cache coverage", c.CSCStats())
			}
			s.assertPairs(t)
		})
	}
}

func TestCSCPolicyBypassPreservesCachedEntry(t *testing.T) {
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		t.Run(string(mode), func(t *testing.T) {
			var n atomic.Int32
			s := newCSCPolicyServer(t, func(args []string) string {
				if strings.EqualFold(args[0], "get") {
					return fmt.Sprintf("$1\r\n%d\r\n", n.Add(1))
				}
				return ""
			})
			c := policyClient(t, cscPolicyOptions(s.addr, mode))
			ctx := context.Background()
			if v, _ := c.Get(WithCache(ctx), "k").Result(); v != "1" {
				t.Fatal(v)
			}
			before := c.CSCStats()
			if v, _ := c.Get(WithoutCache(ctx), "k").Result(); v != "2" {
				t.Fatal(v)
			}
			after := c.CSCStats()
			if before.Entries != after.Entries || before.Hits != after.Hits {
				t.Fatal("bypass touched cache")
			}
			if v, _ := c.Get(WithCache(ctx), "k").Result(); v != "1" {
				t.Fatal("entry was refreshed", v)
			}
			if n.Load() != 2 {
				t.Fatal(n.Load())
			}
		})
	}
}

func TestCSCPolicyUnavailableTracking(t *testing.T) {
	for _, coalesce := range []bool{false, true} {
		t.Run(strconv.FormatBool(coalesce), func(t *testing.T) {
			logs := captureCSCPolicyLogs(t)
			s := newCSCPolicyServer(t, func(a []string) string {
				if len(a) > 1 && strings.EqualFold(a[1], "tracking") {
					return "-NOPERM tracking\r\n"
				}
				return ""
			})
			opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
			opt.ClientSideCacheCoalesceMisses = coalesce
			c := policyClient(t, opt)
			for i := 0; i < 2; i++ {
				if got, err := c.Get(WithCache(context.Background()), "k").Result(); err != nil || got != "v" {
					t.Fatal("unavailable tracking changed the read", got, err)
				}
			}
			if s.count("get") != 2 || s.prefixes() != 0 || c.CSCStats().Entries != 0 {
				t.Fatal("untracked response was cached", s.snapshot())
			}
			if !logs.contains("NOPERM tracking") || logs.contains("cannot cache") {
				t.Fatal("tracking diagnostic lost or replaced by misuse warning")
			}
		})
	}
}

func TestCSCPolicyEligibilityAndGuards(t *testing.T) {
	logs := captureCSCPolicyLogs(t)
	s := newCSCPolicyServer(t, func(args []string) string {
		if args[0] == "touch" && args[1] == "denied" {
			return "-NOPERM touch\r\n"
		}
		return ""
	})
	opt := cscPolicyOptions(s.addr, TrackingModeOptOut)
	var calls atomic.Int32
	opt.ClientSideCacheConfig.Cacheable = func(string, []string) bool { calls.Add(1); return true }
	c := policyClient(t, opt)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if got, err := c.Touch(WithCache(ctx), "k").Result(); err != nil || got != 1 {
			t.Fatal("ineligible mark changed the result", got, err)
		}
	}
	if s.count("touch") != 2 || calls.Load() != 0 || c.CSCStats().Entries != 0 || logs.contains("cannot cache") {
		t.Fatal("ineligible mark cached, ran predicate or warned", s.snapshot())
	}
	if err := c.Touch(ctx, "k").Err(); err != nil {
		t.Fatal(err)
	}
	if s.prefixes() != 3 {
		t.Fatal("trackable ineligible command did not receive NO")
	}
	if err := c.Touch(WithCache(ctx), "denied").Err(); err == nil || err.Error() != "NOPERM touch" {
		t.Fatal("real command error was suppressed", err)
	}
	for _, sub := range []string{"TRACKING", "CACHING"} {
		if err := c.Do(WithCache(ctx), []byte("CLIENT"), []byte(sub), "YES").Err(); err == nil {
			t.Fatal(sub)
		}
	}
}

func TestCSCPolicyControlFailures(t *testing.T) {
	for _, coalesce := range []bool{false, true} {
		for _, response := range []string{"-NOPERM caching\r\n", "+BAD\r\n", ":1\r\n", "*1\r\n-ERR nested\r\n", "DROP"} {
			t.Run(fmt.Sprintf("%t/%q", coalesce, response), func(t *testing.T) {
				logs := captureCSCPolicyLogs(t)
				s := newCSCPolicyServer(t, func(a []string) string {
					if len(a) > 1 && strings.EqualFold(a[1], "caching") {
						return response
					}
					return ""
				})
				opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
				opt.ClientSideCacheCoalesceMisses = coalesce
				c := policyClient(t, opt)
				for i := 0; i < 2; i++ {
					v, err := c.Get(WithCache(context.Background()), "k").Result()
					if strings.HasPrefix(response, "-NOPERM") {
						if err != nil || v != "v" {
							t.Fatalf("%q %v", v, err)
						}
					} else if err == nil {
						t.Fatal("protocol/transport failure succeeded")
					}
				}
				if c.CSCStats().Entries != 0 {
					t.Fatal("published untracked data")
				}
				if strings.HasPrefix(response, "-NOPERM") && !logs.contains("CLIENT CACHING rejected") {
					t.Fatal("CACHING rejection lost its operational diagnostic")
				}
			})
		}
	}
}

func TestCSCPolicyPushesBetweenReplies(t *testing.T) {
	for _, coalesce := range []bool{false, true} {
		t.Run(strconv.FormatBool(coalesce), func(t *testing.T) {
			push := ">2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nother\r\n"
			s := newCSCPolicyServer(t, func(a []string) string {
				if len(a) > 1 && strings.EqualFold(a[1], "caching") {
					return push + "+OK\r\n"
				}
				if strings.EqualFold(a[0], "get") {
					return push + "$1\r\nv\r\n"
				}
				return ""
			})
			opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
			opt.ClientSideCacheCoalesceMisses = coalesce
			c := policyClient(t, opt)
			for i := 0; i < 2; i++ {
				if v, e := c.Get(WithCache(context.Background()), "k").Result(); v != "v" || e != nil {
					t.Fatalf("%q %v", v, e)
				}
			}
			if s.count("get") != 1 {
				t.Fatal("not cached")
			}
		})
	}
}

func TestCSCPolicyBypassDoesNotJoinFetch(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var n atomic.Int32
	s := newCSCPolicyServer(t, func(a []string) string {
		if strings.EqualFold(a[0], "get") {
			i := n.Add(1)
			if i == 1 {
				close(started)
				<-release
			}
			return fmt.Sprintf("$1\r\n%d\r\n", i)
		}
		return ""
	})
	c := policyClient(t, cscPolicyOptions(s.addr, TrackingModeOptIn))
	ctx := context.Background()
	first := make(chan error, 1)
	go func() { first <- c.Get(WithCache(ctx), "k").Err() }()
	<-started
	done := make(chan string, 1)
	go func() { done <- c.Get(WithoutCache(ctx), "k").Val() }()
	select {
	case v := <-done:
		if v != "2" {
			t.Fatal(v)
		}
	case <-time.After(time.Second):
		t.Fatal("bypass joined reservation")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-first; err != nil {
		t.Fatal(err)
	}
}

func TestCSCPolicyCancelledPair(t *testing.T) {
	for _, coalesce := range []bool{false, true} {
		t.Run(strconv.FormatBool(coalesce), func(t *testing.T) {
			entered := make(chan struct{})
			release := make(chan struct{})
			var releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			var once sync.Once
			s := newCSCPolicyServer(t, func(a []string) string {
				if strings.EqualFold(a[0], "get") {
					once.Do(func() { close(entered) })
					<-release
				}
				return ""
			})
			opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
			opt.ContextTimeoutEnabled = true
			opt.ClientSideCacheCoalesceMisses = coalesce
			c := policyClient(t, opt)
			ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- c.Get(WithCache(ctx), "k").Err() }()
			select {
			case <-entered:
			case <-time.After(time.Second):
				t.Fatal("not written")
			}
			select {
			case err := <-done:
				if err == nil {
					t.Fatal("cancel succeeded")
				}
			case <-time.After(2 * time.Second):
				t.Fatal("cancel stuck")
			}
			releaseOnce.Do(func() { close(release) })
			waitCtx, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			_, _ = c.csc.Get(waitCtx, cscEntryKey(c.cscKeyPrefix, c.metadataView().cscFingerprint, string(mustCSCWire(t, "get", "k"))))
			if waitCtx.Err() != nil {
				t.Fatal("cancel left a reservation stuck")
			}
			if !coalesce && c.CSCStats().Entries != 0 {
				t.Fatal("cancel published")
			}
		})
	}
}

func TestCSCPolicyCustomCacheZeroToken(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
	opt.ClientSideCache = &cscZeroTokenCache{Cache: NewLocalCache(CacheConfig{MaxEntries: 32})}
	c := policyClient(t, opt)
	for i := 0; i < 2; i++ {
		if err := c.Get(WithCache(context.Background()), "k").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if s.count("get") != 1 {
		t.Fatal("zero token was treated as admission failure")
	}
}

func TestCSCPolicyAdmissionDeclined(t *testing.T) {
	for _, mode := range []TrackingMode{TrackingModeOptIn, TrackingModeOptOut} {
		t.Run(string(mode), func(t *testing.T) {
			s := newCSCPolicyServer(t, nil)
			opt := cscPolicyOptions(s.addr, mode)
			opt.ClientSideCacheConfig.MaxMemoryBytes = 1

			c := policyClient(t, opt)
			if got, err := c.Get(WithCache(context.Background()), "k").Result(); got != "v" || err != nil {
				t.Fatalf("admission became misuse %q %v", got, err)
			}
			want := 0
			if mode == TrackingModeOptOut {
				want = 1
			}
			if s.prefixes() != want {
				t.Fatalf("prefixes %d want %d", s.prefixes(), want)
			}
		})
	}
}

func TestCSCPolicyRetryPreservesReadError(t *testing.T) {
	for _, denyFlag := range []bool{false, true} {
		t.Run(strconv.FormatBool(denyFlag), func(t *testing.T) {
			var reads atomic.Int32
			s := newCSCPolicyServer(t, func(a []string) string {
				if len(a) > 1 && strings.EqualFold(a[1], "caching") && denyFlag {
					return "-NOPERM flag\r\n"
				}
				if strings.EqualFold(a[0], "get") && reads.Add(1) == 1 {
					return "-TRYAGAIN transient\r\n"
				}
				return ""
			})
			opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
			opt.MaxRetries = 1
			opt.MinRetryBackoff = -1
			opt.MaxRetryBackoff = -1
			c := policyClient(t, opt)
			if got, err := c.Get(WithCache(context.Background()), "k").Result(); got != "v" || err != nil {
				t.Fatalf("read error masked %q %v", got, err)
			}
			if reads.Load() != 2 || s.prefixes() != 2 {
				t.Fatal("retry did not repeat pair")
			}
			if denyFlag && c.CSCStats().Entries != 0 {
				t.Fatal("published without ACK")
			}
		})
	}
}

type cscPolicyEncoderFailure struct{}

func (cscPolicyEncoderFailure) MarshalBinary() ([]byte, error) {
	return nil, proto.RedisError("ERR encoder")
}

func TestCSCPolicyEncodingFailureRetiresPair(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := cscPolicyOptions(s.addr, TrackingModeOptOut)
	opt.PoolSize = 1
	c := policyClient(t, opt)
	ctx := context.Background()
	if err := c.Do(WithoutCache(ctx), "hget", "k", cscPolicyEncoderFailure{}).Err(); err == nil {
		t.Fatal("encoder error lost")
	}
	for i := 0; i < 2; i++ {
		if err := c.Get(ctx, "next").Err(); err != nil {
			t.Fatal(err)
		}
	}
	if s.count("hello") != 2 || s.count("get") != 1 {
		t.Fatalf("partial pair connection reused: hellos=%d GETs=%d", s.count("hello"), s.count("get"))
	}
}
