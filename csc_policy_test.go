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

	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
)

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
							cmd = c.WithCache(mark == 1).Get(context.Background(), "user:42")
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
	if err := c.WithCache(true).Get(context.Background(), "k").Err(); err != nil {
		t.Fatal(err)
	}
	if s.prefixes() != 1 {
		t.Fatal("mode mutated")
	}
	bad := cscPolicyOptions(s.addr, TrackingMode("invalid"))
	invalid := policyClient(t, bad)
	before := len(s.snapshot())
	if err := invalid.Get(context.Background(), "invalid").Err(); err == nil {
		t.Fatal("invalid mode accepted")
	}
	if len(s.snapshot()) != before {
		t.Fatal("invalid mode used network")
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
	cfg := &ClientSideCacheConfig{TrackingMode: TrackingModeOptOut, Strict: true, Cacheable: func(string, []string) bool { return false }}
	u := &UniversalOptions{Addrs: []string{"localhost:1"}, ClientSideCacheConfig: cfg}
	for name, opt := range map[string]*Options{"simple": u.Simple(), "cluster": u.Cluster().clientOptions(), "failoverCluster": u.Failover().clusterOptions().clientOptions()} {
		p := cachePolicy(opt.ClientSideCacheConfig)
		if p.mode != TrackingModeOptOut || !p.strict || p.cacheable == nil {
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
					if got, err := c.WithCache(true).Get(ctx, "k").Result(); got != "v" || err != nil {
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
	if got, err := c.WithCache(false).Do(context.Background(), "get", key).Text(); got != "v" || err != nil {
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
			if err := WithCache(c, true).Get(ctx, key).Err(); err != nil {
				t.Fatal(err)
			}
			for _, arg := range []interface{}{key, []byte(key), &key} {
				before := s.prefixes()
				if got, err := WithCache(c, false).Do(ctx, "get", arg).Text(); got != "v" || err != nil {
					t.Fatal(got, err)
				}
				if s.prefixes() != before+1 {
					t.Fatalf("%T bypass omitted CACHING NO: %v", arg, s.snapshot())
				}
			}
			before := s.count("get")
			if err := WithCache(c, true).Get(ctx, key).Err(); err != nil {
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

func TestCSCPolicyR6(t *testing.T) {
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
			if v, _ := c.WithCache(true).Get(ctx, "k").Result(); v != "1" {
				t.Fatal(v)
			}
			before := c.CSCStats()
			if v, _ := c.WithCache(false).Get(ctx, "k").Result(); v != "2" {
				t.Fatal(v)
			}
			after := c.CSCStats()
			if before.Entries != after.Entries || before.Hits != after.Hits {
				t.Fatal("bypass touched cache")
			}
			if v, _ := c.WithCache(true).Get(ctx, "k").Result(); v != "1" {
				t.Fatal("entry was refreshed", v)
			}
			if n.Load() != 2 {
				t.Fatal(n.Load())
			}
		})
	}
}

func TestCSCPolicyStrictHandshake(t *testing.T) {
	for _, coalesce := range []bool{false, true} {
		t.Run(strconv.FormatBool(coalesce), func(t *testing.T) {
			s := newCSCPolicyServer(t, func(a []string) string {
				if len(a) > 1 && strings.EqualFold(a[1], "tracking") {
					return "-NOPERM tracking\r\n"
				}
				return ""
			})
			opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
			opt.ClientSideCacheConfig.Strict = true
			opt.ClientSideCacheCoalesceMisses = coalesce
			c := policyClient(t, opt)
			if err := c.WithCache(true).Get(context.Background(), "k").Err(); err == nil || !strings.Contains(err.Error(), "cannot cache") {
				t.Fatal(err)
			}
			if s.count("get") != 0 {
				t.Fatal("strict sent GET")
			}
		})
	}
}

func TestCSCPolicyEligibilityAndGuards(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := cscPolicyOptions(s.addr, TrackingModeOptOut)
	var calls atomic.Int32
	opt.ClientSideCacheConfig.Cacheable = func(string, []string) bool { calls.Add(1); return true }
	opt.ClientSideCacheConfig.Strict = true
	c := policyClient(t, opt)
	ctx := context.Background()
	if err := c.WithCache(true).Touch(ctx, "k").Err(); err == nil {
		t.Fatal("strict TOUCH")
	}
	if s.count("touch") != 0 || calls.Load() != 0 {
		t.Fatal("ineligible call escaped")
	}
	if err := c.Touch(ctx, "k").Err(); err != nil {
		t.Fatal(err)
	}
	if s.prefixes() != 1 {
		t.Fatal("trackable ineligible command did not receive NO")
	}
	for _, sub := range []string{"TRACKING", "CACHING"} {
		if err := c.Do(ctx, []byte("CLIENT"), []byte(sub), "YES").Err(); err == nil {
			t.Fatal(sub)
		}
	}
}

func TestCSCPolicyControlFailures(t *testing.T) {
	for _, coalesce := range []bool{false, true} {
		for _, response := range []string{"-NOPERM caching\r\n", "+BAD\r\n", ":1\r\n", "*1\r\n-ERR nested\r\n", "DROP"} {
			t.Run(fmt.Sprintf("%t/%q", coalesce, response), func(t *testing.T) {
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
					v, err := c.WithCache(true).Get(context.Background(), "k").Result()
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
				if v, e := c.WithCache(true).Get(context.Background(), "k").Result(); v != "v" || e != nil {
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
	go func() { first <- c.WithCache(true).Get(ctx, "k").Err() }()
	<-started
	done := make(chan string, 1)
	go func() { done <- c.WithCache(false).Get(ctx, "k").Val() }()
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
			go func() { done <- c.WithCache(true).Get(ctx, "k").Err() }()
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
		if err := c.WithCache(true).Get(context.Background(), "k").Err(); err != nil {
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
			opt.ClientSideCacheConfig.Strict = true
			c := policyClient(t, opt)
			if got, err := c.WithCache(true).Get(context.Background(), "k").Result(); got != "v" || err != nil {
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
			if got, err := c.WithCache(true).Get(context.Background(), "k").Result(); got != "v" || err != nil {
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
	if err := c.WithCache(false).Do(ctx, "hget", "k", cscPolicyEncoderFailure{}).Err(); err == nil {
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
