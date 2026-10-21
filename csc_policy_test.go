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
