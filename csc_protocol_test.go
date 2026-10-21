package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

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
