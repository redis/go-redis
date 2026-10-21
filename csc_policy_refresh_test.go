package redis

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
)

func TestCSCPolicyRefresh(t *testing.T) {
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		t.Run(string(mode), func(t *testing.T) {
			s := newCSCPolicyServer(t, nil)
			opt := cscPolicyOptions(s.addr, mode)
			var calls atomic.Int32
			opt.ClientSideCacheConfig.Cacheable = func(string, []string) bool { calls.Add(1); return true }
			c := policyClient(t, opt)
			ctx := context.Background()
			if err := c.Get(ctx, "k").Err(); err != nil {
				t.Fatal(err)
			}
			view := c.metadataView()
			key, ok := cscRenderEntryKey(c.cscKeyPrefix, view.cscFingerprint, NewStringCmd(ctx, "get", "k"))
			if !ok {
				t.Fatal("key")
			}
			c.csc.DeleteByCacheKey(key)
			n, err := c.refreshInvalidatedBatch(ctx, []cscRefreshTarget{{cacheKey: key, redisKeys: []string{c.cscKeyPrefix + "k"}}})
			if n != 1 || err != nil {
				t.Fatalf("refresh %d %v", n, err)
			}
			if calls.Load() != 1 {
				t.Fatal("refresh ran predicate")
			}
			if s.count("get") != 2 {
				t.Fatal(s.count("get"))
			}
			if mode == TrackingModeOptIn && s.prefixes() != 2 {
				t.Fatal("refresh missing YES")
			}
			s.assertPairs(t)
		})
	}
}

func TestCSCPolicyRefreshDeniedTracking(t *testing.T) {
	var flags atomic.Int32
	s := newCSCPolicyServer(t, func(a []string) string {
		if len(a) > 1 && strings.EqualFold(a[1], "caching") && flags.Add(1) > 1 {
			return "-NOPERM flag\r\n"
		}
		return ""
	})
	c := policyClient(t, cscPolicyOptions(s.addr, TrackingModeOptIn))
	ctx := context.Background()
	if err := c.WithCache(true).Get(ctx, "k").Err(); err != nil {
		t.Fatal(err)
	}
	view := c.metadataView()
	key, _ := cscRenderEntryKey(c.cscKeyPrefix, view.cscFingerprint, NewStringCmd(ctx, "get", "k"))
	c.csc.DeleteByCacheKey(key)
	n, err := c.refreshInvalidatedBatch(ctx, []cscRefreshTarget{{cacheKey: key, redisKeys: []string{c.cscKeyPrefix + "k"}}})
	if err != nil || n != 0 || c.CSCStats().Entries != 0 {
		t.Fatalf("untracked refresh published %d %v", n, err)
	}
}

func TestCSCPolicyRefreshPreludeBudget(t *testing.T) {
	targets := []cscRefreshTarget{{cacheKey: "123456", valBytes: 1}, {cacheKey: "123456", valBytes: 1}}
	end := cscRefreshChunkEndWithPrelude(targets, 0, 0, 12+len(cscYesWire), len(cscYesWire))
	if end != 1 {
		t.Fatalf("prelude excluded from byte budget: %d", end)
	}
}
