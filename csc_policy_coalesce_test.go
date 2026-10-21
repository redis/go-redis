package redis

import (
	"context"
	"strconv"
	"sync"
	"testing"
)

func TestCSCPolicyConcurrentCoalescing(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
	opt.ClientSideCacheCoalesceMisses = true
	c := policyClient(t, opt)
	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := strconv.Itoa(i % 8)
			if v, e := c.WithCache(true).Get(context.Background(), key).Result(); v != "v" || e != nil {
				t.Errorf("%q %v", v, e)
			}
		}(i)
	}
	wg.Wait()
	if s.count("get") != 8 {
		t.Fatalf("GETs %d", s.count("get"))
	}
	s.assertPairs(t)
}

func TestCSCPolicyCoalescerBudgetFallback(t *testing.T) {
	s := newCSCPolicyServer(t, nil)
	opt := cscPolicyOptions(s.addr, TrackingModeOptIn)
	opt.ClientSideCacheCoalesceMisses = true
	opt.ClientSideCacheConfig.Strict = true
	c := policyClient(t, opt)
	mc := c.cscMissCoalescer.Load()
	if mc == nil {
		t.Fatal("no coalescer")
	}
	mc.wireBytes.Store(cscMissWireBudgetBytes)
	if err := c.WithCache(true).Get(context.Background(), "k").Err(); err != nil {
		t.Fatal(err)
	}
	mc.wireBytes.Store(0)
	if err := c.WithCache(true).Get(context.Background(), "k").Err(); err != nil {
		t.Fatal(err)
	}
	if s.count("get") != 1 || s.prefixes() != 1 {
		t.Fatal("fallback lost cache intent", s.count("get"), s.prefixes())
	}
}
