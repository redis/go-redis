package redis

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/routing"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Uses the existing owned six-process fixture, never a developer database.
func TestCSCPolicyLive(t *testing.T) {
	writer, nodes := newClusterCSCLive(t)
	ctx := context.Background()
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		for _, coalesce := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/coalesce=%t", mode, coalesce), func(t *testing.T) {
				r := NewClusterClient(&ClusterOptions{
					Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true, PoolSize: 4,
					ClientSideCacheConfig:         &ClientSideCacheConfig{MaxEntries: 128, TrackingMode: mode, DrainInterval: time.Millisecond, Cacheable: func(string, []string) bool { return true }},
					ClientSideCacheCoalesceMisses: coalesce, ClientSideCacheRefreshOnInvalidate: true, ClientSideCacheInvalidationBatchWindow: time.Millisecond,
					MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				})
				defer r.Close()
				key := fmt.Sprintf("{policy-%s-%t}:key", mode, coalesce)
				if err := writer.Set(ctx, key, "before", 0).Err(); err != nil {
					t.Fatal(err)
				}
				for i := 0; i < 2; i++ {
					if got, err := r.Get(ctx, key).Result(); got != "before" || err != nil {
						t.Fatalf("warm %q %v", got, err)
					}
				}
				if r.CSCStats().Hits < 1 {
					t.Fatal("no cache hit")
				}
				if err := writer.Set(ctx, key, "after", 0).Err(); err != nil {
					t.Fatal(err)
				}
				clusterCSCLiveEventually(t, func() bool { return r.CSCRefreshStats().Refreshed > 0 })
				if got, err := r.Get(ctx, key).Result(); got != "after" || err != nil {
					t.Fatalf("refresh %q %v", got, err)
				}
				var wg sync.WaitGroup
				for i := 0; i < 16; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						if got, err := r.WithCache(true).Get(ctx, key).Result(); got != "after" || err != nil {
							t.Errorf("parallel %q %v", got, err)
						}
					}()
				}
				wg.Wait()
			})
		}
	}
	for _, mode := range []TrackingMode{TrackingModeOptIn, TrackingModeOptOut} {
		t.Run(string(mode)+"/MOVED", func(t *testing.T) {
			key := "{policy-moved}:key"
			if err := writer.Set(ctx, key, "moved", 0).Err(); err != nil {
				t.Fatal(err)
			}
			owner, err := writer.MasterForKey(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			var wrong *Client
			for _, n := range nodes[:3] {
				if n.opt.Addr != owner.opt.Addr {
					wrong = n
					break
				}
			}
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: wrong.opt.Addr}}}}
			})
			opt.ClientSideCacheConfig.TrackingMode = mode
			opt.DisableRoutingPolicies = false
			var predicates atomic.Int32
			opt.ClientSideCacheConfig.Cacheable = func(_ string, _ []string) bool {
				predicates.Add(1)
				return true
			}
			r := NewClusterClient(opt)
			defer r.Close()
			r.SetCommandInfoResolver(NewCommandInfoResolver(cscPolicyLiveMGetPolicy))
			if got, err := r.WithCache(true).Get(ctx, key).Result(); got != "moved" || err != nil {
				t.Fatalf("MOVED %q %v", got, err)
			}
			if r.CSCStats().Entries != 1 {
				t.Fatal("MOVED did not publish tracked result")
			}
			other := key + ":other"
			if err := writer.Set(ctx, other, "other", 0).Err(); err != nil {
				t.Fatal(err)
			}
			got, err := r.MGet(ctx, key, other).Result()
			if err != nil || len(got) != 2 || got[0] != "moved" || got[1] != "other" || predicates.Load() != 1 {
				t.Fatalf("one-slot MOVED %v %v; predicate calls %d", got, err, predicates.Load())
			}
		})
	}
	for _, mode := range []TrackingMode{TrackingModeOptIn, TrackingModeOptOut} {
		t.Run(string(mode)+"/ASK", func(t *testing.T) {
			key := fmt.Sprintf("{policy-ask-%s}:key", mode)
			owner, err := writer.MasterForKey(ctx, key)
			if err != nil {
				t.Fatal(err)
			}
			var source, target *Client
			for _, n := range nodes[:3] {
				if n.opt.Addr == owner.opt.Addr {
					source = n
				} else if target == nil {
					target = n
				}
			}
			aid, err := source.ClusterMyID(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			bid, err := target.ClusterMyID(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			slot := hashtag.Slot(key)
			if err := target.Do(ctx, "cluster", "setslot", slot, "importing", aid).Err(); err != nil {
				t.Fatal(err)
			}
			defer target.Do(ctx, "cluster", "setslot", slot, "stable")
			if err := source.Do(ctx, "cluster", "setslot", slot, "migrating", bid).Err(); err != nil {
				t.Fatal(err)
			}
			defer source.Do(ctx, "cluster", "setslot", slot, "stable")
			cn := target.Conn()
			defer cn.Close()
			if err := cn.Do(ctx, "asking").Err(); err != nil {
				t.Fatal(err)
			}
			if err := cn.Set(ctx, key, "imported", 0).Err(); err != nil {
				t.Fatal(err)
			}
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: source.opt.Addr}}}}
			})
			opt.ClientSideCacheConfig.TrackingMode = mode
			opt.ClientSideCacheConfig.Strict = true
			opt.DisableRoutingPolicies = false
			r := NewClusterClient(opt)
			defer r.Close()
			r.SetCommandInfoResolver(NewCommandInfoResolver(cscPolicyLiveMGetPolicy))
			if got, err := r.WithCache(true).Get(ctx, key).Result(); got != "imported" || err != nil {
				t.Fatalf("ASK %q %v", got, err)
			}
			if r.CSCStats().Entries != 0 {
				t.Fatal("ASK published")
			}
			got, err := r.WithCache(true).MGet(ctx, key, key).Result()
			if err != nil || len(got) != 2 || got[0] != "imported" || got[1] != "imported" || r.CSCStats().Entries != 0 {
				t.Fatalf("one-slot ASK %v %v", got, err)
			}
		})
	}
}

func cscPolicyLiveMGetPolicy(_ context.Context, cmd Cmder) *routing.CommandPolicy {
	if cmd.Name() == "mget" {
		return &routing.CommandPolicy{Request: routing.ReqMultiShard, Response: routing.RespDefaultHashSlot}
	}
	return nil
}
