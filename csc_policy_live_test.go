package redis

import (
	"context"
	"fmt"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/routing"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Exercise three physical masters, not merely three slots on one server. Live
// metadata, refresh and disconnects must retain fragment-local publication.
func TestCSCPolicyFanoutLive(t *testing.T) {
	writer, nodes := newClusterCSCLive(t)
	ctx := context.Background()
	keys := make([]string, 3)
	for i := 0; i < 3; i++ {
		for n := 0; ; n++ {
			key := fmt.Sprintf("{policy-fanout-%d-%d}:key", i, n)
			if slot := hashtag.Slot(key); slot >= i*5461 && (i == 2 || slot < (i+1)*5461) {
				keys[i] = key
				break
			}
		}
	}
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		for _, coalesce := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/coalesce=%t", mode, coalesce), func(t *testing.T) {
				for _, key := range keys {
					if err := writer.Set(ctx, key, "before", 0).Err(); err != nil {
						t.Fatal(err)
					}
				}
				var predicates atomic.Int32
				r := NewClusterClient(&ClusterOptions{
					Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true, PoolSize: 4,
					CommandMetadata: &CommandMetadataConfig{Mode: CommandMetadataPreferLive},
					ClientSideCacheConfig: &ClientSideCacheConfig{
						MaxEntries: 128, TrackingMode: mode, DrainInterval: time.Millisecond,
						Cacheable: func(name string, got []string) bool {
							predicates.Add(1)
							if name != "mget" || !reflect.DeepEqual(got, keys) {
								t.Errorf("predicate saw fragment: %s %v", name, got)
							}
							return true
						},
					},
					ClientSideCacheCoalesceMisses: coalesce, ClientSideCacheRefreshOnInvalidate: true,
					ClientSideCacheInvalidationBatchWindow: time.Millisecond,
					MaintNotificationsConfig:               &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				})
				defer r.Close()
				r.SetCommandInfoResolver(r.NewDynamicResolver())
				if err := r.MGet(ctx, keys...).Err(); err != nil {
					t.Fatal(err)
				}
				clusterCSCLiveEventually(t, func() bool { return r.metadataView().live })
				for i := 0; i < 2; i++ {
					got, err := r.MGet(ctx, keys...).Result()
					if err != nil || !reflect.DeepEqual(got, []interface{}{"before", "before", "before"}) {
						t.Fatal(got, err)
					}
				}
				if r.CSCStats().Entries != 3 || r.CSCStats().Hits < 3 || predicates.Load() != 3 {
					t.Fatalf("fanout did not cache independently: %+v predicate=%d", r.CSCStats(), predicates.Load())
				}
				before := r.CSCRefreshStats().Refreshed
				if err := writer.Set(ctx, keys[1], "after", 0).Err(); err != nil {
					t.Fatal(err)
				}
				clusterCSCLiveEventually(t, func() bool { return r.CSCRefreshStats().Refreshed > before })
				if predicates.Load() != 3 {
					t.Fatal("refresh re-evaluated the application predicate")
				}
				var wg sync.WaitGroup
				for i := 0; i < 16; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						got, err := r.MGet(ctx, keys...).Result()
						if err != nil || !reflect.DeepEqual(got, []interface{}{"before", "after", "before"}) {
							t.Errorf("parallel fanout: %v %v", got, err)
						}
					}()
				}
				wg.Wait()
				if predicates.Load() != 19 {
					t.Fatal("predicate count changed during fragment execution", predicates.Load())
				}
				owner, err := r.MasterForKey(ctx, keys[0])
				if err != nil {
					t.Fatal(err)
				}
				if err := owner.connPool.(*pool.ConnPool).Filter(func(*pool.Conn) bool { return true }); err != nil {
					t.Fatal(err)
				}
				clusterCSCLiveEventually(t, func() bool { return r.CSCStats().Entries == 2 })
				if got, err := r.WithCache(true).MGet(ctx, keys...).Result(); err != nil || !reflect.DeepEqual(got, []interface{}{"before", "after", "before"}) {
					t.Fatal(got, err)
				}
				if r.CSCStats().Entries != 3 || predicates.Load() != 19 {
					t.Fatal("disconnected fragment failed to reacquire tracking")
				}
			})
		}
	}
	for _, mode := range []TrackingMode{TrackingModeOptIn, TrackingModeOptOut} {
		t.Run(string(mode)+"/fanout-MOVED", func(t *testing.T) {
			opt := clusterCSCTestOptions(func() []ClusterSlot {
				return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: nodes[0].opt.Addr}}}}
			})
			opt.DisableRoutingPolicies = false
			opt.ClientSideCacheConfig.TrackingMode = mode
			opt.ClientSideCacheConfig.Strict = true
			r := NewClusterClient(opt)
			defer r.Close()
			r.SetCommandInfoResolver(NewCommandMetadataResolver())
			if got, err := r.WithCache(true).MGet(ctx, keys...).Result(); err != nil || !reflect.DeepEqual(got, []interface{}{"before", "after", "before"}) {
				t.Fatal(got, err)
			}
			if r.CSCStats().Entries != 3 {
				t.Fatal("redirected fragments did not publish", r.CSCStats())
			}
		})
		t.Run(string(mode)+"/fanout-ASK", func(t *testing.T) {
			// Leave the source empty while the key exists on the importer.
			// Other slots remain ordinary cacheable fragments on their owners.
			source, target := nodes[0], nodes[1]
			key, slot := keys[0], hashtag.Slot(keys[0])
			if err := writer.Del(ctx, key).Err(); err != nil {
				t.Fatal(err)
			}
			aid, err := source.ClusterMyID(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			bid, err := target.ClusterMyID(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
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
			// Delete on the importer while it still accepts ASKING, then restore
			// the original source value after both nodes leave migration state.
			defer func() {
				_ = cn.Do(ctx, "asking").Err()
				_ = cn.Del(ctx, key).Err()
				_ = target.Do(ctx, "cluster", "setslot", slot, "stable").Err()
				_ = source.Do(ctx, "cluster", "setslot", slot, "stable").Err()
				_ = writer.Set(ctx, key, "before", 0).Err()
			}()
			r := NewClusterClient(&ClusterOptions{
				Addrs: []string{nodes[0].opt.Addr}, Protocol: 3, DisableIdentity: true,
				ClientSideCacheConfig:         &ClientSideCacheConfig{MaxEntries: 128, TrackingMode: mode, Strict: true},
				ClientSideCacheCoalesceMisses: true,
				MaintNotificationsConfig:      &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
			})
			defer r.Close()
			r.SetCommandInfoResolver(NewCommandMetadataResolver())
			for i := 0; i < 2; i++ {
				if got, err := r.WithCache(true).MGet(ctx, keys...).Result(); err != nil || !reflect.DeepEqual(got, []interface{}{"imported", "after", "before"}) {
					t.Fatal(got, err)
				}
				if r.CSCStats().Entries != 2 {
					t.Fatal("ASK fragment published or siblings were lost", r.CSCStats())
				}
			}
		})
	}
}

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
