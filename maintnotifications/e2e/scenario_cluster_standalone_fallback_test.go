package e2e

import (
	"context"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

const clusterDisabledReply = "-ERR This instance has cluster support disabled\r\n"

// TestClusterStandaloneFallbackTransitions moves the proxy endpoint from a
// three-shard cluster to a single non-cluster instance and back, with traffic
// running, and checks that a ClusterClient with StandaloneFallback follows.
//
// The proxy nodes forward to one standalone Redis. A CLUSTER SLOTS
// interceptor makes them look like a cluster; replacing its reply with the
// "cluster support disabled" error makes the seed look like a standalone
// server. READONLY and CLUSTER SHARDS always reach Redis and fail there, as
// on a real standalone server.
func TestClusterStandaloneFallbackTransitions(t *testing.T) {
	if os.Getenv("FAULT_INJECTOR_URL") != "" {
		t.Skip("requires the proxy mock")
	}
	injector, err := NewNotificationInjector()
	if err != nil {
		t.Fatalf("NewNotificationInjector() = %v", err)
	}
	proxy, ok := injector.(*ProxyNotificationInjector)
	if !ok {
		t.Skip("requires the proxy mock")
	}
	if err := proxy.Start(); err != nil {
		t.Skipf("proxy not available: %v", err)
	}
	defer proxy.Stop()

	clusterSlots := proxy.buildClusterSlotsResponse()
	setCluster := func(t *testing.T) {
		t.Helper()
		if err := proxy.SetClusterSlotsReply(clusterSlots); err != nil {
			t.Fatalf("set cluster topology: %v", err)
		}
	}
	setStandalone := func(t *testing.T) {
		t.Helper()
		if err := proxy.SetClusterSlotsReply(clusterDisabledReply); err != nil {
			t.Fatalf("set standalone: %v", err)
		}
	}
	// Later tests share the proxy and expect the cluster topology.
	t.Cleanup(func() { _ = proxy.SetClusterSlotsReply(clusterSlots) })

	addrs := proxy.GetClusterAddrs()
	seed := addrs[0]
	clusterMasters := slices.Clone(addrs[:len(proxy.visibleNodes)])
	slices.Sort(clusterMasters)

	for _, tc := range []struct {
		name          string
		routeRandomly bool
	}{
		{name: "primary"},
		// Node clients send READONLY, which the standalone Redis rejects.
		{name: "route randomly", routeRandomly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			setCluster(t)

			client := redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:                      []string{seed},
				Protocol:                   3,
				StandaloneFallback:         true,
				RouteRandomly:              tc.routeRandomly,
				ClusterStateReloadInterval: 500 * time.Millisecond,
			})
			defer client.Close()

			waitForMasters(t, client, clusterMasters)
			t.Logf("cluster: masters %v", clusterMasters)

			traffic := startFallbackTraffic(client)
			defer traffic.stop()

			// Cluster -> standalone: no MOVED, the periodic reload sees it.
			traffic.expectProgress(t, "cluster")
			setStandalone(t)
			waitForMasters(t, client, []string{seed})
			t.Logf("standalone: master %s", seed)
			traffic.expectProgress(t, "standalone")

			// A client without the option cannot start against this endpoint,
			// so the endpoint really is standalone now.
			plain := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{seed}, Protocol: 3})
			err := plain.Ping(ctx).Err()
			_ = plain.Close()
			if err == nil || !strings.Contains(err.Error(), "cluster support disabled") {
				t.Fatalf("Ping() without StandaloneFallback = %v; want cluster support disabled", err)
			}

			// Standalone -> cluster.
			setCluster(t)
			waitForMasters(t, client, clusterMasters)
			t.Logf("cluster again: masters %v", clusterMasters)
			traffic.expectProgress(t, "cluster again")

			ops, errs, firstErr := traffic.stop()
			t.Logf("traffic: %d ops, %d errors", ops, errs)
			if errs > 0 {
				t.Fatalf("traffic saw %d errors across the transitions, first: %v", errs, firstErr)
			}
		})
	}
}

// waitForMasters waits until the traffic keys map to exactly the masters in
// want (sorted). It reads the topology through MasterForKey, which, like
// commands, only reloads lazily; ForEachMaster would force a reload and hide
// whether the periodic reload works.
func waitForMasters(t *testing.T, client *redis.ClusterClient, want []string) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(15 * time.Second)
	var got []string
	for time.Now().Before(deadline) {
		got = got[:0]
		var err error
		for w := 0; w < fallbackWorkers && err == nil; w++ {
			for i := 0; i < fallbackKeysPerWorker; i++ {
				var master *redis.Client
				if master, err = client.MasterForKey(ctx, fallbackKey(w, i)); err != nil {
					break
				}
				if addr := master.Options().Addr; !slices.Contains(got, addr) {
					got = append(got, addr)
				}
			}
		}
		slices.Sort(got)
		if err == nil && slices.Equal(got, want) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("masters = %v; want %v", got, want)
}

const (
	fallbackWorkers       = 4
	fallbackKeysPerWorker = 64
)

func fallbackKey(worker, i int) string {
	return fmt.Sprintf("fallback-e2e:%d:%d", worker, i)
}

// fallbackTraffic runs SET/GET on keys across all slots until stopped.
type fallbackTraffic struct {
	ops, errs atomic.Int64
	done      chan struct{}
	wg        sync.WaitGroup
	once      sync.Once

	mu       sync.Mutex
	firstErr error
}

func startFallbackTraffic(client *redis.ClusterClient) *fallbackTraffic {
	tr := &fallbackTraffic{done: make(chan struct{})}
	for w := 0; w < fallbackWorkers; w++ {
		tr.wg.Add(1)
		go func() {
			defer tr.wg.Done()
			ctx := context.Background()
			for i := 0; ; i++ {
				select {
				case <-tr.done:
					return
				default:
				}
				key := fallbackKey(w, i%fallbackKeysPerWorker)
				val := fmt.Sprint(i)
				err := client.Set(ctx, key, val, time.Minute).Err()
				if err == nil {
					var got string
					got, err = client.Get(ctx, key).Result()
					if err == nil && got != val {
						err = fmt.Errorf("GET %s = %q; want %q", key, got, val)
					}
				}
				tr.ops.Add(1)
				if err != nil {
					tr.errs.Add(1)
					tr.mu.Lock()
					if tr.firstErr == nil {
						tr.firstErr = err
					}
					tr.mu.Unlock()
				}
				time.Sleep(5 * time.Millisecond)
			}
		}()
	}
	return tr
}

// expectProgress fails the test unless traffic completes more operations.
func (tr *fallbackTraffic) expectProgress(t *testing.T, phase string) {
	t.Helper()
	start := tr.ops.Load()
	deadline := time.Now().Add(5 * time.Second)
	for tr.ops.Load() < start+50 {
		if time.Now().After(deadline) {
			t.Fatalf("%s: traffic stalled at %d ops", phase, tr.ops.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (tr *fallbackTraffic) stop() (ops, errs int64, firstErr error) {
	tr.once.Do(func() { close(tr.done) })
	tr.wg.Wait()
	tr.mu.Lock()
	defer tr.mu.Unlock()
	return tr.ops.Load(), tr.errs.Load(), tr.firstErr
}
