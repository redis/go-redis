package redis

import (
	"sync"
	"testing"
)

// SetAddrs snapshots c.shards under c.mu before calling newRingShards, but
// newRingShards then ranges over c.onNewNode without holding c.mu, while
// OnNewNode appends to that slice under the lock. A goroutine resharding via
// SetAddrs and one registering a callback via OnNewNode therefore race on the
// onNewNode slice. clusterNodes.GetOrCreateWithNodeAddress reads the same kind
// of callback list under its lock; the ring path missed it. Run with -race
// against the pre-fix tree to see the report at ring.go.
func TestRingShardingOnNewNodeConcurrent(t *testing.T) {
	opt := &RingOptions{Addrs: map[string]string{"shard0": ":6390"}}
	opt.init()

	c := newRingSharding(opt)
	defer c.Close()

	var wg sync.WaitGroup
	wg.Add(2)

	// Register callbacks continuously for the whole duration of the resharding
	// loop so an append is in flight while newRingShards iterates the slice.
	done := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			select {
			case <-done:
				return
			default:
				c.OnNewNode(func(*Client) {})
			}
		}
	}()

	go func() {
		defer wg.Done()
		defer close(done)
		for i := 0; i < 50; i++ {
			// Toggle the shard set so newRingShards creates a fresh shard on the
			// even iterations, which is what makes it iterate onNewNode.
			if i%2 == 0 {
				c.SetAddrs(map[string]string{"shard0": ":6390", "shard1": ":6391"})
			} else {
				c.SetAddrs(map[string]string{"shard0": ":6390"})
			}
		}
	}()

	wg.Wait()
}
