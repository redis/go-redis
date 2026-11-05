package redis

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Many callers on a cluster full-duplex autopipeliner created before the cluster
// topology is loaded must not each open a pooled connection: the commands go to
// the per-node full-duplex connections once the topology loads. v9.23.0 opened one
// connection per concurrent caller here (the main pool filled up); a warm client
// opens only the full-duplex connections.
func TestClusterFDColdStartOpensFewConnections(t *testing.T) {
	probe := NewClusterClient(&ClusterOptions{Addrs: clusterFDTestAddrs})
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := probe.Ping(ctx).Err(); err != nil {
		_ = probe.Close()
		t.Skipf("local cluster not reachable at %v: %v", clusterFDTestAddrs, err)
	}
	state, err := probe.state.ReloadOrGet(ctx)
	_ = probe.Close()
	if err != nil {
		t.Skipf("cluster state not loadable: %v", err)
	}
	masters := len(state.Masters)

	var dials atomic.Int64
	cc := NewClusterClient(&ClusterOptions{
		Addrs: clusterFDTestAddrs,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			dials.Add(1)
			var d net.Dialer
			return d.DialContext(ctx, network, addr)
		},
	})
	t.Cleanup(func() { _ = cc.Close() })
	// No Ping: the topology is loaded by the first commands.
	ap, err := cc.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ap.Close() })

	const callers = 256
	start := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, callers)
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			for j := 0; j < 10; j++ {
				if err := ap.Get(context.Background(), fmt.Sprintf("fd:coldstart:%d:%d", i, j)).Err(); err != nil && err != Nil {
					errs <- err
					return
				}
			}
		}(i)
	}
	close(start)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("GET: %v", err)
	}

	// Per master: the full-duplex connection and its warm spare; plus the topology
	// loads and a little slack. One connection per caller would be ~callers.
	if got, limit := dials.Load(), int64(4*masters+4); got > limit {
		t.Fatalf("cold start opened %d connections for %d callers on %d masters, want <= %d", got, callers, masters, limit)
	}
}
