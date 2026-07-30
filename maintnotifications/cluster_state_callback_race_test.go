package maintnotifications

import (
	"context"
	"sync"
	"testing"
)

// clusterStateReloadCallback is set from the OnNewNode hook while a node client
// is being created (GetOrCreateWithNodeAddress) and read from
// TriggerClusterStateReload, which runs when a SMIGRATED push notification is
// handled on one of that node's connections (handleSMigrated). With
// MinIdleConns>0 the node's background dial goroutines process push frames
// during initConn, so a SMIGRATED can reach TriggerClusterStateReload while the
// OnNewNode hook is still setting the callback. Run with -race against the
// pre-fix tree to see the report at manager.go (read in TriggerClusterStateReload
// vs write in SetClusterStateReloadCallback).
func TestManagerClusterStateReloadCallbackConcurrent(t *testing.T) {
	hm := &Manager{}

	const (
		workers    = 4
		iterations = 2000
	)

	var wg sync.WaitGroup
	start := make(chan struct{})

	// Setters, as the OnNewNode hook does when node clients are created.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cb := func(context.Context, string, []string) {}
			<-start
			for j := 0; j < iterations; j++ {
				hm.SetClusterStateReloadCallback(cb)
			}
		}()
	}

	// Triggers, as the SMIGRATED push handler does.
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			for j := 0; j < iterations; j++ {
				hm.TriggerClusterStateReload(context.Background(), "127.0.0.1:6379", nil)
			}
		}()
	}

	close(start)
	wg.Wait()
}
