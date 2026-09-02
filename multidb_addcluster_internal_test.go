package redis

import (
	"context"
	"errors"
	"sync"
	"testing"
)

// AddDatabase must refuse a cluster member once Close has begun tearing the
// client down. Close clears the cached autopipeliner pointers and flips
// autopipelinerClosed under autopipelinerMu before it drains the accepted
// batches. Without the closed check the liveness guard sees no live
// autopipeliner and would admit the cluster member while the drain is still
// flushing, letting a failing-over batch reach it through the unsharded
// autopipeline path the cluster member does not support.
func TestAddClusterDatabaseRejectedAfterCloseBegan(t *testing.T) {
	core := newMultidbCore(&MultiDBOptions{})
	c := &MultiDBClient{core: core, autopipelinerMu: new(sync.Mutex), autopipelinerClosed: true}

	before := len(core.dbs)
	id, err := c.AddDatabase(context.Background(), MultiDBClientConfig{
		ClusterOptions:         &ClusterOptions{Addrs: []string{"127.0.0.1:6379"}},
		SkipInitialHealthCheck: true,
	})
	if !errors.Is(err, ErrClosed) {
		t.Fatalf("cluster add during shutdown: got id=%d err=%v, want ErrClosed", id, err)
	}
	if got := len(core.dbs); got != before {
		t.Fatalf("cluster add during shutdown mutated membership: %d -> %d", before, got)
	}
}
