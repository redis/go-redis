package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/routing"
)

// clusterState is published as an immutable snapshot shared by every concurrent
// caller. Building the routed node list with append(state.Masters,
// state.Slaves...) reuses the spare capacity of the shared Masters backing array
// (newClusterState grows it via appendIfNotExist, so cap > len is common), so two
// goroutines routing commands at the same time write the same slots. Run with
// -race against the pre-fix tree to see the report at osscluster_router.go.
func TestPickArbitraryNodeConcurrent(t *testing.T) {
	// len 3, cap 4 mirrors the appendIfNotExist growth for a 3-master cluster;
	// one replica fits the spare slot, so append would alias without the fix.
	masters := make([]*clusterNode, 0, 4)
	masters = append(masters, &clusterNode{}, &clusterNode{}, &clusterNode{})
	state := &clusterState{
		Masters:   masters,
		Slaves:    []*clusterNode{{}},
		createdAt: time.Now(),
	}

	c := &ClusterClient{
		opt: &ClusterOptions{ShardPicker: &routing.RoundRobinPicker{}},
		state: newClusterStateHolder(
			func(ctx context.Context) (*clusterState, error) { return state, nil },
			time.Hour,
		),
	}
	c.state.state.Store(state)

	var wg sync.WaitGroup
	for i := 0; i < 32; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				if node := c.pickArbitraryNode(context.Background()); node == nil {
					t.Error("pickArbitraryNode returned nil")
					return
				}
			}
		}()
	}
	wg.Wait()
}
