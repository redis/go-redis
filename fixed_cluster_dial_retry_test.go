package redis

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/routing"
)

type clusterFixedDialFailureHook struct {
	fixedDialFailureHook
	before func(Cmder)
}

func (h clusterFixedDialFailureHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		// Route selection may independently ping a node before dispatch.
		if cmd.Name() == "ping" {
			return nil
		}
		h.before(cmd)
		return next(ctx, cmd)
	}
}

func TestClusterFixedEndpointDialExhaustion(t *testing.T) {
	for _, name := range []string{"scan", "fanout", "cursor", "cluster_pipeline", "cluster_tx_pipeline", "cluster_tx_dedicated"} {
		t.Run(name, func(t *testing.T) {
			const addr = "127.0.0.1:7301"
			opt := &ClusterOptions{
				Addrs: []string{addr}, MaxRedirects: 3, DialerRetries: 2, PoolSize: 16,
				DialerRetryBackoff: func(int) time.Duration { return 0 },
				MinRetryBackoff:    -1, MaxRetryBackoff: -1,
			}
			if name == "cluster_tx_dedicated" {
				opt.PipelineReadBufferSize = 64 * 1024
				opt.PipelineWriteBufferSize = 64 * 1024
			}
			c := NewClusterClient(opt)
			t.Cleanup(func() { _ = c.Close() })
			c.SetCommandInfoResolver(NewCommandMetadataResolver())
			state, err := newClusterStateFromShards(c.nodes, []ClusterShard{{
				Slots: []SlotRange{{Start: 0, End: 16383}},
				Nodes: []Node{{ID: "master", Endpoint: "127.0.0.1", Port: 7301, Role: "master", Health: "online"}},
			}}, addr, false)
			if err != nil {
				t.Fatal(err)
			}
			c.state.state.Store(state)
			reloaded := make(chan struct{}, 1)
			c.state.load = func(context.Context) (*clusterState, error) {
				select {
				case reloaded <- struct{}{}:
				default:
				}
				return state, nil
			}
			node := state.declaredMasters()[0]
			dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
			var calls atomic.Int32
			var scans atomic.Int32
			node.Client.AddHook(clusterFixedDialFailureHook{fixedDialFailureHook: fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				return nil, dialErr
			}}, before: func(cmd Cmder) {
				if cmd.Name() == "scan" {
					scans.Add(1)
				}
			}})
			ctx := context.Background()
			if name == "fanout" {
				err = c.executeParallelTarget(ctx, node, -1, NewCmd(ctx, "dbsize"))
				select {
				case <-reloaded:
				case <-time.After(2 * time.Second):
					t.Fatal("dial timeout did not schedule topology reload")
				}
			} else if name == "cursor" {
				cmd := NewCmd(ctx, "ft.cursor", "read", "idx", int64(123))
				err = c.Process(ctx, cmd)
			} else if name == "cluster_pipeline" {
				pipe := c.Pipeline()
				pipe.Get(ctx, "key")
				_, err = pipe.Exec(ctx)
			} else if name == "cluster_tx_pipeline" || name == "cluster_tx_dedicated" {
				pipe := c.TxPipeline()
				pipe.Get(ctx, "key")
				_, err = pipe.Exec(ctx)
			} else {
				cmd := NewScanCmd(ctx, c.Process, "scan", uint64(0))
				decision := c.routingDecisionInView(ctx, cmd, defaultCommandMetadataView(), nil)
				cmd.setClusterRoutingDecision(&decision)
				err = c.Process(ctx, cmd)
			}
			if err != dialErr {
				t.Fatalf("returned error = %v (%T), want original *net.OpError", err, err)
			}
			if name == "scan" && scans.Load() != 1 {
				t.Errorf("SCAN attempts = %d, want 1", scans.Load())
			}
			if got := calls.Load(); got != 2 {
				t.Errorf("dial calls = %d, want 2", got)
			}
		})
	}
}

func TestClusterPipelineRemapsExhaustedReplicaToMaster(t *testing.T) {
	const masterAddr = "127.0.0.1:7301"
	const replicaAddr = "127.0.0.1:7302"
	c := NewClusterClient(&ClusterOptions{
		Addrs: []string{masterAddr}, ReadOnly: true, MaxRedirects: 2,
		DialerRetries: 2, PoolSize: 16,
		DialerRetryBackoff: func(int) time.Duration { return 0 },
		MinRetryBackoff:    -1, MaxRetryBackoff: -1,
	})
	t.Cleanup(func() { _ = c.Close() })
	c.SetCommandInfoResolver(NewCommandMetadataResolver())
	state, err := newClusterStateFromShards(c.nodes, []ClusterShard{{
		Slots: []SlotRange{{Start: 0, End: 16383}},
		Nodes: []Node{
			{ID: "master", Endpoint: "127.0.0.1", Port: 7301, Role: "master", Health: "online"},
			{ID: "replica", Endpoint: "127.0.0.1", Port: 7302, Role: "replica", Health: "online"},
		},
	}}, masterAddr, false)
	if err != nil {
		t.Fatal(err)
	}
	c.state.state.Store(state)
	c.state.load = func(context.Context) (*clusterState, error) { return state, nil }
	master, replica := state.declaredMasters()[0], state.declaredSlaves()[0]
	if master.Client.opt.Addr != masterAddr || replica.Client.opt.Addr != replicaAddr {
		t.Fatalf("unexpected cluster endpoints: %s, %s", master.Client.opt.Addr, replica.Client.opt.Addr)
	}
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
	var calls atomic.Int32
	replica.Client.AddHook(fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
		calls.Add(1)
		return nil, dialErr
	}})
	ctx := context.Background()
	cmd := NewStringCmd(ctx, "get", "key")
	cmds := []Cmder{cmd}
	route := c.resolvePipelineRouting(ctx, cmds)
	defer route.clear(cmds)
	initial, err := c.slotReadOnlyNode(state, clusterKeySlot("key"))
	if err != nil || initial != replica {
		t.Fatalf("initial read-only node = %v, want replica %v (err=%v)", initial, replica, err)
	}
	failed := newCmdsMap()
	c.processPipelineNode(ctx, replica, cmds, failed, route)
	if got := calls.Load(); got < 2 {
		t.Errorf("failed replica dials = %d, want exhausted sequence", got)
	}
	if _, ok := failed.m[replica]; ok {
		t.Fatal("must not requeue on the exhausted replica")
	}
	if got := failed.m[master]; len(got) != 1 || got[0] != cmd {
		t.Errorf("retry nodes = %v, want command remapped to healthy master", failed.m)
	}
}

func TestClusterUnpinnedScanRetriesAnotherMasterAfterDialExhaustion(t *testing.T) {
	const firstAddr = "127.0.0.1:7301"
	c := NewClusterClient(&ClusterOptions{
		Addrs: []string{firstAddr}, ShardPicker: routing.NewStaticShardPicker(0),
		MaxRedirects: 2, DialerRetries: 2, PoolSize: 16,
		DialerRetryBackoff: func(int) time.Duration { return 0 },
		MinRetryBackoff: -1, MaxRetryBackoff: -1,
	})
	t.Cleanup(func() { _ = c.Close() })
	c.SetCommandInfoResolver(NewCommandMetadataResolver())
	state, err := newClusterStateFromShards(c.nodes, []ClusterShard{
		{
			Slots: []SlotRange{{Start: 0, End: 8191}},
			Nodes: []Node{{ID: "first", Endpoint: "127.0.0.1", Port: 7301, Role: "master", Health: "online"}},
		},
		{
			Slots: []SlotRange{{Start: 8192, End: 16383}},
			Nodes: []Node{{ID: "second", Endpoint: "127.0.0.1", Port: 7302, Role: "master", Health: "online"}},
		},
	}, firstAddr, false)
	if err != nil {
		t.Fatal(err)
	}
	c.state.state.Store(state)
	c.state.load = func(context.Context) (*clusterState, error) { return state, nil }
	if len(state.Masters) != 2 {
		t.Fatalf("masters = %d, want 2", len(state.Masters))
	}
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
	var failedDials atomic.Int32
	var failedScans atomic.Int32
	state.Masters[0].Client.AddHook(clusterFixedDialFailureHook{
		fixedDialFailureHook: fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
			failedDials.Add(1)
			return nil, dialErr
		}},
		before: func(cmd Cmder) {
			if cmd.Name() == "scan" {
				failedScans.Add(1)
			}
		},
	})
	var healthyScans int
	state.Masters[1].Client.AddHook(pongHook{pings: &healthyScans})

	if err := c.Do(context.Background(), "SCAN", 0).Err(); err != nil {
		t.Fatalf("unpinned SCAN did not recover on another master: %v", err)
	}
	if got := failedScans.Load(); got != 1 {
		t.Errorf("failed master SCANs = %d, want 1", got)
	}
	if got := failedDials.Load(); got < 2 {
		t.Errorf("failed master dials = %d, want an exhausted dial sequence", got)
	}
	if healthyScans != 1 {
		t.Errorf("healthy master SCANs = %d, want 1", healthyScans)
	}
}
