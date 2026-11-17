package redis

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"
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
	for _, name := range []string{"scan", "fanout", "cursor", "cluster_pipeline"} {
		t.Run(name, func(t *testing.T) {
			const addr = "127.0.0.1:7301"
			c := NewClusterClient(&ClusterOptions{
				Addrs: []string{addr}, MaxRedirects: 3, DialerRetries: 2, PoolSize: 16,
				DialerRetryBackoff: func(int) time.Duration { return 0 },
				MinRetryBackoff:    -1, MaxRetryBackoff: -1,
			})
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
