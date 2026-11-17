package redis

import (
	"context"
	"errors"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/routing"
)

type reselectionShardPicker struct {
	before func()
}

func (p reselectionShardPicker) Next(int) int {
	p.before()
	return 0
}

func TestExhaustedEndpointReselection(t *testing.T) {
	t.Run("cluster", func(t *testing.T) {
		for _, mode := range []string{"keyed", "legacy", "multi_slot"} {
			t.Run(mode, func(t *testing.T) {
				server := newCSCPolicyServer(t, nil)
				const dead = "127.0.0.1:7301"
				c := NewClusterClient(&ClusterOptions{Addrs: []string{dead}, MaxRedirects: 3,
					DisableRoutingPolicies: mode == "legacy", ReadOnly: true, PoolSize: 16, DialerRetries: 2,
					DialerRetryBackoff: func(int) time.Duration { return 0 }, MinRetryBackoff: -1, MaxRetryBackoff: -1})
				t.Cleanup(func() { _ = c.Close() })
				c.SetCommandInfoResolver(NewCommandMetadataResolver())
				makeState := func(nodes ...ClusterNode) *clusterState {
					state, err := newClusterState(c.nodes, []ClusterSlot{{Start: 0, End: 16383, Nodes: nodes}}, "")
					if err != nil {
						t.Fatal(err)
					}
					return state
				}
				stale := makeState(ClusterNode{Addr: dead}, ClusterNode{Addr: server.addr})
				fresh := makeState(ClusterNode{Addr: server.addr})
				stale.Slaves[0].MarkAsFailing()
				c.state.state.Store(stale)
				c.state.load = func(context.Context) (*clusterState, error) { return fresh, nil }
				selections := 0
				c.opt.ShardPicker = reselectionShardPicker{before: func() {
					selections++
					if selections == 2 {
						// The second routing attempt already captured the stale state.
						// Publish a failover now: only a later attempt can observe it.
						if _, err := c.state.Reload(context.Background()); err != nil {
							t.Fatal(err)
						}
					}
				}}
				var dials atomic.Int32
				stale.Masters[0].Client.AddHook(fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
					dials.Add(1)
					return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("endpoint unavailable")}
				}})
				ctx := context.Background()
				if mode == "multi_slot" {
					got, err := c.Do(ctx, "MGET", "{key}a", "{key}b").Slice()
					if err != nil || len(got) != 2 || got[0] != "v" || got[1] != "v" {
						t.Errorf("MGET after topology replacement = %v, %v; want [v v], nil", got, err)
					}
				} else {
					got, err := c.Get(ctx, "key").Result()
					if err != nil || got != "v" {
						t.Errorf("GET after topology replacement = %q, %v; want v, nil", got, err)
					}
				}
				if got := dials.Load(); got != 2 {
					t.Errorf("exhausted endpoint dials = %d, want 2", got)
				}
			})
		}
	})
	t.Run("ring_keyed", func(t *testing.T) {
		server := newCSCPolicyServer(t, nil)
		const dead = "127.0.0.1:7301"
		var ring *Ring
		var dials atomic.Int32
		ring = NewRing(&RingOptions{Addrs: map[string]string{"dead": dead, "healthy": server.addr},
			PoolSize: 16, MaxRetries: 3, DialerRetries: 2, HeartbeatFrequency: time.Hour,
			DialerRetryBackoff: func(int) time.Duration {
				// Simulate a heartbeat rebalance between the failed dial attempts.
				ring.sharding.mu.Lock()
				shard := ring.sharding.shards.m["dead"]
				for range 3 {
					shard.Vote(false)
				}
				ring.sharding.rebalanceLocked()
				ring.sharding.mu.Unlock()
				return 0
			},
			MinRetryBackoff: -1, MaxRetryBackoff: -1,
			NewClient: func(opt *Options) *Client {
				client := NewClient(opt)
				if opt.Addr == dead {
					client.AddHook(fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
						dials.Add(1)
						return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("endpoint unavailable")}
					}})
				}
				return client
			}})
		t.Cleanup(func() { _ = ring.Close() })
		key := ""
		for i := 0; i < 1000; i++ {
			candidate := strings.Repeat("k", i+1)
			shard, err := ring.sharding.GetByKey(candidate)
			if err != nil {
				t.Fatal(err)
			}
			if shard.addr == dead {
				key = candidate
				break
			}
		}
		if key == "" {
			t.Fatal("could not find key on dead shard")
		}
		got, err := ring.Get(context.Background(), key).Result()
		if err != nil || got != "v" {
			t.Errorf("GET after ring rebalance = %q, %v; want v, nil", got, err)
		}
		if got := dials.Load(); got != 2 {
			t.Errorf("exhausted endpoint dials = %d, want 2", got)
		}
	})
	for _, tc := range []struct {
		name        string
		readOnly    bool
		metadata    bool
		wantReplica bool
	}{
		{name: "scan_replica", readOnly: true, metadata: true, wantReplica: true},
		{name: "scan_master_only", metadata: true},
		{name: "scan_default_policy", readOnly: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := newCSCPolicyServer(t, func(args []string) string {
				if strings.EqualFold(args[0], "scan") {
					return "*2\r\n$1\r\n0\r\n*1\r\n$1\r\nv\r\n"
				}
				return ""
			})
			const dead = "127.0.0.1:7301"
			c := NewClusterClient(&ClusterOptions{Addrs: []string{dead}, ReadOnly: tc.readOnly, ShardPicker: routing.NewStaticShardPicker(0), MaxRedirects: 3,
				PoolSize: 16, DialerRetries: 2, DialerRetryBackoff: func(int) time.Duration { return 0 }, MinRetryBackoff: -1, MaxRetryBackoff: -1})
			t.Cleanup(func() { _ = c.Close() })
			if tc.metadata {
				c.SetCommandInfoResolver(NewCommandMetadataResolver())
			}
			state, err := newClusterState(c.nodes, []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: dead}, {Addr: server.addr}}}}, "")
			if err != nil {
				t.Fatal(err)
			}
			c.state.state.Store(state)
			c.state.load = func(context.Context) (*clusterState, error) { return state, nil }
			var dials atomic.Int32
			state.Masters[0].Client.AddHook(clusterFixedDialFailureHook{fixedDialFailureHook: fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
				dials.Add(1)
				return nil, &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("endpoint unavailable")}
			}}, before: func(Cmder) {}})
			got, err := c.Do(context.Background(), "SCAN", 0).Slice()
			if tc.wantReplica && (err != nil || len(got) != 2) {
				t.Errorf("SCAN after master dial exhaustion = %v, %v; want replica page, nil", got, err)
			}
			if tc.wantReplica && err == nil && len(got) == 2 {
				keys, ok := got[1].([]interface{})
				if got[0] != "0" || !ok || len(keys) != 1 || keys[0] != "v" {
					t.Errorf("replica SCAN page = %v, want cursor 0 and key v", got)
				}
			}
			wantScans := 0
			if tc.wantReplica {
				wantScans = 1
			} else if err == nil {
				t.Error("SCAN succeeded without an eligible replacement")
			}
			if got := server.count("scan"); got != wantScans {
				t.Errorf("replica SCANs = %d, want %d", got, wantScans)
			}
			if got := dials.Load(); got != 2 {
				t.Errorf("exhausted endpoint dials = %d, want 2", got)
			}
		})
	}
}

func TestExhaustedEndpointRetryBounds(t *testing.T) {
	for _, command := range []string{"get", "mget"} {
		t.Run(command, func(t *testing.T) {
			for _, noRetry := range []bool{false, true} {
				name := "retryable"
				if noRetry {
					name = "no_retry"
				}
				t.Run(name, func(t *testing.T) {
					const dead = "127.0.0.1:7301"
					c := NewClusterClient(&ClusterOptions{Addrs: []string{dead}, MaxRedirects: 3, PoolSize: 16, DialerRetries: 2,
						DialerRetryBackoff: func(int) time.Duration { return 0 }, MinRetryBackoff: -1, MaxRetryBackoff: -1})
					t.Cleanup(func() { _ = c.Close() })
					c.SetCommandInfoResolver(NewCommandMetadataResolver())
					state, err := newClusterState(c.nodes, []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: dead}}}}, "")
					if err != nil {
						t.Fatal(err)
					}
					c.state.state.Store(state)
					var probes atomic.Int32
					c.state.load = func(context.Context) (*clusterState, error) {
						probes.Add(1)
						return state, nil
					}
					dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("endpoint unavailable")}
					var dials atomic.Int32
					state.Masters[0].Client.AddHook(fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
						dials.Add(1)
						return nil, dialErr
					}})
					ctx := context.Background()
					cmd := NewCmd(ctx, command, "key")
					cmd.setNoRetry(noRetry)
					if err := c.Process(ctx, cmd); err != dialErr {
						t.Errorf("returned error = %v, want original dial error", err)
					}
					if got := dials.Load(); got != 2 {
						t.Errorf("exhausted endpoint dials = %d, want 2", got)
					}
					if got := probes.Load(); got != 0 {
						t.Errorf("discovery probes = %d, want 0", got)
					}
				})
			}
		})
	}
}
