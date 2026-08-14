package redis

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// TestPooledClientTrackingPreservesArgs verifies the pooled-client CLIENT
// TRACKING / MAINT_NOTIFICATIONS wrappers build the full argument list (mirroring
// the stateful command) before failing with guidance, instead of dropping the
// caller's options (#3961). The command is pre-failed without dispatch, so no
// server is needed.
func TestPooledClientTrackingPreservesArgs(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{Addr: ":6379"})
	defer c.Close()

	opt := &ClientTrackingOptions{Redirect: 42, Bcast: true, Prefixes: []string{"foo"}, NoLoop: true}

	cases := []struct {
		name string
		cmd  *StatusCmd
		err  error
		want []interface{}
	}{
		{
			name: "ClientTrackingOn",
			cmd:  c.ClientTrackingOn(ctx, opt),
			err:  errClientTrackingOnPooledClient,
			want: []interface{}{"client", "tracking", "on", "redirect", int64(42), "bcast", "prefix", "foo", "noloop"},
		},
		{
			name: "ClientTracking(on)",
			cmd:  c.ClientTracking(ctx, true, opt),
			err:  errClientTrackingOnPooledClient,
			want: []interface{}{"client", "tracking", "on", "redirect", int64(42), "bcast", "prefix", "foo", "noloop"},
		},
		{
			name: "ClientMaintNotifications(on)",
			cmd:  c.ClientMaintNotifications(ctx, true, "external"),
			err:  errClientMaintNotificationsOnPooledClient,
			want: []interface{}{"client", "maint_notifications", "on", "moving-endpoint-type", "external"},
		},
		{
			name: "ClientMaintNotifications(off)",
			cmd:  c.ClientMaintNotifications(ctx, false, ""),
			err:  errClientMaintNotificationsOnPooledClient,
			want: []interface{}{"client", "maint_notifications", "off"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if !errors.Is(tc.cmd.Err(), tc.err) {
				t.Fatalf("Err() = %v, want %v", tc.cmd.Err(), tc.err)
			}
			if got := tc.cmd.Args(); !reflect.DeepEqual(got, tc.want) {
				t.Fatalf("Args() = %v, want %v (options dropped)", got, tc.want)
			}
		})
	}
}

// TestPooledClientMaintNotificationsDefaultEndpoint verifies the empty endpoint
// type defaults to "none", matching the stateful command.
func TestPooledClientMaintNotificationsDefaultEndpoint(t *testing.T) {
	c := NewClient(&Options{Addr: ":6379"})
	defer c.Close()
	cmd := c.ClientMaintNotifications(context.Background(), true, "")
	want := []interface{}{"client", "maint_notifications", "on", "moving-endpoint-type", "none"}
	if got := cmd.Args(); !reflect.DeepEqual(got, want) {
		t.Fatalf("Args() = %v, want %v", got, want)
	}
}

// TestPipelineRejectsStateCommandsWhenPooled verifies per-connection state
// commands (CLIENT TRACKING / MAINT_NOTIFICATIONS) are rejected in a POOLED
// pipeline — whose borrowed connection returns to the pool after Exec — but
// allowed (queued) in a pipeline from a dedicated *Conn (#3961). No server needed:
// rejections are pre-failed, and the Conn pipeline only queues.
func TestPipelineRejectsStateCommandsWhenPooled(t *testing.T) {
	ctx := context.Background()
	// localhost:1 is never dialed: the pooled-pipeline guard in
	// generalProcessPipeline fires before any connection is acquired.
	c := NewClient(&Options{Addr: "localhost:1"})
	defer c.Close()

	// Pooled pipeline: the returned command carries the guidance error AND the
	// command is queued, so Exec surfaces the rejection even when the caller
	// ignores the returned command. The command must never be sent to a borrowed
	// pooled connection.
	pp := c.Pipeline()
	if cmd := pp.ClientTrackingOn(ctx, &ClientTrackingOptions{Bcast: true}); !errors.Is(cmd.Err(), errClientTrackingOnPooledClient) {
		t.Fatalf("pooled Pipeline ClientTrackingOn err = %v, want errClientTrackingOnPooledClient", cmd.Err())
	}
	if cmd := pp.ClientTrackingOff(ctx); !errors.Is(cmd.Err(), errClientTrackingOnPooledClient) {
		t.Fatalf("pooled Pipeline ClientTrackingOff err = %v, want reject", cmd.Err())
	}
	if cmd := pp.ClientMaintNotifications(ctx, true, "none"); !errors.Is(cmd.Err(), errClientMaintNotificationsOnPooledClient) {
		t.Fatalf("pooled Pipeline ClientMaintNotifications err = %v, want reject", cmd.Err())
	}
	if pp.Len() != 3 {
		t.Fatalf("pooled Pipeline queued %d state commands, want 3 (queued so Exec surfaces the rejection)", pp.Len())
	}
	// Exec surfaces the rejection through the guard, before any dial (this would
	// otherwise fail dialing localhost:1 with a different error).
	if _, err := pp.Exec(ctx); !errors.Is(err, errClientTrackingOnPooledClient) {
		t.Fatalf("pooled Pipeline Exec err = %v, want errClientTrackingOnPooledClient (guarded before dial)", err)
	}

	// The common Pipelined pattern — callback ignores the returned command — must
	// still fail rather than silently reporting success (the dropped-error bug).
	if _, err := c.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientTrackingOff(ctx)
		return nil
	}); !errors.Is(err, errClientTrackingOnPooledClient) {
		t.Fatalf("Pipelined ClientTrackingOff err = %v, want errClientTrackingOnPooledClient", err)
	}

	// Discard drops queued rejections like any other queued command.
	pp2 := c.Pipeline()
	pp2.ClientTrackingOff(ctx)
	pp2.Discard()
	if _, err := pp2.Exec(ctx); err != nil {
		t.Fatalf("after Discard, Exec err = %v, want nil (empty pipeline)", err)
	}

	// Dedicated-Conn pipeline: sticky → allowed → queued (state stays on the conn).
	conn := c.Conn()
	defer conn.Close()
	cp := conn.Pipeline()
	if cmd := cp.ClientTrackingOn(ctx, &ClientTrackingOptions{Bcast: true}); errors.Is(cmd.Err(), errClientTrackingOnPooledClient) {
		t.Fatalf("Conn Pipeline wrongly rejected ClientTrackingOn: %v", cmd.Err())
	}
	if cp.Len() != 1 {
		t.Fatalf("Conn Pipeline queued %d, want 1 (state command should be queued on a dedicated conn)", cp.Len())
	}
}

// TestClusterPipelineRejectsStateCommands pins the cluster-side gap (#3961
// review): ClusterClient.processPipeline/processTxPipeline bypass
// baseClient.generalProcessPipeline, so they must check stateRejectedErr
// themselves — otherwise the queued CLIENT TRACKING runs on a borrowed node
// connection and the server reply overwrites the guidance error. The guard
// fires before any node mapping, so no cluster (or dial) is needed.
func TestClusterPipelineRejectsStateCommands(t *testing.T) {
	ctx := context.Background()
	cc := NewClusterClient(&ClusterOptions{Addrs: []string{"localhost:1"}})
	defer cc.Close()

	if _, err := cc.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientTrackingOff(ctx)
		return nil
	}); !errors.Is(err, errClientTrackingOnPooledClient) {
		t.Fatalf("cluster Pipelined ClientTrackingOff err = %v, want errClientTrackingOnPooledClient", err)
	}

	if _, err := cc.TxPipelined(ctx, func(pipe Pipeliner) error {
		pipe.ClientMaintNotifications(ctx, true, "none")
		return nil
	}); !errors.Is(err, errClientMaintNotificationsOnPooledClient) {
		t.Fatalf("cluster TxPipelined ClientMaintNotifications err = %v, want errClientMaintNotificationsOnPooledClient", err)
	}
}

// TestTxPipelineAllowsStateCommands pins that a Tx pipeline is sticky: WATCH
// pins one connection for the whole Tx, so CLIENT TRACKING queues there instead
// of being rejected as pooled (#3961 regression flagged by review). Needs a
// server because Watch dials.
func TestTxPipelineAllowsStateCommands(t *testing.T) {
	ctx := context.Background()
	c := NewClient(&Options{Addr: ":6379"})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	err := c.Watch(ctx, func(tx *Tx) error {
		for _, p := range []Pipeliner{tx.Pipeline(), tx.TxPipeline()} {
			cmd := p.ClientTrackingOn(ctx, nil)
			if errors.Is(cmd.Err(), errClientTrackingOnPooledClient) {
				t.Fatalf("Tx pipeline wrongly rejected CLIENT TRACKING (should be sticky): %v", cmd.Err())
			}
			if p.Len() != 1 {
				t.Fatalf("Tx pipeline queued %d, want 1 (state command allowed on the pinned conn)", p.Len())
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
}
