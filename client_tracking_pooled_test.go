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
	c := NewClient(&Options{Addr: ":6379"})
	defer c.Close()

	// Pooled pipeline: rejected, and NOT queued.
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
	if pp.Len() != 0 {
		t.Fatalf("pooled Pipeline queued %d rejected state commands, want 0", pp.Len())
	}

	// Dedicated-Conn pipeline: allowed → queued.
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
