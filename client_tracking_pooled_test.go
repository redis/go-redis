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
