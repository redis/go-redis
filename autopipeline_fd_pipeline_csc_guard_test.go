package redis

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"
)

// An FD pipeline on a client-side-caching client refuses the commands that
// would change the held connection's state (SELECT, AUTH, HELLO with a
// protocol, RESET, CLIENT TRACKING), as an ordinary pipeline does. They leave
// the pipe (runsOutsidePipeline), so Pipeline() falls back to the ordinary
// pipeline, whose CSC guard rejects them, and FDPipelined refuses the batch.
// The held connection stays on DB 0.
func TestFDPipelineRejectsCSCStateCommands(t *testing.T) {
	ctx := context.Background()
	if err := probeRedis(internalTestRedisAddr()); err != nil {
		t.Skipf("no redis: %v", err)
	}
	c := NewClient(&Options{
		Addr:                  internalTestRedisAddr(),
		Protocol:              3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 128},
	})
	defer c.Close()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if !c.autopipelineCSCActive() {
		t.Skip("client-side caching did not attach (server lacks CLIENT TRACKING?)")
	}
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	defer ap.Close()
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}

	key := "fd-csc-guard:" + strconv.FormatInt(time.Now().UnixNano(), 10)
	plain := NewClient(&Options{Addr: internalTestRedisAddr()})
	defer plain.Close()
	if err := plain.Set(ctx, key, "db0", 0).Err(); err != nil {
		t.Fatalf("seed: %v", err)
	}
	defer plain.Del(ctx, key)

	for _, tc := range []struct {
		args []interface{}
		want error
	}{
		{[]interface{}{"select", 1}, errSelectWithCSC},
		{[]interface{}{"SELECT", 1}, errSelectWithCSC},
		{[]interface{}{"auth", "user", "pass"}, errAuthWithCSC},
		{[]interface{}{"hello", 2}, errHelloWithCSC},
		{[]interface{}{"reset"}, errResetWithCSC},
		{[]interface{}{"client", "tracking", "off"}, errClientTrackingWithCSC},
	} {
		pipe := ap.Pipeline()
		cmd := pipe.Do(ctx, tc.args...)
		if _, err := pipe.Exec(ctx); !errors.Is(err, tc.want) {
			t.Fatalf("Pipeline %v: Exec err=%v, want %v", tc.args, err, tc.want)
		}
		if !errors.Is(cmd.Err(), tc.want) {
			t.Fatalf("Pipeline %v: cmd err=%v, want %v", tc.args, cmd.Err(), tc.want)
		}
		if err := ap.FDPipelined(ctx, []Cmder{NewCmd(ctx, tc.args...)}); !errors.Is(err, ErrFDPipelineDiverts) {
			t.Fatalf("FDPipelined %v: err=%v, want ErrFDPipelineDiverts", tc.args, err)
		}
	}

	// The held connection must still be on DB 0.
	pipe := ap.Pipeline()
	get := pipe.Get(ctx, key)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("GET after the refused SELECT: %v", err)
	}
	if v := get.Val(); v != "db0" {
		t.Fatalf("GET %s = %q on the FD pipe, want db0: the connection changed DB", key, v)
	}
}
