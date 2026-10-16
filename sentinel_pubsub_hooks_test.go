package redis

import (
	"context"
	"net"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
)

// TestFailoverPubSubHooksRetireOnMasterSwitch drives the failover
// client's pub/sub retirement through the real onFailover callback: a
// connection dialed under one master must be retired once Sentinel
// switches to another, and never by the initial master discovery.
func TestFailoverPubSubHooksRetireOnMasterSwitch(t *testing.T) {
	ctx := context.Background()

	// newConn simulates a pub/sub dial: the connection is tagged with
	// the generation current at dial time, as pubsubConnect does.
	newConn := func(t *testing.T, h *failoverPubSubHooks) *pool.Conn {
		t.Helper()
		client, server := net.Pipe()
		t.Cleanup(func() { _ = client.Close(); _ = server.Close() })
		cn := pool.NewConn(client)
		h.connGen.Store(cn, h.gen.Load())
		return cn
	}
	// setup also counts recheck calls: a switch must ask the manager at
	// once, so an idle connection moves without waiting for a frame.
	setup := func(replicaOnly bool, discovered string) (*sentinelFailover, *failoverPubSubHooks, *atomic.Int32) {
		failover := &sentinelFailover{
			opt:        &FailoverOptions{MasterName: "mymaster"},
			masterAddr: discovered,
		}
		c := &baseClient{}
		rechecks := &atomic.Int32{}
		h := &failoverPubSubHooks{
			baseClient:  c,
			replicaOnly: replicaOnly,
			recheck:     func() { rechecks.Add(1) },
		}
		installOnFailover(failover, c, h)
		return failover, h, rechecks
	}

	t.Run("master discovered before the callback was installed", func(t *testing.T) {
		// A MinIdleConns pool dial resolved the master during
		// construction, before onFailover existed: the first real switch
		// must still retire the connection.
		failover, h, rechecks := setup(false, "10.0.0.1:6379")
		if got := rechecks.Load(); got != 0 {
			t.Fatalf("seeding the discovered master rechecked %d times, want 0", got)
		}
		cn := newConn(t, h)

		failover.trySwitchMaster(ctx, "10.0.0.2:6379")
		if !h.pubsubShouldReplaceConn(cn) {
			t.Fatal("connection on the demoted master was not retired")
		}
		if got := rechecks.Load(); got != 1 {
			t.Fatalf("master switch rechecked %d times, want 1", got)
		}
	})

	t.Run("initial discovery retires nothing", func(t *testing.T) {
		failover, h, rechecks := setup(false, "")
		cn := newConn(t, h)

		failover.trySwitchMaster(ctx, "10.0.0.1:6379")
		if h.pubsubShouldReplaceConn(cn) {
			t.Fatal("initial master discovery retired the connection")
		}
		if got := rechecks.Load(); got != 0 {
			t.Fatalf("initial discovery rechecked %d times, want 0", got)
		}
		failover.trySwitchMaster(ctx, "10.0.0.2:6379")
		if !h.pubsubShouldReplaceConn(cn) {
			t.Fatal("connection on the demoted master was not retired")
		}
		if got := rechecks.Load(); got != 1 {
			t.Fatalf("master switch rechecked %d times, want 1", got)
		}
		if fresh := newConn(t, h); h.pubsubShouldReplaceConn(fresh) {
			t.Fatal("connection dialed after the switch was retired")
		}
	})

	t.Run("replica-only never retires on a master switch", func(t *testing.T) {
		failover, h, rechecks := setup(true, "10.0.0.1:6379")
		cn := newConn(t, h)

		failover.trySwitchMaster(ctx, "10.0.0.2:6379")
		if h.pubsubShouldReplaceConn(cn) {
			t.Fatal("replica-only connection was retired on a master switch")
		}
		if got := rechecks.Load(); got != 0 {
			t.Fatalf("replica-only master switch rechecked %d times, want 0", got)
		}
	})
}
