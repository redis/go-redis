package e2e

import (
	"context"
	"fmt"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Matrix rows: topology_change / endpoint_removed_added, endpoint_removed,
// endpoint_added, slot_shuffle, without SCH.
//
// The with-SCH counterpart is TestClusterSlotMigrate_AllEffects: its client
// gets SMIGRATING and SMIGRATED and reloads the topology on them. Here the
// maintenance notifications are disabled, so the cluster client only learns
// about the move from the redirections and errors it meets, the way a client
// against a server without the feature does.
//
// Assertions: no hang, no lost data, and traffic works again on the SAME client.
// Data is checked with many commands (see data_verify_test.go): a data set
// seeded before the change (GET/SET, HSET/HGET/HGETALL, MSET/MGET, counters,
// TTL and PERSIST, sets, lists, sorted sets, DEL) must read back exactly, and
// every write acknowledged during the move must still be there. The number of
// failed commands during the move is logged, not asserted.
//
// Real fault injector only: the proxy mock injects notifications but moves no
// data, so there is nothing for a client without notifications to follow.

// withoutSCHEffects is one trigger per effect. The with-SCH test covers every
// trigger; each case here provisions a database, so one keeps the package
// inside its time budget.
var withoutSCHEffects = []struct {
	name    string
	effect  SlotMigrateEffect
	trigger SlotMigrateVariant
}{
	{"SlotShuffle_Migrate", SlotMigrateEffectSlotShuffle, SlotMigrateVariantMigrate},
	{"Add_Migrate", SlotMigrateEffectAdd, SlotMigrateVariantMigrate},
	{"RemoveAdd_Migrate", SlotMigrateEffectRemoveAdd, SlotMigrateVariantMigrate},
	{"Remove_Migrate", SlotMigrateEffectRemove, SlotMigrateVariantMigrate},
}

func TestClusterSlotMigrate_WithoutSCH(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}
	if GetTestMode() != TestModeRealFaultInjector {
		t.Skip("needs a real fault injector: the proxy mock injects notifications but moves no data")
	}

	for _, tc := range withoutSCHEffects {
		t.Run(tc.name, func(t *testing.T) {
			runClusterSlotMigrateWithoutSCH(t, tc.effect, tc.trigger)
		})
	}
}

// requireClusterSCHOff fails when any shard client of c has a
// maintenance-notification manager.
func requireClusterSCHOff(t *testing.T, c *redis.ClusterClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	// ForEachShard runs the callback on every node at the same time, so the
	// counter must be atomic. t.Errorf is safe to call concurrently.
	var shards atomic.Int32
	err := c.ForEachShard(ctx, func(ctx context.Context, node *redis.Client) error {
		shards.Add(1)
		if node.GetMaintNotificationsManager() != nil {
			t.Errorf("shard %s has a maintenance-notification manager, SCH is not off", node.Options().Addr)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("visiting shards: %v", err)
	}
	if shards.Load() == 0 {
		t.Fatal("no shards visited")
	}
}

func runClusterSlotMigrateWithoutSCH(t *testing.T, effect SlotMigrateEffect, trigger SlotMigrateVariant) {
	t.Helper()
	ctx := context.Background()

	bdbID, factory, testMode, fiClient, cleanup := SetupTestDatabaseForSlotMigrate(t, ctx, effect, trigger)
	defer cleanup()
	t.Logf("database %d for effect %s, trigger %s (mode: %s)", bdbID, effect, trigger, testMode.Mode)

	// Count the topology reloads the client logs, as information on how it
	// recovered.
	localLogCollector := NewTestLogCollector()
	localLogCollector.Clear()
	if debugE2E() {
		localLogCollector.DoPrint()
	}
	redis.SetLogger(localLogCollector)
	defer redis.SetLogger(logCollector)

	clientIface, err := factory.Create("without-sch", &CreateClientOptions{
		Protocol:                 3,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	if err != nil {
		t.Fatalf("create client: %v", err)
	}
	c := clientIface.(*redis.ClusterClient)
	defer c.Close()

	if err := c.Ping(ctx).Err(); err != nil {
		t.Fatalf("connect: %v", err)
	}
	// Discover every shard first, then check none of them runs SCH.
	requireClusterSCHOff(t, c)

	// Data written before the change, with many different commands, must read
	// back after it. The data set is spread over the keyspace, so it lives on
	// every shard.
	prefix := fmt.Sprintf("e2e:wosch:%d:%d", bdbID, time.Now().UnixNano())
	data := newDataVerifier(prefix)
	if err := data.seed(ctx, c); err != nil {
		t.Fatalf("seed: %v", err)
	}
	if err := data.verify(ctx, c); err != nil {
		t.Fatalf("data set is wrong before the change: %v", err)
	}

	// Mixed traffic over its own keys keeps every shard busy across the move.
	// Failures during the move are expected, how long they last is logged, and
	// every write the client got an acknowledgement for must survive.
	live := newLiveLoad(prefix)
	stopLive := live.run(ctx, c)
	var stats liveLoadStats
	stopped := false
	stopLoad := func() {
		if stopped {
			return
		}
		stopped = true
		var finished bool
		stats, finished = stopLive()
		if !finished {
			t.Error("background load never stopped: a command is stuck")
		}
	}
	// Deferred so a t.Fatalf on any later line (a recovery timeout, a failed
	// trigger) still stops the goroutine before the client is closed and the
	// database torn down.
	defer stopLoad()

	resp, err := fiClient.TriggerSlotMigrate(ctx, SlotMigrateRequest{
		Effect:  effect,
		BdbID:   fmt.Sprintf("%d", bdbID),
		Trigger: trigger,
	})
	if err != nil {
		stopLoad()
		t.Fatalf("trigger slot migration: %v", err)
	}
	st, err := fiClient.WaitForAction(ctx, resp.ActionID,
		WithMaxWaitTime(testMode.ActionWaitTimeout),
		WithPollInterval(testMode.ActionPollInterval))
	if err != nil {
		stopLoad()
		t.Fatalf("wait for action %s: %v", resp.ActionID, err)
	}
	if st.Status != StatusSuccess && st.Status != StatusFinished {
		stopLoad()
		t.Fatalf("action %s ended %s: %v", resp.ActionID, st.Status, st.Error)
	}

	// Traffic works again on the same client, with nothing but its own
	// redirection handling to get it there.
	standaloneEventually(t, 90*time.Second, "writes to succeed again", func() bool {
		wctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return c.Set(wctx, prefix+":after", "ok", 0).Err() == nil
	})
	stopLoad()

	// No lost data: the seeded data set reads back exactly as written, and every
	// write that was acknowledged during the move is still there.
	if err := data.verifyEventually(ctx, c, 60*time.Second); err != nil {
		t.Errorf("seeded data set after the change: %v", err)
	}
	if err := live.verify(ctx, c); err != nil {
		t.Errorf("acknowledged writes after the change: %v", err)
	}

	// SCH stayed off, including on shards the client discovered during the move.
	requireClusterSCHOff(t, c)

	t.Logf("without_sch/%s/%s: %d ok, %d failed steps, longest failure window %v, topology reloads %d",
		effect, trigger, stats.OK, stats.Failed, stats.LongestFailure, localLogCollector.GetAnalysis().ClusterStateReloadCount)
}
