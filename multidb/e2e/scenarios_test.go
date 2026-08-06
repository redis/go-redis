package e2e

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestFailoverOnMemberOutage: traffic on the active member, then a hard stop.
// Commands must keep succeeding via the next-weight member.
// Spec: test_standalone_connection_failover.
func TestFailoverOnMemberOutage(t *testing.T) {
	farm := newProxyFarm(t)
	opts := fastMultiDBOptions(farm)

	var failoverFrom, failoverTo atomic.Int32
	failoverFrom.Store(-1)
	failoverTo.Store(-1)
	opts.OnFailover = func(_ context.Context, from, to int) {
		failoverFrom.Store(int32(from))
		failoverTo.Store(int32(to))
	}

	mdb := newE2EClient(t, opts)
	ctx := context.Background()

	if got := mdb.ActiveIndex(); got != 0 {
		t.Fatalf("initial active = %d, want 0 (highest weight)", got)
	}
	if err := mdb.Set(ctx, "e2e:failover", "before", 0).Err(); err != nil {
		t.Fatalf("Set before outage: %v", err)
	}

	farm.Stop(0)

	// The value written through member 0 must be visible through the new
	// active member BEFORE this test overwrites it (shared backend behind
	// every proxy ≈ a converged CRDB).
	eventually(t, 15*time.Second, "pre-failover data visible via the new active", func() bool {
		val, err := mdb.Get(ctx, "e2e:failover").Result()
		return err == nil && val == "before" && mdb.ActiveIndex() == 1
	})

	// Keep issuing commands; they must succeed again once failover lands.
	eventually(t, 15*time.Second, "commands succeeding on the new active", func() bool {
		return mdb.Set(ctx, "e2e:failover", "after", 0).Err() == nil && mdb.ActiveIndex() == 1
	})

	// Same backend behind every proxy: data written before the outage is
	// visible through the new member (converged-CRDB approximation).
	val, err := mdb.Get(ctx, "e2e:failover").Result()
	if err != nil || val != "after" {
		t.Fatalf("Get after failover: %q, %v", val, err)
	}
	// The callback runs after the active index is published (announce fires
	// outside the failover lock), so poll rather than assert immediately.
	eventually(t, 5*time.Second, "OnFailover(0 -> 1) callback", func() bool {
		return failoverFrom.Load() == 0 && failoverTo.Load() == 1
	})
}

// TestBackgroundDrivenFailover: NO command traffic; a paused (hung) active
// member must be detected by the background health checks alone.
// Spec: Q1/D2 — background-driven failover.
func TestBackgroundDrivenFailover(t *testing.T) {
	farm := newProxyFarm(t)
	mdb := newE2EClient(t, fastMultiDBOptions(farm))

	farm.Pause(0)

	eventually(t, 15*time.Second, "background failover with zero traffic", func() bool {
		return mdb.ActiveIndex() == 1
	})
}

// TestAutoFallbackToHigherWeight: after the highest-weight member recovers,
// the client must switch back without operator action.
// Spec: test_automatic_fallback.
func TestAutoFallbackToHigherWeight(t *testing.T) {
	farm := newProxyFarm(t)
	mdb := newE2EClient(t, fastMultiDBOptions(farm))
	ctx := context.Background()

	farm.Stop(0)
	eventually(t, 15*time.Second, "failover away from member 0", func() bool {
		return mdb.Set(ctx, "e2e:fallback", "x", 0).Err() == nil && mdb.ActiveIndex() != 0
	})

	farm.Start(0)
	// Recovery: grace period (2s) + health checks close the circuit +
	// fallback interval (3s).
	eventually(t, 30*time.Second, "fallback to the recovered member 0", func() bool {
		return mdb.ActiveIndex() == 0
	})
	if err := mdb.Get(ctx, "e2e:fallback").Err(); err != nil {
		t.Fatalf("Get after fallback: %v", err)
	}
}

// TestEscalationWhenAllMembersDown: with every member stopped the client
// reports temporary unavailability, then permanent after the attempt budget;
// restarting a member during the temporary phase recovers.
// Spec: test_all_databases_unreachable_error + escalation chain.
func TestEscalationWhenAllMembersDown(t *testing.T) {
	farm := newProxyFarm(t)
	mdb := newE2EClient(t, fastMultiDBOptions(farm))
	ctx := context.Background()

	farm.Stop(0)
	farm.Stop(1)
	farm.Stop(2)

	// Phase 1 — temporary unavailability, and recovery FROM the temporary
	// phase: "temporary" promises callers that retrying can still succeed,
	// so a member restarted during it must bring the client back without
	// ever reaching the terminal error.
	eventually(t, 20*time.Second, "temporary unavailability with all members down", func() bool {
		return errors.Is(mdb.Set(ctx, "e2e:esc", "x", 0).Err(), redis.ErrTemporarilyNotAvailable)
	})
	farm.Start(1)
	// Recovery must complete WITHIN the temporary phase: reaching the
	// terminal error while a member is already back means the attempt
	// budget was exhausted during the documented keep-retrying window.
	eventually(t, 20*time.Second, "recovery during the temporary phase", func() bool {
		err := mdb.Set(ctx, "e2e:esc", "y", 0).Err()
		if errors.Is(err, redis.ErrPermanentlyNotAvailable) {
			t.Fatalf("escalated to permanent during the temporary-phase recovery window")
		}
		return err == nil
	})

	// Phase 2 — escalation to the terminal error: with everything down
	// again, the attempt budget must run out and report permanent
	// unavailability (observing the temporary phase again on the way).
	farm.Stop(1)
	sawTemporary := false
	eventually(t, 30*time.Second, "escalation to permanent unavailability", func() bool {
		err := mdb.Set(ctx, "e2e:esc", "x", 0).Err()
		if errors.Is(err, redis.ErrTemporarilyNotAvailable) {
			sawTemporary = true
		}
		return errors.Is(err, redis.ErrPermanentlyNotAvailable)
	})
	if !sawTemporary {
		// The escalation contract is temporary-then-permanent; skipping the
		// temporary phase means callers never got the "keep retrying" signal.
		t.Error("escalated straight to permanent without ever reporting ErrTemporarilyNotAvailable")
	}

	// Recovery: one member back is enough for one_available-style operation
	// even after the terminal error was reported.
	farm.Start(1)
	eventually(t, 20*time.Second, "recovery after restart of member 1", func() bool {
		return mdb.Set(ctx, "e2e:esc", "y", 0).Err() == nil
	})
}

// TestManualFailover: SetActiveIndex refuses a stopped member with
// ErrTargetUnhealthy; ForceActiveIndex switches unconditionally.
// Spec: test_manual_failover_trigger / test_manual_failover_unhealthy_target.
func TestManualFailover(t *testing.T) {
	farm := newProxyFarm(t)
	mdb := newE2EClient(t, fastMultiDBOptions(farm))
	ctx := context.Background()

	farm.Stop(2)

	if err := mdb.SetActiveIndex(ctx, 2); !errors.Is(err, redis.ErrTargetUnhealthy) {
		t.Fatalf("SetActiveIndex to stopped member: err = %v, want ErrTargetUnhealthy", err)
	}
	if got := mdb.ActiveIndex(); got != 0 {
		t.Fatalf("active moved to %d after refused manual switch", got)
	}

	// Healthy target: probe-then-switch succeeds.
	if err := mdb.SetActiveIndex(ctx, 1); err != nil {
		t.Fatalf("SetActiveIndex to healthy member: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active = %d, want 1", got)
	}

	// Force onto the dead member: the switch must happen unconditionally
	// (asserted before any traffic can fail it back over), and the next
	// commands then drive an automatic failover away again.
	if err := mdb.ForceActiveIndex(ctx, 2); err != nil {
		t.Fatalf("ForceActiveIndex: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 2 {
		t.Fatalf("active = %d immediately after ForceActiveIndex(2)", got)
	}
	eventually(t, 15*time.Second, "automatic failover away from the forced dead member", func() bool {
		return mdb.Set(ctx, "e2e:manual", "x", 0).Err() == nil && mdb.ActiveIndex() != 2
	})
}

// TestPubSubFollowsActive: a subscription created through the MultiDB client
// keeps receiving messages after the active member dies, by re-dialing the
// new active member.
func TestPubSubFollowsActive(t *testing.T) {
	farm := newProxyFarm(t)
	mdb := newE2EClient(t, fastMultiDBOptions(farm))
	ctx := context.Background()

	sub := mdb.Subscribe(ctx, "e2e:channel")
	t.Cleanup(func() { _ = sub.Close() })
	// Bound the subscription handshake: a hung proxy must fail the scenario
	// promptly, not stall until the package timeout.
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	_, err := sub.Receive(rctx)
	rcancel()
	if err != nil {
		t.Fatalf("subscribe receive: %v", err)
	}
	msgs := sub.Channel()

	// Publisher through a member that stays alive (same backend bus).
	pub := redis.NewClient(memberOptions(farm, 2))
	t.Cleanup(func() { _ = pub.Close() })

	publishUntilReceived := func(tag string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if err := pub.Publish(ctx, "e2e:channel", tag).Err(); err != nil {
				// The publisher uses a member that stays up; a publish error
				// is a real problem, not an expected failover artifact.
				t.Logf("publish error (will retry): %v", err)
			}
			select {
			case m := <-msgs:
				if m.Payload == tag {
					return
				}
			case <-time.After(250 * time.Millisecond):
			}
		}
		t.Fatalf("message %q never received", tag)
	}

	publishUntilReceived("before-failover")

	farm.Stop(0)
	eventually(t, 15*time.Second, "failover away from member 0", func() bool {
		return mdb.ActiveIndex() != 0
	})

	publishUntilReceived("after-failover")
}

// TestPSubscribeFollowsActive: the pattern-subscription variant of the test
// above — psubscriptions must survive an active-member outage too.
func TestPSubscribeFollowsActive(t *testing.T) {
	farm := newProxyFarm(t)
	mdb := newE2EClient(t, fastMultiDBOptions(farm))
	ctx := context.Background()

	sub := mdb.PSubscribe(ctx, "e2e:pat:*")
	t.Cleanup(func() { _ = sub.Close() })
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	_, err := sub.Receive(rctx)
	rcancel()
	if err != nil {
		t.Fatalf("psubscribe receive: %v", err)
	}
	msgs := sub.Channel()

	pub := redis.NewClient(memberOptions(farm, 2))
	t.Cleanup(func() { _ = pub.Close() })

	publishUntilReceived := func(tag string) {
		t.Helper()
		deadline := time.Now().Add(20 * time.Second)
		for time.Now().Before(deadline) {
			if err := pub.Publish(ctx, "e2e:pat:1", tag).Err(); err != nil {
				t.Logf("publish error (will retry): %v", err)
			}
			select {
			case m := <-msgs:
				if m.Payload == tag {
					return
				}
			case <-time.After(250 * time.Millisecond):
			}
		}
		t.Fatalf("message %q never received", tag)
	}

	publishUntilReceived("pat-before-failover")

	farm.Stop(0)
	eventually(t, 15*time.Second, "failover away from member 0", func() bool {
		return mdb.ActiveIndex() != 0
	})

	publishUntilReceived("pat-after-failover")
}

// TestRuntimeMembershipUnderFaults: a member added at runtime must be a real
// failover target, and removing a (stopped, passive) member must keep the
// shifted indexes coherent.
// Spec: test_add_remove_database at runtime.
func TestRuntimeMembershipUnderFaults(t *testing.T) {
	farm := newProxyFarm(t)
	opts := fastMultiDBOptions(farm)
	opts.Clients = opts.Clients[:2] // start with members 0 and 1 only
	mdb := newE2EClient(t, opts)
	ctx := context.Background()

	idx, err := mdb.AddDatabase(ctx, redis.MultiDBClientConfig{
		Options: memberOptions(farm, 2),
		Weight:  1,
	})
	if err != nil {
		t.Fatalf("AddDatabase: %v", err)
	}
	if idx != 2 {
		t.Fatalf("AddDatabase index = %d, want 2", idx)
	}

	// With both original members down, traffic must land on the member that
	// only ever existed at runtime.
	farm.Stop(0)
	farm.Stop(1)
	eventually(t, 20*time.Second, "commands succeeding on the runtime-added member", func() bool {
		return mdb.Set(ctx, "e2e:member", "x", 0).Err() == nil && mdb.ActiveIndex() == 2
	})

	// Removing the stopped, passive member 0 shifts the slice; the active
	// member keeps serving under its new index.
	if err := mdb.RemoveDatabase(ctx, 0); err != nil {
		t.Fatalf("RemoveDatabase: %v", err)
	}
	if got := mdb.ActiveIndex(); got != 1 {
		t.Fatalf("active index after removal = %d, want 1 (shifted)", got)
	}
	if err := mdb.Set(ctx, "e2e:member", "y", 0).Err(); err != nil {
		t.Fatalf("Set after removal: %v", err)
	}
}

// TestSetWeightSteersFallback: a runtime weight change must redirect
// auto-fallback to the new heaviest healthy member.
// Spec: test_set_weight + auto-fallback interaction.
func TestSetWeightSteersFallback(t *testing.T) {
	farm := newProxyFarm(t)
	mdb := newE2EClient(t, fastMultiDBOptions(farm))
	ctx := context.Background()

	farm.Stop(0)
	eventually(t, 15*time.Second, "failover away from member 0", func() bool {
		return mdb.Set(ctx, "e2e:weight", "x", 0).Err() == nil && mdb.ActiveIndex() != 0
	})

	// Member 2 becomes the heaviest healthy member: the next fallback pass
	// must switch to it — not back toward the (still dead) member 0.
	if err := mdb.SetWeight(2, 10); err != nil {
		t.Fatalf("SetWeight: %v", err)
	}
	eventually(t, 30*time.Second, "fallback to the re-weighted member 2", func() bool {
		return mdb.ActiveIndex() == 2
	})
	if err := mdb.Get(ctx, "e2e:weight").Err(); err != nil {
		t.Fatalf("Get on the re-weighted member: %v", err)
	}
}

// TestConcurrentTrafficAcrossFailover: parallel writers must all converge on
// the new active member after an outage — no goroutine may be left behind on
// a stale snapshot or wedged on the dead member.
func TestConcurrentTrafficAcrossFailover(t *testing.T) {
	farm := newProxyFarm(t)
	mdb := newE2EClient(t, fastMultiDBOptions(farm))
	ctx := context.Background()

	const workers = 8
	var stop atomic.Bool
	var postFailover [workers]atomic.Int64
	var wg sync.WaitGroup
	// Deferred (not just at the happy end): an eventually() failure calls
	// t.Fatal, and workers still running while t.Cleanup closes the client
	// would race the teardown.
	defer func() {
		stop.Store(true)
		wg.Wait()
	}()
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := fmt.Sprintf("e2e:conc:%d", g)
			for !stop.Load() {
				if err := mdb.Set(ctx, key, "v", 0).Err(); err == nil && mdb.ActiveIndex() == 1 {
					postFailover[g].Add(1)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}(g)
	}

	time.Sleep(time.Second) // steady-state traffic on member 0 first
	farm.Stop(0)

	eventually(t, 20*time.Second, "every worker succeeding on the new active", func() bool {
		for g := range postFailover {
			if postFailover[g].Load() < 5 {
				return false
			}
		}
		return true
	})
	stop.Store(true)
	wg.Wait()
}

// TestInitialAllAvailableRefusesDownMember: with the all_available policy and
// no init deadline, a down member must fail construction immediately.
// Spec: test_initialization_with_unavailable_database.
func TestInitialAllAvailableRefusesDownMember(t *testing.T) {
	farm := newProxyFarm(t)
	farm.Stop(2)

	opts := fastMultiDBOptions(farm)
	mdb, err := redis.NewMultiDBClient(context.Background(), opts) // no deadline: single pass
	if err == nil {
		_ = mdb.Close()
		t.Fatal("NewMultiDBClient succeeded with a down member under all_available")
	}
	if !errors.Is(err, redis.ErrInsufficientHealthyDatabases) {
		t.Fatalf("err = %v, want ErrInsufficientHealthyDatabases", err)
	}
}

// TestFailoverCallbacksObserved: an outage-driven failover must surface both
// the active-change callback and the breaker-open callback for the dead
// member.
func TestFailoverCallbacksObserved(t *testing.T) {
	farm := newProxyFarm(t)
	var activeChanged, circuitOpened atomic.Bool
	opts := fastMultiDBOptions(farm)
	opts.OnActiveDatabaseChanged = func(from, to int) {
		if from == 0 && to == 1 {
			activeChanged.Store(true)
		}
	}
	opts.OnCircuitStateChanged = func(dbIndex int, from, to string) {
		if dbIndex == 0 && to == "open" {
			circuitOpened.Store(true)
		}
	}
	mdb := newE2EClient(t, opts)
	ctx := context.Background()

	farm.Stop(0)
	eventually(t, 15*time.Second, "failover away from member 0", func() bool {
		return mdb.Set(ctx, "e2e:cb", "x", 0).Err() == nil && mdb.ActiveIndex() == 1
	})
	// Both callbacks are delivered asynchronously — poll.
	eventually(t, 5*time.Second, "failover callbacks", func() bool {
		return activeChanged.Load() && circuitOpened.Load()
	})
}
