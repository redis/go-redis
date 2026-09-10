package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// skipIfRealMode marks a test mock-only: it drives docker directly
// (proxyFarm) rather than through the fault-injector protocol, so it cannot
// run against a real Active-Active deployment. Real mode is signaled the
// same way TestMain detects it: FAULT_INJECTION_API_URL set.
func skipIfRealMode(t *testing.T, why string) {
	t.Helper()
	if os.Getenv("FAULT_INJECTION_API_URL") != "" {
		t.Skip("mock-only: " + why)
	}
}

// TestFailoverOnMemberOutage: traffic on the active member, then a hard
// outage. Commands must keep succeeding via the next-weight member.
// Spec: test_standalone_connection_failover.
func TestFailoverOnMemberOutage(t *testing.T) {
	opts := fiMultiDBOptions()

	var failoverFrom, failoverTo atomic.Int32
	failoverFrom.Store(-1)
	failoverTo.Store(-1)
	opts.OnFailover = func(_ context.Context, from, to int) {
		failoverFrom.Store(int32(from))
		failoverTo.Store(int32(to))
	}

	mdb := newE2EClient(t, opts)
	ctx := context.Background()

	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("initial active = %d, want 0 (highest weight)", got)
	}
	if err := mdb.Set(ctx, "e2e:failover", "before", 0).Err(); err != nil {
		t.Fatalf("Set before outage: %v", err)
	}

	// Outlasts every assertion below with margin; cleanup waits for the
	// action's own timer to restore it before the next test starts.
	triggerNetworkFailure(t, 0, 60*time.Second)

	// The value written through member 0 must be visible through the new
	// active member BEFORE this test overwrites it (shared backend behind
	// every proxy ≈ a converged CRDB).
	eventually(t, 15*time.Second, "pre-failover data visible via the new active", func() bool {
		val, err := mdb.Get(ctx, "e2e:failover").Result()
		return err == nil && val == "before" && mdb.ActiveDatabaseID() == 1
	})

	// Keep issuing commands; they must succeed again once failover lands.
	eventually(t, 15*time.Second, "commands succeeding on the new active", func() bool {
		return mdb.Set(ctx, "e2e:failover", "after", 0).Err() == nil && mdb.ActiveDatabaseID() == 1
	})

	// Same backend behind every proxy: data written before the outage is
	// visible through the new member (converged-CRDB approximation).
	val, err := mdb.Get(ctx, "e2e:failover").Result()
	if err != nil || val != "after" {
		t.Fatalf("Get after failover: %q, %v", val, err)
	}
	// The callback runs after the active id is published (announce fires
	// outside the failover lock), so poll rather than assert immediately.
	eventually(t, 5*time.Second, "OnFailover(0 -> 1) callback", func() bool {
		return failoverFrom.Load() == 0 && failoverTo.Load() == 1
	})
}

// TestBackgroundDrivenFailover: NO command traffic; a paused (hung) active
// member must be detected by the background health checks alone.
// Spec: Q1/D2 — background-driven failover.
//
// Mock-only: `network_failure`'s default mechanism (docker stop/start)
// refuses rather than hangs, so it cannot reproduce this fault. It becomes
// portable if MULTIDB_E2E_FI_MECHANISM=iptables is verified and adopted
// (iptables blackholes rather than refuses, same as docker pause) — see the
// design doc. Until then this drives proxyFarm.Pause directly, which needs
// docker regardless of the FI mechanism setting.
func TestBackgroundDrivenFailover(t *testing.T) {
	skipIfRealMode(t, "docker-pause hang semantics have no real-FI equivalent under the default mechanism")
	farm := newProxyFarm(t)
	mdb := newE2EClient(t, fastMultiDBOptions(farm))

	farm.Pause(0)

	eventually(t, 15*time.Second, "background failover with zero traffic", func() bool {
		return mdb.ActiveDatabaseID() == 1
	})
}

// TestAutoFallbackToHigherWeight: after the highest-weight member recovers,
// the client must switch back without operator action.
// Spec: test_automatic_fallback.
//
// Recovery here is `network_failure`'s own delay timer, not a second call:
// the action restores the member automatically once delay elapses. delay is
// tuned to safely exceed the failover-detection window (~2-4s under these
// timings) so the failover-away assertion below cannot pass by the member
// already being back.
func TestAutoFallbackToHigherWeight(t *testing.T) {
	mdb := newE2EClient(t, fiMultiDBOptions())
	ctx := context.Background()

	const delay = 15 * time.Second
	actionID := triggerNetworkFailure(t, 0, delay)
	eventually(t, 10*time.Second, "failover away from member 0", func() bool {
		return mdb.Set(ctx, "e2e:fallback", "x", 0).Err() == nil && mdb.ActiveDatabaseID() != 0
	})

	// This assertion is specifically about post-restore behavior, unlike
	// the outage-observation assertions elsewhere in this file: wait for
	// the action to actually finish restoring member 0 first.
	waitForActionDone(t, actionID, delay+30*time.Second)

	// Recovery: grace period (2s) + health checks close the circuit +
	// fallback interval (3s).
	eventually(t, 30*time.Second, "fallback to the recovered member 0", func() bool {
		return mdb.ActiveDatabaseID() == 0
	})
	if err := mdb.Get(ctx, "e2e:fallback").Err(); err != nil {
		t.Fatalf("Get after fallback: %v", err)
	}
}

// TestEscalationWhenAllMembersDown: with every member down the client
// reports temporary unavailability, then permanent after the attempt budget;
// restarting a member during the temporary phase recovers.
// Spec: test_all_databases_unreachable_error + escalation chain.
//
// Mock-only: expressible for real mode only via multiple stacked
// network_failure calls with independently tuned delays (short on the
// member meant to recover early, long on the other two, plus a second wave
// timed to land before the first wave's restores) — not a fundamental
// protocol mismatch, but three-way timing choreography tight enough to be
// jitter-sensitive in CI. Not worth burning the port on; drives proxyFarm
// directly instead.
func TestEscalationWhenAllMembersDown(t *testing.T) {
	skipIfRealMode(t, "needs test-controlled mid-chain restarts; expressible for real mode only via jitter-sensitive multi-call timing choreography")
	farm := newProxyFarm(t)
	opts := fastMultiDBOptions(farm)
	// A larger attempt budget than the harness default: the temporary phase
	// must comfortably outlast a docker start (~1-2s), or the strict
	// no-permanent-during-recovery assertion below flakes on the legitimate
	// budget-exhaustion boundary (4 x 500ms was too tight).
	opts.MaxFailoverAttempts = 10
	mdb := newE2EClient(t, opts)
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

// TestManualFailover: SetActiveDatabase refuses a down member with
// ErrTargetUnhealthy; ForceActiveDatabase switches unconditionally.
// Spec: test_manual_failover_trigger / test_manual_failover_unhealthy_target.
//
// Needs a distinct id for "down", "healthy target", and "currently active",
// so it needs at least 3 members.
func TestManualFailover(t *testing.T) {
	if len(e2eTopology.Endpoints) < 3 {
		t.Skip("needs at least 3 members (distinct ids for down / healthy-target / active)")
	}
	mdb := newE2EClient(t, fiMultiDBOptions())
	ctx := context.Background()

	dead := len(e2eTopology.Endpoints) - 1
	const healthy = 1

	triggerNetworkFailure(t, dead, 60*time.Second)

	if err := mdb.SetActiveDatabase(ctx, dead); !errors.Is(err, redis.ErrTargetUnhealthy) {
		t.Fatalf("SetActiveDatabase to down member: err = %v, want ErrTargetUnhealthy", err)
	}
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("active moved to %d after refused manual switch", got)
	}

	// Healthy target: probe-then-switch succeeds.
	if err := mdb.SetActiveDatabase(ctx, healthy); err != nil {
		t.Fatalf("SetActiveDatabase to healthy member: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != healthy {
		t.Fatalf("active = %d, want %d", got, healthy)
	}

	// Force onto the dead member: the switch must happen unconditionally
	// (asserted before any traffic can fail it back over), and the next
	// commands then drive an automatic failover away again.
	if err := mdb.ForceActiveDatabase(ctx, dead); err != nil {
		t.Fatalf("ForceActiveDatabase: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != dead {
		t.Fatalf("active = %d immediately after ForceActiveDatabase(%d)", got, dead)
	}
	eventually(t, 15*time.Second, "automatic failover away from the forced dead member", func() bool {
		return mdb.Set(ctx, "e2e:manual", "x", 0).Err() == nil && mdb.ActiveDatabaseID() != dead
	})
}

// TestPubSubFollowsActive: a subscription created through the MultiDB client
// keeps receiving messages after the active member dies, by re-dialing the
// new active member.
func TestPubSubFollowsActive(t *testing.T) {
	mdb := newE2EClient(t, fiMultiDBOptions())
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
	pub := redis.NewClient(endpointOptions(lastMemberAddr()))
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

	triggerNetworkFailure(t, 0, 60*time.Second)
	eventually(t, 15*time.Second, "failover away from member 0", func() bool {
		return mdb.ActiveDatabaseID() != 0
	})

	publishUntilReceived("after-failover")
}

// TestPSubscribeFollowsActive: the pattern-subscription variant of the test
// above — psubscriptions must survive an active-member outage too.
func TestPSubscribeFollowsActive(t *testing.T) {
	mdb := newE2EClient(t, fiMultiDBOptions())
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

	pub := redis.NewClient(endpointOptions(lastMemberAddr()))
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

	triggerNetworkFailure(t, 0, 60*time.Second)
	eventually(t, 15*time.Second, "failover away from member 0", func() bool {
		return mdb.ActiveDatabaseID() != 0
	})

	publishUntilReceived("pat-after-failover")
}

// TestRuntimeMembershipUnderFaults: a member added at runtime must be a real
// failover target, and removing a (down, passive) member must leave the
// surviving members' ids unchanged (stable ids, no renumbering).
// Spec: test_add_remove_database at runtime.
//
// Needs a spare endpoint beyond the initial two to add at runtime.
func TestRuntimeMembershipUnderFaults(t *testing.T) {
	if len(e2eTopology.Endpoints) < 3 {
		t.Skip("needs a spare endpoint beyond the initial 2 to add at runtime")
	}
	opts := fiMultiDBOptions()
	opts.Clients = opts.Clients[:2] // start with members 0 and 1 only
	mdb := newE2EClient(t, opts)
	ctx := context.Background()

	spare := len(e2eTopology.Endpoints) - 1
	id, err := mdb.AddDatabase(ctx, redis.MultiDBClientConfig{
		Options: endpointOptions(e2eTopology.Endpoints[spare]),
		Weight:  1,
	})
	if err != nil {
		t.Fatalf("AddDatabase: %v", err)
	}
	if id != spare {
		t.Fatalf("AddDatabase id = %d, want %d", id, spare)
	}

	// With both original members down, traffic must land on the member that
	// only ever existed at runtime.
	triggerNetworkFailure(t, 0, 60*time.Second)
	triggerNetworkFailure(t, 1, 60*time.Second)
	eventually(t, 20*time.Second, "commands succeeding on the runtime-added member", func() bool {
		return mdb.Set(ctx, "e2e:member", "x", 0).Err() == nil && mdb.ActiveDatabaseID() == spare
	})

	// Removing the down, passive member 0 does not renumber survivors: ids
	// are stable, so the active member keeps its id and keeps serving.
	if err := mdb.RemoveDatabase(ctx, 0); err != nil {
		t.Fatalf("RemoveDatabase: %v", err)
	}
	if got := mdb.ActiveDatabaseID(); got != spare {
		t.Fatalf("active id after removal = %d, want %d (unchanged; ids are stable)", got, spare)
	}
	if err := mdb.Set(ctx, "e2e:member", "y", 0).Err(); err != nil {
		t.Fatalf("Set after removal: %v", err)
	}
}

// TestSetWeightSteersFallback: a runtime weight change must redirect
// auto-fallback to the new heaviest healthy member. Needs a third member to
// re-weight independently of the down and initially-active ones.
// Spec: test_set_weight + auto-fallback interaction.
func TestSetWeightSteersFallback(t *testing.T) {
	if len(e2eTopology.Endpoints) < 3 {
		t.Skip("needs at least 3 members (a third member to re-weight)")
	}
	mdb := newE2EClient(t, fiMultiDBOptions())
	ctx := context.Background()

	target := len(e2eTopology.Endpoints) - 1

	triggerNetworkFailure(t, 0, 60*time.Second)
	// The failover must land on member 1 (next weight): only then does the
	// later switch to the target member prove the runtime weight change
	// steered it.
	eventually(t, 15*time.Second, "failover to member 1", func() bool {
		return mdb.Set(ctx, "e2e:weight", "x", 0).Err() == nil && mdb.ActiveDatabaseID() == 1
	})

	// The target member becomes the heaviest healthy member: the next
	// fallback pass must switch to it — not back toward the (still down)
	// member 0.
	if err := mdb.SetWeight(target, 10); err != nil {
		t.Fatalf("SetWeight: %v", err)
	}
	eventually(t, 30*time.Second, "fallback to the re-weighted member", func() bool {
		return mdb.ActiveDatabaseID() == target
	})
	if err := mdb.Get(ctx, "e2e:weight").Err(); err != nil {
		t.Fatalf("Get on the re-weighted member: %v", err)
	}
}

// TestConcurrentTrafficAcrossFailover: parallel writers must all converge on
// the new active member after an outage — no goroutine may be left behind on
// a stale snapshot or wedged on the dead member.
func TestConcurrentTrafficAcrossFailover(t *testing.T) {
	mdb := newE2EClient(t, fiMultiDBOptions())
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
				if err := mdb.Set(ctx, key, "v", 0).Err(); err == nil && mdb.ActiveDatabaseID() == 1 {
					postFailover[g].Add(1)
				}
				time.Sleep(20 * time.Millisecond)
			}
		}(g)
	}

	time.Sleep(time.Second) // steady-state traffic on member 0 first
	triggerNetworkFailure(t, 0, 60*time.Second)

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
	dead := len(e2eTopology.Endpoints) - 1
	triggerNetworkFailure(t, dead, 60*time.Second)
	// TriggerAction returns before the fault is necessarily in effect (it
	// runs asynchronously server-side, same as the real service's RQ-job
	// model) — confirm the member is actually unreachable before relying on
	// that for the construction-time assertion below.
	awaitUnreachable(t, endpointOptions(e2eTopology.Endpoints[dead]).Addr, 15*time.Second)

	opts := fiMultiDBOptions()
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
	var activeChanged, circuitOpened atomic.Bool
	opts := fiMultiDBOptions()
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

	triggerNetworkFailure(t, 0, 60*time.Second)
	eventually(t, 15*time.Second, "failover away from member 0", func() bool {
		return mdb.Set(ctx, "e2e:cb", "x", 0).Err() == nil && mdb.ActiveDatabaseID() == 1
	})
	// Both callbacks are delivered asynchronously — poll.
	eventually(t, 5*time.Second, "failover callbacks", func() bool {
		return activeChanged.Load() && circuitOpened.Load()
	})
}
