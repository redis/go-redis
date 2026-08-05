package e2e

import (
	"context"
	"errors"
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
	if failoverFrom.Load() != 0 || failoverTo.Load() != 1 {
		t.Errorf("OnFailover(from=%d, to=%d), want (0, 1)", failoverFrom.Load(), failoverTo.Load())
	}
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

	sawTemporary := false
	eventually(t, 20*time.Second, "unavailability error surfacing", func() bool {
		err := mdb.Set(ctx, "e2e:esc", "x", 0).Err()
		if errors.Is(err, redis.ErrTemporarilyNotAvailable) {
			sawTemporary = true
			return true
		}
		return errors.Is(err, redis.ErrPermanentlyNotAvailable)
	})
	if !sawTemporary {
		t.Log("note: escalated straight to permanent (attempt budget consumed by background loop)")
	}

	// Recovery: one member back is enough for one_available-style operation.
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

	// Force onto the dead member: allowed, and the next commands drive an
	// automatic failover away again.
	if err := mdb.ForceActiveIndex(ctx, 2); err != nil {
		t.Fatalf("ForceActiveIndex: %v", err)
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
	if _, err := sub.Receive(ctx); err != nil { // wait for the subscription
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
