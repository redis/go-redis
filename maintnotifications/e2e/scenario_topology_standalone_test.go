package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Matrix rows: topology_change_standalone / data_movement_conn_drop,
// data_movement_no_conn_drop, conn_drop, dns_resolution_change, crossed with
// the SCH client and the regular, pub/sub and blocking workloads.
//
// The fault injector owns the database: SetupTopologyChangeStandalone creates
// one shaped for the effect and trigger, and teardown deletes it and restores
// node state. A plain client with maintenance notifications left at their
// default (Auto, RESP3) is the SCH client under test.
//
// Assertions are deliberately modest: no hang, and every workload works again
// on the SAME client once the action finished. Notification counts are not
// asserted; they are what differs most between Redis Enterprise versions.
//
// Real fault injector only: the proxy mock has no /topology-change-standalone.

// envAllStandaloneTriggers runs every trigger of an effect instead of the
// first one. Each run provisions and deletes a database, so the default keeps
// the package inside its time budget.
const envAllStandaloneTriggers = "E2E_STANDALONE_ALL_TRIGGERS"

func TestStandaloneTopologyChange(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}
	if GetTestMode() != TestModeRealFaultInjector || faultInjector == nil {
		t.Skip("needs a real fault injector: the proxy mock has no /topology-change-standalone")
	}
	for _, effect := range StandaloneEffects {
		t.Run(string(effect), func(t *testing.T) {
			runStandaloneEffect(t, faultInjector, effect)
		})
	}
}

func runStandaloneEffect(t *testing.T, fic *FaultInjectorClient, effect StandaloneEffect) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := fic.GetStandaloneTriggers(ctx, effect, 0)
	if err != nil {
		t.Fatalf("list triggers for %s: %v", effect, err)
	}
	names := make([]string, 0, len(resp.Triggers))
	for _, tr := range resp.Triggers {
		names = append(names, tr.Name)
	}
	t.Logf("%s: %d-node cluster, triggers %v", effect, resp.Cluster.Nodes, names)
	if len(resp.Triggers) == 0 {
		t.Skipf("no triggers for %s (the action needs a cluster of at least 3 nodes)", effect)
	}

	triggers := resp.Triggers
	if os.Getenv(envAllStandaloneTriggers) != "true" {
		triggers = triggers[:1]
	}
	for _, tr := range triggers {
		t.Run(tr.Name, func(t *testing.T) {
			variant := plainVariant(tr)
			if variant < 0 {
				t.Skipf("%s has only TLS/mTLS database configs", tr.Name)
			}
			runStandaloneScenario(t, fic, effect, tr.Name, variant)
		})
	}
}

// plainVariant returns the index of the first requirement that is not TLS or
// mTLS, or -1. Those need certificates this scenario does not wire up.
func plainVariant(tr StandaloneTrigger) int {
	for i, req := range tr.Requirements {
		if !strings.Contains(strings.ToLower(req.Config), "tls") {
			return i
		}
	}
	return -1
}

// standaloneClient dials the endpoint the fault injector returned for setup.
func standaloneClient(t *testing.T, s *StandaloneSetup) *redis.Client {
	t.Helper()
	if len(s.Endpoints) == 0 {
		t.Fatalf("setup returned no endpoints: %+v", s)
	}
	opts, err := redis.ParseURL(s.Endpoints[0])
	if err != nil {
		t.Fatalf("endpoint %q: %v", s.Endpoints[0], err)
	}
	if s.Password != "" {
		opts.Password = s.Password
	}
	// RESP3 with the default maintenance-notification config (Auto) is the SCH
	// client: it handshakes the notifications and degrades if unsupported.
	opts.Protocol = 3
	c := redis.NewClient(opts)
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func standaloneEventually(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(250 * time.Millisecond)
	}
	t.Fatalf("timed out after %v waiting for %s", timeout, what)
}

// standaloneResult is what a scenario run observed, beyond pass or fail.
type standaloneResult struct {
	// Reconnected: the server-side connection id of the client changed across
	// the action. go-redis retries through most drops, so command failures alone
	// do not show whether the connection was really dropped.
	Reconnected bool
}

// standaloneObserve, when set, receives the result of each scenario run.
var standaloneObserve func(effect StandaloneEffect, trigger string, r standaloneResult)

// runStandaloneScenario provisions a database, runs workloads across the
// topology change and checks they recover on the same client.
func runStandaloneScenario(t *testing.T, fic *FaultInjectorClient, effect StandaloneEffect, trigger string, variant int) {
	t.Helper()
	ctx := context.Background()

	setup, err := fic.SetupTopologyChangeStandalone(ctx, effect, trigger, 0, variant)
	if err != nil {
		t.Fatalf("setup %s/%s: %v", effect, trigger, err)
	}
	// Registered first so the database is removed even when a later step fails.
	t.Cleanup(func() {
		tctx, cancel := context.WithTimeout(context.Background(), setupTimeout)
		defer cancel()
		if err := fic.TeardownTopologyChangeStandalone(tctx, setup); err != nil {
			t.Errorf("teardown %s: %v", setup.SetupID, err)
		}
	})
	if setup.TLS {
		t.Skip("the provisioned database requires TLS")
	}
	t.Logf("setup %s: bdb %s, config %q, endpoint %v", setup.SetupID, setup.BDBID, setup.Config, setup.Endpoints)

	c := standaloneClient(t, setup)
	pub := standaloneClient(t, setup)

	key := fmt.Sprintf("e2e:tcs:%s:%d", setup.SetupID, time.Now().UnixNano())
	channel := key + ":chan"
	list := key + ":list"

	// Baseline: all three workloads work before the fault.
	if err := c.Set(ctx, key, "before", 0).Err(); err != nil {
		t.Fatalf("baseline SET: %v", err)
	}
	sub := c.Subscribe(ctx, channel)
	t.Cleanup(func() { _ = sub.Close() })
	rctx, rcancel := context.WithTimeout(ctx, 15*time.Second)
	_, err = sub.Receive(rctx)
	rcancel()
	if err != nil {
		t.Fatalf("baseline subscribe: %v", err)
	}
	msgs := sub.Channel()
	publishUntilReceived := func(payload string, within time.Duration) {
		t.Helper()
		deadline := time.Now().Add(within)
		for time.Now().Before(deadline) {
			pctx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = pub.Publish(pctx, channel, payload).Err()
			cancel()
			select {
			case m := <-msgs:
				if m.Payload == payload {
					return
				}
			case <-time.After(500 * time.Millisecond):
			}
		}
		t.Fatalf("message %q not received within %v", payload, within)
	}
	publishUntilReceived("before", 15*time.Second)
	idBefore, err := c.ClientID(ctx).Result()
	if err != nil {
		t.Fatalf("baseline CLIENT ID: %v", err)
	}

	// Regular traffic keeps running across the whole action; record how long
	// it failed for, which is information, not an assertion.
	var stop atomic.Bool
	var okOps, failOps atomic.Int64
	var mu sync.Mutex
	var failStart time.Time
	var longestFail time.Duration
	var load sync.WaitGroup
	load.Add(1)
	go func() {
		defer load.Done()
		for !stop.Load() {
			lctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := c.Set(lctx, key, "load", 0).Err()
			cancel()
			mu.Lock()
			if err != nil {
				failOps.Add(1)
				if failStart.IsZero() {
					failStart = time.Now()
				}
			} else {
				okOps.Add(1)
				if !failStart.IsZero() {
					if d := time.Since(failStart); d > longestFail {
						longestFail = d
					}
					failStart = time.Time{}
				}
			}
			mu.Unlock()
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// A blocked BLPOP across the fault must come back, with a value or an error.
	blocked := make(chan error, 1)
	go func() {
		_, err := c.BLPop(ctx, 20*time.Second, list).Result()
		blocked <- err
	}()

	resp, err := fic.TriggerTopologyChangeStandalone(ctx, StandaloneTriggerRequest{
		Effect: effect, Trigger: trigger, SetupID: setup.SetupID, BDBID: setup.BDBID, ClusterIndex: setup.ClusterIndex,
	})
	if err != nil {
		stop.Store(true)
		load.Wait()
		t.Fatalf("trigger %s/%s: %v", effect, trigger, err)
	}
	st, err := fic.WaitForAction(ctx, resp.ActionID, WithMaxWaitTime(10*time.Minute))
	if err != nil {
		stop.Store(true)
		load.Wait()
		t.Fatalf("waiting for action %s: %v", resp.ActionID, err)
	}
	if st.Status != StatusSuccess && st.Status != StatusFinished {
		stop.Store(true)
		load.Wait()
		t.Fatalf("action %s ended %s: %v", resp.ActionID, st.Status, st.Error)
	}

	select {
	case <-blocked:
	case <-time.After(60 * time.Second):
		t.Error("blocking BLPOP never returned across the topology change")
	}

	// Recovery on the same client, for each workload.
	standaloneEventually(t, 90*time.Second, "regular commands to succeed again", func() bool {
		rctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		return c.Set(rctx, key, "after", 0).Err() == nil
	})
	publishUntilReceived("after", 90*time.Second)
	standaloneEventually(t, 90*time.Second, "BLPOP to work again", func() bool {
		bctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if pub.RPush(bctx, list, "v").Err() != nil {
			return false
		}
		vals, err := c.BLPop(bctx, 2*time.Second, list).Result()
		return err == nil && len(vals) == 2 && vals[1] == "v"
	})

	stop.Store(true)
	done := make(chan struct{})
	go func() { load.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("background load never stopped: a command is stuck")
	}
	var res standaloneResult
	if idAfter, err := c.ClientID(ctx).Result(); err == nil {
		res.Reconnected = idAfter != idBefore
	}
	mu.Lock()
	t.Logf("%s/%s: %d ok, %d failed ops, longest failure window %v, reconnected=%v",
		effect, trigger, okOps.Load(), failOps.Load(), longestFail, res.Reconnected)
	mu.Unlock()
	if standaloneObserve != nil {
		standaloneObserve(effect, trigger, res)
	}
}
