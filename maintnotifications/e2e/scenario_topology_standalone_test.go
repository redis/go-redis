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
	"github.com/redis/go-redis/v9/maintnotifications"
)

// Matrix rows: topology_change_standalone / data_movement_conn_drop,
// data_movement_no_conn_drop, conn_drop, dns_resolution_change, crossed with
// the regular, pub/sub and blocking workloads, once with SCH and once without.
//
// The fault injector owns the database: SetupTopologyChangeStandalone creates
// one shaped for the effect and trigger, and teardown deletes it and restores
// node state. Each effect runs in two modes (see schModes): with SCH, a plain
// client with maintenance notifications left at their default (Auto, RESP3);
// without SCH, the same client with maintenance notifications disabled, which
// has to recover from the drop or the data movement by reconnecting alone.
//
// Assertions are deliberately modest: no hang, every workload works again on
// the SAME client once the action finished, and no data is lost. Data is
// checked with many commands (see data_verify_test.go): a data set seeded
// before the change (GET/SET, HSET/HGET/HGETALL, MSET/MGET, counters, TTL and
// PERSIST, sets, lists, sorted sets, DEL) must read back exactly, and every
// write acknowledged while the change ran must still be there. Notification
// counts are not asserted; they are what differs most between Redis
// Enterprise versions.
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

// schMode is how the client under test treats maintenance notifications (SCH).
type schMode struct {
	name string
	// cfg is the client's MaintNotificationsConfig. nil keeps the library
	// default (Mode Auto), which is the "with SCH" client.
	cfg *maintnotifications.Config
	// off is true when the client must have no maintenance-notification manager.
	off bool
}

var (
	schWith    = schMode{name: "with_sch"}
	schWithout = schMode{name: "without_sch", cfg: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}, off: true}

	// schModes is the order the scenarios run in.
	schModes = []schMode{schWith, schWithout}
)

// requireSCHState fails when the client does not match the mode and returns
// whether the client has a maintenance-notification manager. A without-SCH run
// must have none, otherwise its label would not be true. A with-SCH run only
// logs: the manager exists whenever the mode is not disabled, even if the
// server turns out not to support the feature.
func requireSCHState(t *testing.T, mode schMode, c *redis.Client) bool {
	t.Helper()
	active := c.GetMaintNotificationsManager() != nil
	if mode.off && active {
		t.Fatalf("%s: client has a maintenance-notification manager, SCH is not off", mode.name)
	}
	t.Logf("%s: maintenance notifications active=%v", mode.name, active)
	return active
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
	for _, mode := range schModes {
		t.Run(mode.name, func(t *testing.T) {
			for _, tr := range triggers {
				t.Run(tr.Name, func(t *testing.T) {
					variant := plainVariant(tr)
					if variant < 0 {
						t.Skipf("%s has only TLS/mTLS database configs", tr.Name)
					}
					runStandaloneScenario(t, fic, effect, tr.Name, variant, mode)
				})
			}
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

// standaloneClient dials the endpoint the fault injector returned for setup,
// configured for the given SCH mode.
func standaloneClient(t *testing.T, s *StandaloneSetup, mode schMode) *redis.Client {
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
	// RESP3 in both modes, so the only difference is the maintenance-notification
	// config. With the default (Auto) the client handshakes the notifications and
	// degrades if unsupported; without SCH it is disabled outright.
	opts.Protocol = 3
	opts.MaintNotificationsConfig = mode.cfg
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
	// Mode is the SCH mode the run used.
	Mode string
	// SCHActive: the client had a maintenance-notification manager.
	SCHActive bool
	// Reconnected: the server-side connection id of the client changed across
	// the action. go-redis retries through most drops, so command failures alone
	// do not show whether the connection was really dropped.
	Reconnected bool
}

// standaloneObserve, when set, receives the result of each scenario run.
var standaloneObserve func(effect StandaloneEffect, trigger string, r standaloneResult)

// runStandaloneScenario provisions a database, runs workloads across the
// topology change and checks they recover on the same client.
func runStandaloneScenario(t *testing.T, fic *FaultInjectorClient, effect StandaloneEffect, trigger string, variant int, mode schMode) {
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

	c := standaloneClient(t, setup, mode)
	pub := standaloneClient(t, setup, mode)
	schActive := requireSCHState(t, mode, c)

	key := fmt.Sprintf("e2e:tcs:%s:%d", setup.SetupID, time.Now().UnixNano())
	channel := key + ":chan"
	list := key + ":list"

	// Baseline: all three workloads work before the fault, and a data set written
	// with many commands (see data_verify_test.go) is in place to check afterwards.
	if err := c.Set(ctx, key, "before", 0).Err(); err != nil {
		t.Fatalf("baseline SET: %v", err)
	}
	// Own prefixes: the scenario's own keys (<key>, :chan, :list) must not overlap
	// the data set, or the blocking workload would consume its list.
	data := newDataVerifier(key + ":data")
	if err := data.seed(ctx, c); err != nil {
		t.Fatalf("seed data set: %v", err)
	}
	if err := data.verify(ctx, c); err != nil {
		t.Fatalf("data set is wrong before the fault: %v", err)
	}
	live := newLiveLoad(key + ":lv")
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
		for i := 0; !stop.Load(); i++ {
			lctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := live.step(lctx, c, i)
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

	// No lost or corrupted data, whatever command wrote it: the seeded data set
	// reads back exactly, and every write acknowledged during the change is there.
	if err := data.verifyEventually(ctx, c, 60*time.Second); err != nil {
		t.Errorf("seeded data set after the change: %v", err)
	}
	if err := live.verify(ctx, c); err != nil {
		t.Errorf("acknowledged writes after the change: %v", err)
	}

	res := standaloneResult{Mode: mode.name, SCHActive: schActive}
	if idAfter, err := c.ClientID(ctx).Result(); err == nil {
		res.Reconnected = idAfter != idBefore
	}
	mu.Lock()
	t.Logf("%s/%s/%s: %d ok, %d failed ops, longest failure window %v, reconnected=%v",
		mode.name, effect, trigger, okOps.Load(), failOps.Load(), longestFail, res.Reconnected)
	mu.Unlock()
	if standaloneObserve != nil {
		standaloneObserve(effect, trigger, res)
	}
}
