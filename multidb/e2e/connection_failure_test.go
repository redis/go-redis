package e2e

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
	fi "github.com/redis/go-redis/v9/maintnotifications/e2e"
)

// Matrix rows: connection_failure / connection_dropped, connection_refused,
// connection_timeout, crossed with the matrix workload columns (regular data
// operations, Pub/Sub, blocking commands, mixed workload).
//
// The client under test is a plain *redis.Client pointed at Topology member 0,
// with MaxRetries disabled and short timeouts (endpointOptions), so a fault
// shows up as an error on the very next command instead of being retried away.
// Sharded Pub/Sub needs a cluster endpoint and is not part of this topology.
//
// Each effect injects its fault through the fault injector where the real
// service has an equivalent (network_failure) and through docker otherwise
// (mock mode only, like the proxyFarm scenarios).

// failureEffect is one connection_failure effect: how to break member 0, how
// to put it back, and what error class a fresh command must report.
type failureEffect struct {
	name     string
	mockOnly string // non-empty: skip in real mode, with this reason
	realOnly string // non-empty: skip in mock mode, with this reason
	// inject breaks member 0 and returns restore, which must leave member 0
	// reachable again.
	inject func(t *testing.T) (restore func())
	// wantErr reports whether err is the error class this effect must produce
	// for a fresh command. nil means any non-nil error.
	wantErr func(err error) bool
}

func isConnRefused(err error) bool { return errors.Is(err, syscall.ECONNREFUSED) }

// isRealMode reports whether the suite talks to a real fault injector.
func isRealMode() bool { return os.Getenv("FAULT_INJECTION_API_URL") != "" }

// skipIfMockMode marks a test real-only: it needs a fault the mock cannot
// produce (blackholed traffic, tc latency).
func skipIfMockMode(t *testing.T, why string) {
	t.Helper()
	if !isRealMode() {
		t.Skip("real-only: " + why)
	}
}

// injectProxyStop stops the DMC proxy of the database behind member 0 and
// returns restore, which starts it again. The start is registered as a cleanup
// BEFORE the stop is sent, and runs at most once: the fault injector never
// undoes a stop on its own, so a run that dies half way would otherwise leave
// member 0 down for every later test.
func injectProxyStop(t *testing.T) func() {
	t.Helper()
	var once sync.Once
	start := func() {
		once.Do(func() {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()
			resp, err := faultInjector.TriggerProxyFailure(ctx, fi.ProxyFailureRequest{
				BDBID: e2eTopology.BDBID, ClusterIndex: 0, Op: fi.ProxyStart, WaitForActive: true,
			})
			if err != nil {
				t.Errorf("proxy_failure start: %v", err)
				return
			}
			st, err := faultInjector.WaitForAction(ctx, resp.ActionID, fi.WithMaxWaitTime(5*time.Minute))
			if err != nil || st.Status == fi.StatusFailed {
				t.Errorf("proxy_failure start did not finish: %v %+v", err, st)
			}
		})
	}
	t.Cleanup(start)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	resp, err := faultInjector.TriggerProxyFailure(ctx, fi.ProxyFailureRequest{
		BDBID: e2eTopology.BDBID, ClusterIndex: 0, Op: fi.ProxyStop,
	})
	if err != nil {
		t.Fatalf("proxy_failure stop: %v", err)
	}
	waitForActionDone(t, resp.ActionID, 2*time.Minute)
	return start
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func failureEffects() []failureEffect {
	return []failureEffect{
		{
			// network_failure is the FI action for connection_dropped; the real
			// service blackholes ingress/egress for the delay, the mock stops
			// the member's proxy container.
			name: "connection_dropped/network_failure",
			inject: func(t *testing.T) func() {
				// 30s covers the blocking workload's in-flight wait and the
				// failFast call that follows it; a 10s outage could lift between.
				id := triggerNetworkFailure(t, 0, 30*time.Second)
				return func() { waitForActionDone(t, id, 90*time.Second) }
			},
		},
		{
			// proxy_failure(stop) takes the DMC proxy down, so new TCP
			// connections are refused. Unlike network_failure the fault
			// injector does NOT undo it: restore sends the matching start.
			name:   "connection_refused/proxy_failure",
			inject: injectProxyStop,
			// The mock stops a container, so refused is exact there. On a real
			// deployment the DMC proxy of every node hosting the database is
			// stopped; any client-side error is accepted until the exact class
			// is confirmed against one.
			wantErr: func(err error) bool { return isRealMode() || isConnRefused(err) },
		},
		{
			// The doc's other connection_timeout trigger: network_failure
			// blackholes the nodes (iptables DROP), so fresh dials time out
			// rather than being refused. The mock cannot blackhole.
			name:     "connection_timeout/network_failure",
			realOnly: "the mock stops a container, which refuses; it has no blackhole",
			inject: func(t *testing.T) func() {
				// 30s covers the blocking workload's in-flight wait and the
				// failFast call that follows it; a 10s outage could lift between.
				id := triggerNetworkFailure(t, 0, 30*time.Second)
				return func() { waitForActionDone(t, id, 90*time.Second) }
			},
			wantErr: isTimeout,
		},
		{
			// A paused proxy keeps accepting TCP but never answers: every
			// command must end in a client-side timeout, never hang.
			name:     "connection_timeout/proxy_pause",
			mockOnly: "drives docker directly; blackhole parity with the real FI needs the iptables mechanism",
			inject: func(t *testing.T) func() {
				f := newProxyFarm(t)
				f.Pause(0)
				return func() { f.RestoreAll() }
			},
			wantErr: isTimeout,
		},
	}
}

// failureWorkload is one matrix workload column.
type failureWorkload struct {
	name string
	// run drives the workload against c across the fault. It calls fault()
	// once the workload is established and healthy, then recovered() after
	// member 0 is restored, and must assert behavior in every phase.
	run func(t *testing.T, e failureEffect, c *redis.Client, fault func(), recovered func())
}

func newFailureClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(endpointOptions(e2eTopology.Endpoints[0]))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// sideClient talks to a member that stays up (same backend bus/keyspace).
func sideClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(endpointOptions(lastMemberAddr()))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// failFast runs op and requires it to fail on the client's own timeouts: a
// context deadline would mean the client hung until the test gave up.
func failFast(t *testing.T, what string, e failureEffect, op func(ctx context.Context) error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	start := time.Now()
	err := op(ctx)
	if err == nil {
		t.Fatalf("%s: succeeded while member 0 is down", what)
	}
	if ctx.Err() != nil {
		t.Fatalf("%s: hung until the test deadline (%v): %v", what, time.Since(start), err)
	}
	if e.wantErr != nil && !e.wantErr(err) {
		t.Fatalf("%s: error %v (%T) is not the class %q must produce", what, err, err, e.name)
	}
}

func TestConnectionFailureMatrix(t *testing.T) {
	workloads := []failureWorkload{
		{name: "regular", run: runRegularWorkload},
		{name: "pubsub", run: runPubSubWorkload},
		{name: "blocking", run: runBlockingWorkload},
		{name: "mixed", run: runMixedWorkload},
	}
	for _, e := range failureEffects() {
		for _, w := range workloads {
			t.Run(e.name+"/"+w.name, func(t *testing.T) {
				if e.mockOnly != "" {
					skipIfRealMode(t, e.mockOnly)
				}
				if e.realOnly != "" {
					skipIfMockMode(t, e.realOnly)
				}
				settleAfterFault(t)
				c := newFailureClient(t)
				var restore func()
				fault := func() {
					t.Helper()
					restore = e.inject(t)
					awaitFaultVisible(t, e, c)
				}
				recovered := func() {
					t.Helper()
					restore()
					awaitReachable(t, e2eTopology.Endpoints[0])
				}
				w.run(t, e, c, fault, recovered)
			})
		}
	}
}

// awaitFaultVisible waits until the client itself sees the fault. TriggerAction
// returns before the effect is guaranteed, and asserting too early would test
// the pre-fault state.
func awaitFaultVisible(t *testing.T, e failureEffect, c *redis.Client) {
	t.Helper()
	eventually(t, 20*time.Second, "client to observe the fault", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		return c.Ping(ctx).Err() != nil
	})
}

// awaitReachable waits for a TCP dial to a Topology endpoint to succeed. The
// endpoint is a redis:// URL in real mode and host:port in the mock, so the
// dial address comes from the parsed options, not the raw string.
func awaitReachable(t *testing.T, endpoint string) {
	t.Helper()
	addr := endpointOptions(endpoint).Addr
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := AwaitTCPReachable(ctx, addr, 60*time.Second); err != nil {
		t.Fatalf("member 0 (%s) did not come back: %v", addr, err)
	}
}

// settleAfterFault registers a cleanup that returns only once the Topology is
// replicating again: a canary written through member 0 must show up on the
// last member. Active-Active links need time to re-sync after an outage even
// when every member is reachable, and the next scenario's first write would
// otherwise race that recovery. Registered before the fault, so (LIFO) it
// runs after the fault-injector cleanup that waits for the action to finish.
func settleAfterFault(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		if len(e2eTopology.Endpoints) < 2 {
			return
		}
		src := redis.NewClient(endpointOptions(e2eTopology.Endpoints[0]))
		dst := redis.NewClient(endpointOptions(lastMemberAddr()))
		defer func() { _ = src.Close(); _ = dst.Close() }()

		canary := fmt.Sprintf("e2e:cf:canary:%d", time.Now().UnixNano())
		deadline := time.Now().Add(90 * time.Second)
		for time.Now().Before(deadline) {
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			if src.Set(ctx, canary, "1", time.Minute).Err() == nil {
				if v, err := dst.Get(ctx, canary).Result(); err == nil && v == "1" {
					cancel()
					return
				}
			}
			cancel()
			time.Sleep(500 * time.Millisecond)
		}
		t.Errorf("topology did not resume replicating within 90s of the fault ending")
	})
}

// requireRecovered retries op until it succeeds on the SAME client: the pool
// must drop the dead connections and redial without the caller rebuilding it.
func requireRecovered(t *testing.T, what string, op func(ctx context.Context) error) {
	t.Helper()
	var last error
	eventually(t, 30*time.Second, what+" to recover on the same client", func() bool {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		last = op(ctx)
		return last == nil
	})
}

func runRegularWorkload(t *testing.T, e failureEffect, c *redis.Client, fault, recovered func()) {
	ctx := context.Background()
	if err := c.Set(ctx, "e2e:cf:regular", "before", 0).Err(); err != nil {
		t.Fatalf("Set before fault: %v", err)
	}
	if got, err := c.Get(ctx, "e2e:cf:regular").Result(); err != nil || got != "before" {
		t.Fatalf("Get before fault: %q, %v", got, err)
	}

	fault()
	failFast(t, "SET during fault", e, func(ctx context.Context) error {
		return c.Set(ctx, "e2e:cf:regular", "during", 0).Err()
	})
	failFast(t, "GET during fault", e, func(ctx context.Context) error {
		return c.Get(ctx, "e2e:cf:regular").Err()
	})

	recovered()
	requireRecovered(t, "SET", func(ctx context.Context) error {
		return c.Set(ctx, "e2e:cf:regular", "after", 0).Err()
	})
	if got, err := c.Get(ctx, "e2e:cf:regular").Result(); err != nil || got != "after" {
		t.Fatalf("Get after recovery: %q, %v", got, err)
	}
}

func runPubSubWorkload(t *testing.T, e failureEffect, c *redis.Client, fault, recovered func()) {
	ctx := context.Background()
	const channel = "e2e:cf:channel"

	sub := c.Subscribe(ctx, channel)
	t.Cleanup(func() { _ = sub.Close() })
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	_, err := sub.Receive(rctx)
	rcancel()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	msgs := sub.Channel()
	pub := sideClient(t)

	publishUntilReceived := func(tag string, within time.Duration) {
		t.Helper()
		deadline := time.Now().Add(within)
		for time.Now().Before(deadline) {
			_ = pub.Publish(ctx, channel, tag).Err()
			select {
			case m := <-msgs:
				if m.Payload == tag {
					return
				}
			case <-time.After(250 * time.Millisecond):
			}
		}
		t.Fatalf("message %q not received within %v", tag, within)
	}

	publishUntilReceived("before-fault", 10*time.Second)

	fault()
	// While the member is down nothing can be delivered through c.
	_ = pub.Publish(ctx, channel, "during-fault").Err()
	select {
	case m := <-msgs:
		if m.Payload == "during-fault" {
			t.Fatalf("received %q through a downed member", m.Payload)
		}
	case <-time.After(time.Second):
	}

	recovered()
	// The subscription must resubscribe by itself once the member returns.
	publishUntilReceived("after-recovery", 30*time.Second)
}

func runBlockingWorkload(t *testing.T, e failureEffect, c *redis.Client, fault, recovered func()) {
	ctx := context.Background()
	const key = "e2e:cf:blocking"
	side := sideClient(t)
	_ = side.Del(ctx, key).Err()

	// Healthy: a blocked BLPOP is woken by a push.
	woke := make(chan error, 1)
	go func() {
		_, err := c.BLPop(ctx, 10*time.Second, key).Result()
		woke <- err
	}()
	time.Sleep(300 * time.Millisecond)
	if err := side.RPush(ctx, key, "v1").Err(); err != nil {
		t.Fatalf("RPush: %v", err)
	}
	select {
	case err := <-woke:
		if err != nil {
			t.Fatalf("BLPOP before fault: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("BLPOP before fault never returned")
	}

	// In flight: the fault lands while a BLPOP is blocked server-side. It
	// must come back with an error on the client's timeouts, not hang.
	// The BLPOP must outlive the fault: with a 5s timeout it returned
	// redis.Nil on its own before the outage could break it, and the
	// assertion counted that as a broken connection.
	inflight := make(chan error, 1)
	go func() {
		_, err := c.BLPop(ctx, 30*time.Second, key).Result()
		inflight <- err
	}()
	time.Sleep(300 * time.Millisecond)
	fault()
	select {
	case err := <-inflight:
		if err == nil || errors.Is(err, redis.Nil) {
			t.Fatalf("in-flight BLPOP was not broken by the fault: %v", err)
		}
	case <-time.After(20 * time.Second):
		t.Fatal("in-flight BLPOP hung through the fault")
	}

	// New blocking calls fail fast too.
	failFast(t, "BLPOP during fault", e, func(ctx context.Context) error {
		return c.BLPop(ctx, time.Second, key).Err()
	})

	recovered()
	requireRecovered(t, "BLPOP", func(ctx context.Context) error {
		if err := side.RPush(ctx, key, "v2").Err(); err != nil {
			return err
		}
		vals, err := c.BLPop(ctx, 2*time.Second, key).Result()
		if err != nil {
			return err
		}
		if len(vals) != 2 || vals[1] != "v2" {
			t.Fatalf("BLPOP after recovery: %v, want [key v2]", vals)
		}
		return nil
	})
}

func runMixedWorkload(t *testing.T, e failureEffect, c *redis.Client, fault, recovered func()) {
	ctx := context.Background()

	// Pipeline, MULTI/EXEC and plain commands interleave on the same pool.
	step := func(ctx context.Context) error {
		var errs [3]error
		var wg sync.WaitGroup
		wg.Add(3)
		go func() {
			defer wg.Done()
			_, errs[0] = c.Pipelined(ctx, func(p redis.Pipeliner) error {
				for i := 0; i < 10; i++ {
					p.Set(ctx, "e2e:cf:mixed:p", i, 0)
				}
				return nil
			})
		}()
		go func() {
			defer wg.Done()
			_, errs[1] = c.TxPipelined(ctx, func(p redis.Pipeliner) error {
				p.Incr(ctx, "e2e:cf:mixed:tx")
				p.Incr(ctx, "e2e:cf:mixed:tx")
				return nil
			})
		}()
		go func() {
			defer wg.Done()
			errs[2] = c.Set(ctx, "e2e:cf:mixed:r", "x", 0).Err()
		}()
		wg.Wait()
		return errors.Join(errs[:]...)
	}

	if err := step(ctx); err != nil {
		t.Fatalf("mixed step before fault: %v", err)
	}

	// Keep load running through the whole fault window: failures must be
	// errors, never a panic or a stuck goroutine.
	var stop atomic.Bool
	var loops, failures atomic.Int64
	var load sync.WaitGroup
	load.Add(1)
	go func() {
		defer load.Done()
		for !stop.Load() {
			cctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			if err := step(cctx); err != nil {
				failures.Add(1)
			}
			cancel()
			loops.Add(1)
			time.Sleep(20 * time.Millisecond)
		}
	}()
	// Stop the load on every exit path: a Fatal below must not leave the loop
	// issuing commands against a closed client for the rest of the suite.
	stopLoad := func() {
		stop.Store(true)
		done := make(chan struct{})
		go func() { load.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Errorf("background mixed load never stopped: a command is stuck")
		}
	}
	t.Cleanup(stopLoad)

	fault()
	failFast(t, "mixed step during fault", e, step)
	recovered()
	requireRecovered(t, "mixed workload", step)

	stopLoad()
	if failures.Load() == 0 {
		t.Fatalf("background load saw no failures across %d iterations: the fault never reached it", loops.Load())
	}
}

// TestMultiDBBlockingAcrossFailover: a blocking command in flight on the
// active member when it dies returns an error (MultiDB never replays blocking
// commands), and blocking commands work on the new active member afterwards.
// Matrix: connection_failure x blocking commands x MultiDB.
func TestMultiDBBlockingAcrossFailover(t *testing.T) {
	settleAfterFault(t)
	mdb := newE2EClient(t, fiMultiDBOptions())
	ctx := context.Background()
	const key = "e2e:cf:mdb-blocking"
	side := sideClient(t)
	_ = side.Del(ctx, key).Err()

	// Outlive the outage: a 5s BLPOP would return redis.Nil on its own, which
	// the old assertion treated as the fault breaking the connection.
	inflight := make(chan error, 1)
	go func() {
		_, err := mdb.BLPop(ctx, 60*time.Second, key).Result()
		inflight <- err
	}()
	time.Sleep(300 * time.Millisecond)
	triggerNetworkFailure(t, 0, 60*time.Second)

	select {
	case err := <-inflight:
		if err == nil || errors.Is(err, redis.Nil) {
			t.Fatalf("in-flight BLPOP was not broken by the fault: %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("in-flight BLPOP hung through the member outage")
	}

	eventually(t, 15*time.Second, "failover away from member 0", func() bool {
		return mdb.ActiveDatabaseID() != 0
	})
	eventually(t, 15*time.Second, "BLPOP on the new active member", func() bool {
		if err := side.RPush(ctx, key, "after").Err(); err != nil {
			return false
		}
		vals, err := mdb.BLPop(ctx, 2*time.Second, key).Result()
		return err == nil && len(vals) == 2 && vals[1] == "after"
	})
}

// schEnabledOptions is fiMultiDBOptions with Smart Client Handoff switched on
// for every member. MultiDB turns it off unless the member Options set it
// (see disableMaintNotificationsIfUnset), so this is the only way to cover the
// matrix's "MultiDB client with SCH enabled" column. ModeAuto: the mock proxies
// do not speak maintenance notifications, and ModeEnabled would fail the
// handshake instead of degrading.
func schEnabledOptions() *redis.MultiDBOptions {
	opts := fiMultiDBOptions()
	for i := range opts.Clients {
		o := opts.Clients[i].Options
		o.Protocol = 3
		o.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeAuto}
	}
	return opts
}

// TestMultiDBWithSCHFailoverOnMemberOutage: with SCH enabled on the members,
// commands and subscriptions still fail over when the active member dies.
// Matrix: connection_failure x MultiDB client with SCH enabled.
func TestMultiDBWithSCHFailoverOnMemberOutage(t *testing.T) {
	settleAfterFault(t)
	mdb := newE2EClient(t, schEnabledOptions())
	ctx := context.Background()

	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("initial active = %d, want 0 (highest weight)", got)
	}
	if err := mdb.Set(ctx, "e2e:cf:sch", "before", 0).Err(); err != nil {
		t.Fatalf("Set before outage: %v", err)
	}

	const channel = "e2e:cf:sch-channel"
	sub := mdb.Subscribe(ctx, channel)
	t.Cleanup(func() { _ = sub.Close() })
	rctx, rcancel := context.WithTimeout(ctx, 10*time.Second)
	_, err := sub.Receive(rctx)
	rcancel()
	if err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	msgs := sub.Channel()
	pub := sideClient(t)

	triggerNetworkFailure(t, 0, 60*time.Second)

	eventually(t, 15*time.Second, "commands succeeding on a new active member", func() bool {
		return mdb.ActiveDatabaseID() != 0 && mdb.Set(ctx, "e2e:cf:sch", "after", 0).Err() == nil
	})
	if got, err := mdb.Get(ctx, "e2e:cf:sch").Result(); err != nil || got != "after" {
		t.Fatalf("Get after failover: %q, %v", got, err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		_ = pub.Publish(ctx, channel, "after-failover").Err()
		select {
		case m := <-msgs:
			if m.Payload == "after-failover" {
				return
			}
		case <-time.After(250 * time.Millisecond):
		}
		if time.Now().After(deadline) {
			t.Fatal("subscription did not follow the failover with SCH enabled")
		}
	}
}

// TestMultiDBFailoverOnSlowResponse: a member that answers too slowly, not one
// that is down, must be failed away from, and traffic must return once the
// latency is gone. network_latency makes commands TIME OUT instead of being
// refused, which is what drives the circuit breaker.
// Matrix: response_degradation / slow_response x MultiDB.
//
// Real-only: the mock proxy image has no tc, so the mock FI rejects the action.
func TestMultiDBFailoverOnSlowResponse(t *testing.T) {
	skipIfMockMode(t, "network_latency needs tc on the nodes; the mock proxy image has none")
	settleAfterFault(t)

	const (
		delay    = 3 * time.Second // above the 1s client read timeout and probe timeout
		duration = 45 * time.Second
	)
	mdb := newE2EClient(t, fiMultiDBOptions())
	ctx := context.Background()
	if got := mdb.ActiveDatabaseID(); got != 0 {
		t.Fatalf("initial active = %d, want 0 (highest weight)", got)
	}
	if err := mdb.Set(ctx, "e2e:cf:slow", "before", 0).Err(); err != nil {
		t.Fatalf("Set before latency: %v", err)
	}

	actx, acancel := context.WithTimeout(ctx, 2*time.Minute)
	defer acancel()
	resp, err := faultInjector.TriggerBDBNetworkLatency(actx, e2eTopology.BDBID, 0, delay, duration)
	if err != nil {
		t.Fatalf("network_latency: %v", err)
	}
	// The node removes the latency by itself `duration` after the action's
	// result, so the next test must not start before then.
	var applied time.Time
	t.Cleanup(func() {
		wctx, wcancel := context.WithTimeout(context.Background(), 3*time.Minute)
		defer wcancel()
		if _, err := faultInjector.WaitForAction(wctx, resp.ActionID, fi.WithMaxWaitTime(3*time.Minute)); err != nil {
			t.Errorf("waiting for network_latency action %s: %v", resp.ActionID, err)
		}
		if applied.IsZero() {
			applied = time.Now()
		}
		time.Sleep(time.Until(applied.Add(duration + 5*time.Second)))
	})
	waitForActionDone(t, resp.ActionID, 2*time.Minute)
	applied = time.Now()

	eventually(t, 30*time.Second, "failover away from the slow member", func() bool {
		return mdb.ActiveDatabaseID() != 0 && mdb.Set(ctx, "e2e:cf:slow", "during", 0).Err() == nil
	})

	// Once the node drops the latency, the higher-weight member takes over again.
	eventually(t, duration+60*time.Second, "fallback to member 0 after the latency ends", func() bool {
		return mdb.ActiveDatabaseID() == 0 && mdb.Set(ctx, "e2e:cf:slow", "after", 0).Err() == nil
	})
}

// Real-mode endpoints are redis:// URLs; the dial address must come out as
// host:port. A raw URL passed to a TCP dial made every dropped-connection
// cell fail against a real deployment while the mock (plain host:port) passed.
func TestEndpointDialAddrFromURL(t *testing.T) {
	for _, tc := range []struct{ raw, want string }{
		{"redis://redis-11236.example.com:11236", "redis-11236.example.com:11236"},
		{"redis://user:pw@10.0.0.1:6379", "10.0.0.1:6379"},
		{"127.0.0.1:17100", "127.0.0.1:17100"},
	} {
		if got := endpointOptions(tc.raw).Addr; got != tc.want {
			t.Errorf("endpointOptions(%q).Addr = %q, want %q", tc.raw, got, tc.want)
		}
	}
}
