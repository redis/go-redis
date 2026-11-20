package e2e

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/logging"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// apSchMode is one autopipeline configuration driven through maintenance.
type apSchMode struct {
	name      string
	opts      *redis.AutoPipelineOptions // nil = defaults
	async     bool                       // AsyncAutoPipeline instead of AutoPipeline
	pipelined bool                       // traffic through ap.Pipelined batches
}

var standaloneAPModes = []apSchMode{
	{name: "blocking"},
	{name: "async", async: true},
	{name: "async-unordered", async: true, opts: &redis.AutoPipelineOptions{Unordered: true, MaxConcurrentBatches: 4}},
	{name: "async-shards", async: true, opts: &redis.AutoPipelineOptions{Unordered: true, NumShards: 4}},
	{name: "fd-blocking", opts: &redis.AutoPipelineOptions{FullDuplex: true}},
	{name: "fd-async", async: true, opts: &redis.AutoPipelineOptions{FullDuplex: true}},
	{name: "fd-engines", opts: &redis.AutoPipelineOptions{FullDuplex: true, NumShards: 2}},
	{name: "fd-pipelined", pipelined: true, opts: &redis.AutoPipelineOptions{FullDuplex: true}},
}

var clusterAPModes = []apSchMode{
	{name: "blocking"},
	{name: "async", async: true},
	{name: "async-unordered", async: true, opts: &redis.AutoPipelineOptions{Unordered: true, MaxConcurrentBatches: 4}},
	{name: "fd-blocking", opts: &redis.AutoPipelineOptions{FullDuplex: true}},
	{name: "fd-async", async: true, opts: &redis.AutoPipelineOptions{FullDuplex: true}},
}

// apSchClient is one client with one autopipeliner and its traffic.
type apSchClient struct {
	mode    apSchMode
	client  redis.UniversalClient
	ap      *redis.AutoPipeliner
	tracker *TrackingNotificationsHook
	key     func(worker, seq int) string

	ops  atomic.Int64 // successful steps
	errs atomic.Int64 // failed steps

	badMu sync.Mutex
	bad   []error // replies delivered to the wrong command

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func newAPSchClient(t *testing.T, factory *ClientFactory, mode apSchMode, cluster bool) *apSchClient {
	t.Helper()
	cfg := &maintnotifications.Config{
		Mode:                       maintnotifications.ModeEnabled,
		HandoffTimeout:             40 * time.Second,
		RelaxedTimeout:             10 * time.Second,
		PostHandoffRelaxedDuration: 2 * time.Second,
		MaxWorkers:                 20,
	}
	if !cluster {
		cfg.EndpointType = maintnotifications.EndpointTypeExternalIP
	}
	client, err := factory.Create("ap-"+mode.name, &CreateClientOptions{
		Protocol:                 3,
		PoolSize:                 10,
		MaintNotificationsConfig: cfg,
		ClientName:               "ap-sch-" + mode.name,
	})
	if err != nil {
		t.Fatalf("%s: create client: %v", mode.name, err)
	}
	c := &apSchClient{mode: mode, client: client, tracker: NewTrackingNotificationsHook()}
	setupNotificationHooks(client, c.tracker, maintnotifications.NewLoggingHook(int(logging.LogLevelDebug)))

	type apGetter interface {
		AutoPipelineWithOptions(*redis.AutoPipelineOptions) (*redis.AutoPipeliner, error)
		AsyncAutoPipelineWithOptions(*redis.AutoPipelineOptions) (*redis.AutoPipeliner, error)
	}
	g, ok := client.(apGetter)
	if !ok {
		t.Fatalf("%s: %T has no autopipeline getters", mode.name, client)
	}
	if mode.async {
		c.ap, err = g.AsyncAutoPipelineWithOptions(mode.opts)
	} else {
		c.ap, err = g.AutoPipelineWithOptions(mode.opts)
	}
	if err != nil {
		t.Fatalf("%s: autopipeliner: %v", mode.name, err)
	}
	// The mode must run on the engine it names: a silent fallback to
	// half-duplex would make the FD rows test nothing.
	if want := mode.opts != nil && mode.opts.FullDuplex; c.ap.Config().FullDuplex != want {
		t.Fatalf("%s: FullDuplex engaged = %v, want %v", mode.name, c.ap.Config().FullDuplex, want)
	}

	if cluster {
		c.key = func(w, seq int) string {
			slot := (seq % 17) * 1001 // spread over every shard
			return fmt.Sprintf("{%s}:ap:%s:%d", slotKeys[slot], mode.name, w)
		}
	} else {
		c.key = func(w, seq int) string { return fmt.Sprintf("ap:%s:%d:%d", mode.name, w, seq%8) }
	}
	return c
}

func (c *apSchClient) recordBad(err error) {
	c.badMu.Lock()
	c.bad = append(c.bad, err)
	c.badMu.Unlock()
}

// checkReply reports whether a GET reply is acceptable: a value written for
// this key, or no value yet. A value for another key means a reply reached
// the wrong command.
func (c *apSchClient) checkReply(key, val string, err error) bool {
	switch {
	case errors.Is(err, redis.Nil):
		return true
	case err != nil:
		return false
	case !strings.HasPrefix(val, key+"|"):
		c.recordBad(fmt.Errorf("%s: GET %s returned %q: reply for another command", c.mode.name, key, val))
		return false
	}
	return true
}

func (c *apSchClient) step(ctx context.Context, w, seq int) bool {
	const window = 8
	switch {
	case c.mode.pipelined:
		keys := make([]string, window)
		gets := make([]*redis.StringCmd, window)
		_, err := c.ap.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i := range window {
				keys[i] = c.key(w, seq*window+i)
				p.Set(ctx, keys[i], fmt.Sprintf("%s|%d", keys[i], seq), 0)
				gets[i] = p.Get(ctx, keys[i])
			}
			return nil
		})
		if err != nil {
			return false
		}
		ok := true
		for i := range window {
			val, gerr := gets[i].Result()
			ok = c.checkReply(keys[i], val, gerr) && ok
		}
		return ok
	case c.mode.async:
		keys := make([]string, window)
		sets := make([]*redis.StatusCmd, window)
		gets := make([]*redis.StringCmd, window)
		for i := range window {
			keys[i] = c.key(w, seq*window+i)
			sets[i] = c.ap.Set(ctx, keys[i], fmt.Sprintf("%s|%d", keys[i], seq), 0)
			gets[i] = c.ap.Get(ctx, keys[i])
		}
		ok := true
		for i := range window {
			if sets[i].Err() != nil {
				ok = false
			}
			val, gerr := gets[i].Result()
			ok = c.checkReply(keys[i], val, gerr) && ok
		}
		return ok
	default:
		key := c.key(w, seq)
		if err := c.ap.Set(ctx, key, fmt.Sprintf("%s|%d", key, seq), 0).Err(); err != nil {
			return false
		}
		val, err := c.ap.Get(ctx, key).Result()
		return c.checkReply(key, val, err)
	}
}

func (c *apSchClient) start() {
	ctx, cancel := context.WithCancel(context.Background())
	c.cancel = cancel
	for w := range 2 {
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			for seq := 0; ctx.Err() == nil; seq++ {
				sctx, scancel := context.WithTimeout(ctx, 15*time.Second)
				ok := c.step(sctx, w, seq)
				scancel()
				if ok {
					c.ops.Add(1)
				} else {
					c.errs.Add(1)
					time.Sleep(20 * time.Millisecond)
				}
			}
		}()
	}
}

func (c *apSchClient) stop() {
	if c.cancel != nil {
		c.cancel()
		c.wg.Wait()
		c.cancel = nil
	}
}

// waitProgress waits until every client completed at least n more steps.
func waitProgress(t *testing.T, clients []*apSchClient, n int64, timeout time.Duration, what string) {
	t.Helper()
	base := make([]int64, len(clients))
	for i, c := range clients {
		base[i] = c.ops.Load()
	}
	deadline := time.Now().Add(timeout)
	for {
		var behind []string
		for i, c := range clients {
			if c.ops.Load()-base[i] < n {
				behind = append(behind, c.mode.name)
			}
		}
		if len(behind) == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s: no progress within %v for %v", what, timeout, behind)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// waitNotification waits until every client's tracker saw a notification.
func waitNotification(t *testing.T, clients []*apSchClient, notification string, timeout time.Duration) {
	t.Helper()
	for _, c := range clients {
		if _, found := c.tracker.FindOrWaitForNotification(notification, timeout); !found {
			t.Fatalf("%s: %s notification not received within %v", c.mode.name, notification, timeout)
		}
	}
}

func finishAPSch(t *testing.T, clients []*apSchClient) {
	t.Helper()
	for _, c := range clients {
		c.stop()
	}
	for _, c := range clients {
		a := c.tracker.GetAnalysis()
		t.Logf("%s: ops=%d errs=%d notifications=%d moving=%d smigrating=%d smigrated=%d processingErrors=%d",
			c.mode.name, c.ops.Load(), c.errs.Load(), a.TotalNotifications, a.MovingCount,
			a.SMigratingCount, a.SMigratedCount, a.NotificationProcessingErrors)
		for _, b := range c.bad {
			t.Error(b)
		}
		if a.NotificationProcessingErrors != 0 {
			t.Errorf("%s: %d notification processing errors", c.mode.name, a.NotificationProcessingErrors)
		}
	}
}

// TestAutoPipelinePushNotifications drives every standalone autopipeline mode
// (half-duplex, unordered, sharded, full-duplex, full-duplex pipelines)
// through FAILING_OVER/FAILED_OVER, MIGRATING/MIGRATED and MOVING. Each mode
// has its own client. Checks, per mode: the notifications arrive, traffic
// keeps making progress after each phase (including after the MOVING
// handoff), and no reply reaches the wrong command.
func TestAutoPipelinePushNotifications(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	_, factory, testMode, cleanup := SetupTestDatabaseAndFactory(t, ctx, "standalone")
	defer cleanup()
	defer factory.DestroyAll()
	time.Sleep(testMode.DatabaseReadyDelay)
	logCollector.ClearLogs()
	defer logCollector.Clear()

	injector, err := NewNotificationInjector()
	if err != nil {
		t.Fatalf("notification injector: %v", err)
	}
	defer injector.Stop()
	var fi *FaultInjectorClient
	if !testMode.IsProxyMock() {
		if fi, err = CreateTestFaultInjector(); err != nil {
			t.Fatalf("fault injector: %v", err)
		}
	}
	endpointConfig := factory.GetConfig()

	clients := make([]*apSchClient, 0, len(standaloneAPModes))
	for _, m := range standaloneAPModes {
		clients = append(clients, newAPSchClient(t, factory, m, false))
	}
	defer finishAPSch(t, clients)
	for _, c := range clients {
		c.start()
	}
	waitProgress(t, clients, 20, 30*time.Second, "before maintenance")

	notifTimeout := 3 * time.Minute
	if testMode.IsProxyMock() {
		notifTimeout = 15 * time.Second
	}
	runAction := func(action string) {
		t.Helper()
		resp, err := fi.TriggerAction(ctx, ActionRequest{Type: ActionType(action), Parameters: map[string]interface{}{"bdb_id": endpointConfig.BdbID}})
		if err != nil {
			t.Fatalf("trigger %s: %v", action, err)
		}
		status, err := fi.WaitForAction(ctx, resp.ActionID, WithMaxWaitTime(testMode.ActionWaitTimeout), WithPollInterval(2*time.Second))
		if err != nil {
			t.Fatalf("%s action: %v", action, err)
		}
		if status.Status == StatusFailed {
			t.Fatalf("%s action failed: %v", action, status.Error)
		}
	}

	// Failover.
	if testMode.IsProxyMock() {
		if err := injector.InjectFAILING_OVER(ctx, 1000); err != nil {
			t.Fatalf("inject FAILING_OVER: %v", err)
		}
		waitNotification(t, clients, "FAILING_OVER", notifTimeout)
		if err := injector.InjectFAILED_OVER(ctx, 1001); err != nil {
			t.Fatalf("inject FAILED_OVER: %v", err)
		}
	} else {
		runAction("failover")
		waitNotification(t, clients, "FAILING_OVER", notifTimeout)
	}
	waitNotification(t, clients, "FAILED_OVER", notifTimeout)
	waitProgress(t, clients, 20, notifTimeout, "after failover")

	// Migrate.
	if testMode.IsProxyMock() {
		if err := injector.InjectMIGRATING(ctx, 2000, 5000); err != nil {
			t.Fatalf("inject MIGRATING: %v", err)
		}
		waitNotification(t, clients, "MIGRATING", notifTimeout)
		if err := injector.InjectMIGRATED(ctx, 2001, 5000); err != nil {
			t.Fatalf("inject MIGRATED: %v", err)
		}
	} else {
		runAction("migrate")
		waitNotification(t, clients, "MIGRATING", notifTimeout)
	}
	waitNotification(t, clients, "MIGRATED", notifTimeout)
	waitProgress(t, clients, 20, notifTimeout, "after migrate")

	// MOVING: traffic keeps running through the handoff.
	const movingTimeS = 6
	if testMode.IsProxyMock() {
		if err := injector.InjectMOVING(ctx, 3000, movingTimeS, ""); err != nil {
			t.Fatalf("inject MOVING: %v", err)
		}
	} else {
		runAction("bind")
	}
	waitNotification(t, clients, "MOVING", notifTimeout)
	if testMode.IsProxyMock() {
		// Let the handoff (scheduled at timeS/2) and the post-handoff relaxed
		// window pass with traffic running.
		time.Sleep(movingTimeS * time.Second)
	}
	waitProgress(t, clients, 20, notifTimeout, "after MOVING handoff")
}

// TestAutoPipelineClusterSlotMigrate drives the cluster autopipeline modes,
// including cluster full-duplex, through a slot migration
// (SMIGRATING/SMIGRATED). Checks, per mode: both notifications arrive,
// traffic keeps making progress after the migration, and no reply reaches the
// wrong command.
func TestAutoPipelineClusterSlotMigrate(t *testing.T) {
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" {
		t.Skip("Scenario tests require E2E_SCENARIO_TESTS=true")
	}
	ctx, cancel := context.WithTimeout(context.Background(), defaultTestTimeout)
	defer cancel()

	bdbID, factory, testMode, fiClient, cleanup := SetupTestDatabaseForSlotMigrate(t, ctx, SlotMigrateEffectSlotShuffle, SlotMigrateVariantMigrate)
	defer cleanup()
	defer factory.DestroyAll()

	clients := make([]*apSchClient, 0, len(clusterAPModes))
	for _, m := range clusterAPModes {
		clients = append(clients, newAPSchClient(t, factory, m, true))
	}
	defer finishAPSch(t, clients)
	for _, c := range clients {
		if err := c.client.Ping(ctx).Err(); err != nil {
			t.Fatalf("%s: ping: %v", c.mode.name, err)
		}
		c.start()
	}
	waitProgress(t, clients, 20, 30*time.Second, "before slot migration")

	resp, err := fiClient.TriggerSlotMigrateSlotShuffle(ctx, fmt.Sprintf("%d", bdbID), SlotMigrateVariantMigrate)
	if err != nil {
		t.Fatalf("trigger slot migration: %v", err)
	}
	status, err := fiClient.WaitForAction(ctx, resp.ActionID,
		WithMaxWaitTime(testMode.ActionWaitTimeout), WithPollInterval(testMode.ActionPollInterval))
	if err != nil {
		t.Fatalf("slot migration action: %v", err)
	}
	if status.Status == StatusFailed {
		t.Fatalf("slot migration failed: %v", status.Error)
	}

	notifTimeout := 30 * time.Second
	waitNotification(t, clients, "SMIGRATING", notifTimeout)
	waitNotification(t, clients, "SMIGRATED", notifTimeout)
	waitProgress(t, clients, 20, 60*time.Second, "after slot migration")
}
