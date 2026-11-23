package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"weak"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// sentinelCSCDiscovery supplies protocol replies without relying on global Redis
// fixtures. Real replication and invalidation are covered by the live tests.
type sentinelCSCDiscovery struct {
	mu    sync.Mutex
	addr  string
	epoch uint64
}

func (d *sentinelCSCDiscovery) reply(args []string) string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if args[0] == "subscribe" {
		var out string
		for i, ch := range args[1:] {
			out += fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(ch), ch, i+1)
		}
		return out
	}
	if args[0] != "sentinel" {
		return ""
	}
	host, port, _ := net.SplitHostPort(d.addr)
	bulk := func(s string) string { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }
	switch args[1] {
	case "get-master-addr-by-name":
		return "*2\r\n" + bulk(host) + bulk(port)
	case "master":
		return "*8\r\n" + bulk("ip") + bulk(host) + bulk("port") + bulk(port) + bulk("config-epoch") + bulk(strconv.FormatUint(d.epoch, 10)) + bulk("runid") + bulk(d.addr)
	default:
		return "*0\r\n"
	}
}

func (d *sentinelCSCDiscovery) switchTo(addr string) {
	d.mu.Lock()
	d.addr = addr
	d.epoch++
	d.mu.Unlock()
}

func waitSentinelCSC(t testing.TB, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("Sentinel CSC condition timed out")
		}
		time.Sleep(time.Millisecond)
	}
}

func waitSentinelCSCSignal(t testing.TB, signal <-chan struct{}, action string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(6 * time.Second):
		t.Fatalf("timed out waiting for %s", action)
	}
}

// reconcileSentinelCSC waits for every request accepted by this worker run,
// rather than treating an observed wire command as completed reconciliation.
func reconcileSentinelCSC(t testing.TB, c *Client, uncertain bool) {
	t.Helper()
	f := c.opt.failover
	f.enqueueReconcile(uncertain)
	f.mu.RLock()
	done := f.reconcileDone
	f.mu.RUnlock()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("reconciliation did not finish")
	}
}

func sentinelCSCReadyDialer(ready <-chan struct{}) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		select {
		case <-ready:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}
}

func sentinelCSCSubscription(t testing.TB, s *cscPolicyServer) net.Conn {
	t.Helper()
	select {
	case cn := <-s.subscribed:
		return cn
	case <-time.After(5 * time.Second):
		t.Fatal("subscription acknowledgement missing")
		return nil
	}
}

func sentinelCSCSend(t testing.TB, cn net.Conn, channel, payload string) {
	t.Helper()
	if _, err := fmt.Fprintf(cn, "*3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n", len(channel), channel, len(payload), payload); err != nil {
		t.Fatal(err)
	}
}

func sentinelCSCData(t testing.TB, value string) *cscPolicyServer {
	return newCSCPolicyServer(t, func(a []string) string {
		if a[0] == "hello" {
			return "%1\r\n+role\r\n+master\r\n"
		}
		if a[0] == "get" {
			return fmt.Sprintf("$%d\r\n%s\r\n", len(value), value)
		}
		return ""
	})
}

func TestSentinelCSCConnectionModes(t *testing.T) {
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		for _, first := range []string{"client", "conn", "watch", "ephemeral", "coalescer"} {
			t.Run(fmt.Sprint(mode, "/", first), func(t *testing.T) {
				data := sentinelCSCData(t, "value")
				d := &sentinelCSCDiscovery{addr: data.addr}
				sentinel := newCSCPolicyServer(t, d.reply)
				c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{sentinel.addr}, Protocol: 3, PoolSize: 1, MaxRetries: -1, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}, ClientSideCacheTrackingMode: mode, ClientSideCacheCoalesceMisses: first == "coalescer"})
				defer c.Close()
				ctx := context.Background()
				switch first {
				case "conn":
					cn := c.Conn()
					if err := cn.Ping(ctx).Err(); err != nil {
						t.Fatal(err)
					}
					cn.Close()
				case "watch":
					if err := c.Watch(ctx, func(tx *Tx) error { return tx.Ping(ctx).Err() }); err != nil {
						t.Fatal(err)
					}
				case "ephemeral":
					cn := c.EphemeralConn()
					if err := cn.Do(ctx, "reset").Err(); err != nil {
						t.Fatal(err)
					}
					cn.Close()
				}
				for range 2 {
					if got, err := c.Get(WithCache(ctx), "key").Result(); err != nil || got != "value" {
						t.Fatalf("GET=%q, %v", got, err)
					}
				}
				if data.count("get") != 1 || c.CSCStats().Hits < 1 {
					t.Fatalf("GETs=%d stats=%+v", data.count("get"), c.CSCStats())
				}
				if err := c.Get(WithoutCache(ctx), "key").Err(); err != nil {
					t.Fatal(err)
				}
				if data.count("get") != 2 {
					t.Fatal("bypass returned a cached reply")
				}
				for _, e := range data.snapshot() {
					if len(e.args) > 2 && e.args[0] == "client" && e.args[1] == "tracking" {
						text := strings.ToLower(strings.Join(e.args, " "))
						if mode == TrackingModeOptIn && !strings.Contains(text, "optin") {
							t.Fatal(text)
						}
						if mode == TrackingModeOptOut && !strings.Contains(text, "optout") {
							t.Fatal(text)
						}
					}
				}
				data.assertPairs(t)
			})
		}
	}
}

func TestSentinelCSCSwitchAndResubscribe(t *testing.T) {
	a, b := sentinelCSCData(t, "a"), sentinelCSCData(t, "b")
	d := &sentinelCSCDiscovery{addr: a.addr}
	sentinel := newCSCPolicyServer(t, d.reply)
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{sentinel.addr}, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}, Protocol: 3})
	defer c.Close()
	ctx := WithCache(context.Background())
	if got := c.Get(ctx, "key").Val(); got != "a" {
		t.Fatal(got)
	}
	// Ensure the initial subscription reconciliation has completed.
	reconcileSentinelCSC(t, c, true)

	generation := c.opt.failover.epoch.Load()
	reconcileSentinelCSC(t, c, true)

	if c.opt.failover.epoch.Load() != generation {
		t.Fatal("unchanged resubscribe retired connections")
	}
	for _, target := range []struct{ addr, value string }{{b.addr, "b"}, {a.addr, "a"}} {
		before := c.opt.failover.epoch.Load()
		d.switchTo(target.addr)
		reconcileSentinelCSC(t, c, true)
		waitSentinelCSC(t, func() bool { return c.opt.failover.epoch.Load() > before && c.CSCStats().Entries == 0 })
		if got, err := c.Get(ctx, "key").Result(); err != nil || got != target.value {
			t.Fatalf("GET=%q %v", got, err)
		}
	}
}

func TestSentinelCSCLateDial(t *testing.T) {
	for _, drop := range []bool{false, true} {
		t.Run(fmt.Sprint(drop), func(t *testing.T) {
			a, b := sentinelCSCData(t, "a"), sentinelCSCData(t, "b")
			d := &sentinelCSCDiscovery{addr: a.addr}
			sentinel := newCSCPolicyServer(t, d.reply)
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{sentinel.addr}, MaxRetries: -1, Protocol: 3, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}, Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
				if addr == a.addr {
					once.Do(func() { close(entered); <-release })
				}
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			}})
			defer c.Close()
			if drop {
				c.AddHook(sentinelCSCDropDialContext{})
			}
			done := make(chan error, 1)
			go func() {
				got, err := c.Get(WithCache(context.Background()), "key").Result()
				if err == nil && got != "b" {
					err = fmt.Errorf("got %q", got)
				}
				done <- err
			}()
			select {
			case <-entered:
			case <-time.After(5 * time.Second):
				t.Fatal("dial never started")
			}
			d.switchTo(b.addr)
			before := c.opt.failover.epoch.Load()
			reconcileSentinelCSC(t, c, true)
			waitSentinelCSC(t, func() bool { return c.opt.failover.epoch.Load() > before })
			close(release)
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("late dial hung")
			}
			if a.count("get") != 0 {
				t.Fatal("obsolete primary received data command")
			}
		})
	}
}

type sentinelCSCDropDialContext struct{}

func (sentinelCSCDropDialContext) DialHook(next DialHook) DialHook {
	return func(_ context.Context, network, addr string) (net.Conn, error) {
		return next(context.Background(), network, addr)
	}
}
func (sentinelCSCDropDialContext) ProcessHook(next ProcessHook) ProcessHook { return next }
func (sentinelCSCDropDialContext) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

func TestSentinelCSCReplicaOnlyAndCustomScope(t *testing.T) {
	shared := NewLocalCache(ClientSideCacheConfig{MaxEntries: 16})
	aData, bData := sentinelCSCData(t, "a"), sentinelCSCData(t, "b")
	aSentinel := newCSCPolicyServer(t, (&sentinelCSCDiscovery{addr: aData.addr}).reply)
	bSentinel := newCSCPolicyServer(t, (&sentinelCSCDiscovery{addr: bData.addr}).reply)
	opts := &FailoverOptions{MasterName: "test", SentinelAddrs: []string{aSentinel.addr}, ClientSideCache: shared, Protocol: 3}
	a := NewFailoverClient(opts)
	defer a.Close()
	second := *opts
	second.SentinelAddrs = []string{bSentinel.addr}
	b := NewFailoverClient(&second)
	defer b.Close()
	ctx := WithCache(context.Background())
	if got := a.Get(ctx, "key").Val(); got != "a" {
		t.Fatal(got)
	}
	if got := b.Get(ctx, "key").Val(); got != "b" {
		t.Fatalf("cache crossed clients: %q", got)
	}
	clone := a.WithTimeout(time.Second)
	defer clone.Close()
	if got := clone.Get(ctx, "key").Val(); got != "a" || aData.count("get") != 1 {
		t.Fatal("clone did not share its owner's cached reply")
	}
	if bData.count("get") != 1 {
		t.Fatal("independent client did not fetch its own reply")
	}
	opts.ReplicaOnly = true
	c := NewFailoverClient(opts)
	defer c.Close()
	if c.csc != nil {
		t.Fatal("ReplicaOnly enabled CSC")
	}
	u := (&UniversalOptions{ClientSideCache: shared, ReadOnly: true}).Failover()
	if u.ClientSideCache != shared || !u.ReplicaOnly {
		t.Fatal("Universal forwarding")
	}
}

// newSentinelCSCLive owns all processes it changes. It shares the live CSC CI
// gate and process-identity check with the Cluster fixtures.
func newSentinelCSCLive(t *testing.T) ([]*Client, []*SentinelClient) {
	t.Helper()
	binary, err := exec.LookPath("redis-server")
	if err != nil {
		if os.Getenv("GOREDIS_CSC_LIVE") != "" {
			t.Fatal("live CSC tests require redis-server on PATH")
		}
		t.Skip("live Sentinel CSC requires redis-server")
	}
	dir := t.TempDir()
	var nodes []*Client
	var sentinels []*SentinelClient
	for i := 0; i < 5; i++ {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := ln.Addr().(*net.TCPAddr).Port
		ln.Close()
		text := fmt.Sprintf("port %d\nbind 127.0.0.1\nprotected-mode yes\ndir %s\nsave \"\"\nappendonly no\nrepl-diskless-sync-delay 0\n", port, dir)
		if i == 1 {
			text += "replicaof " + strings.ReplaceAll(nodes[0].opt.Addr, ":", " ") + "\n"
		}
		if i >= 2 {
			text += fmt.Sprintf("sentinel monitor test %s 2\nsentinel down-after-milliseconds test 1000\nsentinel failover-timeout test 10000\n", strings.ReplaceAll(nodes[0].opt.Addr, ":", " "))
		}
		cfg := filepath.Join(dir, fmt.Sprintf("node-%d.conf", i))
		if err := os.WriteFile(cfg, []byte(text), 0o600); err != nil {
			t.Fatal(err)
		}
		args := []string{cfg}
		if i >= 2 {
			args = append(args, "--sentinel")
		}
		cmd := exec.Command(binary, args...)
		cmd.Dir = dir
		log, err := os.Create(filepath.Join(dir, fmt.Sprintf("node-%d.log", i)))
		if err != nil {
			t.Fatal(err)
		}
		cmd.Stdout, cmd.Stderr = log, log
		if err := cmd.Start(); err != nil {
			log.Close()
			t.Fatal(err)
		}
		done := make(chan error, 1)
		go func() { done <- cmd.Wait() }()
		t.Cleanup(func() {
			cmd.Process.Kill()
			<-done
			log.Close()
			if t.Failed() {
				output, _ := os.ReadFile(log.Name())
				t.Logf("Redis fixture:\n%s", output)
			}
		})
		addr := net.JoinHostPort("127.0.0.1", strconv.Itoa(port))
		n := NewClient(&Options{Addr: addr, Protocol: 3, MaxRetries: -1, DialTimeout: 100 * time.Millisecond, MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}})
		t.Cleanup(func() { n.Close() })
		clusterCSCLiveOwnedReady(t, n, cmd.Process.Pid)
		if i < 2 {
			nodes = append(nodes, n)
		} else {
			s := NewSentinelClient(&Options{Addr: addr, Protocol: 3, MaxRetries: -1})
			sentinels = append(sentinels, s)
			t.Cleanup(func() { s.Close() })
		}
	}
	clusterCSCLiveEventually(t, func() bool { return len(sentinels[0].Replicas(context.Background(), "test").Val()) == 1 })
	return nodes, sentinels
}

func TestSentinelCSCLive(t *testing.T) {
	nodes, sentinels := newSentinelCSCLive(t)
	ctx := context.Background()
	var clients []*Client
	for _, mode := range []TrackingMode{TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut} {
		addrs := []string{}
		for _, s := range sentinels {
			addrs = append(addrs, s.opt.Addr)
		}
		c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: addrs, Protocol: 3, PoolSize: 2, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 32, DrainInterval: time.Millisecond}, ClientSideCacheTrackingMode: mode, ClientSideCacheCoalesceMisses: mode == TrackingModeOptIn, ClientSideCacheRefreshOnInvalidate: mode == TrackingModeOptOut})
		if mode == TrackingModeOptIn {
			// A cold sticky connection must initialize the same tracking mode.
			cn := c.Conn()
			if err := cn.Ping(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			cn.Close()
		}
		clients = append(clients, c)
		t.Cleanup(func() { c.Close() })
	}
	// This proxy acknowledges subscriptions but never forwards switch events.
	// The fourth client must recover solely from server disconnect/owner eviction.
	discovery := &sentinelCSCDiscovery{addr: nodes[0].opt.Addr}
	proxy := newCSCPolicyServer(t, func(args []string) string {
		if args[0] == "sentinel" {
			info := sentinels[0].Master(ctx, "test").Val()
			discovery.mu.Lock()
			discovery.addr = net.JoinHostPort(info["ip"], info["port"])
			discovery.epoch, _ = strconv.ParseUint(info["config-epoch"], 10, 64)
			discovery.mu.Unlock()
		}
		return discovery.reply(args)
	})
	withoutEvents := NewFailoverClient(&FailoverOptions{
		MasterName: "test", SentinelAddrs: []string{proxy.addr}, Protocol: 3,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 32, DrainInterval: time.Millisecond},
	})
	clients = append(clients, withoutEvents)
	t.Cleanup(func() { withoutEvents.Close() })
	if err := nodes[0].Set(ctx, "key", "first", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if got := nodes[0].Do(ctx, "wait", 1, 5000).Val(); got != int64(1) {
		t.Fatalf("replica not synchronized: %v", got)
	}
	for _, c := range clients {
		if got, err := c.Get(WithCache(ctx), "key").Result(); err != nil || got != "first" {
			t.Fatalf("GET=%q %v", got, err)
		}
		if got := c.Get(WithCache(ctx), "key").Val(); got != "first" || c.CSCStats().Hits == 0 {
			t.Fatalf("cache hit missing: %+v", c.CSCStats())
		}
		cn := c.EphemeralConn()
		if err := cn.Do(ctx, "reset").Err(); err != nil {
			t.Fatal(err)
		}
		cn.Close()
		pub := c.Subscribe(ctx, "test")
		if _, err := pub.ReceiveTimeout(ctx, time.Second); err != nil {
			t.Fatal(err)
		}
		pub.Close()
	}
	// External writes must invalidate foreground/coalesced/background ownership.
	if err := nodes[0].Set(ctx, "key", "second", 0).Err(); err != nil {
		t.Fatal(err)
	}
	for _, c := range clients {
		sentinelCSCLiveEventually(t, func() bool { return c.Get(WithCache(ctx), "key").Val() == "second" })
	}
	for round := 0; round < 2; round++ {
		old := nodes[round%2]
		next := nodes[(round+1)%2]
		if old.Do(ctx, "wait", 1, 5000).Val() != int64(1) {
			t.Fatal("replica did not catch up")
		}
		sentinelCSCLiveEventually(t, func() bool {
			err := sentinels[0].Failover(ctx, "test").Err()
			if err != nil && !strings.Contains(err.Error(), "NOGOODSLAVE") {
				t.Fatal(err)
			}
			return err == nil
		})
		sentinelCSCLiveEventually(t, func() bool {
			addr := sentinels[0].GetMasterAddrByName(ctx, "test").Val()
			return len(addr) == 2 && net.JoinHostPort(addr[0], addr[1]) == next.opt.Addr
		})
		// Do not issue data commands: a lost event must still retire old
		// ownership. Refresh is allowed to have already filled from the new
		// primary, so an empty cache is not the correctness invariant.
		for _, c := range clients {
			sentinelCSCLiveEventually(t, func() bool { return sentinelCSCOwnersCurrent(c) })
		}
		value := fmt.Sprint("round-", round)
		if err := next.Set(ctx, "key", value, 0).Err(); err != nil {
			t.Fatal(err)
		}
		for _, c := range clients {
			// A refresh may have preceded this external write. Its ordinary
			// asynchronous invalidation must arrive before the new value is a hit.
			sentinelCSCLiveEventually(t, func() bool {
				got, err := c.Get(WithCache(ctx), "key").Result()
				return err == nil && got == value
			})
		}
		sentinelCSCLiveEventually(t, func() bool {
			v := old.Info(ctx, "replication").Val()
			return strings.Contains(v, "role:slave") && strings.Contains(v, "master_link_status:up")
		})
		sentinelCSCLiveEventually(t, func() bool {
			v := sentinels[0].Master(ctx, "test").Val()
			return !strings.Contains(v["flags"], "failover")
		})
		// All observers must have seen the demotion before failback. A lagging
		// observer may otherwise issue its old conversion during promotion.
		for _, s := range sentinels {
			sentinelCSCLiveEventually(t, func() bool {
				replicas := s.Replicas(ctx, "test").Val()
				for _, replica := range replicas {
					if net.JoinHostPort(replica["ip"], replica["port"]) == old.opt.Addr {
						return replica["role-reported"] == "slave" && replica["master-link-status"] == "ok"
					}
				}
				return false
			})
		}
	}
}

func sentinelCSCLiveEventually(t testing.TB, check func() bool) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if check() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("live Sentinel condition did not complete")
}

func TestSentinelCSCCustomDialerHostname(t *testing.T) {
	data := sentinelCSCData(t, "value")
	d := &sentinelCSCDiscovery{addr: "primary.example:6379"}
	sentinel := newCSCPolicyServer(t, d.reply)
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{sentinel.addr}, Protocol: 3, MinIdleConns: 2, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}, Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if addr == "primary.example:6379" {
			addr = data.addr
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}})
	defer c.Close()
	for range 2 {
		if got, err := c.Get(WithCache(context.Background()), "key").Result(); err != nil || got != "value" {
			t.Fatalf("GET=%q %v", got, err)
		}
	}
	if data.count("get") != 1 {
		t.Fatal("hostname/custom transport did not cache")
	}
}

func TestSentinelCSCWrongRoleBeforeData(t *testing.T) {
	data := newCSCPolicyServer(t, func(args []string) string {
		if args[0] == "hello" {
			return "%1\r\n+role\r\n+slave\r\n"
		}
		return ""
	})
	d := &sentinelCSCDiscovery{addr: data.addr}
	sentinel := newCSCPolicyServer(t, d.reply)
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{sentinel.addr}, MaxRetries: -1, Protocol: 3, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}})
	defer c.Close()
	if err := c.Get(WithCache(context.Background()), "key").Err(); !errors.Is(err, ErrEndpointChanged) {
		t.Fatalf("wrong role error=%v", err)
	}
	if data.count("get") != 0 {
		t.Fatal("replica received application command")
	}
}

func TestSentinelCSCCloseCancelsReconcile(t *testing.T) {
	data := sentinelCSCData(t, "value")
	d := &sentinelCSCDiscovery{addr: data.addr}
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	sentinel := newCSCPolicyServer(t, func(args []string) string {
		if len(args) > 1 && args[0] == "sentinel" && args[1] == "master" {
			once.Do(func() { close(entered) })
			<-release
		}
		return d.reply(args)
	})
	defer close(release)
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{sentinel.addr}, Protocol: 3, ReadTimeout: -1, WriteTimeout: -1, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}})
	defer c.Close()
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("reconciliation not started")
	}
	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close did not cancel reconciliation")
	}
}

func TestSentinelCSCMetadataRejectsOldHello(t *testing.T) {
	var epoch atomic.Uint64
	epoch.Store(2)
	store := newCommandMetadataStore(&CommandMetadataConfig{Mode: CommandMetadataPreferLive}, nil)
	store.serverFp = "current"
	store.onServerHelloForSource("old", &epoch, 1)
	if fp := store.serverFingerprint(); fp != "current" {
		t.Fatalf("obsolete HELLO changed source identity to %q", fp)
	}
}

func TestSentinelCSCFailedReconcileRetiresCoverage(t *testing.T) {
	data := sentinelCSCData(t, "value")
	d := &sentinelCSCDiscovery{addr: data.addr}
	var unavailable atomic.Bool
	sentinel := newCSCPolicyServer(t, func(args []string) string {
		if unavailable.Load() && args[0] == "sentinel" {
			return "-ERR unavailable\r\n"
		}
		return d.reply(args)
	})
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{sentinel.addr}, Protocol: 3, MaxRetries: -1, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}})
	defer c.Close()
	if err := c.Get(WithCache(context.Background()), "key").Err(); err != nil {
		t.Fatal(err)
	}
	if c.CSCStats().Entries != 1 {
		t.Fatal("entry not cached")
	}
	unavailable.Store(true)
	reconcileSentinelCSC(t, c, true)
	waitSentinelCSC(t, func() bool { return c.CSCStats().Entries == 0 })
}

func TestSentinelCSCInitialSubscriptionWithoutMasterPermission(t *testing.T) {
	data := sentinelCSCData(t, "value")
	d := &sentinelCSCDiscovery{addr: data.addr}
	sentinel := newCSCPolicyServer(t, func(args []string) string {
		if len(args) > 1 && args[0] == "sentinel" && args[1] == "master" {
			return "-NOPERM restricted Sentinel user\r\n"
		}
		return d.reply(args)
	})
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{sentinel.addr}, Protocol: 3, MaxRetries: -1, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}})
	defer c.Close()
	for range 2 {
		if got, err := c.Get(WithCache(context.Background()), "key").Result(); err != nil || got != "value" {
			t.Fatalf("initial GET=%q %v", got, err)
		}
	}
	if c.CSCStats().Hits != 1 {
		t.Fatal("initial subscription discarded valid coverage")
	}
}

func TestSentinelCSCDerivedConnKeepsDiscoveryAlive(t *testing.T) {
	data := sentinelCSCData(t, "value")
	d := &sentinelCSCDiscovery{addr: data.addr}
	sentinel := newCSCPolicyServer(t, d.reply)
	ready := make(chan struct{})
	cn, owner := func() (*Conn, weak.Pointer[Client]) {
		c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{sentinel.addr}, Protocol: 3, Dialer: sentinelCSCReadyDialer(ready), ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}})
		return c.EphemeralConn(), weak.Make(c)
	}()
	defer cn.Close()
	waitSentinelCSC(t, func() bool { runtime.GC(); return owner.Value() == nil })
	close(ready)
	// The canonical wrapper may be collected, but this live derived connection
	// still needs discovery for its first physical connection.
	for range 3 {
		runtime.GC()
		if err := cn.Ping(context.Background()).Err(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSentinelCSCDelayedHelloCannotRestoreOldMetadata(t *testing.T) {
	data := func(version string) *cscPolicyServer {
		return newCSCPolicyServer(t, func(args []string) string {
			if args[0] == "hello" {
				return fmt.Sprintf("%%3\r\n+role\r\n+master\r\n+mode\r\n+standalone\r\n+version\r\n+%s\r\n", version)
			}
			return ""
		})
	}
	a, b := data("7.4.0"), data("8.10.0")
	d := &sentinelCSCDiscovery{addr: a.addr}
	sentinel := newCSCPolicyServer(t, d.reply)
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	c := NewFailoverClient(&FailoverOptions{
		MasterName: "test", SentinelAddrs: []string{sentinel.addr}, Protocol: 3, MaxRetries: -1,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}, CommandMetadata: &CommandMetadataConfig{Mode: CommandMetadataPreferLive},
		OnConnect: func(_ context.Context, cn *Conn) error {
			if cn.opt.failover != nil && !cn.opt.ContextTimeoutEnabled && cn.opt.failover.epoch.Load() == 1 {
				once.Do(func() { close(entered); <-release })
			}
			return nil
		},
	})
	defer c.Close()
	defer releaseOnce.Do(func() { close(release) })
	done := make(chan error, 1)
	go func() { done <- c.Ping(context.Background()).Err() }()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("old initialization did not reach barrier")
	}
	before := c.opt.failover.epoch.Load()
	d.switchTo(b.addr)
	reconcileSentinelCSC(t, c, true)
	waitSentinelCSC(t, func() bool { return c.opt.failover.epoch.Load() > before })
	if err := c.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	fingerprint := c.cmdMeta.serverFingerprint()
	if fingerprint == "" {
		t.Fatal("new primary identity was not initialized")
	}
	releaseOnce.Do(func() { close(release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("old initialization did not complete")
	}
	if got := c.cmdMeta.serverFingerprint(); got != fingerprint {
		t.Fatalf("old HELLO replaced current identity: %q -> %q", fingerprint, got)
	}
}

// sentinelCSCOwnersCurrent permits new-primary refresh publication, but rejects
// every valid entry whose tracked socket was closed or belongs to an old source.
func sentinelCSCOwnersCurrent(c *Client) bool {
	current := c.opt.failover.epoch.Load()
	owners := make(map[uint64]bool)
	p := c.connPool.(*pool.ConnPool)
	_ = p.Filter(func(cn *pool.Conn) bool {
		owners[cn.GetID()] = cn.DialGeneration() == current && !cn.IsClosed()
		return false
	})
	cache := c.csc.(*LocalCache)
	for i := range cache.shards {
		shard := &cache.shards[i]
		shard.mu.RLock()
		valid := true
		for _, entry := range shard.entries {
			if entry.state == cacheEntryValid && !owners[entry.ownerConnID] {
				valid = false
				break
			}
		}
		shard.mu.RUnlock()
		if !valid {
			return false
		}
	}
	return true
}

type sentinelCSCPausedReply struct {
	*StringCmd
	entered chan struct{}
	release chan struct{}
}

func (cmd *sentinelCSCPausedReply) readReply(rd *proto.Reader) error {
	err := cmd.StringCmd.readReply(rd)
	close(cmd.entered)
	<-cmd.release
	return err
}

// A message after SUBSCRIBE is a FIFO barrier for the listener. Waiting for its
// accepted worker, when present, proves the completed result of a real reconnect.
func TestSentinelCSCActualResubscribe(t *testing.T) {
	for _, scenario := range []string{"unchanged", "changed", "unavailable", "older", "disabled", "resp2", "db", "credentials"} {
		t.Run(scenario, func(t *testing.T) {
			data := sentinelCSCData(t, "value")
			d := &sentinelCSCDiscovery{addr: data.addr, epoch: 10}
			var unavailable atomic.Bool
			s := newCSCPolicyServer(t, func(args []string) string {
				if unavailable.Load() && len(args) > 1 && args[0] == "sentinel" && args[1] == "master" {
					return "-NOPERM restricted Sentinel user\r\n"
				}
				return d.reply(args)
			})
			opt := &FailoverOptions{MasterName: "test", SentinelAddrs: []string{s.addr}, Protocol: 3, MaxRetries: -1, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}}
			switch scenario {
			case "disabled":
				opt.ClientSideCacheConfig = nil
			case "resp2":
				opt.Protocol = 2
			case "db":
				opt.DB = 1
			case "credentials":
				opt.CredentialsProvider = func() (string, string) { return "user", "pass" }
			}
			ready := make(chan struct{})
			opt.Dialer = sentinelCSCReadyDialer(ready)
			c := NewFailoverClient(opt)
			defer c.Close()
			f := c.opt.failover
			updates := make(chan struct{}, 8)
			f.onUpdate = func(context.Context) { updates <- struct{}{} }
			close(ready)
			ctx := WithCache(context.Background())
			if got, err := c.Get(ctx, "key").Result(); err != nil || got != "value" {
				t.Fatalf("GET=%q %v", got, err)
			}
			old := sentinelCSCSubscription(t, s)
			select {
			case <-updates:
			case <-time.After(5 * time.Second):
				t.Fatal("listener did not start")
			}
			reconcileSentinelCSC(t, c, false)
			before := f.epoch.Load()
			f.mu.RLock()
			oldDone := f.reconcileDone
			f.mu.RUnlock()
			switch scenario {
			case "changed":
				d.mu.Lock()
				d.epoch++
				d.mu.Unlock()
			case "older":
				d.mu.Lock()
				d.epoch = 1
				d.mu.Unlock()
			case "unavailable", "disabled", "resp2", "db", "credentials":
				unavailable.Store(true)
			}
			if err := old.Close(); err != nil {
				t.Fatal(err)
			}
			replacement := sentinelCSCSubscription(t, s)
			sentinelCSCSend(t, replacement, "+replica-reconf-done", "barrier")
			select {
			case <-updates:
			case <-time.After(5 * time.Second):
				t.Fatal("listener did not pass subscription barrier")
			}
			f.mu.RLock()
			done := f.reconcileDone
			f.mu.RUnlock()
			if done != oldDone {
				select {
				case <-done:
				case <-time.After(6 * time.Second):
					t.Fatal("resubscribe worker did not finish")
				}
			}
			retire := scenario == "changed" || scenario == "unavailable" || scenario == "older"
			if (f.epoch.Load() != before) != retire {
				t.Fatalf("generation %d -> %d, retire=%t", before, f.epoch.Load(), retire)
			}
			if retire && c.CSCStats().Entries != 0 {
				t.Fatal("old coverage survived resubscribe")
			}
			if scenario == "unchanged" {
				if got := c.Get(ctx, "key").Val(); got != "value" || data.count("get") != 1 {
					t.Fatal("unchanged resubscribe discarded a valid hit")
				}
			}
			if !retire && scenario != "unchanged" && data.count("hello") != 1 {
				t.Fatal("disabled cache replaced its healthy data socket")
			}
			if scenario == "older" {
				// An epoch reset is indistinguishable from a lagging observer.
				// Keep retiring on gaps until that observer catches up.
				for _, version := range []uint64{1, 10} {
					if err := c.Get(ctx, "key").Err(); err != nil {
						t.Fatal(err)
					}
					d.mu.Lock()
					d.epoch = version
					d.mu.Unlock()
					before = f.epoch.Load()
					if err := replacement.Close(); err != nil {
						t.Fatal(err)
					}
					replacement = sentinelCSCSubscription(t, s)
					sentinelCSCSend(t, replacement, "+replica-reconf-done", "barrier")
					waitSentinelCSCSignal(t, updates, "repeated resubscribe")
					f.mu.RLock()
					done = f.reconcileDone
					f.mu.RUnlock()
					waitSentinelCSCSignal(t, done, "repeated reconciliation")
					retire = version < 10
					if (f.epoch.Load() != before) != retire || (c.CSCStats().Entries == 0) != retire {
						t.Fatalf("epoch=%d generation %d -> %d, entries=%d", version, before, f.epoch.Load(), c.CSCStats().Entries)
					}
				}
			}
		})
	}
}

func TestSentinelCSCProcessedSwitchKeepsNewCoverage(t *testing.T) {
	a, b := sentinelCSCData(t, "a"), sentinelCSCData(t, "b")
	d := &sentinelCSCDiscovery{addr: a.addr, epoch: 10}
	s := newCSCPolicyServer(t, d.reply)
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{s.addr}, Protocol: 3, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}})
	defer c.Close()
	ctx := WithCache(context.Background())
	if err := c.Get(ctx, "key").Err(); err != nil {
		t.Fatal(err)
	}
	reconcileSentinelCSC(t, c, false)
	d.switchTo(b.addr)
	if err := c.connPool.(*pool.ConnPool).Filter(func(*pool.Conn) bool { return true }); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if got, err := c.Get(ctx, "key").Result(); err != nil || got != "b" {
			t.Fatalf("GET=%q %v", got, err)
		}
	}
	reconcileSentinelCSC(t, c, false)
	before := c.opt.failover.epoch.Load()
	reconcileSentinelCSC(t, c, true)
	if c.opt.failover.epoch.Load() != before || c.CSCStats().Entries != 1 || b.count("get") != 1 {
		t.Fatal("processed switch retired valid new-primary coverage")
	}
	// An obsolete observer without a subscription gap must not retire this source
	// or roll the selected endpoint back. Keep the high-water version intact.
	d.mu.Lock()
	d.epoch = 1
	d.addr = a.addr
	d.mu.Unlock()
	for range 2 {
		reconcileSentinelCSC(t, c, false)
	}
	if c.opt.failover.epoch.Load() != before {
		t.Fatal("baseline refresh repeatedly retired coverage")
	}
	c.opt.failover.mu.RLock()
	addr := c.opt.failover.masterAddr
	version := c.opt.failover.configEpoch
	c.opt.failover.mu.RUnlock()
	if addr != b.addr || version != 11 {
		t.Fatalf("obsolete observer rolled back source: %s, %d", addr, version)
	}
}

func TestSentinelCSCRoleMismatchChoosesAnotherObserver(t *testing.T) {
	a := newCSCPolicyServer(t, func(args []string) string {
		if args[0] == "hello" {
			return "%1\r\n+role\r\n+replica\r\n"
		}
		return ""
	})
	b := sentinelCSCData(t, "b")
	next := newCSCPolicyServer(t, (&sentinelCSCDiscovery{addr: b.addr, epoch: 1}).reply)
	oldDiscovery := &sentinelCSCDiscovery{addr: a.addr}
	old := newCSCPolicyServer(t, func(args []string) string {
		if len(args) > 1 && args[0] == "sentinel" && args[1] == "sentinels" {
			host, port, _ := net.SplitHostPort(next.addr)
			return fmt.Sprintf("*1\r\n*4\r\n+ip\r\n$%d\r\n%s\r\n+port\r\n$%d\r\n%s\r\n", len(host), host, len(port), port)
		}
		return oldDiscovery.reply(args)
	})
	logs := captureCSCPolicyLogs(t)
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{old.addr}, Protocol: 3, MaxRetries: -1})
	defer c.Close()
	ctx := context.Background()
	err := c.Get(ctx, "key").Err()
	if !errors.Is(err, ErrEndpointChanged) || !strings.Contains(err.Error(), "expected primary") || !strings.Contains(err.Error(), "replica") || !strings.Contains(err.Error(), a.addr) {
		t.Fatalf("role diagnostic=%v", err)
	}
	reconcileSentinelCSC(t, c, false)
	if got, err := c.Get(ctx, "key").Result(); err != nil || got != "b" {
		t.Fatalf("recovery GET=%q %v", got, err)
	}
	if a.count("get") != 0 || !logs.contains("expected primary at "+a.addr) {
		t.Fatal("role rejection dispatched data or lost its diagnostic")
	}
}

func TestSentinelCSCPromotionEventEstablishesBaseline(t *testing.T) {
	a, b := sentinelCSCData(t, "a"), sentinelCSCData(t, "b")
	d := &sentinelCSCDiscovery{addr: a.addr, epoch: 10}
	old := &sentinelCSCDiscovery{addr: a.addr, epoch: 11}
	var promoting atomic.Bool
	s := newCSCPolicyServer(t, func(args []string) string {
		if promoting.Load() && len(args) > 1 && args[0] == "sentinel" && args[1] == "master" {
			return old.reply(args)
		}
		return d.reply(args)
	})
	ready := make(chan struct{})
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{s.addr}, Protocol: 3, Dialer: sentinelCSCReadyDialer(ready), ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}})
	defer c.Close()
	f := c.opt.failover
	updates := make(chan struct{}, 8)
	f.onUpdate = func(context.Context) { updates <- struct{}{} }
	close(ready)
	ctx := WithCache(context.Background())
	if err := c.Get(ctx, "key").Err(); err != nil {
		t.Fatal(err)
	}
	sub := sentinelCSCSubscription(t, s)
	waitSentinelCSCSignal(t, updates, "initial subscription")
	reconcileSentinelCSC(t, c, false)
	promoting.Store(true)
	d.switchTo(b.addr)
	if err := c.connPool.(*pool.ConnPool).Filter(func(*pool.Conn) bool { return true }); err != nil {
		t.Fatal(err)
	}
	if got, err := c.Get(ctx, "key").Result(); err != nil || got != "b" {
		t.Fatalf("promoted GET=%q %v", got, err)
	}
	reconcileSentinelCSC(t, c, false)
	f.mu.RLock()
	known := f.configKnown
	f.mu.RUnlock()
	if known {
		t.Fatal("promotion fixture did not leave an unverified baseline")
	}
	promoting.Store(false)
	h1, p1, _ := net.SplitHostPort(a.addr)
	h2, p2, _ := net.SplitHostPort(b.addr)
	sentinelCSCSend(t, sub, "+switch-master", fmt.Sprintf("test %s %s %s %s", h1, p1, h2, p2))
	waitSentinelCSCSignal(t, updates, "promotion event")
	f.mu.RLock()
	done := f.reconcileDone
	f.mu.RUnlock()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("promotion baseline did not complete")
	}
	before := f.epoch.Load()
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	replacement := sentinelCSCSubscription(t, s)
	sentinelCSCSend(t, replacement, "+replica-reconf-done", "barrier")
	waitSentinelCSCSignal(t, updates, "resubscribe barrier")
	f.mu.RLock()
	done = f.reconcileDone
	f.mu.RUnlock()
	select {
	case <-done:
	case <-time.After(6 * time.Second):
		t.Fatal("resubscribe reconciliation did not complete")
	}
	if f.epoch.Load() != before || c.CSCStats().Entries != 1 || c.Get(ctx, "key").Val() != "b" || b.count("get") != 1 {
		t.Fatal("unchanged resubscribe retired coverage established during promotion")
	}
}

func TestSentinelCSCRoleRecoverySkipsStalledObserver(t *testing.T) {
	for _, cache := range []bool{false, true} {
		t.Run(fmt.Sprint("cache=", cache), func(t *testing.T) {
			a := newCSCPolicyServer(t, func(args []string) string {
				if args[0] == "hello" {
					return "%1\r\n+role\r\n+replica\r\n"
				}
				return ""
			})
			b := sentinelCSCData(t, "b")
			entered, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			stalled := newCSCPolicyServer(t, func(args []string) string {
				once.Do(func() { close(entered) })
				<-release
				return "-ERR unavailable\r\n"
			})
			goodDiscovery := &sentinelCSCDiscovery{addr: b.addr, epoch: 1}
			good := newCSCPolicyServer(t, func(args []string) string {
				if len(args) > 1 && args[0] == "sentinel" && args[1] == "get-master-addr-by-name" {
					// Prove the losing query has entered blocking socket I/O.
					select {
					case <-entered:
					case <-release:
					}
				}
				return goodDiscovery.reply(args)
			})
			oldDiscovery := &sentinelCSCDiscovery{addr: a.addr}
			old := newCSCPolicyServer(t, func(args []string) string {
				if len(args) > 1 && args[0] == "sentinel" && args[1] == "sentinels" {
					reply := "*2\r\n"
					for _, addr := range []string{stalled.addr, good.addr} {
						host, port, _ := net.SplitHostPort(addr)
						reply += fmt.Sprintf("*4\r\n+ip\r\n$%d\r\n%s\r\n+port\r\n$%d\r\n%s\r\n", len(host), host, len(port), port)
					}
					return reply
				}
				return oldDiscovery.reply(args)
			})
			opt := &FailoverOptions{MasterName: "test", SentinelAddrs: []string{old.addr}, Protocol: 3, DialTimeout: 2 * time.Second, MaxRetries: -1}
			if cache {
				opt.ClientSideCacheConfig = &ClientSideCacheConfig{MaxEntries: 16}
			}
			c := NewFailoverClient(opt)
			defer c.Close()
			if err := c.Get(context.Background(), "key").Err(); !errors.Is(err, ErrEndpointChanged) {
				t.Fatalf("role error=%v", err)
			}
			result := make(chan error, 2)
			for range 2 {
				go func() {
					got, err := c.opt.failover.MasterAddr(context.Background())
					if err == nil && got != b.addr {
						err = fmt.Errorf("discovered %q, expected %s", got, b.addr)
					}
					result <- err
				}()
			}
			timer := time.NewTimer(500 * time.Millisecond)
			defer timer.Stop()
			for range 2 {
				select {
				case err := <-result:
					if err != nil {
						t.Fatal(err)
					}
				case <-timer.C:
					releaseOnce.Do(func() { close(release) })
					t.Fatal("stalled Sentinel blocked healthy recovery and concurrent acquisition")
				}
			}
			// Let source reconciliation finish before requiring this no-retry GET to succeed.
			reconcileSentinelCSC(t, c, false)
			if got, err := c.Get(context.Background(), "key").Result(); err != nil || got != "b" {
				t.Fatalf("recovery GET=%q %v", got, err)
			}
		})
	}
}

func TestSentinelCSCRoleRecoveryUsesAvailableFallback(t *testing.T) {
	for _, cache := range []bool{false, true} {
		t.Run(fmt.Sprint("cache=", cache), func(t *testing.T) {
			b := sentinelCSCData(t, "b")
			d := &sentinelCSCDiscovery{}
			a := newCSCPolicyServer(t, func(args []string) string {
				if args[0] == "hello" {
					d.switchTo(b.addr)
					return "%1\r\n+role\r\n+replica\r\n"
				}
				return ""
			})
			d.addr = a.addr
			down := newCSCPolicyServer(t, func(args []string) string {
				if args[0] == "sentinel" {
					return "-ERR unavailable\r\n"
				}
				return ""
			})
			const blackhole = "blackhole.test:26379"
			old := newCSCPolicyServer(t, func(args []string) string {
				if len(args) > 1 && args[0] == "sentinel" && args[1] == "sentinels" {
					reply := "*2\r\n"
					for _, addr := range []string{blackhole, down.addr} {
						host, port, _ := net.SplitHostPort(addr)
						reply += fmt.Sprintf("*4\r\n+ip\r\n$%d\r\n%s\r\n+port\r\n$%d\r\n%s\r\n", len(host), host, len(port), port)
					}
					return reply
				}
				return d.reply(args)
			})
			entered, release := make(chan struct{}), make(chan struct{})
			var once, releaseOnce sync.Once
			defer releaseOnce.Do(func() { close(release) })
			opt := &FailoverOptions{MasterName: "test", SentinelAddrs: []string{old.addr}, Protocol: 3, MaxRetries: -1}
			opt.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
				if addr == blackhole {
					once.Do(func() { close(entered) })
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-release:
						return nil, errors.New("test discovery released")
					}
				}
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			}
			if cache {
				opt.ClientSideCacheConfig = &ClientSideCacheConfig{MaxEntries: 16}
			}
			c := NewFailoverClient(opt)
			defer c.Close()
			if err := c.Get(context.Background(), "key").Err(); !errors.Is(err, ErrEndpointChanged) {
				t.Fatalf("role error=%v", err)
			}
			waitSentinelCSCSignal(t, entered, "blackholed discovery dial")
			for attempt := range 2 {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				result := make(chan error, 1)
				go func() {
					addr, err := c.opt.failover.MasterAddr(ctx)
					if err == nil && addr != b.addr {
						err = fmt.Errorf("discovered %q, expected %s", addr, b.addr)
					}
					result <- err
				}()
				select {
				case err := <-result:
					cancel()
					if err != nil {
						t.Fatalf("discovery attempt %d: %v", attempt, err)
					}
				case <-time.After(900 * time.Millisecond):
					cancel()
					releaseOnce.Do(func() { close(release) })
					t.Fatalf("discovery attempt %d ignored the available fallback", attempt)
				}
			}
			reconcileSentinelCSC(t, c, false)
			if got, err := c.Get(context.Background(), "key").Result(); err != nil || got != "b" {
				t.Fatalf("recovery GET=%q %v", got, err)
			}
		})
	}
}

func TestSentinelCSCFallbackPreferenceBudget(t *testing.T) {
	for _, scenario := range []string{"deadline", "unbounded", "canceled"} {
		t.Run(scenario, func(t *testing.T) {
			data := sentinelCSCData(t, "value")
			d := &sentinelCSCDiscovery{addr: data.addr}
			entered, fallbackReady, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var once, readyOnce sync.Once
			defer close(release)
			s := newCSCPolicyServer(t, func(args []string) string {
				if len(args) > 1 && args[0] == "sentinel" && args[1] == "get-master-addr-by-name" {
					select {
					case <-entered:
					case <-release:
					}
					readyOnce.Do(func() { close(fallbackReady) })
				}
				return d.reply(args)
			})
			const blackhole = "blackhole.test:26379"
			opt := &FailoverOptions{MasterName: "test", MaxRetries: -1}
			opt.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
				if addr == blackhole {
					once.Do(func() { close(entered) })
					select {
					case <-ctx.Done():
						return nil, ctx.Err()
					case <-release:
						return nil, errors.New("test discovery released")
					}
				}
				return (&net.Dialer{}).DialContext(ctx, network, addr)
			}
			f := &sentinelFailover{opt: opt, sentinelAddrs: []string{blackhole, s.addr}, rediscover: s.addr}
			t.Cleanup(func() { _ = f.Close() })
			var ctx context.Context
			var cancel context.CancelFunc
			if scenario == "deadline" {
				ctx, cancel = context.WithTimeout(context.Background(), 200*time.Millisecond)
			} else {
				ctx, cancel = context.WithCancel(context.Background())
			}
			defer cancel()
			result := make(chan error, 1)
			go func() {
				addr, err := f.MasterAddr(ctx)
				if err == nil && addr != data.addr {
					err = fmt.Errorf("discovered %q, expected %s", addr, data.addr)
				}
				result <- err
			}()
			waitSentinelCSCSignal(t, fallbackReady, "fallback response")
			if scenario == "canceled" {
				cancel()
			}
			select {
			case err := <-result:
				if scenario == "canceled" {
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("canceled discovery=%v", err)
					}
				} else if err != nil || ctx.Err() != nil {
					t.Fatalf("fallback did not preserve the caller's budget: %v, ctx=%v", err, ctx.Err())
				}
			case <-time.After(900 * time.Millisecond):
				t.Fatal("peer preference excluded an available fallback indefinitely")
			}
		})
	}
}

func TestSentinelCSCDiscoveryCancellationClosesQueries(t *testing.T) {
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s := newCSCPolicyServer(t, func([]string) string {
		once.Do(func() { close(entered) })
		<-release
		return "-ERR unavailable\r\n"
	})
	defer close(release)
	f := &sentinelFailover{
		opt:           &FailoverOptions{MasterName: "test", ReadTimeout: -2, WriteTimeout: -2, MaxRetries: -1},
		sentinelAddrs: []string{s.addr},
	}
	t.Cleanup(func() { _ = f.Close() })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := f.MasterAddr(ctx); done <- err }()
	waitSentinelCSCSignal(t, entered, "blocked discovery query")
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("discovery error=%v", err)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("canceled discovery retained its lock and blocked socket")
	}
}

func TestSentinelCSCReconcileDeadlineWithDisabledSocketTimeouts(t *testing.T) {
	data := sentinelCSCData(t, "value")
	d := &sentinelCSCDiscovery{addr: data.addr}
	var blocked atomic.Bool
	entered, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	s := newCSCPolicyServer(t, func(args []string) string {
		if blocked.Load() && len(args) > 1 && args[0] == "sentinel" && args[1] == "master" {
			once.Do(func() { close(entered) })
			<-release
		}
		return d.reply(args)
	})
	defer close(release)
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{s.addr}, Protocol: 3, ReadTimeout: -2, WriteTimeout: -2, MaxRetries: -1, ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}})
	defer c.Close()
	if err := c.Get(WithCache(context.Background()), "key").Err(); err != nil {
		t.Fatal(err)
	}
	reconcileSentinelCSC(t, c, false)
	blocked.Store(true)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() { c.opt.failover.reconcilePrimary(ctx, true); close(done) }()
	waitSentinelCSCSignal(t, entered, "blocked MASTER reply")
	select {
	case <-done:
		if c.CSCStats().Entries != 0 {
			t.Fatal("unverifiable coverage survived the reconciliation deadline")
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("data socket settings disabled the internal reconciliation deadline")
	}
}

func TestSentinelCSCBaselineOverlapsResubscribe(t *testing.T) {
	data := sentinelCSCData(t, "value")
	d := &sentinelCSCDiscovery{addr: data.addr}
	entered, release := make(chan struct{}), make(chan struct{})
	var once, releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	s := newCSCPolicyServer(t, func(args []string) string {
		if len(args) > 1 && args[0] == "sentinel" && args[1] == "master" {
			once.Do(func() { close(entered); <-release })
		}
		return d.reply(args)
	})
	ready := make(chan struct{})
	c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{s.addr}, Protocol: 3, Dialer: sentinelCSCReadyDialer(ready), ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16}})
	defer c.Close()
	f := c.opt.failover
	updates := make(chan struct{}, 8)
	f.onUpdate = func(context.Context) { updates <- struct{}{} }
	close(ready)
	if err := c.Get(WithCache(context.Background()), "key").Err(); err != nil {
		t.Fatal(err)
	}
	sub := sentinelCSCSubscription(t, s)
	waitSentinelCSCSignal(t, updates, "initial subscription")
	waitSentinelCSCSignal(t, entered, "paused baseline")
	before := f.epoch.Load()
	d.mu.Lock()
	d.epoch++
	d.mu.Unlock()
	if err := sub.Close(); err != nil {
		t.Fatal(err)
	}
	replacement := sentinelCSCSubscription(t, s)
	sentinelCSCSend(t, replacement, "+replica-reconf-done", "barrier")
	waitSentinelCSCSignal(t, updates, "resubscribe barrier")
	f.mu.RLock()
	done, pending := f.reconcileDone, f.reconcileUncertain
	f.mu.RUnlock()
	if !pending {
		t.Fatal("subscription uncertainty did not overlap the paused baseline")
	}
	releaseOnce.Do(func() { close(release) })
	waitSentinelCSCSignal(t, done, "reconciliation")
	if f.epoch.Load() == before || c.CSCStats().Entries != 0 {
		t.Fatal("old baseline task masked the queued subscription gap")
	}
}

func TestSentinelCSCOptionsCopyCannotPublishRetiredEntries(t *testing.T) {
	a, b := sentinelCSCData(t, "a"), sentinelCSCData(t, "b")
	d := &sentinelCSCDiscovery{addr: a.addr}
	s := newCSCPolicyServer(t, d.reply)
	ready := make(chan struct{})
	parent := NewFailoverClient(&FailoverOptions{
		MasterName: "test", SentinelAddrs: []string{s.addr}, Protocol: 3,
		Dialer: sentinelCSCReadyDialer(ready), ClientSideCache: NewLocalCache(ClientSideCacheConfig{MaxEntries: 16}),
	})
	defer parent.Close()
	updates := make(chan struct{}, 8)
	parent.opt.failover.onUpdate = func(context.Context) { updates <- struct{}{} }
	close(ready)
	if err := parent.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	sub := sentinelCSCSubscription(t, s)
	waitSentinelCSCSignal(t, updates, "initial subscription")
	child := NewClient(parent.Options())
	defer child.Close()
	ctx := WithCache(context.Background())
	if got, err := child.Get(ctx, "key").Result(); err != nil || got != "a" {
		t.Fatalf("child GET=%q %v", got, err)
	}
	reconcileSentinelCSC(t, parent, false)
	d.switchTo(b.addr)
	h1, p1, _ := net.SplitHostPort(a.addr)
	h2, p2, _ := net.SplitHostPort(b.addr)
	sentinelCSCSend(t, sub, "+switch-master", fmt.Sprintf("test %s %s %s %s", h1, p1, h2, p2))
	waitSentinelCSCSignal(t, updates, "primary switch")
	for _, c := range []*Client{parent, child} {
		if got, err := c.Get(ctx, "key").Result(); err != nil || got != "b" {
			t.Fatalf("GET after switch=%q %v", got, err)
		}
	}
}

func TestSentinelCSCOptionsCopyKeepsDiscoveryAlive(t *testing.T) {
	for _, initialized := range []bool{false, true} {
		t.Run(fmt.Sprint("initialized=", initialized), func(t *testing.T) {
			data := sentinelCSCData(t, "value")
			sentinel := newCSCPolicyServer(t, (&sentinelCSCDiscovery{addr: data.addr}).reply)
			child, wrapper, owner, source := func() (*Client, weak.Pointer[Client], weak.Pointer[baseClient], *sentinelFailover) {
				parent := NewFailoverClient(&FailoverOptions{
					MasterName: "test", SentinelAddrs: []string{sentinel.addr}, Protocol: 3, MaxRetries: -1,
				})
				opt := *parent.Options()
				if initialized {
					opt.ConnMaxLifetime = time.Nanosecond
				}
				child := NewClient(&opt)
				if initialized {
					if err := child.Ping(context.Background()).Err(); err != nil {
						t.Fatal(err)
					}
				}
				return child, weak.Make(parent), weak.Make(parent.baseClient), parent.opt.failover
			}()
			defer source.Close()
			defer child.Close()
			waitSentinelCSC(t, func() bool { runtime.GC(); return wrapper.Value() == nil })
			// If the shared base was collected, wait for its asynchronous cleanup
			// before testing the child; otherwise an early GET can mask the bug.
			if owner.Value() == nil {
				waitSentinelCSC(t, func() bool {
					source.mu.RLock()
					defer source.mu.RUnlock()
					return source.closed
				})
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if got, err := child.Get(ctx, "key").Result(); err != nil || got != "value" {
				t.Fatalf("GET after source wrapper GC=%q %v", got, err)
			}
		})
	}
}

func TestSentinelCSCOptionsCopyReleasesDiscovery(t *testing.T) {
	owner, source := func() (weak.Pointer[baseClient], *sentinelFailover) {
		parent := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{"127.0.0.1:1"}})
		child := NewClient(parent.Options())
		if err := child.Close(); err != nil {
			t.Fatal(err)
		}
		return weak.Make(parent.baseClient), parent.opt.failover
	}()
	defer source.Close()
	waitSentinelCSC(t, func() bool {
		runtime.GC()
		source.mu.RLock()
		defer source.mu.RUnlock()
		return owner.Value() == nil && source.closed
	})
}

func TestSentinelCSCRecorderSurvivesFailover(t *testing.T) {
	a, b := sentinelCSCData(t, "a"), sentinelCSCData(t, "b")
	d := &sentinelCSCDiscovery{addr: a.addr}
	s := newCSCPolicyServer(t, d.reply)
	ownerRecorder, otherRecorder := newCountingRecorder(), newCountingRecorder()
	other := NewClient(&Options{Addr: a.addr, Protocol: 3, OTelRecorder: otherRecorder})
	defer other.Close()
	if err := other.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	ready := make(chan struct{})
	c := NewFailoverClient(&FailoverOptions{
		MasterName: "test", SentinelAddrs: []string{s.addr}, Protocol: 3,
		Dialer: sentinelCSCReadyDialer(ready), OTelRecorder: ownerRecorder,
		ClientSideCacheConfig: &ClientSideCacheConfig{MaxEntries: 16},
	})
	defer c.Close()
	updates := make(chan struct{}, 8)
	c.opt.failover.onUpdate = func(context.Context) { updates <- struct{}{} }
	close(ready)
	ctx := WithCache(context.Background())
	for range 2 {
		if got, err := c.Get(ctx, "key").Result(); err != nil || got != "a" {
			t.Fatalf("initial GET=%q %v", got, err)
		}
	}
	sub := sentinelCSCSubscription(t, s)
	waitSentinelCSCSignal(t, updates, "initial subscription")
	reconcileSentinelCSC(t, c, false)
	otherRecorder.reset()
	d.switchTo(b.addr)
	h1, p1, _ := net.SplitHostPort(a.addr)
	h2, p2, _ := net.SplitHostPort(b.addr)
	sentinelCSCSend(t, sub, "+switch-master", fmt.Sprintf("test %s %s %s %s", h1, p1, h2, p2))
	waitSentinelCSCSignal(t, updates, "primary switch")
	reconcileSentinelCSC(t, c, false)
	for range 2 {
		if got, err := c.Get(ctx, "key").Result(); err != nil || got != "b" {
			t.Fatalf("recovery GET=%q %v", got, err)
		}
	}
	if a.count("get") != 1 || b.count("get") != 1 || ownerRecorder.get("op:get") != 4 || ownerRecorder.get("closed") == 0 {
		t.Fatal("cache ownership or recorder attribution was lost across failover")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if ownerRecorder.connCount() != 0 {
		t.Fatalf("failover connection metrics did not balance: %d", ownerRecorder.connCount())
	}
	if otherRecorder.get("closed") != 0 || otherRecorder.get("op:get") != 0 || otherRecorder.connCount() != 1 {
		t.Fatal("failover events reached another client's recorder")
	}
}

func TestSentinelCSCDerivedConnectionFailoverAfterGC(t *testing.T) {
	for _, kind := range []string{"conn", "ephemeral", "clone"} {
		for _, cache := range []bool{false, true} {
			if kind == "clone" && cache {
				continue
			} // CSC clones deliberately retain the owner.
			t.Run(fmt.Sprint(kind, "/cache=", cache), func(t *testing.T) {
				a, b := sentinelCSCData(t, "a"), sentinelCSCData(t, "b")
				d := &sentinelCSCDiscovery{addr: a.addr}
				s := newCSCPolicyServer(t, d.reply)
				type session interface {
					Get(context.Context, string) *StringCmd
					Close() error
				}
				updates := make(chan struct{}, 8)
				derived, owner, f := func() (session, weak.Pointer[Client], *sentinelFailover) {
					opt := &FailoverOptions{MasterName: "test", SentinelAddrs: []string{s.addr}, Protocol: 3, MaxRetries: -1}
					if cache {
						opt.ClientSideCacheConfig = &ClientSideCacheConfig{MaxEntries: 16}
					}
					ready := make(chan struct{})
					opt.Dialer = sentinelCSCReadyDialer(ready)
					c := NewFailoverClient(opt)
					c.opt.failover.onUpdate = func(context.Context) { updates <- struct{}{} }
					close(ready)
					var result session
					switch kind {
					case "conn":
						result = c.Conn()
					case "ephemeral":
						result = c.EphemeralConn()
					case "clone":
						result = c.WithTimeout(time.Second)
					}
					if got, err := result.Get(context.Background(), "key").Result(); err != nil || got != "a" {
						t.Fatalf("first GET=%q %v", got, err)
					}
					return result, weak.Make(c), c.opt.failover
				}()
				defer derived.Close()
				defer f.Close()
				sub := sentinelCSCSubscription(t, s)
				select {
				case <-updates:
				case <-time.After(5 * time.Second):
					t.Fatal("listener not started")
				}
				waitSentinelCSC(t, func() bool { runtime.GC(); return owner.Value() == nil })
				d.switchTo(b.addr)
				h1, p1, _ := net.SplitHostPort(a.addr)
				h2, p2, _ := net.SplitHostPort(b.addr)
				sentinelCSCSend(t, sub, "+switch-master", fmt.Sprintf("test %s %s %s %s", h1, p1, h2, p2))
				select {
				case <-updates:
				case <-time.After(5 * time.Second):
					t.Fatal("switch was not processed")
				}
				if kind == "clone" && derived.(*Client).PoolStats().TotalConns != 0 {
					t.Fatal("clone retained the retired idle connection until its next borrow")
				}
				got, err := derived.Get(context.Background(), "key").Result()
				if err == nil && got != "b" {
					t.Fatalf("retired primary served GET=%q", got)
				}
				if a.count("get") != 1 {
					t.Fatal("derived session sent another command to the retired primary")
				}
			})
		}
	}
}

func TestSentinelCSCRoleDiagnosticOnce(t *testing.T) {
	for _, protocol := range []int{2, 3} {
		t.Run(fmt.Sprint("resp=", protocol), func(t *testing.T) {
			data := newCSCPolicyServer(t, func(args []string) string {
				if args[0] == "hello" {
					if args[1] == "2" {
						return "*2\r\n+role\r\n+replica\r\n"
					}
					return "%1\r\n+role\r\n+replica\r\n"
				}
				return ""
			})
			sentinel := newCSCPolicyServer(t, (&sentinelCSCDiscovery{addr: data.addr}).reply)
			logs := captureCSCPolicyLogs(t)
			c := NewFailoverClient(&FailoverOptions{MasterName: "test", SentinelAddrs: []string{sentinel.addr}, Protocol: protocol, MaxRetries: -1})
			defer c.Close()
			for range 2 {
				err := c.Get(context.Background(), "key").Err()
				if !errors.Is(err, ErrEndpointChanged) || !strings.Contains(err.Error(), "replica") || !strings.Contains(err.Error(), data.addr) {
					t.Fatalf("role diagnostic=%v", err)
				}
				reconcileSentinelCSC(t, c, false)
			}
			logs.mu.Lock()
			warnings := 0
			for _, message := range logs.messages {
				if strings.Contains(message, "expected primary at "+data.addr) {
					warnings++
				}
			}
			logs.mu.Unlock()
			if warnings != 1 || data.count("get") != 0 {
				t.Fatalf("warnings=%d, data commands=%d", warnings, data.count("get"))
			}
		})
	}
}
