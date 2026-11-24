package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/hashtag"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
)

const clusterDisabledReply = "-ERR This instance has cluster support disabled\r\n"

// closeConnReply makes fallbackTestServer drop the connection.
const closeConnReply = "\x00close"

func TestIsClusterUnsupportedError(t *testing.T) {
	for _, tc := range []struct {
		err  error
		want bool
	}{
		{nil, false},
		{proto.RedisError("ERR This instance has cluster support disabled"), true},
		{proto.RedisError("ERR unknown command 'CLUSTER', with args beginning with: 'SLOTS' "), true},
		{proto.RedisError("ERR unknown command `READONLY`, with args beginning with: "), true},
		{proto.RedisError("ERR unknown subcommand 'SLOTS'. Try CLUSTER HELP."), true},
		{fmt.Errorf("wrapped: %w", proto.RedisError("ERR This instance has cluster support disabled")), true},
		// A hook that wraps the error must not hide the server's message.
		{fmt.Errorf("operation failed: %w", proto.RedisError("ERR unknown command 'CLUSTER'")), true},
		{fmt.Errorf("operation failed: %w", proto.RedisError("ERR unknown subcommand 'SLOTS'")), true},
		{fmt.Errorf("hook: %w", proto.RedisError("ERR unknown command 'READONLY', with args beginning with: ")), true},
		{proto.RedisError("NOPERM this user has no permissions to run the 'cluster|slots' command"), false},
		{proto.RedisError("CLUSTERDOWN The cluster is down"), false},
		{errors.New("ERR This instance has cluster support disabled"), false}, // not a server reply
		{&net.OpError{Op: "dial", Err: errors.New("connection refused")}, false},
	} {
		if got := isClusterUnsupportedError(tc.err); got != tc.want {
			t.Errorf("isClusterUnsupportedError(%v) = %v; want %v", tc.err, got, tc.want)
		}
	}
}

func TestParseClusterURLStandaloneFallback(t *testing.T) {
	opt, err := ParseClusterURL("redis://localhost:6379?standalone_fallback=true")
	if err != nil {
		t.Fatal(err)
	}
	if !opt.StandaloneFallback {
		t.Fatal("StandaloneFallback = false; want true")
	}
	if !opt.clientOptions().ignoreReadOnlyUnsupported {
		t.Fatal("node options do not ignore a rejected READONLY")
	}
}

func TestStandaloneFallbackReloadIntervalDefault(t *testing.T) {
	for _, tc := range []struct {
		opt  ClusterOptions
		want time.Duration
	}{
		{ClusterOptions{}, 60 * time.Second},
		{ClusterOptions{StandaloneFallback: true}, 10 * time.Second},
		{ClusterOptions{StandaloneFallback: true, ClusterStateReloadInterval: time.Minute}, time.Minute},
	} {
		opt := tc.opt
		opt.init()
		if opt.ClusterStateReloadInterval != tc.want {
			t.Errorf("fallback=%v interval=%v: got %v; want %v",
				tc.opt.StandaloneFallback, tc.opt.ClusterStateReloadInterval, opt.ClusterStateReloadInterval, tc.want)
		}
	}
}

func TestClusterStandaloneFallback(t *testing.T) {
	ctx := context.Background()
	standalone := func(args []string) string {
		switch args[0] {
		case "hello":
			return "%0\r\n"
		case "cluster", "readonly":
			return clusterDisabledReply
		case "get":
			return "$1\r\na\r\n"
		}
		return "+OK\r\n"
	}

	t.Run("enabled", func(t *testing.T) {
		addr := newFallbackTestServer(t, standalone)
		client := newFallbackTestClient(t, &ClusterOptions{Addrs: []string{addr}, StandaloneFallback: true})

		if val, err := client.Get(ctx, "key").Result(); err != nil || val != "a" {
			t.Fatalf("Get() = %q, %v; want %q, nil", val, err, "a")
		}
		assertStandaloneState(t, client, addr)

		var masters atomic.Int32
		if err := client.ForEachMaster(ctx, func(ctx context.Context, c *Client) error {
			masters.Add(1)
			return nil
		}); err != nil {
			t.Fatalf("ForEachMaster() = %v", err)
		}
		if n := masters.Load(); n != 1 {
			t.Fatalf("ForEachMaster visited %d nodes; want 1", n)
		}

		// Keys in different slots go to the one node.
		cmds, err := client.Pipelined(ctx, func(pipe Pipeliner) error {
			for i := 0; i < 10; i++ {
				pipe.Get(ctx, "key"+strconv.Itoa(i))
			}
			return nil
		})
		if err != nil {
			t.Fatalf("Pipelined() = %v", err)
		}
		for _, cmd := range cmds {
			if val := cmd.(*StringCmd).Val(); val != "a" {
				t.Fatalf("pipeline Get = %q; want %q", val, "a")
			}
		}
	})

	t.Run("disabled", func(t *testing.T) {
		addr := newFallbackTestServer(t, standalone)
		client := newFallbackTestClient(t, &ClusterOptions{Addrs: []string{addr}})

		err := client.Get(ctx, "key").Err()
		if !isClusterUnsupportedError(err) {
			t.Fatalf("Get() = %v; want cluster support disabled error", err)
		}
	})

	t.Run("read only", func(t *testing.T) {
		addr := newFallbackTestServer(t, standalone)
		client := newFallbackTestClient(t, &ClusterOptions{
			Addrs:              []string{addr},
			StandaloneFallback: true,
			RouteRandomly:      true,
		})

		if val, err := client.Get(ctx, "key").Result(); err != nil || val != "a" {
			t.Fatalf("Get() = %q, %v; want %q, nil", val, err, "a")
		}
		assertStandaloneState(t, client, addr)
	})

	t.Run("rejected READONLY", func(t *testing.T) {
		// READONLY fails conn init unless the fallback asks to ignore it.
		addr := newFallbackTestServer(t, standalone)
		for _, ignore := range []bool{false, true} {
			node := NewClient(&Options{
				Addr:                      addr,
				Protocol:                  3,
				DisableIdentity:           true,
				MaintNotificationsConfig:  &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				readOnly:                  true,
				ignoreReadOnlyUnsupported: ignore,
			})
			err := node.Get(ctx, "key").Err()
			_ = node.Close()
			if ignore && err != nil {
				t.Fatalf("Get() with ignored READONLY error = %v; want nil", err)
			}
			if !ignore && !isClusterUnsupportedError(err) {
				t.Fatalf("Get() with READONLY = %v; want cluster support disabled error", err)
			}
		}
	})

	t.Run("other init errors still fail", func(t *testing.T) {
		addr := newFallbackTestServer(t, func(args []string) string {
			if args[0] == "client" && args[1] == "setname" {
				return "-ERR setname rejected\r\n"
			}
			return standalone(args)
		})
		node := NewClient(&Options{
			Addr:                      addr,
			Protocol:                  3,
			DisableIdentity:           true,
			ClientName:                "x",
			MaintNotificationsConfig:  &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
			readOnly:                  true,
			ignoreReadOnlyUnsupported: true,
		})
		t.Cleanup(func() { _ = node.Close() })
		if err := node.Get(ctx, "key").Err(); err == nil || !strings.Contains(err.Error(), "setname rejected") {
			t.Fatalf("Get() = %v; want setname error", err)
		}
	})

	t.Run("network error does not fall back", func(t *testing.T) {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		addr := ln.Addr().String()
		_ = ln.Close()

		client := newFallbackTestClient(t, &ClusterOptions{
			Addrs:              []string{addr},
			StandaloneFallback: true,
			MaxRedirects:       -1,
			DialTimeout:        100 * time.Millisecond,
		})
		if err := client.Get(ctx, "key").Err(); err == nil || isClusterUnsupportedError(err) {
			t.Fatalf("Get() = %v; want dial error", err)
		}
		if state, _ := client.state.state.Load().(*clusterState); state != nil {
			t.Fatalf("state loaded from an unreachable node")
		}
	})

	t.Run("real cluster seed wins", func(t *testing.T) {
		var clusterAddr string
		clusterAddr = newFallbackTestServer(t, func(args []string) string {
			switch {
			case args[0] == "hello":
				return "%0\r\n"
			case args[0] == "cluster" && args[1] == "shards":
				return "-ERR unknown subcommand 'shards'\r\n"
			case args[0] == "cluster" && args[1] == "slots":
				return slotsReply(slotRange{0, 16383, clusterAddr})
			case args[0] == "get":
				return "$1\r\nc\r\n"
			}
			return "+OK\r\n"
		})
		standaloneAddr := newFallbackTestServer(t, standalone)

		for i := 0; i < 10; i++ { // seeds are tried in random order
			client := newFallbackTestClient(t, &ClusterOptions{
				Addrs:              []string{standaloneAddr, clusterAddr},
				StandaloneFallback: true,
			})
			if val, err := client.Get(ctx, "key").Result(); err != nil || val != "c" {
				t.Fatalf("Get() = %q, %v; want %q, nil", val, err, "c")
			}
			state, _ := client.state.Get(ctx)
			if state.standalone {
				t.Fatalf("standalone state chosen over a real cluster seed")
			}
			_ = client.Close()
		}
	})
}

// An endpoint that turns into a cluster is picked up through MOVED, and one
// that turns back into a standalone server through a topology reload.
func TestClusterStandaloneFallbackTransitions(t *testing.T) {
	ctx := context.Background()

	var aAddr, bAddr string
	var aCluster, bUp atomic.Bool
	bUp.Store(true)
	clusterSlots := func() string {
		return slotsReply(slotRange{0, 8191, aAddr}, slotRange{8192, 16383, bAddr})
	}
	aAddr = newFallbackTestServer(t, func(args []string) string {
		switch args[0] {
		case "hello":
			return "%0\r\n"
		case "cluster":
			if !aCluster.Load() {
				return clusterDisabledReply
			}
			if args[1] == "slots" {
				return clusterSlots()
			}
			return "-ERR unknown subcommand\r\n"
		case "get":
			if aCluster.Load() && hashtag.Slot(args[1]) > 8191 {
				return fmt.Sprintf("-MOVED %d %s\r\n", hashtag.Slot(args[1]), bAddr)
			}
			return "$1\r\na\r\n"
		}
		return "+OK\r\n"
	})
	bAddr = newFallbackTestServer(t, func(args []string) string {
		if !bUp.Load() {
			return closeConnReply
		}
		switch args[0] {
		case "hello":
			return "%0\r\n"
		case "cluster":
			if args[1] == "slots" {
				return clusterSlots()
			}
			return "-ERR unknown subcommand\r\n"
		case "get":
			return "$1\r\nb\r\n"
		}
		return "+OK\r\n"
	})

	keyB := "key"
	for i := 0; hashtag.Slot(keyB) <= 8191; i++ {
		keyB = "key" + strconv.Itoa(i)
	}

	client := newFallbackTestClient(t, &ClusterOptions{Addrs: []string{aAddr}, StandaloneFallback: true})

	// Standalone: every key goes to A.
	if val, err := client.Get(ctx, keyB).Result(); err != nil || val != "a" {
		t.Fatalf("standalone Get() = %q, %v; want %q, nil", val, err, "a")
	}
	assertStandaloneState(t, client, aAddr)

	// A becomes a cluster member: MOVED sends the command to B and reloads.
	aCluster.Store(true)
	if val, err := client.Get(ctx, keyB).Result(); err != nil || val != "b" {
		t.Fatalf("cluster Get() = %q, %v; want %q, nil", val, err, "b")
	}
	waitForState(t, client, func(s *clusterState) bool { return !s.standalone && len(s.Masters) == 2 })
	if val, err := client.Get(ctx, keyB).Result(); err != nil || val != "b" {
		t.Fatalf("cluster Get() after reload = %q, %v; want %q, nil", val, err, "b")
	}

	// A goes back to standalone and B goes away.
	aCluster.Store(false)
	bUp.Store(false)
	if _, err := client.state.Reload(ctx); err != nil {
		t.Fatalf("Reload() = %v", err)
	}
	assertStandaloneState(t, client, aAddr)
	if val, err := client.Get(ctx, keyB).Result(); err != nil || val != "a" {
		t.Fatalf("standalone Get() after downgrade = %q, %v; want %q, nil", val, err, "a")
	}
}

// errWrapHook wraps every command error and keeps it on the command, as the
// hook contract asks.
type errWrapHook struct{}

func (errWrapHook) DialHook(next DialHook) DialHook { return next }

func (errWrapHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		if err := next(ctx, cmd); err != nil {
			wrapped := fmt.Errorf("operation failed: %w", err)
			cmd.SetErr(wrapped)
			return wrapped
		}
		return nil
	}
}

func (errWrapHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook { return next }

// A node hook that wraps errors must not hide an unknown CLUSTER command.
func TestClusterStandaloneFallbackWrappedError(t *testing.T) {
	ctx := context.Background()
	addr := newFallbackTestServer(t, func(args []string) string {
		switch args[0] {
		case "hello":
			return "%0\r\n"
		case "cluster":
			return "-ERR unknown command 'CLUSTER', with args beginning with: '" + args[1] + "'\r\n"
		case "get":
			return "$1\r\na\r\n"
		}
		return "+OK\r\n"
	})
	client := newFallbackTestClient(t, &ClusterOptions{Addrs: []string{addr}, StandaloneFallback: true})
	client.OnNewNode(func(node *Client) { node.AddHook(errWrapHook{}) })

	if val, err := client.Get(ctx, "key").Result(); err != nil || val != "a" {
		t.Fatalf("Get() = %q, %v; want %q, nil", val, err, "a")
	}
	assertStandaloneState(t, client, addr)
}

func TestPickStandaloneAddr(t *testing.T) {
	for _, tc := range []struct {
		seeds, candidates []string
		want              string
	}{
		{[]string{"s1", "s2"}, []string{"n1", "s2", "s1"}, "s1"},
		{[]string{"s1", "s2"}, []string{"n1", "s2"}, "s2"},
		{[]string{"s1"}, []string{"n1", "n2"}, "n1"},
	} {
		if got := pickStandaloneAddr(tc.seeds, tc.candidates); got != tc.want {
			t.Errorf("pickStandaloneAddr(%v, %v) = %s; want %s", tc.seeds, tc.candidates, got, tc.want)
		}
	}
}

// The seed is an endpoint outside the cluster topology (a DNS name or a
// proxy). After a downgrade, a node from the old topology that is now its
// own standalone server must not win over the seed.
func TestClusterStandaloneFallbackPrefersSeed(t *testing.T) {
	ctx := context.Background()

	var seedAddr, oldAddr string
	var downgraded atomic.Bool
	reply := func(name string) func(args []string) string {
		return func(args []string) string {
			switch args[0] {
			case "hello":
				return "%0\r\n"
			case "cluster":
				if downgraded.Load() {
					return clusterDisabledReply
				}
				if args[1] == "slots" {
					return slotsReply(slotRange{0, 16383, oldAddr})
				}
				return "-ERR unknown subcommand\r\n"
			case "get":
				return "$" + strconv.Itoa(len(name)) + "\r\n" + name + "\r\n"
			}
			return "+OK\r\n"
		}
	}
	seedAddr = newFallbackTestServer(t, reply("seed"))
	oldAddr = newFallbackTestServer(t, reply("old"))

	client := newFallbackTestClient(t, &ClusterOptions{Addrs: []string{seedAddr}, StandaloneFallback: true})
	if val, err := client.Get(ctx, "key").Result(); err != nil || val != "old" {
		t.Fatalf("cluster Get() = %q, %v; want %q, nil", val, err, "old")
	}
	state, _ := client.state.Get(ctx)
	if state.standalone {
		t.Fatal("state is standalone; want the cluster topology")
	}
	// Leave only the topology nodes as known addresses, as the delayed GC does.
	client.nodes.GC(state.generation)
	if addrs, _ := client.nodes.Addrs(); slices.Contains(addrs, seedAddr) {
		t.Fatalf("known addrs %v still include the seed", addrs)
	}

	downgraded.Store(true)
	if _, err := client.state.Reload(ctx); err != nil {
		t.Fatalf("Reload() = %v", err)
	}
	assertStandaloneState(t, client, seedAddr)
	if val, err := client.Get(ctx, "key").Result(); err != nil || val != "seed" {
		t.Fatalf("standalone Get() = %q, %v; want %q, nil", val, err, "seed")
	}
}

// Without MOVED, the periodic reload picks up the change.
func TestClusterStandaloneFallbackPeriodicReload(t *testing.T) {
	ctx := context.Background()
	var addr string
	var isCluster atomic.Bool
	addr = newFallbackTestServer(t, func(args []string) string {
		switch args[0] {
		case "hello":
			return "%0\r\n"
		case "cluster":
			if !isCluster.Load() {
				return clusterDisabledReply
			}
			if args[1] == "slots" {
				return slotsReply(slotRange{0, 16383, addr})
			}
			return "-ERR unknown subcommand\r\n"
		case "get":
			return "$1\r\na\r\n"
		}
		return "+OK\r\n"
	})
	client := newFallbackTestClient(t, &ClusterOptions{
		Addrs:                      []string{addr},
		StandaloneFallback:         true,
		ClusterStateReloadInterval: 20 * time.Millisecond,
	})

	if err := client.Get(ctx, "key").Err(); err != nil {
		t.Fatalf("Get() = %v", err)
	}
	assertStandaloneState(t, client, addr)

	isCluster.Store(true)
	waitForState(t, client, func(s *clusterState) bool { return !s.standalone })

	isCluster.Store(false)
	waitForState(t, client, func(s *clusterState) bool { return s.standalone })
}

func newFallbackTestClient(t *testing.T, opt *ClusterOptions) *ClusterClient {
	t.Helper()
	opt.Protocol = 3
	opt.DisableIdentity = true
	opt.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	client := NewClusterClient(opt)
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func assertStandaloneState(t *testing.T, client *ClusterClient, addr string) {
	t.Helper()
	state, err := client.state.Get(context.Background())
	if err != nil {
		t.Fatalf("state.Get() = %v", err)
	}
	if !state.standalone {
		t.Fatalf("state is not standalone")
	}
	if len(state.Masters) != 1 || len(state.Slaves) != 0 {
		t.Fatalf("state has %d masters, %d slaves; want 1, 0", len(state.Masters), len(state.Slaves))
	}
	if got := state.Masters[0].Client.opt.Addr; got != addr {
		t.Fatalf("master addr = %s; want %s", got, addr)
	}
	for _, slot := range []int{0, 8191, 16383} {
		if nodes := state.slotNodes(slot); len(nodes) != 1 || nodes[0] != state.Masters[0] {
			t.Fatalf("slot %d is not owned by the standalone node", slot)
		}
	}
}

// waitForState drives traffic so lazy reloads run, until ok holds.
func waitForState(t *testing.T, client *ClusterClient, ok func(*clusterState) bool) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		_ = client.Get(ctx, "key").Err()
		if state, _ := client.state.Get(ctx); state != nil && ok(state) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("cluster state did not change in time")
}

type slotRange struct {
	start, end int
	addr       string
}

func slotsReply(ranges ...slotRange) string {
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", len(ranges))
	for _, r := range ranges {
		host, port, _ := net.SplitHostPort(r.addr)
		fmt.Fprintf(&b, "*3\r\n:%d\r\n:%d\r\n*2\r\n$%d\r\n%s\r\n:%s\r\n", r.start, r.end, len(host), host, port)
	}
	return b.String()
}

// newFallbackTestServer starts a RESP server on 127.0.0.1. reply receives
// the lowercased command arguments (case-preserved for keys is not needed)
// and returns the raw reply, or closeConnReply to drop the connection.
func newFallbackTestServer(t *testing.T, reply func(args []string) string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var conns []net.Conn
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			mu.Lock()
			conns = append(conns, conn)
			mu.Unlock()
			wg.Add(1)
			go func() { defer wg.Done(); serveFallbackTestConn(conn, reply) }()
		}
	}()
	t.Cleanup(func() {
		_ = ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return ln.Addr().String()
}

func serveFallbackTestConn(conn net.Conn, reply func(args []string) string) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		n, err := strconv.Atoi(strings.TrimPrefix(scanner.Text(), "*"))
		if err != nil || n <= 0 {
			return
		}
		args := make([]string, 0, n+1)
		for i := 0; i < n; i++ {
			if !scanner.Scan() || !scanner.Scan() {
				return
			}
			args = append(args, strings.ToLower(scanner.Text()))
		}
		args = append(args, "") // so reply can index args[1]
		out := reply(args)
		if out == closeConnReply {
			return
		}
		if _, err := conn.Write([]byte(out)); err != nil {
			return
		}
	}
}
