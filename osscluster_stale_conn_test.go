package redis

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestClusterMovedToSameAddrRecyclesConn tests a case when a connection within the same hostname was replaced
// but still points to the old node. Such connection must be released and reopened
func TestClusterMovedToSameAddrRecyclesConn(t *testing.T) {
	const addr = "node-1.example:6379"
	for _, mode := range []string{"single", "pipeline", "txpipeline"} {
		t.Run(mode, func(t *testing.T) {
			var oldGets, newGets atomic.Int32
			oldSrv := newMovedTestServer(t, addr, &oldGets, nil)
			newSrv := newOKTestServer(t, &newGets)

			client, switchTo := newSwitchedClusterClient(t, addr, oldSrv, 3)
			ctx := context.Background()

			switch mode {
			case "pipeline":
				_, _ = client.Pipelined(ctx, func(pipe Pipeliner) error { pipe.Ping(ctx); return nil })
			case "txpipeline":
				_, _ = client.TxPipelined(ctx, func(pipe Pipeliner) error { pipe.Ping(ctx); return nil })
			default:
				_ = client.Ping(ctx).Err()
			}
			switchTo(newSrv)

			var get *StringCmd
			switch mode {
			case "pipeline":
				_, _ = client.Pipelined(ctx, func(pipe Pipeliner) error { get = pipe.Get(ctx, "key"); return nil })
			case "txpipeline":
				_, _ = client.TxPipelined(ctx, func(pipe Pipeliner) error { get = pipe.Get(ctx, "key"); return nil })
			default:
				get = client.Get(ctx, "key")
			}

			if val, err := get.Result(); err != nil || val != "ok" {
				t.Fatalf("get %s, error: %q; want %s, nil", val, err, "ok")
			}
			if n := oldGets.Load(); n != 1 {
				t.Fatalf("old server served %d GETs, want 1 (the stale connection was reused)", n)
			}
			if n := newGets.Load(); n != 1 {
				t.Fatalf("new server served %d GETs, want 1", n)
			}
		})
	}
}

// TestClusterMovedToOtherAddrKeepsConn tests a case when a node redirects to a
// different node whose address ends with its own (redis:6379 -> new-redis:6379).
// Such connection is still healthy and must not be released.
func TestClusterMovedToOtherAddrKeepsConn(t *testing.T) {
	const srcAddr, dstAddr = "redis:6379", "new-redis:6379"
	for _, mode := range []string{"pipeline", "txpipeline"} {
		t.Run(mode, func(t *testing.T) {
			var srcGets, dstGets atomic.Int32
			srcSrv := newMovedTestServer(t, dstAddr, &srcGets, nil)
			dstSrv := newOKTestServer(t, &dstGets)

			var srcDials atomic.Int32
			client := NewClusterClient(&ClusterOptions{
				Protocol:                 3,
				DisableIdentity:          true,
				PoolSize:                 1,
				PipelinePoolSize:         1,
				MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
					return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: srcAddr}}}}, nil
				},
				Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
					target := dstSrv
					if addr == srcAddr {
						srcDials.Add(1)
						target = srcSrv
					}
					var d net.Dialer
					return d.DialContext(ctx, network, "127.0.0.1"+target)
				},
			})
			t.Cleanup(func() { _ = client.Close() })
			ctx := context.Background()

			run := client.Pipelined
			if mode == "txpipeline" {
				run = client.TxPipelined
			}
			_, _ = run(ctx, func(pipe Pipeliner) error { pipe.Ping(ctx); return nil })
			warm := srcDials.Load()

			var get *StringCmd
			_, _ = run(ctx, func(pipe Pipeliner) error { get = pipe.Get(ctx, "key"); return nil })
			if val, err := get.Result(); err != nil || val != "ok" {
				t.Fatalf("get %s, error: %q; want %s, nil", val, err, "ok")
			}
			if n := dstGets.Load(); n != 1 {
				t.Fatalf("destination served %d GETs, want 1 (the redirect was not followed)", n)
			}

			_, _ = run(ctx, func(pipe Pipeliner) error { pipe.Ping(ctx); return nil })
			if n := srcDials.Load() - warm; n != 0 {
				t.Fatalf("redialed the source %d times, want 0 (its connection was dropped)", n)
			}
		})
	}
}

// TestClusterMovedToSameAddrMixedBatch tests batches where MOVED to the
// connection own address comes with another error for another command causing a connection to be released and retried
func TestClusterMovedToSameAddrMixedBatch(t *testing.T) {
	const addr = "node-1.example:6379"
	tests := []struct {
		name         string
		replies      map[string]string // stale server replies besides the MOVED to GET
		himport      bool
		otherFirst   bool
		tx           bool
		maxRedirects int
		cmd          func(ctx context.Context, pipe Pipeliner) Cmder
	}{
		{
			// retry first req
			name:         "retryable first reply",
			replies:      map[string]string{"set": "-TRYAGAIN Multiple keys request during rehashing of slot\r\n"},
			otherFirst:   true,
			maxRedirects: 1,
			cmd:          func(ctx context.Context, pipe Pipeliner) Cmder { return pipe.Set(ctx, "key", "v", 0) },
		},
		{
			// TRYAGAIN goes first
			name:         "tx retryable first reply",
			replies:      map[string]string{"set": "-TRYAGAIN Multiple keys request during rehashing of slot\r\n"},
			otherFirst:   true,
			tx:           true,
			maxRedirects: 1,
			cmd:          func(ctx context.Context, pipe Pipeliner) Cmder { return pipe.Set(ctx, "key", "v", 0) },
		},
		{
			// HIMPORT
			name:         "himport session lost",
			replies:      map[string]string{"himport": "-ERR no such fieldset\r\n"},
			himport:      true,
			maxRedirects: 3,
			cmd:          func(ctx context.Context, pipe Pipeliner) Cmder { return pipe.HImportSet(ctx, "key", "fs", "v") },
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var staleGets, freshGets atomic.Int32
			var replaced atomic.Bool
			staleSrv := newMovedTestServer(t, addr, &staleGets, func(cmd string) (string, bool) {
				reply, ok := tc.replies[cmd]
				return reply, ok && replaced.Load()
			})
			freshSrv := newOKTestServer(t, &freshGets)

			client, switchTo := newSwitchedClusterClient(t, addr, staleSrv, tc.maxRedirects)
			ctx := context.Background()

			if tc.himport {
				if err := client.HImportPrepare(ctx, "fs", "f").Err(); err != nil {
					t.Fatalf("HImportPrepare: %v", err)
				}
			}
			_, _ = client.Pipelined(ctx, func(pipe Pipeliner) error { pipe.Ping(ctx); return nil })
			replaced.Store(true)
			switchTo(freshSrv)

			run := client.Pipelined
			if tc.tx {
				run = client.TxPipelined
			}
			var other Cmder
			var get *StringCmd
			_, _ = run(ctx, func(pipe Pipeliner) error {
				if tc.otherFirst {
					other = tc.cmd(ctx, pipe)
				}
				get = pipe.Get(ctx, "key")
				if !tc.otherFirst {
					other = tc.cmd(ctx, pipe)
				}
				return nil
			})

			if err := other.Err(); err != nil {
				t.Fatalf("%s error: %q; want nil", other.Name(), err)
			}
			if val, err := get.Result(); err != nil || val != "ok" {
				t.Fatalf("get %s, error: %q; want %s, nil", val, err, "ok")
			}
			if n := staleGets.Load(); n != 1 {
				t.Fatalf("stale server served %d GETs, want 1 (the stale connection was reused)", n)
			}
		})
	}
}

// TestClusterPipelineRetryDoesNotDuplicateRedirect tests a batch whose first
// reply is retryable while a later command gets MOVED. The redirected command
// must be sent to its new node only once, in the same retry round.
func TestClusterPipelineRetryDoesNotDuplicateRedirect(t *testing.T) {
	const srcAddr, dstAddr = "node-a:6379", "node-b:6379"
	var tryAgain atomic.Bool
	tryAgain.Store(true)
	srcSrv := newClusterCSCTestServer(t, func(cmd string) string {
		switch cmd {
		case "hello":
			return "%0\r\n"
		case "set":
			if tryAgain.Swap(false) {
				return "-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
			}
		case "incr":
			return "-MOVED 0 " + dstAddr + "\r\n"
		}
		return "+OK\r\n"
	})
	var incrs atomic.Int32
	dstSrv := newClusterCSCTestServer(t, func(cmd string) string {
		switch cmd {
		case "hello":
			return "%0\r\n"
		case "incr":
			return fmt.Sprintf(":%d\r\n", incrs.Add(1))
		}
		return "+OK\r\n"
	})

	client := NewClusterClient(&ClusterOptions{
		Protocol:                 3,
		DisableIdentity:          true,
		MaxRedirects:             1,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: srcAddr}}}}, nil
		},
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			target := srcSrv
			if addr == dstAddr {
				target = dstSrv
			}
			var d net.Dialer
			return d.DialContext(ctx, network, "127.0.0.1"+target)
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	// A Cmder value with a slice is not comparable and must not be hashed.
	type taggedCmd struct {
		*IntCmd
		tags []string
	}
	var set *StatusCmd
	incr := taggedCmd{IntCmd: NewIntCmd(ctx, "incr", "counter"), tags: []string{"t"}}
	_, _ = client.Pipelined(ctx, func(pipe Pipeliner) error {
		set = pipe.Set(ctx, "key", "v", 0)
		return pipe.Process(ctx, incr)
	})

	if err := set.Err(); err != nil {
		t.Fatalf("set error: %q; want nil", err)
	}
	if val, err := incr.Result(); err != nil || val != 1 {
		t.Fatalf("incr %d, error: %q; want 1, nil", val, err)
	}
	if n := incrs.Load(); n != 1 {
		t.Fatalf("destination served %d INCRs, want 1", n)
	}
}

func TestClusterPipelineRetryDoesNotReplaySuccess(t *testing.T) {
	const addr = "node:6379"

	t.Run("tryagain", func(t *testing.T) {
		var sets, incrs atomic.Int32
		srv := newClusterCSCTestServer(t, func(cmd string) string {
			switch cmd {
			case "hello":
				return "%0\r\n"
			case "set":
				if sets.Add(1) == 1 {
					return "-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
				}
				return "+OK\r\n"
			case "incr":
				return fmt.Sprintf(":%d\r\n", incrs.Add(1))
			default:
				return "+OK\r\n"
			}
		})
		client := newPipelineReplayClusterClient(t, addr, srv, 1)
		ctx := context.Background()
		var set *StatusCmd
		var incr *IntCmd
		_, _ = client.Pipelined(ctx, func(pipe Pipeliner) error {
			set = pipe.Set(ctx, "key", "v", 0)
			incr = pipe.Incr(ctx, "counter")
			return nil
		})
		if err := set.Err(); err != nil {
			t.Fatalf("set error: %q; want nil", err)
		}
		if val, err := incr.Result(); err != nil || val != 1 {
			t.Fatalf("incr %d, error: %q; want 1, nil", val, err)
		}
		if n := incrs.Load(); n != 1 {
			t.Fatalf("server served %d INCRs, want 1", n)
		}
	})

	t.Run("read error", func(t *testing.T) {
		var incrs atomic.Int32
		srv := newCloseAfterIncrServer(t, &incrs)
		client := newPipelineReplayClusterClient(t, addr, srv, 1)
		ctx := context.Background()
		var incr *IntCmd
		var get *StringCmd
		_, _ = client.Pipelined(ctx, func(pipe Pipeliner) error {
			incr = pipe.Incr(ctx, "counter")
			get = pipe.Get(ctx, "key")
			return nil
		})
		if val, err := incr.Result(); err != nil || val != 1 {
			t.Fatalf("incr %d, error: %q; want 1, nil", val, err)
		}
		if err := get.Err(); err == nil {
			t.Fatal("get error: nil; want a read error")
		}
		if n := incrs.Load(); n != 1 {
			t.Fatalf("server served %d INCRs, want 1", n)
		}
	})
}

func newPipelineReplayClusterClient(t *testing.T, addr, srv string, maxRedirects int) *ClusterClient {
	t.Helper()
	client := NewClusterClient(&ClusterOptions{
		Protocol:                 3,
		DisableIdentity:          true,
		MaxRedirects:             maxRedirects,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}, nil
		},
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, "127.0.0.1"+srv)
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

func newCloseAfterIncrServer(t *testing.T, incrs *atomic.Int32) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveCloseAfterIncr(conn, incrs)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return ":" + port
}

func serveCloseAfterIncr(netConn net.Conn, incrs *atomic.Int32) {
	defer netConn.Close()
	scanner := bufio.NewScanner(netConn)
	for scanner.Scan() {
		header := scanner.Text()
		if !strings.HasPrefix(header, "*") {
			return
		}
		n, err := strconv.Atoi(strings.TrimPrefix(header, "*"))
		if err != nil || n <= 0 {
			return
		}
		command := ""
		for i := 0; i < n; i++ {
			if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "$") || !scanner.Scan() {
				return
			}
			if i == 0 {
				command = strings.ToLower(scanner.Text())
			}
		}
		reply := "+OK\r\n"
		closeAfter := false
		switch command {
		case "hello":
			reply = "%0\r\n"
		case "incr":
			reply = fmt.Sprintf(":%d\r\n", incrs.Add(1))
			closeAfter = true
		}
		if _, err := netConn.Write([]byte(reply)); err != nil || closeAfter {
			return
		}
	}
}

// TestClusterPipelineRetryKeepsHImportRecovery tests a batch whose first
// reply is TRYAGAIN while a later HIMPORT SET of a registered fieldset
// fails with "no such fieldset". The SET is retried, and the HIMPORT SET
// is re-queued beside it so the session-loss recovery still runs.
func TestClusterPipelineRetryKeepsHImportRecovery(t *testing.T) {
	const addr = "node-1.example:6379"
	var sets atomic.Int32
	var himportSets atomic.Int32
	var mu sync.Mutex
	var seen []string
	ln := newArgReplyServer(t, func(args []string) string {
		mu.Lock()
		seen = append(seen, strings.Join(args, " "))
		mu.Unlock()
		if len(args) == 0 {
			return "+OK\r\n"
		}
		switch strings.ToLower(args[0]) {
		case "hello":
			return "%0\r\n"
		case "set":
			if sets.Add(1) == 1 {
				return "-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
			}
			return "+OK\r\n"
		case "himport":
			sub := ""
			if len(args) > 1 {
				sub = strings.ToLower(args[1])
			}
			switch sub {
			case "set":
				if himportSets.Add(1) == 1 {
					return "-ERR no such fieldset\r\n"
				}
				return "+OK\r\n"
			case "discard", "discardall":
				return ":1\r\n"
			default:
				return "+OK\r\n"
			}
		default:
			return "+OK\r\n"
		}
	})
	client := newPipelineReplayClusterClient(t, addr, ln, 1)
	ctx := context.Background()
	if err := client.HImportPrepare(ctx, "fs", "f").Err(); err != nil {
		t.Fatalf("HImportPrepare: %v", err)
	}
	var set *StatusCmd
	var himport *StatusCmd
	_, err := client.Pipelined(ctx, func(pipe Pipeliner) error {
		set = pipe.Set(ctx, "key", "v", 0)
		himport = pipe.HImportSet(ctx, "hash", "fs", "value")
		return nil
	})
	if err != nil || set.Err() != nil || himport.Err() != nil {
		mu.Lock()
		log := strings.Join(seen, "\n")
		mu.Unlock()
		t.Fatalf("exec=%v set=%v himport=%v sets=%d himportSets=%d\n%s",
			err, set.Err(), himport.Err(), sets.Load(), himportSets.Load(), log)
	}
	if sets.Load() != 2 || himportSets.Load() != 2 {
		t.Fatalf("sets=%d himportSets=%d, want 2 and 2", sets.Load(), himportSets.Load())
	}
}

func newArgReplyServer(t *testing.T, reply func(args []string) string) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveArgReply(conn, reply)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	_, port, _ := net.SplitHostPort(ln.Addr().String())
	return ":" + port
}

func serveArgReply(netConn net.Conn, reply func(args []string) string) {
	defer netConn.Close()
	scanner := bufio.NewScanner(netConn)
	for scanner.Scan() {
		header := scanner.Text()
		if !strings.HasPrefix(header, "*") {
			return
		}
		n, err := strconv.Atoi(strings.TrimPrefix(header, "*"))
		if err != nil || n <= 0 {
			return
		}
		args := make([]string, 0, n)
		for i := 0; i < n; i++ {
			if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "$") || !scanner.Scan() {
				return
			}
			args = append(args, scanner.Text())
		}
		if _, err := netConn.Write([]byte(reply(args))); err != nil {
			return
		}
	}
}

// TestClusterPipelineRetryKeepsHImportOrder tests a retryable command,
// a registered HIMPORT SET that lost its fieldset, and a later retryable
// DEL of that key. The retry must keep replace-then-delete order.
func TestClusterPipelineRetryKeepsHImportOrder(t *testing.T) {
	const addr = "node-1.example:6379"
	var sets atomic.Int32
	var dels atomic.Int32
	var himportSets atomic.Int32
	var mu sync.Mutex
	var seen []string
	ln := newArgReplyServer(t, func(args []string) string {
		mu.Lock()
		seen = append(seen, strings.Join(args, " "))
		mu.Unlock()
		if len(args) == 0 {
			return "+OK\r\n"
		}
		switch strings.ToLower(args[0]) {
		case "hello":
			return "%0\r\n"
		case "set":
			if sets.Add(1) == 1 {
				return "-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
			}
			return "+OK\r\n"
		case "del":
			if dels.Add(1) == 1 {
				return "-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
			}
			return ":1\r\n"
		case "himport":
			sub := ""
			if len(args) > 1 {
				sub = strings.ToLower(args[1])
			}
			switch sub {
			case "set":
				if himportSets.Add(1) == 1 {
					return "-ERR no such fieldset\r\n"
				}
				return "+OK\r\n"
			case "discard", "discardall":
				return ":1\r\n"
			default:
				return "+OK\r\n"
			}
		default:
			return "+OK\r\n"
		}
	})
	client := newPipelineReplayClusterClient(t, addr, ln, 1)
	ctx := context.Background()
	if err := client.HImportPrepare(ctx, "fs", "f").Err(); err != nil {
		t.Fatalf("HImportPrepare: %v", err)
	}
	var set *StatusCmd
	var himport *StatusCmd
	var del *IntCmd
	_, err := client.Pipelined(ctx, func(pipe Pipeliner) error {
		set = pipe.Set(ctx, "hash", "v", 0)
		himport = pipe.HImportSet(ctx, "hash", "fs", "value")
		del = pipe.Del(ctx, "hash")
		return nil
	})
	mu.Lock()
	log := strings.Join(seen, "\n")
	mu.Unlock()
	if err != nil || set.Err() != nil || himport.Err() != nil || del.Err() != nil {
		t.Fatalf("exec=%v set=%v himport=%v del=%v\n%s", err, set.Err(), himport.Err(), del.Err(), log)
	}
	var user []string
	for _, line := range seen {
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch strings.ToLower(fields[0]) {
		case "set", "del":
			user = append(user, strings.ToLower(fields[0]))
		case "himport":
			if len(fields) > 1 && strings.EqualFold(fields[1], "set") {
				user = append(user, "himport")
			}
		}
	}
	got := strings.Join(user, " ")
	const want = "set himport del set himport del"
	if got != want {
		t.Fatalf("order %q, want %q\n%s", got, want, log)
	}
}

// TestClusterPipelineRetryIncludesHImportPrepareError tests an injected
// PREPARE that fails with LOADING while its HIMPORT SET replies
// "no such fieldset". The SET has to be retried with the PREPARE error.
func TestClusterPipelineRetryIncludesHImportPrepareError(t *testing.T) {
	const addr = "node-1.example:6379"
	var sets atomic.Int32
	var himportSets atomic.Int32
	var prepares atomic.Int32
	var mu sync.Mutex
	var seen []string
	ln := newArgReplyServer(t, func(args []string) string {
		mu.Lock()
		seen = append(seen, strings.Join(args, " "))
		mu.Unlock()
		if len(args) == 0 {
			return "+OK\r\n"
		}
		switch strings.ToLower(args[0]) {
		case "hello":
			return "%0\r\n"
		case "set":
			if sets.Add(1) == 1 {
				return "-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
			}
			return "+OK\r\n"
		case "himport":
			sub := ""
			if len(args) > 1 {
				sub = strings.ToLower(args[1])
			}
			switch sub {
			case "set":
				if himportSets.Add(1) < 3 {
					return "-ERR no such fieldset\r\n"
				}
				return "+OK\r\n"
			case "prepare":
				if prepares.Add(1) == 2 {
					return "-LOADING Redis is loading the dataset in memory\r\n"
				}
				return "+OK\r\n"
			case "discard", "discardall":
				return ":1\r\n"
			default:
				return "+OK\r\n"
			}
		default:
			return "+OK\r\n"
		}
	})
	client := newPipelineReplayClusterClient(t, addr, ln, 2)
	ctx := context.Background()
	if err := client.HImportPrepare(ctx, "fs", "f").Err(); err != nil {
		t.Fatalf("HImportPrepare: %v", err)
	}
	var set *StatusCmd
	var himport *StatusCmd
	_, err := client.Pipelined(ctx, func(pipe Pipeliner) error {
		set = pipe.Set(ctx, "key", "v", 0)
		himport = pipe.HImportSet(ctx, "hash", "fs", "value")
		return nil
	})
	if err != nil || set.Err() != nil || himport.Err() != nil || sets.Load() != 2 || himportSets.Load() != 3 {
		mu.Lock()
		log := strings.Join(seen, "\n")
		mu.Unlock()
		t.Fatalf("exec=%v set=%v himport=%v sets=%d himportSets=%d\n%s",
			err, set.Err(), himport.Err(), sets.Load(), himportSets.Load(), log)
	}
}

// TestClusterPipelineRetryKeepsOrder tests a batch whose first reply is
// retryable while a later command gets MOVED to the same node. The retry
// must keep the original command order.
func TestClusterPipelineRetryKeepsOrder(t *testing.T) {
	const addr = "node-1.example:6379"
	var staleGets atomic.Int32
	var replaced atomic.Bool
	staleSrv := newMovedTestServer(t, addr, &staleGets, func(cmd string) (string, bool) {
		return "-TRYAGAIN Multiple keys request during rehashing of slot\r\n", cmd == "set" && replaced.Load()
	})
	var mu sync.Mutex
	var sent []string
	freshSrv := newClusterCSCTestServer(t, func(cmd string) string {
		if cmd == "hello" {
			return "%0\r\n"
		}
		mu.Lock()
		sent = append(sent, cmd)
		mu.Unlock()
		if cmd == "get" {
			return "$2\r\nok\r\n"
		}
		return "+OK\r\n"
	})

	client, switchTo := newSwitchedClusterClient(t, addr, staleSrv, 1)
	ctx := context.Background()
	_, _ = client.Pipelined(ctx, func(pipe Pipeliner) error { pipe.Ping(ctx); return nil })
	replaced.Store(true)
	switchTo(freshSrv)

	_, _ = client.Pipelined(ctx, func(pipe Pipeliner) error {
		pipe.Set(ctx, "key", "v", 0)
		pipe.Get(ctx, "key")
		return nil
	})

	mu.Lock()
	defer mu.Unlock()
	if got := strings.Join(sent, " "); got != "set get" {
		t.Fatalf("fresh server got %q, want %q", got, "set get")
	}
}

// newSwitchedClusterClient returns a client and func for host reslver.
func newSwitchedClusterClient(t *testing.T, addr, srv string, maxRedirects int) (*ClusterClient, func(string)) {
	var target atomic.Value
	target.Store(srv)
	client := NewClusterClient(&ClusterOptions{
		Protocol:                 3,
		DisableIdentity:          true,
		PoolSize:                 1,
		PipelinePoolSize:         1,
		MaxRedirects:             maxRedirects,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}, nil
		},
		Dialer: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, "127.0.0.1"+target.Load().(string))
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client, func(srv string) { target.Store(srv) }
}

// newMovedTestServer always answers with MOVED
func newMovedTestServer(t *testing.T, target string, gets *atomic.Int32, override func(cmd string) (string, bool)) string {
	var dirty atomic.Bool
	return newClusterCSCTestServer(t, func(cmd string) string {
		if override != nil {
			if reply, ok := override(cmd); ok {
				return reply
			}
		}
		switch cmd {
		case "hello":
			return "%0\r\n"
		case "get":
			gets.Add(1)
			dirty.Store(true)
			return "-MOVED 0 " + target + "\r\n"
		case "exec":
			if dirty.Swap(false) {
				return "-EXECABORT Transaction discarded because of previous errors.\r\n"
			}
			return "*1\r\n+PONG\r\n"
		case "ping":
			return "+PONG\r\n"
		}
		return "+OK\r\n"
	})
}

func newOKTestServer(t *testing.T, gets *atomic.Int32) string {
	var queued []string // replies of the commands queued since MULTI; nil outside a transaction
	var mu sync.Mutex
	return newClusterCSCTestServer(t, func(cmd string) string {
		reply := "+OK\r\n"
		switch cmd {
		case "hello":
			return "%0\r\n"
		case "multi":
			mu.Lock()
			queued = []string{}
			mu.Unlock()
			return "+OK\r\n"
		case "exec":
			mu.Lock()
			defer mu.Unlock()
			replies := fmt.Sprintf("*%d\r\n%s", len(queued), strings.Join(queued, ""))
			queued = nil
			return replies
		case "get":
			gets.Add(1)
			reply = "$2\r\nok\r\n"
		}
		mu.Lock()
		defer mu.Unlock()
		if queued != nil {
			queued = append(queued, reply)
			return "+QUEUED\r\n"
		}
		return reply
	})
}
