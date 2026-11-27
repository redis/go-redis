package redis

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/maintnotifications"
)

// newSingleNodeRetryClient returns a cluster client whose only node is served
// by srv.
func newSingleNodeRetryClient(t *testing.T, srv string) *ClusterClient {
	t.Helper()
	client := NewClusterClient(&ClusterOptions{
		Protocol:                 3,
		DisableIdentity:          true,
		MaxRedirects:             3,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: "node-1.example:6379"}}}}, nil
		},
		Dialer: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, "127.0.0.1"+srv)
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestClusterPipelineRetryDoesNotReplaySuccessfulCmds tests a batch whose first
// reply is a retryable Redis error while a later command already succeeded.
// Only the command Redis asked to retry may be sent again; replaying the
// successful INCR would apply it twice.
func TestClusterPipelineRetryDoesNotReplaySuccessfulCmds(t *testing.T) {
	for _, retryable := range []string{
		"-TRYAGAIN Multiple keys request during rehashing of slot\r\n",
		"-LOADING Redis is loading the dataset in memory\r\n",
		"-READONLY You can't write against a read only replica.\r\n",
		"-CLUSTERDOWN The cluster is down\r\n",
	} {
		t.Run(retryable[1:9], func(t *testing.T) {
			var mu sync.Mutex
			var sent []string
			var incrs atomic.Int32
			var failed atomic.Bool
			srv := newClusterCSCTestServer(t, func(cmd string) string {
				if cmd == "hello" {
					return "%0\r\n"
				}
				mu.Lock()
				sent = append(sent, cmd)
				mu.Unlock()
				switch cmd {
				case "set":
					if !failed.Swap(true) {
						return retryable
					}
				case "incr":
					return fmt.Sprintf(":%d\r\n", incrs.Add(1))
				}
				return "+OK\r\n"
			})
			client := newSingleNodeRetryClient(t, srv)
			ctx := context.Background()

			var set *StatusCmd
			var incr *IntCmd
			_, err := client.Pipelined(ctx, func(pipe Pipeliner) error {
				set = pipe.Set(ctx, "key", "v", 0)
				incr = pipe.Incr(ctx, "counter")
				return nil
			})
			if err != nil {
				t.Fatalf("exec error: %q; want nil", err)
			}
			if err := set.Err(); err != nil {
				t.Fatalf("set error: %q; want nil", err)
			}
			if val, err := incr.Result(); err != nil || val != 1 {
				t.Fatalf("incr %d, error: %q; want 1, nil", val, err)
			}
			if n := incrs.Load(); n != 1 {
				mu.Lock()
				defer mu.Unlock()
				t.Fatalf("server applied %d INCRs, want 1 (sent %v)", n, sent)
			}
		})
	}
}

// TestClusterPipelineRetryKeepsFinalErrorReplies tests that an error reply
// which is not retryable is final: when another command in the batch is
// retried, the command that failed for good must not be sent again.
func TestClusterPipelineRetryKeepsFinalErrorReplies(t *testing.T) {
	var incrs atomic.Int32
	var failed atomic.Bool
	srv := newClusterCSCTestServer(t, func(cmd string) string {
		switch cmd {
		case "hello":
			return "%0\r\n"
		case "set":
			if !failed.Swap(true) {
				return "-TRYAGAIN Multiple keys request during rehashing of slot\r\n"
			}
		case "incr":
			incrs.Add(1)
			return "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
		}
		return "+OK\r\n"
	})
	client := newSingleNodeRetryClient(t, srv)
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
	if err := incr.Err(); err == nil || !strings.HasPrefix(err.Error(), "WRONGTYPE") {
		t.Fatalf("incr error: %v; want WRONGTYPE", err)
	}
	if n := incrs.Load(); n != 1 {
		t.Fatalf("server received %d INCRs, want 1", n)
	}
}

// TestClusterPipelineReadErrorRetriesOnlyUnreadCmds tests a connection that
// fails while the replies are read. The commands whose replies were already
// read must not be sent again, and the ones that were not read must be.
func TestClusterPipelineReadErrorRetriesOnlyUnreadCmds(t *testing.T) {
	var mu sync.Mutex
	counts := map[string]int{}
	srv := newClusterCSCTestServer(t, func(cmd string) string {
		if cmd == "hello" {
			return "%0\r\n"
		}
		mu.Lock()
		counts[cmd]++
		n := counts[cmd]
		mu.Unlock()
		switch cmd {
		case "incr":
			return fmt.Sprintf(":%d\r\n", n)
		case "del":
			return ":1\r\n"
		case "get":
			if n == 1 {
				// Reply after the client's read timeout expired.
				time.Sleep(3 * time.Second / 5)
			}
			return "$2\r\nok\r\n"
		}
		return "+OK\r\n"
	})
	client := NewClusterClient(&ClusterOptions{
		Protocol:                 3,
		DisableIdentity:          true,
		ReadTimeout:              200 * time.Millisecond,
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: "node-1.example:6379"}}}}, nil
		},
		Dialer: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, network, "127.0.0.1"+srv)
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	ctx := context.Background()

	var set *StatusCmd
	var incr *IntCmd
	var get *StringCmd
	var del *IntCmd
	_, err := client.Pipelined(ctx, func(pipe Pipeliner) error {
		set = pipe.Set(ctx, "key", "v", 0)
		incr = pipe.Incr(ctx, "counter")
		get = pipe.Get(ctx, "key")
		del = pipe.Del(ctx, "other")
		return nil
	})
	if err != nil {
		t.Fatalf("exec error: %q; want nil", err)
	}
	if val, err := get.Result(); err != nil || val != "ok" {
		t.Fatalf("get %q, error: %q; want ok, nil", val, err)
	}
	if val, err := incr.Result(); err != nil || val != 1 {
		t.Fatalf("incr %d, error: %q; want 1, nil", val, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("set error: %q; want nil", err)
	}
	if err := del.Err(); err != nil {
		t.Fatalf("del error: %q; want nil", err)
	}

	mu.Lock()
	defer mu.Unlock()
	for cmd, want := range map[string]int{"set": 1, "incr": 1, "get": 2, "del": 1} {
		if counts[cmd] != want {
			t.Errorf("server received %d %s, want %d", counts[cmd], cmd, want)
		}
	}
}
