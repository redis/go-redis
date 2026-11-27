package redis

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"

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
