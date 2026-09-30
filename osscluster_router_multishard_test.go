package redis

import (
	"context"
	"reflect"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9/internal/routing"
)

// multiShardCaptureHook answers node commands in-process: COMMAND reports
// MSET as a multi_shard command (as Redis does) and every other command is
// recorded and acknowledged, so no server is needed.
type multiShardCaptureHook struct {
	mu   sync.Mutex
	sent [][]interface{}
}

func (h *multiShardCaptureHook) DialHook(next DialHook) DialHook { return next }

func (h *multiShardCaptureHook) ProcessHook(next ProcessHook) ProcessHook {
	return func(ctx context.Context, cmd Cmder) error {
		switch c := cmd.(type) {
		case *CommandsInfoCmd:
			c.SetVal(map[string]*CommandInfo{
				"mset": {
					Name: "mset", Arity: -3, FirstKeyPos: 1, LastKeyPos: -1, StepCount: 2,
					CommandPolicy: &routing.CommandPolicy{
						Request:  routing.ReqMultiShard,
						Response: routing.RespAllSucceeded,
					},
				},
			})
		case *StatusCmd:
			h.mu.Lock()
			h.sent = append(h.sent, c.Args())
			h.mu.Unlock()
			c.SetVal("OK")
		}
		return nil
	}
}

func (h *multiShardCaptureHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

// MSET is routed as multi_shard when the dynamic command-info resolver is
// used. Its values are arbitrary interface{} args (ints, []byte, ...), so the
// multi-shard split must pass them through instead of asserting them to
// string.
func TestClusterMultiShardMSetNonStringValues(t *testing.T) {
	ctx := context.Background()
	const addr = "127.0.0.1:7000"
	hook := &multiShardCaptureHook{}

	client := NewClusterClient(&ClusterOptions{
		Addrs: []string{addr},
		ClusterSlots: func(context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{{Start: 0, End: 16383, Nodes: []ClusterNode{{Addr: addr}}}}, nil
		},
		NewClient: func(opt *Options) *Client {
			c := NewClient(opt)
			c.AddHook(hook)
			return c
		},
	})
	defer client.Close()
	client.SetCommandInfoResolver(client.NewDynamicResolver())

	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("MSet panicked: %v", r)
			}
		}()
		err = client.MSet(ctx, "{t}a", 1, "{t}b", []byte("x"), "{t}c", "s").Err()
	}()
	if err != nil {
		t.Fatalf("MSet returned error: %v", err)
	}

	hook.mu.Lock()
	defer hook.mu.Unlock()
	want := []interface{}{"mset", "{t}a", 1, "{t}b", []byte("x"), "{t}c", "s"}
	if len(hook.sent) != 1 || !reflect.DeepEqual(hook.sent[0], want) {
		t.Fatalf("sent %v, want [%v]", hook.sent, want)
	}
	hook.sent = nil
	hook.mu.Unlock()

	// Keys in different slots are split per slot, each keeping its value.
	if err := client.MSet(ctx, "a", 1, "b", int64(2)).Err(); err != nil {
		t.Fatalf("MSet returned error: %v", err)
	}
	hook.mu.Lock()
	got := map[string]interface{}{}
	for _, args := range hook.sent {
		if len(args) != 3 {
			t.Fatalf("unexpected sub-command %v", args)
		}
		got[args[1].(string)] = args[2]
	}
	if !reflect.DeepEqual(got, map[string]interface{}{"a": 1, "b": int64(2)}) {
		t.Fatalf("sent %v", hook.sent)
	}
}
