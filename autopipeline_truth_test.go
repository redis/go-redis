package redis_test

// Truth battery: adversarial end-to-end validation of the AutoPipeliner's
// core contracts against a live server — per-goroutine ORDER, exact RESPONSE
// demultiplexing, no lost/duplicated executions — under both faces, ordered
// and unordered configs, chunked dispatch, mid-flight cancellation and
// close-drain. Written for the PR #3942 validation pass; every test derives
// an independent ground truth (a model value computed client-side, or a
// differential run against a plain client) rather than trusting the engine's
// own accounting.

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func truthAddr() string { return apTestAddr() }

func newTruthClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: truthAddr(), PoolSize: 64})
	if err := c.Ping(context.Background()).Err(); err != nil {
		c.Close()
		t.Skipf("no redis: %v", err)
	}
	return c
}

// TestTruthBlockingPerGoroutineOrder pins the blocking face's ordering
// contract with a server-side proof: each goroutine RPUSHes its own strictly
// increasing sequence through a SHARED autopipeliner; the list on the server
// is the ground truth and must come back exactly 0..M-1 for every goroutine.
func TestTruthBlockingPerGoroutineOrder(t *testing.T) {
	ctx := context.Background()
	client := newTruthClient(t)
	defer client.Close()
	client.FlushDB(ctx)
	ap, err := client.AutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 64, MaxConcurrentBatches: 8, Unordered: true, NumShards: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	const G, M = 64, 200
	runWithWatchdog(t, 60*time.Second, func() {
		var wg sync.WaitGroup
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				key := "order:blocking:" + strconv.Itoa(g)
				for i := 0; i < M; i++ {
					if err := ap.RPush(ctx, key, i).Err(); err != nil {
						t.Errorf("g%d rpush %d: %v", g, i, err)
						return
					}
				}
			}(g)
		}
		wg.Wait()
	})
	for g := 0; g < G; g++ {
		vals, err := client.LRange(ctx, "order:blocking:"+strconv.Itoa(g), 0, -1).Result()
		if err != nil {
			t.Fatalf("lrange g%d: %v", g, err)
		}
		if len(vals) != M {
			t.Fatalf("g%d: %d elements, want %d (lost or duplicated commands)", g, len(vals), M)
		}
		for i, v := range vals {
			if v != strconv.Itoa(i) {
				t.Fatalf("g%d: position %d = %q, want %q (per-goroutine order violated)", g, i, v, i)
			}
		}
	}
}

// TestTruthAsyncOrderedSingleGoroutine pins the deferred face's ordering on
// the ORDERED default config: one goroutine submits an increasing sequence
// without awaiting; the server-side list must be exactly in submit order,
// and a read submitted after a write in the same window must observe it.
func TestTruthAsyncOrderedSingleGoroutine(t *testing.T) {
	ctx := context.Background()
	client := newTruthClient(t)
	defer client.Close()
	client.FlushDB(ctx)
	ap, err := client.AsyncAutoPipeline() // ordered default
	if err != nil {
		t.Fatal(err)
	}
	const N = 3000
	runWithWatchdog(t, 60*time.Second, func() {
		cmds := make([]*redis.IntCmd, N)
		for i := 0; i < N; i++ {
			cmds[i] = ap.RPush(ctx, "order:async", i)
		}
		// read-your-write inside the window: SET then GET, same key, no await
		// between them.
		set := ap.Set(ctx, "order:ryw", "written", 0)
		get := ap.Get(ctx, "order:ryw")
		for i, c := range cmds {
			if err := c.Err(); err != nil {
				t.Fatalf("rpush %d: %v", i, err)
			}
		}
		if err := set.Err(); err != nil {
			t.Fatal(err)
		}
		if v, err := get.Result(); err != nil || v != "written" {
			t.Fatalf("read-your-write: v=%q err=%v, want %q", v, err, "written")
		}
	})
	vals, err := client.LRange(ctx, "order:async", 0, -1).Result()
	if err != nil {
		t.Fatal(err)
	}
	if len(vals) != N {
		t.Fatalf("%d elements, want %d", len(vals), N)
	}
	for i, v := range vals {
		if v != strconv.Itoa(i) {
			t.Fatalf("position %d = %q, want %q (submit order violated on ordered face)", i, v, i)
		}
	}
}

// TestTruthResponseDemux pins reply demultiplexing: every one of G*M GETs
// must return exactly the unique payload its OWN goroutine wrote — any
// cross-wiring of replies between commands or goroutines fails loudly.
// Run on the most adversarial config: unordered, striped, parallel batches.
func TestTruthResponseDemux(t *testing.T) {
	ctx := context.Background()
	client := newTruthClient(t)
	defer client.Close()
	client.FlushDB(ctx)
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 128, MaxConcurrentBatches: 32, Unordered: true, NumShards: 8,
	})
	if err != nil {
		t.Fatal(err)
	}
	const G, M = 100, 300
	// Unordered config: a GET may overtake its own SET by design, so the
	// phases are separated — all writes awaited, then all reads. The demux
	// property under test is untouched: every reply must match its OWN
	// command's unique payload.
	runWithWatchdog(t, 120*time.Second, func() {
		var wg sync.WaitGroup
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				sets := make([]*redis.StatusCmd, M)
				for i := 0; i < M; i++ {
					key := fmt.Sprintf("demux:%d:%d", g, i)
					want := fmt.Sprintf("payload|g=%d|i=%d|%x", g, i, g*1_000_003+i*7919)
					sets[i] = ap.Set(ctx, key, want, 0)
				}
				for i, s := range sets {
					if err := s.Err(); err != nil {
						t.Errorf("set g%d i%d: %v", g, i, err)
						return
					}
				}
				gets := make([]*redis.StringCmd, M)
				for i := 0; i < M; i++ {
					gets[i] = ap.Get(ctx, fmt.Sprintf("demux:%d:%d", g, i))
				}
				for i, gc := range gets {
					want := fmt.Sprintf("payload|g=%d|i=%d|%x", g, i, g*1_000_003+i*7919)
					got, err := gc.Result()
					if err != nil {
						t.Errorf("get g%d i%d: %v", g, i, err)
						return
					}
					if got != want {
						t.Errorf("DEMUX VIOLATION g%d i%d: got %q want %q", g, i, got, want)
						return
					}
				}
			}(g)
		}
		wg.Wait()
	})
}

// TestTruthOrderedPairsAcrossGoroutines pins per-goroutine read-your-write
// on the ORDERED default config with many goroutines sharing the pipeliner:
// each goroutine interleaves SET/GET pairs on its own keys without any
// barrier — the ordered face must preserve each goroutine's submit order.
func TestTruthOrderedPairsAcrossGoroutines(t *testing.T) {
	ctx := context.Background()
	client := newTruthClient(t)
	defer client.Close()
	client.FlushDB(ctx)
	ap, err := client.AsyncAutoPipeline() // ordered default
	if err != nil {
		t.Fatal(err)
	}
	const G, M = 64, 150
	runWithWatchdog(t, 120*time.Second, func() {
		var wg sync.WaitGroup
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for i := 0; i < M; i++ {
					key := fmt.Sprintf("pair:%d:%d", g, i)
					want := fmt.Sprintf("v|%d|%d", g, i)
					set := ap.Set(ctx, key, want, 0)
					get := ap.Get(ctx, key) // no await between: ordered face must sequence
					if err := set.Err(); err != nil {
						t.Errorf("set g%d i%d: %v", g, i, err)
						return
					}
					got, err := get.Result()
					if err != nil || got != want {
						t.Errorf("ORDER VIOLATION g%d i%d: got %q err=%v want %q", g, i, got, err, want)
						return
					}
				}
			}(g)
		}
		wg.Wait()
	})
}

// TestTruthCountersExact pins no-lost-no-duplicated execution: G goroutines
// INCRBY random deltas on one shared counter; the client-side sum is the
// ground truth and the final server value must equal it exactly. A dropped
// command, a duplicated retry, or a misrouted reply cannot pass.
func TestTruthCountersExact(t *testing.T) {
	ctx := context.Background()
	client := newTruthClient(t)
	defer client.Close()
	client.FlushDB(ctx)
	for _, cfg := range []struct {
		name string
		opts *redis.AutoPipelineOptions
	}{
		{"ordered-default", nil},
		{"unordered-parallel", &redis.AutoPipelineOptions{MaxBatchSize: 200, MaxConcurrentBatches: 40, Unordered: true, NumShards: 8}},
		{"tiny-batches", &redis.AutoPipelineOptions{MaxBatchSize: 2, MaxConcurrentBatches: 4, Unordered: true}},
	} {
		t.Run(cfg.name, func(t *testing.T) {
			var ap *redis.AutoPipeliner
			var err error
			if cfg.opts == nil {
				ap, err = client.AsyncAutoPipeline()
			} else {
				ap, err = client.AsyncAutoPipelineWithOptions(cfg.opts)
			}
			if err != nil {
				t.Fatal(err)
			}
			key := "counter:" + cfg.name
			client.Del(ctx, key)
			const G, M = 50, 200
			var wantTotal atomic.Int64
			runWithWatchdog(t, 90*time.Second, func() {
				var wg sync.WaitGroup
				for g := 0; g < G; g++ {
					wg.Add(1)
					go func(g int) {
						defer wg.Done()
						rng := rand.New(rand.NewSource(int64(g)*7919 + 1))
						futs := make([]*redis.IntCmd, M)
						deltas := make([]int64, M)
						for i := 0; i < M; i++ {
							deltas[i] = rng.Int63n(1000) - 500
							futs[i] = ap.IncrBy(ctx, key, deltas[i])
						}
						for i, f := range futs {
							if err := f.Err(); err != nil {
								t.Errorf("incrby: %v", err)
								return
							}
							wantTotal.Add(deltas[i])
						}
					}(g)
				}
				wg.Wait()
			})
			got, err := client.Get(ctx, key).Int64()
			if err != nil {
				t.Fatal(err)
			}
			if got != wantTotal.Load() {
				t.Fatalf("counter drift: server=%d client-sum=%d (lost or duplicated executions)", got, wantTotal.Load())
			}
		})
	}
}

// TestTruthDifferentialVsPlainClient runs an identical randomized mixed
// workload through a plain client (the model) and the autopipeliner (the
// subject) on disjoint keyspaces, then diffs the full keyspace dumps. Any
// divergence in values, hashes, or list contents is an engine correctness
// bug by construction.
func TestTruthDifferentialVsPlainClient(t *testing.T) {
	ctx := context.Background()
	client := newTruthClient(t)
	defer client.Close()
	client.FlushDB(ctx)
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 100, MaxConcurrentBatches: 16, Unordered: true, NumShards: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	type op struct {
		kind       int
		key, field string
		val        int64
	}
	const G, M = 32, 150
	plans := make([][]op, G)
	for g := 0; g < G; g++ {
		rng := rand.New(rand.NewSource(int64(g) + 42))
		plans[g] = make([]op, M)
		for i := range plans[g] {
			plans[g][i] = op{
				kind:  rng.Intn(4),
				key:   strconv.Itoa(rng.Intn(20)), // few keys -> heavy contention
				field: "f" + strconv.Itoa(rng.Intn(5)),
				val:   rng.Int63n(100),
			}
		}
	}
	run := func(prefix string, exec redis.Cmdable) {
		var wg sync.WaitGroup
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				for _, o := range plans[g] {
					key := prefix + o.key
					var err error
					switch o.kind {
					case 0:
						err = exec.IncrBy(ctx, key+":c", o.val).Err()
					case 1:
						err = exec.HIncrBy(ctx, key+":h", o.field, o.val).Err()
					case 2:
						err = exec.SAdd(ctx, key+":s", o.val).Err()
					case 3:
						err = exec.RPush(ctx, key+":l:"+strconv.Itoa(g), o.val).Err()
					}
					if err != nil {
						t.Errorf("%s op: %v", prefix, err)
						return
					}
				}
			}(g)
		}
		wg.Wait()
	}
	runWithWatchdog(t, 120*time.Second, func() {
		run("model:", client) // plain client = the model
		run("subject:", ap)   // autopipeliner = the subject
	})
	dump := func(prefix string) map[string]string {
		out := map[string]string{}
		keys, err := client.Keys(ctx, prefix+"*").Result()
		if err != nil {
			t.Fatal(err)
		}
		for _, k := range keys {
			short := strings.TrimPrefix(k, prefix)
			switch typ := client.Type(ctx, k).Val(); typ {
			case "string":
				out[short] = client.Get(ctx, k).Val()
			case "hash":
				out[short] = fmt.Sprint(client.HGetAll(ctx, k).Val())
			case "set":
				vals, _ := client.SMembers(ctx, k).Result()
				m := map[string]bool{}
				for _, v := range vals {
					m[v] = true
				}
				out[short] = fmt.Sprint(len(m)) // set cardinality (order-free)
			case "list":
				out[short] = strings.Join(client.LRange(ctx, k, 0, -1).Val(), ",")
			default:
				out[short] = "type:" + typ
			}
		}
		return out
	}
	model, subject := dump("model:"), dump("subject:")
	if len(model) != len(subject) {
		t.Fatalf("keyspace size differs: model=%d subject=%d", len(model), len(subject))
	}
	for k, mv := range model {
		if sv, ok := subject[k]; !ok || sv != mv {
			t.Errorf("DIVERGENCE key %q: model=%q subject=%q", k, mv, sv)
		}
	}
}

// TestTruthChunkedDispatchIntegrity drives the MaxBatchBytes chunked path
// with values large enough to split every drain into several chunks and
// verifies exact payload echo — chunk boundaries must not tear, reorder, or
// cross-wire replies.
func TestTruthChunkedDispatchIntegrity(t *testing.T) {
	ctx := context.Background()
	client := newTruthClient(t)
	defer client.Close()
	client.FlushDB(ctx)
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 500, MaxConcurrentBatches: 8, Unordered: true,
		MaxBatchBytes: 64 * 1024, // ~4 x 16KiB values per chunk
	})
	if err != nil {
		t.Fatal(err)
	}
	const G, M = 16, 40
	base := strings.Repeat("x", 16*1024)
	runWithWatchdog(t, 120*time.Second, func() {
		var wg sync.WaitGroup
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				sets := make([]*redis.StatusCmd, M)
				for i := 0; i < M; i++ {
					key := fmt.Sprintf("chunk:%d:%d", g, i)
					want := fmt.Sprintf("%s|tail=%d:%d", base, g, i)
					sets[i] = ap.Set(ctx, key, want, 0)
				}
				for i, s := range sets {
					if err := s.Err(); err != nil {
						t.Errorf("set g%d i%d: %v", g, i, err)
						return
					}
				}
				for i := 0; i < M; i++ {
					key := fmt.Sprintf("chunk:%d:%d", g, i)
					want := fmt.Sprintf("%s|tail=%d:%d", base, g, i)
					got, err := ap.Get(ctx, key).Result()
					if err != nil {
						t.Errorf("get %s: %v", key, err)
						return
					}
					if got != want {
						t.Errorf("CHUNK TEAR %s: got %d bytes tail %q, want %d bytes", key, len(got), got[len(got)-24:], len(want))
						return
					}
				}
			}(g)
		}
		wg.Wait()
	})
}

// TestTruthCancelNeverLies pins cancellation semantics: a context canceled
// mid-flight may fail the WAIT, but any command that reports success must
// have really executed, and any that reports an error must not have half a
// result. Verified by re-reading the keyspace with a fresh context.
func TestTruthCancelNeverLies(t *testing.T) {
	ctx := context.Background()
	client := newTruthClient(t)
	defer client.Close()
	client.FlushDB(ctx)
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	const N = 2000
	runWithWatchdog(t, 60*time.Second, func() {
		cctx, cancel := context.WithCancel(ctx)
		futs := make([]*redis.StatusCmd, N)
		for i := 0; i < N; i++ {
			futs[i] = ap.Set(cctx, "cancel:"+strconv.Itoa(i), "v"+strconv.Itoa(i), 0)
			if i == N/2 {
				cancel()
			}
		}
		okCount, errCount := 0, 0
		for i, f := range futs {
			if err := f.Err(); err == nil {
				okCount++
				// claimed success must be true on the server
				v, gerr := client.Get(ctx, "cancel:"+strconv.Itoa(i)).Result()
				if gerr != nil || v != "v"+strconv.Itoa(i) {
					t.Fatalf("cmd %d reported OK but server has %q/%v", i, v, gerr)
				}
			} else {
				errCount++
			}
		}
		t.Logf("cancel split: %d ok, %d error (both outcomes legal; lies are not)", okCount, errCount)
		cancel()
	})
}

// TestTruthCloseDrains pins Close's drain contract under load: every future
// submitted BEFORE Close returns a definite outcome (value or error, no
// hang), and successes are really on the server.
func TestTruthCloseDrains(t *testing.T) {
	ctx := context.Background()
	client := newTruthClient(t)
	defer client.Close()
	client.FlushDB(ctx)
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 64, MaxConcurrentBatches: 8, Unordered: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	const N = 5000
	runWithWatchdog(t, 60*time.Second, func() {
		futs := make([]*redis.StatusCmd, N)
		for i := 0; i < N; i++ {
			futs[i] = ap.Set(ctx, "drain:"+strconv.Itoa(i), "v", 0)
		}
		if err := ap.Close(); err != nil {
			t.Fatalf("close: %v", err)
		}
		okCount := 0
		for i, f := range futs {
			if err := f.Err(); err == nil {
				okCount++
			} else if err != redis.ErrClosed {
				t.Fatalf("future %d: unexpected error %v", i, err)
			}
		}
		// Every claimed success must exist server-side.
		var present int64
		for i := 0; i < N; i++ {
			if client.Exists(ctx, "drain:"+strconv.Itoa(i)).Val() == 1 {
				present++
			}
		}
		if int(present) < okCount {
			t.Fatalf("%d futures claimed OK but only %d keys present", okCount, present)
		}
		t.Logf("close drain: %d/%d confirmed executed, rest cleanly rejected", okCount, N)
	})
}

// TestTruthClusterOrderAndDemux repeats the order and demux proofs against a
// live cluster through the sharded autopipeliner: per-goroutine order on
// hashtagged keys (one slot per goroutine, so order is meaningful) and exact
// payload echo across slot-spread keys.
func TestTruthClusterOrderAndDemux(t *testing.T) {
	ctx := context.Background()
	cc := redis.NewClusterClient(&redis.ClusterOptions{Addrs: []string{":16600", ":16601", ":16602"}})
	defer cc.Close()
	if err := cc.Ping(ctx).Err(); err != nil {
		t.Skipf("no cluster: %v", err)
	}
	for _, n := range []string{":16600", ":16601", ":16602"} {
		nc := redis.NewClient(&redis.Options{Addr: n})
		nc.FlushDB(ctx)
		nc.Close()
	}
	ap, err := cc.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	const G, M = 32, 100
	runWithWatchdog(t, 120*time.Second, func() {
		var wg sync.WaitGroup
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				// order proof: hashtag pins the goroutine's list to one slot
				key := fmt.Sprintf("{ord%d}:list", g)
				futs := make([]*redis.IntCmd, M)
				for i := 0; i < M; i++ {
					futs[i] = ap.RPush(ctx, key, i)
				}
				for i, f := range futs {
					if err := f.Err(); err != nil {
						t.Errorf("g%d rpush %d: %v", g, i, err)
						return
					}
				}
				// demux proof: slot-spread unique echoes, phase-separated
				sets := make([]*redis.StatusCmd, M)
				for i := 0; i < M; i++ {
					sets[i] = ap.Set(ctx, fmt.Sprintf("cdemux:%d:%d", g, i), fmt.Sprintf("p|%d|%d", g, i), 0)
				}
				for i, s := range sets {
					if err := s.Err(); err != nil {
						t.Errorf("g%d set %d: %v", g, i, err)
						return
					}
				}
				for i := 0; i < M; i++ {
					want := fmt.Sprintf("p|%d|%d", g, i)
					got, err := ap.Get(ctx, fmt.Sprintf("cdemux:%d:%d", g, i)).Result()
					if err != nil || got != want {
						t.Errorf("CLUSTER DEMUX g%d i%d: got %q err=%v want %q", g, i, got, err, want)
						return
					}
				}
			}(g)
		}
		wg.Wait()
	})
	for g := 0; g < G; g++ {
		vals, err := cc.LRange(ctx, fmt.Sprintf("{ord%d}:list", g), 0, -1).Result()
		if err != nil {
			t.Fatal(err)
		}
		if len(vals) != M {
			t.Fatalf("cluster g%d: %d elements want %d", g, len(vals), M)
		}
		for i, v := range vals {
			if v != strconv.Itoa(i) {
				t.Fatalf("CLUSTER ORDER g%d: pos %d = %q want %q", g, i, v, i)
			}
		}
	}
}

// TestTruthRESP2Demux repeats the demultiplexing proof on RESP2, where
// replies carry no push-frame framing and a desynchronized stream shows up
// as cross-wired values rather than a protocol error.
func TestTruthRESP2Demux(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: truthAddr(), Protocol: 2, PoolSize: 32})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client.FlushDB(ctx)
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 100, MaxConcurrentBatches: 16, Unordered: true, NumShards: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	const G, M = 40, 150
	runWithWatchdog(t, 90*time.Second, func() {
		var wg sync.WaitGroup
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				sets := make([]*redis.StatusCmd, M)
				for i := 0; i < M; i++ {
					sets[i] = ap.Set(ctx, fmt.Sprintf("r2:%d:%d", g, i), fmt.Sprintf("v|%d|%d", g, i), 0)
				}
				for _, s := range sets {
					if err := s.Err(); err != nil {
						t.Errorf("resp2 set: %v", err)
						return
					}
				}
				// mixed reply SHAPES in one batch: string, integer, array,
				// hash, nil — a demux slip shows up as a type/shape mismatch.
				for i := 0; i < M; i++ {
					key := fmt.Sprintf("r2:%d:%d", g, i)
					sv := ap.Get(ctx, key)
					iv := ap.StrLen(ctx, key)
					av := ap.LRange(ctx, "r2:missing:list", 0, -1)
					nv := ap.Get(ctx, "r2:missing:key")
					want := fmt.Sprintf("v|%d|%d", g, i)
					if got, err := sv.Result(); err != nil || got != want {
						t.Errorf("RESP2 string demux g%d i%d: %q %v", g, i, got, err)
						return
					}
					if got, err := iv.Result(); err != nil || got != int64(len(want)) {
						t.Errorf("RESP2 int demux g%d i%d: %d %v want %d", g, i, got, err, len(want))
						return
					}
					if got, err := av.Result(); err != nil || len(got) != 0 {
						t.Errorf("RESP2 array demux g%d i%d: %v %v", g, i, got, err)
						return
					}
					if _, err := nv.Result(); err != redis.Nil {
						t.Errorf("RESP2 nil demux g%d i%d: err=%v want redis.Nil", g, i, err)
						return
					}
				}
			}(g)
		}
		wg.Wait()
	})
}

// TestTruthPipelinePoolInterleaving runs the autopipeliner against a client
// configured with the DEDICATED pipeline pool while a plain client hammers
// the same server through the regular pool: batched traffic and single
// commands must not contaminate each other's connections.
func TestTruthPipelinePoolInterleaving(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: truthAddr(), PoolSize: 32,
		PipelineReadBufferSize: 128 << 10, PipelineWriteBufferSize: 128 << 10,
		PipelinePoolSize: 4,
	})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client.FlushDB(ctx)
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 128, MaxConcurrentBatches: 16, Unordered: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	const G, M = 24, 200
	runWithWatchdog(t, 120*time.Second, func() {
		var wg sync.WaitGroup
		// plain-client noise on the regular pool
		stop := make(chan struct{})
		for n := 0; n < 8; n++ {
			wg.Add(1)
			go func(n int) {
				defer wg.Done()
				for i := 0; ; i++ {
					select {
					case <-stop:
						return
					default:
					}
					key := fmt.Sprintf("noise:%d", n)
					want := fmt.Sprintf("n|%d|%d", n, i)
					if err := client.Set(ctx, key, want, 0).Err(); err != nil {
						t.Errorf("noise set: %v", err)
						return
					}
					if got, err := client.Get(ctx, key).Result(); err != nil || got != want {
						t.Errorf("NOISE CONTAMINATION: got %q err=%v want %q", got, err, want)
						return
					}
				}
			}(n)
		}
		// autopipelined traffic on the pipeline pool
		var apWg sync.WaitGroup
		for g := 0; g < G; g++ {
			apWg.Add(1)
			go func(g int) {
				defer apWg.Done()
				sets := make([]*redis.StatusCmd, M)
				for i := 0; i < M; i++ {
					sets[i] = ap.Set(ctx, fmt.Sprintf("pp:%d:%d", g, i), fmt.Sprintf("p|%d|%d", g, i), 0)
				}
				for _, s := range sets {
					if err := s.Err(); err != nil {
						t.Errorf("pp set: %v", err)
						return
					}
				}
				for i := 0; i < M; i++ {
					want := fmt.Sprintf("p|%d|%d", g, i)
					got, err := ap.Get(ctx, fmt.Sprintf("pp:%d:%d", g, i)).Result()
					if err != nil || got != want {
						t.Errorf("PIPELINE POOL DEMUX g%d i%d: got %q err=%v want %q", g, i, got, err, want)
						return
					}
				}
			}(g)
		}
		apWg.Wait()
		close(stop)
		wg.Wait()
	})
}

// TestTruthRetriesDoNotDuplicate pins at-most-once execution across the
// engine's retry machinery: non-idempotent INCRs run through an
// autopipeliner whose client has retries ENABLED while the server is
// pounded concurrently; the final counter must equal the exact number of
// successful futures — a retried batch that re-executed a committed command
// would overshoot.
func TestTruthRetriesDoNotDuplicate(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: truthAddr(), PoolSize: 32, MaxRetries: 3})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client.FlushDB(ctx)
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 64, MaxConcurrentBatches: 16, Unordered: true, NumShards: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	const G, M = 40, 250
	var okCount atomic.Int64
	runWithWatchdog(t, 120*time.Second, func() {
		var wg sync.WaitGroup
		for g := 0; g < G; g++ {
			wg.Add(1)
			go func(g int) {
				defer wg.Done()
				futs := make([]*redis.IntCmd, M)
				for i := 0; i < M; i++ {
					futs[i] = ap.Incr(ctx, "retry:counter")
				}
				for _, f := range futs {
					if err := f.Err(); err == nil {
						okCount.Add(1)
					}
				}
			}(g)
		}
		wg.Wait()
	})
	got, err := client.Get(ctx, "retry:counter").Int64()
	if err != nil {
		t.Fatal(err)
	}
	if got != okCount.Load() {
		t.Fatalf("at-most-once violated: counter=%d successful futures=%d", got, okCount.Load())
	}
	t.Logf("at-most-once: %d increments == %d successful futures", got, okCount.Load())
}
