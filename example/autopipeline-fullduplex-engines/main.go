// Command autopipeline-fullduplex-engines shows NumShards > 1 on a
// full-duplex autopipeliner: several engines on ONE client, each holding its
// own connection. One connection has a throughput ceiling; more engines lift
// it, at the cost of ordering only per key.
//
// It shows:
//
//  1. Sizing: every engine holds one pipeline-pool connection, and
//     construction fails if the pool cannot hold them all.
//  2. Routing: a command goes to the engine its first key hashes to, so
//     commands for the same key keep their order.
//  3. What is not ordered: different keys, multi-key and keyless commands.
//  4. Pipelines: a pipeline rides an engine only when all its keys hash to it.
//  5. When more engines pay off: a short run at low and high concurrency.
//
// Start a Redis first (e.g. `docker run --rm -p 6379:6379 redis`) then:
//
//	go run .
//
// Set REDIS_ADDR to point at a different server.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const engines = 4

func addr() string {
	if a := os.Getenv("REDIS_ADDR"); a != "" {
		return a
	}
	return "localhost:6379"
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "FAIL: "+format+"\n", args...)
	os.Exit(1)
}

func main() {
	ctx := context.Background()
	admin := redis.NewClient(&redis.Options{Addr: addr()})
	defer admin.Close()
	if err := admin.Ping(ctx).Err(); err != nil {
		fatalf("ping %s: %v (is Redis running?)", addr(), err)
	}

	// 1. Sizing. Each engine holds one connection from the pipeline pool for
	// as long as it runs. A pool too small for the engines is rejected at
	// construction rather than spilling engines onto the main pool. The check
	// covers one autopipeliner: the blocking and async faces share the pool,
	// so size it for every full-duplex autopipeliner the client runs.
	small := redis.NewClient(&redis.Options{Addr: addr(), PipelinePoolSize: 2})
	_, err := small.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true, NumShards: engines})
	small.Close()
	if err == nil {
		fatalf("NumShards %d on a pipeline pool of 2 was accepted", engines)
	}
	fmt.Printf("1. NumShards=%d, PipelinePoolSize=2 -> %v\n", engines, err)

	rdb := redis.NewClient(&redis.Options{Addr: addr(), ClientName: "fd-engines-example", PipelinePoolSize: engines})
	defer rdb.Close()
	ap, err := rdb.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true, NumShards: engines})
	if err != nil {
		fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()
	fmt.Printf("   NumShards=%d, PipelinePoolSize=%d -> %d engines, full duplex %v\n",
		engines, engines, ap.Config().NumShards, ap.Config().FullDuplex)

	// 2. Routing by first key. These SET/GET pairs are submitted without
	// waiting in between; each pair shares a key, so both commands take the
	// same engine and the GET sees the SET. Unordered is not needed.
	gets := make([]*redis.StringCmd, 100)
	for i := range gets {
		key := fmt.Sprintf("fde:key:%d", i)
		ap.Set(ctx, key, i, 0)
		gets[i] = ap.Get(ctx, key)
	}
	for i, g := range gets {
		if g.Val() != fmt.Sprint(i) {
			fatalf("unawaited SET then GET on %d: got %q", i, g.Val())
		}
	}
	fmt.Printf("2. %d unawaited SET+GET pairs, each pair on its key's engine: all read their write\n", len(gets))
	fmt.Printf("   connections: %d (one per engine)\n", connections(ctx, admin, "fd-engines-example"))

	// 3. What is NOT ordered against the rest:
	//   - commands with different first keys (they may be on different engines);
	//   - a multi-key command against its other keys: COPY src dst is routed by
	//     src, so a later GET dst may run first;
	//   - keyless commands (FLUSHDB, SCRIPT FLUSH, ...), which round-robin.
	// When one depends on another, await the first. NumShards 1 keeps the
	// whole submit order.
	ap.Set(ctx, "fde:src", "copied", 0)
	if err := ap.Copy(ctx, "fde:src", "fde:dst", 0, true).Err(); err != nil { // await before the dependent GET
		fatalf("copy: %v", err)
	}
	fmt.Printf("3. COPY awaited before GET dst -> %q\n", ap.Get(ctx, "fde:dst").Val())

	// 4. Pipelines. A batch is contiguous on one connection, so it rides an
	// engine only when every keyed command in it hashes to that engine.
	// A one-key batch always does.
	cmds := []redis.Cmder{
		redis.NewIntCmd(ctx, "hset", "fde:user:1", "name", "ada"),
		redis.NewIntCmd(ctx, "hincrby", "fde:user:1", "visits", 1),
		redis.NewMapStringStringCmd(ctx, "hgetall", "fde:user:1"),
	}
	if err := ap.FDPipelined(ctx, cmds); err != nil {
		fatalf("one-key FDPipelined: %v", err)
	}
	fmt.Printf("4. one-key batch rode its engine: %v\n", cmds[2].(*redis.MapStringStringCmd).Val())
	// Keys that hash to different engines cannot share a connection (engines
	// hash the whole key; a {hash tag} does not group keys here).
	// FDPipelined reports it...
	var spread []redis.Cmder
	for i := 0; i < 64; i++ {
		spread = []redis.Cmder{
			redis.NewStatusCmd(ctx, "set", "fde:a", "1"),
			redis.NewStatusCmd(ctx, "set", fmt.Sprintf("fde:b:%d", i), "2"),
		}
		if err = ap.FDPipelined(ctx, spread); errors.Is(err, redis.ErrFDPipelineSpansEngines) {
			break
		}
	}
	if !errors.Is(err, redis.ErrFDPipelineSpansEngines) {
		fatalf("no two-key batch spanned engines (last err %v)", err)
	}
	fmt.Println("   two-key batch on two engines: FDPipelined ->", err)
	// ...and Pipeline() runs such a batch as an ordinary pipeline on a pooled
	// connection: it still runs, but it is not ordered against unawaited
	// commands on the engines.
	pipe := ap.Pipeline()
	for _, c := range spread {
		_ = pipe.Process(ctx, c)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		fatalf("spread pipeline: %v", err)
	}
	fmt.Println("   the same batch through Pipeline() ran pooled")
	if err := ap.Close(); err != nil {
		fatalf("close: %v", err)
	}

	// 5. When more engines pay off. A few callers cannot fill several
	// queues, so each batch fragments and more engines lose; many callers
	// saturate one connection, and more engines win. Loopback numbers;
	// indicative only.
	fmt.Println("5. SET/s, 2s per run:")
	fmt.Printf("   %-26s %12s %12s\n", "load", "1 engine", fmt.Sprintf("%d engines", engines))
	for _, l := range []struct{ goroutines, window int }{{4, 1}, {512, 32}} {
		one := bench(ctx, 1, l.goroutines, l.window)
		many := bench(ctx, engines, l.goroutines, l.window)
		load := fmt.Sprintf("%d goroutines x window %d", l.goroutines, l.window)
		fmt.Printf("   %-26s %12.0f %12.0f  (%+.0f%%)\n", load, one, many, (many/one-1)*100)
	}
	fmt.Println("done")
}

// bench runs goroutines that each submit windows of SETs on an autopipeliner
// with n engines, and returns SET/s. goroutines x window is the number of
// commands kept in flight. Each run gets its own client: an
// autopipeliner is cached per client, and its first config wins.
func bench(ctx context.Context, n, goroutines, window int) float64 {
	rdb := redis.NewClient(&redis.Options{Addr: addr(), PipelinePoolSize: engines})
	defer rdb.Close()
	ap, err := rdb.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true, NumShards: n})
	if err != nil {
		fatalf("bench: %v", err)
	}
	defer ap.Close()
	var ops atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(2 * time.Second)
	for w := 0; w < goroutines; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cmds := make([]*redis.StatusCmd, window)
			key := fmt.Sprintf("fde:bench:%d", w)
			for time.Now().Before(deadline) {
				for i := range cmds {
					cmds[i] = ap.Set(ctx, key, i, 0)
				}
				for _, c := range cmds {
					if c.Err() != nil {
						fatalf("bench set: %v", c.Err())
					}
				}
				ops.Add(int64(window))
			}
		}(w)
	}
	wg.Wait()
	return float64(ops.Load()) / 2
}

// connections counts the open connections named name: one per engine, plus
// any the main pool or a pooled pipeline opened.
func connections(ctx context.Context, admin *redis.Client, name string) int {
	list, err := admin.ClientList(ctx).Result()
	if err != nil {
		fatalf("client list: %v", err)
	}
	return strings.Count(list, "name="+name+" ")
}
