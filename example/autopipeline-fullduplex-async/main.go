// Command autopipeline-fullduplex-async shows the deferred (async)
// autopipeliner in full-duplex mode, first on one connection, then on
// several (NumShards > 1).
//
// Calls return at once; the result accessors (Val/Result/Err) block until the
// reply lands. Submitting a window of commands before reading any result keeps
// the connection full, which is where full duplex is fastest. One connection
// has a throughput ceiling; NumShards runs several engines on one client, each
// holding its own connection, at the cost of ordering only per key.
//
// Part A, one engine:
//
//  1. A submission window: submit N commands, then read the results.
//  2. Per-goroutine order holds for unawaited commands (SET then GET).
//  3. Submit and AutoFuture for a raw Cmder, with Wait and WaitContext.
//
// Part B, several engines:
//
//  4. Sizing: each engine holds one pipeline-pool connection.
//  5. Routing by first key: same-key commands keep their order.
//  6. What is not ordered: different keys, multi-key and keyless commands.
//  7. Pipelines ride an engine only when all their keys hash to it.
//
// Part C:
//
//  8. Throughput with one engine and with several, at low and high load.
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
	if err := admin.Del(ctx, "fda:counter", "fda:seq", "fde:user:1").Err(); err != nil {
		fatalf("del: %v", err)
	}

	fmt.Println("A. one engine")
	oneEngine(ctx)
	fmt.Println("B. several engines")
	severalEngines(ctx, admin)
	fmt.Println("C. throughput")
	throughput(ctx, admin)
	fmt.Println("done")
}

func oneEngine(ctx context.Context) {
	rdb := redis.NewClient(&redis.Options{Addr: addr()})
	defer rdb.Close()
	// FullDuplexWindow bounds the commands in flight on the connection
	// (written, reply not yet read). The default, 65536, covers ~50 ms links
	// at ~1.3M ops/s; it costs memory only while commands are in flight.
	ap, err := rdb.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()
	fmt.Printf("   full duplex engaged: %v, window %d\n", ap.Config().FullDuplex, ap.Config().FullDuplexWindow)

	// 1. A window: every Set returns at once, so all 1000 are on the wire
	// before the first result is read. Reading a result blocks only until
	// that command's reply has landed.
	const window = 1000
	sets := make([]*redis.StatusCmd, window)
	for i := range sets {
		sets[i] = ap.Set(ctx, fmt.Sprintf("fda:key:%d", i), i, 0)
	}
	for i, cmd := range sets {
		if err := cmd.Err(); err != nil {
			fatalf("set %d: %v", i, err)
		}
	}
	fmt.Printf("1. submitted %d SETs, then read every result\n", window)

	// 2. Order. One goroutine's commands run in the order it submitted them,
	// even when it does not wait in between: this GET sees the SET before it,
	// and the INCRs count up in order. The exception is a command the engine
	// diverts off the shared connection (a blocking command, a per-command
	// read timeout): it can settle after a later one, so await its result
	// before submitting a command that depends on it.
	set := ap.Set(ctx, "fda:seq", "first", 0)
	get := ap.Get(ctx, "fda:seq")
	incrs := make([]*redis.IntCmd, 5)
	for i := range incrs {
		incrs[i] = ap.Incr(ctx, "fda:counter")
	}
	if set.Err() != nil || get.Val() != "first" {
		fatalf("unawaited SET then GET: set err %v, get %q", set.Err(), get.Val())
	}
	for i, c := range incrs {
		if c.Val() != int64(i+1) {
			fatalf("INCR %d returned %d", i, c.Val())
		}
	}
	fmt.Printf("2. unawaited SET then GET -> %q; INCRs -> 1..%d in order\n", get.Val(), len(incrs))

	// 3. Submit takes any Cmder, typed or raw, and returns an AutoFuture.
	// Wait blocks until the reply lands; WaitContext also gives up when its
	// ctx is done. Giving up does not cancel the command: it is already on
	// the connection and will still run.
	f := ap.Submit(ctx, redis.NewCmd(ctx, "incrby", "fda:counter", 10))
	if err := f.Wait(); err != nil {
		fatalf("submit: %v", err)
	}
	v, _ := f.Cmd().(*redis.Cmd).Int64()
	wctx, cancel := context.WithTimeout(ctx, time.Second)
	g := ap.Submit(ctx, redis.NewStringCmd(ctx, "get", "fda:seq"))
	err = g.WaitContext(wctx)
	cancel()
	if err != nil {
		fatalf("WaitContext: %v", err)
	}
	fmt.Printf("3. Submit INCRBY 10 -> %d; WaitContext GET -> %q\n", v, g.Cmd().(*redis.StringCmd).Val())
}

func severalEngines(ctx context.Context, admin *redis.Client) {
	// 4. Sizing. Each engine holds one connection from the pipeline pool for
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
	fmt.Printf("4. NumShards=%d, PipelinePoolSize=2 -> %v\n", engines, err)

	const name = "fd-engines-example"
	rdb := redis.NewClient(&redis.Options{Addr: addr(), ClientName: name, PipelinePoolSize: engines})
	defer rdb.Close()
	ap, err := rdb.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true, NumShards: engines})
	if err != nil {
		fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()
	fmt.Printf("   NumShards=%d, PipelinePoolSize=%d -> %d engines, full duplex %v\n",
		engines, engines, ap.Config().NumShards, ap.Config().FullDuplex)

	// 5. Routing by first key. These SET/GET pairs are submitted without
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
	fmt.Printf("5. %d unawaited SET+GET pairs, each pair on its key's engine: all read their write\n", len(gets))
	fmt.Printf("   connections: %d (one per engine)\n", connections(ctx, admin, name))

	// 6. What is NOT ordered against the rest:
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
	fmt.Printf("6. COPY awaited before GET dst -> %q\n", ap.Get(ctx, "fde:dst").Val())

	// 7. Pipelines. A batch is contiguous on one connection, so it rides an
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
	fmt.Printf("7. one-key batch rode its engine: %v\n", cmds[2].(*redis.MapStringStringCmd).Val())
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
}

// 8. When more engines pay off. With few commands in flight, each batch
// fragments across the engines and more engines lose; with many, one
// connection saturates and more engines win. Loopback numbers; indicative
// only.
func throughput(ctx context.Context, admin *redis.Client) {
	fmt.Println("8. SET/s, 2s per run:")
	fmt.Printf("   %-26s %12s %12s\n", "load", "1 engine", fmt.Sprintf("%d engines", engines))
	for _, l := range []struct{ goroutines, window int }{{4, 1}, {512, 32}} {
		one, c1 := bench(ctx, admin, 1, l.goroutines, l.window)
		many, cn := bench(ctx, admin, engines, l.goroutines, l.window)
		load := fmt.Sprintf("%d goroutines x window %d", l.goroutines, l.window)
		fmt.Printf("   %-26s %12.0f %12.0f  (%+.0f%%; connections %d vs %d)\n",
			load, one, many, (many/one-1)*100, c1, cn)
	}
}

// bench runs goroutines that each submit windows of SETs on an autopipeliner
// with n engines, and returns SET/s and the connections the run held.
// goroutines x window is the number of commands kept in flight. Each run gets
// its own client: an autopipeliner is cached per client, and its first config
// wins.
func bench(ctx context.Context, admin *redis.Client, n, goroutines, window int) (float64, int) {
	name := fmt.Sprintf("fd-bench-%d-%d", n, goroutines)
	rdb := redis.NewClient(&redis.Options{Addr: addr(), ClientName: name, PipelinePoolSize: engines})
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
	return float64(ops.Load()) / 2, connections(ctx, admin, name)
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
