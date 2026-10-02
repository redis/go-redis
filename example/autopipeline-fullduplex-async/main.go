// Command autopipeline-fullduplex-async shows the deferred (async)
// autopipeliner in full-duplex mode. Calls return at once; the result
// accessors (Val/Result/Err) block until the reply lands. Submitting a window
// of commands before reading any result keeps the shared connection full,
// which is where full duplex is fastest.
//
// It shows:
//
//  1. A submission window: submit N commands, then read the results.
//  2. Per-goroutine order holds for unawaited commands (SET then GET).
//  3. Submit and AutoFuture for a raw Cmder, with Wait and WaitContext.
//  4. A short timed run, and the connection count it used.
//
// Start a Redis first (e.g. `docker run --rm -p 6379:6379 redis`) then:
//
//	go run .
//
// Set REDIS_ADDR to point at a different server.
package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const clientName = "fd-async-example"

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
	rdb := redis.NewClient(&redis.Options{Addr: addr(), ClientName: clientName})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fatalf("ping %s: %v (is Redis running?)", addr(), err)
	}
	admin := redis.NewClient(&redis.Options{Addr: addr()})
	defer admin.Close()

	// FullDuplexWindow bounds the commands in flight on the connection
	// (written, reply not yet read). The default, 65536, covers ~50 ms links at
	// ~1.3M ops/s; it costs memory only while commands are really in flight.
	ap, err := rdb.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		fatalf("AsyncAutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()
	fmt.Printf("full duplex engaged: %v, window %d\n", ap.Config().FullDuplex, ap.Config().FullDuplexWindow)
	if err := rdb.Del(ctx, "fda:counter", "fda:seq").Err(); err != nil {
		fatalf("del: %v", err)
	}

	// 1. A window: every Set returns at once, so all 1000 are on the wire
	// before the first result is read. Reading a result blocks only until that
	// command's reply has landed.
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
	// and the INCRs count up in order.
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
	// One exception: a command the engine diverts off the shared connection
	// (a blocking command, a per-command read timeout) can settle after a
	// later one. Await its result before submitting a command that depends
	// on it.

	// 3. Submit takes any Cmder, typed or raw, and returns an AutoFuture.
	// Wait blocks until the reply lands; WaitContext also gives up when its
	// ctx is done. Giving up does not cancel the command: it is already on the
	// connection and will still run.
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

	// 4. A short timed run: 64 goroutines, each submitting windows of 256
	// SETs and then reading them. Every goroutine shares the one connection.
	const (
		workers  = 64
		perBurst = 256
		duration = 2 * time.Second
	)
	var ops atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(duration)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			cmds := make([]*redis.StatusCmd, perBurst)
			key := fmt.Sprintf("fda:bench:%d", w)
			for time.Now().Before(deadline) {
				for i := range cmds {
					cmds[i] = ap.Set(ctx, key, i, 0)
				}
				for _, c := range cmds {
					if c.Err() != nil {
						fatalf("bench set: %v", c.Err())
					}
				}
				ops.Add(perBurst)
			}
		}(w)
	}
	wg.Wait()
	fmt.Printf("4. %d goroutines, windows of %d: %.0f SET/s; %s\n",
		workers, perBurst, float64(ops.Load())/duration.Seconds(), connections(ctx, admin))

	if err := ap.Close(); err != nil {
		fatalf("close: %v", err)
	}
	fmt.Println("done")
}

// connections counts this client's open connections: the full-duplex engine
// holds one, and the main pool keeps the one the Ping and Del used.
func connections(ctx context.Context, admin *redis.Client) string {
	list, err := admin.ClientList(ctx).Result()
	if err != nil {
		return "connections: " + err.Error()
	}
	return fmt.Sprintf("connections named %q: %d", clientName, strings.Count(list, "name="+clientName+" "))
}
