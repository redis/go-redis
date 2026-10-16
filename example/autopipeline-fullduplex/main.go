// Command autopipeline-fullduplex shows the blocking autopipeliner in
// full-duplex mode: every goroutine's commands share ONE held connection, the
// writer keeps sending while the reader receives, and replies are matched to
// commands by their position on the wire.
//
// It shows:
//
//  1. Turning it on, and checking that it engaged.
//  2. The blocking face as a drop-in: many goroutines, one connection.
//  3. Pipeline() and FDPipelined: a whole pipeline rides the same connection,
//     contiguous, with the errors an ordinary pipeline reports.
//  4. What falls back to a pooled connection, and why.
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

	"github.com/redis/go-redis/v9"
)

const clientName = "fd-example"

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

	// ClientName labels this client's connections, so connections() below can
	// count them with CLIENT LIST.
	rdb := redis.NewClient(&redis.Options{Addr: addr(), ClientName: clientName})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		fatalf("ping %s: %v (is Redis running?)", addr(), err)
	}
	admin := redis.NewClient(&redis.Options{Addr: addr()})
	defer admin.Close()

	// 1. Turn it on. FullDuplex needs a standalone *Client with a pipeline
	// pool (it always has one unless PipelinePoolSize < 0). Config reports the
	// effective state, so a FullDuplex that could not engage reads false.
	ap, err := rdb.AutoPipelineWithOptions(&redis.AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		fatalf("AutoPipelineWithOptions: %v", err)
	}
	defer ap.Close()
	fmt.Printf("1. full duplex engaged: %v\n", ap.Config().FullDuplex)
	if err := rdb.Del(ctx, "fd:counter", "fd:list", "fd:missing").Err(); err != nil {
		fatalf("del: %v", err)
	}

	// 2. Drop-in: each call blocks until its reply lands, exactly like a plain
	// client, and each goroutine's commands run in its own order. The engine
	// streams all of them over one connection.
	const goroutines = 200
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			key := fmt.Sprintf("fd:key:%d", g)
			for i := 0; i < 50; i++ {
				if err := ap.Set(ctx, key, i, 0).Err(); err != nil {
					errs <- err
					return
				}
				if v, err := ap.Get(ctx, key).Int(); err != nil || v != i {
					errs <- fmt.Errorf("%s: got %d, %v; want %d", key, v, err, i)
					return
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		fatalf("drop-in: %v", err)
	}
	fmt.Printf("2. %d goroutines x 50 SET+GET, read-your-writes held; %s\n",
		goroutines, connections(ctx, admin))

	// 3. Pipeline() on the full-duplex connection. The batch is admitted
	// whole, so its commands are adjacent on the wire and no other caller's
	// command lands between them. Exec returns what an ordinary pipeline
	// returns: the first error, redis.Nil included, while every command keeps
	// its own result.
	pipe := ap.Pipeline()
	incr := pipe.Incr(ctx, "fd:counter")
	pipe.RPush(ctx, "fd:list", "a", "b", "c")
	lrange := pipe.LRange(ctx, "fd:list", 0, -1)
	missing := pipe.Get(ctx, "fd:missing")
	_, err = pipe.Exec(ctx)
	if !errors.Is(err, redis.Nil) {
		fatalf("pipeline Exec: err %v, want redis.Nil from the GET of a missing key", err)
	}
	fmt.Printf("3. pipeline on the FD connection: INCR=%d LRANGE=%v GET missing=%v (Exec err: %v)\n",
		incr.Val(), lrange.Val(), missing.Err(), err)

	// FDPipelined is the same thing without the Pipeliner: hand it the
	// commands, it blocks until every reply has landed.
	cmds := []redis.Cmder{
		redis.NewIntCmd(ctx, "incr", "fd:counter"),
		redis.NewIntCmd(ctx, "incr", "fd:counter"),
	}
	if err := ap.FDPipelined(ctx, cmds); err != nil {
		fatalf("FDPipelined: %v", err)
	}
	fmt.Printf("   FDPipelined: INCR, INCR -> %d, %d\n",
		cmds[0].(*redis.IntCmd).Val(), cmds[1].(*redis.IntCmd).Val())

	// 4. What cannot ride the stream. A blocking command (BLPOP, XREAD BLOCK,
	// ...) would stall every caller behind it on the shared connection, so
	// single blocking commands are diverted to a pooled connection
	// automatically. A batch that contains one cannot be split without losing
	// its contiguity, so FDPipelined refuses it...
	blocking := []redis.Cmder{
		redis.NewStringSliceCmd(ctx, "blpop", "fd:list", 1),
	}
	if err := ap.FDPipelined(ctx, blocking); !errors.Is(err, redis.ErrFDPipelineDiverts) {
		fatalf("FDPipelined with BLPOP: err %v, want ErrFDPipelineDiverts", err)
	}
	// ...and Pipeline() falls back to an ordinary pipeline on a pooled
	// connection, so the same batch still runs.
	pipe = ap.Pipeline()
	pop := pipe.BLPop(ctx, 0, "fd:list")
	if _, err := pipe.Exec(ctx); err != nil {
		fatalf("pipeline with BLPOP: %v", err)
	}
	fmt.Printf("4. FDPipelined refused a BLPOP batch; Pipeline() ran it pooled: %v\n", pop.Val())
	// TxPipeline always runs pooled: MULTI/EXEC needs the connection for the
	// whole transaction.
	tx := ap.TxPipeline()
	tx.Incr(ctx, "fd:counter")
	if _, err := tx.Exec(ctx); err != nil {
		fatalf("tx: %v", err)
	}
	fmt.Println("   TxPipeline ran on a pooled connection (MULTI/EXEC needs one to itself)")

	if err := ap.Close(); err != nil {
		fatalf("close: %v", err)
	}
	fmt.Println("done")
}

// connections counts this client's open connections. The full-duplex engine
// holds one; the others are the client's main pool (the Ping, the Del) and
// any pooled pipeline the fallbacks used.
func connections(ctx context.Context, admin *redis.Client) string {
	list, err := admin.ClientList(ctx).Result()
	if err != nil {
		return "connections: " + err.Error()
	}
	return fmt.Sprintf("connections named %q: %d", clientName, strings.Count(list, "name="+clientName+" "))
}
