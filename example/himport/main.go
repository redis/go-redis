// Example: fast hash ingestion with the HIMPORT command family (Redis 8.10+).
//
// HIMPORT PREPARE registers a fieldset (an ordered list of hash field names)
// once per connection session; HIMPORT SET then creates hashes by sending
// only the values. go-redis remembers fieldsets registered through
// HImportPrepare and lazily replays the PREPARE on whichever pooled
// connection runs an HImportSet, so the API works transparently on the
// pooled client, pipelines, and transactions.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	fieldsetName = "user-fields"
	bulkKeys     = 10_000
	batchSize    = 500
)

func main() {
	ctx := context.Background()

	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		PoolSize: 8,
	})
	defer rdb.Close()

	// HIMPORT requires Redis 8.10+ built with the hinted hash templates
	// feature; bail out gracefully on older servers.
	if err := rdb.HImportDiscardAll(ctx).Err(); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "unknown command") {
			log.Fatalf("this server does not support HIMPORT (requires Redis 8.10+): %v", err)
		}
		log.Fatalf("connection failed: %v", err)
	}

	basicUsage(ctx, rdb)
	bulkIngestion(ctx, rdb)
	concurrentWriters(ctx, rdb)
	inspectEncoding(ctx, rdb)
	discard(ctx, rdb)
}

// basicUsage registers a fieldset once and creates hashes from values only.
func basicUsage(ctx context.Context, rdb *redis.Client) {
	fmt.Println("== basic usage ==")

	// Register the field names once. The client remembers the fieldset and
	// prepares it on every pooled connection that needs it.
	if err := rdb.HImportPrepare(ctx, fieldsetName, "name", "email", "age").Err(); err != nil {
		log.Fatalf("prepare: %v", err)
	}

	// Values map positionally to the prepared fields.
	if err := rdb.HImportSet(ctx, "user:1", fieldsetName, "alice", "alice@example.com", 25).Err(); err != nil {
		log.Fatalf("set: %v", err)
	}
	if err := rdb.HImportSet(ctx, "user:2", fieldsetName, "bob", "bob@example.com", 30).Err(); err != nil {
		log.Fatalf("set: %v", err)
	}

	// Keys created through HIMPORT SET are regular hashes.
	user, err := rdb.HGetAll(ctx, "user:1").Result()
	if err != nil {
		log.Fatalf("hgetall: %v", err)
	}
	fmt.Printf("user:1 = %v\n\n", user)
}

// bulkIngestion loads many hashes through pipelines — the intended
// high-throughput pattern — and compares it with plain HSET pipelines.
func bulkIngestion(ctx context.Context, rdb *redis.Client) {
	fmt.Println("== bulk ingestion: HIMPORT SET vs HSET ==")

	start := time.Now()
	for base := 0; base < bulkKeys; base += batchSize {
		pipe := rdb.Pipeline()
		for i := base; i < base+batchSize && i < bulkKeys; i++ {
			key := fmt.Sprintf("himport:user:%d", i)
			name := fmt.Sprintf("user-%d", i)
			email := fmt.Sprintf("user-%d@example.com", i)
			pipe.HImportSet(ctx, key, fieldsetName, name, email, 20+i%50)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("himport pipeline: %v", err)
		}
	}
	himportTook := time.Since(start)

	start = time.Now()
	for base := 0; base < bulkKeys; base += batchSize {
		pipe := rdb.Pipeline()
		for i := base; i < base+batchSize && i < bulkKeys; i++ {
			key := fmt.Sprintf("hset:user:%d", i)
			name := fmt.Sprintf("user-%d", i)
			email := fmt.Sprintf("user-%d@example.com", i)
			pipe.HSet(ctx, key, "name", name, "email", email, "age", 20+i%50)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			log.Fatalf("hset pipeline: %v", err)
		}
	}
	hsetTook := time.Since(start)

	fmt.Printf("%d hashes via HIMPORT SET: %v\n", bulkKeys, himportTook)
	fmt.Printf("%d hashes via HSET:        %v\n\n", bulkKeys, hsetTook)
}

// concurrentWriters shows that the pooled client needs no connection
// management: each pooled connection is prepared lazily, at most once.
func concurrentWriters(ctx context.Context, rdb *redis.Client) {
	fmt.Println("== concurrent pooled writers ==")

	var wg sync.WaitGroup
	errCh := make(chan error, 100)
	for w := 0; w < 100; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			key := fmt.Sprintf("concurrent:user:%d", w)
			name := fmt.Sprintf("worker-%d", w)
			email := fmt.Sprintf("worker-%d@example.com", w)
			if err := rdb.HImportSet(ctx, key, fieldsetName, name, email, w).Err(); err != nil {
				errCh <- err
			}
		}(w)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		log.Fatalf("concurrent set: %v", err)
	}
	fmt.Printf("100 goroutines wrote through the pool without explicit PREPARE calls\n\n")
}

// inspectEncoding surfaces the server-side effect: hashes created through a
// shared template report a template encoding, and INFO tracks template use.
func inspectEncoding(ctx context.Context, rdb *redis.Client) {
	fmt.Println("== server-side encoding ==")

	encoding, err := rdb.ObjectEncoding(ctx, "himport:user:0").Result()
	if err != nil {
		log.Fatalf("object encoding: %v", err)
	}
	fmt.Printf("OBJECT ENCODING himport:user:0 = %s\n", encoding)

	info, err := rdb.Info(ctx, "memory").Result()
	if err != nil {
		log.Fatalf("info: %v", err)
	}
	for _, line := range strings.Split(info, "\r\n") {
		if strings.HasPrefix(line, "used_memory_hash_templates") ||
			strings.HasPrefix(line, "hash_templates") ||
			strings.HasPrefix(line, "hash_template_keys") {
			fmt.Println(line)
		}
	}
	fmt.Println()
}

// discard removes the fieldset; hashes created through it are unaffected.
func discard(ctx context.Context, rdb *redis.Client) {
	fmt.Println("== discard ==")

	removed, err := rdb.HImportDiscard(ctx, fieldsetName).Result()
	if err != nil {
		log.Fatalf("discard: %v", err)
	}
	fmt.Printf("discarded fieldset (removed=%d)\n", removed)

	// The fieldset is gone from the client registry and this connection's
	// session: further sets surface the server error unchanged.
	err = rdb.HImportSet(ctx, "user:3", fieldsetName, "carol", "carol@example.com", 35).Err()
	fmt.Printf("set after discard: %v\n", err)

	// Existing hashes survive.
	name, err := rdb.HGet(ctx, "user:1", "name").Result()
	if err != nil {
		log.Fatalf("hget: %v", err)
	}
	fmt.Printf("user:1 name still readable: %s\n", name)
}
