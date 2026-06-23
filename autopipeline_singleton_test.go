package redis_test

import (
	"context"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineConcurrentFirstCall guards a data race where AutoPipeline()
// did its cache check-and-set on the client's autopipeliner field without a
// lock. Concurrent first calls could each build a separate AutoPipeliner —
// racing the field (caught by -race) and leaking the loser's flusher goroutines
// and pipeline pool. Every concurrent caller must observe the SAME instance.
func TestAutoPipelineConcurrentFirstCall(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)

	const G = 64
	results := make([]*redis.AutoPipeliner, G)
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(i int) {
			defer wg.Done()
			<-start // maximize the race on the first call
			results[i] = c.AutoPipeline()
		}(g)
	}
	close(start)
	wg.Wait()

	first := results[0]
	if first == nil {
		t.Fatal("AutoPipeline() returned nil")
	}
	for i, ap := range results {
		if ap != first {
			t.Fatalf("call %d returned a different AutoPipeliner instance; concurrent first calls must share one", i)
		}
	}
	defer first.Close()

	// The shared instance must still work.
	if err := first.Set(ctx, "singleton", "1", 0).Err(); err != nil {
		t.Fatalf("shared autopipeliner unusable: %v", err)
	}
}
