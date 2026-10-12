package example_commands_test

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

// waitForIndexedDocs blocks until the search index reports exactly n
// documents with ingestion settled, or ~5s pass. The search examples HSET /
// JSONSET and query back to back, but indexing is asynchronous — on a loaded
// runner the query raced ingestion and returned zero results. Test-only:
// example bodies call this inside REMOVE_START/REMOVE_END markers, so it
// never appears in the documentation snippets.
func waitForIndexedDocs(ctx context.Context, rdb *redis.Client, index string, n int) {
	for i := 0; i < 100; i++ {
		info, err := rdb.FTInfo(ctx, index).Result()
		if err == nil && info.NumDocs == n && info.Indexing == 0 {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
}
