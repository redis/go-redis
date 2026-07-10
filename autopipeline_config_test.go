package redis_test

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineOptionsNotMutated guards a regression where the autopipeliner
// filled zero-value defaults (MaxBatchSize, MaxConcurrentBatches) directly into
// the caller's *AutoPipelineOptions. A config shared across clients (or inspected
// later by the caller) must not be mutated by creating an AutoPipeliner.
func TestAutoPipelineOptionsNotMutated(t *testing.T) {
	cfg := &redis.AutoPipelineOptions{} // all zero -> defaults applied internally

	c := redis.NewClient(&redis.Options{Addr: ":6379", AutoPipelineOptions: cfg})
	defer c.Close()
	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	if cfg.MaxBatchSize != 0 {
		t.Fatalf("caller config MaxBatchSize mutated to %d (want 0)", cfg.MaxBatchSize)
	}
	if cfg.MaxConcurrentBatches != 0 {
		t.Fatalf("caller config MaxConcurrentBatches mutated to %d (want 0)", cfg.MaxConcurrentBatches)
	}

	// The same config reused for a second client must still be pristine and
	// usable (defaults applied to an internal copy, not the shared struct).
	c2 := redis.NewClient(&redis.Options{Addr: ":6379", AutoPipelineOptions: cfg})
	defer c2.Close()
	ap2, err := c2.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap2.Close()
	if cfg.MaxBatchSize != 0 || cfg.MaxConcurrentBatches != 0 {
		t.Fatal("shared config mutated after second client created an AutoPipeliner")
	}
}
