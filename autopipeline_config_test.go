package redis_test

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestAutoPipelineConfigNotMutated guards a regression where NewAutoPipeliner
// filled zero-value defaults (MaxBatchSize, MaxConcurrentBatches) directly into
// the caller's *AutoPipelineConfig. A config shared across clients (or inspected
// later by the caller) must not be mutated by creating an AutoPipeliner.
func TestAutoPipelineConfigNotMutated(t *testing.T) {
	cfg := &redis.AutoPipelineConfig{} // all zero -> defaults applied internally

	c := redis.NewClient(&redis.Options{Addr: ":6379", AutoPipelineConfig: cfg})
	defer c.Close()
	ap := c.AutoPipeline()
	defer ap.Close()

	if cfg.MaxBatchSize != 0 {
		t.Fatalf("caller config MaxBatchSize mutated to %d (want 0)", cfg.MaxBatchSize)
	}
	if cfg.MaxConcurrentBatches != 0 {
		t.Fatalf("caller config MaxConcurrentBatches mutated to %d (want 0)", cfg.MaxConcurrentBatches)
	}

	// The same config reused for a second client must still be pristine and
	// usable (defaults applied to an internal copy, not the shared struct).
	c2 := redis.NewClient(&redis.Options{Addr: ":6379", AutoPipelineConfig: cfg})
	defer c2.Close()
	ap2 := c2.AutoPipeline()
	defer ap2.Close()
	if cfg.MaxBatchSize != 0 || cfg.MaxConcurrentBatches != 0 {
		t.Fatal("shared config mutated after second client created an AutoPipeliner")
	}
}
