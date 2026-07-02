package redis

import (
	"strings"
	"testing"
)

// TestAutoPipelineShardCountDecoupled verifies that the shard count no longer
// follows MaxConcurrentBatches: a standalone autopipeliner defaults to a single
// deep queue regardless of the permit budget, and NumShards overrides it.
func TestAutoPipelineShardCountDecoupled(t *testing.T) {
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	ap, err := client.AutoPipeline(&AutoPipelineConfig{
		MaxConcurrentBatches: 4,
		Unordered:            true,
	})
	if err != nil {
		t.Fatalf("AutoPipeline: %v", err)
	}
	if got := ap.numShards(); got != 1 {
		t.Fatalf("standalone default shards = %d, want 1 (must not follow MaxConcurrentBatches)", got)
	}
	_ = ap.Close()

	ap2, err := client.AutoPipeline(&AutoPipelineConfig{
		MaxConcurrentBatches: 2,
		Unordered:            true,
		NumShards:            4,
	})
	if err != nil {
		t.Fatalf("AutoPipeline with NumShards: %v", err)
	}
	if got := ap2.numShards(); got != 4 {
		t.Fatalf("NumShards=4 gave %d shards, want 4", got)
	}
	_ = ap2.Close()
}

// TestAutoPipelineNumShardsValidation verifies a negative NumShards is rejected
// at construction instead of being silently coerced.
func TestAutoPipelineNumShardsValidation(t *testing.T) {
	client := NewClient(&Options{Addr: ":6379"})
	defer client.Close()

	_, err := client.AutoPipeline(&AutoPipelineConfig{NumShards: -1})
	if err == nil || !strings.Contains(err.Error(), "NumShards") {
		t.Fatalf("NumShards=-1: got err %v, want NumShards validation error", err)
	}
}

// TestClusterAutoPipelineConfigShardDefault verifies the cluster wiring fills
// in a multi-shard default (slot routing needs several shards) without mutating
// the caller's config, and leaves an explicit NumShards untouched.
func TestClusterAutoPipelineConfigShardDefault(t *testing.T) {
	user := &AutoPipelineConfig{MaxConcurrentBatches: 8, Unordered: true}
	got := clusterAutoPipelineConfig(user)
	if got == user {
		t.Fatalf("expected a copy when filling the default, got the same pointer")
	}
	if user.NumShards != 0 {
		t.Fatalf("caller's config mutated: NumShards=%d, want 0", user.NumShards)
	}
	if want := numAutoPipelineShards(); got.NumShards != want {
		t.Fatalf("cluster default NumShards = %d, want %d", got.NumShards, want)
	}
	if !got.contentSharded {
		t.Fatalf("cluster default must mark contentSharded (slot routing preserves per-key order)")
	}

	// The DEFAULT config (MaxConcurrentBatches=1) must still get several
	// slot-routed shards — deriving the shard count from the permit budget
	// once collapsed cluster slot routing to a single shard at the default.
	def := clusterAutoPipelineConfig(DefaultAutoPipelineConfig())
	if def.NumShards < 2 {
		t.Fatalf("cluster default-config NumShards = %d, want >= 2 (slot routing must not be dead code at defaults)", def.NumShards)
	}

	explicit := &AutoPipelineConfig{MaxConcurrentBatches: 8, Unordered: true, NumShards: 3}
	if got := clusterAutoPipelineConfig(explicit); got != explicit || got.NumShards != 3 {
		t.Fatalf("explicit NumShards must pass through unchanged, got %+v", got)
	}
}
