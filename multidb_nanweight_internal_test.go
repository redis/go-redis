package redis

import (
	"math"
	"testing"

	imultidb "github.com/redis/go-redis/v9/internal/multidb"
)

// TestNaNWeightRejected pins that a NaN member weight is rejected on both the
// config path (validate) and the runtime API (setWeight). A stored NaN makes
// every ordered comparison in selection/auto-fallback false, degenerating
// priority to iteration order.
func TestNaNWeightRejected(t *testing.T) {
	cfg := MultiDBClientConfig{Options: &Options{Addr: "127.0.0.1:6379"}, Weight: math.NaN()}
	if err := cfg.validate(); err == nil {
		t.Fatal("validate accepted a NaN Weight")
	}

	core := newMultidbCore(&MultiDBOptions{})
	core.dbs[0] = &multidbDatabase{id: 0, weight: 1, cb: imultidb.NewCircuitBreaker(imultidb.CircuitBreakerConfig{})}
	if err := core.setWeight(0, math.NaN()); err == nil {
		t.Fatal("setWeight accepted a NaN weight")
	}
	if core.dbs[0].weight != 1 {
		t.Fatalf("NaN weight was stored (weight=%v), member selection is now poisoned", core.dbs[0].weight)
	}
}
