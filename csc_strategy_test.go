package redis_test

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestCSCStrategyDefaultIsSharedTracking pins that the zero value of CSCStrategy is
// SharedTracking (the default when CSC is enabled without choosing a strategy).
func TestCSCStrategyDefaultIsSharedTracking(t *testing.T) {
	var s redis.CSCStrategy
	if s != redis.CSCStrategySharedTracking {
		t.Fatalf("zero-value CSCStrategy = %d, want CSCStrategySharedTracking (%d)", s, redis.CSCStrategySharedTracking)
	}
}
