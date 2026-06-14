package redis_test

import (
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestCSCStrategyDefaultIsBroadcast pins the public contract: the zero value of
// CSCStrategy selects the Broadcast strategy, so callers that enable CSC
// without choosing a strategy get the recommended one. This is a pure
// compile-time/value assertion and needs no server, hence a plain unit test.
func TestCSCStrategyDefaultIsBroadcast(t *testing.T) {
	var s redis.CSCStrategy
	if s != redis.CSCStrategyBroadcast {
		t.Fatalf("zero-value CSCStrategy = %d, want CSCStrategyBroadcast (%d)", s, redis.CSCStrategyBroadcast)
	}
}
