package redis

import "os"

// Test-only: GOREDIS_ROUTING_POISON=1 poisons released routing decisions
// instead of reusing them (not part of the patch).
func init() {
	if os.Getenv("GOREDIS_ROUTING_POISON") == "1" {
		routingDecisionPoison = true
	}
}
