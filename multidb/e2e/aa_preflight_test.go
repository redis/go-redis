package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestAAEndpointsReachable checks, before the scenarios, that every real
// Active-Active endpoint answers PING with the suite's member options, and
// logs the cold (dial + handshake + PING) and warm PING times. A scenario
// that fails in NewMultiDBClient only reports "0 healthy"; this test shows
// the per-endpoint error and how much of the health-check budget a probe
// needs. The file name sorts first, so it runs before the scenarios.
func TestAAEndpointsReachable(t *testing.T) {
	if os.Getenv("FAULT_INJECTION_API_URL") == "" {
		t.Skip("real mode only")
	}
	for i, addr := range e2eTopology.Endpoints {
		opts := endpointOptions(addr)
		// A generous budget: this measures, it does not gate on timing.
		opts.DialTimeout = 5 * time.Second
		opts.ReadTimeout = 5 * time.Second
		c := redis.NewClient(opts)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		start := time.Now()
		coldErr := c.Ping(ctx).Err()
		cold := time.Since(start)
		start = time.Now()
		warmErr := c.Ping(ctx).Err()
		warm := time.Since(start)
		cancel()
		_ = c.Close()
		t.Logf("member %d %s: cold PING %v (err %v), warm PING %v (err %v); username set %v, password set %v",
			i, opts.Addr, cold, coldErr, warm, warmErr, opts.Username != "", opts.Password != "")
		if coldErr != nil || warmErr != nil {
			t.Errorf("member %d %s not reachable: cold %v, warm %v", i, opts.Addr, coldErr, warmErr)
		}
	}
}
