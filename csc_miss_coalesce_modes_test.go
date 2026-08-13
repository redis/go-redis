package redis

import (
	"context"
	"testing"
	"time"
)

// TestFullDuplexIdleInvalidation pins the feature-complete full-duplex engine's
// idle push drain, WITH a negative control.
//
// The engine holds one connection out of the pool, so the background CSC drainer
// never visits it; the session's reader must drain server-initiated pushes even
// when no reply is pending, or an invalidation arriving while the coalescer is
// idle is never processed and the entry serves stale until TTL.
//
//   - idle_drain_on_converges: with the drain at its default 5ms cadence, a key
//     mutated from a second client while the coalescer is idle must converge —
//     only possible if the idle reader processed the invalidation (a cache hit is
//     served locally without touching the socket).
//   - idle_drain_off_stays_stale: the NEGATIVE CONTROL. With the idle drain
//     effectively disabled (probe set to ~1h), the same sequence must NOT
//     converge. If it does, some other path drains the held connection and the
//     positive case would prove nothing.
func TestFullDuplexIdleInvalidation(t *testing.T) {
	probe := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	if err := probe.Ping(context.Background()).Err(); err != nil {
		probe.Close()
		t.Skipf("no redis: %v", err)
	}
	probe.Close()

	t.Run("idle_drain_on_converges", func(t *testing.T) {
		if !fdIdleConverges(t, "fdidle:on") {
			t.Fatal("with the idle drain ON, an invalidation on the held connection " +
				"was never processed: the entry served stale")
		}
	})

	t.Run("idle_drain_off_stays_stale", func(t *testing.T) {
		old := cscFullDuplexIdleProbe
		cscFullDuplexIdleProbe = time.Hour // idle drain effectively disabled
		t.Cleanup(func() { cscFullDuplexIdleProbe = old })
		if fdIdleConverges(t, "fdidle:off") {
			t.Fatal("NEGATIVE CONTROL FAILED: with the idle drain disabled the key still " +
				"converged, so something other than the idle reader drains the held " +
				"connection — the positive case does not prove the idle-drain fix")
		}
	})
}

// fdIdleConverges warms a key through the full-duplex coalescer, lets the
// coalescer go idle, mutates the key from a second client, and reports whether
// the caching client converges to the new value within ~0.8s.
func fdIdleConverges(t *testing.T, key string) bool {
	t.Helper()
	ctx := context.Background()

	cached := NewClient(&Options{
		Addr:                          internalTestRedisAddr(),
		Protocol:                      3,
		PoolSize:                      4,
		ClientSideCacheConfig:         &ClientSideCacheConfig{MaxEntries: 1000},
		ClientSideCacheCoalesceMisses: true,
		ClientSideCacheCoalesceMode:   "fullduplex",
	})
	seeder := NewClient(&Options{Addr: internalTestRedisAddr(), Protocol: 3})
	defer cached.Close()
	defer seeder.Close()
	if err := cached.Ping(ctx).Err(); err != nil {
		t.Fatalf("ping: %v", err)
	}
	if mode := cached.CSCMissCoalesceStats().Mode; mode != "fullduplex" {
		t.Fatalf("coalescer mode = %q; want fullduplex (config not honored?)", mode)
	}
	t.Cleanup(func() { seeder.Del(context.Background(), key) })

	if err := seeder.Set(ctx, key, "v1", 0).Err(); err != nil {
		t.Fatal(err)
	}
	// Warm through the coalescer: the miss is fetched on the held connection and
	// publishes the entry tracked on that connection.
	if got, err := cached.Get(ctx, key).Result(); err != nil || got != "v1" {
		t.Fatalf("warm read = %q, %v; want v1", got, err)
	}
	if got, err := cached.Get(ctx, key).Result(); err != nil || got != "v1" {
		t.Fatalf("second read = %q, %v; want v1 (should be a local hit)", got, err)
	}
	if cached.CSCMissCoalesceStats().Batches == 0 {
		t.Fatal("coalescer never batched; the warm read did not route through it")
	}

	// Go idle, then invalidate from the second client. The push arrives on the
	// held connection; only the session's reader can process it.
	time.Sleep(60 * time.Millisecond)
	if err := seeder.Set(ctx, key, "v2", 0).Err(); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(800 * time.Millisecond)
	for time.Now().Before(deadline) {
		if got, _ := cached.Get(ctx, key).Result(); got == "v2" {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}
