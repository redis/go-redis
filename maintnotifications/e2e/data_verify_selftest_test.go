package e2e

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// Checks the data verifier against a plain Redis: it must pass on good data
// and fail on each kind of corruption, otherwise a green scenario would prove
// nothing. Skipped unless E2E_SCENARIO_TESTS and E2E_FAKE_FI_ENDPOINT are set
// (the same Redis URL the fake-fault-injector harness test uses).
func TestDataVerifierAgainstRedis(t *testing.T) {
	endpoint := os.Getenv("E2E_FAKE_FI_ENDPOINT")
	if os.Getenv("E2E_SCENARIO_TESTS") != "true" || endpoint == "" {
		t.Skip("set E2E_SCENARIO_TESTS and E2E_FAKE_FI_ENDPOINT to run")
	}
	opts, err := redis.ParseURL(endpoint)
	if err != nil {
		t.Fatal(err)
	}
	c := redis.NewClient(opts)
	defer c.Close()
	ctx := context.Background()

	newVerifier := func() *dataVerifier {
		d := newDataVerifier(fmt.Sprintf("e2e:selftest:%d", time.Now().UnixNano()))
		if err := d.seed(ctx, c); err != nil {
			t.Fatalf("seed: %v", err)
		}
		return d
	}

	t.Run("passes on good data", func(t *testing.T) {
		d := newVerifier()
		if err := d.verify(ctx, c); err != nil {
			t.Fatalf("verify on untouched data: %v", err)
		}
	})

	// Each corruption touches one command family and must be reported by it.
	corruptions := []struct {
		name   string
		mutate func(d *dataVerifier) error
		want   string
	}{
		{"string value", func(d *dataVerifier) error { return c.Set(ctx, d.key("str", 2), "wrong", 0).Err() }, "GET"},
		{"missing string", func(d *dataVerifier) error { return c.Del(ctx, d.key("str", 3)).Err() }, "GET"},
		{"lost expiry", func(d *dataVerifier) error { return c.Persist(ctx, d.key("str", 0)).Err() }, "TTL"},
		{"unexpected expiry", func(d *dataVerifier) error { return c.Expire(ctx, d.key("str", 2), time.Hour).Err() }, "TTL"},
		{"hash field", func(d *dataVerifier) error { return c.HSet(ctx, d.key("hash", 4), "a", "wrong").Err() }, "HGET"},
		{"extra hash field", func(d *dataVerifier) error { return c.HSet(ctx, d.key("hash", 4), "extra", "1").Err() }, "HGETALL"},
		{"mset group", func(d *dataVerifier) error { return c.Del(ctx, d.tagged(1, "b")).Err() }, "MGET"},
		{"counter", func(d *dataVerifier) error { return c.Incr(ctx, d.single("ctr")).Err() }, "counter"},
		{"set member", func(d *dataVerifier) error { return c.SRem(ctx, d.single("set"), "a").Err() }, "SMEMBERS"},
		{"list element", func(d *dataVerifier) error { return c.RPop(ctx, d.single("list")).Err() }, "LRANGE"},
		{"zset score", func(d *dataVerifier) error { return c.ZIncrBy(ctx, d.single("zset"), 5, "a").Err() }, "ZRANGE"},
		{"deleted key back", func(d *dataVerifier) error { return c.Set(ctx, d.single("gone"), "x", 0).Err() }, "EXISTS"},
	}
	for _, tc := range corruptions {
		t.Run("detects "+tc.name, func(t *testing.T) {
			d := newVerifier()
			if err := tc.mutate(d); err != nil {
				t.Fatalf("mutate: %v", err)
			}
			err := d.verify(ctx, c)
			if err == nil {
				t.Fatalf("verify passed on corrupted data (%s)", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error %q does not name %s", err, tc.want)
			}
		})
	}

	t.Run("live load keeps acknowledged writes", func(t *testing.T) {
		l := newLiveLoad(fmt.Sprintf("e2e:selftest:live:%d", time.Now().UnixNano()))
		for i := 0; i < 100; i++ {
			if err := l.step(ctx, c, i); err != nil {
				t.Fatalf("step %d: %v", i, err)
			}
		}
		if err := l.verify(ctx, c); err != nil {
			t.Fatalf("verify after clean run: %v", err)
		}
		// A write the server acknowledged but that was overwritten from outside
		// is the shape of a lost write: it must be reported.
		var victim string
		l.mu.Lock()
		for k := range l.acked {
			if !strings.HasSuffix(k, "#f") {
				victim = k
				break
			}
		}
		l.mu.Unlock()
		if err := c.Set(ctx, victim, "stolen", 0).Err(); err != nil {
			t.Fatal(err)
		}
		if err := l.verify(ctx, c); err == nil {
			t.Fatalf("verify passed although %s was overwritten", victim)
		}
	})
}
