package e2e

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// Data checks shared by the topology-change scenarios. A fault or a topology
// change must not lose or corrupt data, whatever command wrote it, so the
// scenarios check a spread of commands instead of GET and SET alone.
//
// Everything here takes a redis.Cmdable so it runs on a plain client and on a
// cluster client. Multi-key commands use hash tags so their keys share a slot.

const dataTTL = time.Hour

// dataVerifier seeds a fixed data set before a fault and verifies it after.
// Nothing else writes these keys, so every value is exactly known.
type dataVerifier struct {
	prefix string
	n      int // keys per family
}

func newDataVerifier(prefix string) *dataVerifier {
	return &dataVerifier{prefix: prefix, n: 50}
}

func (d *dataVerifier) key(kind string, i int) string {
	return fmt.Sprintf("%s:%s:%d", d.prefix, kind, i)
}

// tagged returns a key in the slot of group, for MSET and MGET.
func (d *dataVerifier) tagged(group int, field string) string {
	return fmt.Sprintf("{%s:g%d}:%s", d.prefix, group, field)
}

func (d *dataVerifier) single(kind string) string { return fmt.Sprintf("%s:%s", d.prefix, kind) }

const taggedGroups = 5

// seed writes the data set: strings (with and without expiry), hashes, MSET
// groups, a counter, a set, a list, a sorted set, and a deleted key.
func (d *dataVerifier) seed(ctx context.Context, c redis.Cmdable) error {
	for i := 0; i < d.n; i++ {
		switch {
		case i%5 == 0:
			// SET with EX
			if err := c.Set(ctx, d.key("str", i), fmt.Sprintf("v%d", i), dataTTL).Err(); err != nil {
				return fmt.Errorf("SET EX: %w", err)
			}
		case i%5 == 1:
			// expiry added after the write, then removed again
			k := d.key("str", i)
			if err := c.Set(ctx, k, fmt.Sprintf("v%d", i), 0).Err(); err != nil {
				return fmt.Errorf("SET: %w", err)
			}
			if err := c.Expire(ctx, k, dataTTL).Err(); err != nil {
				return fmt.Errorf("EXPIRE: %w", err)
			}
			if err := c.Persist(ctx, k).Err(); err != nil {
				return fmt.Errorf("PERSIST: %w", err)
			}
		default:
			if err := c.Set(ctx, d.key("str", i), fmt.Sprintf("v%d", i), 0).Err(); err != nil {
				return fmt.Errorf("SET: %w", err)
			}
		}
		if err := c.HSet(ctx, d.key("hash", i), "a", i, "b", "x").Err(); err != nil {
			return fmt.Errorf("HSET: %w", err)
		}
	}
	for g := 0; g < taggedGroups; g++ {
		if err := c.MSet(ctx, d.tagged(g, "a"), "1", d.tagged(g, "b"), "2", d.tagged(g, "c"), "3").Err(); err != nil {
			return fmt.Errorf("MSET: %w", err)
		}
	}
	for k := 0; k < 10; k++ {
		if err := c.IncrBy(ctx, d.single("ctr"), 3).Err(); err != nil {
			return fmt.Errorf("INCRBY: %w", err)
		}
	}
	if err := c.DecrBy(ctx, d.single("ctr"), 5).Err(); err != nil {
		return fmt.Errorf("DECRBY: %w", err)
	}
	if err := c.SAdd(ctx, d.single("set"), "a", "b", "c").Err(); err != nil {
		return fmt.Errorf("SADD: %w", err)
	}
	if err := c.RPush(ctx, d.single("list"), "1", "2", "3").Err(); err != nil {
		return fmt.Errorf("RPUSH: %w", err)
	}
	if err := c.ZAdd(ctx, d.single("zset"), redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Err(); err != nil {
		return fmt.Errorf("ZADD: %w", err)
	}
	if err := c.Set(ctx, d.single("gone"), "x", 0).Err(); err != nil {
		return fmt.Errorf("SET gone: %w", err)
	}
	if err := c.Del(ctx, d.single("gone")).Err(); err != nil {
		return fmt.Errorf("DEL: %w", err)
	}
	return nil
}

// verify reads the data set back and returns every mismatch it found, joined.
func (d *dataVerifier) verify(ctx context.Context, c redis.Cmdable) error {
	var errs []error
	fail := func(format string, args ...any) {
		if len(errs) < 8 { // enough to diagnose, short enough to read
			errs = append(errs, fmt.Errorf(format, args...))
		}
	}

	for i := 0; i < d.n; i++ {
		k := d.key("str", i)
		want := fmt.Sprintf("v%d", i)
		if got, err := c.Get(ctx, k).Result(); err != nil || got != want {
			fail("GET %s = %q, %v; want %q", k, got, err, want)
		}
		ttl, err := c.TTL(ctx, k).Result()
		switch {
		case err != nil:
			fail("TTL %s: %v", k, err)
		case i%5 == 0 && (ttl <= 0 || ttl > dataTTL):
			fail("TTL %s = %v; want 0 < ttl <= %v", k, ttl, dataTTL)
		case i%5 != 0 && ttl != -1:
			fail("TTL %s = %v; want -1 (no expiry)", k, ttl)
		}

		h := d.key("hash", i)
		if got, err := c.HGet(ctx, h, "a").Result(); err != nil || got != fmt.Sprint(i) {
			fail("HGET %s a = %q, %v; want %d", h, got, err, i)
		}
		wantHash := map[string]string{"a": fmt.Sprint(i), "b": "x"}
		if got, err := c.HGetAll(ctx, h).Result(); err != nil || !reflect.DeepEqual(got, wantHash) {
			fail("HGETALL %s = %v, %v; want %v", h, got, err, wantHash)
		}
	}

	for g := 0; g < taggedGroups; g++ {
		keys := []string{d.tagged(g, "a"), d.tagged(g, "b"), d.tagged(g, "c")}
		got, err := c.MGet(ctx, keys...).Result()
		if want := []any{"1", "2", "3"}; err != nil || !reflect.DeepEqual(got, want) {
			fail("MGET %v = %v, %v; want %v", keys, got, err, want)
		}
	}

	if got, err := c.Get(ctx, d.single("ctr")).Int(); err != nil || got != 25 {
		fail("GET counter = %d, %v; want 25", got, err)
	}
	if got, err := c.SMembers(ctx, d.single("set")).Result(); err != nil {
		fail("SMEMBERS: %v", err)
	} else if sort.Strings(got); !reflect.DeepEqual(got, []string{"a", "b", "c"}) {
		fail("SMEMBERS = %v; want [a b c]", got)
	}
	if got, err := c.LRange(ctx, d.single("list"), 0, -1).Result(); err != nil || !reflect.DeepEqual(got, []string{"1", "2", "3"}) {
		fail("LRANGE = %v, %v; want [1 2 3]", got, err)
	}
	if got, err := c.ZRangeWithScores(ctx, d.single("zset"), 0, -1).Result(); err != nil {
		fail("ZRANGE: %v", err)
	} else {
		want := []redis.Z{{Score: 1, Member: "a"}, {Score: 2, Member: "b"}, {Score: 3, Member: "c"}}
		if !reflect.DeepEqual(got, want) {
			fail("ZRANGE = %v; want %v", got, want)
		}
	}
	if got, err := c.Exists(ctx, d.single("gone")).Result(); err != nil || got != 0 {
		fail("EXISTS deleted key = %d, %v; want 0", got, err)
	}
	return errors.Join(errs...)
}

// verifyEventually retries verify until it passes or timeout elapses, and
// returns the last error. Retrying only covers transient errors right after a
// recovery: a missing or wrong value does not heal by waiting.
func (d *dataVerifier) verifyEventually(ctx context.Context, c redis.Cmdable, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		vctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		err := d.verify(vctx, c)
		cancel()
		if err == nil || time.Now().After(deadline) {
			return err
		}
		time.Sleep(time.Second)
	}
}

// liveLoad is write traffic that keeps running across a fault, over a small
// set of keys that are overwritten many times. It remembers which writes were
// acknowledged, so afterwards an acknowledged write that was lost is caught.
// A write that failed with an error may or may not have been applied, so a
// failed value is accepted as well until a later write is acknowledged.
type liveLoad struct {
	prefix string
	keys   int

	mu    sync.Mutex
	acked map[string]string              // key -> last acknowledged value
	maybe map[string]map[string]struct{} // key -> values tried since that ack
}

func newLiveLoad(prefix string) *liveLoad {
	return &liveLoad{prefix: prefix, keys: 20, acked: map[string]string{}, maybe: map[string]map[string]struct{}{}}
}

// liveLoadStats is how a load run went. Failures during a fault are expected;
// they are reported, not asserted.
type liveLoadStats struct {
	OK, Failed     int64
	LongestFailure time.Duration // longest run of consecutive failed steps
}

// run drives step in a goroutine until the returned stop function is called.
// stop waits for the goroutine and reports whether it finished within 30s
// (false means a command is stuck) together with the stats.
func (l *liveLoad) run(ctx context.Context, c redis.Cmdable) (stop func() (liveLoadStats, bool)) {
	var quit atomic.Bool
	var wg sync.WaitGroup
	var mu sync.Mutex
	var stats liveLoadStats
	var failStart time.Time

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; !quit.Load(); i++ {
			sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := l.step(sctx, c, i)
			cancel()
			mu.Lock()
			if err != nil {
				stats.Failed++
				if failStart.IsZero() {
					failStart = time.Now()
				}
			} else {
				stats.OK++
				if !failStart.IsZero() {
					if d := time.Since(failStart); d > stats.LongestFailure {
						stats.LongestFailure = d
					}
					failStart = time.Time{}
				}
			}
			mu.Unlock()
			time.Sleep(20 * time.Millisecond)
		}
	}()

	return func() (liveLoadStats, bool) {
		quit.Store(true)
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		finished := true
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			finished = false
		}
		mu.Lock()
		defer mu.Unlock()
		if !failStart.IsZero() {
			// still failing when stopped: count that window too
			if d := time.Since(failStart); d > stats.LongestFailure {
				stats.LongestFailure = d
			}
		}
		return stats, finished
	}
}

func (l *liveLoad) record(key, val string, err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if err == nil {
		l.acked[key] = val
		delete(l.maybe, key)
		return
	}
	if l.maybe[key] == nil {
		l.maybe[key] = map[string]struct{}{}
	}
	l.maybe[key][val] = struct{}{}
}

// step runs one round of commands: SET and GET, HSET and HGET, MSET and MGET,
// INCR, and SET with EX and TTL. It returns the first error.
func (l *liveLoad) step(ctx context.Context, c redis.Cmdable, i int) error {
	val := fmt.Sprintf("iter%d", i)
	n := i % l.keys

	k := fmt.Sprintf("%s:live:str:%d", l.prefix, n)
	err := c.Set(ctx, k, val, 0).Err()
	l.record(k, val, err)
	if err != nil {
		return err
	}
	if err := c.Get(ctx, k).Err(); err != nil {
		return err
	}

	hk := fmt.Sprintf("%s:live:hash:%d", l.prefix, n)
	err = c.HSet(ctx, hk, "f", val).Err()
	l.record(hk+"#f", val, err)
	if err != nil {
		return err
	}
	if err := c.HGet(ctx, hk, "f").Err(); err != nil {
		return err
	}

	k1 := fmt.Sprintf("{%s:live:m%d}:1", l.prefix, n)
	k2 := fmt.Sprintf("{%s:live:m%d}:2", l.prefix, n)
	err = c.MSet(ctx, k1, val, k2, val).Err()
	l.record(k1, val, err)
	l.record(k2, val, err)
	if err != nil {
		return err
	}
	if err := c.MGet(ctx, k1, k2).Err(); err != nil {
		return err
	}

	if err := c.Incr(ctx, fmt.Sprintf("%s:live:ctr", l.prefix)).Err(); err != nil {
		return err
	}

	ek := fmt.Sprintf("%s:live:ttl:%d", l.prefix, n)
	err = c.Set(ctx, ek, val, dataTTL).Err()
	l.record(ek, val, err)
	if err != nil {
		return err
	}
	return c.TTL(ctx, ek).Err()
}

// verify checks that every key holds its last acknowledged value, or a value
// from a write that failed after it.
func (l *liveLoad) verify(ctx context.Context, c redis.Cmdable) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var errs []error
	for key, want := range l.acked {
		var got string
		var err error
		if hk, ok := cutHashField(key); ok {
			got, err = c.HGet(ctx, hk, "f").Result()
		} else {
			got, err = c.Get(ctx, key).Result()
		}
		if err != nil {
			errs = append(errs, fmt.Errorf("read %s: %w", key, err))
			continue
		}
		if got == want {
			continue
		}
		if _, ok := l.maybe[key][got]; ok {
			continue
		}
		if len(errs) < 8 {
			errs = append(errs, fmt.Errorf("%s = %q; want the acknowledged %q or a later attempt", key, got, want))
		}
	}
	return errors.Join(errs...)
}

// cutHashField splits "<hash>#f" keys recorded for HSET.
func cutHashField(key string) (string, bool) {
	const suffix = "#f"
	if len(key) > len(suffix) && key[len(key)-len(suffix):] == suffix {
		return key[:len(key)-len(suffix)], true
	}
	return "", false
}
