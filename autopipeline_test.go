// Code consolidated from per-topic autopipeline test files.
package redis_test

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/hashtag"
)

// ===== from autopipeline_accessor_test.go =====
// TestAutoPipelineSecondaryAccessors guards a regression where the deferred
// await() was injected only on the canonical Val()/Result() of each command
// type, leaving secondary value-accessors (Int, Bytes, Bool, Float, the generic
// Cmd type, SliceCmd.Val, map getters, ...) reading cmd.val before the batch
// executed. Each accessor below must block until its command runs and return the
// real value, not the zero value. Run under -race to catch the missing barrier.
func TestAutoPipelineSecondaryAccessors(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)
	// The deferred face is where secondary accessors must block until the
	// batch executes (the blocking face waits inside the call itself, so it
	// could never catch a missing await barrier).
	ap, err := c.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const G = 50
	var wg sync.WaitGroup
	var bad int64
	fail := func(cond bool) {
		if cond {
			atomic.AddInt64(&bad, 1)
		}
	}

	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(id int) {
			defer wg.Done()
			ik := fmt.Sprintf("acc:int:%d", id)
			sk := fmt.Sprintf("acc:str:%d", id)
			hk := fmt.Sprintf("acc:hash:%d", id)

			// Seed values via the autopipeline (these return *StatusCmd/*IntCmd).
			ap.Set(ctx, sk, "12345", 0)
			ap.Set(ctx, ik, "0", 0)
			ap.HSet(ctx, hk, "f1", "v1", "f2", "v2")

			// StringCmd secondary converters — previously missing await().
			n, err := ap.Get(ctx, sk).Int()
			fail(err != nil || n != 12345)
			i64, err := ap.Get(ctx, sk).Int64()
			fail(err != nil || i64 != 12345)
			b, err := ap.Get(ctx, sk).Bytes()
			fail(err != nil || string(b) != "12345")

			// IntCmd path + IntCmd.Uint64 (previously missing await()).
			incr := ap.Incr(ctx, ik)
			u, err := incr.Uint64()
			fail(err != nil || u != 1)

			// Generic Cmd via Do() — the whole *Cmd type was missing await().
			doCmd := ap.Do(ctx, "GET", sk)
			v, err := doCmd.Int()
			fail(err != nil || v != 12345)
			txt, err := ap.Do(ctx, "GET", sk).Text()
			fail(err != nil || txt != "12345")

			// SliceCmd.Val()/Result() — previously missing await().
			mget := ap.MGet(ctx, sk, ik)
			vals := mget.Val()
			fail(len(vals) != 2 || vals[0] != "12345")

			// MapStringStringCmd via HGetAll (Val()/Result() are covered, but
			// exercise it as the typical map path).
			m, err := ap.HGetAll(ctx, hk).Result()
			fail(err != nil || m["f1"] != "v1" || m["f2"] != "v2")
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("secondary accessors: %d wrong/zero reads (await regression)", bad)
	}
}

// ===== from autopipeline_backpressure_test.go =====
// TestAutoPipelineWindowedNoDeadlock submits a large window of commands without
// waiting between them (the async producer pattern), then reads them all. It must
// not deadlock and every future must resolve with its correct result — exercises
// the enqueue/permit/flush path under a producer that outruns per-command waits.
func TestAutoPipelineWindowedNoDeadlock(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 20})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 300, MaxConcurrentBatches: 8, Unordered: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const n = 5000
	futs := make([]redis.AutoFuture, n)
	for i := 0; i < n; i++ {
		futs[i] = ap.Submit(ctx, redis.NewCmd(ctx, "set", fmt.Sprintf("bp:%d", i), i))
	}
	for i := range futs {
		if err := futs[i].Wait(); err != nil {
			t.Fatalf("future %d: %v", i, err)
		}
	}
	// spot-check a few landed correctly
	for _, i := range []int{0, n / 2, n - 1} {
		if v, _ := client.Get(ctx, fmt.Sprintf("bp:%d", i)).Result(); v != fmt.Sprintf("%d", i) {
			t.Fatalf("bp:%d = %q, want %d", i, v, i)
		}
	}
	for i := 0; i < n; i++ {
		client.Del(ctx, fmt.Sprintf("bp:%d", i))
	}
}

// TestAutoPipelineSoak drives continuous autopipelined traffic from many
// goroutines for a few seconds and checks that goroutine and connection counts
// return to a stable baseline afterward (no slow leak) and results stay correct.
// Skipped under -short; bump `dur` for a longer soak.
func TestAutoPipelineSoak(t *testing.T) {
	if testing.Short() {
		t.Skip("soak skipped in -short")
	}
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 20})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize: 200, MaxConcurrentBatches: 16, Unordered: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	runtime.GC()
	time.Sleep(100 * time.Millisecond)
	baseGoroutines := runtime.NumGoroutine()

	const dur = 3 * time.Second
	deadline := time.Now().Add(dur)
	var wg sync.WaitGroup
	var mismatch int64
	var mu sync.Mutex
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			i := 0
			for time.Now().Before(deadline) {
				key := fmt.Sprintf("soak:%d:%d", id, i%16)
				val := fmt.Sprintf("%d", i)
				setF := ap.Set(ctx, key, val, 0)
				if err := setF.Err(); err != nil {
					mu.Lock()
					mismatch++
					mu.Unlock()
				}
				i++
			}
		}(g)
	}
	wg.Wait()

	if mismatch != 0 {
		t.Fatalf("%d errors during soak", mismatch)
	}
	runtime.GC()
	time.Sleep(300 * time.Millisecond)
	got := runtime.NumGoroutine()
	if got > baseGoroutines+16 {
		t.Fatalf("goroutine leak: baseline=%d after soak=%d", baseGoroutines, got)
	}
}

// ===== from autopipeline_blocking_test.go =====
var _ = Describe("AutoPipeline Blocking Commands", func() {
	ctx := context.Background()
	var client *redis.Client
	var ap *redis.AutoPipeliner

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		var err error
		ap, err = client.AutoPipeline()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if ap != nil {
			Expect(ap.Close()).NotTo(HaveOccurred())
		}
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should not autopipeline blocking commands", func() {
		// Push a value to the list
		Expect(client.RPush(ctx, "list", "value").Err()).NotTo(HaveOccurred())

		// BLPOP should execute immediately without autopipelining.
		// ap.Do returns a generic *redis.Cmd; use its typed accessors.
		start := time.Now()
		result := ap.Do(ctx, "BLPOP", "list", "1")
		val, err := result.StringSlice()
		elapsed := time.Since(start)

		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal([]string{"list", "value"}))
		// Should complete quickly since value is available
		Expect(elapsed).To(BeNumerically("<", 100*time.Millisecond))
	})

	It("should mix blocking and non-blocking commands", func() {
		// Push values
		Expect(client.RPush(ctx, "list3", "a", "b", "c").Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "key1", "value1", 0).Err()).NotTo(HaveOccurred())

		// Mix blocking and non-blocking commands
		blpopCmd := ap.Do(ctx, "BLPOP", "list3", "1")
		getCmd := ap.Do(ctx, "GET", "key1")
		brpopCmd := ap.Do(ctx, "BRPOP", "list3", "1")

		// Get results — ap.Do returns generic *redis.Cmd; use typed accessors.
		blpopVal, err := blpopCmd.StringSlice()
		Expect(err).NotTo(HaveOccurred())
		Expect(blpopVal).To(Equal([]string{"list3", "a"}))

		getVal, err := getCmd.Text()
		Expect(err).NotTo(HaveOccurred())
		Expect(getVal).To(Equal("value1"))

		brpopVal, err := brpopCmd.StringSlice()
		Expect(err).NotTo(HaveOccurred())
		Expect(brpopVal).To(Equal([]string{"list3", "c"}))
	})
})

// ===== from autopipeline_buffer_test.go =====
// Autopipeline + zero-copy GetToBuffer/SetFromBuffer, standalone, concurrent.
func TestAPZeroCopyBuffer(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:                ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{MaxBatchSize: 100, MaxConcurrentBatches: 30, Unordered: true},
	})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const g = 100
	const perG = 30
	var wg sync.WaitGroup
	var bad int64
	var mu sync.Mutex
	var errs []string
	wg.Add(g)
	for x := 0; x < g; x++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("buf:%d:%d", id, i)
				val := fmt.Sprintf("payload-%d-%d", id, i)
				// write from buffer via autopipeline
				if err := ap.SetFromBuffer(ctx, key, []byte(val)).Err(); err != nil {
					atomic.AddInt64(&bad, 1)
					mu.Lock()
					if len(errs) < 10 {
						errs = append(errs, "set:"+err.Error())
					}
					mu.Unlock()
					continue
				}
				// read into caller buffer via autopipeline
				rbuf := make([]byte, len(val))
				cmd := ap.GetToBuffer(ctx, key, rbuf)
				n, err := cmd.Result()
				if err != nil {
					atomic.AddInt64(&bad, 1)
					mu.Lock()
					if len(errs) < 10 {
						errs = append(errs, "get:"+err.Error())
					}
					mu.Unlock()
					continue
				}
				got := string(cmd.Bytes())
				if n != len(val) || got != val {
					atomic.AddInt64(&bad, 1)
					mu.Lock()
					if len(errs) < 10 {
						errs = append(errs, fmt.Sprintf("key %s n=%d got=%q want=%q", key, n, got, val))
					}
					mu.Unlock()
				}
			}
		}(x)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("zero-copy buffer via autopipeline failed: %d cases; sample=%v", bad, errs)
	}
}

// ===== from autopipeline_close_race_test.go =====
// TestAutoPipelineCloseRace stresses concurrent command submission racing with
// Close(). The hazard: a submit that reads closed==false, then stalls, then
// enqueues onto a shard whose flusher has already drained and exited during
// shutdown — leaving the command's completion signal never fired, so the
// caller's blocking accessor hangs forever. Every submitted command must
// resolve (success or ErrClosed), never block past the deadline.
//
// Best-effort: this is a stress test, not a deterministic reproducer. The exact
// interleaving (a submit preempted between the closed-check and the enqueue for
// the whole duration of Close) is narrow and may not trigger every run; the fix
// (re-checking closed under the shard lock) is justified by code analysis. The
// test guards against regressions of the obvious form and runs many iterations
// to widen the window.
func TestAutoPipelineCloseRace(t *testing.T) {
	for iter := 0; iter < 50; iter++ {
		ctx := context.Background()
		c := redis.NewClient(&redis.Options{Addr: ":6379"})
		ap, err := c.AutoPipeline()
		if err != nil {
			t.Fatal(err)
		}

		const G = 64
		var wg sync.WaitGroup
		wg.Add(G)
		start := make(chan struct{})
		for g := 0; g < G; g++ {
			go func() {
				defer wg.Done()
				<-start
				for i := 0; i < 20; i++ {
					// .Err() blocks until the command resolves. If the command
					// is lost (never signalled), this never returns and the
					// watchdog below fires.
					_ = ap.Set(ctx, "closerace", i, 0).Err()
				}
			}()
		}

		close(start)
		// Close concurrently with in-flight submits — the race window.
		time.Sleep(time.Millisecond)
		_ = ap.Close()

		// Watchdog: all submitters must finish promptly. A hung await() means a
		// lost command (the bug this guards).
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("iter %d: submitters hung after Close — a command was lost", iter)
		}
		c.Close()
	}
}

// ===== from autopipeline_close_wiring_test.go =====
// TestClientCloseClosesAutoPipeliner verifies that closing the client also
// closes the shared AutoPipeliner, so its background flusher goroutines don't
// outlive the client. IsClosed() is the direct signal — checking it (rather than
// a downstream command error, which could just reflect the closed pool) proves
// Client.Close stopped the pipeliner itself. Without the wiring in Client.Close
// the autopipeliner would report open after the client is gone.
func TestClientCloseClosesAutoPipeliner(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	c.FlushDB(ctx)

	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Set(ctx, "cw", "1", 0).Err(); err != nil {
		t.Fatalf("set via autopipeline before close: %v", err)
	}
	if ap.IsClosed() {
		t.Fatal("autopipeliner reported closed before client.Close")
	}

	if err := c.Close(); err != nil {
		t.Fatalf("client close: %v", err)
	}
	if !ap.IsClosed() {
		t.Fatal("client.Close did not close the shared autopipeliner")
	}

	// A second client Close must not panic; like the base client it reports the
	// already-closed error (the autopipeliner part is idempotent and nil here).
	if err := c.Close(); err != nil && err != redis.ErrClosed {
		t.Fatalf("second client close: unexpected error %v", err)
	}
}

// ===== from autopipeline_cmdable_test.go =====
var _ = Describe("AutoPipeline Cmdable Interface", func() {
	ctx := context.Background()
	var client *redis.Client
	var ap *redis.AutoPipeliner

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: redisAddr,
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		var err error
		ap, err = client.AutoPipeline()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if ap != nil {
			Expect(ap.Close()).NotTo(HaveOccurred())
		}
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should support string commands", func() {
		// Use autopipeline like a regular client
		setCmd := ap.Set(ctx, "key1", "value1", 0)
		getCmd := ap.Get(ctx, "key1")
		incrCmd := ap.Incr(ctx, "counter")
		decrCmd := ap.Decr(ctx, "counter")

		// Get results
		Expect(setCmd.Err()).NotTo(HaveOccurred())
		Expect(setCmd.Val()).To(Equal("OK"))

		val, err := getCmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("value1"))

		Expect(incrCmd.Val()).To(Equal(int64(1)))
		Expect(decrCmd.Val()).To(Equal(int64(0)))
	})

	It("should support hash commands", func() {
		// Use hash commands
		hsetCmd := ap.HSet(ctx, "hash1", "field1", "value1", "field2", "value2")
		hgetCmd := ap.HGet(ctx, "hash1", "field1")
		hgetallCmd := ap.HGetAll(ctx, "hash1")

		// Get results
		Expect(hsetCmd.Val()).To(Equal(int64(2)))
		Expect(hgetCmd.Val()).To(Equal("value1"))
		Expect(hgetallCmd.Val()).To(Equal(map[string]string{
			"field1": "value1",
			"field2": "value2",
		}))
	})

	It("should support list commands", func() {
		// Use list commands
		rpushCmd := ap.RPush(ctx, "list1", "a", "b", "c")
		lrangeCmd := ap.LRange(ctx, "list1", 0, -1)
		lpopCmd := ap.LPop(ctx, "list1")

		// Get results
		Expect(rpushCmd.Val()).To(Equal(int64(3)))
		Expect(lrangeCmd.Val()).To(Equal([]string{"a", "b", "c"}))
		Expect(lpopCmd.Val()).To(Equal("a"))
	})

	It("should support set commands", func() {
		// Use set commands
		saddCmd := ap.SAdd(ctx, "set1", "member1", "member2", "member3")
		smembersCmd := ap.SMembers(ctx, "set1")
		sismemberCmd := ap.SIsMember(ctx, "set1", "member1")

		// Get results
		Expect(saddCmd.Val()).To(Equal(int64(3)))
		Expect(smembersCmd.Val()).To(ConsistOf("member1", "member2", "member3"))
		Expect(sismemberCmd.Val()).To(BeTrue())
	})

	It("should support sorted set commands", func() {
		// Use sorted set commands
		zaddCmd := ap.ZAdd(
			ctx, "zset1",
			redis.Z{Score: 1, Member: "one"},
			redis.Z{Score: 2, Member: "two"},
			redis.Z{Score: 3, Member: "three"},
		)
		zrangeCmd := ap.ZRange(ctx, "zset1", 0, -1)
		zscoreCmd := ap.ZScore(ctx, "zset1", "two")

		// Get results
		Expect(zaddCmd.Val()).To(Equal(int64(3)))
		Expect(zrangeCmd.Val()).To(Equal([]string{"one", "two", "three"}))
		Expect(zscoreCmd.Val()).To(Equal(float64(2)))
	})

	It("should support generic commands", func() {
		// Set some keys
		ap.Set(ctx, "key1", "value1", 0)
		ap.Set(ctx, "key2", "value2", 0)
		ap.Set(ctx, "key3", "value3", 0)

		// Use generic commands
		existsCmd := ap.Exists(ctx, "key1", "key2", "key3")
		delCmd := ap.Del(ctx, "key1")
		ttlCmd := ap.TTL(ctx, "key2")

		// Get results
		Expect(existsCmd.Val()).To(Equal(int64(3)))
		Expect(delCmd.Val()).To(Equal(int64(1)))
		Expect(ttlCmd.Val()).To(Equal(time.Duration(-1))) // No expiration
	})

	It("should support Do method for custom commands", func() {
		// Use Do for custom commands
		setCmd := ap.Do(ctx, "SET", "custom_key", "custom_value")
		getCmd := ap.Do(ctx, "GET", "custom_key")

		// Get results
		setVal, err := setCmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(setVal).To(Equal("OK"))

		getVal, err := getCmd.Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(getVal).To(Equal("custom_value"))
	})

	It("should support Pipeline method", func() {
		// Get a traditional pipeline from autopipeliner
		pipe := ap.Pipeline()
		Expect(pipe).NotTo(BeNil())

		// Use the pipeline
		pipe.Set(ctx, "pipe_key", "pipe_value", 0)
		pipe.Get(ctx, "pipe_key")

		cmds, err := pipe.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(2))
	})

	It("should support Pipelined method", func() {
		// Use Pipelined for convenience
		cmds, err := ap.Pipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "pipelined_key", "pipelined_value", 0)
			pipe.Get(ctx, "pipelined_key")
			return nil
		})

		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(2))
		Expect(cmds[0].(*redis.StatusCmd).Val()).To(Equal("OK"))
		Expect(cmds[1].(*redis.StringCmd).Val()).To(Equal("pipelined_value"))
	})

	It("should mix autopipelined and direct commands", func() {
		// Use autopipeline commands
		ap.Set(ctx, "ap_key1", "ap_value1", 0)
		ap.Set(ctx, "ap_key2", "ap_value2", 0)

		// Use traditional pipeline
		pipe := ap.Pipeline()
		pipe.Set(ctx, "pipe_key1", "pipe_value1", 0)
		pipe.Set(ctx, "pipe_key2", "pipe_value2", 0)
		_, err := pipe.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())

		// Verify all keys exist
		val1, _ := ap.Get(ctx, "ap_key1").Result()
		val2, _ := ap.Get(ctx, "ap_key2").Result()
		val3, _ := ap.Get(ctx, "pipe_key1").Result()
		val4, _ := ap.Get(ctx, "pipe_key2").Result()

		Expect(val1).To(Equal("ap_value1"))
		Expect(val2).To(Equal("ap_value2"))
		Expect(val3).To(Equal("pipe_value1"))
		Expect(val4).To(Equal("pipe_value2"))
	})
})

// ===== from autopipeline_config_test.go =====
// TestAutoPipelineOptionsNotMutated guards a regression where the autopipeliner
// filled zero-value defaults (MaxBatchSize, MaxConcurrentBatches) directly into
// the caller's *AutoPipelineOptions. A config shared across clients (or inspected
// later by the caller) must not be mutated by creating an AutoPipeliner.
func TestAutoPipelineOptionsNotMutated(t *testing.T) {
	cfg := &redis.AutoPipelineOptions{} // all zero -> defaults applied internally

	c := redis.NewClient(&redis.Options{Addr: ":6379", AutoPipelineOptions: cfg})
	defer c.Close()
	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	if cfg.MaxBatchSize != 0 {
		t.Fatalf("caller config MaxBatchSize mutated to %d (want 0)", cfg.MaxBatchSize)
	}
	if cfg.MaxConcurrentBatches != 0 {
		t.Fatalf("caller config MaxConcurrentBatches mutated to %d (want 0)", cfg.MaxConcurrentBatches)
	}

	// The same config reused for a second client must still be pristine and
	// usable (defaults applied to an internal copy, not the shared struct).
	c2 := redis.NewClient(&redis.Options{Addr: ":6379", AutoPipelineOptions: cfg})
	defer c2.Close()
	ap2, err := c2.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap2.Close()
	if cfg.MaxBatchSize != 0 || cfg.MaxConcurrentBatches != 0 {
		t.Fatal("shared config mutated after second client created an AutoPipeliner")
	}
}

// ===== from autopipeline_conn_state_test.go =====
// TestAutoPipelineBlockingCommandIsolation verifies a blocking command (detected
// via readTimeout at the submit gate, autopipeline.go:679) runs outside the
// pipeline and does NOT corrupt batched commands interleaved around it: the
// batched SET/GET before and after the BLPOP all return their correct replies.
func TestAutoPipelineBlockingCommandIsolation(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	client.Del(ctx, "cs:list", "cs:a", "cs:b")
	if err := client.RPush(ctx, "cs:list", "item").Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline() // blocking face
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	if v := ap.Set(ctx, "cs:a", "1", 0).Val(); v != "OK" {
		t.Fatalf("set a before blocking: %q", v)
	}
	// Blocking command interleaved with batched work.
	blp, err := ap.BLPop(ctx, time.Second, "cs:list").Result()
	if err != nil {
		t.Fatalf("blpop: %v", err)
	}
	if len(blp) != 2 || blp[1] != "item" {
		t.Fatalf("blpop = %v, want [cs:list item]", blp)
	}
	// Batched work after a blocking command still works and stays uncorrupted.
	if v := ap.Set(ctx, "cs:b", "2", 0).Val(); v != "OK" {
		t.Fatalf("set b after blocking: %q", v)
	}
	if v := ap.Get(ctx, "cs:a").Val(); v != "1" {
		t.Fatalf("get a after blocking: %q, want 1", v)
	}
	if v := ap.Get(ctx, "cs:b").Val(); v != "2" {
		t.Fatalf("get b after blocking: %q, want 2", v)
	}
	client.Del(ctx, "cs:a", "cs:b")
}

// TestAutoPipelineDoubleClose verifies Close is idempotent — a second Close does
// not panic (it may return an error, but must not crash or hang).
func TestAutoPipelineDoubleClose(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("first close: %v", err)
	}
	// Second close must not panic; ErrClosed or nil are both acceptable.
	_ = ap.Close()
}

// ===== from autopipeline_context_test.go =====
// AutoPipeline context contract (previously untested; WaitContext had zero
// coverage). The documented contract:
//   - a caller's context is NOT honored once a command is enqueued (execution
//     runs on context.Background); it only gates the wait via WaitContext.
//   - WaitContext abandons the wait on ctx done, but the command still executes
//     and its result becomes readable via a later Wait.
//   - blocking commands (readTimeout != nil) run directly with the caller ctx
//     and DO honor it.

// AP-C1: a caller ctx cancelled after enqueue does not cancel the command; the
// result is still produced.
func TestAPContextCancelDoesNotCancelExecution(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	cctx, cancel := context.WithCancel(ctx)
	f := ap.Set(cctx, "apctx:k", "v", 0)
	cancel() // cancel after submit, before the batch flushes/completes

	if err := f.Err(); err != nil {
		t.Fatalf("command should still execute after caller ctx cancel, got %v", err)
	}
	if f.Val() != "OK" {
		t.Fatalf("val = %q, want OK", f.Val())
	}
	if v, _ := client.Get(ctx, "apctx:k").Result(); v != "v" {
		t.Fatalf("value not persisted: %q", v)
	}
	client.Del(ctx, "apctx:k")
}

// AP-C2: WaitContext returns ctx.Err while the batch is in flight, and a later
// Wait returns the real result.
func TestAPWaitContextAbandonsWaitOnly(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	// Hold the batch open long enough that the WaitContext deadline fires first.
	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{MaxBatchSize: 300, MaxFlushDelay: 500 * time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	f := ap.Submit(ctx, redis.NewCmd(ctx, "set", "apctx:wc", "v"))

	wctx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
	defer cancel()
	if err := f.WaitContext(wctx); err != context.DeadlineExceeded {
		t.Fatalf("WaitContext err = %v, want DeadlineExceeded (batch held open)", err)
	}
	// The command still completes; a plain Wait returns the real result.
	if err := f.Wait(); err != nil {
		t.Fatalf("Wait after WaitContext timeout: %v", err)
	}
	client.Del(ctx, "apctx:wc")
}

// AP-C3: WaitContext returns the real result when the batch completes first.
func TestAPWaitContextReturnsResult(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	f := ap.Submit(ctx, redis.NewCmd(ctx, "set", "apctx:ok", "v"))
	if err := f.WaitContext(ctx); err != nil {
		t.Fatalf("WaitContext: %v", err)
	}
	client.Del(ctx, "apctx:ok")
}

// AP-C4: a blocking command runs directly with the caller ctx (not batched on
// Background). With ContextTimeoutEnabled the ctx deadline reaches the read, so a
// BLPOP is cut short by the ctx — proving the caller ctx flows to the blocking
// path (unlike batched commands, which execute on Background).
func TestAPBlockingCommandHonorsContext(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", ContextTimeoutEnabled: true})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AutoPipeline() // blocking face
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()
	client.Del(ctx, "apctx:blk")

	bctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	start := time.Now()
	// BLPOP on an empty list blocks; the ctx deadline must cut it short.
	err = ap.BLPop(bctx, 5*time.Second, "apctx:blk").Err()
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected BLPOP to fail on ctx deadline")
	}
	if elapsed > 2*time.Second {
		t.Fatalf("BLPOP did not honor ctx deadline (took %v)", elapsed)
	}
}

// AP-C5: submitting after Close returns ErrClosed.
func TestAPSubmitAfterCloseErrClosed(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	ap, err := client.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	if err := ap.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
	f := ap.Set(ctx, "apctx:closed", "v", 0)
	if err := f.Err(); err != redis.ErrClosed {
		t.Fatalf("submit after close err = %v, want ErrClosed", err)
	}
}

// AP-C6: WaitContext on a zero AutoFuture returns an error, not a panic.
func TestAPZeroFutureWaitContext(t *testing.T) {
	var f redis.AutoFuture
	if err := f.WaitContext(context.Background()); err == nil {
		t.Fatal("zero AutoFuture WaitContext should return an error")
	}
}

// ===== from autopipeline_correctness_test.go =====
// Validates: each command gets its OWN result (no cross-talk), under heavy
// concurrency. Each goroutine writes a unique value, reads it back, asserts it
// got exactly what it wrote.
func TestAPNoCrossTalk(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:                ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{MaxBatchSize: 200, MaxConcurrentBatches: 50, Unordered: true},
	})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 200
	const perG = 50
	var wg sync.WaitGroup
	var mismatches int64
	var mu sync.Mutex
	errs := []string{}

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("ct:%d:%d", id, i)
				want := fmt.Sprintf("v-%d-%d", id, i)
				// SET then GET via autopipeline; assert GET returns our value.
				if err := ap.Set(ctx, key, want, 0).Err(); err != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("set %s: %v", key, err))
					mu.Unlock()
					continue
				}
				got, err := ap.Get(ctx, key).Result()
				if err != nil {
					mu.Lock()
					errs = append(errs, fmt.Sprintf("get %s: %v", key, err))
					mu.Unlock()
					continue
				}
				if got != want {
					mu.Lock()
					mismatches++
					if len(errs) < 20 {
						errs = append(errs, fmt.Sprintf("key %s: got %q want %q", key, got, want))
					}
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()
	if mismatches > 0 || len(errs) > 0 {
		t.Fatalf("cross-talk detected: %d mismatches, errs=%v", mismatches, errs)
	}
}

// Validates: per-goroutine ordering. Each goroutine does INCR on its own key N
// times; final value must equal N and intermediate results must be strictly
// 1,2,3,... in call order (blocking Do guarantees this only if results map to
// the right command).
func TestAPPerGoroutineOrder(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:                ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{MaxBatchSize: 100, MaxConcurrentBatches: 30, Unordered: true},
	})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 100
	const incrs = 100
	var wg sync.WaitGroup
	var bad int64
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("ord:%d", id)
			for i := 1; i <= incrs; i++ {
				v, err := ap.Incr(ctx, key).Result()
				if err != nil || v != int64(i) {
					atomicAdd(&bad)
				}
			}
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("ordering violated on %d INCR results (expected strict 1..N per goroutine)", bad)
	}
}

// Validates: no lost commands. Issue exactly N writes across goroutines, then
// count keys; must be exactly N.
func TestAPNoLostCommands(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:                ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{MaxBatchSize: 256, MaxConcurrentBatches: 64, Unordered: true},
	})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 300
	const perG = 100
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				ap.Set(ctx, fmt.Sprintf("lost:%d:%d", id, i), "1", 0)
			}
		}(g)
	}
	wg.Wait()

	n, err := c.DBSize(ctx).Result()
	if err != nil {
		t.Fatal(err)
	}
	want := int64(goroutines * perG)
	if n != want {
		t.Fatalf("lost commands: DBSize=%d want=%d (%d missing)", n, want, want-n)
	}
}

// Validates: error isolation. A WRONGTYPE error on one command must not
// corrupt sibling commands sharing the same batch.
func TestAPErrorIsolation(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{
		Addr:                ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{MaxBatchSize: 64, MaxConcurrentBatches: 16, Unordered: true},
	})
	defer c.Close()
	c.FlushDB(ctx)
	// Seed a list key so INCR on it errors with WRONGTYPE.
	c.RPush(ctx, "alist", "x")
	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 50
	var wg sync.WaitGroup
	var goodFail, badNeighbor int64
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			// Good neighbor command (own key)
			ok := fmt.Sprintf("iso:%d", id)
			// Interleave a guaranteed-error command (INCR on a list) with a good one.
			errCmd := ap.Incr(ctx, "alist")
			goodCmd := ap.Set(ctx, ok, strconv.Itoa(id), 0)
			if errCmd.Err() == nil {
				atomicAdd(&badNeighbor) // the bad command should have errored
			} else {
				atomicAdd(&goodFail)
			}
			if goodCmd.Err() != nil {
				atomicAdd(&badNeighbor) // the good command must NOT inherit the error
			}
		}(g)
	}
	wg.Wait()
	if badNeighbor > 0 {
		t.Fatalf("error isolation broken: %d cases (good cmd errored or bad cmd succeeded)", badNeighbor)
	}
	if goodFail != goroutines {
		t.Fatalf("expected all %d INCR-on-list to error, got %d", goroutines, goodFail)
	}
}

func atomicAdd(p *int64) {
	atomic.AddInt64(p, 1)
}

// ===== from autopipeline_faces_split_test.go =====
// TestBlockingFaceExecutesOnCall: AutoPipeline() command blocks until executed —
// the returned cmd already holds its result without an explicit further wait.
func TestBlockingFaceExecutesOnCall(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AutoPipeline() // blocking face
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const G, N = 50, 100
	var wg sync.WaitGroup
	var bad int
	var mu sync.Mutex
	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(id int) {
			defer wg.Done()
			key := func() string { return "blk:" + string(rune('a'+id%26)) }()
			_ = key
			for i := 1; i <= N; i++ {
				// Incr blocks until executed; per-goroutine order must hold.
				cmd := ap.Incr(ctx, "blkkey:"+string(rune(id)))
				if v, err := cmd.Result(); err != nil || v != int64(i) {
					mu.Lock()
					bad++
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("blocking face: %d ordering/exec failures", bad)
	}
}

// TestAsyncFaceDeferred: AsyncAutoPipeline() command returns immediately; result
// is read later. Windowed submit then read works.
func TestAsyncFaceDeferred(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)
	ap, err := c.AsyncAutoPipeline() // deferred face, ordered default
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const N = 500
	cmds := make([]*redis.IntCmd, 0, N)
	for i := 0; i < N; i++ {
		cmds = append(cmds, ap.Incr(ctx, "asynckey")) // does not block
	}
	for i, cmd := range cmds {
		if v, err := cmd.Result(); err != nil || v != int64(i+1) {
			t.Fatalf("async windowed: cmd %d got v=%d err=%v", i, v, err)
		}
	}
}

// ===== from autopipeline_faces_test.go =====
// TestFutureFaceTyped verifies the future face exposes the typed command
// surface returning the usual *XxxCmd (same shape as the blocking client),
// with the result deferred until read. No cross-talk across goroutines.
func TestFutureFaceTyped(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)
	fap, err := c.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer fap.Close()

	const G, N = 100, 50
	var wg sync.WaitGroup
	var bad int64
	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(id int) {
			defer wg.Done()
			sets := make([]*redis.StatusCmd, N)
			for i := 0; i < N; i++ {
				sets[i] = fap.Set(ctx, fmt.Sprintf("ff:%d:%d", id, i), fmt.Sprintf("v%d-%d", id, i), 0)
			}
			for _, s := range sets {
				if s.Err() != nil {
					atomic.AddInt64(&bad, 1)
				}
			}
			for i := 0; i < N; i++ {
				want := fmt.Sprintf("v%d-%d", id, i)
				if v, _ := fap.Get(ctx, fmt.Sprintf("ff:%d:%d", id, i)).Result(); v != want {
					atomic.AddInt64(&bad, 1)
				}
			}
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("future face: %d failures", bad)
	}
}

// TestOrderedModeWindowed verifies that MaxConcurrentBatches=1 gives a single
// ordered command stream: a windowed caller (submit many, read later) sees
// strict per-key ordering even though it never blocks between submits.
func TestOrderedModeWindowed(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)
	// Deferred face: submits genuinely don't block, so the windowed claim is
	// real (on the blocking face every Incr would wait, proving nothing).
	fap, err := c.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{MaxBatchSize: 500, MaxConcurrentBatches: 1})
	if err != nil {
		t.Fatal(err)
	}
	defer fap.Close()

	const G, N = 80, 150
	var wg sync.WaitGroup
	var bad int64
	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("om:%d", id)
			cs := make([]*redis.IntCmd, 0, N)
			for i := 0; i < N; i++ {
				cs = append(cs, fap.Incr(ctx, key)) // windowed: no read between submits
			}
			for i, cc := range cs {
				if v, err := cc.Result(); err != nil || v != int64(i+1) {
					atomic.AddInt64(&bad, 1)
				}
			}
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("ordered windowed: %d ordering violations", bad)
	}
}

// TestAutoPipelineOptionsValidate verifies the ordering-safety rule: parallel
// batches (MaxConcurrentBatches>1) require an explicit Unordered:true opt-out.
func TestAutoPipelineOptionsValidate(t *testing.T) {
	// conc>1 without Unordered -> error.
	if err := (&redis.AutoPipelineOptions{MaxConcurrentBatches: 8}).Validate(); err == nil {
		t.Fatal("expected error for MaxConcurrentBatches>1 without Unordered")
	}
	// conc>1 with Unordered -> ok.
	if err := (&redis.AutoPipelineOptions{MaxConcurrentBatches: 8, Unordered: true}).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// ordered single stream -> ok.
	if err := (&redis.AutoPipelineOptions{MaxConcurrentBatches: 1}).Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	// default is ordered and valid.
	if err := redis.DefaultAutoPipelineOptions().Validate(); err != nil {
		t.Fatalf("default config invalid: %v", err)
	}
	// negative values are rejected so a typo surfaces at construction.
	if err := (&redis.AutoPipelineOptions{MaxBatchSize: -1}).Validate(); err == nil {
		t.Fatal("expected error for negative MaxBatchSize")
	}
	if err := (&redis.AutoPipelineOptions{MaxConcurrentBatches: -1}).Validate(); err == nil {
		t.Fatal("expected error for negative MaxConcurrentBatches")
	}
	if err := (&redis.AutoPipelineOptions{MaxFlushDelay: -1}).Validate(); err == nil {
		t.Fatal("expected error for negative MaxFlushDelay")
	}
	// zero values are allowed (mean "use default" / "no delay").
	if err := (&redis.AutoPipelineOptions{}).Validate(); err != nil {
		t.Fatalf("zero-value config should be valid: %v", err)
	}
}

// TestAutoPipelineErrorsOnUnsafeConfig verifies the unsafe config is rejected
// with an error (not a panic): AsyncAutoPipeline runs from a post-init call, so a
// bad config surfaces as a returned error the caller can handle.
func TestAutoPipelineErrorsOnUnsafeConfig(t *testing.T) {
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{MaxConcurrentBatches: 8})
	if err == nil {
		t.Fatal("expected error for MaxConcurrentBatches>1 without Unordered")
	}
	if ap != nil {
		t.Fatal("expected nil AutoPipeliner on config error")
	}
}

// ===== from autopipeline_fuzz_test.go =====
// TestAutoPipelineDifferentialFuzz runs the same randomized, type-diverse command
// stream through a plain client and through an AutoPipeliner (blocking face, so
// per-call order matches the sequential plain run) and diffs every result. This
// is the highest-leverage correctness net for a batching layer: it re-covers
// cross-talk, ordering, and reply-shape demux across arbitrary command mixes,
// including nil/empty/large/binary values. Both types satisfy redis.Cmdable, so
// the exact same op sequence drives both.
func TestAutoPipelineDifferentialFuzz(t *testing.T) {
	ctx := context.Background()
	// Dedicated DB so FlushDB between runs can't touch anything else.
	client := redis.NewClient(&redis.Options{Addr: ":6379", DB: 15})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	for _, seed := range []int64{1, 7, 42, 1337} {
		ops := genOps(rand.New(rand.NewSource(seed)), 400)

		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		plainRes := runOps(ctx, client, ops)

		if err := client.FlushDB(ctx).Err(); err != nil {
			t.Fatal(err)
		}
		ap, err := client.AutoPipeline()
		if err != nil {
			t.Fatal(err)
		}
		apRes := runOps(ctx, ap, ops)
		ap.Close()

		for i := range ops {
			if plainRes[i] != apRes[i] {
				t.Fatalf("seed=%d op#%d %s(%s): plain=%q autopipeline=%q",
					seed, i, ops[i].kind, ops[i].key, plainRes[i], apRes[i])
			}
		}
	}
	client.FlushDB(ctx)
}

type fuzzOp struct {
	kind string
	key  string
	val  string
	n    int64
}

func genOps(rng *rand.Rand, n int) []fuzzOp {
	kinds := []string{"set", "get", "getdel", "incr", "incrby", "append", "strlen",
		"exists", "del", "hset", "hget", "hgetall", "lpush", "lrange", "llen",
		"expire", "ttl", "type", "setbig", "setbin"}
	keys := make([]string, 8)
	for i := range keys {
		keys[i] = fmt.Sprintf("fz:%d", i)
	}
	ops := make([]fuzzOp, n)
	for i := range ops {
		k := keys[rng.Intn(len(keys))]
		ops[i] = fuzzOp{kind: kinds[rng.Intn(len(kinds))], key: k,
			val: fmt.Sprintf("v%d", rng.Intn(1000)), n: int64(rng.Intn(50))}
	}
	return ops
}

// runOps executes the op sequence via the Cmdable interface and returns a
// normalized "value|error" string per op for cross-run comparison.
func runOps(ctx context.Context, c redis.Cmdable, ops []fuzzOp) []string {
	out := make([]string, len(ops))
	res := func(v interface{}, err error) string {
		if err != nil {
			return "ERR:" + err.Error()
		}
		return fmt.Sprintf("%v", v)
	}
	for i, op := range ops {
		switch op.kind {
		case "set":
			out[i] = res(c.Set(ctx, op.key, op.val, 0).Result())
		case "get":
			out[i] = res(c.Get(ctx, op.key).Result())
		case "getdel":
			out[i] = res(c.GetDel(ctx, op.key).Result())
		case "incr":
			out[i] = res(c.Incr(ctx, op.key).Result())
		case "incrby":
			out[i] = res(c.IncrBy(ctx, op.key, op.n).Result())
		case "append":
			out[i] = res(c.Append(ctx, op.key, op.val).Result())
		case "strlen":
			out[i] = res(c.StrLen(ctx, op.key).Result())
		case "exists":
			out[i] = res(c.Exists(ctx, op.key).Result())
		case "del":
			out[i] = res(c.Del(ctx, op.key).Result())
		case "hset":
			out[i] = res(c.HSet(ctx, op.key, op.val, op.n).Result())
		case "hget":
			out[i] = res(c.HGet(ctx, op.key, op.val).Result())
		case "hgetall":
			out[i] = res(c.HGetAll(ctx, op.key).Result())
		case "lpush":
			out[i] = res(c.LPush(ctx, op.key, op.val).Result())
		case "lrange":
			out[i] = res(c.LRange(ctx, op.key, 0, -1).Result())
		case "llen":
			out[i] = res(c.LLen(ctx, op.key).Result())
		case "expire":
			out[i] = res(c.Expire(ctx, op.key, 1000).Result())
		case "ttl":
			// TTL varies with wall clock; normalize to "set"/"unset".
			d, err := c.TTL(ctx, op.key).Result()
			if err != nil {
				out[i] = "ERR:" + err.Error()
			} else if d > 0 {
				out[i] = "ttl>0"
			} else {
				out[i] = fmt.Sprintf("%v", d)
			}
		case "type":
			out[i] = res(c.Type(ctx, op.key).Result())
		case "setbig":
			out[i] = res(c.Set(ctx, op.key, strings.Repeat("Z", 70000), 0).Result())
		case "setbin":
			out[i] = res(c.Set(ctx, op.key, "b\x00\r\n\x01\xffX", 0).Result())
		}
	}
	return out
}

// ===== from autopipeline_hook_deadlock_test.go =====
// resultPeekHook reads every command's result inside the hook chain — the
// redisotel span-status pattern (after next) and the stricter before-next
// variant from the #3867 deadlock report. Reading a deferred command's result
// on the dispatch goroutine used to block on the batch's done channel, which
// that same goroutine closes only after the hook chain returns: a
// deterministic deadlock on the async face.
type resultPeekHook struct{ before bool }

func (h resultPeekHook) DialHook(next redis.DialHook) redis.DialHook { return next }

func (h resultPeekHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		if h.before {
			_ = cmd.Err()
		}
		err := next(ctx, cmd)
		if !h.before {
			_ = cmd.Err()
		}
		return err
	}
}

func (h resultPeekHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		if h.before {
			for _, c := range cmds {
				_ = c.Err()
			}
		}
		err := next(ctx, cmds)
		if !h.before {
			for _, c := range cmds {
				_ = c.Err()
			}
		}
		return err
	}
}

// runWithWatchdog fails the test instead of hanging the suite if fn deadlocks.
func runWithWatchdog(t *testing.T, timeout time.Duration, fn func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { defer close(done); fn() }()
	select {
	case <-done:
	case <-time.After(timeout):
		t.Fatal("deadlock: hook reading deferred results blocked the dispatch")
	}
}

func testAsyncHookPeek(t *testing.T, before bool) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.AddHook(resultPeekHook{before: before})
	c.FlushDB(ctx)

	ap, err := c.AsyncAutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	runWithWatchdog(t, 30*time.Second, func() {
		const N = 200
		sets := make([]*redis.StatusCmd, N)
		for i := 0; i < N; i++ {
			sets[i] = ap.Set(ctx, fmt.Sprintf("hookpeek:%d", i), i, 0)
		}
		for i, s := range sets {
			if s.Err() != nil {
				t.Errorf("set %d: %v", i, s.Err())
			}
		}
		for i := 0; i < N; i++ {
			v, err := ap.Get(ctx, fmt.Sprintf("hookpeek:%d", i)).Result()
			if err != nil || v != fmt.Sprint(i) {
				t.Errorf("get %d: v=%q err=%v", i, v, err)
			}
		}
	})
}

// TestAsyncAutoPipelineHookReadsAfterNext: redisotel-style hook (reads results
// after next()). Deadlocked before the innermost-close fix.
func TestAsyncAutoPipelineHookReadsAfterNext(t *testing.T) { testAsyncHookPeek(t, false) }

// TestAsyncAutoPipelineHookReadsBeforeNext: hook reads results before next().
// Deadlocked before the dispatcher-goroutine guard in await().
func TestAsyncAutoPipelineHookReadsBeforeNext(t *testing.T) { testAsyncHookPeek(t, true) }

// TestAsyncAutoPipelineHookSoloCommand drives the lone-command fast path
// (Process chain, not the pipeline chain) with both hook variants.
func TestAsyncAutoPipelineHookSoloCommand(t *testing.T) {
	for _, before := range []bool{false, true} {
		t.Run(fmt.Sprintf("before=%v", before), func(t *testing.T) {
			ctx := context.Background()
			c := redis.NewClient(&redis.Options{Addr: ":6379"})
			defer c.Close()
			c.AddHook(resultPeekHook{before: before})
			c.FlushDB(ctx)

			ap, err := c.AsyncAutoPipeline()
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()

			runWithWatchdog(t, 30*time.Second, func() {
				// One command, then an immediate read: the lone-caller flush
				// dispatches it through the solo Process path.
				cmd := ap.Set(ctx, "hookpeek:solo", "v", 0)
				if err := cmd.Err(); err != nil {
					t.Errorf("solo set: %v", err)
				}
				if v, err := ap.Get(ctx, "hookpeek:solo").Result(); err != nil || v != "v" {
					t.Errorf("solo get: v=%q err=%v", v, err)
				}
			})
		})
	}
}

// TestBlockingAutoPipelineHookReads: the blocking face was never affected;
// pin that with the same hooks.
func TestBlockingAutoPipelineHookReads(t *testing.T) {
	for _, before := range []bool{false, true} {
		t.Run(fmt.Sprintf("before=%v", before), func(t *testing.T) {
			ctx := context.Background()
			c := redis.NewClient(&redis.Options{Addr: ":6379"})
			defer c.Close()
			c.AddHook(resultPeekHook{before: before})
			c.FlushDB(ctx)

			ap, err := c.AutoPipeline()
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()

			runWithWatchdog(t, 30*time.Second, func() {
				for i := 0; i < 50; i++ {
					if err := ap.Set(ctx, fmt.Sprintf("hookpeek:b:%d", i), i, 0).Err(); err != nil {
						t.Errorf("set %d: %v", i, err)
					}
				}
			})
		})
	}
}

// TestAsyncAutoPipelineDoHookReads drives Do's background goroutine (its own
// close path) under a result-reading ProcessHook.
func TestAsyncAutoPipelineDoHookReads(t *testing.T) {
	for _, before := range []bool{false, true} {
		t.Run(fmt.Sprintf("before=%v", before), func(t *testing.T) {
			ctx := context.Background()
			c := redis.NewClient(&redis.Options{Addr: ":6379"})
			defer c.Close()
			c.AddHook(resultPeekHook{before: before})

			ap, err := c.AsyncAutoPipeline()
			if err != nil {
				t.Fatal(err)
			}
			defer ap.Close()

			runWithWatchdog(t, 30*time.Second, func() {
				cmd := ap.Do(ctx, "PING")
				if v, err := cmd.Result(); err != nil || v != "PONG" {
					t.Errorf("do ping: v=%v err=%v", v, err)
				}
			})
		})
	}
}

// ===== from autopipeline_leak_test.go =====
// TestAutoPipelineNoGoroutineLeak opens and closes many autopipelines (both
// faces, ordered + unordered) and asserts goroutines return to baseline — i.e.
// flushers AND dispatcher workers all exit on Close, nothing leaks.
func TestAutoPipelineNoGoroutineLeak(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", PoolSize: 50})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	cfgs := []*redis.AutoPipelineOptions{
		{MaxBatchSize: 300}, // ordered, 1 permit, 1 stripe
		{MaxBatchSize: 300, MaxConcurrentBatches: 8, Unordered: true},  // unordered, 8 permits, 8 stripes
		{MaxBatchSize: 300, MaxConcurrentBatches: 50, Unordered: true}, // many permits -> many workers
	}

	cycle := func() {
		for _, cfg := range cfgs {
			for _, async := range []bool{false, true} {
				var ap *redis.AutoPipeliner
				var err error
				if async {
					ap, err = client.AsyncAutoPipelineWithOptions(cfg)
				} else {
					ap, err = client.AutoPipelineWithOptions(cfg)
				}
				if err != nil {
					t.Fatal(err)
				}
				var wg = make([]redis.AutoFuture, 0, 200)
				for i := 0; i < 200; i++ {
					if async {
						wg = append(wg, ap.Submit(ctx, redis.NewCmd(ctx, "set", "leak:k", i)))
					} else {
						_ = ap.Set(ctx, "leak:k", i, 0).Err()
					}
				}
				for i := range wg {
					_ = wg[i].Wait()
				}
				if err := ap.Close(); err != nil {
					t.Fatalf("close: %v", err)
				}
			}
		}
	}

	cycle() // warmup so lazy client/pool goroutines exist before we snapshot
	runtime.GC()
	time.Sleep(150 * time.Millisecond)
	base := runtime.NumGoroutine()

	const rounds = 40
	for r := 0; r < rounds; r++ {
		cycle()
	}
	runtime.GC()
	time.Sleep(300 * time.Millisecond) // let any exiting goroutines wind down
	got := runtime.NumGoroutine()

	// Allow small slack for pool/runtime background goroutines; a leak of
	// flushers/workers would be rounds*(shards+workers) ~ hundreds.
	if got > base+8 {
		t.Fatalf("goroutine leak: baseline=%d after %d open/close rounds=%d (delta %d)",
			base, rounds, got, got-base)
	}
	t.Logf("no leak: baseline=%d final=%d (delta %d) over %d rounds", base, got, got-base, rounds)
}

// ===== from autopipeline_retry_test.go =====
// TestAutoPipelineRetriesOnNetworkError verifies AutoPipeline inherits the
// pipeline retry/re-drive loop (it dispatches through the same execer as manual
// Pipeline). A broken conn is seeded; the batch's first attempt fails on it and
// the retry redials a healthy conn. Results are read through the futures, and
// must be correct after the re-drive.
//
// MaxFlushDelay forces the two commands into one real (>=2) batch so they run
// through the pipeline execer; a lone command would take the single-command
// fast path (Process) instead.
func TestAutoPipelineRetriesOnNetworkError(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379", MaxRetries: 2, PoolSize: 1})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	if err := client.Set(ctx, "apr:n", 0, 0).Err(); err != nil {
		t.Fatal(err)
	}

	cn, err := client.Pool().Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	cn.SetNetConn(&badConn{writeErr: io.EOF})
	client.Pool().Put(ctx, cn)

	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize:  300,
		MaxFlushDelay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	s := ap.Set(ctx, "apr:k", "v", 0)
	n := ap.Incr(ctx, "apr:n")
	if err := s.Err(); err != nil {
		t.Fatalf("set after retry: %v", err)
	}
	if err := n.Err(); err != nil {
		t.Fatalf("incr after retry: %v", err)
	}
	if s.Val() != "OK" {
		t.Fatalf("set = %q, want OK", s.Val())
	}
	if n.Val() != 1 {
		t.Fatalf("incr = %d, want 1", n.Val())
	}
}

// TestAutoPipelineRetryExhaustionSurfacesError checks that when the pipeline
// retry loop is EXHAUSTED (every attempt fails on a retryable error), the
// AutoPipeline commands surface the error to their futures.
//
// This matters more for AutoPipeline than for manual Pipeline: manual Pipeline
// returns the error from Exec, but AutoPipeline discards the pipeline error
// (dispatchCmds: `_ = ...processPipelineHook(...)`) and results are observable
// ONLY via each command's future. If generalProcessPipeline does not call
// setCmdsErr on exhaustion (it only does so on the early-exit branch), a caller
// sees a nil error and a zero value — silent loss.
func TestAutoPipelineRetryExhaustionSurfacesError(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:       ":6379",
		MaxRetries: 2,
		PoolSize:   1,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return &badConn{writeErr: io.EOF}, nil // every conn fails all I/O
		},
	})
	defer client.Close()

	ap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
		MaxBatchSize:  300,
		MaxFlushDelay: 50 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	s := ap.Set(ctx, "k", "v", 0)
	n := ap.Incr(ctx, "n")
	if s.Err() == nil || n.Err() == nil {
		t.Fatalf("SILENT LOSS: AutoPipeline commands returned nil error on retry exhaustion (set err=%v, incr err=%v) — every attempt failed but the futures report success",
			s.Err(), n.Err())
	}
}

// ===== from autopipeline_sequential_test.go =====
func TestAutoPipelineSequential(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{
			MaxBatchSize:         10,
			MaxFlushDelay:        50 * time.Millisecond,
			MaxConcurrentBatches: 5,
			Unordered:            true,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AsyncAutoPipeline() // deferred face: submit-then-read (Do bypasses the engine)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Sequential usage - no goroutines needed: typed calls on the deferred
	// face queue without blocking and are batched automatically
	cmds := make([]redis.Cmder, 20)
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		cmds[i] = ap.Set(ctx, key, i, 0)
	}

	// Now access results - this will block until commands execute
	for i, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			t.Fatalf("Command %d failed: %v", i, err)
		}
	}

	// Verify all keys were set
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d", i, val)
		}
	}
}

func TestAutoPipelineSequentialSmallBatches(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{
			MaxBatchSize:         1000,                  // Large batch size
			MaxFlushDelay:        20 * time.Millisecond, // Rely on timer
			MaxConcurrentBatches: 5,
			Unordered:            true,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AsyncAutoPipeline() // deferred face: submit-then-read (Do bypasses the engine)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Queue commands sequentially with small delays
	// They should be flushed by timer, not batch size
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		cmd := ap.Set(ctx, key, i, 0)

		// Access result immediately - should block until flush
		if err := cmd.Err(); err != nil {
			t.Fatalf("Command %d failed: %v", i, err)
		}

		time.Sleep(5 * time.Millisecond)
	}

	// Verify all keys were set
	for i := 0; i < 5; i++ {
		key := fmt.Sprintf("key%d", i)
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d", i, val)
		}
	}
}

func TestAutoPipelineSequentialMixed(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{
			MaxBatchSize:         5,
			MaxFlushDelay:        50 * time.Millisecond,
			MaxConcurrentBatches: 5,
			Unordered:            true,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AsyncAutoPipeline() // deferred face: submit-then-read (Do bypasses the engine)
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Queue some commands
	cmd1 := ap.Set(ctx, "key1", "value1", 0)
	cmd2 := ap.Set(ctx, "key2", "value2", 0)
	cmd3 := ap.Set(ctx, "key3", "value3", 0)

	// Access first result - should trigger batch flush
	if err := cmd1.Err(); err != nil {
		t.Fatalf("cmd1 failed: %v", err)
	}

	// Other commands in same batch should also be done
	if err := cmd2.Err(); err != nil {
		t.Fatalf("cmd2 failed: %v", err)
	}
	if err := cmd3.Err(); err != nil {
		t.Fatalf("cmd3 failed: %v", err)
	}

	// Verify
	val, err := client.Get(ctx, "key1").Result()
	if err != nil || val != "value1" {
		t.Fatalf("key1: expected value1, got %v, err %v", val, err)
	}
}

// ===== from autopipeline_singleton_test.go =====
// TestAutoPipelineConcurrentFirstCall guards a data race where AutoPipeline()
// did its cache check-and-set on the client's autopipeliner field without a
// lock. Concurrent first calls could each build a separate AutoPipeliner —
// racing the field (caught by -race) and leaking the loser's flusher goroutines
// and pipeline pool. Every concurrent caller must observe the SAME instance.
func TestAutoPipelineConcurrentFirstCall(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer c.Close()
	c.FlushDB(ctx)

	const G = 64
	results := make([]*redis.AutoPipeliner, G)
	errs := make([]error, G)
	var wg sync.WaitGroup
	start := make(chan struct{})
	wg.Add(G)
	for g := 0; g < G; g++ {
		go func(i int) {
			defer wg.Done()
			<-start // maximize the race on the first call
			results[i], errs[i] = c.AutoPipeline()
		}(g)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Fatalf("call %d: AutoPipeline() returned error: %v", i, err)
		}
	}

	first := results[0]
	if first == nil {
		t.Fatal("AutoPipeline() returned nil")
	}
	for i, ap := range results {
		if ap != first {
			t.Fatalf("call %d returned a different AutoPipeliner instance; concurrent first calls must share one", i)
		}
	}
	defer first.Close()

	// The shared instance must still work.
	if err := first.Set(ctx, "singleton", "1", 0).Err(); err != nil {
		t.Fatalf("shared autopipeliner unusable: %v", err)
	}
}

// ===== from autopipeline_test.go =====
var _ = Describe("AutoPipeline", func() {
	ctx := context.Background()
	var client *redis.Client
	var ap *redis.AutoPipeliner

	BeforeEach(func() {
		client = redis.NewClient(&redis.Options{
			Addr: redisAddr,
			AutoPipelineOptions: &redis.AutoPipelineOptions{
				MaxBatchSize:         10,
				MaxFlushDelay:        50 * time.Millisecond,
				MaxConcurrentBatches: 5,
				Unordered:            true,
			},
		})
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())

		var err error
		ap, err = client.AutoPipeline()
		Expect(err).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		if ap != nil {
			Expect(ap.Close()).NotTo(HaveOccurred())
		}
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("should batch commands automatically", func() {
		// Issue multiple commands concurrently to enable batching. A typed call
		// without blocking, so each goroutine must await its own command's result
		// (cmd.Err) before reporting done — wg.Wait alone only waits for the
		// queueing, not the batch flush+execute, which would race the reads below.
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				Expect(ap.Set(ctx, key, idx, 0).Err()).NotTo(HaveOccurred())
			}(i)
		}

		// Wait for all commands to execute (each goroutine awaited its result).
		wg.Wait()

		// Verify all commands were executed
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			val, err := client.Get(ctx, key).Int()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(i))
		}
	})

	It("should flush when batch size is reached", func() {
		// Queue exactly MaxBatchSize commands concurrently. Each goroutine awaits
		// its command's result so wg.Wait reflects execution, not just queueing.
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				Expect(ap.Set(ctx, key, idx, 0).Err()).NotTo(HaveOccurred())
			}(i)
		}

		// Wait for all commands to execute (each goroutine awaited its result).
		wg.Wait()

		// Verify all commands were executed
		for i := 0; i < 10; i++ {
			key := fmt.Sprintf("key%d", i)
			val, err := client.Get(ctx, key).Int()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(i))
		}
	})

	It("should flush on timer expiry", func() {
		// Queue a single command (will block until flushed by timer)
		go func() {
			ap.Set(ctx, "key1", "value1", 0)
		}()

		// Wait for timer to expire and command to complete
		time.Sleep(100 * time.Millisecond)

		// Verify command was executed
		val, err := client.Get(ctx, "key1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("value1"))
	})

	It("should handle concurrent commands", func() {
		const numGoroutines = 10
		const commandsPerGoroutine = 100

		var wg sync.WaitGroup
		wg.Add(numGoroutines)

		for g := 0; g < numGoroutines; g++ {
			go func(goroutineID int) {
				defer wg.Done()
				for i := 0; i < commandsPerGoroutine; i++ {
					key := fmt.Sprintf("g%d:key%d", goroutineID, i)
					ap.Set(ctx, key, i, 0)
				}
			}(g)
		}

		wg.Wait()

		// All commands are now complete (Do() blocks until executed)
		// Verify all commands were executed
		for g := 0; g < numGoroutines; g++ {
			for i := 0; i < commandsPerGoroutine; i++ {
				key := fmt.Sprintf("g%d:key%d", g, i)
				val, err := client.Get(ctx, key).Int()
				Expect(err).NotTo(HaveOccurred())
				Expect(val).To(Equal(i))
			}
		}
	})

	It("should support manual flush", func() {
		// Queue commands concurrently
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				ap.Set(ctx, key, idx, 0)
			}(i)
		}

		// Wait for all commands to complete
		wg.Wait()

		// Verify all commands were executed
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			val, err := client.Get(ctx, key).Int()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(i))
		}
	})

	It("should handle errors in commands", func() {
		// Queue a mix of valid and invalid commands concurrently
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			ap.Set(ctx, "key1", "value1", 0)
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			// A command failing with a redis error (WRONGTYPE) inside a batch
			// must not affect its batch neighbours.
			client.Set(ctx, "not-a-counter", "str", 0)
			_ = ap.Incr(ctx, "not-a-counter").Err()
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()
			ap.Set(ctx, "key2", "value2", 0)
		}()

		// Wait for all commands to complete
		wg.Wait()

		// Valid commands should still execute
		val, err := client.Get(ctx, "key1").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("value1"))

		val, err = client.Get(ctx, "key2").Result()
		Expect(err).NotTo(HaveOccurred())
		Expect(val).To(Equal("value2"))
	})

	It("should close gracefully", func() {
		// Queue commands concurrently
		var wg sync.WaitGroup
		for i := 0; i < 5; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				ap.Set(ctx, key, idx, 0)
			}(i)
		}

		// Wait for all commands to complete
		wg.Wait()

		// Close should flush pending commands
		Expect(ap.Close()).NotTo(HaveOccurred())
		ap = nil // Prevent double close in AfterEach

		// Verify all commands were executed
		for i := 0; i < 5; i++ {
			key := fmt.Sprintf("key%d", i)
			val, err := client.Get(ctx, key).Int()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(i))
		}
	})

	It("should return error after close", func() {
		Expect(ap.Close()).NotTo(HaveOccurred())

		// Commands after close should fail
		cmd := ap.Set(ctx, "key", "value", 0)
		Expect(cmd.Err()).To(Equal(redis.ErrClosed))
	})

	It("should respect MaxConcurrentBatches", func() {
		// Create autopipeliner with low concurrency limit
		client2 := redis.NewClient(&redis.Options{
			Addr: redisAddr,
			AutoPipelineOptions: &redis.AutoPipelineOptions{
				MaxBatchSize:         5,
				MaxFlushDelay:        10 * time.Millisecond,
				MaxConcurrentBatches: 2,
				Unordered:            true,
			},
		})
		defer client2.Close()

		ap2, err := client2.AutoPipeline()
		Expect(err).NotTo(HaveOccurred())
		defer ap2.Close()

		// Queue many commands to trigger multiple batches concurrently
		var wg sync.WaitGroup
		for i := 0; i < 50; i++ {
			wg.Add(1)
			go func(idx int) {
				defer wg.Done()
				key := fmt.Sprintf("key%d", idx)
				ap2.Set(ctx, key, idx, 0)
			}(i)
		}

		// Wait for all commands to complete
		wg.Wait()

		// Verify all commands were executed
		for i := 0; i < 50; i++ {
			key := fmt.Sprintf("key%d", i)
			val, err := client2.Get(ctx, key).Int()
			Expect(err).NotTo(HaveOccurred())
			Expect(val).To(Equal(i))
		}
	})

	It("should report queue length", func() {
		// Deferred face with a wide explicit window: queued commands stay
		// queued until the window elapses, so Len is deterministic.
		aap, err := client.AsyncAutoPipelineWithOptions(&redis.AutoPipelineOptions{
			MaxBatchSize:  100,
			MaxFlushDelay: time.Second,
		})
		Expect(err).NotTo(HaveOccurred())
		defer aap.Close()

		Expect(aap.Len()).To(Equal(0))

		cmds := make([]*redis.StatusCmd, 0, 5)
		for i := 0; i < 5; i++ {
			cmds = append(cmds, aap.Set(ctx, fmt.Sprintf("key%d", i), i, 0)) // queued, non-blocking
		}
		Expect(aap.Len()).To(Equal(5))

		for _, c := range cmds {
			Expect(c.Err()).NotTo(HaveOccurred()) // blocks until the window flushes
		}
		Expect(aap.Len()).To(Equal(0))
	})
})

func TestAutoPipelineBasic(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{
			MaxBatchSize:         10,
			MaxFlushDelay:        50 * time.Millisecond,
			MaxConcurrentBatches: 5,
			Unordered:            true,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Queue commands concurrently
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx)
			cmd := ap.Set(ctx, key, idx, 0)
			// Wait for command to complete
			_ = cmd.Err()
		}(i)
	}

	// Wait for all commands to complete
	wg.Wait()

	// Verify
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get key%d: %v", i, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d", i, val)
		}
	}
}

func TestAutoPipelineMaxFlushDelay(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
		AutoPipelineOptions: &redis.AutoPipelineOptions{
			MaxBatchSize:         1000, // Large batch size so only timer triggers flush
			MaxFlushDelay:        50 * time.Millisecond,
			MaxConcurrentBatches: 5,
			Unordered:            true,
		},
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Send commands slowly, one at a time
	// They should be flushed by the timer, not by batch size
	start := time.Now()

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx)
			cmd := ap.Set(ctx, key, idx, 0)
			// Wait for command to complete
			_ = cmd.Err()
		}(i)

		// Wait a bit between commands to ensure they don't batch by size
		time.Sleep(10 * time.Millisecond)
	}

	wg.Wait()
	elapsed := time.Since(start)

	// Verify all keys were set
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("key%d", i)
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d", i, val)
		}
	}

	// Should complete in roughly 10 * 10ms = 100ms + some flush intervals
	// If timer doesn't work, commands would wait until Close() is called
	if elapsed > 500*time.Millisecond {
		t.Fatalf("Commands took too long (%v), flush interval may not be working", elapsed)
	}

	t.Logf("Completed in %v (flush interval working correctly)", elapsed)
}

func TestAutoPipelineConcurrency(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:                ":6379",
		AutoPipelineOptions: redis.DefaultAutoPipelineOptions(),
	})
	defer client.Close()

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const numGoroutines = 100
	const commandsPerGoroutine = 100

	var wg sync.WaitGroup
	var successCount atomic.Int64

	wg.Add(numGoroutines)
	for g := 0; g < numGoroutines; g++ {
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < commandsPerGoroutine; i++ {
				key := fmt.Sprintf("g%d:key%d", goroutineID, i)
				ap.Set(ctx, key, i, 0)
				successCount.Add(1)
			}
		}(g)
	}

	wg.Wait()

	// Wait for all flushes
	time.Sleep(500 * time.Millisecond)

	expected := int64(numGoroutines * commandsPerGoroutine)
	if successCount.Load() != expected {
		t.Fatalf("Expected %d commands, got %d", expected, successCount.Load())
	}
}

// TestAutoPipelineSingleCommandNoBlock verifies that single commands don't block
func TestAutoPipelineSingleCommandNoBlock(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:                ":6379",
		AutoPipelineOptions: redis.DefaultAutoPipelineOptions(),
	})
	defer client.Close()

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	start := time.Now()
	cmd := ap.Ping(ctx)
	err = cmd.Err()
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("Command failed: %v", err)
	}
	if cmd.Val() != "PONG" {
		t.Fatalf("Ping = %q, want PONG", cmd.Val())
	}

	// Single command should complete within 50ms (adaptive delay is 10ms)
	if elapsed > 50*time.Millisecond {
		t.Errorf("Single command took too long: %v (should be < 50ms)", elapsed)
	}

	t.Logf("Single command completed in %v", elapsed)
}

// TestAutoPipelineSequentialSingleThread verifies sequential single-threaded execution
func TestAutoPipelineSequentialSingleThread(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr:                ":6379",
		AutoPipelineOptions: redis.DefaultAutoPipelineOptions(),
	})
	defer client.Close()

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Execute 10 commands sequentially in a single thread
	start := time.Now()
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("test:key:%d", i)
		t.Logf("Sending command %d", i)
		cmd := ap.Set(ctx, key, i, 0)
		t.Logf("Waiting for command %d to complete", i)
		err := cmd.Err()
		if err != nil {
			t.Fatalf("Command %d failed: %v", i, err)
		}
		t.Logf("Command %d completed", i)
	}
	elapsed := time.Since(start)

	// Should complete reasonably fast (< 100ms for 10 commands)
	if elapsed > 100*time.Millisecond {
		t.Errorf("10 sequential commands took too long: %v (should be < 100ms)", elapsed)
	}

	t.Logf("10 sequential commands completed in %v (avg: %v per command)", elapsed, elapsed/10)
}

// ===== from autopipeline_typed_test.go =====
// TestAutoPipelineTypedCommands tests that typed commands like Get() and Set()
// work correctly with autopipelining and block until execution.
func TestAutoPipelineTypedCommands(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	// Skip if Redis is not available
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Test Set and Get
	setResult, setErr := ap.Set(ctx, "test_key", "test_value", 0).Result()
	if setErr != nil {
		t.Fatalf("Set failed: %v", setErr)
	}
	if setResult != "OK" {
		t.Errorf("Expected 'OK', got '%s'", setResult)
	}

	getResult, getErr := ap.Get(ctx, "test_key").Result()
	if getErr != nil {
		t.Fatalf("Get failed: %v", getErr)
	}
	if getResult != "test_value" {
		t.Errorf("Expected 'test_value', got '%s'", getResult)
	}

	// Clean up
	client.Del(ctx, "test_key")
}

// TestAutoPipelineTypedCommandsMultiple tests multiple typed commands
func TestAutoPipelineTypedCommandsMultiple(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	// Skip if Redis is not available
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Queue multiple Set commands
	ap.Set(ctx, "key1", "value1", 0)
	ap.Set(ctx, "key2", "value2", 0)
	ap.Set(ctx, "key3", "value3", 0)

	// Get the values - should block until execution
	val1, err1 := ap.Get(ctx, "key1").Result()
	if err1 != nil {
		t.Fatalf("Get key1 failed: %v", err1)
	}
	if val1 != "value1" {
		t.Errorf("Expected 'value1', got '%s'", val1)
	}

	val2, err2 := ap.Get(ctx, "key2").Result()
	if err2 != nil {
		t.Fatalf("Get key2 failed: %v", err2)
	}
	if val2 != "value2" {
		t.Errorf("Expected 'value2', got '%s'", val2)
	}

	val3, err3 := ap.Get(ctx, "key3").Result()
	if err3 != nil {
		t.Fatalf("Get key3 failed: %v", err3)
	}
	if val3 != "value3" {
		t.Errorf("Expected 'value3', got '%s'", val3)
	}

	// Clean up
	client.Del(ctx, "key1", "key2", "key3")
}

// TestAutoPipelineTypedCommandsVal tests the Val() method
func TestAutoPipelineTypedCommandsVal(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{
		Addr: ":6379",
	})
	defer client.Close()

	// Skip if Redis is not available
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Set a value
	ap.Set(ctx, "test_val_key", "test_val_value", 0)

	// Get using Val() - should block until execution
	val := ap.Get(ctx, "test_val_key").Val()
	if val != "test_val_value" {
		t.Errorf("Expected 'test_val_value', got '%s'", val)
	}

	// Clean up
	client.Del(ctx, "test_val_key")
}

// ===== from autopipeline_universal_test.go =====
// TestUniversalClientAutoPipeline verifies AutoPipeline is reachable through the
// UniversalClient interface: a standalone/cluster-backed UniversalClient returns
// a working AutoPipeliner, while Ring (also a UniversalClient) returns an error
// rather than being silently absent.
func TestUniversalClientAutoPipeline(t *testing.T) {
	ctx := context.Background()

	var uc redis.UniversalClient = redis.NewUniversalClient(&redis.UniversalOptions{
		Addrs: []string{":6379"},
	})
	defer uc.Close()
	if err := uc.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}

	ap, err := uc.AutoPipeline()
	if err != nil {
		t.Fatalf("UniversalClient.AutoPipeline: %v", err)
	}
	defer ap.Close()
	if v := ap.Set(ctx, "uc:k", "v", 0).Val(); v != "OK" {
		t.Fatalf("set via UniversalClient autopipeline = %q, want OK", v)
	}
	if v := ap.Get(ctx, "uc:k").Val(); v != "v" {
		t.Fatalf("get via UniversalClient autopipeline = %q, want v", v)
	}
	uc.Del(ctx, "uc:k")

	// Ring: AutoPipeline is unsupported and must return an error (directly and
	// through the UniversalClient interface).
	ring := redis.NewRing(&redis.RingOptions{Addrs: map[string]string{"s1": ":6379"}})
	defer ring.Close()
	if _, err := ring.AutoPipeline(); err == nil {
		t.Fatal("Ring.AutoPipeline should return an error (unsupported)")
	}
	if _, err := ring.AsyncAutoPipeline(); err == nil {
		t.Fatal("Ring.AsyncAutoPipeline should return an error (unsupported)")
	}
	var ru redis.UniversalClient = ring
	if _, err := ru.AutoPipeline(); err == nil {
		t.Fatal("Ring via UniversalClient.AutoPipeline should return an error")
	}
}

// TestAutoPipelinerAsUniversalClient verifies an *AutoPipeliner is a drop-in
// UniversalClient: data commands are batched through it, while the non-batched
// surface (PoolStats, Do, ...) delegates to the underlying client.
func TestAutoPipelinerAsUniversalClient(t *testing.T) {
	ctx := context.Background()
	client := redis.NewClient(&redis.Options{Addr: ":6379"})
	defer client.Close()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("no redis: %v", err)
	}
	apc, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer apc.Close()

	// Use the autopipeliner strictly through the UniversalClient interface.
	var uc redis.UniversalClient = apc

	// Data commands: batched by the autopipeliner.
	if v := uc.Set(ctx, "ucap:k", "v", 0).Val(); v != "OK" {
		t.Fatalf("Set = %q, want OK", v)
	}
	if v := uc.Get(ctx, "ucap:k").Val(); v != "v" {
		t.Fatalf("Get = %q, want v", v)
	}
	// Non-batched surface delegates to the underlying client.
	if uc.PoolStats() == nil {
		t.Fatal("PoolStats() returned nil via UniversalClient")
	}
	if err := uc.Do(ctx, "PING").Err(); err != nil {
		t.Fatalf("Do(PING): %v", err)
	}
	uc.Del(ctx, "ucap:k")
}

// ===== from osscluster_autopipeline_buffer_test.go =====
// Autopipeline + zero-copy buffer on CLUSTER (cross-slot, concurrent).
func TestAPZeroCopyBufferCluster(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:               []string{":16600", ":16601", ":16602", ":16603", ":16604", ":16605"},
		AutoPipelineOptions: &redis.AutoPipelineOptions{MaxBatchSize: 100, MaxConcurrentBatches: 30, Unordered: true},
	})
	defer c.Close()
	skipIfClusterUnhealthy(t, c)
	_ = c.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error { return m.FlushAll(ctx).Err() })
	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const g = 80
	const perG = 30
	var wg sync.WaitGroup
	var bad int64
	var mu sync.Mutex
	var errs []string
	wg.Add(g)
	for x := 0; x < g; x++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				key := fmt.Sprintf("clbuf:%d:%d:%d", id, i, id*7919+i) // spread slots
				val := fmt.Sprintf("p-%d-%d", id, i)
				if err := ap.SetFromBuffer(ctx, key, []byte(val)).Err(); err != nil {
					atomic.AddInt64(&bad, 1)
					mu.Lock()
					if len(errs) < 10 {
						errs = append(errs, "set:"+err.Error())
					}
					mu.Unlock()
					continue
				}
				rbuf := make([]byte, len(val))
				cmd := ap.GetToBuffer(ctx, key, rbuf)
				n, err := cmd.Result()
				if err != nil || n != len(val) || string(cmd.Bytes()) != val {
					atomic.AddInt64(&bad, 1)
					mu.Lock()
					if len(errs) < 10 {
						errs = append(errs, fmt.Sprintf("key %s n=%d got=%q want=%q err=%v", key, n, string(cmd.Bytes()), val, err))
					}
					mu.Unlock()
				}
			}
		}(x)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("cluster zero-copy buffer via autopipeline failed: %d; sample=%v", bad, errs)
	}
}

// ===== from osscluster_autopipeline_correctness_test.go =====
func newTestCluster() *redis.ClusterClient {
	return redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":16600", ":16601", ":16602", ":16603", ":16604", ":16605"},
		AutoPipelineOptions: &redis.AutoPipelineOptions{
			MaxBatchSize: 200, MaxConcurrentBatches: 50, Unordered: true,
		},
	})
}

// skipIfClusterUnhealthy skips the test unless a real, formed cluster answers.
// Ping alone is insufficient: in CI's standalone test job a Redis may answer on
// the cluster ports while the cluster is not formed (CLUSTER INFO reports
// cluster_state:fail / CLUSTERDOWN, and commands return MOVED), which would turn
// "no cluster available" into spurious failures. Gate on cluster_state:ok.
func skipIfClusterUnhealthy(t *testing.T, c *redis.ClusterClient) {
	t.Helper()
	ctx := context.Background()
	if err := c.Ping(ctx).Err(); err != nil {
		t.Skipf("cluster not reachable: %v", err)
	}
	info, err := c.ClusterInfo(ctx).Result()
	if err != nil {
		t.Skipf("cluster not reachable (CLUSTER INFO): %v", err)
	}
	if !strings.Contains(info, "cluster_state:ok") {
		t.Skipf("cluster not healthy (no cluster_state:ok)")
	}
}

// Validates cluster routing: a single autopipeline batch contains keys that
// hash to MANY different slots/shards. Each command must route to the correct
// node and return its own correct value. If routing were wrong, GETs would
// miss or return another key's value.
func TestAPClusterCrossSlotRouting(t *testing.T) {
	ctx := context.Background()
	c := newTestCluster()
	defer c.Close()
	skipIfClusterUnhealthy(t, c)
	_ = c.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
		return m.FlushAll(ctx).Err()
	})

	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 100
	const perG = 50
	var wg sync.WaitGroup
	var mismatch, slotsSeen sync.Map
	var bad int64
	var mu sync.Mutex

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < perG; i++ {
				// Keys with no hashtag => spread across all 16384 slots.
				key := fmt.Sprintf("cl:%d:%d:%d", id, i, id*7919+i)
				want := fmt.Sprintf("V-%d-%d", id, i)
				slotsSeen.Store(hashtag.Slot(key), true)
				if err := ap.Set(ctx, key, want, 0).Err(); err != nil {
					mu.Lock()
					bad++
					mismatch.Store(key, "set:"+err.Error())
					mu.Unlock()
					continue
				}
				got, err := ap.Get(ctx, key).Result()
				if err != nil || got != want {
					mu.Lock()
					bad++
					mismatch.Store(key, fmt.Sprintf("got=%q want=%q err=%v", got, want, err))
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()

	if bad > 0 {
		shown := 0
		mismatch.Range(func(k, v any) bool {
			t.Logf("MISMATCH %v: %v", k, v)
			shown++
			return shown < 15
		})
		t.Fatalf("cluster cross-slot routing failures: %d", bad)
	}

	// Sanity: we actually exercised many distinct slots (not all one shard).
	n := 0
	slotsSeen.Range(func(_, _ any) bool { n++; return true })
	if n < 100 {
		t.Fatalf("test too weak: only %d distinct slots exercised", n)
	}
	t.Logf("validated cross-slot routing across %d distinct slots", n)
}

// Validates per-key correctness on cluster with INCR ordering per goroutine,
// each goroutine on its own (randomly-slotted) key.
func TestAPClusterPerGoroutineOrder(t *testing.T) {
	ctx := context.Background()
	c := newTestCluster()
	defer c.Close()
	skipIfClusterUnhealthy(t, c)
	// Start from a clean slate so INCR counts are deterministic across runs.
	_ = c.ForEachMaster(ctx, func(ctx context.Context, m *redis.Client) error {
		return m.FlushAll(ctx).Err()
	})

	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const goroutines = 80
	const incrs = 80
	var wg sync.WaitGroup
	var bad int64
	var mu sync.Mutex
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			key := fmt.Sprintf("clord:%d:%d", id, id*104729)
			for i := 1; i <= incrs; i++ {
				v, err := ap.Incr(ctx, key).Result()
				if err != nil || v != int64(i) {
					mu.Lock()
					bad++
					mu.Unlock()
				}
			}
		}(g)
	}
	wg.Wait()
	if bad > 0 {
		t.Fatalf("cluster ordering/correctness failures: %d", bad)
	}
}

// ===== from osscluster_autopipeline_moved_test.go =====
// TestAPClusterMovedRedirectFollowed verifies AutoPipeline inherits the cluster
// MOVED-redirect handling (it dispatches through ClusterClient.processPipeline,
// which runs the MaxRedirects loop + checkMovedErr). SwapNodes forces the client
// to believe a replica is the master for a slot; a write there is answered with
// MOVED and the client must follow it, so the value is still read back correctly.
func TestAPClusterMovedRedirectFollowed(t *testing.T) {
	ctx := context.Background()
	c := newTestCluster()
	defer c.Close()
	skipIfClusterUnhealthy(t, c)

	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const key, val = "moved:key", "moved:val"
	if err := ap.Set(ctx, key, val, 0).Err(); err != nil {
		t.Fatalf("initial set: %v", err)
	}
	// Force the client's in-memory topology to point the slot at the wrong node,
	// so the next access is answered with MOVED and must be followed.
	if err := c.SwapNodes(ctx, key); err != nil {
		t.Fatalf("SwapNodes: %v", err)
	}

	got, err := ap.Get(ctx, key).Result()
	if err != nil {
		t.Fatalf("get after MOVED: %v", err)
	}
	if got != val {
		t.Fatalf("get after MOVED = %q, want %q (redirect not followed correctly)", got, val)
	}
}

// TestAPClusterMovedSurfacedWithoutRedirects anchors the positive test: with
// MaxRedirects disabled (-1 -> 0, one attempt), the forced MOVED is NOT followed
// and must surface as an error on the command's future — proving SwapNodes really
// induced a redirect (so TestAPClusterMovedRedirectFollowed exercised the
// redirect path rather than passing trivially).
func TestAPClusterMovedSurfacedWithoutRedirects(t *testing.T) {
	ctx := context.Background()
	c := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs:        []string{":16600", ":16601", ":16602", ":16603", ":16604", ":16605"},
		MaxRedirects: -1, // normalized to 0: a single attempt, no follow-through
		AutoPipelineOptions: &redis.AutoPipelineOptions{
			MaxBatchSize: 200, MaxConcurrentBatches: 50, Unordered: true,
		},
	})
	defer c.Close()
	skipIfClusterUnhealthy(t, c)

	ap, err := c.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const key = "moved:noredir:key"
	if err := ap.Set(ctx, key, "v", 0).Err(); err != nil {
		t.Fatalf("initial set: %v", err)
	}

	// SwapNodes is racy against the periodic LazyReload, so re-swap and retry
	// until we observe the MOVED (or give up and skip — the swap never stuck).
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if err := c.SwapNodes(ctx, key); err != nil {
			t.Fatalf("SwapNodes: %v", err)
		}
		err := ap.Get(ctx, key).Err()
		if err != nil && strings.Contains(err.Error(), "MOVED") {
			return // proven: the redirect was needed and, with no redirects, surfaced
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Skip("could not observe a MOVED (swap kept being reverted by topology reload)")
}

// ===== from osscluster_autopipeline_test.go =====
func TestClusterAutoPipelineBasic(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002"},
		AutoPipelineOptions: &redis.AutoPipelineOptions{
			MaxBatchSize:         10,
			MaxFlushDelay:        50 * time.Millisecond,
			MaxConcurrentBatches: 5,
			Unordered:            true,
		},
	})
	defer client.Close()

	skipIfClusterUnhealthy(t, client)

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Queue commands concurrently across different keys (different slots)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			key := fmt.Sprintf("key%d", idx)
			cmd := ap.Set(ctx, key, idx, 0)
			// Wait for command to complete
			_ = cmd.Err()
		}(i)
	}

	// Wait for all commands to complete
	wg.Wait()

	// Verify all keys were set correctly
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d", i, val)
		}
	}
}

func TestClusterAutoPipelineConcurrency(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002"},
		AutoPipelineOptions: &redis.AutoPipelineOptions{
			MaxBatchSize:         50,
			MaxFlushDelay:        10 * time.Millisecond,
			MaxConcurrentBatches: 10,
			Unordered:            true,
		},
	})
	defer client.Close()

	skipIfClusterUnhealthy(t, client)

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	const numGoroutines = 10
	const commandsPerGoroutine = 100

	var wg sync.WaitGroup
	for g := 0; g < numGoroutines; g++ {
		wg.Add(1)
		go func(goroutineID int) {
			defer wg.Done()
			for i := 0; i < commandsPerGoroutine; i++ {
				key := fmt.Sprintf("g%d:key%d", goroutineID, i)
				cmd := ap.Set(ctx, key, i, 0)
				// Wait for command to complete
				_ = cmd.Err()
			}
		}(g)
	}

	// Wait for all commands to complete
	wg.Wait()

	// Verify all commands were executed
	for g := 0; g < numGoroutines; g++ {
		for i := 0; i < commandsPerGoroutine; i++ {
			key := fmt.Sprintf("g%d:key%d", g, i)
			val, err := client.Get(ctx, key).Int()
			if err != nil {
				t.Fatalf("Failed to get %s: %v", key, err)
			}
			if val != i {
				t.Fatalf("Expected %d, got %d for key %s", i, val, key)
			}
		}
	}
}

func TestClusterAutoPipelineCrossSlot(t *testing.T) {
	ctx := context.Background()

	client := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{":7000", ":7001", ":7002"},
		AutoPipelineOptions: &redis.AutoPipelineOptions{
			MaxBatchSize:         20,
			MaxFlushDelay:        10 * time.Millisecond,
			MaxConcurrentBatches: 5,
			Unordered:            true,
		},
	})
	defer client.Close()

	skipIfClusterUnhealthy(t, client)

	if err := client.FlushDB(ctx).Err(); err != nil {
		t.Fatal(err)
	}

	ap, err := client.AutoPipeline()
	if err != nil {
		t.Fatal(err)
	}
	defer ap.Close()

	// Use keys that will hash to different slots
	keys := []string{
		"user:1000",
		"user:2000",
		"user:3000",
		"session:abc",
		"session:def",
		"cache:foo",
		"cache:bar",
	}

	var wg sync.WaitGroup
	for i, key := range keys {
		wg.Add(1)
		go func(k string, val int) {
			defer wg.Done()
			cmd := ap.Set(ctx, k, val, 0)
			// Wait for command to complete
			_ = cmd.Err()
		}(key, i)
	}

	wg.Wait()

	// Verify all keys were set
	for i, key := range keys {
		val, err := client.Get(ctx, key).Int()
		if err != nil {
			t.Fatalf("Failed to get %s: %v", key, err)
		}
		if val != i {
			t.Fatalf("Expected %d, got %d for key %s", i, val, key)
		}
	}
}
