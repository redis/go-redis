package redis

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/cpu"

	"github.com/redis/go-redis/v9/internal"
)

// AutoPipelineOptions configures the autopipelining behavior.
type AutoPipelineOptions struct {
	// MaxBatchSize is the target batch size: the accumulator stops waiting for
	// more commands once the shard queue reaches it, so a batch flushes promptly
	// instead of lingering. It is a soft threshold, not a hard cap — under heavy
	// concurrent enqueue (or while a flush waits on the concurrency semaphore) the
	// queue can grow past it and execute as a single larger pipeline, which is
	// safe and simply yields a deeper pipeline.
	// Default: 200
	MaxBatchSize int

	// MaxConcurrentBatches is the maximum number of pipeline batches that may
	// execute concurrently.
	//
	// Default: 1, which gives a single ordered command stream — batches execute
	// serially in submit order, so even a windowed caller (submit many, read
	// later) sees strict ordering, while still reaching high throughput via deep
	// pipelines (~3M ops/sec locally).
	//
	// Setting this above 1 runs batches in parallel for maximum throughput, but
	// commands then have NO guaranteed execution order. Because that trades away
	// ordering, it is only allowed together with Unordered: true — otherwise the
	// configuration is rejected (see Validate). This makes the trade-off
	// explicit: you cannot accidentally lose ordering by raising concurrency.
	MaxConcurrentBatches int

	// Unordered must be set to true to allow MaxConcurrentBatches > 1. It is the
	// caller's explicit acknowledgement that parallel batch execution gives up
	// command ordering in exchange for throughput. With the default (false),
	// MaxConcurrentBatches is forced to 1 (an ordered stream) and any value > 1
	// is a configuration error.
	Unordered bool

	// contentSharded is set internally by cluster wiring when commands are
	// routed to shards by content (slot), so same-key commands always share a
	// shard and per-key order holds even with several shards. It exempts that
	// wiring from the NumShards ordering check in newAutoPipeliner. Never set
	// by users (unexported).
	contentSharded bool

	// NumShards is the number of independent queue+flusher shards the
	// autopipeliner runs. 0 (the default) means auto: a single shard, which
	// funnels every caller into one queue so batches stay deep — measured
	// throughput and latency are best with one shard even under heavy
	// goroutine concurrency. Cluster clients default to several slot-routed
	// shards instead, so commands for different nodes queue independently
	// (per-key order still holds: a key's slot always maps to the same
	// shard). Raising NumShards splits the queue: it reduces enqueue-mutex
	// contention but fragments batches, which usually costs far more than the
	// contention saves. Every shard always has at least one concurrency
	// permit, so the effective global batch concurrency is
	// max(NumShards, MaxConcurrentBatches) — and because shards flush
	// concurrently, NumShards > 1 on the deferred (async) face requires
	// Unordered: true (construction fails otherwise).
	NumShards int

	// MaxFlushDelay is the maximum delay after flushing before checking for more commands.
	// A small delay (e.g., 100μs) can significantly reduce CPU usage by allowing
	// more commands to batch together, at the cost of slightly higher latency.
	//
	// Trade-off:
	// - 0 (default): Lowest latency, higher CPU usage
	// - 100μs: Balanced (recommended for most workloads)
	// - 500μs: Lower CPU usage, higher latency
	//
	// Based on benchmarks, 100μs can reduce CPU usage by 50%
	// while adding only ~100μs average latency per command.
	// Default: 0, meaning the flusher applies no coalescing wait — it flushes
	// each batch as soon as the queue is ready and lets in-flight backpressure
	// coalesce concurrent callers (see accumulateBatch). Set a value here to add
	// an explicit accumulation window, trading latency for larger batches / less
	// CPU as described above.
	MaxFlushDelay time.Duration

	// AdaptiveDelay enables smart delay calculation based on queue fill level.
	// When enabled, the delay is automatically adjusted:
	// - Queue ≥75% full: No delay (flush immediately to prevent overflow)
	// - Queue ≥50% full: 25% of MaxFlushDelay (queue filling up)
	// - Queue ≥25% full: 50% of MaxFlushDelay (moderate load)
	// - Queue <25% full: 100% of MaxFlushDelay (low load, maximize batching)
	//
	// This provides automatic adaptation to varying load patterns without
	// manual tuning. Uses integer-only arithmetic for optimal performance.
	// Default: false (use fixed MaxFlushDelay)
	AdaptiveDelay bool
}

// autoPipelinePermitBackstop bounds how long a flush waits for a concurrency
// permit when all are busy. It is only a safety net against a wedged semaphore:
// every permit holder releases it (via defer) and each batch Exec is itself
// bounded by the connection's read/write timeout, so in normal operation a
// permit frees long before this. It is set well above the default ReadTimeout
// and a maintnotifications relaxed window so a legitimately slow in-flight batch
// never makes waiters fail spuriously. The wait deliberately does NOT end on
// Close: commands taken from the queue were already accepted, and Close's
// contract is to flush them (it waits via wg/batchWg), so permit waits run on
// a background context bounded only by this backstop.
const autoPipelinePermitBackstop = 30 * time.Second

// numAutoPipelineShards is the shard-count default used by CLUSTER wiring,
// where commands are routed to shards by slot so different nodes' batches
// queue independently (every shard keeps at least one concurrency permit, so
// several shards can flush to their nodes in parallel regardless of
// MaxConcurrentBatches). It is NOT used for standalone clients: those default
// to one shard (see newAutoPipeliner), because a single deep queue pipelines
// far better than a fragmented one. Deliberately NOT derived from
// MaxConcurrentBatches — coupling shard count to the permit budget silently
// collapsed cluster slot routing to a single shard at the default budget.
func numAutoPipelineShards() int {
	n := runtime.GOMAXPROCS(0)
	if n < 1 {
		n = 1
	}
	const maxShards = 16
	if n > maxShards {
		n = maxShards
	}
	return n
}

// DefaultAutoPipelineOptions returns the default autopipelining configuration.
//
// The default is ordered: MaxConcurrentBatches is 1, so batches execute
// serially in submit order (a single ordered command stream) while still
// reaching high throughput via deep pipelines when callers submit in windows.
// To trade ordering for parallel-batch throughput, set MaxConcurrentBatches > 1
// together with Unordered: true.
func DefaultAutoPipelineOptions() *AutoPipelineOptions {
	return &AutoPipelineOptions{
		MaxBatchSize:         200,
		MaxConcurrentBatches: 1, // ordered by default
		MaxFlushDelay:        0, // lowest latency; no coalescing wait (batch via in-flight backpressure)
	}
}

// DefaultBlockingAutoPipelineOptions returns the default config for the
// blocking face (Client.AutoPipeline). It uses a single ordered batch stream
// (MaxConcurrentBatches: 1). Counterintuitively this maximizes throughput AND
// minimizes latency for the blocking face: with one batch in flight, callers whose
// commands return while it executes re-enqueue and flush together as the next
// batch, so batches stay deep (a near-continuous, double-buffered pipeline),
// while a lone caller flushes promptly in a single round-trip (no coalescing
// wait — see accumulateBatch). More parallel permits (MaxConcurrentBatches>1) do the
// opposite: each command finds a free permit and flushes on its own before
// others accumulate, collapsing batch size — and throughput — toward one command
// per round-trip while latency rises. For maximum throughput use the async face
// (AsyncAutoPipeline) with a window of in-flight commands (inflight>1); it keeps
// MaxConcurrentBatches: 1 as well.
func DefaultBlockingAutoPipelineOptions() *AutoPipelineOptions {
	return &AutoPipelineOptions{
		MaxBatchSize:         300,
		MaxConcurrentBatches: 1,
	}
}

// Validate reports whether the configuration is self-consistent. It returns an
// error if MaxConcurrentBatches > 1 without Unordered: true — raising
// concurrency gives up command ordering, so the caller must opt in explicitly.
func (cfg *AutoPipelineOptions) Validate() error {
	if cfg.MaxConcurrentBatches > 1 && !cfg.Unordered {
		return fmt.Errorf("redis: AutoPipelineOptions.MaxConcurrentBatches=%d requires Unordered:true "+
			"(parallel batches do not preserve command ordering); set Unordered:true to allow it, "+
			"or keep MaxConcurrentBatches=1 for an ordered stream", cfg.MaxConcurrentBatches)
	}
	// Reject obviously-wrong negatives so a typo surfaces at construction rather
	// than being silently coerced to a default. Zero is allowed and means "use
	// the default" (MaxBatchSize) or "no delay" (MaxFlushDelay).
	if cfg.MaxBatchSize < 0 {
		return fmt.Errorf("redis: AutoPipelineOptions.MaxBatchSize=%d must be >= 0", cfg.MaxBatchSize)
	}
	if cfg.MaxConcurrentBatches < 0 {
		return fmt.Errorf("redis: AutoPipelineOptions.MaxConcurrentBatches=%d must be >= 0", cfg.MaxConcurrentBatches)
	}
	if cfg.MaxFlushDelay < 0 {
		return fmt.Errorf("redis: AutoPipelineOptions.MaxFlushDelay=%s must be >= 0", cfg.MaxFlushDelay)
	}
	if cfg.NumShards < 0 {
		return fmt.Errorf("redis: AutoPipelineOptions.NumShards=%d must be >= 0", cfg.NumShards)
	}
	if cfg.AdaptiveDelay && cfg.MaxFlushDelay <= 0 {
		return fmt.Errorf("redis: AutoPipelineOptions.AdaptiveDelay requires MaxFlushDelay > 0 " +
			"(adaptive delay scales MaxFlushDelay by queue fill; with no MaxFlushDelay it would " +
			"silently disable batch accumulation entirely)")
	}
	return nil
}

// cmdableClient is an interface for clients that support pipelining.
// Both Client and ClusterClient implement this interface. It embeds
// UniversalClient (Cmdable + Process + Do + AddHook + Watch + Subscribe... +
// Close + PoolStats) so the AutoPipeliner can delegate the non-batched surface
// back to the underlying client and itself satisfy UniversalClient.
type cmdableClient interface {
	UniversalClient
	// processPipelineHook is the hook-wrapped []Cmder pipeline entry — the same
	// method Pipeline.Exec is wired to (see Client.Pipeline). The flusher
	// dispatches drained batches through it directly, skipping the per-batch
	// Pipeline construction; hooks/OTel see the identical call.
	processPipelineHook(ctx context.Context, cmds []Cmder) error
}

// apBatch is the completion signal shared by every command flushed together.
// Its done channel is closed exactly once, when the batch's pipeline has
// executed. Closing one channel wakes all waiters in a single operation,
// instead of doing one buffered-channel send per command — under high
// concurrency the per-command sends dominated CPU (channel-lock contention and
// one goroutine wake-up apiece).
type apBatch struct {
	done chan struct{}
}

func newAPBatch() *apBatch { return &apBatch{done: make(chan struct{})} }

// The shard queue stores bare Cmders. The batch a command waits on is the
// shard's curBatch at enqueue time — read once to wire the command's ready
// channel and never needed per-command afterward (the flusher closes the one
// shared batch). Storing []Cmder removes a per-command wrapper allocation.

var queueSlicePool = sync.Pool{
	New: func() interface{} { s := make([]Cmder, 0, 100); return &s },
}

func getQueueSlice(capacity int) []Cmder {
	slice := (*queueSlicePool.Get().(*[]Cmder))[:0]
	if cap(slice) < capacity {
		queueSlicePool.Put(&slice)
		return make([]Cmder, 0, capacity)
	}
	return slice
}

func putQueueSlice(slice []Cmder) {
	if cap(slice) <= 1000 {
		// Zero only the used prefix: elements beyond len are already nil —
		// slices enter the pool fully zeroed (here) and are only appended to
		// afterwards, so the tail invariant holds. Zeroing the whole capacity
		// memclr'd up to 8 KB per flush for small batches on large recycled
		// arrays.
		for i := range slice {
			slice[i] = nil
		}
		queueSlicePool.Put(&slice)
	}
}

// AutoPipeliner automatically batches commands and executes them in pipelines.
// It's safe for concurrent use by multiple goroutines.
//
// AutoPipeliner works by collecting commands from multiple goroutines into a
// shared queue and flushing them as one Redis pipeline when the batch reaches
// MaxBatchSize or a configured coalescing window (MaxFlushDelay) elapses. By
// default there is no window: each batch flushes as soon as the queue is ready
// and concurrent callers coalesce via in-flight backpressure, so a lone command
// flushes in a single round-trip while batches stay deep under load.
//
// This provides significant performance improvements for workloads with many
// concurrent small operations, as it reduces the number of network round-trips.
//
// AutoPipeliner implements the Cmdable interface, so you can use it like a
// regular client. Prefer the typed methods (Set, Get, ...); Do runs OUTSIDE
// the pipeline on a normal connection (see Do).
// AutoPipeline / AsyncAutoPipeline return an error for an invalid config, so check it once:
//
//	ap, err := client.AutoPipeline()
//	if err != nil {
//		return err
//	}
//	ap.Set(ctx, "key", "value", 0)
//	ap.Get(ctx, "key")
//	ap.Close()
//
// Per-command contexts: a command is batched and executed on the AutoPipeliner's
// own long-lived context, NOT the context passed to the command. A per-command
// deadline or cancellation is therefore not honored once the command is queued
// (this is deliberate — a per-batch timer per command would cost a goroutine
// each). Use a plain client for commands that need their own deadline.
// The one exception is a blocking command (readTimeout() != nil, e.g. BLPOP):
// it is never batched and runs directly on the caller's context, which is
// honored as usual.
//
// Retries: like any pipeline, a batch that fails on a network error is retried
// as a whole (up to Options.MaxRetries). If the connection drops after the
// server executed part of the batch, non-idempotent commands (INCR, LPUSH, ...)
// may execute twice. Run commands that must not be retransmitted on a plain
// client, or set MaxRetries: -1.
//
// Lifetime: AutoPipeline() returns a single, client-owned instance shared by all
// callers. Close()ing it (or closing the client) stops the shared pipeliner for
// everyone; a later AutoPipeline() call builds a fresh one.
type AutoPipeliner struct {
	cmdable // Embed cmdable to get all Redis command methods

	pipeliner cmdableClient
	config    *AutoPipelineOptions
	// blocking selects how the typed command surface (Set, Get, ...) behaves:
	// when true the command call itself blocks until the command has executed
	// (drop-in, synchronous shape); when false the call returns immediately and
	// the result accessors (Val/Result/Err) block. See AutoPipeline (blocking)
	// vs AsyncAutoPipeline (deferred).
	blocking bool

	// Sharded command queues. Each shard has its own queue, mutex and flusher
	// goroutine, so enqueues from many goroutines spread across shards instead
	// of all contending on a single mutex and being drained by a single
	// flusher. Commands are assigned to shards round-robin; per-goroutine
	// ordering is still guaranteed because Do blocks for each command's result
	// before issuing the next one.
	shards []*apShard
	next   atomic.Uint32 // round-robin shard selector
	// shardFn, when set, picks a command's shard from its content (cluster mode
	// sets it to route by slot so all commands for one node land in the same
	// shard's batch — keeping per-node pipelines deep instead of splitting every
	// batch across nodes). When nil, commands are assigned round-robin.
	shardFn func(Cmder) int

	// expectedArrivals counts how many commands the engine expects to arrive
	// at any moment: a completed batch of N≥2 commands wakes its N waiters
	// together, and in a closed loop each immediately submits its next command
	// — so completion announces N expected arrivals, and every enqueue accounts
	// for one. The default coalescing wait (awaitExpectedArrivals) holds the
	// flusher while arrivals are still expected, so the whole wakeup wave
	// flushes as one deep pipeline — an exact count, not a smoothed estimate,
	// which cannot ratchet into fragmentation. Single-command batches announce
	// nothing, so a lone caller and open-loop traffic never wait. May
	// transiently go negative (arrivals nobody announced); readers clamp to
	// zero. Pipeliner-global, not per-shard: cluster routing may land a
	// follow-up on a different shard than the batch that woke its caller.
	expectedArrivals atomic.Int64

	// execEWMA is an exponentially-weighted moving average (alpha 1/8) of
	// batch execution time in nanoseconds — the engine's own view of the
	// server round-trip. It scales awaitExpectedArrivals's silence fallback so a
	// wave staggered by scheduling on a slow link is not split mid-landing. Updates
	// are racy read-modify-writes by design: losing an occasional sample is
	// harmless for a smoothing heuristic. 0 means "no sample yet".
	execEWMA atomic.Int64

	// Lifecycle
	ctx     context.Context
	cancel  context.CancelFunc
	wg      sync.WaitGroup // Tracks flusher goroutines
	batchWg sync.WaitGroup // Tracks batch execution goroutines
	closed  atomic.Bool
}

// apShard is one queue + flusher. Its fields are touched only by enqueuing
// goroutines (under mu) and by its own single flusher goroutine.
// apEnqueueStripes is how many enqueue stripes a shard runs when striping is
// safe (unordered configs, and every blocking-face shard — a blocking caller
// waits for each command, so stripes cannot reorder its stream). The
// enqueue mutex is the hottest lock in the engine (128 concurrent callers on
// one shard spend ~half their CPU in lock slow paths); striping the queue
// spreads that contention while the flusher still drains every stripe into ONE
// merged pipeline, so batches stay deep. Ordered shards always use a single
// stripe: with several stripes a caller's consecutive commands can land in
// stripes on opposite sides of an in-progress drain and execute out of order.
const apEnqueueStripes = 8

// apStripe is one striped slice of a shard's enqueue queue. Each stripe has
// its own batch-completion signal so a drain can take stripes one lock at a
// time; every batch taken in one drain completes together after the merged
// pipeline executes. Padded so neighbouring stripes' mutexes do not share a
// cache line.
type apStripe struct {
	mu       sync.Mutex
	queue    []Cmder
	queueLen atomic.Int32
	curBatch *apBatch // completion signal for currently-queued cmds
	// Pad each stripe onto its own cache line(s). Without it, one stripe's hot
	// fields (queueLen/curBatch) share a cache line with the NEXT stripe's
	// contended mutex, so a lock-free counter bump on stripe i invalidates the
	// line a different core is trying to lock stripe i+1 on — false sharing
	// that measured ~16x on a contended microbenchmark. cpu.CacheLinePad is
	// sized per GOARCH (64 B on x86-64/arm64, 128 B on ppc64, 256 B on s390x),
	// so this is correct on every target rather than a hand-tuned constant.
	_ cpu.CacheLinePad
}

type apShard struct {
	ap *AutoPipeliner

	next    atomic.Uint32           // round-robin stripe pick (unordered mode)
	stripes []apStripe              // 1 stripe when ordered, apEnqueueStripes when Unordered
	notify  chan struct{}           // buffered (cap 1) enqueue wake-up
	sem     *internal.FIFOSemaphore // per-shard concurrent-batch budget

	// inFlight counts this shard's dispatched-but-unfinished batches. When it
	// is zero and no arrivals are expected, the shard is idle and a
	// new command flushes immediately; when batches are in flight, arrivals
	// are mid-stream and the flusher holds them briefly to coalesce (see
	// awaitExpectedArrivals).
	inFlight atomic.Int32
}

// stripe picks the enqueue stripe for the next command: the single stripe in
// ordered mode (preserving strict FIFO), round-robin in unordered mode.
func (s *apShard) stripe() *apStripe {
	if len(s.stripes) == 1 {
		return &s.stripes[0]
	}
	return &s.stripes[s.next.Add(1)%uint32(len(s.stripes))]
}

// getOrCreateAutoPipeliner is the shared caching protocol behind the four
// AutoPipeline/AsyncAutoPipeline getters (Client and ClusterClient, each
// face): return the cached live instance, refuse on a closed client, or build
// and cache a new one. The caller supplies its cached-slot pointer, its
// closed flag (both guarded by the mutex), the explicit-config override, the
// fallback config, and a build closure (the cluster one wraps
// clusterAutoPipelineOptions and installs slot sharding).
func getOrCreateAutoPipeliner(
	mu *sync.Mutex,
	slot **AutoPipeliner,
	closed *bool,
	override *AutoPipelineOptions,
	fallback func() *AutoPipelineOptions,
	build func(*AutoPipelineOptions) (*AutoPipeliner, error),
) (*AutoPipeliner, error) {
	mu.Lock()
	defer mu.Unlock()
	if *closed {
		return nil, ErrClosed
	}
	if *slot != nil && !(*slot).closed.Load() {
		return *slot, nil
	}
	cfg := fallback()
	if override != nil {
		cfg = override
	}
	ap, err := build(cfg)
	if err != nil {
		return nil, err
	}
	*slot = ap
	return ap, nil
}

// newAutoPipeliner builds an autopipeliner in either blocking or deferred mode.
// It is unexported on purpose: the public entry points are
// Client/ClusterClient.AutoPipeline and AsyncAutoPipeline, which also install
// cluster slot-sharding. Constructing one directly would skip that wiring and
// give a *ClusterClient degraded (cross-node) batching.
func newAutoPipeliner(pipeliner cmdableClient, config *AutoPipelineOptions, blocking bool) (*AutoPipeliner, error) {
	if config == nil {
		config = DefaultAutoPipelineOptions()
	} else {
		// Copy so default-filling below doesn't mutate the caller's struct — the
		// same *AutoPipelineOptions may be shared across clients (e.g. a reused
		// Options.AutoPipelineOptions), and callers may inspect it afterward.
		cfgCopy := *config
		config = &cfgCopy
	}

	// Apply defaults for zero values
	if config.MaxBatchSize <= 0 {
		config.MaxBatchSize = 200
	}

	if config.MaxConcurrentBatches <= 0 {
		// Default to an ordered single stream. Callers raise this (with
		// Unordered:true) to opt into parallel-batch throughput.
		config.MaxConcurrentBatches = 1
	}

	// Reject configurations that silently drop ordering. This is a deterministic
	// misconfiguration; surface it as an error so the post-init AutoPipeline /
	// AsyncAutoPipeline calls never panic on a bad config.
	if err := config.Validate(); err != nil {
		return nil, err
	}

	// NumShards > 1 on the deferred (async) face distributes commands
	// round-robin across shards that flush concurrently, so submit order is
	// not preserved — require the explicit Unordered opt-in, exactly like
	// MaxConcurrentBatches > 1. The blocking face is exempt (each caller waits
	// per command, and Submit is rejected there), as is cluster slot sharding
	// (contentSharded: same-key commands always land in the same shard, so
	// per-key order holds).
	if config.NumShards > 1 && !config.Unordered && !blocking && !config.contentSharded {
		return nil, fmt.Errorf(
			"redis: AutoPipelineOptions.NumShards=%d requires Unordered:true on the deferred (async) face "+
				"(commands are distributed round-robin across shards, which flush concurrently and do not preserve submit order)",
			config.NumShards)
	}

	ctx, cancel := context.WithCancel(context.Background())

	ap := &AutoPipeliner{
		pipeliner: pipeliner,
		config:    config,
		blocking:  blocking,
		ctx:       ctx,
		cancel:    cancel,
	}

	// Route the typed command surface. Blocking: the command call blocks until
	// executed (synchronous drop-in shape). Deferred: the call returns at once
	// and the result accessors block until the batch executes.
	if blocking {
		ap.cmdable = ap.processBlocking
	} else {
		ap.cmdable = ap.processAsync
	}

	// Pick the shard count. NumShards=0 (auto) means ONE shard: a single deep
	// queue outperforms a sharded one because batches stay large — sharding by
	// core count coupled batch fragmentation to MaxConcurrentBatches and
	// collapsed pipelining (measured: 16 shards cut async throughput ~4x and
	// tripled latency versus one shard at the same permit count). Cluster
	// wiring passes an explicit NumShards so slot-routed shards keep each
	// batch on one node.
	nShards := config.NumShards
	if nShards <= 0 {
		nShards = 1
	}
	// Split the concurrent-batch budget across shards so each shard has its own
	// semaphore. A single shared semaphore became a contention point once the
	// per-shard queue mutexes were no longer the bottleneck. Integer division
	// drops a remainder, so hand the leftover permits to the first shards: the
	// per-shard permits then sum to exactly MaxConcurrentBatches.
	perShard := config.MaxConcurrentBatches / nShards
	remainder := config.MaxConcurrentBatches % nShards
	if perShard < 1 {
		// Budget smaller than the shard count: give every shard one permit so
		// each flusher can still make progress. The sum then exceeds the
		// configured budget, which is unavoidable with per-shard semaphores.
		perShard = 1
		remainder = 0
	}
	ap.shards = make([]*apShard, nShards)
	for i := range ap.shards {
		permits := perShard
		if i < remainder {
			permits++
		}
		// Stripe when reordering is impossible or waived: a BLOCKING caller
		// waits for each command before issuing its next, so its per-goroutine
		// order holds no matter which stripe each command lands in; the async
		// face may only stripe when the user set Unordered. The remaining case
		// (async, ordered) keeps one stripe to preserve strict submit order.
		nStripes := 1
		if config.Unordered || blocking {
			nStripes = apEnqueueStripes
		}
		s := &apShard{
			ap:      ap,
			notify:  make(chan struct{}, 1),
			stripes: make([]apStripe, nStripes),
			sem:     internal.NewFIFOSemaphore(int32(permits)),
		}
		for j := range s.stripes {
			s.stripes[j].queue = getQueueSlice(config.MaxBatchSize)
			s.stripes[j].curBatch = newAPBatch()
		}
		ap.shards[i] = s
		ap.wg.Add(1)
		go s.flusher()
	}

	return ap, nil
}

// Do executes a raw command on a NORMAL connection, outside the pipeline.
// Arbitrary command names can carry connection state (SELECT, MULTI, SUBSCRIBE,
// CLIENT ...) or block the connection (BLPOP ...); batching those onto a shared
// pipeline connection would silently poison it for every later batch, or stall
// unrelated commands. The typed surface (ap.Set, ap.Get, ...) is safe by
// construction and IS batched — prefer it. Do carries the same caveats as
// Client.Do: a stateful command still affects the (normal, non-pipeline)
// pooled connection it runs on. Do keeps each face's call shape: on
// a blocking autopipeliner the call blocks until the command has executed; on a
// deferred (async) one it returns immediately and the command's result
// accessors (Err/Val/Result) block until it completes.
func (ap *AutoPipeliner) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	if len(args) == 0 {
		cmd.SetErr(errDoNoArgs)
		return cmd
	}
	if ap.closed.Load() {
		cmd.SetErr(ErrClosed)
		return cmd
	}

	if ap.blocking {
		_ = ap.pipeliner.Process(ctx, cmd)
		return cmd
	}
	// Deferred face: keep the returns-immediately shape. Run on a normal
	// connection in the background; the ready channel makes the command's
	// result accessors block until it completes. Not tracked by batchWg — a
	// Do racing Close simply fails with a closed-pool error like any other
	// in-flight command on a closing client.
	done := make(chan struct{})
	cmd.setReady(done)
	go func() {
		defer close(done)
		_ = ap.pipeliner.Process(ctx, cmd)
	}()
	return cmd
}

// Process queues a command for autopipelined execution, following the
// autopipeliner's mode like the typed methods and Do: on a blocking
// autopipeliner the call blocks until the command has executed; on a deferred
// (async) one it returns immediately and reading the command's result
// (Val/Result/Err) blocks until its batch is flushed.
func (ap *AutoPipeliner) Process(ctx context.Context, cmd Cmder) error {
	return ap.cmdable(ctx, cmd)
}

// The methods below complete the UniversalClient surface by delegating to the
// underlying client. They are NOT autopipelined — pub/sub, transactions (Watch),
// hooks, Do and pool stats cannot be batched — so an AutoPipeliner used as a
// UniversalClient batches only the typed data commands; everything here runs on
// the underlying client exactly as it would there.
//
// Note on lifecycle: Close() (defined elsewhere) closes the AUTOPIPELINER —
// drains in-flight batches and stops flushers — but does NOT close the
// underlying client, whose lifecycle is owned by whoever created it.

// AddHook adds a hook to the underlying client. Autopipelined batches are hooked
// too, since dispatch goes through the hook-wrapped pipeline entry.
func (ap *AutoPipeliner) AddHook(hook Hook) { ap.pipeliner.AddHook(hook) }

// Watch runs a transactional function on the underlying client (not batched).
func (ap *AutoPipeliner) Watch(ctx context.Context, fn func(*Tx) error, keys ...string) error {
	return ap.pipeliner.Watch(ctx, fn, keys...)
}

// Subscribe opens a pub/sub on the underlying client (not batched — pub/sub
// needs a dedicated connection).
func (ap *AutoPipeliner) Subscribe(ctx context.Context, channels ...string) *PubSub {
	return ap.pipeliner.Subscribe(ctx, channels...)
}

// PSubscribe opens a pattern pub/sub on the underlying client (not batched).
func (ap *AutoPipeliner) PSubscribe(ctx context.Context, channels ...string) *PubSub {
	return ap.pipeliner.PSubscribe(ctx, channels...)
}

// SSubscribe opens a sharded pub/sub on the underlying client (not batched).
func (ap *AutoPipeliner) SSubscribe(ctx context.Context, channels ...string) *PubSub {
	return ap.pipeliner.SSubscribe(ctx, channels...)
}

// PoolStats returns the underlying client's connection pool statistics.
func (ap *AutoPipeliner) PoolStats() *PoolStats { return ap.pipeliner.PoolStats() }

// AutoPipeline delegates to the underlying client, which returns its cached
// autopipeliner (typically this same instance). Present to satisfy the
// UniversalClient surface.
func (ap *AutoPipeliner) AutoPipeline() (*AutoPipeliner, error) {
	return ap.pipeliner.AutoPipeline()
}

// AutoPipelineWithOptions delegates to the underlying client.
func (ap *AutoPipeliner) AutoPipelineWithOptions(config *AutoPipelineOptions) (*AutoPipeliner, error) {
	return ap.pipeliner.AutoPipelineWithOptions(config)
}

// AsyncAutoPipeline delegates to the underlying client. Present to satisfy the
// UniversalClient surface.
func (ap *AutoPipeliner) AsyncAutoPipeline() (*AutoPipeliner, error) {
	return ap.pipeliner.AsyncAutoPipeline()
}

// AsyncAutoPipelineWithOptions delegates to the underlying client.
func (ap *AutoPipeliner) AsyncAutoPipelineWithOptions(config *AutoPipelineOptions) (*AutoPipeliner, error) {
	return ap.pipeliner.AsyncAutoPipelineWithOptions(config)
}

// validate AutoPipeliner implements UniversalClient (drop-in for the real
// clients; non-data operations delegate to the underlying client).
var _ UniversalClient = (*AutoPipeliner)(nil)

// AutoFuture is the handle returned by Submit. Call Wait (or Result on the
// command after Wait) once the result is needed; it blocks only until the
// command's batch has executed.
type AutoFuture struct {
	cmd   Cmder
	batch *apBatch
}

// Wait blocks until the submitted command has executed, then returns its error.
// The zero AutoFuture (no submitted command) returns an error rather than
// panicking.
func (f AutoFuture) Wait() error {
	if f.batch == nil {
		if f.cmd != nil {
			return f.cmd.Err()
		}
		return errZeroAutoFuture
	}
	<-f.batch.done
	return f.cmd.Err()
}

// WaitContext is like Wait but stops waiting when ctx is done. The command
// still executes and its result remains readable once its batch completes —
// ctx abandons only this wait, it does not cancel the command (per-command
// contexts are not honored after enqueue; see the AutoPipeliner doc).
//
// After a ctx error the result may simply not be there YET: the batch is
// still in flight and may populate the command at any moment, so do not read
// Cmd()'s value or error directly — that races the executing batch. Call Wait
// (or WaitContext with a fresh context) again; once it returns a non-context
// error, the command's result is complete and safe to read.
func (f AutoFuture) WaitContext(ctx context.Context) error {
	if f.batch == nil {
		if f.cmd != nil {
			return f.cmd.Err()
		}
		return errZeroAutoFuture
	}
	select {
	case <-f.batch.done:
		return f.cmd.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Cmd returns the underlying command (call Wait first before reading results).
func (f AutoFuture) Cmd() Cmder { return f.cmd }

// submit queues a command without blocking and returns its completion future.
func (ap *AutoPipeliner) submit(ctx context.Context, cmd Cmder) AutoFuture {
	if cmd.readTimeout() != nil {
		// Blocking commands are executed directly, outside the pipeline. They
		// still must respect a closed AutoPipeliner: enqueue() rejects on the
		// batched path, so mirror that here instead of running after Close().
		if ap.closed.Load() {
			cmd.SetErr(ErrClosed)
			return AutoFuture{cmd: cmd, batch: completedBatch}
		}
		_ = ap.pipeliner.Process(ctx, cmd)
		return AutoFuture{cmd: cmd, batch: completedBatch}
	}
	return AutoFuture{cmd: cmd, batch: ap.enqueue(cmd)}
}

// errSubmitBlockingFace rejects Submit on the blocking face: Submit does not
// wait, so a windowed caller could have several commands in flight at once —
// but the blocking face stripes its enqueue queue on the strength of every
// caller waiting per command, and a non-waiting window there can be reordered.
// The deferred face (AsyncAutoPipeline) is built for exactly that usage.
var errSubmitBlockingFace = errors.New(
	"redis: Submit requires the deferred autopipeliner (AsyncAutoPipeline); on the blocking face use the typed methods or Do")

// errZeroAutoFuture is returned by Wait/WaitContext on a zero AutoFuture.
var errZeroAutoFuture = errors.New("redis: Wait on a zero AutoFuture")

// errDoNoArgs is returned by Do when called without a command.
var errDoNoArgs = errors.New("redis: AutoPipeliner.Do requires at least one argument")

// Submit queues a command without blocking and returns an AutoFuture; Wait on
// it when the result is needed. This is the explicit form for working with raw
// Cmders on the deferred (async) face, where the typed methods (Set, Get, ...)
// provide the same deferred behaviour returning the usual *XxxCmd. On a
// BLOCKING autopipeliner Submit is rejected (the future's Wait returns an
// error): the blocking face's ordering relies on every caller waiting for each
// command before issuing the next, which Submit by design does not do.
func (ap *AutoPipeliner) Submit(ctx context.Context, cmd Cmder) AutoFuture {
	if ap.blocking {
		cmd.SetErr(errSubmitBlockingFace)
		return AutoFuture{cmd: cmd, batch: completedBatch}
	}
	return ap.submit(ctx, cmd)
}

// processAsync is the cmdable backing the typed command surface: it queues a
// command without blocking the caller and marks it ready so the command's
// result accessors (Val/Result/Err) block until the batch executes. This gives
// the autopipeliner the full typed surface (ap.Set, ap.Get, ...) with the exact
// same call shape as a normal client — only the wait is deferred to the point a
// result is read.
func (ap *AutoPipeliner) processAsync(ctx context.Context, cmd Cmder) error {
	f := ap.submit(ctx, cmd)
	cmd.setReady(f.batch.done)
	return nil
}

// processBlocking is the cmdable backing the blocking face: it queues the
// command and blocks until its batch has executed, so the command call has the
// same synchronous shape as a normal client (the returned *XxxCmd already holds
// its result). The flusher still batches this command with other concurrent
// callers' commands into a pipeline, so throughput is far above a plain client
// even though each caller waits. Per-goroutine ordering holds regardless of
// MaxConcurrentBatches: a caller cannot issue its next command until this one
// returns, so its commands execute in submit order.
func (ap *AutoPipeliner) processBlocking(ctx context.Context, cmd Cmder) error {
	return ap.submit(ctx, cmd).Wait()
}

// completedBatch is a reusable already-completed batch: returned both for
// commands that already executed directly (blocking commands, Submit-time
// rejections) and for error cases like enqueue-after-Close, so Wait returns
// immediately and the command's own error tells the story.
var completedBatch = func() *apBatch {
	b := newAPBatch()
	close(b.done)
	return b
}()

// enqueue queues a command and returns the batch whose done channel completes
// when it has executed. On a closed autopipeliner it errors the command and
// returns the already-closed batch.
func (ap *AutoPipeliner) enqueue(cmd Cmder) *apBatch {
	if ap.closed.Load() {
		cmd.SetErr(ErrClosed)
		return completedBatch
	}

	// Pick a shard. With shardFn (cluster mode) route by command content so all
	// commands for one node collect in the same shard's batch; otherwise spread
	// round-robin to keep each shard's mutex lightly contended.
	var s *apShard
	if ap.shardFn != nil {
		idx := ap.shardFn(cmd)
		if idx < 0 {
			idx = -idx
		}
		s = ap.shards[idx%len(ap.shards)]
	} else if len(ap.shards) == 1 {
		// Single shard (the standalone default): skip the round-robin counter —
		// it is a shared cache line bumped by every enqueue for a pick that is
		// constant. Same guard the stripe pick already has.
		s = ap.shards[0]
	} else {
		// Unsigned modulo: converting to int first goes negative after the
		// uint32 counter passes 2^31 on 32-bit platforms and panics.
		s = ap.shards[int((ap.next.Add(1)-1)%uint32(len(ap.shards)))]
	}

	st := s.stripe()
	st.mu.Lock()
	// Re-check closed under the stripe lock (see Close): either we win the lock
	// first and the shutdown drain flushes us, or the drain ran first and we
	// reject here — so a late enqueue never hangs on an unclosed done.
	if ap.closed.Load() {
		st.mu.Unlock()
		cmd.SetErr(ErrClosed)
		return completedBatch
	}
	batch := st.curBatch
	st.queue = append(st.queue, cmd)
	st.queueLen.Store(int32(len(st.queue)))
	st.mu.Unlock()

	// One expected arrival has landed (see expectedArrivals).
	ap.expectedArrivals.Add(-1)

	s.wake()
	return batch
}

// wake signals the shard's flusher that work is available without blocking.
func (s *apShard) wake() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// IsBlocking reports which face this autopipeliner is: true for the blocking
// face (Client.AutoPipeline — calls wait for execution), false for the
// deferred face (AsyncAutoPipeline — calls return immediately and result
// accessors block). The two faces reject different usage (Submit is
// blocking-face-rejected), so code handed an *AutoPipeliner can branch on
// this instead of probing with errors.
func (ap *AutoPipeliner) IsBlocking() bool { return ap.blocking }

// Config returns a copy of the effective configuration (defaults filled in).
func (ap *AutoPipeliner) Config() AutoPipelineOptions { return *ap.config }

// IsClosed reports whether the AutoPipeliner has been closed, either by an
// explicit Close or by closing the owning client. A closed AutoPipeliner
// rejects new commands with ErrClosed.
func (ap *AutoPipeliner) IsClosed() bool {
	return ap.closed.Load()
}

// numShards reports how many shards this autopipeliner runs.
func (ap *AutoPipeliner) numShards() int { return len(ap.shards) }

// setShardFn installs a content-based shard selector (cluster mode routes by
// slot so each shard's batch stays on one node). Must be called before the
// autopipeliner is used. Not safe to change concurrently with enqueues.
func (ap *AutoPipeliner) setShardFn(fn func(Cmder) int) { ap.shardFn = fn }

// Close stops the autopipeliner and flushes any pending commands.
func (ap *AutoPipeliner) Close() error {
	if !ap.closed.CompareAndSwap(false, true) {
		return nil // Already closed
	}

	// Cancel context to stop flushers
	ap.cancel()

	// Wake every shard's flusher so each observes the cancelled context promptly.
	for _, s := range ap.shards {
		s.wake()
	}

	// Wait for flushers to finish
	ap.wg.Wait()

	// Final sweep: a command can pass enqueue's under-lock closed-recheck just
	// before Close's CompareAndSwap and append to a shard AFTER that shard's
	// flusher has already drained and exited — leaving its batch.done unclosed
	// and the caller's accessor blocked forever. With the flushers provably gone
	// (wg.Wait above), drain each shard once more under its lock. This pairs with
	// the closed-recheck in enqueue: s.mu serializes the two, so either the late
	// enqueue appends first and this sweep flushes it, or the sweep runs first
	// and the enqueue then observes closed==true and rejects with ErrClosed.
	for _, s := range ap.shards {
		s.flushBatchSliceShutdown()
	}

	// Wait for all batch execution goroutines to finish
	ap.batchWg.Wait()

	return nil
}

// flusher is the per-shard background goroutine that flushes batches.
func (s *apShard) flusher() {
	defer s.ap.wg.Done()
	ap := s.ap

	for {
		// Wait for a command to arrive (or shutdown). The notify channel is a
		// cheap buffered wake-up; no lock is taken on the hot enqueue path.
		if s.Len() == 0 {
			select {
			case <-s.notify:
			case <-ap.ctx.Done():
			}
		}

		// Check if context is cancelled
		if ap.ctx.Err() != nil {
			// Final flush before shutdown - use background context to avoid immediate cancellation
			s.flushBatchSliceShutdown()
			return
		}

		// Apply the coalescing window if one is configured (MaxFlushDelay /
		// AdaptiveDelay). With the default config this returns at once: batching
		// under concurrent load comes from in-flight backpressure, not a wait —
		// see accumulateBatch.
		s.accumulateBatch()

		// Flush all pending commands
		for s.Len() > 0 {
			select {
			case <-ap.ctx.Done():
				// Final flush before shutdown
				s.flushBatchSliceShutdown()
				return
			default:
			}

			s.flushBatchSlice()

			// Between batches, apply the configured window again so the next
			// pipeline is also full. A no-op with the default config (see
			// accumulateBatch); the next drain picks up whatever has queued.
			if s.Len() > 0 && s.Len() < ap.config.MaxBatchSize {
				s.accumulateBatch()
			}
		}
	}
}

// accumulateBatch lets commands pile up before the flusher drains the queue,
// so pipelines carry many commands instead of one. It returns as soon as any
// of these holds:
//
//   - the queue reaches MaxBatchSize (batch is full);
//   - a configured MaxFlushDelay / AdaptiveDelay window elapses; or
//   - with no configured window (the default), the expected resubmission
//     wave of arrivals has landed — see awaitExpectedArrivals.
//
// A configured MaxFlushDelay / AdaptiveDelay is an intentional accumulation
// window and is waited in full (AdaptiveDelay scales it down as the queue fills
// and returns 0 — flush now — once the queue is ≥75% full).
func (s *apShard) accumulateBatch() {
	ap := s.ap
	batchSize := ap.config.MaxBatchSize
	if batchSize <= 0 {
		batchSize = 1
	}
	if s.Len() >= batchSize {
		return
	}

	// Pick the accumulation window. calculateDelay returns 0 both when no
	// MaxFlushDelay is configured (the default) and when AdaptiveDelay resolves
	// the current fill level to "flush immediately". The fill level is this
	// shard's own length — each shard flushes independently, so a global count
	// would mis-tune a quiet shard while another is busy.
	window := ap.calculateDelay(s.Len())
	if window <= 0 {
		if ap.config.MaxFlushDelay == 0 && !ap.config.AdaptiveDelay {
			// Default: coalesce by expected-arrival count, not by wall-clock.
			s.awaitExpectedArrivals(batchSize)
		}
		return
	}

	// Explicit window: wait the whole delay (or until the batch fills). Each
	// enqueue sends on notify, so we re-check the queue length on every wake-up
	// and return once the batch is full.
	deadline := time.NewTimer(window)
	defer deadline.Stop()
	for {
		select {
		case <-ap.ctx.Done():
			return
		case <-deadline.C:
			return
		case <-s.notify:
			if s.Len() >= batchSize {
				return
			}
		}
	}
}

// silenceGapFloor / silenceGapCeil bound awaitExpectedArrivals's silence fallback.
// The floor covers fast links; the RTT-scaled value (execEWMA/8) takes over on
// slow ones, where a wakeup wave staggered by goroutine scheduling can pause
// longer than the floor mid-landing and a premature flush is expensive (each
// batch fragment occupies a pipeline connection for a full round trip). The
// ceiling bounds how long a stale expectation (callers that left) can delay a
// flush.
const (
	silenceGapFloor = 200 * time.Microsecond
	silenceGapCeil  = 2 * time.Millisecond
)

// coalesceMinFlush is the smallest pipeline worth dispatching while other
// batches are still executing. Below it, a gap-fire holds the queued
// stragglers for the next wave instead of burning a connection on a
// near-empty flush; once nothing is in flight, any size flushes immediately.
const coalesceMinFlush = 8

// observeBatchExec folds one batch execution duration into execEWMA.
func (ap *AutoPipeliner) observeBatchExec(d time.Duration) {
	sample := int64(d)
	if sample <= 0 {
		return
	}
	old := ap.execEWMA.Load()
	if old == 0 {
		ap.execEWMA.Store(sample)
		return
	}
	ap.execEWMA.Store(old + (sample-old)/8)
}

// silenceGap returns the silence fallback for awaitExpectedArrivals, scaled to the
// observed batch round-trip: clamp(execEWMA/8, floor, ceil).
func (ap *AutoPipeliner) silenceGap() time.Duration {
	g := time.Duration(ap.execEWMA.Load() / 8)
	if g < silenceGapFloor {
		return silenceGapFloor
	}
	if g > silenceGapCeil {
		return silenceGapCeil
	}
	return g
}

// awaitExpectedArrivals holds the flusher while related work is in motion, so
// commands flush as deep pipelines instead of fragmenting into small batches
// (each fragment costs a pipeline connection for a full round trip). Two
// signals — both facts the engine already has, not wall-clock guesses — decide
// whether anything is imminent:
//
//   - expectedArrivals: a completed batch of N commands wakes its N waiters
//     together, and in a closed loop each immediately submits its next
//     command. Completion announces the exact count; every enqueue accounts
//     for one; the wait ends the moment the count drains — the wave of
//     arrivals has fully landed. An exact per-wave count has no failure mode
//     where an averaged estimate undershoots the true wave and locks the
//     engine into fragmented flushes.
//   - inFlight: batches still executing mean their waiters will wake shortly
//     and stragglers are mid-stream — worth holding a moment to coalesce with,
//     bounded by the silence gap. This also recovers a fragmented state (many
//     singles in flight, which announce nothing): their staggered returns land
//     within one gap, merge into a real batch, and arrival tracking resumes.
//
// When neither holds, the shard is idle and the flush happens immediately: a
// lone caller pays a single round trip with no timer armed. That is the point
// of the design — the previous fixed ~20µs debounce timer armed on every flush
// fires ~1ms late on an idle or low-core host (wakeup latency dominates the
// requested delay), taxing every low-concurrency command ~5x its round trip.
// Here the gap timer never fires in steady state, closed loop or open; it only
// ends waits for callers that left.
func (s *apShard) awaitExpectedArrivals(batchSize int) {
	ap := s.ap
	expected := ap.expectedArrivals.Load()
	if expected < 0 {
		// Arrivals outran what was announced (open-loop traffic); re-zero so
		// the deficit does not mask the next wave. CAS: only clear the value
		// we saw, never a concurrent announcement.
		ap.expectedArrivals.CompareAndSwap(expected, 0)
		expected = 0
	}
	expectingWave := expected > 0
	if !expectingWave && s.inFlight.Load() == 0 {
		// Idle shard: nothing imminent, flush in one round trip.
		return
	}

	gap := ap.silenceGap()
	// Reset is drain-safe on Go 1.23+ (see go.mod: go 1.24).
	fallback := time.NewTimer(gap)
	defer fallback.Stop()
	lastSeenExpected := expected // count as of the most recent timer (re)arm
	var holdStart time.Time      // set on the first straggler-hold gap fire
	for {
		select {
		case <-ap.ctx.Done():
			return
		case <-fallback.C:
			if !expectingWave && s.Len() < coalesceMinFlush && s.inFlight.Load() > 0 {
				// Only stragglers queued while batches are still executing:
				// flushing a near-empty pipeline burns a connection for a full
				// round trip (measured at high WAN concurrency: straggler
				// flushes of 1-3 commands starved the connection pool and
				// doubled p50). Hold them — the next completed batch's wave
				// sweeps them along, and the wave path below flushes promptly.
				// The hold is bounded like the permit wait: with read timeouts
				// disabled a wedged batch could pin inFlight forever, and the
				// held stragglers must not hang with it.
				if holdStart.IsZero() {
					holdStart = time.Now()
				}
				if time.Since(holdStart) < autoPipelinePermitBackstop {
					lastSeenExpected = ap.expectedArrivals.Load()
					fallback.Reset(gap)
					continue
				}
			}
			if expectingWave {
				// A whole gap passed with no arrivals on this shard: the
				// expected callers left (workload shrank), so clear the stale
				// expectation or future flushes will wait for ghosts. But only
				// if it did not GROW during the silent gap — growth means a
				// batch elsewhere (another shard, or racing this fire)
				// announced a fresh wave, and erasing that would fragment a
				// wave that is really coming. CAS, never a blind store, so an
				// announcement racing the reset itself also survives.
				if d := ap.expectedArrivals.Load(); d > 0 && d <= lastSeenExpected {
					ap.expectedArrivals.CompareAndSwap(d, 0)
				}
			}
			return
		case <-s.notify:
			if s.Len() >= batchSize {
				return
			}
			if d := ap.expectedArrivals.Load(); d > 0 {
				// An in-flight batch completed mid-wait: its wave is now the
				// thing to wait out, with the exact-count exit below.
				expectingWave = true
				lastSeenExpected = d
			} else if expectingWave {
				// The wave has fully landed; flush it as one batch.
				return
			} else if s.inFlight.Load() == 0 {
				// Nothing executing, no wave expected: no completion will
				// wake more callers, so flush what we have now.
				return
			}
			fallback.Reset(gap)
		}
	}
}

// dispatchCmds executes the drained stripe queues as one pipeline without
// constructing a Pipeline object: the queue slices go straight to the client's
// hook-wrapped pipeline processor (the exact entry Pipeline.Exec is wired to),
// so hooks and OTel behave identically while the per-batch Pipeline allocation,
// its append-growth reallocations and the per-command Process calls disappear.
// A single-stripe drain (every ordered shard, and any drain that found one
// non-empty stripe) passes its queue zero-copy; multi-stripe drains merge into
// one pooled slice.
func (ap *AutoPipeliner) dispatchCmds(ctx context.Context, queues [][]Cmder, total int) {
	if len(queues) == 1 {
		_ = ap.pipeliner.processPipelineHook(ctx, queues[0])
		return
	}
	merged := getQueueSlice(total)
	for i := range queues {
		merged = append(merged, queues[i]...)
	}
	_ = ap.pipeliner.processPipelineHook(ctx, merged)
	putQueueSlice(merged)
}

// flushBatchSlice takes the shard's currently-queued commands as one batch,
// swaps in a fresh batch for subsequent enqueues, and dispatches the taken
// batch. Completion is signalled by closing the batch's done channel once
// (waking every waiter in a single operation) rather than one channel send
// per command.
func (s *apShard) flushBatchSlice() {
	ap := s.ap

	// Drain every stripe into one combined batch and roll fresh queues for the
	// commands enqueued after this point. Striped enqueue spreads the hot
	// mutex; one merged flush keeps the pipeline deep. accumulateBatch already
	// bounds the total to roughly MaxBatchSize before we get here.
	queues := make([][]Cmder, 0, len(s.stripes))
	batches := make([]*apBatch, 0, len(s.stripes))
	total := 0
	for i := range s.stripes {
		st := &s.stripes[i]
		// Skip provably-empty stripes without taking their mutex. Safe in
		// THIS path only: an enqueue publishes queueLen under the stripe lock
		// and wakes the flusher after unlocking, so a command that appears
		// concurrently with this unlocked read is re-observed by the
		// flusher's Len() loop or the buffered notify — the same protocol the
		// flusher already relies on. The shutdown drain must keep locking
		// unconditionally (see flushBatchSliceShutdown).
		if st.queueLen.Load() == 0 {
			continue
		}
		st.mu.Lock()
		if len(st.queue) > 0 {
			queues = append(queues, st.queue)
			batches = append(batches, st.curBatch)
			total += len(st.queue)
			st.queue = getQueueSlice(ap.config.MaxBatchSize)
			st.curBatch = newAPBatch()
			st.queueLen.Store(0)
		}
		st.mu.Unlock()
	}
	if total == 0 {
		return
	}

	// Acquire a concurrency permit. The wait runs on a background context with
	// a generous backstop deadline against a wedged semaphore: commands taken
	// from the queue were already ACCEPTED, so a concurrent Close must not
	// cancel them mid-acquire — Close's contract is to flush pending commands
	// (it waits for this dispatch via wg/batchWg before tearing anything
	// down). The backstop is deliberately well above both the default
	// ReadTimeout and a maintnotifications relaxed window, so a legitimately
	// slow batch (e.g. during a failover) holding a permit does not cause
	// waiters to spuriously fail.
	if !s.sem.TryAcquire() {
		err := s.sem.Acquire(context.Background(), autoPipelinePermitBackstop, context.DeadlineExceeded)
		if err != nil {
			// A permit not freeing within the backstop means the in-flight
			// batch is wedged well past any configured timeout — leave an
			// operator breadcrumb before failing the drained commands.
			internal.Logger.Printf(context.Background(),
				"redis: autopipeline: no batch permit after %s; failing %d queued commands",
				autoPipelinePermitBackstop, total)
			batchErr := err
			for i := range queues {
				for _, qc := range queues[i] {
					qc.SetErr(batchErr)
				}
				close(batches[i].done)
				putQueueSlice(queues[i])
			}
			return
		}

		// Wave merge. We took the queue and then waited a full batch round
		// trip for the permit; callers whose replies landed just after our
		// take re-submitted into the FRESH queue during that wait. Executing
		// without them splits the group into two alternating waves — each
		// observing two round trips, at half throughput — a state that is
		// stable once entered (measured: p50 pinned at 2xRTT for entire runs
		// at mid worker counts on a 52ms link). On the default window, let the
		// wave of follow-ups land and fold it into this batch before
		// executing, which merges the waves back into one batch per round
		// trip. Explicit-delay configs keep their own timing.
		if ap.config.MaxFlushDelay == 0 && !ap.config.AdaptiveDelay {
			s.awaitExpectedArrivals(ap.config.MaxBatchSize)
			for i := range s.stripes {
				st := &s.stripes[i]
				if st.queueLen.Load() == 0 {
					continue
				}
				st.mu.Lock()
				if len(st.queue) > 0 {
					queues = append(queues, st.queue)
					batches = append(batches, st.curBatch)
					total += len(st.queue)
					st.queue = getQueueSlice(ap.config.MaxBatchSize)
					st.curBatch = newAPBatch()
					st.queueLen.Store(0)
				}
				st.mu.Unlock()
			}
		}
	}

	// Fast path for single command: skip the pipeline and Process directly, in
	// its own goroutine. The dispatch MUST NOT run inline in the flusher: a
	// synchronous Process blocks the flusher for a full round trip, and on a
	// slow link a solo straggler then holds up an entire landed wave for one
	// RTT — whose flush then delays the straggler's next command in turn, a
	// stable phase-lock where everyone pays 2x RTT (measured: ~25% of runs on
	// a 57ms link locked at exactly 2x RTT until perturbed).
	// No expectedArrivals announcement: a single waiter waking is the
	// lone-caller case, which must keep flushing immediately.
	if total == 1 {
		ap.batchWg.Add(1)
		s.inFlight.Add(1)
		go func() {
			defer ap.batchWg.Done()
			defer s.inFlight.Add(-1)
			defer s.sem.Release()
			defer putQueueSlice(queues[0])
			defer close(batches[0].done)
			// Background for the same reason as the batch goroutine below:
			// accepted commands execute even under a concurrent Close.
			execStart := time.Now()
			_ = ap.pipeliner.Process(context.Background(), queues[0][0])
			ap.observeBatchExec(time.Since(execStart))
		}()
		return
	}

	// Track this goroutine in the batchWg so Close() waits for it.
	// IMPORTANT: Add to WaitGroup AFTER semaphore is acquired to avoid deadlock.
	ap.batchWg.Add(1)
	s.inFlight.Add(1)
	go func() {
		defer ap.batchWg.Done()
		defer s.inFlight.Add(-1)
		defer s.sem.Release()
		// Signal completion with one close per taken stripe. Deferred so a
		// panic in Process/Exec (e.g. a malformed command or encoder panic)
		// still wakes every waiter in await() instead of hanging them forever;
		// the closes run after Exec on the happy path, so results are
		// populated first.
		defer func() {
			for i := range queues {
				close(batches[i].done)
				putQueueSlice(queues[i])
			}
		}()

		// Execute on a background context: these commands were accepted before
		// any concurrent Close, and Close waits for this goroutine (batchWg)
		// before the client tears down its pools — cancelling here would
		// error already-accepted commands while the shutdown sweep flushes
		// later ones, an inverted outcome. The wire timeouts (Read/Write
		// Timeout, or maintnotifications relaxed windows) still bound the
		// execution; no per-batch timer is allocated.
		ctx := context.Background()

		execStart := time.Now()
		ap.dispatchCmds(ctx, queues, total)
		ap.observeBatchExec(time.Since(execStart))

		// Announce the expected arrivals BEFORE the deferred closes wake this
		// batch's waiters, so the flusher knows the wave size the moment its
		// first command lands (see expectedArrivals).
		ap.expectedArrivals.Add(int64(total))
	}()
}

// flushBatchSliceShutdown flushes commands during shutdown.
// Unlike flushBatchSlice, this doesn't use ap.ctx for semaphore acquisition
// because ap.ctx is already cancelled during shutdown.
// Executes synchronously to preserve command order.
func (s *apShard) flushBatchSliceShutdown() {
	ap := s.ap
	// Flush all remaining commands synchronously to preserve order.
	//
	// The loop condition is checked UNDER each stripe's lock (not via the
	// unlocked s.Len()): a late enqueue appends to a stripe's queue and updates
	// its queueLen under that stripe's mutex, so reading queueLen without the
	// lock could miss a command that was just appended (seeing 0 and exiting
	// while a command sits in the queue). Locking first makes "is the stripe
	// empty?" and "take the stripe" atomic against that enqueue — this is what
	// closes the lost-command race on Close.
	for {
		// Take every stripe's queue as one merged batch and roll fresh queues.
		queues := make([][]Cmder, 0, len(s.stripes))
		batches := make([]*apBatch, 0, len(s.stripes))
		total := 0
		for i := range s.stripes {
			st := &s.stripes[i]
			st.mu.Lock()
			if len(st.queue) > 0 {
				queues = append(queues, st.queue)
				batches = append(batches, st.curBatch)
				total += len(st.queue)
				st.queue = getQueueSlice(ap.config.MaxBatchSize)
				st.curBatch = newAPBatch()
				st.queueLen.Store(0)
			}
			st.mu.Unlock()
		}
		if total == 0 {
			return
		}

		// Serialize with any still-running in-flight batch: the shutdown drain
		// used to bypass the per-shard permit, so under MaxConcurrentBatches:1
		// a drained command could execute CONCURRENTLY with the in-flight
		// batch during Close and be observed out of order. Acquire the permit
		// (bounded by the backstop, on a background context — ap.ctx is
		// already cancelled here); if the backstop expires the permit holder
		// is wedged and we proceed anyway rather than strand the commands.
		acquired := s.sem.TryAcquire()
		if !acquired {
			acquired = s.sem.Acquire(context.Background(), autoPipelinePermitBackstop, context.DeadlineExceeded) == nil
			if !acquired {
				internal.Logger.Printf(context.Background(),
					"redis: autopipeline: no batch permit after %s during shutdown; flushing unserialized",
					autoPipelinePermitBackstop)
			}
		}

		// Execute each batch in a func so close(batch.done) is deferred: a panic
		// in Process/Exec still signals completion (waking await()) before it
		// propagates, instead of leaving shutdown waiters hung.
		func() {
			if acquired {
				defer s.sem.Release()
			}
			defer func() {
				for i := range queues {
					close(batches[i].done)
					putQueueSlice(queues[i])
				}
			}()

			// ap.ctx is already cancelled here (Close cancels it before draining),
			// so use a fresh background context with no artificial deadline. The
			// wire timeout is then governed by the connection's ReadTimeout /
			// WriteTimeout — exactly like the normal flush path and a plain client
			// Exec. Crucially this lets a relaxed timeout (set by maintnotifications
			// during a failover/migration) take effect; a hardcoded short deadline
			// here would cap that relaxed window and time out in-flight commands the
			// relaxation was meant to protect. (A user who wants shutdown bounded
			// sets ReadTimeout/WriteTimeout on the client, as for any command.)
			ap.dispatchCmds(context.Background(), queues, total)
		}()
	}
}

// Len returns the number of queued commands in this shard.
func (s *apShard) Len() int {
	n := 0
	for i := range s.stripes {
		n += int(s.stripes[i].queueLen.Load())
	}
	return n
}

// Len returns the current number of queued commands across all shards.
func (ap *AutoPipeliner) Len() int {
	total := 0
	for _, s := range ap.shards {
		total += s.Len()
	}
	return total
}

// calculateDelay calculates the delay based on the given queue length (the
// caller's own shard, not the global total, so each shard tunes independently).
// Uses integer-only arithmetic for optimal performance (no float operations).
// Returns 0 if MaxFlushDelay is 0.
func (ap *AutoPipeliner) calculateDelay(queueLen int) time.Duration {
	maxDelay := ap.config.MaxFlushDelay
	if maxDelay == 0 {
		return 0
	}

	// If adaptive delay is disabled, return fixed delay
	if !ap.config.AdaptiveDelay {
		return maxDelay
	}

	if queueLen == 0 {
		return 0
	}

	maxBatch := ap.config.MaxBatchSize

	// Use integer arithmetic to avoid float operations
	// Calculate thresholds: 75%, 50%, 25% of maxBatch
	// Multiply by 4 to avoid division: queueLen * 4 vs maxBatch * 3 (75%)
	//
	// Adaptive delay strategy:
	// - ≥75% full: No delay (flush immediately to prevent overflow)
	// - ≥50% full: 25% of max delay (queue filling up)
	// - ≥25% full: 50% of max delay (moderate load)
	// - <25% full: 100% of max delay (low load, maximize batching)
	switch {
	case queueLen*4 >= maxBatch*3: // queueLen >= 75% of maxBatch
		return 0 // Flush immediately
	case queueLen*2 >= maxBatch: // queueLen >= 50% of maxBatch
		return maxDelay >> 2 // Divide by 4 using bit shift (faster)
	case queueLen*4 >= maxBatch: // queueLen >= 25% of maxBatch
		return maxDelay >> 1 // Divide by 2 using bit shift (faster)
	default:
		return maxDelay
	}
}

// Pipeline returns a new pipeline that uses the underlying pipeliner.
// This allows you to create a traditional pipeline from an autopipeliner.
func (ap *AutoPipeliner) Pipeline() Pipeliner {
	return ap.pipeliner.Pipeline()
}

// Pipelined executes a function in a pipeline context.
// This is a convenience method that creates a pipeline, executes the function,
// and returns the results.
func (ap *AutoPipeliner) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return ap.pipeliner.Pipeline().Pipelined(ctx, fn)
}

// TxPipelined executes a function in a transaction pipeline context.
// This is a convenience method that creates a transaction pipeline, executes the function,
// and returns the results. It delegates to the underlying client's TxPipeline.
func (ap *AutoPipeliner) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return ap.pipeliner.TxPipeline().Pipelined(ctx, fn)
}

// TxPipeline returns a new transaction pipeline that uses the underlying pipeliner.
// This allows you to create a traditional transaction pipeline from an autopipeliner.
// It delegates to the underlying client's TxPipeline.
func (ap *AutoPipeliner) TxPipeline() Pipeliner {
	return ap.pipeliner.TxPipeline()
}

// validate AutoPipeliner implements Cmdable
var _ Cmdable = (*AutoPipeliner)(nil)
