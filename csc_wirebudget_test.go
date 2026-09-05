package redis

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// TestCSCMissCoalescerWireBudget pins the in-flight wire-byte budget that bounds a
// burst of large coalesced misses: over budget, a miss sheds to the pooled path
// (reserveWireBytes returns false) instead of holding a wire copy while blocked.
func TestCSCMissCoalescerWireBudget(t *testing.T) {
	mc := &cscMissCoalescer{}
	// Reserve up to the budget: succeeds.
	if !mc.reserveWireBytes(cscMissWireBudgetBytes) {
		t.Fatal("reserve up to budget should succeed")
	}
	if got := mc.wireBytes.Load(); got != cscMissWireBudgetBytes {
		t.Fatalf("wireBytes = %d, want %d", got, int64(cscMissWireBudgetBytes))
	}
	// Over budget: sheds and reserves nothing (counter unchanged).
	if mc.reserveWireBytes(1) {
		t.Fatal("reserve over budget should shed (return false)")
	}
	if got := mc.wireBytes.Load(); got != cscMissWireBudgetBytes {
		t.Fatalf("shed must not change the counter; wireBytes = %d, want %d", got, int64(cscMissWireBudgetBytes))
	}
	// Release frees the budget again.
	mc.releaseWireBytes(cscMissWireBudgetBytes)
	if got := mc.wireBytes.Load(); got != 0 {
		t.Fatalf("wireBytes after release = %d, want 0", got)
	}
	// A single command larger than the whole budget sheds too (the pooled path runs
	// it, so there is no progress hazard) and leaves the counter at 0.
	if mc.reserveWireBytes(cscMissWireBudgetBytes + 1) {
		t.Fatal("a single over-budget command should shed")
	}
	if got := mc.wireBytes.Load(); got != 0 {
		t.Fatalf("wireBytes after over-budget shed = %d, want 0", got)
	}
}

// bigBinArg is a command argument whose serialized size is large but which
// cmdApproxBytes charges only ~8 bytes for (the BinaryMarshaler default branch).
type bigBinArg struct{ n int }

func (a bigBinArg) MarshalBinary() ([]byte, error) { return make([]byte, a.n), nil }

// TestCSCMissCoalescerReconcilesWireBudget pins the fix for the under-counted wire
// budget: cmdApproxBytes charges ~8 bytes for a variable-size non-string/[]byte arg
// (BinaryMarshaler, net.IP, *string, ...), so the pre-serialization reserve can
// undercount a large miss and let the in-flight budget drift past
// cscMissWireBudgetBytes. reconcileWireBytes corrects the counter to the ACTUAL
// serialized size and keeps req.reserved in lockstep so settle() releases exactly
// once.
func TestCSCMissCoalescerReconcilesWireBudget(t *testing.T) {
	ctx := context.Background()
	cmd := NewCmd(ctx, "set", "k", bigBinArg{4096})

	estimate := cmdApproxBytes(cmd)
	var buf bytes.Buffer
	if err := writeCmd(proto.NewWriter(&buf), cmd); err != nil {
		t.Fatalf("writeCmd: %v", err)
	}
	wire := buf.Bytes()
	actual := int64(len(wire))
	// Precondition: the estimate really does undercount, or the test proves nothing.
	if estimate >= actual {
		t.Fatalf("estimate %d >= actual %d; cmdApproxBytes did not undercount, test is vacuous",
			estimate, actual)
	}

	mc := &cscMissCoalescer{}
	// Reserve the estimate (as fetch does BEFORE serialization), then reconcile once
	// the wire exists.
	if !mc.reserveWireBytes(estimate) {
		t.Fatal("reserve estimate failed")
	}
	req := &cscMissReq{reserved: estimate, wire: wire}
	mc.reconcileWireBytes(req)

	if got := mc.wireBytes.Load(); got != actual {
		t.Fatalf("after reconcile wireBytes = %d, want the actual wire size %d "+
			"(undercount not corrected)", got, actual)
	}
	if req.reserved != actual {
		t.Fatalf("req.reserved = %d, want %d (settle must release the reconciled amount)",
			req.reserved, actual)
	}
	// settle() releases exactly req.reserved; the counter must return to zero.
	mc.releaseWireBytes(req.reserved)
	if got := mc.wireBytes.Load(); got != 0 {
		t.Fatalf("after release wireBytes = %d, want 0 (release is not exactly-once)", got)
	}
}

// TestCSCMissCoalescerCancelIfStopping pins the shutdown-strand fix (B2). On a
// caller-deadline abandon the LIVE reader still dequeues the req and publishes, so
// the token is left to it. But once the coalescer is stopping, a req stranded in
// mc.ch past the shutdown drain would leave its key IN_PROGRESS and block a later
// reader until StaleTimeout — so cancelIfStopping cancels the reservation, but only
// when stop is observed.
func TestCSCMissCoalescerCancelIfStopping(t *testing.T) {
	cache := NewLocalCache(CacheConfig{MaxEntries: 8})
	mc := &cscMissCoalescer{
		c:    &baseClient{csc: cache},
		ch:   make(chan *cscMissReq, 4),
		stop: make(chan struct{}),
	}

	token, fetch := cache.Reserve("ck", []string{"rk"})
	if token == 0 || !fetch {
		t.Fatalf("Reserve = (%d, %v); want a fresh in-progress reservation", token, fetch)
	}

	// Live coalescer: no-op. The reservation stays IN_PROGRESS (a second Reserve
	// declines) so the live reader can still publish.
	mc.cancelIfStopping("ck", token)
	if _, sf := cache.Reserve("ck", []string{"rk"}); sf {
		t.Fatal("live cancelIfStopping released the reservation; the reader must still publish")
	}

	// Stopping: the reservation MUST be released so a later reader misses and
	// refetches instead of blocking to StaleTimeout.
	close(mc.stop)
	mc.cancelIfStopping("ck", token)
	if tok2, sf := cache.Reserve("ck", []string{"rk"}); !sf || tok2 == 0 {
		t.Fatalf("after stopping cancelIfStopping: Reserve = (%d, %v); want a fresh "+
			"reservation (the IN_PROGRESS token was not cancelled)", tok2, sf)
	}
}

// TestCSCMissCoalescerShedsWhenActualOverBudget pins finding #1's second half:
// reconcileWireBytes must SHED (return false) when the true serialized size would
// push the in-flight total past the budget, not just correct the counter and keep
// an over-budget wire — else N concurrent undercounted misses each pass the
// pre-serialize gate and then blow the budget while every wire stays queued.
func TestCSCMissCoalescerShedsWhenActualOverBudget(t *testing.T) {
	ctx := context.Background()
	cmd := NewCmd(ctx, "set", "k", bigBinArg{4096})
	estimate := cmdApproxBytes(cmd)
	var buf bytes.Buffer
	if err := writeCmd(proto.NewWriter(&buf), cmd); err != nil {
		t.Fatalf("writeCmd: %v", err)
	}
	wire := buf.Bytes()
	actual := int64(len(wire))
	if estimate >= actual {
		t.Fatalf("estimate %d >= actual %d; test is vacuous", estimate, actual)
	}

	mc := &cscMissCoalescer{}
	// Preload the budget so the estimate still fits but reconciling to the ACTUAL
	// size crosses the cap.
	preload := cscMissWireBudgetBytes - estimate - actual/2
	mc.wireBytes.Store(preload)
	if !mc.reserveWireBytes(estimate) {
		t.Fatal("reserve estimate should fit (only the reconciled actual exceeds the cap)")
	}
	req := &cscMissReq{reserved: estimate, wire: wire}

	if mc.reconcileWireBytes(req) {
		t.Fatal("reconcileWireBytes kept an over-budget wire; want shed (false)")
	}
	// Rolled back to the estimate reservation (reserved unchanged), so fetch's
	// !enqueued defer releases exactly the estimate and the counter returns to preload.
	if req.reserved != estimate {
		t.Fatalf("req.reserved = %d, want the estimate %d (roll back on shed)", req.reserved, estimate)
	}
	if got := mc.wireBytes.Load(); got != preload+estimate {
		t.Fatalf("wireBytes = %d, want preload+estimate %d (delta must be rolled back)",
			got, preload+estimate)
	}
	mc.releaseWireBytes(req.reserved)
	if got := mc.wireBytes.Load(); got != preload {
		t.Fatalf("after release wireBytes = %d, want preload %d", got, preload)
	}
}

// TestCSCMissCoalescerBoundsConcurrentSerialize pins #3965 F3: the serialization
// admission caps HOW MANY callers hold a wire snapshot at once
// (cscMissMaxConcurrentSerialize), so a concurrent burst cannot coexist an unbounded
// NUMBER of snapshots. It bounds peak snapshot memory to cap x per-snapshot size, NOT
// to the wire budget: a single undercounted argument (e.g. a BinaryMarshaler returning
// hundreds of MiB) still allocates its full snapshot before reconcileWireBytes sheds
// it, so up to cap such snapshots can coexist. A per-request byte ceiling (bounding the
// serializer itself, and capping buildCacheKey which serializes uncapped upstream) is a
// deferred follow-up. Single-threaded and deterministic: it fills the semaphore, then
// confirms further acquisitions shed (never exceed the cap) and that a release frees
// exactly one slot.
func TestCSCMissCoalescerBoundsConcurrentSerialize(t *testing.T) {
	// Shed immediately when the sem is full, so the over-cap attempts are
	// deterministic (no timing dependence on the wait).
	origWait := cscMissSerializeWait
	cscMissSerializeWait = 0
	defer func() { cscMissSerializeWait = origWait }()

	const cap = cscMissMaxConcurrentSerialize
	mc := &cscMissCoalescer{
		serializeSem: make(chan struct{}, cap),
		stop:         make(chan struct{}),
	}

	// Fill every slot: the sem has exactly cap slots, so all cap acquisitions win.
	for i := 0; i < cap; i++ {
		if !mc.acquireSerialize() {
			t.Fatalf("acquire %d/%d failed with the sem not yet full", i+1, cap)
		}
	}
	if got := int(mc.maxSerializing.Load()); got != cap {
		t.Fatalf("maxSerializing = %d after filling; want %d", got, cap)
	}

	// With every slot held, further acquisitions must SHED — never past the cap.
	for i := 0; i < cap*3; i++ {
		if mc.acquireSerialize() {
			t.Fatal("acquireSerialize admitted a caller past the cap (#3965 F3)")
		}
	}
	if got := int(mc.maxSerializing.Load()); got != cap {
		t.Fatalf("maxSerializing = %d after over-cap attempts; want %d (the cap must bind)", got, cap)
	}

	// A release frees exactly one slot.
	mc.releaseSerialize()
	if !mc.acquireSerialize() {
		t.Fatal("acquireSerialize failed after a release freed a slot")
	}
}

// TestCSCMissCoalescerFetchShedsWhenSerializeSaturated pins that fetch actually
// CONSULTS the serialization admission (#3965 F3): with every slot held and a zero
// wait, a fetch must shed to the pooled path (errCSCRetryUncached) before it
// serializes or enqueues. This is the fetch-level red-check the admission-only test
// lacks — remove the acquireSerialize call from fetch and this hangs on the
// un-settled reply wait (timeout failure).
func TestCSCMissCoalescerFetchShedsWhenSerializeSaturated(t *testing.T) {
	origWait := cscMissSerializeWait
	cscMissSerializeWait = 0 // shed immediately when the sem is full
	defer func() { cscMissSerializeWait = origWait }()

	cache := NewLocalCache(CacheConfig{MaxEntries: 8})
	mc := &cscMissCoalescer{
		c:            &baseClient{opt: &Options{}, csc: cache},
		ch:           make(chan *cscMissReq, 1),
		stop:         make(chan struct{}),
		serializeSem: make(chan struct{}, cscMissMaxConcurrentSerialize),
	}
	// Saturate every serialization slot so fetch's acquireSerialize must shed.
	for i := 0; i < cscMissMaxConcurrentSerialize; i++ {
		mc.serializeSem <- struct{}{}
	}

	token, fetch := cache.Reserve("ck", []string{"rk"})
	if token == 0 || !fetch {
		t.Fatalf("Reserve = (%d, %v); want a fresh reservation", token, fetch)
	}

	errCh := make(chan error, 1)
	go func() {
		_, err := mc.fetch(context.Background(), makeCmd("get", "rk"), "ck", token)
		errCh <- err
	}()

	select {
	case err := <-errCh:
		if err != errCSCRetryUncached {
			t.Fatalf("fetch returned %v; want errCSCRetryUncached (a saturated serialization "+
				"admission must shed before serializing/enqueueing)", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("fetch did not shed on a saturated serialization admission; it must consult " +
			"acquireSerialize before writeCmd (#3965 F3)")
	}
}
