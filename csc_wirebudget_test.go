package redis

import "testing"

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
