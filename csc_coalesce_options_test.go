package redis

import (
	"testing"
	"time"
)

// TestCSCCoalesceModeRejectsPinnedPublicly pins that the buggy "pinned" PROTOTYPE
// engine (no idle invalidation drain; can serve stale) is NOT selectable from the
// public ClientSideCacheCoalesceMode option — it falls back to "workers" — while
// the internal benchmark hook can still force it.
func TestCSCCoalesceModeRejectsPinnedPublicly(t *testing.T) {
	if got := cscCoalesceMode(&Options{ClientSideCacheCoalesceMode: "pinned"}); got != "workers" {
		t.Fatalf("public \"pinned\" => %q, want \"workers\"", got)
	}
	if got := cscCoalesceMode(&Options{ClientSideCacheCoalesceMode: "fullduplex"}); got != "fullduplex" {
		t.Fatalf("\"fullduplex\" => %q, want \"fullduplex\"", got)
	}
	if got := cscCoalesceMode(&Options{ClientSideCacheCoalesceMode: ""}); got != "workers" {
		t.Fatalf("\"\" => %q, want \"workers\"", got)
	}
	if got := cscCoalesceMode(nil); got != "workers" {
		t.Fatalf("nil opt => %q, want \"workers\"", got)
	}

	cscForcePinned = true
	defer func() { cscForcePinned = false }()
	if got := cscCoalesceMode(&Options{ClientSideCacheCoalesceMode: "pinned"}); got != "pinned" {
		t.Fatalf("forced \"pinned\" => %q, want \"pinned\" (benchmark hook)", got)
	}
}

// TestUniversalOptionsSimpleCopiesCSCCoalesce guards that the new CSC miss-
// coalescing / invalidation-batching knobs reach a standalone Client through
// UniversalOptions.Simple() (they were previously Options-only, so UniversalClient
// users could not enable them).
func TestUniversalOptionsSimpleCopiesCSCCoalesce(t *testing.T) {
	u := &UniversalOptions{
		ClientSideCacheRefreshOnInvalidate:     true,
		ClientSideCacheCoalesceMisses:          true,
		ClientSideCacheCoalesceMode:            "fullduplex",
		ClientSideCacheCoalesceWorkers:         5,
		ClientSideCacheInvalidationBatchWindow: 7 * time.Millisecond,
	}
	o := u.Simple()
	if !o.ClientSideCacheRefreshOnInvalidate {
		t.Error("Simple() dropped ClientSideCacheRefreshOnInvalidate")
	}
	if !o.ClientSideCacheCoalesceMisses {
		t.Error("Simple() dropped ClientSideCacheCoalesceMisses")
	}
	if o.ClientSideCacheCoalesceMode != "fullduplex" {
		t.Errorf("Simple() ClientSideCacheCoalesceMode = %q, want \"fullduplex\"", o.ClientSideCacheCoalesceMode)
	}
	if o.ClientSideCacheCoalesceWorkers != 5 {
		t.Errorf("Simple() ClientSideCacheCoalesceWorkers = %d, want 5", o.ClientSideCacheCoalesceWorkers)
	}
	if o.ClientSideCacheInvalidationBatchWindow != 7*time.Millisecond {
		t.Errorf("Simple() ClientSideCacheInvalidationBatchWindow = %v, want 7ms", o.ClientSideCacheInvalidationBatchWindow)
	}
}
