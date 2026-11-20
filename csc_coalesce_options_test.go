package redis

import (
	"testing"
	"time"
)

// TestUniversalOptionsSimpleCopiesCSCCoalesce guards that the new CSC miss-
// coalescing / invalidation-batching knobs reach a standalone Client through
// UniversalOptions.Simple() (they were previously Options-only, so UniversalClient
// users could not enable them).
func TestUniversalOptionsSimpleCopiesCSCCoalesce(t *testing.T) {
	u := &UniversalOptions{
		ClientSideCacheRefreshOnInvalidate:     true,
		ClientSideCacheCoalesceMisses:          true,
		ClientSideCacheInvalidationBatchWindow: 7 * time.Millisecond,
	}
	o := u.Simple()
	if !o.ClientSideCacheRefreshOnInvalidate {
		t.Error("Simple() dropped ClientSideCacheRefreshOnInvalidate")
	}
	if !o.ClientSideCacheCoalesceMisses {
		t.Error("Simple() dropped ClientSideCacheCoalesceMisses")
	}
	if o.ClientSideCacheInvalidationBatchWindow != 7*time.Millisecond {
		t.Errorf("Simple() ClientSideCacheInvalidationBatchWindow = %v, want 7ms", o.ClientSideCacheInvalidationBatchWindow)
	}
}
