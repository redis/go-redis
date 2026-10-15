package redis

import (
	"context"
	"testing"
)

// A cacheable command whose public key position was set to a negative value
// is not cacheable, and the key gate does not index args with it. The span
// let -1 through (only 0 was rejected), and cscKeysRenderable then read
// args[-1] and panicked the process; the old keyArg bounds check had sent such
// a command normally.
func TestCSCNegativeKeyPositionIsUncacheable(t *testing.T) {
	ctx := context.Background()
	get := NewStringCmd(ctx, "get", "k")
	get.SetFirstKeyPos(-1)
	mget := NewSliceCmd(ctx, "mget", "a", "b")
	mget.SetFirstKeyPos(-3)
	for _, cmd := range []Cmder{get, mget} {
		if isCacheable(cmd) {
			t.Errorf("%s with a negative key position reported cacheable", cmd.Name())
		}
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("%s: key gate panicked: %v", cmd.Name(), r)
				}
			}()
			if lo, hi, ok := cscKeySpan(cmd); ok {
				t.Errorf("%s: span (%d, %d) accepted for a negative key position", cmd.Name(), lo, hi)
				_ = cscKeysRenderable(cmd, lo, hi)
			}
		}()
	}
}
