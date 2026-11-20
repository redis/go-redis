package redis

import (
	"context"
	"testing"
	"time"
)

// The initConn wait path only runs when the state was read as CREATED and another
// goroutine then won the CAS to INITIALIZING, so the decision it makes is tested
// directly here. A non-positive DialTimeout means the option is disabled (ParseURL
// uses a value of 0 or less for that) and must not cap the wait at all: handing 0
// to context.WithTimeout expires the wait immediately, which is exactly what the
// -1 sentinel did before the normalisation.
func TestInitConnWaitContext(t *testing.T) {
	t.Parallel()

	base := context.Background()

	t.Run("disabled", func(t *testing.T) {
		for _, d := range []time.Duration{0, -1, -2 * time.Second} {
			ctx, cancel := initConnWaitContext(base, d)
			if cancel != nil {
				cancel()
			}
			if ctx != base {
				t.Errorf("dialTimeout %v: expected the caller context unchanged, got %v", d, ctx)
			}
			if _, ok := ctx.Deadline(); ok {
				t.Errorf("dialTimeout %v: expected no deadline", d)
			}
		}
	})

	t.Run("positive caps the wait", func(t *testing.T) {
		ctx, cancel := initConnWaitContext(base, 5*time.Second)
		if cancel == nil {
			t.Fatal("expected a cancel func for a positive dial timeout")
		}
		defer cancel()
		deadline, ok := ctx.Deadline()
		if !ok {
			t.Fatal("expected a deadline")
		}
		if remaining := time.Until(deadline); remaining > 5*time.Second || remaining < 4*time.Second {
			t.Errorf("deadline %v away, want about 5s", remaining)
		}
	})

	t.Run("sooner caller deadline wins", func(t *testing.T) {
		caller, callerCancel := context.WithTimeout(base, 100*time.Millisecond)
		defer callerCancel()

		ctx, cancel := initConnWaitContext(caller, 5*time.Second)
		if cancel != nil {
			cancel()
		}
		if ctx != caller {
			t.Error("expected the caller context when its deadline is sooner than DialTimeout")
		}
	})
}
