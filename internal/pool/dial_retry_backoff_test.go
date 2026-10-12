package pool

import (
	"testing"
	"time"
)

func TestDialRetryBackoff_ConstantByDefault_ExponentialWhenMaxSet(t *testing.T) {
	p := &ConnPool{cfg: &Options{DialerRetryTimeout: 10 * time.Millisecond}}

	// default constant when max not set
	for i := 0; i < 5; i++ {
		if got := p.dialRetryBackoff(i); got != 10*time.Millisecond {
			t.Fatalf("constant: attempt=%d got %v", i, got)
		}
	}

	// custom function
	p.cfg.DialerRetryBackoff = func(attempt int) time.Duration {
		return time.Duration(attempt+1) * time.Millisecond
	}
	for i := 0; i < 5; i++ {
		got := p.dialRetryBackoff(i)
		want := time.Duration(i+1) * time.Millisecond
		if got != want {
			t.Fatalf("custom: attempt=%d got %v, want %v", i, got, want)
		}
	}
}
