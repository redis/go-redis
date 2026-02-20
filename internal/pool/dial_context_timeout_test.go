package pool

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

// Ensures ConnPool applies DialTimeout per attempt via context (so dialing doesn't hang
// when a custom dialer ignores timeouts).
func TestDialConn_AppliesDialTimeoutPerAttemptViaContext(t *testing.T) {
	p := NewConnPool(&Options{
		Dialer: func(ctx context.Context) (net.Conn, error) {
			// Pool should apply DialTimeout per attempt via context.
			dl, ok := ctx.Deadline()
			if !ok {
				return nil, errors.New("expected context deadline")
			}
			remaining := time.Until(dl)
			// Allow slack for scheduling jitter.
			if remaining <= 50*time.Millisecond || remaining > 250*time.Millisecond {
				return nil, errors.New("unexpected context deadline duration")
			}
			return nil, errors.New("dial failed")
		},
		PoolSize:           1,
		MaxConcurrentDials: 1,
		DialTimeout:        200 * time.Millisecond,
		PoolTimeout:        10 * time.Millisecond,
		DialerRetries:      1,
	})
	defer p.Close()

	_, err := p.newConn(context.Background(), true)
	if err == nil {
		t.Fatalf("expected error")
	}
}
