package redis

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
)

func TestSingleNodeDialRetryScope(t *testing.T) {
	dialErr := fmt.Errorf("dial failed: %w", &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded})
	if !shouldRetry(dialErr, true) {
		t.Fatal("dial failures must remain retryable for cluster and MultiDB failover")
	}
	if shouldRetrySingleNode(dialErr, true) {
		t.Fatal("a single-node command must not restart the exhausted pool dial budget")
	}
	if !shouldRetrySingleNode(io.EOF, true) {
		t.Fatal("non-dial connection errors should still retry")
	}
}
