package pool

import (
	"context"
	"errors"
	"io"
	"net"
	"time"
)

// ContextDeadlineError preserves the socket timeout for connection lifecycle decisions.
type ContextDeadlineError struct {
	original error
	timeout  net.Error
	cause    error
}

func (e *ContextDeadlineError) Error() string {
	return e.cause.Error() + ": " + e.original.Error()
}

func (e *ContextDeadlineError) Unwrap() error        { return e.original }
func (e *ContextDeadlineError) Is(target error) bool { return target == e.cause }

func (e *ContextDeadlineError) Timeout() bool { return e.timeout.Timeout() }

// Temporary is declared so the error still satisfies net.Error.
func (e *ContextDeadlineError) Temporary() bool { return false }

type deadlineReader struct {
	reader   io.Reader
	ctx      context.Context
	deadline time.Time
}

func (r *deadlineReader) Read(b []byte) (int, error) {
	n, err := r.reader.Read(b)
	return n, contextDeadlineError(r.ctx, r.deadline, err)
}

// Only translate socket timeouts when the context supplied the applied deadline.
func contextDeadlineError(ctx context.Context, deadline time.Time, err error) error {
	if err == nil || ctx == nil || deadline.IsZero() {
		return err
	}
	ctxDeadline, ok := ctx.Deadline()
	if !ok || !deadline.Equal(ctxDeadline) || time.Now().Before(deadline) {
		return err
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		return err
	}
	ctxErr := ctx.Err()
	if ctxErr == nil {
		// The socket timer can fire before the context timer publishes its error.
		ctxErr = context.DeadlineExceeded
	}
	return &ContextDeadlineError{original: err, timeout: netErr, cause: ctxErr}
}
