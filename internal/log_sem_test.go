package internal

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestLogLevelString(t *testing.T) {
	tests := map[LogLevelT]string{
		LogLevelError: "ERROR",
		LogLevelWarn:  "WARN",
		LogLevelInfo:  "INFO",
		LogLevelDebug: "DEBUG",
		LogLevelT(99): "UNKNOWN",
	}
	for lvl, want := range tests {
		if got := lvl.String(); got != want {
			t.Errorf("LogLevelT(%d).String() = %q, want %q", lvl, got, want)
		}
	}
}

func TestLogLevelPredicates(t *testing.T) {
	if !LogLevelError.IsValid() || !LogLevelDebug.IsValid() {
		t.Errorf("valid levels reported invalid")
	}
	if LogLevelT(-1).IsValid() || LogLevelT(99).IsValid() {
		t.Errorf("invalid levels reported valid")
	}

	if LogLevelError.WarnOrAbove() {
		t.Errorf("error should not be WarnOrAbove")
	}
	if !LogLevelWarn.WarnOrAbove() {
		t.Errorf("warn should be WarnOrAbove")
	}
	if LogLevelWarn.InfoOrAbove() {
		t.Errorf("warn should not be InfoOrAbove")
	}
	if !LogLevelInfo.InfoOrAbove() {
		t.Errorf("info should be InfoOrAbove")
	}
	if LogLevelInfo.DebugOrAbove() {
		t.Errorf("info should not be DebugOrAbove")
	}
	if !LogLevelDebug.DebugOrAbove() {
		t.Errorf("debug should be DebugOrAbove")
	}
}

func TestOnceDo(t *testing.T) {
	var once Once
	calls := 0

	// First call fails: should re-arm.
	wantErr := errors.New("fail")
	if err := once.Do(func() error { calls++; return wantErr }); err != wantErr {
		t.Fatalf("first Do error = %v, want %v", err, wantErr)
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}

	// Second call succeeds.
	if err := once.Do(func() error { calls++; return nil }); err != nil {
		t.Fatalf("second Do error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2", calls)
	}

	// Third call must be a no-op.
	if err := once.Do(func() error { calls++; return errors.New("should not run") }); err != nil {
		t.Fatalf("third Do error = %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (no-op expected)", calls)
	}
}

func testSemaphore(t *testing.T, acquire func(context.Context, time.Duration, error) error,
	tryAcquire func() bool, release func(), length func() int32) {
	if got := length(); got != 0 {
		t.Fatalf("initial Len() = %d, want 0", got)
	}

	if err := acquire(context.Background(), time.Second, errors.New("timeout")); err != nil {
		t.Fatalf("Acquire error: %v", err)
	}
	if got := length(); got != 1 {
		t.Fatalf("Len() after acquire = %d, want 1", got)
	}

	// Capacity is 1, so a second acquire should time out.
	timeoutErr := errors.New("timeout")
	if err := acquire(context.Background(), 10*time.Millisecond, timeoutErr); err != timeoutErr {
		t.Fatalf("expected timeout error, got %v", err)
	}

	// Cancelled context should return ctx error.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := acquire(ctx, time.Second, timeoutErr); err == nil || err == timeoutErr {
		t.Fatalf("expected context error, got %v", err)
	}

	release()
	if !tryAcquire() {
		t.Fatalf("TryAcquire should succeed after release")
	}
	if tryAcquire() {
		t.Fatalf("TryAcquire should fail when empty")
	}
	release()
}

func TestFastSemaphore(t *testing.T) {
	s := NewFastSemaphore(1)
	defer s.Close()
	testSemaphore(t, s.Acquire, s.TryAcquire, s.Release, s.Len)

	s.AcquireBlocking()
	if s.Len() != 1 {
		t.Errorf("Len after AcquireBlocking = %d, want 1", s.Len())
	}
	s.Release()
}

func TestFIFOSemaphore(t *testing.T) {
	s := NewFIFOSemaphore(1)
	defer s.Close()
	testSemaphore(t, s.Acquire, s.TryAcquire, s.Release, s.Len)

	s.AcquireBlocking()
	if s.Len() != 1 {
		t.Errorf("Len after AcquireBlocking = %d, want 1", s.Len())
	}
	s.Release()
}
