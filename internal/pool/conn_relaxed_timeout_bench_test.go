package pool

import (
	"net"
	"testing"
	"time"
)

// BenchmarkGetEffectiveReadTimeout_NoRelaxedTimeout benchmarks the fast path (no relaxed timeout set)
func BenchmarkGetEffectiveReadTimeout_NoRelaxedTimeout(b *testing.B) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	normalTimeout := 5 * time.Second

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = cn.getEffectiveReadTimeout(normalTimeout)
	}
}

// BenchmarkGetEffectiveReadTimeout_WithRelaxedTimeout benchmarks when relaxed timeout is set
func BenchmarkGetEffectiveReadTimeout_WithRelaxedTimeout(b *testing.B) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	// Set relaxed timeout with a deadline far in the future
	cn.SetRelaxedTimeoutWithDeadline(10*time.Second, 10*time.Second, time.Now().Add(1*time.Hour))

	normalTimeout := 5 * time.Second

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = cn.getEffectiveReadTimeout(normalTimeout)
	}
}

// BenchmarkGetEffectiveWriteTimeout_NoRelaxedTimeout benchmarks the fast path (no relaxed timeout set)
func BenchmarkGetEffectiveWriteTimeout_NoRelaxedTimeout(b *testing.B) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	normalTimeout := 5 * time.Second

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = cn.getEffectiveWriteTimeout(normalTimeout)
	}
}

// BenchmarkGetEffectiveWriteTimeout_WithRelaxedTimeout benchmarks when relaxed timeout is set
func BenchmarkGetEffectiveWriteTimeout_WithRelaxedTimeout(b *testing.B) {
	netConn := &net.TCPConn{}
	cn := NewConn(netConn)
	defer cn.Close()

	// Set relaxed timeout with a deadline far in the future
	cn.SetRelaxedTimeoutWithDeadline(10*time.Second, 10*time.Second, time.Now().Add(1*time.Hour))

	normalTimeout := 5 * time.Second

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = cn.getEffectiveWriteTimeout(normalTimeout)
	}
}

