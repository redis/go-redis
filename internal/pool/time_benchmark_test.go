package pool

import (
	"sync/atomic"
	"testing"
	"time"
)

// BenchmarkTimeNow measures the cost of time.Now()
func BenchmarkTimeNow(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = time.Now()
	}
}

// BenchmarkTimeNowUnixNano measures the cost of time.Now().UnixNano()
func BenchmarkTimeNowUnixNano(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = time.Now().UnixNano()
	}
}

// BenchmarkAtomicLoad measures the cost of atomic load
func BenchmarkAtomicLoad(b *testing.B) {
	var val atomic.Int64
	val.Store(12345)
	
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = val.Load()
	}
}

// BenchmarkGetCachedTimeNs measures the cost of our cached time function
func BenchmarkGetCachedTimeNs(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = getCachedTimeNs()
	}
}

