//go:build linux

package main

import (
	"net"
	"syscall"
	"unsafe"
)

// Linux-specific TCP_QUICKACK constant (from <netinet/tcp.h>).
const tcpQuickAck = 12

// MADV_HUGEPAGE value from <sys/mman.h> on Linux.
const madvHugepage = 14

// tuneTCPConn applies Linux-only TCP tuning to an established connection.
// TCP_QUICKACK disables delayed ACKs, reducing round-trip latency for
// pipelined / request-response workloads. The kernel may re-enable delayed
// ACKs after a burst, so this is a best-effort hint.
func tuneTCPConn(tc *net.TCPConn) {
	raw, err := tc.SyscallConn()
	if err != nil {
		return
	}
	_ = raw.Control(func(fd uintptr) {
		_ = syscall.SetsockoptInt(int(fd), syscall.IPPROTO_TCP, tcpQuickAck, 1)
	})
}

// madviseHugepage hints the kernel to back a large buffer with transparent
// huge pages, reducing TLB misses during bulk memcpy. Best-effort; silently
// ignored on kernels where THP is disabled.
func madviseHugepage(buf []byte) {
	if len(buf) < 2*1024*1024 {
		return
	}
	_, _, _ = syscall.Syscall(syscall.SYS_MADVISE,
		uintptr(unsafe.Pointer(&buf[0])), uintptr(len(buf)), uintptr(madvHugepage))
}

// readCPUTime returns the process user + sys CPU time in nanoseconds.
func readCPUTime() cpuTime {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return cpuTime{}
	}
	return cpuTime{
		user: ru.Utime.Sec*int64(1e9) + int64(ru.Utime.Usec)*1000,
		sys:  ru.Stime.Sec*int64(1e9) + int64(ru.Stime.Usec)*1000,
	}
}
