//go:build !linux

package main

import (
	"net"
	"syscall"
)

// tuneTCPConn is a no-op on non-Linux platforms.
func tuneTCPConn(tc *net.TCPConn) {}

// madviseHugepage is a no-op on non-Linux platforms.
func madviseHugepage(buf []byte) {}

// readCPUTime returns the process user + sys CPU time in nanoseconds.
// Uses getrusage where available; Rusage types differ across BSDs and Darwin
// but Utime/Stime fields are portable Timeval.
func readCPUTime() cpuTime {
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		return cpuTime{}
	}
	return cpuTime{
		user: int64(ru.Utime.Sec)*int64(1e9) + int64(ru.Utime.Usec)*1000,
		sys:  int64(ru.Stime.Sec)*int64(1e9) + int64(ru.Stime.Usec)*1000,
	}
}
