//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd || solaris || illumos

package pool_test

import (
	"crypto/tls"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

// opaqueConn deliberately exposes only net.Conn, modelling wrappers that do
// not provide syscall.Conn or an underlying NetConn.
type opaqueConn struct {
	net.Conn
}

func TestMaybeHasData_ConservativeForOpaqueConn(t *testing.T) {
	server, client := net.Pipe()
	t.Cleanup(func() {
		_ = server.Close()
		_ = client.Close()
	})

	cn := pool.NewConn(&opaqueConn{Conn: client})
	if cn.MaybeHasData() {
		t.Fatal("an opaque connection must not force a timed read on every readiness check")
	}
	cn.MarkCscReadPending()
	if !cn.TakeCscReadPending() {
		t.Fatal("an opaque connection must request one bounded post-command read")
	}
	if !cn.TakeCscPeriodicReadPending(time.Hour) {
		t.Fatal("an opaque connection must retain a throttled periodic fallback read")
	}
	if cn.TakeCscPeriodicReadPending(time.Hour) {
		t.Fatal("periodic fallback reads must be throttled")
	}
}

func TestMaybeHasData_UnwrapsTLSConn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			accepted <- conn
		}
	}()

	rawClient, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { _ = rawClient.Close() })

	var server net.Conn
	select {
	case server = <-accepted:
	case <-time.After(time.Second):
		t.Fatal("accept timed out")
	}
	t.Cleanup(func() { _ = server.Close() })

	tlsClient := tls.Client(rawClient, &tls.Config{
		MinVersion: tls.VersionTLS12,
	})
	cn := pool.NewConn(tlsClient)

	if cn.MaybeHasData() {
		t.Fatal("an empty TLS transport must not trigger continuous drain reads")
	}
	cn.MarkCscReadPending()
	if !cn.TakeCscReadPending() {
		t.Fatal("TLS command reads must request one conservative post-read drain")
	}
	if cn.TakeCscReadPending() {
		t.Fatal("the conservative post-read request must be consumed exactly once")
	}
	if cn.TakeCscPeriodicReadPending(time.Millisecond) {
		t.Fatal("TLS exposes socket readiness and must not use periodic fallback reads")
	}

	// Raw data waiting below the TLS wrapper must be reported. It need not be
	// valid TLS because MaybeHasData only peeks and does not consume it.
	if _, err := server.Write([]byte{1}); err != nil {
		t.Fatalf("write: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for !cn.MaybeHasData() && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if !cn.MaybeHasData() {
		t.Fatal("TLS wrapper hid data waiting on its underlying socket")
	}
}
