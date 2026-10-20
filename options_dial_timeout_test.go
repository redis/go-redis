package redis

import (
	"context"
	"crypto/tls"
	"net"
	"testing"
	"time"
)

// ParseURL documents that a duration option is disabled with a value of 0 or
// less, and ReadTimeout/WriteTimeout turn that into "no deadline". DialTimeout
// did not: the disabled value stayed negative, and net.Dialer treats a negative
// Timeout as an already expired deadline, so every dial failed at once.
func TestDialTimeoutDisabled(t *testing.T) {
	t.Parallel()

	// Dialing a socket that is listening must keep working when the dial
	// timeout is disabled; a negative timeout makes it fail immediately.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	for _, value := range []string{"0", "0s", "-1"} {
		opt, err := ParseURL("redis://" + ln.Addr().String() + "?dial_timeout=" + value)
		if err != nil {
			t.Fatalf("ParseURL(dial_timeout=%s): %v", value, err)
		}
		opt.init()

		conn, err := opt.NewDialer()(context.Background(), "tcp", ln.Addr().String())
		if err != nil {
			t.Fatalf("dial_timeout=%s: dialing a listening socket failed: %v (DialTimeout=%v)",
				value, err, opt.DialTimeout)
		}
		conn.Close()
	}
}

// The failover client derives its Options from the same FailoverOptions, and
// masterReplicaDialer reads that derived value, so a disabled timeout has to end
// up as "no deadline" there as well.
func TestFailoverClientDisabledDialTimeout(t *testing.T) {
	t.Parallel()

	rdb := NewFailoverClient(&FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:26379"},
		DialTimeout:   -1,
	})
	defer func() { _ = rdb.Close() }()

	if got := rdb.Options().DialTimeout; got != 0 {
		t.Errorf("Options().DialTimeout = %v, want 0", got)
	}
}

// NewDialer is exported, so a caller can build a dialer straight from the Options
// ParseURL returned — without NewClient, which clones the options and calls the
// unexported init. The disabled value has to be normalised on that path too.
func TestNewDialerDisabledDialTimeout(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	opt, err := ParseURL("redis://" + ln.Addr().String() + "?dial_timeout=0")
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if opt.DialTimeout != -1 {
		t.Fatalf("ParseURL should leave the disabled sentinel, got %v", opt.DialTimeout)
	}

	conn, err := opt.NewDialer()(context.Background(), "tcp", ln.Addr().String())
	if err != nil {
		t.Fatalf("NewDialer with a disabled dial timeout: dialing a listening socket failed: %v (DialTimeout=%v)",
			err, opt.DialTimeout)
	}
	_ = conn.Close()
}

// NewSentinelClient takes the caller's *Options and runs init() on it, so the
// clone is what keeps a disabled DialTimeout disabled: a second construction
// would otherwise take the "== 0 -> 5s" default branch and, because the dialer
// captures that same pointer, change the first client's dials as well.
func TestNewSentinelClientKeepsDisabledDialTimeout(t *testing.T) {
	t.Parallel()

	opt, err := ParseURL("redis://127.0.0.1:26379?dial_timeout=0")
	if err != nil {
		t.Fatalf("ParseURL: %v", err)
	}
	if opt.DialTimeout != -1 {
		t.Fatalf("ParseURL should leave the disabled sentinel, got %v", opt.DialTimeout)
	}

	first := NewSentinelClient(opt)
	defer func() { _ = first.Close() }()
	if got := first.opt.DialTimeout; got != 0 {
		t.Fatalf("first client DialTimeout = %v, want 0", got)
	}

	second := NewSentinelClient(opt)
	defer func() { _ = second.Close() }()

	if got := first.opt.DialTimeout; got != 0 {
		t.Fatalf("constructing a second client changed the first client's DialTimeout to %v", got)
	}
	if got := second.opt.DialTimeout; got != 0 {
		t.Fatalf("second client DialTimeout = %v, want 0", got)
	}
	if opt.DialTimeout != -1 {
		t.Fatalf("NewSentinelClient rewrote the caller's options: DialTimeout = %v", opt.DialTimeout)
	}
}

// A disabled DialTimeout means "no deadline", but the context still has to be
// able to stop the dial: tls.DialWithDialer ignores the context it is given and
// uses context.Background internally, so a rediss peer that accepts TCP and then
// stalls the handshake would pin the caller until the OS gives up.
func TestNewDialerTLSHonoursContext(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	// Accept and never speak TLS, so the handshake stalls.
	var held []net.Conn
	accepted := make(chan struct{})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			held = append(held, conn)
			select {
			case <-accepted:
			default:
				close(accepted)
			}
		}
	}()

	opt := &Options{
		Addr:        ln.Addr().String(),
		DialTimeout: 0, // disabled
		TLSConfig:   &tls.Config{InsecureSkipVerify: true},
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		conn, err := NewDialer(opt)(ctx, "tcp", ln.Addr().String())
		if conn != nil {
			_ = conn.Close()
		}
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the stalled TLS handshake to fail when the context expired")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("dial did not return after the context expired: the TLS handshake ignores the context")
	}

	<-accepted
}
