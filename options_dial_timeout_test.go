package redis

import (
	"context"
	"net"
	"testing"
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
