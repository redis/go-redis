package redis

import (
	"testing"
	"time"
)

// ParseFailoverURL disables a duration option with a value of 0 or less, and
// Options.init turns the disabled DialTimeout into "no deadline". The failover
// dialer reads FailoverOptions.DialTimeout directly instead of the derived
// Options, so it has to apply the same rule: a negative value in net.Dialer is
// an already expired deadline, and every connection to the master the sentinel
// just returned would fail at once.
func TestFailoverDialTimeoutDisabled(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		in   time.Duration
		want time.Duration
	}{
		{in: -1, want: 0},
		{in: -2 * time.Second, want: 0},
		{in: 0, want: 0},
		{in: 3 * time.Second, want: 3 * time.Second},
	} {
		if got := failoverDialTimeout(tc.in); got != tc.want {
			t.Errorf("failoverDialTimeout(%v) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

// The failover client derives its Options from the same FailoverOptions the
// dialer reads, so a disabled timeout has to end up as "no deadline" there too:
// ParseFailoverURL("...?dial_timeout=0") leaves FailoverOptions.DialTimeout at -1.
func TestNewFailoverClientDisabledDialTimeout(t *testing.T) {
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
