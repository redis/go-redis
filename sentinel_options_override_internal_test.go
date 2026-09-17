package redis

import (
	"context"
	"net"
	"testing"
	"time"
)

func baseFailoverOptions() *FailoverOptions {
	return &FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:26379"},

		DialTimeout:  7 * time.Second,
		ReadTimeout:  8 * time.Second,
		WriteTimeout: 9 * time.Second,

		MaxRetries:         4,
		DialerRetries:      6,
		DialerRetryTimeout: 250 * time.Millisecond,

		PoolSize:           50,
		PoolTimeout:        2 * time.Second,
		MaxConcurrentDials: 25,
		MinIdleConns:       10,
		MaxIdleConns:       20,
		MaxActiveConns:     60,
		ConnMaxIdleTime:    30 * time.Minute,
		ConnMaxLifetime:    time.Hour,
	}
}

// A nil override must reproduce the historical behaviour exactly: sentinel
// connections inherit every top-level setting.
func TestSentinelOptionsOverride_NilInheritsEverything(t *testing.T) {
	opt := baseFailoverOptions()
	sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"DialTimeout", sentinelOpt.DialTimeout, opt.DialTimeout},
		{"ReadTimeout", sentinelOpt.ReadTimeout, opt.ReadTimeout},
		{"WriteTimeout", sentinelOpt.WriteTimeout, opt.WriteTimeout},
		{"MaxRetries", sentinelOpt.MaxRetries, opt.MaxRetries},
		{"DialerRetries", sentinelOpt.DialerRetries, opt.DialerRetries},
		{"DialerRetryTimeout", sentinelOpt.DialerRetryTimeout, opt.DialerRetryTimeout},
		{"PoolSize", sentinelOpt.PoolSize, opt.PoolSize},
		{"PoolTimeout", sentinelOpt.PoolTimeout, opt.PoolTimeout},
		{"MaxConcurrentDials", sentinelOpt.MaxConcurrentDials, opt.MaxConcurrentDials},
		{"MinIdleConns", sentinelOpt.MinIdleConns, opt.MinIdleConns},
		{"MaxIdleConns", sentinelOpt.MaxIdleConns, opt.MaxIdleConns},
		{"MaxActiveConns", sentinelOpt.MaxActiveConns, opt.MaxActiveConns},
		{"ConnMaxIdleTime", sentinelOpt.ConnMaxIdleTime, opt.ConnMaxIdleTime},
		{"ConnMaxLifetime", sentinelOpt.ConnMaxLifetime, opt.ConnMaxLifetime},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want inherited %v", c.name, c.got, c.want)
		}
	}
}

// Every field set on the override must win over the top-level value.
func TestSentinelOptionsOverride_OverridesWin(t *testing.T) {
	opt := baseFailoverOptions()
	opt.SentinelOptionsOverride = &SentinelOptionsOverride{
		DialTimeout:        100 * time.Millisecond,
		ReadTimeout:        200 * time.Millisecond,
		WriteTimeout:       300 * time.Millisecond,
		MaxRetries:         1,
		DialerRetries:      2,
		DialerRetryTimeout: 50 * time.Millisecond,
		PoolSize:           3,
		PoolTimeout:        400 * time.Millisecond,
		MaxConcurrentDials: 2,
		MaxIdleConns:       1,
		MaxActiveConns:     3,
		ConnMaxIdleTime:    time.Minute,
		ConnMaxLifetime:    5 * time.Minute,
	}

	sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])
	ov := opt.SentinelOptionsOverride

	checks := []struct {
		name string
		got  any
		want any
	}{
		{"DialTimeout", sentinelOpt.DialTimeout, ov.DialTimeout},
		{"ReadTimeout", sentinelOpt.ReadTimeout, ov.ReadTimeout},
		{"WriteTimeout", sentinelOpt.WriteTimeout, ov.WriteTimeout},
		{"MaxRetries", sentinelOpt.MaxRetries, ov.MaxRetries},
		{"DialerRetries", sentinelOpt.DialerRetries, ov.DialerRetries},
		{"DialerRetryTimeout", sentinelOpt.DialerRetryTimeout, ov.DialerRetryTimeout},
		{"PoolSize", sentinelOpt.PoolSize, ov.PoolSize},
		{"PoolTimeout", sentinelOpt.PoolTimeout, ov.PoolTimeout},
		{"MaxConcurrentDials", sentinelOpt.MaxConcurrentDials, ov.MaxConcurrentDials},
		{"MaxIdleConns", sentinelOpt.MaxIdleConns, ov.MaxIdleConns},
		{"MaxActiveConns", sentinelOpt.MaxActiveConns, ov.MaxActiveConns},
		{"ConnMaxIdleTime", sentinelOpt.ConnMaxIdleTime, ov.ConnMaxIdleTime},
		{"ConnMaxLifetime", sentinelOpt.ConnMaxLifetime, ov.ConnMaxLifetime},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want override %v", c.name, c.got, c.want)
		}
	}
}

// A partial override must leave every unnamed field inherited.
func TestSentinelOptionsOverride_ZeroFieldsInherit(t *testing.T) {
	opt := baseFailoverOptions()
	opt.SentinelOptionsOverride = &SentinelOptionsOverride{PoolSize: 2}

	sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])

	if sentinelOpt.PoolSize != 2 {
		t.Errorf("PoolSize = %d, want override 2", sentinelOpt.PoolSize)
	}
	if sentinelOpt.ReadTimeout != opt.ReadTimeout {
		t.Errorf("ReadTimeout = %v, want inherited %v", sentinelOpt.ReadTimeout, opt.ReadTimeout)
	}
	if sentinelOpt.MinIdleConns != opt.MinIdleConns {
		t.Errorf("MinIdleConns = %d, want inherited %d", sentinelOpt.MinIdleConns, opt.MinIdleConns)
	}
}

// -1 expresses an explicit zero, which the plain zero value cannot: it means
// "keep no idle connections to sentinels" even though the data-node pool keeps
// some warm. This is the case that motivates the whole option.
func TestSentinelOptionsOverride_NegativeMeansExplicitZero(t *testing.T) {
	opt := baseFailoverOptions()
	opt.SentinelOptionsOverride = &SentinelOptionsOverride{
		MinIdleConns:   -1,
		MaxIdleConns:   -1,
		MaxActiveConns: -1,
	}

	sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])

	if sentinelOpt.MinIdleConns != 0 {
		t.Errorf("MinIdleConns = %d, want 0 (explicitly none)", sentinelOpt.MinIdleConns)
	}
	if sentinelOpt.MaxIdleConns != 0 {
		t.Errorf("MaxIdleConns = %d, want 0", sentinelOpt.MaxIdleConns)
	}
	if sentinelOpt.MaxActiveConns != 0 {
		t.Errorf("MaxActiveConns = %d, want 0", sentinelOpt.MaxActiveConns)
	}
	// The data-node pool must keep its warm connections.
	if opt.MinIdleConns != 10 {
		t.Errorf("top-level MinIdleConns = %d, want 10 (untouched)", opt.MinIdleConns)
	}
}

// DialTimeout is the one timeout with no "disable" representation: Options.init
// only defaults a zero value, and a negative one reaches net.Dialer.Timeout,
// which treats it as a deadline already in the past and fails every dial. A
// non-positive override must therefore inherit, so the client still dials.
func TestSentinelOptionsOverride_NonPositiveDialTimeoutInherits(t *testing.T) {
	for _, dialTimeout := range []time.Duration{-1, -time.Second} {
		opt := baseFailoverOptions()
		opt.SentinelOptionsOverride = &SentinelOptionsOverride{DialTimeout: dialTimeout}

		sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])
		if sentinelOpt.DialTimeout != opt.DialTimeout {
			t.Errorf("DialTimeout %v: got %v, want inherited %v",
				dialTimeout, sentinelOpt.DialTimeout, opt.DialTimeout)
		}

		// After init() the effective timeout must still be usable, otherwise
		// net.Dialer would fail every sentinel dial immediately.
		sentinelOpt.init()
		if sentinelOpt.DialTimeout <= 0 {
			t.Errorf("DialTimeout %v: post-init %v would break every dial",
				dialTimeout, sentinelOpt.DialTimeout)
		}
	}
}

// PoolTimeout has the same exposure as DialTimeout: Options.init defaults only
// the zero case, and FastSemaphore.Acquire resets a timer to the given duration,
// so a negative value makes every wait for a sentinel connection fail at once
// with ErrPoolTimeout. A non-positive override must inherit instead.
func TestSentinelOptionsOverride_NonPositivePoolTimeoutInherits(t *testing.T) {
	for _, poolTimeout := range []time.Duration{-1, -time.Second} {
		opt := baseFailoverOptions()
		opt.SentinelOptionsOverride = &SentinelOptionsOverride{PoolTimeout: poolTimeout}

		sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])
		if sentinelOpt.PoolTimeout != opt.PoolTimeout {
			t.Errorf("PoolTimeout %v: got %v, want inherited %v",
				poolTimeout, sentinelOpt.PoolTimeout, opt.PoolTimeout)
		}

		sentinelOpt.init()
		if sentinelOpt.PoolTimeout <= 0 {
			t.Errorf("PoolTimeout %v: post-init %v would fail every pool wait",
				poolTimeout, sentinelOpt.PoolTimeout)
		}
	}
}

// The durations whose underlying Options value does give a non-positive value a
// defined meaning must still forward it unchanged.
func TestSentinelOptionsOverride_NegativeConnLifetimesPassThrough(t *testing.T) {
	opt := baseFailoverOptions()
	opt.SentinelOptionsOverride = &SentinelOptionsOverride{
		ConnMaxIdleTime: -1,
		ConnMaxLifetime: -1,
	}

	sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])

	// The pool guards both with > 0, so -1 means "never expire".
	if sentinelOpt.ConnMaxIdleTime != -1 {
		t.Errorf("ConnMaxIdleTime = %v, want -1 forwarded", sentinelOpt.ConnMaxIdleTime)
	}
	if sentinelOpt.ConnMaxLifetime != -1 {
		t.Errorf("ConnMaxLifetime = %v, want -1 forwarded", sentinelOpt.ConnMaxLifetime)
	}
}

// PoolSize is the one count with no non-positive meaning. Options.init defaults
// only the zero case, so a negative value would survive into NewConnPool, where
// make(chan struct{}, PoolSize) panics with "makechan: size out of range". A
// non-positive override must inherit, and constructing a client must not panic.
func TestSentinelOptionsOverride_NonPositivePoolSizeInherits(t *testing.T) {
	for _, poolSize := range []int{-1, -10} {
		opt := baseFailoverOptions()
		opt.SentinelOptionsOverride = &SentinelOptionsOverride{PoolSize: poolSize}

		sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])
		if sentinelOpt.PoolSize != opt.PoolSize {
			t.Errorf("PoolSize %d: got %d, want inherited %d",
				poolSize, sentinelOpt.PoolSize, opt.PoolSize)
		}

		sentinelOpt.init()
		if sentinelOpt.PoolSize <= 0 {
			t.Errorf("PoolSize %d: post-init %d would panic in NewConnPool",
				poolSize, sentinelOpt.PoolSize)
		}

		// The real guard: building the client must not panic.
		func() {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("PoolSize %d: NewSentinelClient panicked: %v", poolSize, r)
				}
			}()
			NewSentinelClient(opt.sentinelOptions(opt.SentinelAddrs[0])).Close()
		}()
	}
}

// Read/WriteTimeout do have a disable convention, and it must survive the
// override path: Options.init maps -1 to 0, i.e. no timeout.
func TestSentinelOptionsOverride_NegativeReadWriteTimeoutDisables(t *testing.T) {
	opt := baseFailoverOptions()
	opt.SentinelOptionsOverride = &SentinelOptionsOverride{
		ReadTimeout:  -1,
		WriteTimeout: -1,
	}

	sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])
	sentinelOpt.init()

	if sentinelOpt.ReadTimeout != 0 {
		t.Errorf("ReadTimeout = %v, want 0 (no timeout)", sentinelOpt.ReadTimeout)
	}
	if sentinelOpt.WriteTimeout != 0 {
		t.Errorf("WriteTimeout = %v, want 0 (no timeout)", sentinelOpt.WriteTimeout)
	}
}

// A sentinel-scoped Dialer must be used for sentinel connections and must not
// replace the Dialer used for master/replica connections.
func TestSentinelOptionsOverride_DialerIsSentinelScoped(t *testing.T) {
	var dataNodeDialerUsed, sentinelDialerUsed bool

	opt := baseFailoverOptions()
	opt.Dialer = func(ctx context.Context, network, addr string) (net.Conn, error) {
		dataNodeDialerUsed = true
		return nil, net.UnknownNetworkError(network)
	}
	opt.SentinelOptionsOverride = &SentinelOptionsOverride{
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			sentinelDialerUsed = true
			return nil, net.UnknownNetworkError(network)
		},
	}

	sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])
	_, _ = sentinelOpt.Dialer(context.Background(), "tcp", "127.0.0.1:26379")
	if !sentinelDialerUsed {
		t.Error("sentinel connections did not use the override Dialer")
	}
	if dataNodeDialerUsed {
		t.Error("sentinel connections used the top-level Dialer")
	}

	// The data-node path keeps the top-level Dialer.
	sentinelDialerUsed, dataNodeDialerUsed = false, false
	clusterOpt := opt.clusterOptions()
	_, _ = clusterOpt.Dialer(context.Background(), "tcp", "127.0.0.1:6379")
	if !dataNodeDialerUsed {
		t.Error("master/replica connections did not use the top-level Dialer")
	}
	if sentinelDialerUsed {
		t.Error("master/replica connections leaked the sentinel override Dialer")
	}
}

// The override must never alter the options used for master/replica connections.
func TestSentinelOptionsOverride_DataNodeOptionsUnaffected(t *testing.T) {
	opt := baseFailoverOptions()
	opt.SentinelOptionsOverride = &SentinelOptionsOverride{
		PoolSize:     2,
		MinIdleConns: -1,
		ReadTimeout:  time.Millisecond,
	}

	clusterOpt := opt.clusterOptions()
	clientOpt := opt.clientOptions()

	dataNodes := map[string]struct {
		poolSize     int
		minIdleConns int
		readTimeout  time.Duration
	}{
		"clusterOptions": {clusterOpt.PoolSize, clusterOpt.MinIdleConns, clusterOpt.ReadTimeout},
		"clientOptions":  {clientOpt.PoolSize, clientOpt.MinIdleConns, clientOpt.ReadTimeout},
	}

	for name, dataNode := range dataNodes {
		if dataNode.poolSize != opt.PoolSize {
			t.Errorf("%s PoolSize = %d, want %d", name, dataNode.poolSize, opt.PoolSize)
		}
		if dataNode.minIdleConns != opt.MinIdleConns {
			t.Errorf("%s MinIdleConns = %d, want %d", name, dataNode.minIdleConns, opt.MinIdleConns)
		}
		if dataNode.readTimeout != opt.ReadTimeout {
			t.Errorf("%s ReadTimeout = %v, want %v", name, dataNode.readTimeout, opt.ReadTimeout)
		}
	}
}

// UniversalOptions must carry the override through to FailoverOptions.
func TestSentinelOptionsOverride_PropagatesFromUniversalOptions(t *testing.T) {
	override := &SentinelOptionsOverride{PoolSize: 2}
	universal := &UniversalOptions{
		Addrs:                   []string{"127.0.0.1:26379"},
		MasterName:              "mymaster",
		PoolSize:                50,
		SentinelOptionsOverride: override,
	}

	failover := universal.Failover()
	if failover.SentinelOptionsOverride != override {
		t.Fatal("UniversalOptions.Failover() dropped SentinelOptionsOverride")
	}
	if got := failover.sentinelOptions("127.0.0.1:26379").PoolSize; got != 2 {
		t.Errorf("sentinel PoolSize = %d, want 2", got)
	}
	if got := failover.clusterOptions().PoolSize; got != 50 {
		t.Errorf("data-node PoolSize = %d, want 50", got)
	}
}

// ConnPool.dialRetryBackoff consults DialerRetryBackoff first and only reads
// DialerRetryTimeout when that callback is nil. A backoff callback configured
// for the data nodes is inherited by sentinelOptions, so without clearing it a
// sentinel-scoped DialerRetryTimeout would never be consulted and a long
// data-node backoff would keep delaying master discovery.
func TestSentinelOptionsOverride_RetryTimeoutDisplacesInheritedBackoff(t *testing.T) {
	opt := baseFailoverOptions()
	opt.DialerRetryBackoff = func(attempt int) time.Duration { return time.Minute }
	opt.SentinelOptionsOverride = &SentinelOptionsOverride{
		DialerRetryTimeout: 20 * time.Millisecond,
	}

	sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])
	if sentinelOpt.DialerRetryBackoff != nil {
		t.Error("inherited DialerRetryBackoff must be cleared, or the override is never read")
	}
	if sentinelOpt.DialerRetryTimeout != 20*time.Millisecond {
		t.Errorf("DialerRetryTimeout: got %v, want %v",
			sentinelOpt.DialerRetryTimeout, 20*time.Millisecond)
	}

	// The data-node options keep their callback.
	if opt.DialerRetryBackoff == nil {
		t.Error("data-node DialerRetryBackoff must not be touched")
	}
}

// Without a DialerRetryTimeout override there is nothing to displace, so the
// callback is inherited unchanged — sentinels keep following the data-node
// backoff policy, which is the documented default.
func TestSentinelOptionsOverride_BackoffInheritedWhenRetryTimeoutUnset(t *testing.T) {
	opt := baseFailoverOptions()
	opt.DialerRetryBackoff = func(attempt int) time.Duration { return time.Minute }
	opt.SentinelOptionsOverride = &SentinelOptionsOverride{PoolSize: 3}

	sentinelOpt := opt.sentinelOptions(opt.SentinelAddrs[0])
	if sentinelOpt.DialerRetryBackoff == nil {
		t.Fatal("DialerRetryBackoff must be inherited when DialerRetryTimeout is unset")
	}
	if got := sentinelOpt.DialerRetryBackoff(1); got != time.Minute {
		t.Errorf("inherited backoff: got %v, want %v", got, time.Minute)
	}
}
