package redis

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
)

type pipelineSpillTimeoutPool struct {
	pool.Pooler
	gets atomic.Int32
}

func (p *pipelineSpillTimeoutPool) Get(context.Context) (*pool.Conn, error) {
	p.gets.Add(1)
	return nil, pool.ErrPoolTimeout
}

func TestPipelineSpillDoesNotCarryExhaustionIntoMainPoolTimeout(t *testing.T) {
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("pipeline dial unavailable")}
	var dials atomic.Int32
	client := NewClient(&Options{
		Addr: "127.0.0.1:6379", PoolSize: 16, PipelinePoolSize: 1,
		DialerRetries:      2,
		DialerRetryBackoff: func(int) time.Duration { return 0 },
	})
	t.Cleanup(func() { _ = client.Close() })
	client.AddHook(fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
		dials.Add(1)
		return nil, dialErr
	}})

	mainPool := client.baseClient.connPool.(*pool.ConnPool)
	spill := &pipelineSpillTimeoutPool{Pooler: mainPool}
	client.baseClient.connPool = spill
	client.baseClient.dialExhaustionPool = mainPool

	ctx, marker := client.baseClient.captureFixedPipelineDialExhaustion(context.Background())
	if marker == nil {
		t.Fatal("fixed endpoint must track dial exhaustion")
	}
	err := client.baseClient.withPipelineConn(ctx, func(context.Context, *pool.Conn) error {
		t.Fatal("no connection should be acquired")
		return nil
	})
	if !errors.Is(err, pool.ErrPoolTimeout) {
		t.Fatalf("spill error = %v, want main pool timeout", err)
	}
	if got := dials.Load(); got < 2 {
		t.Errorf("pipeline dial calls = %d, want an exhausted dial sequence", got)
	}
	if got := spill.gets.Load(); got != 1 {
		t.Errorf("main pool acquisitions = %d, want 1", got)
	}
	if marker.Load() {
		t.Fatal("pipeline dial exhaustion must not suppress retry of main pool timeout")
	}
}

type fixedDialFailureHook struct {
	dial DialHook
}

func (h fixedDialFailureHook) DialHook(DialHook) DialHook               { return h.dial }
func (h fixedDialFailureHook) ProcessHook(next ProcessHook) ProcessHook { return next }
func (h fixedDialFailureHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

func TestCommandRetryAfterPoolDialExhaustion(t *testing.T) {
	for _, tc := range []struct {
		name   string
		custom bool
		want   int
	}{
		{name: "fixed_endpoint", want: 2},
		{name: "custom_dialer", custom: true, want: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const addr = "127.0.0.1:6379"
			dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("dial unavailable")}
			calls := 0
			dial := func(_ context.Context, network, address string) (net.Conn, error) {
				calls++
				if network != "tcp" || address != addr {
					t.Errorf("dial target = %s %s", network, address)
				}
				return nil, dialErr
			}
			opt := &Options{
				Addr: addr, PoolSize: 16, MaxRetries: 2, DialerRetries: 2,
				DialerRetryBackoff: func(int) time.Duration { return 0 },
				MinRetryBackoff:    -1, MaxRetryBackoff: -1,
			}
			if tc.custom {
				opt.Dialer = dial
			}
			client := NewClient(opt)
			t.Cleanup(func() { _ = client.Close() })
			if !tc.custom {
				client.AddHook(fixedDialFailureHook{dial: dial})
			}
			err := client.Ping(context.Background()).Err()
			if err != dialErr {
				t.Fatalf("returned error = %v (%T), want original *net.OpError", err, err)
			}
			if calls != tc.want {
				t.Errorf("dial calls = %d, want %d", calls, tc.want)
			}
		})
	}
}

func TestSentinelDoesNotRestartFixedDialSequence(t *testing.T) {
	for _, tc := range []struct {
		name   string
		custom bool
		want   int32
	}{
		{name: "fixed_endpoint", want: 2},
		{name: "custom_dialer", custom: true, want: 6},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const addr = "127.0.0.1:6379"
			dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("dial unavailable")}
			var calls atomic.Int32
			dial := func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				return nil, dialErr
			}
			opt := &Options{
				Addr: addr, PoolSize: 16, MaxRetries: 2, DialerRetries: 2,
				DialerRetryBackoff: func(int) time.Duration { return 0 },
				MinRetryBackoff:    -1, MaxRetryBackoff: -1,
			}
			if tc.custom {
				opt.Dialer = dial
			}
			client := NewSentinelClient(opt)
			t.Cleanup(func() { _ = client.Close() })
			if !tc.custom {
				client.AddHook(fixedDialFailureHook{dial: dial})
			}
			if err := client.Ping(context.Background()).Err(); err != dialErr {
				t.Fatalf("returned error = %v (%T), want original *net.OpError", err, err)
			}
			if got := calls.Load(); got != tc.want {
				t.Errorf("dial calls = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestStickyConnDoesNotRestartFixedDialSequence(t *testing.T) {
	for _, operation := range []string{"conn", "ephemeral"} {
		t.Run(operation, func(t *testing.T) {
			dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("dial unavailable")}
			var calls atomic.Int32
			client := NewClient(&Options{
				Addr: "127.0.0.1:6379", PoolSize: 16, MaxRetries: 2, DialerRetries: 2,
				DialerRetryBackoff: func(int) time.Duration { return 0 },
				MinRetryBackoff:    -1, MaxRetryBackoff: -1,
			})
			t.Cleanup(func() { _ = client.Close() })
			client.AddHook(fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				return nil, dialErr
			}})

			var conn *Conn
			if operation == "conn" {
				conn = client.Conn()
			} else {
				conn = client.EphemeralConn()
			}
			t.Cleanup(func() { _ = conn.Close() })
			if err := conn.Ping(context.Background()).Err(); err != dialErr {
				t.Fatalf("returned error = %v (%T), want original *net.OpError", err, err)
			}
			if got := calls.Load(); got != 2 {
				t.Errorf("dial calls = %d, want 2", got)
			}
		})
	}
}

func TestRingDoesNotRestartDialSequenceForSameShard(t *testing.T) {
	const addr = "127.0.0.1:6379"
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("dial unavailable")}
	var calls atomic.Int32
	ring := NewRing(&RingOptions{
		Addrs: map[string]string{"test": addr}, PoolSize: 16, MaxRetries: 2, DialerRetries: 2,
		DialerRetryBackoff: func(int) time.Duration { return 0 },
		MinRetryBackoff:    -1, MaxRetryBackoff: -1, HeartbeatFrequency: time.Hour,
		NewClient: func(opt *Options) *Client {
			client := NewClient(opt)
			client.AddHook(fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				return nil, dialErr
			}})
			return client
		},
	})
	t.Cleanup(func() { _ = ring.Close() })
	if err := ring.Get(context.Background(), "key").Err(); err != dialErr {
		t.Fatalf("returned error = %v (%T), want original *net.OpError", err, err)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("dial calls = %d, want 2", got)
	}
}

func TestPipelineAndWatchDoNotRestartFixedDialSequence(t *testing.T) {
	for _, operation := range []string{"pipeline", "dedicated_pipeline", "watch"} {
		t.Run(operation, func(t *testing.T) {
			const addr = "127.0.0.1:6379"
			dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: context.DeadlineExceeded}
			var calls atomic.Int32
			opt := &Options{
				Addr: addr, PoolSize: 16, MaxRetries: 2, DialerRetries: 2,
				DialerRetryBackoff: func(int) time.Duration { return 0 },
				MinRetryBackoff:    -1, MaxRetryBackoff: -1,
			}
			if operation == "dedicated_pipeline" {
				opt.PipelineReadBufferSize = 64 * 1024
				opt.PipelineWriteBufferSize = 64 * 1024
			}
			client := NewClient(opt)
			t.Cleanup(func() { _ = client.Close() })
			client.AddHook(fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
				calls.Add(1)
				return nil, dialErr
			}})

			ctx := context.Background()
			var err error
			if operation == "watch" {
				err = client.Watch(ctx, func(*Tx) error { return nil }, "key")
			} else {
				pipe := client.Pipeline()
				pipe.Ping(ctx)
				_, err = pipe.Exec(ctx)
			}
			if err != dialErr {
				t.Fatalf("returned error = %v (%T), want original *net.OpError", err, err)
			}
			if got := calls.Load(); got != 2 {
				t.Errorf("dial calls = %d, want 2", got)
			}
		})
	}
}

type ringRetrySelectionHash struct{ calls atomic.Int32 }

func (h *ringRetrySelectionHash) Get(string) string {
	if h.calls.Add(1) <= 2 {
		return "dead"
	}
	return "healthy"
}

func TestRingKeylessRetrySkipsExhaustedShard(t *testing.T) {
	const deadAddr = "127.0.0.1:7301"
	const healthyAddr = "127.0.0.1:7302"
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("dial unavailable")}
	var failedDials atomic.Int32
	var healthyPings int
	selection := &ringRetrySelectionHash{}
	ring := NewRing(&RingOptions{
		Addrs: map[string]string{"dead": deadAddr, "healthy": healthyAddr},
		PoolSize: 16, MaxRetries: 2, DialerRetries: 2,
		DialerRetryBackoff: func(int) time.Duration { return 0 },
		MinRetryBackoff: -1, MaxRetryBackoff: -1, HeartbeatFrequency: time.Hour,
		NewConsistentHash: func([]string) ConsistentHash { return selection },
		NewClient: func(opt *Options) *Client {
			client := NewClient(opt)
			if opt.Addr == deadAddr {
				client.AddHook(fixedDialFailureHook{dial: func(context.Context, string, string) (net.Conn, error) {
					failedDials.Add(1)
					return nil, dialErr
				}})
			} else {
				client.AddHook(pongHook{pings: &healthyPings})
			}
			return client
		},
	})
	t.Cleanup(func() { _ = ring.Close() })
	if got, err := ring.Ping(context.Background()).Result(); err != nil || got != "PONG" {
		t.Fatalf("keyless retry = (%q, %v), want (PONG, nil)", got, err)
	}
	if got := selection.calls.Load(); got != 3 {
		t.Errorf("shard selections = %d, want 3 (dead, dead, healthy)", got)
	}
	if got := failedDials.Load(); got != 2 {
		t.Errorf("failed shard dials = %d, want one exhausted sequence of 2", got)
	}
	if healthyPings != 1 {
		t.Errorf("healthy shard PINGs = %d, want 1", healthyPings)
	}
}
