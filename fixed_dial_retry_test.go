package redis

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

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
