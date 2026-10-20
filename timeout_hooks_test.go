package redis_test

import (
	"context"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

type countingHook struct {
	process  atomic.Int32
	pipeline atomic.Int32
}

func TestWithTimeoutPreservesHooks(t *testing.T) {
	reachedDial := errors.New("dial reached")
	parent := redis.NewClient(&redis.Options{
		Addr:       "unused:1",
		MaxRetries: -1,
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			return nil, reachedDial
		},
	})
	t.Cleanup(func() {
		if err := parent.Close(); err != nil {
			t.Error(err)
		}
	})
	early := new(countingHook)
	parent.AddHook(early)
	clone := parent.WithTimeout(100 * time.Millisecond)
	late := new(countingHook)
	parent.AddHook(late)
	cloneOnly := new(countingHook)
	clone.AddHook(cloneOnly)
	for _, tc := range []struct {
		name   string
		client *redis.Client
	}{
		{"parent", parent}, {"clone", clone},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := tc.client.Ping(t.Context()).Err(); !errors.Is(err, reachedDial) {
				t.Errorf("PING error = %v, want dial sentinel", err)
			}
			_, err := tc.client.Pipelined(t.Context(), func(pipe redis.Pipeliner) error {
				pipe.Ping(t.Context())
				return nil
			})
			if !errors.Is(err, reachedDial) {
				t.Errorf("pipeline error = %v, want dial sentinel", err)
			}
			_, err = tc.client.TxPipelined(t.Context(), func(pipe redis.Pipeliner) error {
				pipe.Ping(t.Context())
				return nil
			})
			if !errors.Is(err, reachedDial) {
				t.Errorf("transaction pipeline error = %v, want dial sentinel", err)
			}
		})
	}
	for _, tc := range []struct {
		name              string
		hook              *countingHook
		process, pipeline int32
	}{
		{"inherited", early, 2, 4}, {"parent-only", late, 1, 2}, {"clone-only", cloneOnly, 1, 2},
	} {
		if got := tc.hook.process.Load(); got != tc.process {
			t.Errorf("%s ProcessHook calls = %d, want %d", tc.name, got, tc.process)
		}
		if got := tc.hook.pipeline.Load(); got != tc.pipeline {
			t.Errorf("%s ProcessPipelineHook calls = %d, want %d", tc.name, got, tc.pipeline)
		}
	}
	if parent.Options().ReadTimeout == 100*time.Millisecond || parent.Options().WriteTimeout == 100*time.Millisecond {
		t.Error("clone changed parent timeouts")
	}
	if clone.Options().ReadTimeout != 100*time.Millisecond || clone.Options().WriteTimeout != 100*time.Millisecond {
		t.Error("clone did not apply its timeouts")
	}
}

func (h *countingHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}
func (h *countingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.process.Add(1)
		return next(ctx, cmd)
	}
}
func (h *countingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		h.pipeline.Add(1)
		return next(ctx, cmds)
	}
}
