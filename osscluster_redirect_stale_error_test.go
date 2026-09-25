package redis_test

import (
	"context"
	"errors"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestClusterRedirectClearsExecutionError(t *testing.T) {
	errSecondAttempt := errors.New("redis: reached second attempt")
	var secondAttempts int

	options := &redis.ClusterOptions{
		Addrs:                  []string{"127.0.0.1:0"},
		DisableRoutingPolicies: true,
		MaxRedirects:           1,
		NewClient: func(opt *redis.Options) *redis.Client {
			client := redis.NewClient(opt)
			client.AddHook(&hook{
				processHook: func(next redis.ProcessHook) redis.ProcessHook {
					return func(ctx context.Context, cmd redis.Cmder) error {
						if opt.Addr == "127.0.0.1:0" {
							return errors.New("MOVED 0 127.0.0.1:1")
						}
						secondAttempts++
						if err := cmd.Err(); err != nil {
							return err
						}
						return errSecondAttempt
					}
				},
			})
			return client
		},
		ClusterSlots: func(context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{{
				Start: 0,
				End:   16383,
				Nodes: []redis.ClusterNode{
					{Addr: "127.0.0.1:0"},
					{Addr: "127.0.0.1:1"},
				},
			}}, nil
		},
	}
	cluster := redis.NewClusterClient(options)
	defer cluster.Close()

	cmd := redis.NewStatusCmd(context.Background(), "ping")
	err := cluster.Process(context.Background(), cmd)
	if secondAttempts != 1 {
		t.Fatalf("second-node attempts = %d, want 1; err = %v", secondAttempts, err)
	}
	if !errors.Is(err, errSecondAttempt) {
		t.Fatalf("Process err = %v, want second-attempt error", err)
	}
}
