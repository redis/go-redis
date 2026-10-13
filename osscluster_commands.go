package redis

import (
	"context"
	"sync"
	"sync/atomic"
)

// Preserve the existing cluster-wide helpers unless a resolver selects a policy
// for this invocation. A resolver for an unrelated command must not change them.
func (c *ClusterClient) usesLegacyClusterHelpers(ctx context.Context, cmd Cmder) bool {
	if c.opt.DisableRoutingPolicies || c.cmdInfoResolver == nil ||
		(c.cmdInfoResolver.legacyDefault && c.cmdInfoResolver.fallBackResolver == nil) {
		return true
	}
	decision := c.resolveCommandRoutingDecision(ctx, cmd)
	if decision.policy == nil && !decision.policyFromMetadata {
		return true
	}
	// Process consumes this decision without invoking a custom resolver twice.
	cmd.setClusterRoutingDecision(&decision)
	return false
}

func (c *ClusterClient) DBSize(ctx context.Context) *IntCmd {
	cmd := NewIntCmd(ctx, "dbsize")
	if c.usesLegacyClusterHelpers(ctx, cmd) {
		_ = c.withProcessHook(ctx, cmd, func(ctx context.Context, _ Cmder) error {
			var size atomic.Int64
			err := c.ForEachMaster(ctx, func(ctx context.Context, master *Client) error {
				n, err := master.DBSize(ctx).Result()
				if err != nil {
					return err
				}
				size.Add(n)
				return nil
			})
			if err != nil {
				cmd.SetErr(err)
			} else {
				cmd.val = size.Load()
			}
			return nil
		})
		return cmd
	}
	_ = c.Process(ctx, cmd)
	cmd.setClusterRoutingDecision(nil) // A process hook may have short-circuited.
	return cmd
}

func (c *ClusterClient) ScriptLoad(ctx context.Context, script string) *StringCmd {
	cmd := NewStringCmd(ctx, "script", "load", script)
	if c.usesLegacyClusterHelpers(ctx, cmd) {
		_ = c.withProcessHook(ctx, cmd, func(ctx context.Context, _ Cmder) error {
			var mu sync.Mutex
			err := c.ForEachShard(ctx, func(ctx context.Context, shard *Client) error {
				val, err := shard.ScriptLoad(ctx, script).Result()
				if err != nil {
					return err
				}

				mu.Lock()
				if cmd.Val() == "" {
					cmd.val = val
				}
				mu.Unlock()

				return nil
			})
			if err != nil {
				cmd.SetErr(err)
			}
			return nil
		})
		return cmd
	}
	_ = c.Process(ctx, cmd)
	cmd.setClusterRoutingDecision(nil) // A process hook may have short-circuited.
	return cmd
}

func (c *ClusterClient) ScriptFlush(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd(ctx, "script", "flush")
	if c.usesLegacyClusterHelpers(ctx, cmd) {
		_ = c.withProcessHook(ctx, cmd, func(ctx context.Context, _ Cmder) error {
			err := c.ForEachShard(ctx, func(ctx context.Context, shard *Client) error {
				return shard.ScriptFlush(ctx).Err()
			})
			if err != nil {
				cmd.SetErr(err)
			}
			return nil
		})
		return cmd
	}
	_ = c.Process(ctx, cmd)
	cmd.setClusterRoutingDecision(nil) // A process hook may have short-circuited.
	return cmd
}

func (c *ClusterClient) ScriptExists(ctx context.Context, hashes ...string) *BoolSliceCmd {
	args := make([]interface{}, 2+len(hashes))
	args[0] = "script"
	args[1] = "exists"
	for i, hash := range hashes {
		args[2+i] = hash
	}
	cmd := NewBoolSliceCmd(ctx, args...)
	if c.usesLegacyClusterHelpers(ctx, cmd) {
		result := make([]bool, len(hashes))
		for i := range result {
			result[i] = true
		}

		_ = c.withProcessHook(ctx, cmd, func(ctx context.Context, _ Cmder) error {
			var mu sync.Mutex
			err := c.ForEachShard(ctx, func(ctx context.Context, shard *Client) error {
				val, err := shard.ScriptExists(ctx, hashes...).Result()
				if err != nil {
					return err
				}

				mu.Lock()
				for i, v := range val {
					result[i] = result[i] && v
				}
				mu.Unlock()

				return nil
			})
			if err != nil {
				cmd.SetErr(err)
			} else {
				cmd.val = result
			}
			return nil
		})
		return cmd
	}
	_ = c.Process(ctx, cmd)
	cmd.setClusterRoutingDecision(nil) // A process hook may have short-circuited.
	return cmd
}
