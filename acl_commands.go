package redis

import "context"

type ACLCmdable interface {
	ACLDryRun(ctx context.Context, username string, command ...interface{}) *StringCmd
	ACLLog(ctx context.Context, count int64) *ACLLogCmd
	ACLLogReset(ctx context.Context) *StatusCmd
	ACLSetUser(ctx context.Context, username string, rules ...string) *StatusCmd
	ACLDelUser(ctx context.Context, username string) *IntCmd
	ACLList(ctx context.Context) *StringSliceCmd
}

func (c cmdable) ACLDryRun(ctx context.Context, username string, command ...interface{}) *StringCmd {
	args := make([]interface{}, 0, 3+len(command))
	args = append(args, "acl", "dryrun", username)
	args = append(args, command...)
	cmd := NewStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ACLLog(ctx context.Context, count int64) *ACLLogCmd {
	args := make([]interface{}, 0, 3)
	args = append(args, "acl", "log")
	if count > 0 {
		args = append(args, count)
	}
	cmd := NewACLLogCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ACLLogReset(ctx context.Context) *StatusCmd {
	cmd := NewStatusCmd(ctx, "acl", "log", "reset")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ACLDelUser(ctx context.Context, username string) *IntCmd {
	args := make([]interface{}, 3, 3)
	args[0] = "acl"
	args[1] = "deluser"
	args[2] = username
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ACLSetUser(ctx context.Context, username string, rules ...string) *StatusCmd {
	args := make([]interface{}, 3+len(rules), 3+len(rules))
	args[0] = "acl"
	args[1] = "setuser"
	args[2] = username
	for i, rule := range rules {
		args[i+3] = rule
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) ACLList(ctx context.Context) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "acl", "list")
	_ = c(ctx, cmd)
	return cmd
}
