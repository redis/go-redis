package redis_test

import (
	"context"
	"strings"
	"sync"

	"github.com/redis/go-redis/v9"
)

// commandRecorder records the last N commands executed by a Redis client.
type commandRecorder struct {
	mu       sync.Mutex
	commands []string
	maxSize  int
}

// newCommandRecorder creates a new command recorder with the specified maximum size.
func newCommandRecorder(maxSize int) *commandRecorder {
	return &commandRecorder{
		commands: make([]string, 0, maxSize),
		maxSize:  maxSize,
	}
}

// Record adds a command to the recorder.
func (r *commandRecorder) Record(cmd string) {
	cmd = strings.ToLower(cmd)
	r.mu.Lock()
	defer r.mu.Unlock()

	r.commands = append(r.commands, cmd)
	if len(r.commands) > r.maxSize {
		r.commands = r.commands[1:]
	}
}

// LastCommands returns a copy of the recorded commands.
func (r *commandRecorder) LastCommands() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.commands...)
}

// Contains checks if the recorder contains a specific command.
func (r *commandRecorder) Contains(cmd string) bool {
	cmd = strings.ToLower(cmd)
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, c := range r.commands {
		if strings.Contains(c, cmd) {
			return true
		}
	}
	return false
}

// Hook returns a Redis hook that records commands.
func (r *commandRecorder) Hook() redis.Hook {
	return &commandHook{recorder: r}
}

// commandHook implements the redis.Hook interface to record commands.
type commandHook struct {
	recorder *commandRecorder
}

func (h *commandHook) DialHook(next redis.DialHook) redis.DialHook {
	return next
}

func (h *commandHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		h.recorder.Record(cmd.String())
		return next(ctx, cmd)
	}
}

func (h *commandHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		for _, cmd := range cmds {
			h.recorder.Record(cmd.String())
		}
		return next(ctx, cmds)
	}
}
