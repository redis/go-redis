package redis

import (
	"context"
)

// mockCmdable is a mock implementation of cmdable that records the last command.
// This is used for unit testing command construction without requiring a Redis server.
type mockCmdable struct {
	lastCmd   Cmder
	returnErr error
}

func (m *mockCmdable) call(ctx context.Context, cmd Cmder) error {
	m.lastCmd = cmd
	if m.returnErr != nil {
		cmd.SetErr(m.returnErr)
	}
	return m.returnErr
}

func (m *mockCmdable) asCmdable() cmdable {
	return func(ctx context.Context, cmd Cmder) error {
		return m.call(ctx, cmd)
	}
}
