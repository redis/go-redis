package redis

import (
	"context"
	"testing"
)

// mockCmdable is a mock implementation of cmdable that records the last command.
// This is used for unit testing command construction without requiring a Redis server.
type mockCmdable struct {
	lastCmd   Cmder
	returnErr error
}

func (m *mockCmdable) call(_ context.Context, cmd Cmder) error {
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

// TestCmdFirstKeyPosWithInfo_UsesCommandInfoWhenWarm checks that once the
// cmdsInfoCache has been populated, cmdFirstKeyPosWithInfo returns
// CommandInfo.FirstKeyPos rather than the hardcoded fallback of 1.
// Uses an injected cache fn so no Redis server is required.
func TestCmdFirstKeyPosWithInfo_UsesCommandInfoWhenWarm(t *testing.T) {
	// "mymodule.cmd" is not in keylessCommands and has no firstKeyPos set,
	// so cold-cache behaviour falls back to the default of 1.
	const name = "mymodule.cmd"
	ctx := context.Background()
	cmd := NewCmd(ctx, name, "arg1")

	if got := cmdFirstKeyPosWithInfo(cmd, nil); got != 1 {
		t.Fatalf("cold cache: got %d, want 1", got)
	}

	// With a warm CommandInfo saying FirstKeyPos=0 (e.g. a module keyless command
	// not in our hand-maintained table), the function must return 0.
	info := &CommandInfo{Name: name, FirstKeyPos: 0}
	if got := cmdFirstKeyPosWithInfo(cmd, info); got != 0 {
		t.Fatalf("warm cache FirstKeyPos=0: got %d, want 0", got)
	}

	info2 := &CommandInfo{Name: name, FirstKeyPos: 2}
	if got := cmdFirstKeyPosWithInfo(cmd, info2); got != 2 {
		t.Fatalf("warm cache FirstKeyPos=2: got %d, want 2", got)
	}

	// End-to-end: Peek returns nil before Get, the real entry after.
	cache := newCmdsInfoCache(func(_ context.Context) (map[string]*CommandInfo, error) {
		return map[string]*CommandInfo{name: {Name: name, FirstKeyPos: 0}}, nil
	})

	if cache.Peek() != nil {
		t.Fatal("Peek() must return nil before Get()")
	}

	if _, err := cache.Get(ctx); err != nil {
		t.Fatalf("cache.Get: %v", err)
	}

	warmInfo := cache.Peek()[name]
	if warmInfo == nil {
		t.Fatalf("Peek()[%q] is nil after Get()", name)
	}
	if pos := cmdFirstKeyPosWithInfo(cmd, warmInfo); pos != 0 {
		t.Fatalf("post-Get: got %d, want 0", pos)
	}
}

// TestCmdFirstKeyPosWithInfo_PolicyTableKeyless checks that a module command
// registered keyless in the static policy table routes as keyless even on a
// cold command-info cache, and that key-carrying or specially-routed module
// commands are not affected.
func TestCmdFirstKeyPosWithInfo_PolicyTableKeyless(t *testing.T) {
	ctx := context.Background()

	// Keyless per the policy table: the index argument must not be treated
	// as a key while the command-info cache is cold.
	for _, name := range []string{"ft.aliaslist", "ft.aliasadd", "ft.search", "ft.spellcheck"} {
		cmd := NewCmd(ctx, name, "idx")
		if got := cmdFirstKeyPosWithInfo(cmd, nil); got != 0 {
			t.Errorf("%s cold cache: got %d, want 0 (keyless)", name, got)
		}
	}

	// Slot-from-key (RespDefaultHashSlot) and special request routing
	// (ReqSpecial) keep the hardcoded fallback.
	for _, name := range []string{"ft.suglen", "ft.sugdel", "ft.cursor"} {
		cmd := NewCmd(ctx, name, "key")
		if got := cmdFirstKeyPosWithInfo(cmd, nil); got != 1 {
			t.Errorf("%s cold cache: got %d, want 1", name, got)
		}
	}

	// Unregistered module commands are untouched.
	cmd := NewCmd(ctx, "mymodule.cmd", "arg1")
	if got := cmdFirstKeyPosWithInfo(cmd, nil); got != 1 {
		t.Errorf("unregistered module cmd: got %d, want 1", got)
	}
}
