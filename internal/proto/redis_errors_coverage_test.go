package proto

import (
	"fmt"
	"testing"
)

func TestParseTypedRedisError(t *testing.T) {
	tests := []struct {
		msg      string
		wantType interface{}
	}{
		{"LOADING Redis is loading", &LoadingError{}},
		{"READONLY can't write", &ReadOnlyError{}},
		{"MOVED 3999 127.0.0.1:6381", &MovedError{}},
		{"ASK 3999 127.0.0.1:6381", &AskError{}},
		{"CLUSTERDOWN cluster is down", &ClusterDownError{}},
		{"TRYAGAIN try again", &TryAgainError{}},
		{"MASTERDOWN master down", &MasterDownError{}},
		{"NOREPLICAS not enough replicas", &NoReplicasError{}},
		{"ERR max number of clients reached", &MaxClientsError{}},
		{"NOAUTH authentication required", &AuthError{}},
		{"WRONGPASS invalid password", &AuthError{}},
		{"NOPERM no permission", &PermissionError{}},
		{"EXECABORT transaction aborted", &ExecAbortError{}},
		{"OOM out of memory", &OOMError{}},
		{"ERR something generic", RedisError("")},
	}
	for _, tt := range tests {
		got := parseTypedRedisError(tt.msg)
		if got == nil {
			t.Errorf("parseTypedRedisError(%q) = nil", tt.msg)
			continue
		}
		if got.Error() != tt.msg {
			t.Errorf("parseTypedRedisError(%q).Error() = %q", tt.msg, got.Error())
		}
		if fmt.Sprintf("%T", got) != fmt.Sprintf("%T", tt.wantType) {
			t.Errorf("parseTypedRedisError(%q) type = %T, want %T", tt.msg, got, tt.wantType)
		}
	}
}

func TestMovedAndAskAddr(t *testing.T) {
	m := NewMovedError("MOVED 1 host:1", "host:1")
	if m.Addr() != "host:1" {
		t.Errorf("MovedError.Addr() = %q", m.Addr())
	}
	a := NewAskError("ASK 1 host:2", "host:2")
	if a.Addr() != "host:2" {
		t.Errorf("AskError.Addr() = %q", a.Addr())
	}
	// extractAddr without spaces returns "".
	if got := extractAddr("MOVED"); got != "" {
		t.Errorf("extractAddr no-space = %q", got)
	}
}

func TestIsErrorPredicates(t *testing.T) {
	type predicate func(error) bool
	cases := []struct {
		name    string
		fn      predicate
		match   error
		wrapped error
		nonE    error
	}{
		{"loading", IsLoadingError, NewLoadingError("LOADING x"), RedisError("LOADING x"), RedisError("ERR x")},
		{"readonly", IsReadOnlyError, NewReadOnlyError("READONLY x"), RedisError("READONLY x"), RedisError("ERR x")},
		{"clusterdown", IsClusterDownError, NewClusterDownError("CLUSTERDOWN x"), RedisError("CLUSTERDOWN x"), RedisError("ERR x")},
		{"tryagain", IsTryAgainError, NewTryAgainError("TRYAGAIN x"), RedisError("TRYAGAIN x"), RedisError("ERR x")},
		{"masterdown", IsMasterDownError, NewMasterDownError("MASTERDOWN x"), RedisError("MASTERDOWN x"), RedisError("ERR x")},
		{"maxclients", IsMaxClientsError, NewMaxClientsError("ERR max number of clients reached"), RedisError("ERR max number of clients reached"), RedisError("ERR x")},
		{"auth", IsAuthError, NewAuthError("NOAUTH x"), RedisError("WRONGPASS x"), RedisError("ERR x")},
		{"perm", IsPermissionError, NewPermissionError("NOPERM x"), RedisError("NOPERM x"), RedisError("ERR x")},
		{"execabort", IsExecAbortError, NewExecAbortError("EXECABORT x"), RedisError("EXECABORT x"), RedisError("ERR x")},
		{"oom", IsOOMError, NewOOMError("OOM x"), RedisError("OOM x"), RedisError("ERR x")},
		{"noreplicas", IsNoReplicasError, NewNoReplicasError("NOREPLICAS x"), RedisError("NOREPLICAS x"), RedisError("ERR x")},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.fn(nil) {
				t.Error("predicate(nil) should be false")
			}
			if !c.fn(c.match) {
				t.Error("predicate(typed) should be true")
			}
			if !c.fn(c.wrapped) {
				t.Error("predicate(wrapped RedisError) should be true")
			}
			if !c.fn(fmt.Errorf("wrap: %w", c.match)) {
				t.Error("predicate(errors.As wrapped) should be true")
			}
			if c.fn(c.nonE) {
				t.Error("predicate(non-matching) should be false")
			}
		})
	}
}

func TestIsReadOnlyErrorLuaScript(t *testing.T) {
	luaErr := RedisError("ERR Error running script: -READONLY You can't write against a read only replica")
	if !IsReadOnlyError(luaErr) {
		t.Error("expected Lua-wrapped READONLY to be detected")
	}
	if !IsReadOnlyError(fmt.Errorf("%s", luaErr.Error())) {
		t.Error("expected fallback string READONLY detection")
	}
}

func TestIsMovedAndAskError(t *testing.T) {
	if _, ok := IsMovedError(nil); ok {
		t.Error("IsMovedError(nil) should be false")
	}
	m, ok := IsMovedError(NewMovedError("MOVED 1 h:1", "h:1"))
	if !ok || m.Addr() != "h:1" {
		t.Errorf("IsMovedError(typed) = %v, %v", m, ok)
	}
	m, ok = IsMovedError(RedisError("MOVED 3999 127.0.0.1:6381"))
	if !ok || m.Addr() != "127.0.0.1:6381" {
		t.Errorf("IsMovedError(string) = %v, %v", m, ok)
	}
	if _, ok := IsMovedError(RedisError("ERR x")); ok {
		t.Error("IsMovedError(non-moved) should be false")
	}

	if _, ok := IsAskError(nil); ok {
		t.Error("IsAskError(nil) should be false")
	}
	a, ok := IsAskError(NewAskError("ASK 1 h:2", "h:2"))
	if !ok || a.Addr() != "h:2" {
		t.Errorf("IsAskError(typed) = %v, %v", a, ok)
	}
	a, ok = IsAskError(RedisError("ASK 3999 127.0.0.1:6381"))
	if !ok || a.Addr() != "127.0.0.1:6381" {
		t.Errorf("IsAskError(string) = %v, %v", a, ok)
	}
	if _, ok := IsAskError(RedisError("ERR x")); ok {
		t.Error("IsAskError(non-ask) should be false")
	}
}
