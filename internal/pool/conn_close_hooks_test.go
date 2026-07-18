package pool

import (
	"net"
	"testing"
)

// TestConn_CloseRunsBothCloseHooks verifies the CSC close hook is a separate
// slot from onClose: installing it must not clobber a previously registered
// onClose (e.g. the streaming-credentials unsubscribe), and Close must run
// both.
func TestConn_CloseRunsBothCloseHooks(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cn := NewConn(client)

	var onCloseCalls, onCscCloseCalls int
	cn.SetOnClose(func() error {
		onCloseCalls++
		return nil
	})
	cn.SetOnCscClose(func() error {
		onCscCloseCalls++
		return nil
	})

	if err := cn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if onCloseCalls != 1 {
		t.Fatalf("onClose calls: got %d want 1 (CSC hook must not clobber it)", onCloseCalls)
	}
	if onCscCloseCalls != 1 {
		t.Fatalf("onCscClose calls: got %d want 1", onCscCloseCalls)
	}
}

// TestConn_SetOnCscCloseOverwrites pins overwrite semantics for the CSC slot:
// re-running initConn on the same conn must replace, not stack, the hook.
func TestConn_SetOnCscCloseOverwrites(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cn := NewConn(client)

	var first, second int
	cn.SetOnCscClose(func() error {
		first++
		return nil
	})
	cn.SetOnCscClose(func() error {
		second++
		return nil
	})

	if err := cn.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if first != 0 || second != 1 {
		t.Fatalf("overwrite semantics violated: first=%d (want 0) second=%d (want 1)", first, second)
	}
}
