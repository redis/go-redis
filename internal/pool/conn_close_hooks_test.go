package pool

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
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
	if cn.onCscClose != nil {
		t.Fatal("Close must clear the CSC close hook")
	}
}

func TestConn_ConcurrentCloseRunsCscHookOnce(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()

	cn := NewConn(client)
	var calls atomic.Int32
	cn.SetOnCscClose(func() error {
		calls.Add(1)
		return nil
	})

	const closers = 32
	var wg sync.WaitGroup
	wg.Add(closers)
	for range closers {
		go func() {
			defer wg.Done()
			_ = cn.Close()
		}()
	}
	wg.Wait()

	if got := calls.Load(); got != 1 {
		t.Fatalf("onCscClose calls: got %d want 1", got)
	}
	if cn.onCscClose != nil {
		t.Fatal("Close must clear the CSC close hook")
	}
}

func TestConn_CscPeriodicProbeRecoversFromClockRollback(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cn := NewConn(client)
	const interval = time.Minute
	if !cn.TakeCscPeriodicReadPending(interval) {
		t.Fatal("first periodic probe must be due")
	}
	if cn.TakeCscPeriodicReadPending(interval) {
		t.Fatal("periodic probe must be throttled within the interval")
	}

	now := time.Since(cn.createdAt).Nanoseconds()
	cn.lastCscPeriodicProbeNs.Store(now + int64(interval))
	if !cn.TakeCscPeriodicReadPending(interval) {
		t.Fatal("a timestamp ahead of the monotonic clock must reset the throttle")
	}
	if cn.TakeCscPeriodicReadPending(interval) {
		t.Fatal("the reset periodic probe must restart the throttle")
	}
}

func TestConn_HardReadDeadlineIsCleared(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	cn := NewConn(client)
	err := cn.WithReaderHardDeadline(time.Millisecond, func(rd *proto.Reader) error {
		_, err := rd.PeekReplyType()
		return err
	})
	if err == nil {
		t.Fatal("empty hard-deadline read must time out")
	}

	writeDone := make(chan error, 1)
	go func() {
		_, err := server.Write([]byte("+OK\r\n"))
		writeDone <- err
	}()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var reply interface{}
	if err := cn.WithReader(ctx, -1, func(rd *proto.Reader) error {
		var err error
		reply, err = rd.ReadReply()
		return err
	}); err != nil {
		t.Fatalf("read after hard deadline: %v", err)
	}
	if reply != "OK" {
		t.Fatalf("reply after hard deadline: got %#v, want OK", reply)
	}
	if err := <-writeDone; err != nil {
		t.Fatalf("write reply: %v", err)
	}
}

type failClearReadDeadlineConn struct {
	net.Conn
	err error
}

func (c *failClearReadDeadlineConn) SetReadDeadline(deadline time.Time) error {
	if deadline.IsZero() {
		return c.err
	}
	return c.Conn.SetReadDeadline(deadline)
}

func TestConn_HardReadDeadlineReturnsClearFailure(t *testing.T) {
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()

	clearErr := errors.New("clear read deadline")
	cn := NewConn(&failClearReadDeadlineConn{Conn: client, err: clearErr})
	err := cn.WithReaderHardDeadline(time.Millisecond, func(rd *proto.Reader) error {
		_, err := rd.PeekReplyType()
		return err
	})
	if !errors.Is(err, clearErr) {
		t.Fatalf("hard read deadline: got %v, want %v", err, clearErr)
	}
}

func TestWaitForDrainerPrefersCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := &ConnPool{}
	err := p.waitForDrainer(ctx, make(chan struct{}), time.Now().Add(-time.Second))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("waitForDrainer: got %v, want context.Canceled", err)
	}
}

func TestWaitForDrainerAcceptsCompletedDrainAfterDeadline(t *testing.T) {
	done := make(chan struct{})
	close(done)

	p := &ConnPool{}
	if err := p.waitForDrainer(context.Background(), done, time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("waitForDrainer: got %v, want nil", err)
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

func TestConn_CscReinitHookRunsBeforeSocketReplacement(t *testing.T) {
	oldServer, oldClient := net.Pipe()
	defer oldServer.Close()
	defer oldClient.Close()
	newServer, newClient := net.Pipe()
	defer newServer.Close()
	defer newClient.Close()

	cn := NewConn(oldClient)
	hookCalled := false
	cn.SetOnCscReinit(func() {
		hookCalled = true
		if cn.GetNetConn() != oldClient {
			t.Error("CSC reinit hook ran after the old socket was replaced")
		}
	})
	cn.SetInitConnFunc(func(context.Context, *Conn) error {
		if !hookCalled {
			t.Error("init ran before the CSC reinit hook")
		}
		if cn.GetNetConn() != newClient {
			t.Error("init did not run on the replacement socket")
		}
		cn.GetStateMachine().Transition(StateIdle)
		return nil
	})

	if err := cn.SetNetConnAndInitConn(context.Background(), newClient); err != nil {
		t.Fatalf("SetNetConnAndInitConn: %v", err)
	}
}
