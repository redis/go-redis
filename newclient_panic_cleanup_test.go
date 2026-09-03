package redis

import (
	"context"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

// fakeConn is a no-I/O net.Conn: the min-idle connection is created in the pool
// but never initialized (initConn is deferred to first checkout), so its
// Read/Write are never exercised. Close bumps a counter.
type fakeConn struct{ closed *int64 }

func (f *fakeConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (f *fakeConn) Write(b []byte) (int, error)      { return len(b), nil }
func (f *fakeConn) Close() error                     { atomic.AddInt64(f.closed, 1); return nil }
func (f *fakeConn) LocalAddr() net.Addr              { return fakeAddr{} }
func (f *fakeConn) RemoteAddr() net.Addr             { return fakeAddr{} }
func (f *fakeConn) SetDeadline(time.Time) error      { return nil }
func (f *fakeConn) SetReadDeadline(time.Time) error  { return nil }
func (f *fakeConn) SetWriteDeadline(time.Time) error { return nil }

type fakeAddr struct{}

func (fakeAddr) Network() string { return "fake" }
func (fakeAddr) String() string  { return "fake" }

// panicOnRegisterProcessor is a push.NotificationProcessor whose RegisterHandler
// panics — modelling a custom processor that rejects handler registration. With
// MaintNotificationsConfig in ModeEnabled, NewClient registers maintenance
// handlers (after the pools are created) and this panic propagates out.
//
// RegisterHandler first waits for dialed, which the test's dialer closes on its
// first call. The min-idle dial runs in a pool goroutine; without this wait the
// deferred Close in NewClient can win the race, the pool's dialConn sees the
// closed pool and never calls the dialer, and the test observes opened=0 — a
// legal outcome for the client, but not the leak scenario this test pins.
type panicOnRegisterProcessor struct{ dialed <-chan struct{} }

func (panicOnRegisterProcessor) GetHandler(string) push.NotificationHandler { return nil }
func (panicOnRegisterProcessor) ProcessPendingNotifications(context.Context, push.NotificationHandlerContext, *proto.Reader) error {
	return nil
}

func (p panicOnRegisterProcessor) RegisterHandler(string, push.NotificationHandler, bool) error {
	select {
	case <-p.dialed:
	case <-time.After(5 * time.Second):
		// Fall through and panic anyway; the caller's poll reports opened=0.
	}
	panic("push processor rejects handler registration")
}
func (panicOnRegisterProcessor) UnregisterHandler(string) error { return nil }

// TestNewClientPanicClosesPartialClient pins that when a NewClient construction
// step panics AFTER the connection pools are created, the partial client is
// closed on the way out so the pools (and their connections) do not leak. The
// panic still propagates to the caller; here it is recovered.
func TestNewClientPanicClosesPartialClient(t *testing.T) {
	var opened, closed int64
	var dialedOnce sync.Once
	dialed := make(chan struct{})
	dialer := func(context.Context, string, string) (net.Conn, error) {
		atomic.AddInt64(&opened, 1)
		dialedOnce.Do(func() { close(dialed) })
		return &fakeConn{closed: &closed}, nil
	}

	func() {
		defer func() {
			if recover() == nil {
				t.Fatal("NewClient did not panic; the trigger no longer fires")
			}
		}()
		NewClient(&Options{
			Addr:                      "localhost:6379", // never contacted (fake dialer)
			Protocol:                  3,
			PoolSize:                  1,
			MinIdleConns:              1,
			Dialer:                    dialer,
			MaintNotificationsConfig:  &maintnotifications.Config{Mode: maintnotifications.ModeEnabled},
			PushNotificationProcessor: panicOnRegisterProcessor{dialed: dialed},
		})
	}()

	// The dialer has run at least once by the time the panic fires (see
	// panicOnRegisterProcessor). The dialed connection is closed either by the
	// pool's Close (if it was registered in time) or by addIdleConn when it
	// finds the pool already closed, so poll until every dialed conn is closed.
	deadline := time.Now().Add(2 * time.Second)
	for {
		o := atomic.LoadInt64(&opened)
		if o >= 1 && atomic.LoadInt64(&closed) == o {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("partial client leaked its pool after NewClient panicked: opened=%d closed=%d", o, atomic.LoadInt64(&closed))
		}
		time.Sleep(time.Millisecond)
	}
}
