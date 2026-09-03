package redis

import (
	"context"
	"io"
	"net"
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
type panicOnRegisterProcessor struct{}

func (panicOnRegisterProcessor) GetHandler(string) push.NotificationHandler { return nil }
func (panicOnRegisterProcessor) ProcessPendingNotifications(context.Context, push.NotificationHandlerContext, *proto.Reader) error {
	return nil
}
func (panicOnRegisterProcessor) RegisterHandler(string, push.NotificationHandler, bool) error {
	panic("push processor rejects handler registration")
}
func (panicOnRegisterProcessor) UnregisterHandler(string) error { return nil }

// TestNewClientPanicClosesPartialClient pins that when a NewClient construction
// step panics AFTER the connection pools are created, the partial client is
// closed on the way out so the pools (and their connections) do not leak. The
// panic still propagates to the caller; here it is recovered.
func TestNewClientPanicClosesPartialClient(t *testing.T) {
	var opened, closed int64
	dialer := func(context.Context, string, string) (net.Conn, error) {
		atomic.AddInt64(&opened, 1)
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
			PushNotificationProcessor: panicOnRegisterProcessor{},
		})
	}()

	// The min-idle dial runs in a pool goroutine, so poll: the pool must have
	// dialed at least once and every dialed connection must end up closed.
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
