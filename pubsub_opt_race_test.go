package redis

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

// handoffOnPushHandler marks the connection a push notification arrived
// on for handoff to endpoint, the way the MOVING handler does.
type handoffOnPushHandler struct{ endpoint string }

func (h handoffOnPushHandler) HandlePushNotification(_ context.Context, handlerCtx push.NotificationHandlerContext, _ []any) error {
	cn, ok := handlerCtx.Conn.(*pool.Conn)
	if !ok {
		return fmt.Errorf("push handler got conn of type %T", handlerCtx.Conn)
	}
	return cn.MarkForHandoff(h.endpoint, 1)
}

// pubSubHandoffSetup is a client whose shared pub/sub connection is
// handed off from server A to server B: A confirms the subscribe, then
// pushes a notification that marks the connection for handoff to B, and
// sends nothing more.
type pubSubHandoffSetup struct {
	client        *Client
	addrA         string
	subscribedOnB atomic.Bool // a subscribe reaching B proves the move
}

func newPubSubHandoffSetup(t *testing.T, opt *Options) *pubSubHandoffSetup {
	t.Helper()
	const pushName = "TEST_HANDOFF"
	subscribeReply := func(channel string) string {
		return fmt.Sprintf("*3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(channel), channel)
	}

	listen := func(replyFor func(command string) string) string {
		ln, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		t.Cleanup(func() { _ = ln.Close() })
		go func() {
			for {
				netConn, err := ln.Accept()
				if err != nil {
					return
				}
				go serveTestRESPConn(netConn, replyFor)
			}
		}()
		return ln.Addr().String()
	}

	s := &pubSubHandoffSetup{}
	addrB := listen(func(command string) string {
		switch command {
		case "hello":
			return "%0\r\n"
		case "subscribe":
			s.subscribedOnB.Store(true)
			return subscribeReply(pubSubHandoffChannel)
		}
		return "+OK\r\n"
	})
	s.addrA = listen(func(command string) string {
		switch command {
		case "hello":
			return "%0\r\n"
		case "subscribe":
			return subscribeReply(pubSubHandoffChannel) + fmt.Sprintf(">2\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
				len(pushName), pushName, len(addrB), addrB)
		}
		return "+OK\r\n"
	})

	opt.Addr = s.addrA
	opt.Protocol = 3
	opt.MaxRetries = -1
	opt.DisableIdentity = true
	opt.MaintNotificationsConfig = &maintnotifications.Config{Mode: maintnotifications.ModeDisabled}
	s.client = NewClient(opt)
	t.Cleanup(func() { _ = s.client.Close() })
	if err := s.client.RegisterPushNotificationHandler(pushName, handoffOnPushHandler{endpoint: addrB}, false); err != nil {
		t.Fatalf("register push handler: %v", err)
	}
	return s
}

const pubSubHandoffChannel = "handoff-ch"

// waitHandedOff reports whether the pub/sub connection reached B within
// timeout.
func (s *pubSubHandoffSetup) waitHandedOff(timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for !s.subscribedOnB.Load() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(5 * time.Millisecond)
	}
	return true
}

// The pub/sub engine used to alias the client's *Options (the old PubSub
// took opt: c.opt) and rewrote opt.Addr on a handoff, racing the pool
// dialer's lock-free read of the same field. The manager must keep its
// own copy: a handoff redirects only the pub/sub connection and never
// writes the client's Options.
//
// Run with -race: a write to the shared Options races the reader below.
// Without -race, the Addr check still catches a redirect that leaked into
// the client.
func TestPubSubHandoffDoesNotWriteClientOptions(t *testing.T) {
	s := newPubSubHandoffSetup(t, &Options{})

	// Reader: the pool dialer reads the client's opt.Addr lock-free.
	stop := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		for {
			select {
			case <-stop:
				return
			default:
				_ = s.client.opt.Addr
			}
		}
	})

	pubsub := s.client.Subscribe(context.Background(), pubSubHandoffChannel)
	t.Cleanup(func() { _ = pubsub.Close() })

	handedOff := s.waitHandedOff(5 * time.Second)
	close(stop)
	wg.Wait()
	if !handedOff {
		t.Fatal("pub/sub connection was not handed off to the second server")
	}
	if got := s.client.opt.Addr; got != s.addrA {
		t.Fatalf("client Options.Addr = %q after pub/sub handoff, want %q", got, s.addrA)
	}
}

// A push that marks the shared pub/sub connection for handoff is acted
// on at once, not when the next frame arrives: after the push the
// connection is idle, and with the health check disabled no frame ever
// would — the connection would stay on the old endpoint indefinitely.
func TestPubSubHandoffPushOnIdleConnRetiresAtOnce(t *testing.T) {
	s := newPubSubHandoffSetup(t, &Options{PubSubHealthCheckInterval: -1})

	pubsub := s.client.Subscribe(context.Background(), pubSubHandoffChannel)
	t.Cleanup(func() { _ = pubsub.Close() })

	if !s.waitHandedOff(2 * time.Second) {
		t.Fatal("handoff push on an idle pub/sub connection was not acted on")
	}
}
