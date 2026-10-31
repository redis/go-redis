package redis

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
)

type contextStreamingTestProvider struct {
	called       chan context.Context
	wait         bool
	registered   *atomic.Bool
	unsubscribed chan struct{}
}

func (p *contextStreamingTestProvider) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	panic("legacy Subscribe called")
}

func (p *contextStreamingTestProvider) SubscribeWithContext(ctx context.Context, listener auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	p.called <- ctx
	if p.wait {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	if p.registered != nil {
		p.registered.Store(true)
	}
	return auth.NewBasicCredentials("user", "pass"), func() error {
		if p.unsubscribed != nil {
			close(p.unsubscribed)
		}
		return nil
	}, nil
}

func TestSubscribeStreamingCredentialsPrefersContextMethod(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider := &contextStreamingTestProvider{
		called:       make(chan context.Context, 1),
		unsubscribed: make(chan struct{}),
	}
	listener := &testCredentialsListener{}

	creds, unsubscribe, err := subscribeStreamingCredentials(ctx, provider, listener)
	if err != nil {
		t.Fatal(err)
	}
	if got := <-provider.called; got != ctx {
		t.Fatalf("SubscribeWithContext got context %p, want caller context %p", got, ctx)
	}
	if creds == nil || unsubscribe == nil {
		t.Fatalf("subscription = (%v, %v); want credentials and unsubscribe", creds, unsubscribe)
	}
	cancel()
	select {
	case <-provider.unsubscribed:
		t.Fatal("canceling the caller context ended the established subscription")
	default:
	}
}

type legacyStreamingTestProvider struct {
	called  chan struct{}
	release chan struct{}
}

func (p *legacyStreamingTestProvider) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	p.called <- struct{}{}
	<-p.release
	return auth.NewBasicCredentials("legacy", "pass"), func() error { return nil }, nil
}

func TestSubscribeStreamingCredentialsKeepsLegacySubscribeSynchronous(t *testing.T) {
	provider := &legacyStreamingTestProvider{called: make(chan struct{}, 1), release: make(chan struct{})}
	result := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		_, _, err := subscribeStreamingCredentials(ctx, provider, &testCredentialsListener{})
		result <- err
	}()
	<-provider.called
	cancel()
	select {
	case err := <-result:
		t.Fatalf("legacy Subscribe returned before it was released: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(provider.release)
	if err := <-result; err != nil {
		t.Fatal(err)
	}
}

func TestSubscribeStreamingCredentialsReturnsCooperativeCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	provider := &contextStreamingTestProvider{called: make(chan context.Context, 1), wait: true}
	listener := &testCredentialsListener{}
	registered := &atomic.Bool{}
	provider.registered = registered
	errCh := make(chan error, 1)
	go func() {
		_, _, err := subscribeStreamingCredentials(ctx, provider, listener)
		errCh <- err
	}()
	<-provider.called
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("cooperative context-aware Subscribe did not return promptly")
	}
	if registered.Load() {
		t.Fatal("listener registered after canceled subscription")
	}
}

func TestStreamingSubscriptionCancellationClosesInitializingConn(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	provider := &contextStreamingTestProvider{called: make(chan context.Context, 1), wait: true}
	var peer net.Conn
	client := NewClient(&Options{
		Addr: "test",
		Dialer: func(context.Context, string, string) (net.Conn, error) {
			clientConn, serverConn := net.Pipe()
			peer = serverConn
			return clientConn, nil
		},
		StreamingCredentialsProvider: provider,
	})
	defer client.Close()

	errCh := make(chan error, 1)
	go func() { errCh <- client.Ping(ctx).Err() }()
	<-provider.called
	closeErrCh := make(chan error, 1)
	go func() { closeErrCh <- client.Close() }()
	select {
	case err := <-closeErrCh:
		if err != nil {
			t.Fatalf("Close returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("client Close blocked while provider subscription was waiting")
	}
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Ping returned %v, want context.Canceled", err)
		}
	case <-time.After(time.Second):
		t.Fatal("connection initialization did not return after cooperative cancellation")
	}
	if peer == nil {
		t.Fatal("dialer did not create the server-side socket")
	}
	_ = peer.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err := peer.Read(buf); err == nil {
		t.Fatal("server-side socket remained open after failed initialization")
	}
	_ = peer.Close()
}

type lateSuccessStreamingProvider struct {
	called      chan struct{}
	returnNow   chan struct{}
	unsubscribe atomic.Int32
}

func (p *lateSuccessStreamingProvider) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	panic("legacy Subscribe called")
}

func (p *lateSuccessStreamingProvider) SubscribeWithContext(context.Context, auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	close(p.called)
	<-p.returnNow
	return auth.NewBasicCredentials("user", "pass"), func() error {
		p.unsubscribe.Add(1)
		return nil
	}, nil
}

func TestStreamingSubscriptionSuccessAfterConnCloseUnsubscribesOnce(t *testing.T) {
	for _, pubsub := range []bool{false, true} {
		for _, closeBeforeReturn := range []bool{true, false} {
			name := fmt.Sprintf("pubsub=%t/closeBeforeReturn=%t", pubsub, closeBeforeReturn)
			t.Run(name, func(t *testing.T) {
				provider := &lateSuccessStreamingProvider{called: make(chan struct{}), returnNow: make(chan struct{})}
				clientConn, peer := net.Pipe()
				t.Cleanup(func() { _ = peer.Close() })
				client := NewClient(&Options{
					Addr:                         "test",
					StreamingCredentialsProvider: provider,
					Dialer:                       func(context.Context, string, string) (net.Conn, error) { return clientConn, nil },
				})
				t.Cleanup(func() { _ = client.Close() })
				cn := pool.NewConn(clientConn)
				if pubsub {
					var err error
					cn, err = client.pubSubPool.NewConn(context.Background(), "tcp", "test", nil)
					if err != nil {
						t.Fatal(err)
					}
				}
				t.Cleanup(func() { _ = cn.Close() })
				release := sync.OnceFunc(func() { close(provider.returnNow) })
				initDone := make(chan error, 1)
				go func() { initDone <- client.initConn(context.Background(), cn) }()
				t.Cleanup(release)
				select {
				case <-provider.called:
				case <-time.After(time.Second):
					t.Fatal("subscription setup did not start")
				}
				if !client.streamingCredentialsManager.IsTracking(cn.GetID()) {
					t.Fatal("listener not tracked during setup")
				}
				closeDone := make(chan error, 1)
				if closeBeforeReturn {
					closeDone <- cn.Close()
					release()
				} else {
					startRace := make(chan struct{})
					go func() { <-startRace; closeDone <- cn.Close() }()
					go func() { <-startRace; release() }()
					close(startRace)
				}
				select {
				case err := <-closeDone:
					if err != nil {
						t.Fatal(err)
					}
				case <-time.After(time.Second):
					t.Fatal("connection Close did not finish")
				}
				select {
				case err := <-initDone:
					if err == nil {
						t.Fatal("initialization succeeded after connection close")
					}
				case <-time.After(time.Second):
					t.Fatal("connection initialization did not finish")
				}
				_ = cn.Close()
				if got := provider.unsubscribe.Load(); got != 1 {
					t.Fatalf("unsubscribe called %d times, want exactly once", got)
				}
				if pubsub && client.streamingCredentialsManager.IsTracking(cn.GetID()) {
					t.Fatal("closed Pub/Sub connection still has re-auth state")
				}
			})
		}
	}
}

// Exercise successful subscription setup through a real client and a local
// RESP peer; the peer is deliberately not a Redis or Azure service.
func TestStreamingSubscriptionSurvivesRequestCancellation(t *testing.T) {
	provider := &contextStreamingTestProvider{
		called:       make(chan context.Context, 2),
		unsubscribed: make(chan struct{}),
	}
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { _ = serverEnd.Close() })
	peerDone := make(chan struct{})
	go func() {
		defer close(peerDone)
		rd := proto.NewReader(serverEnd)
		for {
			args, err := rd.ReadSlice()
			if err != nil {
				return
			}
			reply := "+OK\r\n"
			switch args[0] {
			case "hello":
				reply = "%0\r\n"
			case "ping":
				reply = "+PONG\r\n"
			}
			if _, err := serverEnd.Write([]byte(reply)); err != nil {
				return
			}
		}
	}()
	client := NewClient(&Options{
		StreamingCredentialsProvider: provider,
		ContextTimeoutEnabled:        true,
		DisableIdentity:              true,
		MaxRetries:                   -1,
		MaintNotificationsConfig:     &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
		Dialer:                       func(context.Context, string, string) (net.Conn, error) { return clientEnd, nil },
	})
	t.Cleanup(func() { _ = client.Close(); <-peerDone })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatal(err)
	}
	if got := <-provider.called; got != ctx {
		t.Fatal("provider did not receive caller context")
	}
	cancel()
	select {
	case <-provider.unsubscribed:
		t.Fatal("canceling request removed established subscription")
	default:
	}
	if err := client.Ping(context.Background()).Err(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.called:
		t.Fatal("client resubscribed instead of reusing authenticated connection")
	default:
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-provider.unsubscribed:
	default:
		t.Fatal("closing client did not unsubscribe")
	}
}

type testCredentialsListener struct{}

func (*testCredentialsListener) OnNext(auth.Credentials) {}
func (*testCredentialsListener) OnError(error)           {}
