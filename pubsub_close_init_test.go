package redis

import (
	"context"
	"errors"
	"io"
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

type blockedPubSubCredentials struct {
	entered chan struct{}
	release chan struct{}
	active  atomic.Int32
}

func (p *blockedPubSubCredentials) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	p.active.Add(1)
	close(p.entered)
	<-p.release
	var once sync.Once
	return auth.NewBasicCredentials("", ""), func() error {
		once.Do(func() { p.active.Add(-1) })
		return nil
	}, nil
}

func TestClientCloseDuringPubSubInitialization(t *testing.T) {
	provider := &blockedPubSubCredentials{
		entered: make(chan struct{}),
		release: make(chan struct{}),
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(provider.release) }) }

	clientConn, serverConn := net.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	serverDone := make(chan error, 1)
	go func() {
		rd := proto.NewReader(serverConn)
		for {
			cmd, err := rd.ReadSlice()
			if err != nil {
				serverDone <- err
				return
			}
			// Only HELLO needs a reply: Subscribe writes without reading.
			if cmd[0] == "hello" {
				if _, err := io.WriteString(serverConn, "%0\r\n"); err != nil {
					serverDone <- err
					return
				}
			}
		}
	}()

	client := NewClient(&Options{
		Dialer:                       func(context.Context, string, string) (net.Conn, error) { return clientConn, nil },
		Protocol:                     3,
		DisableIdentity:              true,
		StreamingCredentialsProvider: provider,
		MaintNotificationsConfig:     &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	defer client.Close()
	ps := client.pubSub()
	defer ps.Close()
	// Unblock initialization before cleanup tries to acquire the PubSub mutex,
	// including when a timeout causes this test to exit early.
	defer release()
	subscribeDone := make(chan error, 1)
	go func() { subscribeDone <- ps.Subscribe(context.Background(), "test") }()

	select {
	case <-provider.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("PubSub initialization did not reach credential acquisition")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	release()
	select {
	case err := <-subscribeDone:
		if !errors.Is(err, pool.ErrClosed) {
			t.Errorf("Subscribe error = %v, want pool.ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("PubSub initialization did not settle")
	}
	if got := provider.active.Load(); got != 0 {
		t.Errorf("active credential subscriptions = %d, want 0", got)
	}
	if got := client.pubSubPool.Stats().Active; got != 0 {
		t.Errorf("active PubSub connections = %d, want 0", got)
	}
	select {
	case err := <-serverDone:
		if !errors.Is(err, io.EOF) {
			t.Errorf("server read error = %v, want EOF", err)
		}
	case <-time.After(time.Second):
		t.Error("PubSub transport remains open after Close and initialization settled")
	}
}
