package redis

import (
	"context"
	"errors"
	"io"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
)

type deadlineStreamingProvider struct {
	entered chan struct{}
	release chan struct{}
}

func (p *deadlineStreamingProvider) Subscribe(auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	close(p.entered)
	<-p.release
	return nil, nil, errors.New("acquisition released by test cleanup")
}

func (p *deadlineStreamingProvider) SubscribeWithContext(ctx context.Context, _ auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	close(p.entered)
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func TestStreamingCredentialWaitHonorsDeadline(t *testing.T) {
	p := &deadlineStreamingProvider{make(chan struct{}), make(chan struct{})}
	clientEnd, serverEnd := net.Pipe()
	t.Cleanup(func() { _ = serverEnd.Close() })
	c := NewClient(&Options{
		StreamingCredentialsProvider: p,
		ContextTimeoutEnabled:        true,
		MaxRetries:                   -1,
		DialerRetries:                -1,
		Dialer:                       func(context.Context, string, string) (net.Conn, error) { return clientEnd, nil },
	})
	t.Cleanup(func() { _ = c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	done := make(chan error, 1)
	finished := make(chan struct{})
	go func() {
		defer close(finished)
		done <- c.Ping(ctx).Err()
	}()
	t.Cleanup(func() { close(p.release); <-finished })
	select {
	case <-p.entered:
	case <-ctx.Done():
		t.Fatal("setup failed: provider was not entered")
	}
	<-ctx.Done()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Ping returned %v, want context.DeadlineExceeded", err)
		}
	case <-time.After(100 * time.Millisecond):
		t.Fatal("Ping still blocked in Subscribe after its deadline")
	}
	// Failed initialization must close its transport, not just mark it closed.
	_ = serverEnd.SetReadDeadline(time.Now().Add(time.Second))
	var b [1]byte
	if _, err := serverEnd.Read(b[:]); !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF after canceled initialization, got %v", err)
	}
}
