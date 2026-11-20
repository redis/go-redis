package redis

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/auth"
)

type closeOnBasicAuth struct {
	auth.Credentials
	close func()
}

func (c closeOnBasicAuth) BasicAuth() (string, string) {
	c.close()
	return c.Credentials.BasicAuth()
}

func TestStreamingCredentialsCloseBoundaries(t *testing.T) {
	for _, pubsub := range []bool{false, true} {
		for _, boundary := range []string{"after-registration", "during-hello", "after-hello-read", "after-init", "client-close"} {
			t.Run(fmt.Sprintf("pubsub=%t/%s", pubsub, boundary), func(t *testing.T) {
				client, cn, provider := newCloseRaceConnection(t, pubsub)
				close(provider.release)
				switch boundary {
				case "after-registration":
					provider.credentials = closeOnBasicAuth{provider.credentials, func() { _ = cn.Close() }}
				case "during-hello":
					provider.onHello = func() { _ = cn.Close() }
				case "after-hello-read":
					provider.onRead = sync.OnceFunc(func() { _ = cn.Close() })
				}
				err := client.initConn(context.Background(), cn)
				if boundary == "after-init" || boundary == "client-close" {
					if err != nil {
						t.Fatal(err)
					}
					if !provider.helloAuth.Load() {
						t.Fatal("HELLO did not include the provider's credentials")
					}
					if provider.onConnect.Load() != 1 {
						t.Fatal("OnConnect did not run after successful authentication")
					}
					if provider.active.Load() != 1 || provider.unsubscribes.Load() != 0 {
						t.Fatal("live subscription was cleaned up before close")
					}
					if !client.streamingCredentialsManager.IsTracking(cn.GetID()) {
						t.Fatal("live connection has no streaming credentials listener")
					}
					provider.listener.OnNext(provider.credentials)
					if boundary == "client-close" {
						if pubsub {
							if err := client.pubSubPool.TrackConn(cn); err != nil {
								t.Fatal(err)
							}
						}
						_ = client.Close()
					} else {
						_ = cn.Close()
					}
				} else if err == nil {
					t.Fatal("initialization succeeded after close")
				} else if provider.onConnect.Load() != 0 {
					t.Fatal("OnConnect ran after connection closure during setup")
				}
				provider.listener.OnNext(provider.credentials)
				checkCloseRaceCleanup(t, client, cn, provider)
			})
		}
	}
}

func TestStreamingCredentialsConcurrentSubscribeClose(t *testing.T) {
	const childEnv = "GO_REDIS_STREAMING_CLOSE_CHILD"
	if os.Getenv(childEnv) != "1" {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		cmd := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestStreamingCredentialsConcurrentSubscribeClose$", "-test.timeout=50s")
		cmd.Env = append(os.Environ(), childEnv+"=1")
		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("concurrent close subprocess: %v\n%s", err, output)
		}
		return
	}
	for i := 0; i < 100; i++ {
		for _, pubsub := range []bool{false, true} {
			t.Run(fmt.Sprintf("iteration=%d/pubsub=%t", i, pubsub), func(t *testing.T) {
				client, cn, provider := newCloseRaceConnection(t, pubsub)
				release := sync.OnceFunc(func() { close(provider.release) })
				t.Cleanup(release)
				done := make(chan error, 1)
				go func() { done <- client.initConn(context.Background(), cn) }()
				select {
				case <-provider.entered:
				case <-time.After(5 * time.Second):
					t.Fatal("subscription did not start")
				}
				provider.listener.OnNext(provider.credentials)
				start := make(chan struct{})
				closed := make(chan struct{})
				go func() {
					<-start
					_ = cn.Close()
					close(closed)
				}()
				go func() { <-start; release() }()
				close(start)
				select {
				case <-closed:
				case <-time.After(5 * time.Second):
					t.Fatal("close did not finish")
				}
				select {
				case <-done:
					// Close may land after successful init or during handshake I/O.
				case <-time.After(5 * time.Second):
					t.Fatal("initialization did not finish")
				}
				provider.listener.OnNext(provider.credentials)
				checkCloseRaceCleanup(t, client, cn, provider)
			})
		}
	}
}
