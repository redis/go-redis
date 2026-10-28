package redis

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestSingleflightGroupCoalesces(t *testing.T) {
	var g singleflightGroup
	var calls atomic.Int32
	release := make(chan struct{})
	started := make(chan struct{})

	const n = 50
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err, _ := g.Do(context.Background(), "k", func(shared context.Context) (interface{}, error) {
				if calls.Add(1) == 1 {
					close(started)
				}
				<-release
				return "ok", nil
			})
			if err != nil {
				errs <- err
				return
			}
			if v != "ok" {
				errs <- errors.New("unexpected value")
			}
		}()
	}

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("shared call did not start")
	}
	// Allow concurrent callers to attach as waiters while the leader holds the key.
	time.Sleep(100 * time.Millisecond)
	close(release)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("shared calls = %d, want 1", got)
	}
}

func TestSingleflightGroupWaiterCancelDoesNotAbortShared(t *testing.T) {
	var g singleflightGroup
	var calls atomic.Int32
	var sharedFinished atomic.Bool
	started := make(chan struct{})
	release := make(chan struct{})

	leaderDone := make(chan struct{})
	go func() {
		defer close(leaderDone)
		_, err, _ := g.Do(context.Background(), "k", func(shared context.Context) (interface{}, error) {
			calls.Add(1)
			close(started)
			<-release
			sharedFinished.Store(true)
			return "ok", nil
		})
		if err != nil {
			t.Errorf("leader err: %v", err)
		}
	}()

	<-started
	waiterCtx, cancel := context.WithCancel(context.Background())
	waiterErr := make(chan error, 1)
	go func() {
		_, err, _ := g.Do(waiterCtx, "k", func(shared context.Context) (interface{}, error) {
			t.Error("waiter must not run shared fn")
			return nil, nil
		})
		waiterErr <- err
	}()
	time.Sleep(20 * time.Millisecond)
	cancel()
	select {
	case err := <-waiterErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("waiter did not return on cancel")
	}
	if sharedFinished.Load() {
		t.Fatal("shared finished before release")
	}
	close(release)
	<-leaderDone
	if !sharedFinished.Load() {
		t.Fatal("shared call was aborted")
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("shared calls = %d, want 1", got)
	}
}

func TestSentinelOptionsMinIdleConnsZero(t *testing.T) {
	opt := &FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"127.0.0.1:26379"},
		PoolSize:      100,
		MinIdleConns:  20,
	}
	sopt := opt.sentinelOptions("127.0.0.1:26379")
	if sopt.MinIdleConns != 0 {
		t.Fatalf("sentinel MinIdleConns = %d, want 0", sopt.MinIdleConns)
	}
	if sopt.PoolSize != 100 {
		t.Fatalf("sentinel PoolSize = %d, want inherited 100", sopt.PoolSize)
	}
}
