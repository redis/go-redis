package otel

import (
	"sync"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
)

type raceRecorder struct{ noopRecorder }

func (raceRecorder) RegisterPool(string, pool.Pooler)        {}
func (raceRecorder) UnregisterPool(pool.Pooler)              {}
func (raceRecorder) RegisterPubSubPool(string, PubSubPooler) {}
func (raceRecorder) UnregisterPubSubPool(PubSubPooler)       {}

// TestRegisterPoolsRaceWithSetGlobalRecorder pins that the pool registration
// helpers read the global recorder under its lock. Installing telemetry while
// another goroutine creates or closes clients is ordinary usage, and reading
// globalRecorder without recorderMu while SetGlobalRecorder writes it under the
// lock is a data race that -race reports (Copilot review on #3942).
func TestRegisterPoolsRaceWithSetGlobalRecorder(t *testing.T) {
	t.Cleanup(func() { SetGlobalRecorder(noopRecorder{}) })

	stop := make(chan struct{})
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			if i%2 == 0 {
				SetGlobalRecorder(raceRecorder{})
			} else {
				SetGlobalRecorder(noopRecorder{})
			}
		}
	}()

	var workers sync.WaitGroup
	for i := 0; i < 4; i++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			for j := 0; j < 2000; j++ {
				RegisterPools(nil, nil, nil, "127.0.0.1:6379")
				UnregisterPools(nil, nil, nil)
			}
		}()
	}
	workers.Wait()
	close(stop)
	<-writerDone
}
