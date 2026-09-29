package redis

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/logging"
)

type raceTestLogger struct{}

func (raceTestLogger) Printf(ctx context.Context, format string, v ...interface{}) {}

// internal.Logger and internal.LogLevel are process-wide logging config that
// SetLogger/SetLogLevel and logging.Enable/Disable swap while command and pool
// goroutines read them: internal.Logger through Printf (e.g. the sub-millisecond
// duration warning in formatSec) and internal.LogLevel through the *OrAbove
// guards (isHealthyConn on the Get path). Run with -race: the writers below race
// the reads unless both are accessed atomically.
func TestSetLoggerLogLevelConcurrentWithReads(t *testing.T) {
	origLogger := internal.Logger.Load()
	origLevel := internal.LogLevel.Load()
	defer func() {
		internal.Logger.Store(origLogger)
		internal.LogLevel.Store(origLevel)
	}()

	ctx := context.Background()
	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Writers: swap the logger and level through the public setters.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					SetLogger(raceTestLogger{})
					SetLogLevel(internal.LogLevelDebug)
					logging.Disable()
					SetLogLevel(internal.LogLevelError)
				}
			}
		}()
	}

	// Readers: the exact patterns the library uses from its goroutines.
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					internal.Logger.Printf(ctx, "probe %d", 1)
					_ = internal.LogLevel.DebugOrAbove()
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}
