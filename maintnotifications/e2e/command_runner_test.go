package e2e

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

type CommandRunnerStats struct {
	Operations    int64
	Errors        int64
	TimeoutErrors int64
	ErrorsList    []error
}

// CommandRunner provides utilities for running commands during tests
type CommandRunner struct {
	executing      atomic.Bool
	client         redis.UniversalClient
	stopCh         chan struct{}
	operationCount atomic.Int64
	errorCount     atomic.Int64
	timeoutErrors  atomic.Int64
	errors         []error
	errorsMutex    sync.Mutex
}

// NewCommandRunner creates a new command runner
func NewCommandRunner(client redis.UniversalClient) (*CommandRunner, func()) {
	stopCh := make(chan struct{})
	return &CommandRunner{
			client: client,
			stopCh: stopCh,
			errors: make([]error, 0),
		}, func() {
			stopCh <- struct{}{}
		}
}

func (cr *CommandRunner) Stop() {
	select {
	case cr.stopCh <- struct{}{}:
		return
	case <-time.After(500 * time.Millisecond):
		return
	}
}

func (cr *CommandRunner) Close() {
	close(cr.stopCh)
}

// FireCommandsUntilStop runs commands continuously until stop signal
func (cr *CommandRunner) FireCommandsUntilStop(ctx context.Context) {
	if !cr.executing.CompareAndSwap(false, true) {
		return
	}
	defer cr.executing.Store(false)
	fmt.Printf("[CR] Starting command runner...\n")
	defer fmt.Printf("[CR] Command runner stopped\n")
	// High frequency for timeout testing
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	counter := 0
	for {
		select {
		case <-cr.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			poolSize := cr.client.PoolStats().IdleConns
			if poolSize == 0 {
				poolSize = 1
			}
			wg := sync.WaitGroup{}
			for i := 0; i < int(poolSize); i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					key := fmt.Sprintf("timeout-test-key-%d-%d", counter, i)
					value := fmt.Sprintf("timeout-test-value-%d-%d", counter, i)

					// Use a short timeout context for individual operations
					opCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
					err := cr.client.Set(opCtx, key, value, time.Minute).Err()
					cancel()

					cr.operationCount.Add(1)
					if err != nil {
						if err == redis.ErrClosed || strings.Contains(err.Error(), "client is closed") {
							select {
							case <-cr.stopCh:
								return
							default:
							}
							return
						}

						fmt.Printf("Error: %v\n", err)
						cr.errorCount.Add(1)

						// Check if it's a timeout error
						if isTimeoutError(err) {
							cr.timeoutErrors.Add(1)
						}

						cr.errorsMutex.Lock()
						cr.errors = append(cr.errors, err)
						cr.errorsMutex.Unlock()
					}
				}(i)
			}
			wg.Wait()
			counter++
		}
	}
}

// GetStats returns operation statistics
func (cr *CommandRunner) GetStats() CommandRunnerStats {
	cr.errorsMutex.Lock()
	defer cr.errorsMutex.Unlock()

	errorList := make([]error, len(cr.errors))
	copy(errorList, cr.errors)

	stats := CommandRunnerStats{
		Operations:    cr.operationCount.Load(),
		Errors:        cr.errorCount.Load(),
		TimeoutErrors: cr.timeoutErrors.Load(),
		ErrorsList:    errorList,
	}

	return stats
}
