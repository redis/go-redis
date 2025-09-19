package e2e

import (
	"context"
	"sync"

	"github.com/redis/go-redis/v9"
)

func pingOnAllIdleConns(client redis.UniversalClient) error {
	idleConnsNum := client.PoolStats().IdleConns
	wg := sync.WaitGroup{}
	err := error(nil)
	errCh := make(chan error, idleConnsNum)
	if idleConnsNum == 0 {
		idleConnsNum = 1
	}
	for i := uint32(0); i < idleConnsNum; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errCh <- client.Ping(context.Background()).Err()
		}()
	}
	wg.Wait()
	close(errCh)
	for err = range errCh {
		if err != nil {
			return err
		}
	}
	return nil
}

func isTimeout(errMsg string) bool {
	return contains(errMsg, "i/o timeout") ||
		contains(errMsg, "deadline exceeded") ||
		contains(errMsg, "context deadline exceeded")
}

// isTimeoutError checks if an error is a timeout error
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	// Check for various timeout error types
	errStr := err.Error()
	return isTimeout(errStr)
}

// contains checks if a string contains a substring (case-insensitive)
func contains(s, substr string) bool {
	return len(s) >= len(substr) &&
		(s == substr ||
			(len(s) > len(substr) &&
				(s[:len(substr)] == substr ||
					s[len(s)-len(substr):] == substr ||
					containsSubstring(s, substr))))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
