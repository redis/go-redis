package redis

import (
	"log"
	"sync"
	"time"
)

type TokenManager struct {
	token       string
	expiresAt   time.Time
	mutex       sync.Mutex
	refreshFunc func() (string, time.Duration, error)
	stopChan    chan struct{}
}

// NewTokenManager initializes a new TokenManager.
func NewTokenManager(refreshFunc func() (string, time.Duration, error)) *TokenManager {
	return &TokenManager{
		refreshFunc: refreshFunc,
		stopChan:    make(chan struct{}),
	}
}

// SetToken updates the token and its expiration.
func (tm *TokenManager) SetToken(token string, ttl time.Duration) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.token = token
	tm.expiresAt = time.Now().Add(ttl)
	log.Printf("Token updated with TTL: %s", ttl)
}

// GetToken returns the current token if it's still valid.
func (tm *TokenManager) GetToken() (string, bool) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	if time.Now().After(tm.expiresAt) {
		return "", false
	}
	return tm.token, true
}

// RefreshToken fetches a new token using the provided refresh function.
func (tm *TokenManager) RefreshToken() error {
	if tm.refreshFunc == nil {
		return nil
	}
	token, ttl, err := tm.refreshFunc()
	if err != nil {
		return err
	}
	tm.SetToken(token, ttl)
	return nil
}

// StartAutoRefresh starts a goroutine to proactively refresh the token.
func (tm *TokenManager) StartAutoRefresh() {
	go func() {
		ticker := time.NewTicker(1 * time.Minute) // Check periodically
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				if tm.shouldRefresh() {
					log.Println("Proactively refreshing token...")
					if err := tm.RefreshToken(); err != nil {
						log.Printf("Failed to refresh token: %v", err)
					}
				}
			case <-tm.stopChan:
				log.Println("Stopping auto-refresh...")
				return
			}
		}
	}()
}

// StopAutoRefresh stops the auto-refresh goroutine.
func (tm *TokenManager) StopAutoRefresh() {
	close(tm.stopChan)
}

// shouldRefresh checks if the token is nearing expiration.
func (tm *TokenManager) shouldRefresh() bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	remaining := time.Until(tm.expiresAt)
	return remaining < 5*time.Minute // Refresh if less than 5 minutes remain
}

// MonitorTelemetry adds monitoring for token usage and expiration.
func (tm *TokenManager) MonitorTelemetry() {
	go func() {
		ticker := time.NewTicker(30 * time.Second) // Adjust as needed
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				token, valid := tm.GetToken()
				if !valid {
					log.Println("Token has expired.")
				} else {
					log.Printf("Token is valid: %s, expires in: %s", token, time.Until(tm.expiresAt))
				}
			case <-tm.stopChan:
				return
			}
		}
	}()
}
