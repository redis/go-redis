package entra_id

import (
	"log"
	"sync"
	"time"
)

type TokenManager struct {
	token            string
	expiresAt        time.Time
	mutex            sync.Mutex
	refreshFunc      func() (string, time.Duration, error)
	stopChan         chan struct{}
	refreshTicker    *time.Ticker
	refreshInterval  time.Duration
	telemetryEnabled bool
}

// NewTokenManager initializes a new TokenManager.
func NewTokenManager(refreshFunc func() (string, time.Duration, error), refreshInterval time.Duration, telemetryEnabled bool) *TokenManager {
	return &TokenManager{
		refreshFunc:      refreshFunc,
		stopChan:         make(chan struct{}),
		refreshInterval:  refreshInterval,
		telemetryEnabled: telemetryEnabled,
	}
}

// SetToken updates the token and its expiration.
func (tm *TokenManager) SetToken(token string, ttl time.Duration) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.token = token
	tm.expiresAt = time.Now().Add(ttl)
	log.Printf("[TokenManager] Token updated with TTL: %s", ttl)
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
		log.Printf("[TokenManager] Failed to refresh token: %v", err)
		return err
	}
	tm.SetToken(token, ttl)
	log.Println("[TokenManager] Token refreshed successfully.")
	return nil
}

// StartAutoRefresh starts a goroutine to proactively refresh the token.
func (tm *TokenManager) StartAutoRefresh() {
	tm.refreshTicker = time.NewTicker(tm.refreshInterval)
	go func() {
		for {
			select {
			case <-tm.refreshTicker.C:
				if tm.shouldRefresh() {
					log.Println("[TokenManager] Proactively refreshing token...")
					if err := tm.RefreshToken(); err != nil {
						log.Printf("[TokenManager] Error during token refresh: %v", err)
					}
				}
			case <-tm.stopChan:
				log.Println("[TokenManager] Stopping auto-refresh...")
				return
			}
		}
	}()
}

// StopAutoRefresh stops the auto-refresh goroutine and cleans up resources.
func (tm *TokenManager) StopAutoRefresh() {
	if tm.refreshTicker != nil {
		tm.refreshTicker.Stop()
	}
	close(tm.stopChan)
	log.Println("[TokenManager] Auto-refresh stopped and resources cleaned.")
}

// shouldRefresh determines if the token should be refreshed.
func (tm *TokenManager) shouldRefresh() bool {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	remaining := time.Until(tm.expiresAt)

	// Trigger refresh when less than 20% of TTL remains
	return remaining < (tm.refreshInterval / 5)
}

// MonitorTelemetry adds monitoring for token usage and expiration.
func (tm *TokenManager) MonitorTelemetry() {
	if !tm.telemetryEnabled {
		return
	}

	go func() {
		ticker := time.NewTicker(30 * time.Second) // Adjust as needed
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				_, valid := tm.GetToken()
				if !valid {
					log.Println("[TokenManager] Token has expired.")
				} else {
					log.Printf("[TokenManager] Token is valid: expires in %s", time.Until(tm.expiresAt))
				}
			case <-tm.stopChan:
				log.Println("[TokenManager] Telemetry monitoring stopped.")
				return
			}
		}
	}()
}
