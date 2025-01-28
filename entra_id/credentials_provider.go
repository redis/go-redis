package entra_id

import (
	"log"
	"time"
)

// EntraIdIdentityProvider defines the interface for an identity provider
type EntraIdIdentityProvider interface {
	RequestToken(forceRefresh bool) (string, time.Duration, error)
}

// EntraIdCredentialsProvider manages credentials and token lifecycle
type EntraIdCredentialsProvider struct {
	tokenManager *TokenManager
	isStreaming  bool
}

// NewEntraIdCredentialsProvider initializes a new credentials provider
func NewEntraIdCredentialsProvider(idp EntraIdIdentityProvider, refreshInterval time.Duration, telemetryEnabled bool) *EntraIdCredentialsProvider {
	refreshFunc := func() (string, time.Duration, error) {
		return idp.RequestToken(false)
	}

	tokenManager := NewTokenManager(refreshFunc, refreshInterval, telemetryEnabled)

	return &EntraIdCredentialsProvider{
		tokenManager: tokenManager,
		isStreaming:  false,
	}
}

// GetCredentials retrieves the current token or refreshes it if needed
func (cp *EntraIdCredentialsProvider) GetCredentials() (string, error) {
	token, valid := cp.tokenManager.GetToken()
	if !valid {
		if err := cp.tokenManager.RefreshToken(); err != nil {
			log.Printf("[EntraIdCredentialsProvider] Failed to refresh token: %v", err)
			return "", err
		}
		token, _ = cp.tokenManager.GetToken()
	}

	// Start streaming if not already started
	if !cp.isStreaming {
		cp.tokenManager.StartAutoRefresh()
		cp.isStreaming = true
	}

	return token, nil
}

// Stop stops the credentials provider and cleans up resources
func (cp *EntraIdCredentialsProvider) Stop() {
	cp.tokenManager.StopAutoRefresh()
	log.Println("[EntraIdCredentialsProvider] Stopped and cleaned up resources.")
}
