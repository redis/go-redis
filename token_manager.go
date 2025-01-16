package redis

import (
	"sync"
	"time"
)

type TokenManager struct {
	token     string
	expiresAt time.Time
	mutex     sync.Mutex
}

func NewTokenManager() *TokenManager {
	return &TokenManager{}
}

func (tm *TokenManager) SetToken(token string, ttl time.Duration) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	tm.token = token
	tm.expiresAt = time.Now().Add(ttl)
}

func (tm *TokenManager) GetToken() (string, bool) {
	tm.mutex.Lock()
	defer tm.mutex.Unlock()
	if time.Now().After(tm.expiresAt) {
		return "", false
	}
	return tm.token, true
}

func (tm *TokenManager) RefreshToken(fetchToken func() (string, time.Duration, error)) error {
	token, ttl, err := fetchToken()
	if err != nil {
		return err
	}
	tm.SetToken(token, ttl)
	return nil
}
