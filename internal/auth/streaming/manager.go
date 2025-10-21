package streaming

import (
	"errors"
	"time"

	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
)

type Manager struct {
	credentialsListeners *CredentialsListeners
	pool                 pool.Pooler
	poolHookRef          *ReAuthPoolHook
}

func NewManager(pl pool.Pooler, reAuthTimeout time.Duration) *Manager {
	m := &Manager{
		pool:                 pl,
		poolHookRef:          NewReAuthPoolHook(pl.Size(), reAuthTimeout),
		credentialsListeners: NewCredentialsListeners(),
	}
	m.poolHookRef.manager = m
	return m
}

func (m *Manager) PoolHook() pool.PoolHook {
	return m.poolHookRef
}

func (m *Manager) Listener(
	poolCn *pool.Conn,
	reAuth func(*pool.Conn, auth.Credentials) error,
	onErr func(*pool.Conn, error),
) (auth.CredentialsListener, error) {
	if poolCn == nil {
		return nil, errors.New("poolCn cannot be nil")
	}
	connID := poolCn.GetID()
	// if we reconnect the underlying network connection, the streaming credentials listener will continue to work
	// so we can get the old listener from the cache and use it.
	// subscribing the same (an already subscribed) listener for a StreamingCredentialsProvider SHOULD be a no-op
	listener, ok := m.credentialsListeners.Get(connID)
	if !ok || listener == nil {
		// Create new listener for this connection
		// Note: Callbacks (reAuth, onErr) are captured once and reused for the connection's lifetime
		newCredListener := &ConnReAuthCredentialsListener{
			conn:    poolCn,
			reAuth:  reAuth,
			onErr:   onErr,
			manager: m,
		}

		m.credentialsListeners.Add(connID, newCredListener)
		listener = newCredListener
	}
	return listener, nil
}

func (m *Manager) MarkForReAuth(poolCn *pool.Conn, reAuthFn func(error)) {
	connID := poolCn.GetID()
	m.poolHookRef.MarkForReAuth(connID, reAuthFn)
}

func (m *Manager) RemoveListener(connID uint64) {
	m.credentialsListeners.Remove(connID)
}
