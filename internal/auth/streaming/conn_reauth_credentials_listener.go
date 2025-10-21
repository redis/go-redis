package streaming

import (
	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/internal/pool"
)

// ConnReAuthCredentialsListener is a struct that implements the CredentialsListener interface.
// It is used to re-authenticate the credentials when they are updated.
// It holds reference to the connection to re-authenticate and will pass it to the reAuth and onErr callbacks.
// It contains:
// - reAuth: a function that takes the new credentials and returns an error if any.
// - onErr: a function that takes an error and handles it.
// - conn: the connection to re-authenticate.
type ConnReAuthCredentialsListener struct {
	// reAuth is called when the credentials are updated.
	reAuth func(conn *pool.Conn, credentials auth.Credentials) error
	// onErr is called when an error occurs.
	onErr func(conn *pool.Conn, err error)
	// conn is the connection to re-authenticate.
	conn *pool.Conn

	manager *Manager
}

// OnNext is called when the credentials are updated.
// It calls the reAuth function with the new credentials.
// If the reAuth function returns an error, it calls the onErr function with the error.
func (c *ConnReAuthCredentialsListener) OnNext(credentials auth.Credentials) {
	if c.conn == nil || c.conn.IsClosed() || c.manager == nil || c.reAuth == nil {
		return
	}

	// Always use async reauth to avoid complex pool semaphore issues
	// The synchronous path can cause deadlocks in the pool's semaphore mechanism
	// when called from the Subscribe goroutine, especially with small pool sizes.
	// The connection pool hook will re-authenticate the connection when it is
	// returned to the pool in a clean, idle state.
	c.manager.MarkForReAuth(c.conn, func(err error) {
		// err is from connection acquisition (timeout, etc.)
		if err != nil {
			// Log the error
			c.OnError(err)
			return
		}
		// err is from reauth command execution
		err = c.reAuth(c.conn, credentials)
		if err != nil {
			// Log the error
			c.OnError(err)
			return
		}
	})
}

// OnError is called when an error occurs.
// It can be called from both the credentials provider and the reAuth function.
func (c *ConnReAuthCredentialsListener) OnError(err error) {
	if c.onErr == nil {
		return
	}

	c.onErr(c.conn, err)
}

// Ensure ConnReAuthCredentialsListener implements the CredentialsListener interface.
var _ auth.CredentialsListener = (*ConnReAuthCredentialsListener)(nil)
