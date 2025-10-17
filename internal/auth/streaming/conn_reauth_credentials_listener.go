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
	if c.conn.IsClosed() {
		return
	}

	if c.reAuth == nil {
		return
	}

	// this connection is not in use, so we can re-authenticate it
	if c.conn.Used.CompareAndSwap(false, true) {
		// try to acquire the connection for background operation
		if c.conn.Usable.CompareAndSwap(true, false) {
			err := c.reAuth(c.conn, credentials)
			if err != nil {
				c.OnError(err)
			}
			c.conn.Usable.Store(true)
			c.conn.Used.Store(false)
			return
		}
		c.conn.Used.Store(false)
	}
	// else if the connection is in use, mark it for re-authentication
	// and connection pool hook will re-authenticate it when it is returned to the pool
	// or in case the connection WAS in the pool, but handoff is in progress, the pool hook
	// will re-authenticate it when the handoff is complete
	// and the connection is acquired from the pool
	c.manager.MarkForReAuth(c.conn, func(err error) {
		if err != nil {
			c.OnError(err)
			return
		}
		err = c.reAuth(c.conn, credentials)
		if err != nil {
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
