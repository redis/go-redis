package auth

import (
	"time"

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
	reAuth func(conn *pool.Conn, credentials Credentials) error
	onErr  func(conn *pool.Conn, err error)
	conn   *pool.Conn
}

// OnNext is called when the credentials are updated.
// It calls the reAuth function with the new credentials.
// If the reAuth function returns an error, it calls the onErr function with the error.
func (c *ConnReAuthCredentialsListener) OnNext(credentials Credentials) {
	if c.conn.IsClosed() {
		return
	}

	if c.reAuth == nil {
		return
	}

	var err error
	timeout := time.After(1 * time.Second)
	// wait for the connection to be usable
	// this is important because the connection pool may be in the process of reconnecting the connection
	// and we don't want to interfere with that process
	// but we also don't want to block for too long, so incorporate a timeout
	for !c.conn.Usable.CompareAndSwap(true, false) {
		select {
		case <-timeout:
			err = pool.ErrConnUnusableTimeout
			break
		default:
		}
	}
	if err != nil {
		c.OnError(err)
		return
	}
	// we set the usable flag, so restore it back to usable after we're done
	defer c.conn.SetUsable(true)

	err = c.reAuth(c.conn, credentials)
	if err != nil {
		c.OnError(err)
	}
}

// OnError is called when an error occurs.
// It can be called from both the credentials provider and the reAuth function.
func (c *ConnReAuthCredentialsListener) OnError(err error) {
	if c.onErr == nil {
		return
	}

	c.onErr(c.conn, err)
}

// NewConnReAuthCredentialsListener creates a new ConnReAuthCredentialsListener.
// Implements the auth.CredentialsListener interface.
func NewConnReAuthCredentialsListener(conn *pool.Conn, reAuth func(conn *pool.Conn, credentials Credentials) error, onErr func(conn *pool.Conn, err error)) *ConnReAuthCredentialsListener {
	return &ConnReAuthCredentialsListener{
		conn:   conn,
		reAuth: reAuth,
		onErr:  onErr,
	}
}

// Ensure ConnReAuthCredentialsListener implements the CredentialsListener interface.
var _ CredentialsListener = (*ConnReAuthCredentialsListener)(nil)
