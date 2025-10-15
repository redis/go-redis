package auth

import (
	"runtime"
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
// - checkUsableTimeout: the timeout to wait for the connection to be usable - default is 1 second.
type ConnReAuthCredentialsListener struct {
	// reAuth is called when the credentials are updated.
	reAuth func(conn *pool.Conn, credentials Credentials) error
	// onErr is called when an error occurs.
	onErr func(conn *pool.Conn, err error)
	// conn is the connection to re-authenticate.
	conn *pool.Conn
	// checkUsableTimeout is the timeout to wait for the connection to be usable
	// when the credentials are updated.
	// default is 1 second
	checkUsableTimeout time.Duration
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

	// this hard-coded timeout is not ideal
	timeout := time.After(c.checkUsableTimeout)
	// wait for the connection to be usable
	// this is important because the connection pool may be in the process of reconnecting the connection
	// and we don't want to interfere with that process
	// but we also don't want to block for too long, so incorporate a timeout
	for err == nil && !c.conn.Usable.CompareAndSwap(true, false) {
		select {
		case <-timeout:
			err = pool.ErrConnUnusableTimeout
		default:
			runtime.Gosched()
		}
	}
	if err == nil {
		defer c.conn.SetUsable(true)
	}

	for err == nil && !c.conn.Used.CompareAndSwap(false, true) {
		select {
		case <-timeout:
			err = pool.ErrConnUnusableTimeout
		default:
			runtime.Gosched()
		}
	}

	// we timed out waiting for the connection to be usable
	// do not try to re-authenticate, instead call the onErr function
	// which will handle the error and close the connection if needed
	if err != nil {
		c.OnError(err)
		return
	}

	defer c.conn.Used.Store(false)
	// we set the usable flag, so restore it back to usable after we're done
	if err = c.reAuth(c.conn, credentials); err != nil {
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

// SetCheckUsableTimeout sets the timeout for the connection to be usable.
func (c *ConnReAuthCredentialsListener) SetCheckUsableTimeout(timeout time.Duration) {
	c.checkUsableTimeout = timeout
}

// NewConnReAuthCredentialsListener creates a new ConnReAuthCredentialsListener.
// Implements the auth.CredentialsListener interface.
func NewConnReAuthCredentialsListener(conn *pool.Conn, reAuth func(conn *pool.Conn, credentials Credentials) error, onErr func(conn *pool.Conn, err error)) *ConnReAuthCredentialsListener {
	return &ConnReAuthCredentialsListener{
		conn:               conn,
		reAuth:             reAuth,
		onErr:              onErr,
		checkUsableTimeout: 1 * time.Second,
	}
}

// Ensure ConnReAuthCredentialsListener implements the CredentialsListener interface.
var _ CredentialsListener = (*ConnReAuthCredentialsListener)(nil)
