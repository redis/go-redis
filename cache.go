package redis

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

type Cache interface{}

type cache struct {
	client *Client

	// cluster? sentinel?
	conn   *Conn
	prefix []string

	closed int32 // atomic
}

func newCache() Cache {
	// ?
	return &cache{}
}

// ------------------------------------------------------------------------------------------

// extension method

func (c *Conn) readReply(ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error) error {
	return c.withConn(ctx, func(ctx context.Context, conn *pool.Conn) error {
		return conn.WithReader(ctx, timeout, fn)
	})
}

// ------------------------------------------------------------------------------------------

// Client-side caching command.

type trackingArgs struct {
	redirect  int
	prefixes  []string
	broadcast bool
	optIn     bool
	optOut    bool
	noLoop    bool
}

func (c *cache) clientTracking(ctx context.Context, t *trackingArgs) *StringCmd {
	args := make([]any, 0, 7+len(t.prefixes))
	args = append(args, "CLIENT", "TRACKING", "ON")
	if t.redirect > 0 {
		args = append(args, "REDIRECT", t.redirect)
	}
	if len(t.prefixes) > 0 {
		for _, prefix := range t.prefixes {
			args = append(args, "PREFIX", prefix)
		}
	}
	if t.optIn {
		args = append(args, "OPTIN")
	}
	if t.optOut {
		args = append(args, "OPTOUT")
	}
	if t.noLoop {
		args = append(args, "NOLOOP")
	}
	cmd := NewStringCmd(ctx, args...)
	_ = c.conn.Process(ctx, cmd)
	return cmd
}

func (c *cache) trackingClose(ctx context.Context) error {
	return c.conn.Process(ctx, NewStringCmd(ctx, "CLIENT", "TRACKING", "OFF"))
}

func (c *cache) cachingYes(ctx context.Context) error {
	return c.conn.Process(ctx, NewStringCmd(ctx, "CLIENT", "CACHING", "YES"))
}

func (c *cache) cachingNo(ctx context.Context) error {
	return c.conn.Process(ctx, NewStringCmd(ctx, "CLIENT", "CACHING", "NO"))
}

// ------------------------------------------------------------------------------------

// readInvalidate To read the expired message push from redis-server,
// we only read for invalidate messages, and consider any other data that is read as an error.
func (c *cache) readInvalidate(rd *proto.Reader) ([]string, error) {
	line, err := rd.ReadLine()
	if err != nil {
		return nil, err
	}

	if line[0] != proto.RespPush {
		return nil, fmt.Errorf("invalid data-%s", string(line))
	}

	n, err := strconv.Atoi(string(line[1:]))
	if err != nil {
		return nil, err
	}
	if n != 2 {
		return nil, fmt.Errorf("got %d elements in the map, wanted %d", n, 2)
	}

	// read `invalidate`
	s, err := rd.ReadString()
	if err != nil {
		return nil, err
	}
	if s != "invalidate" {
		return nil, fmt.Errorf("not a client-side caching push message, data-%s", s)
	}

	n, err = rd.ReadArrayLen()
	if err != nil {
		return nil, err
	}

	keys := make([]string, 0, n)
	for i := 0; i < n; i++ {
		key, err := rd.ReadString()
		if err != nil {
			return nil, err
		}
		keys = append(keys, key)
	}

	return keys, nil
}

// ------------------------------------- Broadcasting -------------------------------------

func (c *cache) listen(timeout time.Duration) {
	ctx := context.Background()
	defer func() {
		if err := recover(); err != nil {
			internal.Logger.Printf(ctx, "redis cache: panic - %v", err)
		}
	}()

	if timeout == 0 {
		timeout = 30 * time.Second
	}
	internal.Logger.Printf(ctx, "redis cache: listen working, read timeout-%d second", int(timeout/time.Second))

	// state, 0-normal, 1-need init track
	const (
		normal = 0
		bad    = 1
	)
	var state = normal
	for {
		if atomic.LoadInt32(&c.closed) == 1 {
			_ = c.conn.Close()
			internal.Logger.Printf(ctx, "redis cache: close, quit listen")
			return
		}

		if state == bad {
			internal.Logger.Printf(ctx, "redis cache: state bad")
			if err := c.initTrack(ctx); err != nil {
				internal.Logger.Printf(ctx, "redis cache: listen init track error-%s", err.Error())
				time.Sleep(1 * time.Second)
				continue
			}
		}

		if err := c.conn.Ping(ctx).Err(); err != nil {
			internal.Logger.Printf(ctx, "redis cache: listen ping error-%s", err.Error())
			state = bad
			continue
		}
		state = normal

		var keys []string
		err := c.conn.withConn(ctx, func(ctx context.Context, conn *pool.Conn) error {
			return conn.WithReader(ctx, timeout, func(rd *proto.Reader) (err error) {
				keys, err = c.readInvalidate(rd)

				if err == nil {
					return nil
				}

				// The timeout error is considered normal, and it is triggered when we fail
				// to receive a notification. We handle it as nil.
				// We cannot return the timeout error, as go-redis would consider it a network
				// problem and close the network connection.
				if isNetTimeout(err) {
					err = nil
					return err
				}

				// We only listen for redis-push notifications, so under normal circumstances,
				// we should not receive any redis-error notifications.
				// If we do, we need to handle them as errors; otherwise,
				// go-redis may consider redis errors as normal occurrences.
				if isRedisError(err) {
					err = fmt.Errorf("redis cache: unexpected response redis-error-msg-%s", err.Error())
				}

				return err
			})
		})

		// under normal circumstances, we should not receive any errors, including redis errors.
		if err != nil {
			state = bad

			internal.Logger.Printf(ctx, "redis cache: read push data error-%s", err.Error())
			continue
		}

		// it's possible that we may not receive any notifications for keys.
		if len(keys) > 0 {
			// handle keys
		}
	}
}

func (c *cache) initTrack(ctx context.Context) error {
	internal.Logger.Printf(ctx, "redis cache: init track")
	if c.conn != nil {
		_ = c.conn.Close()
	}
	c.conn = c.client.Conn()

	args := make([]any, 0, 3+2*len(c.prefix)+1)
	args = append(args, "CLIENT", "TRACKING", "ON")
	for _, prefix := range c.prefix {
		args = append(args, "PREFIX", prefix)
	}
	args = append(args, "BCAST")
	cmd := NewStringCmd(ctx, args...)

	if err := c.conn.Process(ctx, cmd); err != nil {
		_ = c.conn.Close()
		return err
	}

	return nil
}

// isNetTimeout check err == net timeout
func isNetTimeout(err error) bool {
	if err == nil {
		return false
	}
	netErr, ok := err.(net.Error)
	if !ok {
		return false
	}
	return netErr.Timeout()
}
