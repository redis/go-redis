package redis

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

type Cache interface{}

type cache struct {
	// cluster? sentinel?
	conn *Conn
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

// invalidatePush check if the data packet is a key-invalidate push and return the invalid key.
func (c *cache) invalidatePush(v []any) ([]string, bool) {
	if len(v) != 2 {
		return nil, false
	}
	if s, ok := v[0].(string); !ok || s != "invalidate" {
		return nil, false
	}
	slice, ok := v[1].([]any)
	if !ok {
		return nil, false
	}

	keys := make([]string, 0, len(slice))
	for i := 0; i < len(slice); i++ {
		if s, ok := slice[i].(string); ok {
			keys = append(keys, s)
		}
	}
	return keys, true
}

// ------------------------------------- Broadcasting -------------------------------------
