package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

var (
	cscYesWire = []byte("*3\r\n$6\r\nCLIENT\r\n$7\r\nCACHING\r\n$3\r\nYES\r\n")
	cscNoWire  = []byte("*3\r\n$6\r\nCLIENT\r\n$7\r\nCACHING\r\n$2\r\nNO\r\n")
)

func (c *baseClient) cscFlag(cmd Cmder, store bool) byte {
	if !c.cscTrackingRequested() || cmd.cacheState().ask {
		return 0
	}
	if c.cscPolicy.mode == TrackingModeOptIn && store {
		return 'Y'
	}
	if c.cscPolicy.mode == TrackingModeOptOut && !store && c.cscTrackable(cmd) {
		return 'N'
	}
	return 0
}

func writeCSCPrefix(wr *proto.Writer, flag byte) error {
	var err error
	if flag == 'Y' {
		_, err = wr.Write(cscYesWire)
	}
	if flag == 'N' {
		_, err = wr.Write(cscNoWire)
	}
	return err
}

// readCSCAck consumes one complete control reply. A Redis error is recoverable:
// the following data reply still belongs to this request, but cannot publish.
// Everything else must establish the exact OK frame or retire the connection.
func (c *baseClient) readCSCAck(ctx context.Context, cn *pool.Conn, rd *proto.Reader) (bool, error) {
	if err := c.drainPushFrames(ctx, cn, rd, true); err != nil {
		return false, err
	}
	typ, err := rd.PeekReplyType()
	if err != nil {
		return false, err
	}
	if typ != proto.RespStatus && typ != proto.RespError && typ != proto.RespBlobError {
		return false, fmt.Errorf("redis: invalid CLIENT CACHING reply type %q", typ)
	}
	status, err := rd.ReadString()
	if isRedisError(err) {
		internal.Logger.Printf(ctx, "csc: CLIENT CACHING rejected: %v; returning uncached data", err)
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if status != "OK" {
		return false, fmt.Errorf("redis: invalid CLIENT CACHING status %q", status)
	}
	return true, nil
}
