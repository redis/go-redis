package redis

import (
	"context"
	"io"
	"net"
	"strings"

	"github.com/go-redis/redis/internal/pool"
	"github.com/go-redis/redis/internal/proto"
)

func isRetryableError(err error, retryTimeout bool) bool {
	switch err {
	case nil, context.Canceled, context.DeadlineExceeded, pool.ErrBadConn:
		return false
	case io.EOF:
		return true
	}
	if netErr, ok := err.(net.Error); ok {
		if netErr.Timeout() {
			return retryTimeout
		}
		return true
	}

	s := err.Error()
	if s == "ERR max number of clients reached" {
		return true
	}
	if strings.HasPrefix(s, "LOADING ") {
		return true
	}
	if strings.HasPrefix(s, "READONLY ") {
		return true
	}
	if strings.HasPrefix(s, "CLUSTERDOWN ") {
		return true
	}
	return false
}

func isRedisError(err error) bool {
	_, ok := err.(proto.RedisError)
	return ok
}

func isBadConn(err error, allowTimeout bool) bool {
	switch err {
	case nil:
		return false
	case pool.ErrBadConn:
		return true
	}
	if isRedisError(err) {
		return isReadOnlyError(err) // #790
	}
	if allowTimeout {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false
		}
	}
	return true
}

func isMovedError(err error) (moved bool, ask bool, addr string) {
	if !isRedisError(err) {
		return
	}

	s := err.Error()
	switch {
	case strings.HasPrefix(s, "MOVED "):
		moved = true
	case strings.HasPrefix(s, "ASK "):
		ask = true
	default:
		return
	}

	ind := strings.LastIndex(s, " ")
	if ind == -1 {
		return false, false, ""
	}
	addr = s[ind+1:]
	return
}

func isLoadingError(err error) bool {
	return strings.HasPrefix(err.Error(), "LOADING ")
}

func isReadOnlyError(err error) bool {
	return strings.HasPrefix(err.Error(), "READONLY ")
}
