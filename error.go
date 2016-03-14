package redis

import (
	"fmt"
	"io"
	"net"
	"strings"
)

// Redis nil reply, .e.g. when key does not exist.
var Nil = errorf("redis: nil")

// Redis transaction failed.
var TxFailedErr = errorf("redis: transaction failed")

type redisError struct {
	s string
}

func errorf(s string, args ...interface{}) redisError {
	return redisError{s: fmt.Sprintf(s, args...)}
}

func (err redisError) Error() string {
	return err.s
}

func isNetworkError(err error) bool {
	if err == io.EOF {
		return true
	}
	_, ok := err.(net.Error)
	return ok
}

func isBadConn(err error, allowTimeout bool) bool {
	if err == nil {
		return false
	}
	if _, ok := err.(redisError); ok {
		return false
	}
	if allowTimeout {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false
		}
	}
	return true
}

func isMovedError(err error) (moved bool, ask bool, addr string) {
	if _, ok := err.(redisError); !ok {
		return
	}

	parts := strings.SplitN(err.Error(), " ", 3)
	if len(parts) != 3 {
		return
	}

	switch parts[0] {
	case "MOVED":
		moved = true
		addr = parts[2]
	case "ASK":
		ask = true
		addr = parts[2]
	}

	return
}

// shouldRetry reports whether failed command should be retried.
func shouldRetry(err error) bool {
	if err == nil {
		return false
	}
	return isNetworkError(err)
}
