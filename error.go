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

func isInternalError(err error) bool {
	_, ok := err.(redisError)
	return ok
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
	if isInternalError(err) {
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

	s := err.Error()
	if strings.HasPrefix(s, "MOVED ") {
		moved = true
	} else if strings.HasPrefix(s, "ASK ") {
		ask = true
	} else {
		return
	}

	ind := strings.LastIndexByte(s, ' ')
	if ind == -1 {
		return false, false, ""
	}
	addr = s[ind+1:]
	return
}

// shouldRetry reports whether failed command should be retried.
func shouldRetry(err error) bool {
	return isNetworkError(err)
}
