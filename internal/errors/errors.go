package errors

import (
	"io"
	"net"
	"strings"
)

const Nil = RedisError("redis: nil")

type RedisError string

func (e RedisError) Error() string { return string(e) }

func IsRetryable(err error) bool {
	return IsNetwork(err)
}

func IsInternal(err error) bool {
	_, ok := err.(RedisError)
	return ok
}

func IsNetwork(err error) bool {
	if err == io.EOF {
		return true
	}
	_, ok := err.(net.Error)
	return ok
}

func IsBadConn(err error, allowTimeout bool) bool {
	if err == nil {
		return false
	}
	if IsInternal(err) {
		return false
	}
	if allowTimeout {
		if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
			return false
		}
	}
	return true
}

func IsMoved(err error) (moved bool, ask bool, addr string) {
	if !IsInternal(err) {
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

	ind := strings.LastIndex(s, " ")
	if ind == -1 {
		return false, false, ""
	}
	addr = s[ind+1:]
	return
}
