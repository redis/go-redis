package redis

import (
	"fmt"
	"io"
	"net"
	"strings"
)

// Redis nil reply.
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
	if _, ok := err.(*net.OpError); ok || err == io.EOF {
		return true
	}
	return false
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
