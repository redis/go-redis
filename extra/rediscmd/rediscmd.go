package rediscmd

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

func CmdString(cmd redis.Cmder) string {
	b := make([]byte, 0, 32)
	b = AppendCmd(b, cmd)
	return String(b)
}

func CmdsString(cmds []redis.Cmder) (string, string) {
	const numNameLimit = 10

	seen := make(map[string]struct{}, numNameLimit)
	unqNames := make([]string, 0, numNameLimit)

	b := make([]byte, 0, 32*len(cmds))

	for i, cmd := range cmds {
		if i > 0 {
			b = append(b, '\n')
		}
		b = AppendCmd(b, cmd)

		if len(unqNames) >= numNameLimit {
			continue
		}

		name := cmd.FullName()
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			unqNames = append(unqNames, name)
		}
	}

	summary := strings.Join(unqNames, " ")
	return summary, String(b)
}

func AppendCmd(b []byte, cmd redis.Cmder) []byte {
	args := cmd.Args()
	secret := secretArgs(args)

	for i, arg := range args {
		if i > 0 {
			b = append(b, ' ')
		}
		if secret != nil && secret[i] {
			b = append(b, redactedArg...)
			continue
		}
		b = appendArg(b, arg)
	}

	if err := cmd.Err(); err != nil {
		b = append(b, ": "...)
		b = append(b, err.Error()...)
	}

	return b
}

const redactedArg = "<redacted>"

// secretConfigParams are the CONFIG SET parameters whose value is a credential.
var secretConfigParams = []string{
	"requirepass",
	"masterauth",
	"tls-key-file-pass",
	"tls-client-key-file-pass",
}

// secretArgs marks the positions in args that hold a credential. It returns nil
// when the command carries none, which is the common case.
//
// The client sends HELLO ... AUTH on every handshake and AUTH on every
// streaming-credentials rotation, both through the regular hook chain, so a
// tracing hook sees them whether or not the caller ever issued one.
func secretArgs(args []interface{}) []bool {
	if len(args) < 2 {
		return nil
	}

	var marks []bool
	mark := func(i int) {
		if i < 1 || i >= len(args) {
			return
		}
		if marks == nil {
			marks = make([]bool, len(args))
		}
		marks[i] = true
	}

	switch {
	case equalFoldArg(args, 0, "auth"):
		// AUTH password | AUTH username password
		mark(len(args) - 1)

	case equalFoldArg(args, 0, "hello"):
		// HELLO ver [AUTH username password] [SETNAME name]
		if equalFoldArg(args, 2, "auth") {
			mark(4)
		}

	case equalFoldArg(args, 0, "config") && equalFoldArg(args, 1, "set"):
		// CONFIG SET param value [param value ...]
		for i := 2; i+1 < len(args); i += 2 {
			for _, param := range secretConfigParams {
				if equalFoldArg(args, i, param) {
					mark(i + 1)
					break
				}
			}
		}

	case equalFoldArg(args, 0, "acl") && equalFoldArg(args, 1, "setuser"):
		// ACL SETUSER username rule...; the >pass, <pass, #hash and !hash rules
		// embed the credential in the rule itself.
		for i := 3; i < len(args); i++ {
			rule := argString(args, i)
			if len(rule) < 2 {
				continue
			}
			switch rule[0] {
			case '>', '<', '#', '!':
				mark(i)
			}
		}

	case equalFoldArg(args, 0, "migrate"):
		// MIGRATE host port key db timeout [AUTH password] [AUTH2 user password]
		for i := 6; i < len(args); i++ {
			if equalFoldArg(args, i, "keys") {
				break
			}
			if equalFoldArg(args, i, "auth") {
				mark(i + 1)
			} else if equalFoldArg(args, i, "auth2") {
				mark(i + 2)
			}
		}
	}

	return marks
}

func equalFoldArg(args []interface{}, i int, want string) bool {
	return strings.EqualFold(argString(args, i), want)
}

func argString(args []interface{}, i int) string {
	if i < 0 || i >= len(args) {
		return ""
	}
	switch v := args[i].(type) {
	case string:
		return v
	case []byte:
		return String(v)
	}
	return ""
}

func appendArg(b []byte, v interface{}) []byte {
	switch v := v.(type) {
	case nil:
		return append(b, "<nil>"...)
	case string:
		return appendUTF8String(b, Bytes(v))
	case []byte:
		return appendUTF8String(b, v)
	case int:
		return strconv.AppendInt(b, int64(v), 10)
	case int8:
		return strconv.AppendInt(b, int64(v), 10)
	case int16:
		return strconv.AppendInt(b, int64(v), 10)
	case int32:
		return strconv.AppendInt(b, int64(v), 10)
	case int64:
		return strconv.AppendInt(b, v, 10)
	case uint:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint8:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint16:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint32:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint64:
		return strconv.AppendUint(b, v, 10)
	case float32:
		return strconv.AppendFloat(b, float64(v), 'f', -1, 64)
	case float64:
		return strconv.AppendFloat(b, v, 'f', -1, 64)
	case bool:
		if v {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case time.Time:
		return v.AppendFormat(b, time.RFC3339Nano)
	default:
		return append(b, fmt.Sprint(v)...)
	}
}

func appendUTF8String(dst []byte, src []byte) []byte {
	if isSimple(src) {
		dst = append(dst, src...)
		return dst
	}

	s := len(dst)
	dst = append(dst, make([]byte, hex.EncodedLen(len(src)))...)
	hex.Encode(dst[s:], src)
	return dst
}

func isSimple(b []byte) bool {
	for _, c := range b {
		if !isSimpleByte(c) {
			return false
		}
	}
	return true
}

func isSimpleByte(c byte) bool {
	return c >= 0x20 && c <= 0x7e
}
