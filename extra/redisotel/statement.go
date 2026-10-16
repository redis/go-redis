package redisotel

import (
	"encoding/hex"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/extra/rediscmd/v9"
	"github.com/redis/go-redis/v9"
)

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
			if hasSecretRulePrefix(args[i]) {
				mark(i)
			}
		}

	case equalFoldArg(args, 0, "migrate"):
		// MIGRATE host port key db timeout [AUTH password] [AUTH2 user password]
		for i := 6; i < len(args); i++ {
			if equalFoldArg(args, i, "keys") {
				break
			}
			// Advance past the marked value(s): otherwise a password that is
			// itself "auth"/"auth2" would be re-checked as a keyword and redact
			// the following argument (and leave the real password in the clear).
			if equalFoldArg(args, i, "auth") {
				mark(i + 1)
				i++
			} else if equalFoldArg(args, i, "auth2") {
				mark(i + 2)
				i += 2
			}
		}

	case equalFoldArg(args, 0, "sentinel") && equalFoldArg(args, 1, "set"):
		// SENTINEL SET <master> option value [option value ...]; auth-pass holds
		// the monitored master's password. Every option takes one value except
		// rename-command <oldname> <newname>, which takes two.
		for i := 3; i+1 < len(args); i += 2 {
			if equalFoldArg(args, i, "rename-command") {
				i++
				continue
			}
			if equalFoldArg(args, i, "auth-pass") {
				mark(i + 1)
			}
		}
	}

	return marks
}

func equalFoldArg(args []interface{}, i int, want string) bool {
	return strings.EqualFold(argString(args, i), want)
}

func hasSecretRulePrefix(v interface{}) bool {
	switch s := v.(type) {
	case string:
		return len(s) >= 2 && isRulePrefix(s[0])
	case []byte:
		return len(s) >= 2 && isRulePrefix(s[0])
	}
	return false
}

func isRulePrefix(b byte) bool {
	switch b {
	case '>', '<', '#', '!':
		return true
	}
	return false
}

func argString(args []interface{}, i int) string {
	if i < 0 || i >= len(args) {
		return ""
	}
	switch v := args[i].(type) {
	case string:
		return v
	case []byte:
		return string(v)
	}
	return ""
}

func buildCmdStatement(cmd redis.Cmder, maxLen int) string {
	if maxLen <= 0 {
		return rediscmd.CmdString(cmd)
	}
	b := make([]byte, 0, 64)
	b = appendCmdTruncated(b, cmd, maxLen)
	return string(b)
}

func buildPipelineStatement(cmds []redis.Cmder, maxLen int) string {
	if maxLen <= 0 {
		_, cmdsString := rediscmd.CmdsString(cmds)
		return cmdsString
	}
	bufCap := 64 * len(cmds)
	if maxLen > 0 {
		target := maxLen
		if target < math.MaxInt-64 {
			target += 64
		}
		if bufCap > target {
			bufCap = target
		}
	}
	b := make([]byte, 0, bufCap)
	for i, cmd := range cmds {
		if maxLen > 0 && len(b) >= maxLen {
			b = append(b, "\n…"...)
			break
		}
		if i > 0 {
			b = append(b, '\n')
		}
		b = appendCmdTruncated(b, cmd, maxLen)
	}
	return string(b)
}

func appendCmdTruncated(b []byte, cmd redis.Cmder, maxLen int) []byte {
	args := cmd.Args()
	if len(args) == 0 {
		name := cmd.Name()
		if name != "" {
			b = append(b, name...)
		}
		if err := cmd.Err(); err != nil {
			b = append(b, ": "...)
			b = append(b, err.Error()...)
		}
		return b
	}

	secret := secretArgs(args)

	for i, arg := range args {
		if maxLen > 0 && len(b) >= maxLen {
			b = append(b, " …"...)
			break
		}
		if i > 0 {
			b = append(b, ' ')
		}
		if secret != nil && secret[i] {
			b = append(b, redactedArg...)
			continue
		}
		remaining := maxLen
		if maxLen > 0 {
			remaining = maxLen - len(b)
			if remaining <= 0 {
				b = append(b, "…"...)
				break
			}
		}
		b = appendArgTruncated(b, arg, remaining)
	}

	if err := cmd.Err(); err != nil {
		b = append(b, ": "...)
		b = append(b, err.Error()...)
	}

	return b
}

func appendArgTruncated(b []byte, v interface{}, limit int) []byte {
	switch v := v.(type) {
	case nil:
		return append(b, "<nil>"...)
	case string:
		checkLen := len(v)
		if limit > 0 && checkLen > limit {
			checkLen = limit
		}
		isHex := !isSimpleString(v[:checkLen])
		charLimit := limit
		if isHex && limit > 0 {
			charLimit = limit / 2
		}
		if limit > 0 && len(v) > charLimit {
			if charLimit > 0 {
				b = appendUTF8String(b, []byte(v[:charLimit]))
			}
			b = append(b, "…(+"...)
			b = strconv.AppendInt(b, int64(len(v)-charLimit), 10)
			return append(b, " bytes)"...)
		}
		return appendUTF8String(b, []byte(v))
	case []byte:
		checkLen := len(v)
		if limit > 0 && checkLen > limit {
			checkLen = limit
		}
		isHex := !isSimple(v[:checkLen])
		byteLimit := limit
		if isHex && limit > 0 {
			byteLimit = limit / 2
		}
		if limit > 0 && len(v) > byteLimit {
			if byteLimit > 0 {
				b = appendUTF8String(b, v[:byteLimit])
			}
			b = append(b, "…(+"...)
			b = strconv.AppendInt(b, int64(len(v)-byteLimit), 10)
			return append(b, " bytes)"...)
		}
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
		s := fmt.Sprint(v)
		checkLen := len(s)
		if limit > 0 && checkLen > limit {
			checkLen = limit
		}
		isHex := !isSimpleString(s[:checkLen])
		charLimit := limit
		if isHex && limit > 0 {
			charLimit = limit / 2
		}
		if limit > 0 && len(s) > charLimit {
			if charLimit > 0 {
				b = appendUTF8String(b, []byte(s[:charLimit]))
			}
			b = append(b, "…(+"...)
			b = strconv.AppendInt(b, int64(len(s)-charLimit), 10)
			return append(b, " bytes)"...)
		}
		return appendUTF8String(b, []byte(s))
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

func isSimpleString(s string) bool {
	for i := 0; i < len(s); i++ {
		if !isSimpleByte(s[i]) {
			return false
		}
	}
	return true
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

func pipelineSummary(cmds []redis.Cmder) string {
	const numNameLimit = 10
	seen := make(map[string]struct{}, numNameLimit)
	unqNames := make([]string, 0, numNameLimit)
	for _, cmd := range cmds {
		name := cmd.FullName()
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			unqNames = append(unqNames, name)
			if len(unqNames) >= numNameLimit {
				break
			}
		}
	}
	return strings.Join(unqNames, " ")
}
