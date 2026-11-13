package rediscmd

import (
	"encoding/hex"
	"fmt"
	"math"
	"net"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
)

// CmdString returns the string representation of a command with credentials redacted.
func CmdString(cmd redis.Cmder) string {
	return CmdStringLimit(cmd, 0)
}

// CmdStringLimit returns the string representation of a command with credentials redacted
// and total length bounded by maxLen. If maxLen <= 0, no truncation is applied.
func CmdStringLimit(cmd redis.Cmder, maxLen int) string {
	b := make([]byte, 0, 32)
	b = AppendCmdLimit(b, cmd, maxLen)
	return String(b)
}

// CmdsString returns a summary and string representation of a pipeline of commands with credentials redacted.
func CmdsString(cmds []redis.Cmder) (string, string) {
	return CmdsStringLimit(cmds, 0)
}

// CmdsSummary returns a space-separated summary of up to 10 unique command names in cmds
// without formatting or allocating command arguments.
func CmdsSummary(cmds []redis.Cmder) string {
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

// CmdsStringLimit returns a summary and string representation of a pipeline of commands with credentials redacted
// and total length bounded by maxLen. If maxLen <= 0, no truncation is applied.
func CmdsStringLimit(cmds []redis.Cmder, maxLen int) (string, string) {
	const numNameLimit = 10

	seen := make(map[string]struct{}, numNameLimit)
	unqNames := make([]string, 0, numNameLimit)

	bufCap := 32 * len(cmds)
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
		b = AppendCmdLimit(b, cmd, maxLen)

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

// AppendCmd appends the string representation of a command to b with credentials redacted.
func AppendCmd(b []byte, cmd redis.Cmder) []byte {
	return AppendCmdLimit(b, cmd, 0)
}

// AppendCmdLimit appends the string representation of a command to b with credentials redacted
// and total length bounded by maxLen. If maxLen <= 0, no truncation is applied.
func AppendCmdLimit(b []byte, cmd redis.Cmder, maxLen int) []byte {
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
		b = appendArgLimit(b, arg, remaining)
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
	case *string:
		return s != nil && len(*s) >= 2 && isRulePrefix((*s)[0])
	case []byte:
		return len(s) >= 2 && isRulePrefix(s[0])
	case *[]byte:
		return s != nil && len(*s) >= 2 && isRulePrefix((*s)[0])
	}
	if v == nil {
		return false
	}
	val := reflect.ValueOf(v)
	if val.IsValid() {
		if val.Kind() == reflect.String {
			s := val.String()
			return len(s) >= 2 && isRulePrefix(s[0])
		}
		if val.Kind() == reflect.Slice && val.Type().Elem().Kind() == reflect.Uint8 {
			b := val.Bytes()
			return len(b) >= 2 && isRulePrefix(b[0])
		}
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
	case *string:
		if v != nil {
			return *v
		}
	case []byte:
		return String(v)
	case *[]byte:
		if v != nil {
			return String(*v)
		}
	}
	if args[i] == nil {
		return ""
	}
	val := reflect.ValueOf(args[i])
	if val.IsValid() {
		if val.Kind() == reflect.String {
			return val.String()
		}
		if val.Kind() == reflect.Slice && val.Type().Elem().Kind() == reflect.Uint8 {
			return String(val.Bytes())
		}
	}
	return ""
}

func appendArg(b []byte, v interface{}) []byte {
	return appendArgLimit(b, v, 0)
}

func appendArgLimit(b []byte, v interface{}, limit int) []byte {
	// 1. Check fmt.Stringer first to honor custom String() methods (prevents credential leaks on named types).
	// Exception: time.Time implements fmt.Stringer (returning "2006-01-02 15:04:05..."), but go-redis formats times as RFC3339Nano.
	if _, isTime := v.(time.Time); !isTime {
		if _, isTimePtr := v.(*time.Time); !isTimePtr {
			if s, ok := v.(fmt.Stringer); ok {
				if v == nil || (reflect.ValueOf(v).Kind() == reflect.Pointer && reflect.ValueOf(v).IsNil()) {
					return append(b, "<nil>"...)
				}
				return appendStringTruncated(b, safeString(s), limit)
			}
		}
	}

	switch v := v.(type) {
	case nil:
		return append(b, "<nil>"...)
	case string:
		return appendStringTruncated(b, v, limit)
	case *string:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return appendStringTruncated(b, *v, limit)
	case []byte:
		return appendBytesTruncated(b, v, limit)
	case *[]byte:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return appendBytesTruncated(b, *v, limit)
	case net.IP:
		return appendBytesTruncated(b, v, limit)
	case time.Time:
		return v.AppendFormat(b, time.RFC3339Nano)
	case *time.Time:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return v.AppendFormat(b, time.RFC3339Nano)
	case time.Duration:
		return append(b, v.String()...)
	case *time.Duration:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return append(b, v.String()...)
	case int:
		return strconv.AppendInt(b, int64(v), 10)
	case *int:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendInt(b, int64(*v), 10)
	case int8:
		return strconv.AppendInt(b, int64(v), 10)
	case *int8:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendInt(b, int64(*v), 10)
	case int16:
		return strconv.AppendInt(b, int64(v), 10)
	case *int16:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendInt(b, int64(*v), 10)
	case int32:
		return strconv.AppendInt(b, int64(v), 10)
	case *int32:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendInt(b, int64(*v), 10)
	case int64:
		return strconv.AppendInt(b, v, 10)
	case *int64:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendInt(b, *v, 10)
	case uint:
		return strconv.AppendUint(b, uint64(v), 10)
	case *uint:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendUint(b, uint64(*v), 10)
	case uint8:
		return strconv.AppendUint(b, uint64(v), 10)
	case *uint8:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendUint(b, uint64(*v), 10)
	case uint16:
		return strconv.AppendUint(b, uint64(v), 10)
	case *uint16:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendUint(b, uint64(*v), 10)
	case uint32:
		return strconv.AppendUint(b, uint64(v), 10)
	case *uint32:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendUint(b, uint64(*v), 10)
	case uint64:
		return strconv.AppendUint(b, v, 10)
	case *uint64:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendUint(b, *v, 10)
	case float32:
		return strconv.AppendFloat(b, float64(v), 'f', -1, 64)
	case *float32:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendFloat(b, float64(*v), 'f', -1, 64)
	case float64:
		return strconv.AppendFloat(b, v, 'f', -1, 64)
	case *float64:
		if v == nil {
			return append(b, "<nil>"...)
		}
		return strconv.AppendFloat(b, *v, 'f', -1, 64)
	case bool:
		if v {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case *bool:
		if v == nil {
			return append(b, "<nil>"...)
		}
		if *v {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	}

	val := reflect.ValueOf(v)
	if val.IsValid() {
		if val.Kind() == reflect.String {
			return appendStringTruncated(b, val.String(), limit)
		}
		if val.Kind() == reflect.Slice && val.Type().Elem().Kind() == reflect.Uint8 {
			return appendBytesTruncated(b, val.Bytes(), limit)
		}
		if val.Kind() == reflect.Pointer && val.IsNil() {
			return append(b, "<nil>"...)
		}
	}

	// When truncation is enabled, use a bounded type placeholder for non-string/non-[]byte
	// custom values (structs, arrays, maps, etc.) to avoid large memory allocations.
	if limit > 0 && val.IsValid() {
		t := val.Type()
		if t.Kind() == reflect.Pointer {
			t = t.Elem()
		}
		placeholder := "<" + t.String() + ">"
		return appendStringTruncated(b, placeholder, limit)
	}

	s := fmt.Sprint(v)
	return appendStringTruncated(b, s, limit)
}

func safeString(s fmt.Stringer) (out string) {
	defer func() {
		if recover() != nil {
			out = fmt.Sprint(s) // fmt recovers the panic itself
		}
	}()
	return s.String()
}

func appendStringTruncated(b []byte, v string, limit int) []byte {
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
			b = appendUTF8String(b, Bytes(v[:charLimit]))
		}
		b = append(b, "…(+"...)
		b = strconv.AppendInt(b, int64(len(v)-charLimit), 10)
		return append(b, " bytes)"...)
	}
	return appendUTF8String(b, Bytes(v))
}

func appendBytesTruncated(b []byte, v []byte, limit int) []byte {
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
