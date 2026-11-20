package internal

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9/internal/util"
)

func AppendArg(b []byte, v interface{}) []byte {
	switch v := v.(type) {
	case nil:
		return append(b, "<nil>"...)
	case string:
		return appendUTF8String(b, util.StringToBytes(v))
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
		// proto.Writer sends a pointer to a basic type or time.Time by value,
		// so print the value, not the address.
		if rv := reflect.ValueOf(v); rv.Kind() == reflect.Pointer && !rv.IsNil() {
			e := rv.Elem()
			_, isTime := e.Interface().(time.Time)
			if isTime || e.Kind() == reflect.String || (e.Kind() >= reflect.Bool && e.Kind() <= reflect.Float64) {
				return AppendArg(b, e.Interface())
			}
		}
		return append(b, fmt.Sprint(v)...)
	}
}

func appendUTF8String(dst []byte, src []byte) []byte {
	dst = append(dst, src...)
	return dst
}
