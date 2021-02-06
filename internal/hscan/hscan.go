package hscan

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

// decoderFunc represents decoding functions for default built-in types.
type decoderFunc func(reflect.Value, string) bool

var (
	// List of built-in decoders indexed by their numeric constant values (eg: reflect.Bool = 1).
	decoders = []decoderFunc{
		reflect.Bool:          decodeBool,
		reflect.Int:           decodeInt,
		reflect.Int8:          decodeInt,
		reflect.Int16:         decodeInt,
		reflect.Int32:         decodeInt,
		reflect.Int64:         decodeInt,
		reflect.Uint:          decodeUint,
		reflect.Uint8:         decodeUint,
		reflect.Uint16:        decodeUint,
		reflect.Uint32:        decodeUint,
		reflect.Uint64:        decodeUint,
		reflect.Float32:       decodeFloat,
		reflect.Float64:       decodeFloat,
		reflect.Complex64:     decodeUnsupported,
		reflect.Complex128:    decodeUnsupported,
		reflect.Array:         decodeUnsupported,
		reflect.Chan:          decodeUnsupported,
		reflect.Func:          decodeUnsupported,
		reflect.Interface:     decodeUnsupported,
		reflect.Map:           decodeUnsupported,
		reflect.Ptr:           decodeUnsupported,
		reflect.Slice:         decodeSlice,
		reflect.String:        decodeString,
		reflect.Struct:        decodeUnsupported,
		reflect.UnsafePointer: decodeUnsupported,
	}

	// Global map of struct field specs that is populated once for every new
	// struct type that is scanned. This caches the field types and the corresponding
	// decoder functions to avoid iterating through struct fields on subsequent scans.
	globalStructMap = newStructMap()
)

func Struct(dst interface{}) (StructValue, error) {
	v := reflect.ValueOf(dst)

	// The dstination to scan into should be a struct pointer.
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return StructValue{}, fmt.Errorf("redis.Scan(non-pointer %T)", dst)
	}

	v = v.Elem()
	if v.Kind() != reflect.Struct {
		return StructValue{}, fmt.Errorf("redis.Scan(non-struct %T)", dst)
	}

	return StructValue{
		spec:  globalStructMap.get(v.Type()),
		value: v,
	}, nil
}

// Scan scans the results from a key-value Redis map result set to a destination struct.
// The Redis keys are matched to the struct's field with the `redis` tag.
func Scan(dst interface{}, keys []interface{}, vals []interface{}) error {
	if len(keys) != len(vals) {
		return errors.New("args should have the same number of keys and vals")
	}

	strct, err := Struct(dst)
	if err != nil {
		return err
	}

	// Iterate through the (key, value) sequence.
	for i := 0; i < len(vals); i++ {
		key, ok := keys[i].(string)
		if !ok {
			continue
		}

		val, ok := vals[i].(string)
		if !ok {
			continue
		}

		if err := strct.Scan(key, val); err != nil {
			return err
		}
	}

	return nil
}

func decodeBool(f reflect.Value, s string) bool {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return false
	}
	f.SetBool(b)
	return true
}

func decodeInt(f reflect.Value, s string) bool {
	v, err := strconv.ParseInt(s, 10, 64)
	if err != nil || f.OverflowInt(v) {
		return false
	}
	f.SetInt(v)
	return true
}

func decodeUint(f reflect.Value, s string) bool {
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil || f.OverflowUint(v) {
		return false
	}
	f.SetUint(v)
	return true
}

// although the default is float64, but we better define it.
func decodeFloat(f reflect.Value, s string) bool {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil || f.OverflowFloat(v) {
		return false
	}
	f.SetFloat(v)
	return true
}

func decodeString(f reflect.Value, s string) bool {
	f.SetString(s)
	return true
}

func decodeSlice(f reflect.Value, s string) bool {
	// []byte slice ([]uint8).
	if f.Type().Elem().Kind() == reflect.Uint8 {
		f.SetBytes([]byte(s))
	}
	return true
}

func decodeUnsupported(v reflect.Value, s string) bool {
	panic(fmt.Sprintf("redis.Scan(unsupported %s)", v.Type()))
}
