package hscan

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
)

// decoderFunc represents decoding functions for default built-in types.
type decoderFunc func(reflect.Value, string) error

var (
	// List of built-in decoders indexed by their numeric constant values (eg: reflect.Bool = 1)
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
	structSpecs = newStructMap()
)

// Scan scans the results from a key-value Redis map result set ([]interface{})
// to a destination struct. The Redis keys are matched to the struct's field
// with the `redis` tag.
func Scan(vals []interface{}, dest interface{}) error {
	if len(vals)%2 != 0 {
		return errors.New("args should have an even number of items (key-val)")
	}

	// The destination to scan into should be a struct pointer.
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Ptr || v.IsNil() {
		return fmt.Errorf("redis.Scan(non-pointer %T)", dest)
	}
	v = v.Elem()

	if v.Kind() != reflect.Struct {
		return fmt.Errorf("redis.Scan(non-struct %T)", dest)
	}

	// If the struct field spec is not cached, build and cache it to avoid
	// iterating through the fields of a struct type every time values are
	// scanned into it.
	typ := v.Type()
	fMap := structSpecs.get(typ)

	// Iterate through the (key, value) sequence.
	for i := 0; i < len(vals); i += 2 {
		key, ok := vals[i].(string)
		if !ok {
			continue
		}

		val, ok := vals[i+1].(string)
		if !ok {
			continue
		}

		// Check if the field name is in the field spec map.
		field, ok := fMap.get(key)
		if !ok {
			continue
		}

		if err := field.fn(v.Field(field.index), val); err != nil {
			return err
		}
	}

	return nil
}

func decodeBool(f reflect.Value, s string) error {
	b, err := strconv.ParseBool(s)
	if err != nil {
		return err
	}
	f.SetBool(b)
	return nil
}

func decodeInt(f reflect.Value, s string) error {
	v, err := strconv.ParseInt(s, 10, 0)
	if err != nil {
		return err
	}
	f.SetInt(v)
	return nil
}

func decodeUint(f reflect.Value, s string) error {
	v, err := strconv.ParseUint(s, 10, 0)
	if err != nil {
		return err
	}
	f.SetUint(v)
	return nil
}

func decodeFloat(f reflect.Value, s string) error {
	v, err := strconv.ParseFloat(s, 0)
	if err != nil {
		return err
	}
	f.SetFloat(v)
	return nil
}

func decodeString(f reflect.Value, s string) error {
	f.SetString(s)
	return nil
}

func decodeSlice(f reflect.Value, s string) error {
	// []byte slice ([]uint8).
	if f.Type().Elem().Kind() == reflect.Uint8 {
		f.SetBytes([]byte(s))
	}
	return nil
}

func decodeUnsupported(v reflect.Value, s string) error {
	return fmt.Errorf("redis.Scan(unsupported %s)", v.Type())
}
