package proto

import (
	"encoding"
	"fmt"
	"reflect"

	"gopkg.in/redis.v5/internal"
)

func Scan(b []byte, v interface{}) error {
	switch v := v.(type) {
	case nil:
		return internal.RedisError("redis: Scan(nil)")
	case *string:
		*v = internal.BytesToString(b)
		return nil
	case *[]byte:
		*v = b
		return nil
	case *int:
		var err error
		*v, err = atoi(b)
		return err
	case *int8:
		n, err := parseInt(b, 10, 8)
		if err != nil {
			return err
		}
		*v = int8(n)
		return nil
	case *int16:
		n, err := parseInt(b, 10, 16)
		if err != nil {
			return err
		}
		*v = int16(n)
		return nil
	case *int32:
		n, err := parseInt(b, 10, 32)
		if err != nil {
			return err
		}
		*v = int32(n)
		return nil
	case *int64:
		n, err := parseInt(b, 10, 64)
		if err != nil {
			return err
		}
		*v = n
		return nil
	case *uint:
		n, err := parseUint(b, 10, 64)
		if err != nil {
			return err
		}
		*v = uint(n)
		return nil
	case *uint8:
		n, err := parseUint(b, 10, 8)
		if err != nil {
			return err
		}
		*v = uint8(n)
		return nil
	case *uint16:
		n, err := parseUint(b, 10, 16)
		if err != nil {
			return err
		}
		*v = uint16(n)
		return nil
	case *uint32:
		n, err := parseUint(b, 10, 32)
		if err != nil {
			return err
		}
		*v = uint32(n)
		return nil
	case *uint64:
		n, err := parseUint(b, 10, 64)
		if err != nil {
			return err
		}
		*v = n
		return nil
	case *float32:
		n, err := parseFloat(b, 32)
		if err != nil {
			return err
		}
		*v = float32(n)
		return err
	case *float64:
		var err error
		*v, err = parseFloat(b, 64)
		return err
	case *bool:
		*v = len(b) == 1 && b[0] == '1'
		return nil
	case encoding.BinaryUnmarshaler:
		return v.UnmarshalBinary(b)
	default:
		return fmt.Errorf(
			"redis: can't unmarshal %T (consider implementing BinaryUnmarshaler)", v)
	}
}

// Scan a string slice into a custom container
// Example:
// var container []YourStruct; ScanSlice([]string{""},&container)
// var container []*YourStruct; ScanSlice([]string{""},&container)
func ScanSlice(sSlice []string, container interface{}) error {
	val := reflect.ValueOf(container)

	if !val.IsValid() {
		return fmt.Errorf("redis: ScanSlice(nil)")
	}

	// Check the if the container is pointer
	if val.Kind() != reflect.Ptr {
		return fmt.Errorf("redis: ScanSlice(non-pointer %T)", container)
	}

	// Is a valid value
	val = val.Elem()
	if !val.IsValid() {
		return fmt.Errorf("redis: ScanSlice(non-pointer %T)", container)
	}

	// if the container is slice
	if val.Kind() != reflect.Slice {
		return fmt.Errorf("redis: Wrong object type `%T` for ScanSlice(), need *[]*Type or *[]Type", container)
	}

	elemType := val.Type().Elem()
	if elemType.Kind() == reflect.Ptr {
		// is container of ptr
		for index, s := range sSlice {
			elem := internal.SliceNextElem(val)
			if err := Scan([]byte(s), elem.Addr().Interface()); err != nil {
				return fmt.Errorf("redis: ScanSlice failed at index of %d => %s, %s", index, s, err.Error())
			}
		}
	} else {
		// is container of object
		for index, s := range sSlice {
			elem := internal.SliceNextElem(val)
			if err := Scan([]byte(s), elem.Addr().Interface()); err != nil {
				return fmt.Errorf("redis: ScanSlice failed at index of %d => %s, %s", index, s, err.Error())
			}
		}
	}
	return nil
}
