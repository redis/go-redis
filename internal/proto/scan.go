package proto

import (
	"encoding"
	"fmt"
	"strconv"
	"reflect"
	
	"gopkg.in/redis.v5/internal"
)

func Scan(s string, v interface{}) error {
	switch v := v.(type) {
	case nil:
		return internal.RedisError("redis: Scan(nil)")
	case *string:
		*v = s
		return nil
	case *[]byte:
		*v = []byte(s)
		return nil
	case *int:
		var err error
		*v, err = strconv.Atoi(s)
		return err
	case *int8:
		n, err := strconv.ParseInt(s, 10, 8)
		if err != nil {
			return err
		}
		*v = int8(n)
		return nil
	case *int16:
		n, err := strconv.ParseInt(s, 10, 16)
		if err != nil {
			return err
		}
		*v = int16(n)
		return nil
	case *int32:
		n, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return err
		}
		*v = int32(n)
		return nil
	case *int64:
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return err
		}
		*v = n
		return nil
	case *uint:
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		*v = uint(n)
		return nil
	case *uint8:
		n, err := strconv.ParseUint(s, 10, 8)
		if err != nil {
			return err
		}
		*v = uint8(n)
		return nil
	case *uint16:
		n, err := strconv.ParseUint(s, 10, 16)
		if err != nil {
			return err
		}
		*v = uint16(n)
		return nil
	case *uint32:
		n, err := strconv.ParseUint(s, 10, 32)
		if err != nil {
			return err
		}
		*v = uint32(n)
		return nil
	case *uint64:
		n, err := strconv.ParseUint(s, 10, 64)
		if err != nil {
			return err
		}
		*v = n
		return nil
	case *float32:
		n, err := strconv.ParseFloat(s, 32)
		if err != nil {
			return err
		}
		*v = float32(n)
		return err
	case *float64:
		var err error
		*v, err = strconv.ParseFloat(s, 64)
		return err
	case *bool:
		*v = len(s) == 1 && s[0] == '1'
		return nil
	case encoding.BinaryUnmarshaler:
		return v.UnmarshalBinary([]byte(s))
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
	ind := reflect.Indirect(val)
	elemType := ind.Type().Elem()

	// Check the if the container is pointer to slice
	errTyp := true
	isElemPtr := true
	if val.Kind() == reflect.Ptr {
		if ind.Kind() == reflect.Slice {
			errTyp = false
			isElemPtr = elemType.Kind() == reflect.Ptr
		}
	}
	if errTyp {
		return fmt.Errorf("wrong object type `%s` for rows scan, need *[]*Type or *[]Type", val.Type())
	}

	slice := ind
	if isElemPtr {
		for index, s := range sSlice {
			elem := reflect.New(elemType)
			mind := reflect.Indirect(elem)

			if err := Scan(s, mind.Addr().Interface()); err != nil {
				return fmt.Errorf("scan slice failed at index of %d: %s", index, err.Error())
			}
			// If elem is ptr, append with addr
			slice = reflect.Append(slice, mind.Addr())
		}
	} else {
		for index, s := range sSlice {
			elem := reflect.New(elemType)
			mind := reflect.Indirect(elem)

			if err := Scan(s, mind.Addr().Interface()); err != nil {
				return fmt.Errorf("scan slice failed at index of %d: %s", index, err.Error())
			}
			// If elem is ptr, append with value
			slice = reflect.Append(slice, mind)
		}
	}
	if slice.Len() > 0 {
		ind.Set(slice)
	} else {
		// when a result is empty and container is nil
		// to set a empty container
		if ind.IsNil() {
			ind.Set(reflect.MakeSlice(ind.Type(), 0, 0))
		}
	}
	return nil
}

