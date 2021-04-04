package proto

import (
	"errors"
	"fmt"
	"math/big"
	"strconv"
)

type Value struct {
	Typ       byte
	Str       string
	StrFmt    string
	Integer   int64
	Float     float64
	BigInt    *big.Int
	Slice     []*Value
	Map       map[*Value]*Value
	Attribute *AttributeType
	Boolean   bool

	RedisError error
}

type AttributeType struct {
	Attr  map[*Value]*Value
	Value *Value
}

var (
	errInt    = errors.New("redis return value is not int")
	errFloat  = errors.New("redis return value is not float")
	errBool   = errors.New("redis return value is not bool")
	errStatus = errors.New("redis return value is not int/bool/string")
	errVerb   = errors.New("redis return value is not verb")
	errMap    = errors.New("redis return value is not map")
	errSlice  = errors.New("redis return value is not array")
	errAttr   = errors.New("redis return value is not attr")
	errStr    = errors.New("redis return value cannot be converted to string")
)

func (v *Value) IsSimpleType() bool {
	switch v.Typ {
	case redisStatus, redisString, redisInteger,
		redisFloat, redisBool, redisVerb, redisBigInt:
		return true
	}
	return false
}

func (v *Value) IsSlice() bool {
	switch v.Typ {
	case redisArray, redisSet, redisPush:
		return true
	}
	return false
}

// Convert the result set to int, there may be overflow problems.
func (v *Value) Int64() (int64, error) {
	if v.RedisError != nil {
		return 0, v.RedisError
	}
	switch v.Typ {
	case redisInteger:
		return v.Integer, nil
	case redisBigInt:
		return v.BigInt.Int64(), nil
	case redisString, redisStatus, redisVerb:
		return strconv.ParseInt(v.Str, 10, 64)
	}

	return 0, errInt
}

func (v *Value) Float64() (float64, error) {
	if v.RedisError != nil {
		return 0, v.RedisError
	}
	switch v.Typ {
	case redisFloat:
		return v.Float, nil
	case redisString, redisStatus, redisVerb:
		return strconv.ParseFloat(v.Str, 64)
	}

	return 0, errFloat
}

func (v *Value) Bool() (bool, error) {
	if v.RedisError != nil {
		return false, v.RedisError
	}
	switch v.Typ {
	case redisBool:
		return v.Boolean, nil
	case redisInteger:
		return v.Integer != 0, nil
	case redisString, redisStatus, redisVerb:
		return strconv.ParseBool(v.Str)
	}

	return false, errBool
}

func (v *Value) Status() (bool, error) {
	switch v.RedisError {
	case Nil:
		return false, nil
	case nil:
	default:
		return false, v.RedisError
	}

	switch v.Typ {
	case redisBool:
		return v.Boolean, nil
	case redisInteger:
		return v.Integer == 1, nil
	case redisString, redisStatus, redisVerb:
		return v.Str == "OK", nil
	}

	return false, errStatus
}

// The Value can be converted to the type of string, try to use string to represent.
func (v *Value) String() (string, error) {
	if v.RedisError != nil {
		return "", v.RedisError
	}

	switch v.Typ {
	case redisStatus, redisString:
		return v.Str, nil
	case redisInteger:
		return strconv.FormatInt(v.Integer, 10), nil
	case redisFloat:
		return strconv.FormatFloat(v.Float, 'g', -1, 64), nil
	case redisBool:
		if v.Boolean {
			return "true", nil
		}
		return "false", nil
	case redisVerb:
		return fmt.Sprintf("%s:%s", v.StrFmt, v.Str), nil
	case redisBigInt:
		return v.BigInt.String(), nil
	}

	return "", errStr
}

func (v *Value) FmtString() (format string, str string, err error) {
	if v.RedisError != nil {
		return "", "", v.RedisError
	}
	if v.Typ == redisVerb {
		return v.StrFmt, v.Str, nil
	}

	return "", "", errVerb
}

// Try to convert the result set to map[string]interface,
// which requires the result set to be a simple k-v structure.
// If it is a complex map, it needs to be converted in the outer layer.
func (v *Value) MapStringString() (map[string]string, error) { //nolint:dupl
	if v.RedisError != nil {
		return nil, v.RedisError
	}

	m := make(map[string]string)
	switch v.Typ {
	case redisMap:
		for key, val := range v.Map {
			keyStr, err := key.String()
			if err != nil {
				return nil, err
			}

			// val is allowed to be redis.Nil.
			valStr, err := val.String()
			if err != nil && err != Nil {
				return nil, err
			}

			m[keyStr] = valStr
		}
		return m, nil
	case redisArray, redisSet, redisPush:
		n := len(v.Slice)
		if n == 0 {
			return m, nil
		}
		if n%2 != 0 {
			return nil, errors.New("the map requires the result set to be a multiple of 2")
		}
		for i := 0; i < n; i += 2 {
			keyStr, err := v.Slice[i].String()
			if err != nil {
				return nil, err
			}

			// val is allowed to be redis.Nil.
			valStr, err := v.Slice[i+1].String()
			if err != nil && err != Nil {
				return nil, err
			}

			m[keyStr] = valStr
		}

		return m, nil
	}

	return nil, errMap
}

func (v *Value) MapStringInt64() (map[string]int64, error) { //nolint:dupl
	if v.RedisError != nil {
		return nil, v.RedisError
	}

	m := make(map[string]int64)
	switch v.Typ {
	case redisMap:
		for key, val := range v.Map {
			keyStr, err := key.String()
			if err != nil {
				return nil, err
			}

			// val is allowed to be redis.Nil.
			valInt, err := val.Int64()
			if err != nil && err != Nil {
				return nil, err
			}

			m[keyStr] = valInt
		}
		return m, nil
	case redisArray, redisSet, redisPush:
		n := len(v.Slice)
		if n == 0 {
			return m, nil
		}
		if n%2 != 0 {
			return nil, errors.New("the map requires the result set to be a multiple of 2")
		}
		for i := 0; i < n; i += 2 {
			keyStr, err := v.Slice[i].String()
			if err != nil {
				return nil, err
			}

			// val is allowed to be redis.Nil.
			valInt, err := v.Slice[i+1].Int64()
			if err != nil && err != Nil {
				return nil, err
			}

			m[keyStr] = valInt
		}

		return m, nil
	}

	return nil, errMap
}

// Convert the result set to []string, which requires the result set to be a simple array,
// if it is a complex array, it needs to be converted in the outer layer.
func (v *Value) SliceString() ([]string, error) {
	if v.RedisError != nil {
		return nil, v.RedisError
	}

	switch v.Typ {
	case redisArray, redisSet, redisPush:
		ss := make([]string, 0, len(v.Slice))
		for _, val := range v.Slice {
			// val is allowed to be redis.Nil.
			tmp, err := val.String()
			if err != nil && err != Nil {
				return nil, err
			}
			ss = append(ss, tmp)
		}
		return ss, nil
	}

	return nil, errSlice
}

func (v *Value) SliceInt64() ([]int64, error) {
	if v.RedisError != nil {
		return nil, v.RedisError
	}

	switch v.Typ {
	case redisArray, redisSet, redisPush:
		si := make([]int64, 0, len(v.Slice))
		for _, val := range v.Slice {
			i, err := val.Int64()
			if err != nil && err != Nil {
				return nil, err
			}
			si = append(si, i)
		}
		return si, nil
	}

	return nil, errSlice
}

func (v *Value) SliceFloat64() ([]float64, error) {
	if v.RedisError != nil {
		return nil, v.RedisError
	}

	switch v.Typ {
	case redisArray, redisSet, redisPush:
		sf := make([]float64, 0, len(v.Slice))
		for _, val := range v.Slice {
			i, err := val.Float64()
			if err != nil && err != Nil {
				return nil, err
			}
			sf = append(sf, i)
		}
		return sf, nil
	}

	return nil, errSlice
}

func (v *Value) SliceStatus() ([]bool, error) {
	if v.RedisError != nil {
		return nil, v.RedisError
	}

	switch v.Typ {
	case redisArray, redisSet, redisPush:
		sb := make([]bool, 0, len(v.Slice))
		for _, val := range v.Slice {
			b, err := val.Status()
			if err != nil && err != Nil {
				return nil, err
			}
			sb = append(sb, b)
		}
		return sb, nil
	}

	return nil, errSlice
}

func (v *Value) SliceInterface() ([]interface{}, error) {
	if v.RedisError != nil {
		return nil, v.RedisError
	}

	switch v.Typ {
	case redisArray, redisSet, redisPush:
		si := make([]interface{}, 0, len(v.Slice))
		for _, val := range v.Slice {
			iv := val.Interface()
			if iv == Nil {
				iv = nil
			}
			si = append(si, iv)
		}
		return si, nil
	}

	return nil, errSlice
}

func (v *Value) AttrInterface() interface{} {
	if v.RedisError != nil {
		return v.RedisError
	}

	if v.Typ == redisAttr {
		m := make(map[interface{}]interface{})
		for key, val := range v.Attribute.Attr {
			m[key.Interface()] = val.Interface()
		}
		return m
	}

	return errAttr
}

func (v *Value) Interface() interface{} {
	if v.RedisError != nil {
		return v.RedisError
	}

	switch v.Typ {
	case redisStatus, redisString:
		return v.Str
	case redisInteger:
		return v.Integer
	case redisFloat:
		return v.Float
	case redisBool:
		return v.Boolean
	case redisVerb:
		return fmt.Sprintf("%s:%s", v.StrFmt, v.Str)
	case redisBigInt:
		return v.BigInt.String()

	case redisArray, redisSet, redisPush:
		slice := make([]interface{}, 0, len(v.Slice))
		for _, val := range v.Slice {
			slice = append(slice, val.Interface())
		}
		return slice
	case redisMap:
		m := make(map[interface{}]interface{})
		for key, val := range v.Map {
			m[key.Interface()] = val.Interface()
		}
		return m
	case redisAttr:
		return v.Attribute.Value.Interface()
	}

	// Value.Typ are all valid types and will not come here.
	return nil
}

func (v *Value) SliceScan(fn func(item *Value) error) error {
	if v.RedisError != nil {
		return v.RedisError
	}

	switch v.Typ {
	case redisArray, redisSet, redisPush:
		for _, item := range v.Slice {
			if err := fn(item); err != nil {
				return err
			}
		}
		return nil
	}

	return errSlice
}

func (v *Value) MapScan(fn func(key, item *Value) error) error {
	if v.RedisError != nil {
		return v.RedisError
	}

	if v.Typ == redisMap {
		for key, item := range v.Map {
			if err := fn(key, item); err != nil {
				return err
			}
		}
		return nil
	}

	return errMap
}

func (v *Value) MapLen() (int, error) {
	if v.RedisError != nil {
		return 0, v.RedisError
	}

	if v.Typ == redisMap {
		return len(v.Map), nil
	}

	return 0, errMap
}

func (v *Value) SliceLen() (int, error) {
	if v.RedisError != nil {
		return 0, v.RedisError
	}
	switch v.Typ {
	case redisArray, redisSet, redisPush:
		return len(v.Slice), nil
	}

	return 0, errSlice
}
