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
	errInt   = errors.New("redis return value is not int")
	errFloat = errors.New("redis return value is not float")
	errBool  = errors.New("redis return value is not bool")
	errVerb  = errors.New("redis return value is not verb")
	errMap   = errors.New("redis return value is not map")
	errSlice = errors.New("redis return value is not array")
	errAttr  = errors.New("redis return value is not attr")
	errStr   = errors.New("redis return value cannot be converted to string")
)

// func (v *Value) isSimpleType() bool {
//	switch v.Typ {
//	case ReplyStatus, ReplyString, ReplyInteger,
//		ReplyFloat, ReplyBool, ReplyVerb, ReplyBigInt:
//		return true
//	}
//	return false
// }

// Convert the result set to int, there may be overflow problems.
func (v *Value) Int64() (int64, error) {
	if v.RedisError != nil {
		return 0, v.RedisError
	}
	switch v.Typ {
	case ReplyInteger:
		return v.Integer, nil
	case ReplyBigInt:
		return v.BigInt.Int64(), nil
	case ReplyString, ReplyStatus, ReplyVerb:
		return strconv.ParseInt(v.Str, 10, 64)
	}

	return 0, errInt
}

func (v *Value) Float64() (float64, error) {
	if v.RedisError != nil {
		return 0, v.RedisError
	}
	switch v.Typ {
	case ReplyFloat:
		return v.Float, nil
	case ReplyString, ReplyStatus, ReplyVerb:
		return strconv.ParseFloat(v.Str, 64)
	}

	return 0, errFloat
}

func (v *Value) Bool() (bool, error) {
	if v.RedisError != nil {
		return false, v.RedisError
	}
	switch v.Typ {
	case ReplyBool:
		return v.Boolean, nil
	case ReplyInteger:
		return v.Integer != 0, nil
	case ReplyString, ReplyStatus, ReplyVerb:
		return strconv.ParseBool(v.Str)
	}

	return false, errBool
}

// The Value can be converted to the type of string, try to use string to represent.
func (v *Value) String() (string, error) {
	if v.RedisError != nil {
		return "", v.RedisError
	}

	switch v.Typ {
	case ReplyStatus, ReplyString:
		return v.Str, nil
	case ReplyInteger:
		return strconv.FormatInt(v.Integer, 10), nil
	case ReplyFloat:
		return strconv.FormatFloat(v.Float, 'g', -1, 64), nil
	case ReplyBool:
		if v.Boolean {
			return "true", nil
		}
		return "false", nil
	case ReplyVerb:
		return fmt.Sprintf("%s:%s", v.StrFmt, v.Str), nil
	case ReplyBigInt:
		return v.BigInt.String(), nil
	}

	return "", errStr
}

func (v *Value) FmtString() (format string, str string, err error) {
	if v.RedisError != nil {
		return "", "", v.RedisError
	}
	if v.Typ == ReplyVerb {
		return v.StrFmt, v.Str, nil
	}

	return "", "", errVerb
}

// Try to convert the result set to map[string]string,
// which requires the result set to be a simple k-v structure.
// If it is a complex map, it needs to be converted in the outer layer.
func (v *Value) MapStringString() (map[string]string, error) {
	if v.RedisError != nil {
		return nil, v.RedisError
	}

	m := make(map[string]string)
	switch v.Typ {
	case ReplyMap:
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
	case ReplyArray, ReplySet, ReplyPush:
		n := len(v.Slice)
		if n == 0 {
			return m, nil
		}
		if n%2 == 0 {
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
	case ReplyArray, ReplySet, ReplyPush:
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

func (v *Value) SliceInterface() ([]interface{}, error) {
	if v.RedisError != nil {
		return nil, v.RedisError
	}

	switch v.Typ {
	case ReplyArray, ReplySet, ReplyPush:
		si := make([]interface{}, 0, len(v.Slice))
		for _, val := range v.Slice {
			si = append(si, val.Interface())
		}
		return si, nil
	}

	return nil, errSlice
}

func (v *Value) AttrInterface() interface{} {
	if v.RedisError != nil {
		return v.RedisError
	}

	if v.Typ == ReplyAttr {
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
	case ReplyStatus, ReplyString:
		return v.Str
	case ReplyInteger:
		return v.Integer
	case ReplyFloat:
		return v.Float
	case ReplyBool:
		return v.Boolean
	case ReplyVerb:
		return fmt.Sprintf("%s:%s", v.StrFmt, v.Str)
	case ReplyBigInt:
		return v.BigInt.String()

	case ReplyArray, ReplySet, ReplyPush:
		slice := make([]interface{}, 0, len(v.Slice))
		for _, val := range v.Slice {
			slice = append(slice, val.Interface())
		}
		return slice
	case ReplyMap:
		m := make(map[interface{}]interface{})
		for key, val := range v.Map {
			m[key.Interface()] = val.Interface()
		}
		return m
	case ReplyAttr:
		return v.Attribute.Value.Interface()
	}

	// Value.Typ are all valid types and will not come here.
	return nil
}
