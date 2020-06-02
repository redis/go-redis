// Copyright 2020 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package proto

import "fmt"

type RespType byte

const (
	TypeString    RespType = '+'
	TypeError     RespType = '-'
	TypeInt       RespType = ':'
	TypeBulkBytes RespType = '$'
	TypeArray     RespType = '*'
)

func (t RespType) String() string {
	switch t {
	case TypeString:
		return "<string>"
	case TypeError:
		return "<error>"
	case TypeInt:
		return "<int>"
	case TypeBulkBytes:
		return "<bulkbytes>"
	case TypeArray:
		return "<array>"
	default:
		return fmt.Sprintf("<unknown-0x%02x>", byte(t))
	}
}

type Resp struct {
	Type RespType

	Value []byte
	Array []*Resp
}

func (r *Resp) IsString() bool {
	return r.Type == TypeString
}

func (r *Resp) IsError() bool {
	return r.Type == TypeError
}

func (r *Resp) IsInt() bool {
	return r.Type == TypeInt
}

func (r *Resp) IsBulkBytes() bool {
	return r.Type == TypeBulkBytes
}

func (r *Resp) IsArray() bool {
	return r.Type == TypeArray
}

func NewString(value []byte) *Resp {
	r := &Resp{}
	r.Type = TypeString
	r.Value = value
	return r
}

func NewError(value []byte) *Resp {
	r := &Resp{}
	r.Type = TypeError
	r.Value = value
	return r
}

func NewErrorf(format string, args ...interface{}) *Resp {
	return NewError([]byte(fmt.Sprintf(format, args...)))
}

func NewInt(value []byte) *Resp {
	r := &Resp{}
	r.Type = TypeInt
	r.Value = value
	return r
}

func NewBulkBytes(value []byte) *Resp {
	r := &Resp{}
	r.Type = TypeBulkBytes
	r.Value = value
	return r
}

func NewArray(array []*Resp) *Resp {
	r := &Resp{}
	r.Type = TypeArray
	r.Array = array
	return r
}
