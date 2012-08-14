package redis

import (
	"strconv"
)

type Req interface {
	Args() []string
	ParseReply(ReadLiner) (interface{}, error)
	SetErr(error)
	Err() error
	SetVal(interface{})
	InterfaceVal() interface{}
}

//------------------------------------------------------------------------------

type BaseReq struct {
	args []string

	val interface{}
	err error
}

func NewBaseReq(args ...string) *BaseReq {
	return &BaseReq{
		args: args,
	}
}

func (r *BaseReq) Args() []string {
	return r.args
}

func (r *BaseReq) SetErr(err error) {
	if err == nil {
		panic("non-nil value expected")
	}
	r.err = err
}

func (r *BaseReq) Err() error {
	if r.err != nil {
		return r.err
	}
	if r.val == nil {
		return errValNotSet
	}
	return nil
}

func (r *BaseReq) SetVal(val interface{}) {
	if val == nil {
		panic("non-nil value expected")
	}
	r.val = val
}

func (r *BaseReq) InterfaceVal() interface{} {
	return r.val
}

func (r *BaseReq) ParseReply(rd ReadLiner) (interface{}, error) {
	return ParseReply(rd)
}

//------------------------------------------------------------------------------

type StatusReq struct {
	*BaseReq
}

func NewStatusReq(args ...string) *StatusReq {
	return &StatusReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *StatusReq) Val() string {
	if r.val == nil {
		return ""
	}
	return r.val.(string)
}

//------------------------------------------------------------------------------

type IntReq struct {
	*BaseReq
}

func NewIntReq(args ...string) *IntReq {
	return &IntReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *IntReq) Val() int64 {
	if r.val == nil {
		return 0
	}
	return r.val.(int64)
}

//------------------------------------------------------------------------------

type BoolReq struct {
	*BaseReq
}

func NewBoolReq(args ...string) *BoolReq {
	return &BoolReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *BoolReq) ParseReply(rd ReadLiner) (interface{}, error) {
	v, err := ParseReply(rd)
	if err != nil {
		return nil, err
	}
	return v.(int64) == 1, nil
}

func (r *BoolReq) Val() bool {
	if r.val == nil {
		return false
	}
	return r.val.(bool)
}

//------------------------------------------------------------------------------

type BulkReq struct {
	*BaseReq
}

func NewBulkReq(args ...string) *BulkReq {
	return &BulkReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *BulkReq) Val() string {
	if r.val == nil {
		return ""
	}
	return r.val.(string)
}

//------------------------------------------------------------------------------

type FloatReq struct {
	*BaseReq
}

func NewFloatReq(args ...string) *FloatReq {
	return &FloatReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *FloatReq) ParseReply(rd ReadLiner) (interface{}, error) {
	v, err := ParseReply(rd)
	if err != nil {
		return nil, err
	}
	return strconv.ParseFloat(v.(string), 64)
}

func (r *FloatReq) Val() float64 {
	if r.val == nil {
		return 0
	}
	return r.val.(float64)
}

//------------------------------------------------------------------------------

type MultiBulkReq struct {
	*BaseReq
}

func NewMultiBulkReq(args ...string) *MultiBulkReq {
	return &MultiBulkReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *MultiBulkReq) Val() []interface{} {
	if r.val == nil {
		return nil
	}
	return r.val.([]interface{})
}
