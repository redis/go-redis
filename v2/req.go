package redis

import (
	"fmt"
	"strconv"
	"strings"
)

type Req interface {
	Args() []string
	ParseReply(reader) (interface{}, error)
	SetErr(error)
	Err() error
	SetVal(interface{})
	IfaceVal() interface{}
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

func (r *BaseReq) IfaceVal() interface{} {
	return r.val
}

func (r *BaseReq) ParseReply(rd reader) (interface{}, error) {
	return parseReply(rd)
}

func (r *BaseReq) String() string {
	args := strings.Join(r.args, " ")
	if r.err != nil {
		return args + ": " + r.err.Error()
	} else if r.val != nil {
		return args + ": " + fmt.Sprint(r.val)
	}
	return args
}

//------------------------------------------------------------------------------

type IfaceReq struct {
	*BaseReq
}

func NewIfaceReq(args ...string) *IfaceReq {
	return &IfaceReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *IfaceReq) Val() interface{} {
	return r.val
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

func (r *BoolReq) ParseReply(rd reader) (interface{}, error) {
	v, err := parseReply(rd)
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

type StringReq struct {
	*BaseReq
}

func NewStringReq(args ...string) *StringReq {
	return &StringReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *StringReq) Val() string {
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

func (r *FloatReq) ParseReply(rd reader) (interface{}, error) {
	v, err := parseReply(rd)
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

type IfaceSliceReq struct {
	*BaseReq
}

func NewIfaceSliceReq(args ...string) *IfaceSliceReq {
	return &IfaceSliceReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *IfaceSliceReq) Val() []interface{} {
	if r.val == nil {
		return nil
	}
	return r.val.([]interface{})
}

//------------------------------------------------------------------------------

type StringSliceReq struct {
	*BaseReq
}

func NewStringSliceReq(args ...string) *StringSliceReq {
	return &StringSliceReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *StringSliceReq) ParseReply(rd reader) (interface{}, error) {
	return parseStringSliceReply(rd)
}

func (r *StringSliceReq) Val() []string {
	if r.val == nil {
		return nil
	}
	return r.val.([]string)
}

//------------------------------------------------------------------------------

type BoolSliceReq struct {
	*BaseReq
}

func NewBoolSliceReq(args ...string) *BoolSliceReq {
	return &BoolSliceReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *BoolSliceReq) ParseReply(rd reader) (interface{}, error) {
	return parseBoolSliceReply(rd)
}

func (r *BoolSliceReq) Val() []bool {
	if r.val == nil {
		return nil
	}
	return r.val.([]bool)
}

//------------------------------------------------------------------------------

type StringStringMapReq struct {
	*BaseReq
}

func NewStringStringMapReq(args ...string) *StringStringMapReq {
	return &StringStringMapReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *StringStringMapReq) ParseReply(rd reader) (interface{}, error) {
	return parseStringStringMapReply(rd)
}

func (r *StringStringMapReq) Val() map[string]string {
	if r.val == nil {
		return nil
	}
	return r.val.(map[string]string)
}

//------------------------------------------------------------------------------

type StringFloatMapReq struct {
	*BaseReq
}

func NewStringFloatMapReq(args ...string) *StringFloatMapReq {
	return &StringFloatMapReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *StringFloatMapReq) ParseReply(rd reader) (interface{}, error) {
	return parseStringFloatMapReply(rd)
}

func (r *StringFloatMapReq) Val() map[string]float64 {
	if r.val == nil {
		return nil
	}
	return r.val.(map[string]float64)
}
