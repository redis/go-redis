package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Req interface {
	Args() []string
	ParseReply(reader) (interface{}, error)
	SetErr(error)
	Err() error
	SetVal(interface{})
	IfaceVal() interface{}

	writeTimeout() *time.Duration
	readTimeout() *time.Duration
}

//------------------------------------------------------------------------------

type baseReq struct {
	args []string

	val interface{}
	err error

	_writeTimeout, _readTimeout *time.Duration
}

func newBaseReq(args ...string) *baseReq {
	return &baseReq{
		args: args,
	}
}

func (r *baseReq) Args() []string {
	return r.args
}

func (r *baseReq) SetErr(err error) {
	if err == nil {
		panic("non-nil value expected")
	}
	r.err = err
}

func (r *baseReq) Err() error {
	if r.err != nil {
		return r.err
	}
	if r.val == nil {
		return errValNotSet
	}
	return nil
}

func (r *baseReq) SetVal(val interface{}) {
	if val == nil {
		panic("non-nil value expected")
	}
	r.val = val
}

func (r *baseReq) IfaceVal() interface{} {
	return r.val
}

func (r *baseReq) ParseReply(rd reader) (interface{}, error) {
	return parseReply(rd)
}

func (r *baseReq) String() string {
	args := strings.Join(r.args, " ")
	if r.err != nil {
		return args + ": " + r.err.Error()
	} else if r.val != nil {
		return args + ": " + fmt.Sprint(r.val)
	}
	return args
}

func (r *baseReq) readTimeout() *time.Duration {
	return r._readTimeout
}

func (r *baseReq) setReadTimeout(d time.Duration) {
	r._readTimeout = &d
}

func (r *baseReq) writeTimeout() *time.Duration {
	return r._writeTimeout
}

func (r *baseReq) setWriteTimeout(d time.Duration) {
	r._writeTimeout = &d
}

//------------------------------------------------------------------------------

type IfaceReq struct {
	*baseReq
}

func NewIfaceReq(args ...string) *IfaceReq {
	return &IfaceReq{
		baseReq: newBaseReq(args...),
	}
}

func (r *IfaceReq) Val() interface{} {
	return r.val
}

//------------------------------------------------------------------------------

type StatusReq struct {
	*baseReq
}

func NewStatusReq(args ...string) *StatusReq {
	return &StatusReq{
		baseReq: newBaseReq(args...),
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
	*baseReq
}

func NewIntReq(args ...string) *IntReq {
	return &IntReq{
		baseReq: newBaseReq(args...),
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
	*baseReq
}

func NewBoolReq(args ...string) *BoolReq {
	return &BoolReq{
		baseReq: newBaseReq(args...),
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
	*baseReq
}

func NewStringReq(args ...string) *StringReq {
	return &StringReq{
		baseReq: newBaseReq(args...),
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
	*baseReq
}

func NewFloatReq(args ...string) *FloatReq {
	return &FloatReq{
		baseReq: newBaseReq(args...),
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
	*baseReq
}

func NewIfaceSliceReq(args ...string) *IfaceSliceReq {
	return &IfaceSliceReq{
		baseReq: newBaseReq(args...),
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
	*baseReq
}

func NewStringSliceReq(args ...string) *StringSliceReq {
	return &StringSliceReq{
		baseReq: newBaseReq(args...),
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
	*baseReq
}

func NewBoolSliceReq(args ...string) *BoolSliceReq {
	return &BoolSliceReq{
		baseReq: newBaseReq(args...),
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
	*baseReq
}

func NewStringStringMapReq(args ...string) *StringStringMapReq {
	return &StringStringMapReq{
		baseReq: newBaseReq(args...),
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
	*baseReq
}

func NewStringFloatMapReq(args ...string) *StringFloatMapReq {
	return &StringFloatMapReq{
		baseReq: newBaseReq(args...),
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
