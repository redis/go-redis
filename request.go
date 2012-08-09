package redis

import (
	"bufio"
	"errors"
	"fmt"
	"strconv"
)

var Nil = errors.New("(nil)")

var errResultMissing = errors.New("Request was not run properly.")

//------------------------------------------------------------------------------

func isNil(line []byte) bool {
	return len(line) == 3 && line[0] == '$' && line[1] == '-' && line[2] == '1'
}

func isEmpty(line []byte) bool {
	return len(line) == 2 && line[0] == '$' && line[1] == '0'
}

func isNoReplies(line []byte) bool {
	return len(line) >= 2 && line[1] == '*' && line[1] == '0'
}

//------------------------------------------------------------------------------

func ParseReq(rd *bufio.Reader) ([]string, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	if line[0] != '*' {
		return []string{string(line)}, nil
	}
	numReplies, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		return nil, err
	}

	args := make([]string, 0)
	for i := int64(0); i < numReplies; i++ {
		line, err = readLine(rd)
		if err != nil {
			return nil, err
		}
		if line[0] != '$' {
			buf, _ := rd.Peek(rd.Buffered())
			return nil, fmt.Errorf("Expected '$', but got %q of %q.", line, buf)
		}

		line, err = readLine(rd)
		if err != nil {
			return nil, err
		}
		args = append(args, string(line))
	}
	return args, nil
}

//------------------------------------------------------------------------------

func PackReq(args []string) []byte {
	buf := make([]byte, 0, 1024)
	buf = append(buf, '*')
	buf = strconv.AppendUint(buf, uint64(len(args)), 10)
	buf = append(buf, '\r', '\n')
	for _, arg := range args {
		buf = append(buf, '$')
		buf = strconv.AppendUint(buf, uint64(len(arg)), 10)
		buf = append(buf, '\r', '\n')
		buf = append(buf, []byte(arg)...)
		buf = append(buf, '\r', '\n')
	}
	return buf
}

//------------------------------------------------------------------------------

type Req interface {
	Req() []byte
	ParseReply(*bufio.Reader) (interface{}, error)
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

func (r *BaseReq) Req() []byte {
	return PackReq(r.args)
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
		return errResultMissing
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

func (r *BaseReq) ParseReply(rd *bufio.Reader) (interface{}, error) {
	panic("abstract")
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

func (r *StatusReq) ParseReply(rd *bufio.Reader) (interface{}, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != '+' {
		buf, _ := rd.Peek(rd.Buffered())
		return nil, fmt.Errorf("Expected '+', but got %q of %q.", line, buf)
	}

	return string(line[1:]), nil
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

func (r *IntReq) ParseReply(rd *bufio.Reader) (interface{}, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != ':' {
		buf, _ := rd.Peek(rd.Buffered())
		return nil, fmt.Errorf("Expected ':', but got %q of %q.", line, buf)
	}

	return strconv.ParseInt(string(line[1:]), 10, 64)
}

func (r *IntReq) Val() int64 {
	if r.val == nil {
		return 0
	}
	return r.val.(int64)
}

//------------------------------------------------------------------------------

type IntNilReq struct {
	*BaseReq
}

func NewIntNilReq(args ...string) *IntNilReq {
	return &IntNilReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *IntNilReq) ParseReply(rd *bufio.Reader) (interface{}, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] == ':' {
		return strconv.ParseInt(string(line[1:]), 10, 64)
	} else if isNil(line) {
		return nil, Nil
	}

	buf, _ := rd.Peek(rd.Buffered())
	return nil, fmt.Errorf("Expected ':', but got %q of %q.", line, buf)
}

func (r *IntNilReq) Val() int64 {
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

func (r *BoolReq) ParseReply(rd *bufio.Reader) (interface{}, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != ':' {
		buf, _ := rd.Peek(rd.Buffered())
		return nil, fmt.Errorf("Expected ':', but got %q of %q.", line, buf)
	}

	return line[1] == '1', nil
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

func (r *BulkReq) ParseReply(rd *bufio.Reader) (interface{}, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != '$' {
		buf, _ := rd.Peek(rd.Buffered())
		return nil, fmt.Errorf("Expected '$', but got %q of %q.", line, buf)
	}

	if isNil(line) {
		return nil, Nil
	}

	line, err = readLine(rd)
	if err != nil {
		return nil, err
	}

	return string(line), nil
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

func (r *FloatReq) ParseReply(rd *bufio.Reader) (interface{}, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != '$' {
		buf, _ := rd.Peek(rd.Buffered())
		return nil, fmt.Errorf("Expected '$', but got %q of %q.", line, buf)
	}

	if isNil(line) {
		return nil, Nil
	}

	line, err = readLine(rd)
	if err != nil {
		return nil, err
	}

	return strconv.ParseFloat(string(line), 64)
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

func (r *MultiBulkReq) ParseReply(rd *bufio.Reader) (interface{}, error) {
	line, err := readLine(rd)
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != '*' {
		buf, _ := rd.Peek(rd.Buffered())
		return nil, fmt.Errorf("Expected '*', but got line %q of %q.", line, buf)
	} else if isNil(line) {
		return nil, Nil
	}

	val := make([]interface{}, 0)
	if isNoReplies(line) {
		return val, nil
	}
	numReplies, err := strconv.ParseInt(string(line[1:]), 10, 64)
	if err != nil {
		return nil, err
	}

	for i := int64(0); i < numReplies; i++ {
		line, err = readLine(rd)
		if err != nil {
			return nil, err
		}

		if line[0] == ':' {
			var n int64
			n, err = strconv.ParseInt(string(line[1:]), 10, 64)
			if err != nil {
				return nil, err
			}
			val = append(val, n)
		} else if line[0] == '$' {
			if isEmpty(line) {
				val = append(val, "")
			} else if isNil(line) {
				val = append(val, nil)
			} else {
				line, err = readLine(rd)
				if err != nil {
					return nil, err
				}
				val = append(val, string(line))
			}
		} else {
			buf, _ := rd.Peek(rd.Buffered())
			return nil, fmt.Errorf("Expected '$', but got line %q of %q.", line, buf)
		}
	}

	return val, nil
}

func (r *MultiBulkReq) Val() []interface{} {
	if r.val == nil {
		return nil
	}
	return r.val.([]interface{})
}
