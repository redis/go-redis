package redis

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/vmihailenco/bufreader"
)

var Nil = errors.New("(nil)")

var errResultMissing = errors.New("Request was not run properly.")

//------------------------------------------------------------------------------

func isNil(buf []byte) bool {
	return len(buf) == 3 && buf[0] == '$' && buf[1] == '-' && buf[2] == '1'
}

func isEmpty(buf []byte) bool {
	return len(buf) == 2 && buf[0] == '$' && buf[1] == '0'
}

//------------------------------------------------------------------------------

func ParseReq(rd *bufreader.Reader) ([]string, error) {
	line, err := rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}
	if line[0] != '*' {
		return []string{string(line)}, nil
	}

	args := make([]string, 0)
	for {
		line, err = rd.ReadLine('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}
		if line[0] != '$' {
			return nil, fmt.Errorf("Expected '$', but got %q of %q.", line, rd.Bytes())
		}

		line, err = rd.ReadLine('\n')
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
	ParseReply(*bufreader.Reader) (interface{}, error)
	SetErr(error)
	Err() error
	SetVal(interface{})
	Val() interface{}
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
	return r.err
}

func (r *BaseReq) SetVal(val interface{}) {
	if val == nil {
		panic("non-nil value expected")
	}
	r.val = val
}

func (r *BaseReq) Val() interface{} {
	return r.val
}

func (r *BaseReq) ParseReply(rd *bufreader.Reader) (interface{}, error) {
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

func (r *StatusReq) ParseReply(rd *bufreader.Reader) (interface{}, error) {
	line, err := rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != '+' {
		return nil, fmt.Errorf("Expected '+', but got %q of %q.", line, rd.Bytes())
	}

	return string(line[1:]), nil
}

func (r *StatusReq) Reply() (string, error) {
	if r.val == nil && r.err == nil {
		return "", errResultMissing
	} else if r.err != nil {
		return "", r.err
	}
	return r.val.(string), nil
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

func (r *IntReq) ParseReply(rd *bufreader.Reader) (interface{}, error) {
	line, err := rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != ':' {
		return nil, fmt.Errorf("Expected ':', but got %q of %q.", line, rd.Bytes())
	}

	return strconv.ParseInt(string(line[1:]), 10, 64)
}

func (r *IntReq) Reply() (int64, error) {
	if r.val == nil && r.err == nil {
		return 0, errResultMissing
	} else if r.err != nil {
		return 0, r.err
	}
	return r.val.(int64), nil
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

func (r *IntNilReq) ParseReply(rd *bufreader.Reader) (interface{}, error) {
	line, err := rd.ReadLine('\n')
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

	return nil, fmt.Errorf("Expected ':', but got %q of %q.", line, rd.Bytes())
}

func (r *IntNilReq) Reply() (int64, error) {
	if r.val == nil && r.err == nil {
		return 0, errResultMissing
	} else if r.err != nil {
		return 0, r.err
	}
	return r.val.(int64), nil
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

func (r *BoolReq) ParseReply(rd *bufreader.Reader) (interface{}, error) {
	line, err := rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != ':' {
		return nil, fmt.Errorf("Expected ':', but got %q of %q.", line, rd.Bytes())
	}

	return line[1] == '1', nil
}

func (r *BoolReq) Reply() (bool, error) {
	if r.val == nil && r.err == nil {
		return false, errResultMissing
	} else if r.err != nil {
		return false, r.err
	}
	return r.val.(bool), nil
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

func (r *BulkReq) ParseReply(rd *bufreader.Reader) (interface{}, error) {
	line, err := rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != '$' {
		return nil, fmt.Errorf("Expected '$', but got %q of %q.", line, rd.Bytes())
	}

	if isNil(line) {
		return nil, Nil
	}

	line, err = rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}

	return string(line), nil
}

func (r *BulkReq) Reply() (string, error) {
	if r.val == nil && r.err == nil {
		return "", errResultMissing
	} else if r.err != nil {
		return "", r.err
	}
	return r.val.(string), nil
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

func (r *FloatReq) ParseReply(rd *bufreader.Reader) (interface{}, error) {
	line, err := rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != '$' {
		return nil, fmt.Errorf("Expected '$', but got %q of %q.", line, rd.Bytes())
	}

	if isNil(line) {
		return nil, Nil
	}

	line, err = rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}

	return strconv.ParseFloat(string(line), 64)
}

func (r *FloatReq) Reply() (float64, error) {
	if r.val == nil && r.err == nil {
		return 0, errResultMissing
	} else if r.err != nil {
		return 0, r.err
	}
	return r.val.(float64), nil
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

func (r *MultiBulkReq) ParseReply(rd *bufreader.Reader) (interface{}, error) {
	line, err := rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}

	if line[0] == '-' {
		return nil, errors.New(string(line[1:]))
	} else if line[0] != '*' {
		return nil, fmt.Errorf("Expected '*', but got line %q of %q.", line, rd.Bytes())
	}

	val := make([]interface{}, 0)

	if len(line) >= 2 && line[1] == '0' {
		return val, nil
	} else if isNil(line) {
		return nil, Nil
	}

	line, err = rd.ReadLine('\n')
	if err != nil {
		return nil, err
	}
	for {
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
				line, err = rd.ReadLine('\n')
				if err != nil {
					return nil, err
				}
				val = append(val, string(line))
			}
		} else {
			return nil, fmt.Errorf("Expected '$', but got line %q of %q.", line, rd.Bytes())
		}

		line, err = rd.ReadLine('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		// Check for the header of another reply.
		if line[0] == '*' {
			rd.UnreadLine('\n')
			break
		}
	}

	return val, nil
}

func (r *MultiBulkReq) Reply() ([]interface{}, error) {
	if r.val == nil && r.err == nil {
		return nil, errResultMissing
	} else if r.err != nil {
		return nil, r.err
	}
	return r.val.([]interface{}), nil
}
