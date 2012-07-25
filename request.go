package redis

import (
	"errors"
	"fmt"
	"io"
	"strconv"

	"github.com/togoio/redisgoproxy/bufreader"
)

var Nil = errors.New("(nil)")

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
	ParseReply(*bufreader.Reader)
	SetErr(error)
	Err() error
}

//------------------------------------------------------------------------------

type BaseReq struct {
	args []string
	err  error
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
	r.err = err
}

func (r *BaseReq) Err() error {
	return r.err
}

//------------------------------------------------------------------------------

type StatusReq struct {
	*BaseReq
	val string
}

func NewStatusReq(args ...string) *StatusReq {
	return &StatusReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *StatusReq) ParseReply(rd *bufreader.Reader) {
	var line []byte

	line, r.err = rd.ReadLine('\n')
	if r.err != nil {
		return
	}

	if line[0] == '-' {
		r.err = errors.New(string(line[1:]))
		return
	} else if line[0] != '+' {
		r.err = fmt.Errorf("Expected '+', but got %q of %q.", line, rd.Bytes())
		return
	}

	r.val = string(line[1:])
}

func (r *StatusReq) Reply() (string, error) {
	return r.val, r.err
}

//------------------------------------------------------------------------------

type IntReq struct {
	*BaseReq
	val int64
}

func NewIntReq(args ...string) *IntReq {
	return &IntReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *IntReq) ParseReply(rd *bufreader.Reader) {
	var line []byte

	line, r.err = rd.ReadLine('\n')
	if r.err != nil {
		return
	}

	if line[0] == '-' {
		r.err = errors.New(string(line[1:]))
		return
	} else if line[0] != ':' {
		r.err = fmt.Errorf("Expected ':', but got %q of %q.", line, rd.Bytes())
		return
	}

	r.val, r.err = strconv.ParseInt(string(line[1:]), 10, 64)
}

func (r *IntReq) Reply() (int64, error) {
	return r.val, r.err
}

//------------------------------------------------------------------------------

type BoolReq struct {
	*BaseReq
	val bool
}

func NewBoolReq(args ...string) *BoolReq {
	return &BoolReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *BoolReq) ParseReply(rd *bufreader.Reader) {
	var line []byte

	line, r.err = rd.ReadLine('\n')
	if r.err != nil {
		return
	}

	if line[0] == '-' {
		r.err = errors.New(string(line[1:]))
		return
	} else if line[0] != ':' {
		r.err = fmt.Errorf("Expected ':', but got %q of %q.", line, rd.Bytes())
		return
	}

	r.val = line[1] == '1'
}

func (r *BoolReq) Reply() (bool, error) {
	return r.val, r.err
}

//------------------------------------------------------------------------------

type BulkReq struct {
	*BaseReq
	val string
}

func NewBulkReq(args ...string) *BulkReq {
	return &BulkReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *BulkReq) ParseReply(rd *bufreader.Reader) {
	var line []byte

	line, r.err = rd.ReadLine('\n')
	if r.err != nil {
		return
	}

	if line[0] == '-' {
		r.err = errors.New(string(line[1:]))
		return
	} else if line[0] != '$' {
		r.err = fmt.Errorf("Expected '$', but got %q of %q.", line, rd.Bytes())
		return
	}

	if len(line) >= 3 && line[1] == '-' && line[2] == '1' {
		r.err = Nil
		return
	}

	line, r.err = rd.ReadLine('\n')
	if r.err != nil {
		return
	}

	r.val = string(line)
}

func (r *BulkReq) Reply() (string, error) {
	return r.val, r.err
}

//------------------------------------------------------------------------------

type MultiBulkReq struct {
	*BaseReq
	val []interface{}
}

func NewMultiBulkReq(args ...string) *MultiBulkReq {
	return &MultiBulkReq{
		BaseReq: NewBaseReq(args...),
	}
}

func (r *MultiBulkReq) ParseReply(rd *bufreader.Reader) {
	var line []byte

	line, r.err = rd.ReadLine('\n')
	if r.err != nil {
		return
	}

	if line[0] == '-' {
		r.err = errors.New(string(line[1:]))
		return
	} else if line[0] != '*' {
		r.err = fmt.Errorf("Expected '*', but got line %q of %q.", line, rd.Bytes())
		return
	}

	val := make([]interface{}, 0)

	if len(line) >= 2 && line[1] == '0' {
		r.val = val
		return
	} else if len(line) >= 3 && line[1] == '-' && line[2] == '1' {
		r.err = Nil
		return
	}

	line, r.err = rd.ReadLine('\n')
	if r.err != nil {
		return
	}
	for {
		if line[0] == ':' {
			var n int64
			n, r.err = strconv.ParseInt(string(line[1:]), 10, 64)
			if r.err != nil {
				return
			}
			val = append(val, n)
		} else if line[0] == '$' {
			if len(line) >= 2 && line[1] == '0' {
				val = append(val, "")
			} else if len(line) >= 3 && line[1] == '-' && line[2] == '1' {
				val = append(val, nil)
			} else {
				line, r.err = rd.ReadLine('\n')
				if r.err != nil {
					return
				}
				val = append(val, string(line))
			}
		} else {
			r.err = fmt.Errorf("Expected '$', but got line %q of %q.", line, rd.Bytes())
			return
		}

		line, r.err = rd.ReadLine('\n')
		if r.err == io.EOF {
			r.err = nil
			break
		}
		// Check for start of another reply.
		if line[0] == '*' {
			rd.UnreadLine('\n')
			break
		}
	}

	r.val = val
}

func (r *MultiBulkReq) Reply() ([]interface{}, error) {
	return r.val, r.err
}
