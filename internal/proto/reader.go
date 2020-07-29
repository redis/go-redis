package proto

import (
	"bufio"
	"fmt"
	"io"

	"github.com/go-redis/redis/v8/internal/util"
)

/**
状态响应（status reply）的第一个字节是 "+"
+OK
错误响应（error reply）的第一个字节是 "-"
-Error Msg
整数响应（integer reply）的第一个字节是 ":"
:10
主体响应（bulk reply）的第一个字节是 "$"
例如：
$3
abc
批量主体响应（multi bulk reply）的第一个字节是 "*"
*2
$2
ab
$3
abc
*/

const (
	ErrorReply  = '-'
	StatusReply = '+'
	IntReply    = ':'
	StringReply = '$'
	ArrayReply  = '*'
)

//------------------------------------------------------------------------------

const Nil = RedisError("redis: nil")

type RedisError string

func (e RedisError) Error() string { return string(e) }

func (RedisError) RedisError() {}

//------------------------------------------------------------------------------
//便于调用方对多命令进行处理，该方法内进行递归调用
type MultiBulkParse func(*Reader, int64) (interface{}, error)

type Reader struct {
	rd   *bufio.Reader
	_buf []byte
}

//实例化一个Reader结构体
func NewReader(rd io.Reader) *Reader {
	return &Reader{
		rd:   bufio.NewReader(rd),
		_buf: make([]byte, 64),
	}
}

//获取buffer长度
func (r *Reader) Buffered() int {
	return r.rd.Buffered()
}

//获取n个字节
func (r *Reader) Peek(n int) ([]byte, error) {
	return r.rd.Peek(n)
}

func (r *Reader) Reset(rd io.Reader) {
	r.rd.Reset(rd)
}

//读取一行记录
func (r *Reader) ReadLine() ([]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if isNilReply(line) {
		return nil, Nil
	}
	return line, nil
}

// readLine that returns an error if:
//   - there is a pending read error;
//   - or line does not end with \r\n.
//获取单行记录  使用\n判断
func (r *Reader) readLine() ([]byte, error) {
	b, err := r.rd.ReadSlice('\n')
	if err != nil {
		return nil, err
	}
	if len(b) <= 2 || b[len(b)-1] != '\n' || b[len(b)-2] != '\r' {
		return nil, fmt.Errorf("redis: invalid reply: %q", b)
	}
	b = b[:len(b)-2]
	return b, nil
}

//读取redis协议批量响应返回值的入口方法
func (r *Reader) ReadReply(m MultiBulkParse) (interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}

	//确定响应类型
	switch line[0] {
	case ErrorReply: //错误响应
		return nil, ParseErrorReply(line)
	case StatusReply: //状态响应
		return util.BytesToString(line[1:]), nil //adday by still 0729 高并发下无需重复申请和释放内存
		//return string(line[1:]), nil
	case IntReply: //整数响应
		return util.ParseInt(line[1:], 10, 64)
	case StringReply: //主体响应
		return r.readStringReply(line)
	case ArrayReply: //批量响应
		n, err := parseArrayLen(line)
		if err != nil {
			return nil, err
		}
		if m == nil {
			err := fmt.Errorf("redis: got %.100q, but multi bulk parser is nil", line)
			return nil, err
		}
		return m(r, n)
	}
	return nil, fmt.Errorf("redis: can't parse %.100q", line)
}

//读取int类型返回
func (r *Reader) ReadIntReply() (int64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case ErrorReply:
		return 0, ParseErrorReply(line)
	case IntReply:
		return util.ParseInt(line[1:], 10, 64)
	default:
		return 0, fmt.Errorf("redis: can't parse int reply: %.100q", line)
	}
}

//读取string类型返回
func (r *Reader) ReadString() (string, error) {
	line, err := r.ReadLine() //先读取单行
	if err != nil {
		return "", err
	}
	//判断首字符
	switch line[0] {
	case ErrorReply:
		return "", ParseErrorReply(line)
	case StringReply:
		return r.readStringReply(line) //string类型的需要单独处理 相应数据是多行
	case StatusReply:
		return string(line[1:]), nil
	case IntReply:
		return string(line[1:]), nil
	default:
		return "", fmt.Errorf("redis: can't parse reply=%.100q reading string", line)
	}
}

//读取string单行之后的处理
func (r *Reader) readStringReply(line []byte) (string, error) {
	if isNilReply(line) {
		return "", Nil
	}
	//获取string类型的第一行 字符串长度
	replyLen, err := util.Atoi(line[1:])
	if err != nil {
		return "", err
	}

	b := make([]byte, replyLen+2)

	//读取剩余全量数据
	_, err = io.ReadFull(r.rd, b)
	if err != nil {
		return "", err
	}

	//截取\r\n提取真实返回的数据
	return util.BytesToString(b[:replyLen]), nil
}

func (r *Reader) ReadArrayReply(m MultiBulkParse) (interface{}, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case ErrorReply:
		return nil, ParseErrorReply(line)
	case ArrayReply:
		n, err := parseArrayLen(line)
		if err != nil {
			return nil, err
		}
		return m(r, n)
	default:
		return nil, fmt.Errorf("redis: can't parse array reply: %.100q", line)
	}
}

func (r *Reader) ReadArrayLen() (int64, error) {
	line, err := r.ReadLine()
	if err != nil {
		return 0, err
	}
	switch line[0] {
	case ErrorReply:
		return 0, ParseErrorReply(line)
	case ArrayReply:
		return parseArrayLen(line)
	default:
		return 0, fmt.Errorf("redis: can't parse array reply: %.100q", line)
	}
}

func (r *Reader) ReadScanReply() ([]string, uint64, error) {
	n, err := r.ReadArrayLen()
	if err != nil {
		return nil, 0, err
	}
	if n != 2 {
		return nil, 0, fmt.Errorf("redis: got %d elements in scan reply, expected 2", n)
	}

	cursor, err := r.ReadUint()
	if err != nil {
		return nil, 0, err
	}

	n, err = r.ReadArrayLen()
	if err != nil {
		return nil, 0, err
	}

	keys := make([]string, n)
	for i := int64(0); i < n; i++ {
		key, err := r.ReadString()
		if err != nil {
			return nil, 0, err
		}
		keys[i] = key
	}

	return keys, cursor, err
}

func (r *Reader) ReadInt() (int64, error) {
	b, err := r.readTmpBytesReply()
	if err != nil {
		return 0, err
	}
	return util.ParseInt(b, 10, 64)
}

func (r *Reader) ReadUint() (uint64, error) {
	b, err := r.readTmpBytesReply()
	if err != nil {
		return 0, err
	}
	return util.ParseUint(b, 10, 64)
}

func (r *Reader) ReadFloatReply() (float64, error) {
	b, err := r.readTmpBytesReply()
	if err != nil {
		return 0, err
	}
	return util.ParseFloat(b, 64)
}

func (r *Reader) readTmpBytesReply() ([]byte, error) {
	line, err := r.ReadLine()
	if err != nil {
		return nil, err
	}
	switch line[0] {
	case ErrorReply:
		return nil, ParseErrorReply(line)
	case StringReply:
		return r._readTmpBytesReply(line)
	case StatusReply:
		return line[1:], nil
	default:
		return nil, fmt.Errorf("redis: can't parse string reply: %.100q", line)
	}
}

func (r *Reader) _readTmpBytesReply(line []byte) ([]byte, error) {
	if isNilReply(line) {
		return nil, Nil
	}

	replyLen, err := util.Atoi(line[1:])
	if err != nil {
		return nil, err
	}

	buf := r.buf(replyLen + 2)
	_, err = io.ReadFull(r.rd, buf)
	if err != nil {
		return nil, err
	}

	return buf[:replyLen], nil
}

func (r *Reader) buf(n int) []byte {
	if n <= cap(r._buf) {
		return r._buf[:n]
	}
	d := n - cap(r._buf)
	r._buf = append(r._buf, make([]byte, d)...)
	return r._buf
}

func isNilReply(b []byte) bool {
	return len(b) == 3 &&
		(b[0] == StringReply || b[0] == ArrayReply) &&
		b[1] == '-' && b[2] == '1'
}

func ParseErrorReply(line []byte) error {
	return RedisError(string(line[1:]))
}

//获取长度
func parseArrayLen(line []byte) (int64, error) {
	if isNilReply(line) {
		return 0, Nil
	}
	return util.ParseInt(line[1:], 10, 64)
}
