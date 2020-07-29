package proto

import (
	"encoding"
	"fmt"
	"io"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8/internal/util"
)

/**
redis 请求协议
以 set key value为例
*3 //表示一共有三个参数
$3 //表示set命令的字符串长度是3
set//表示set命令
$3 //表示key的字符串长度是3
key
$5
value
最终通过byte将请求发出
*/

type writer interface {
	io.Writer
	io.ByteWriter
	// io.StringWriter
	WriteString(s string) (n int, err error)
}

type Writer struct {
	writer //可以用实现了writer接口的类型来实例化Writer结构体

	lenBuf []byte
	numBuf []byte
}

/**
通过writer接口 实例化Writer结构体
*/
func NewWriter(wr writer) *Writer {
	return &Writer{
		writer: wr,

		lenBuf: make([]byte, 64),
		numBuf: make([]byte, 64),
	}
}

//发送redis命令的入口
func (w *Writer) WriteArgs(args []interface{}) error {
	//写入*开头
	if err := w.WriteByte(ArrayReply); err != nil {
		return err
	}

	//写入具体的参数长度
	if err := w.writeLen(len(args)); err != nil {
		return err
	}

	//遍历参数写入：$长度\r\n参数\r\n
	for _, arg := range args {
		if err := w.WriteArg(arg); err != nil {
			return err
		}
	}

	return nil
}

func (w *Writer) writeLen(n int) error {
	//将unit类型的n 转化为字符串 追加到w.lenBuf
	w.lenBuf = strconv.AppendUint(w.lenBuf[:0], uint64(n), 10)
	//追加\r \n
	w.lenBuf = append(w.lenBuf, '\r', '\n')
	//将n\r\n写入
	_, err := w.Write(w.lenBuf)
	return err
}

func (w *Writer) WriteArg(v interface{}) error {
	//不同类型单独处理 重点关注下string int即可
	switch v := v.(type) {
	case nil:
		return w.string("")
	case string:
		return w.string(v) //string
	case []byte:
		return w.bytes(v)
	case int:
		return w.int(int64(v)) //int
	case int8:
		return w.int(int64(v))
	case int16:
		return w.int(int64(v))
	case int32:
		return w.int(int64(v))
	case int64:
		return w.int(v)
	case uint:
		return w.uint(uint64(v))
	case uint8:
		return w.uint(uint64(v))
	case uint16:
		return w.uint(uint64(v))
	case uint32:
		return w.uint(uint64(v))
	case uint64:
		return w.uint(v)
	case float32:
		return w.float(float64(v))
	case float64:
		return w.float(v)
	case bool:
		if v {
			return w.int(1)
		}
		return w.int(0)
	case time.Time:
		w.numBuf = v.AppendFormat(w.numBuf[:0], time.RFC3339Nano)
		return w.bytes(w.numBuf)
	case encoding.BinaryMarshaler:
		b, err := v.MarshalBinary()
		if err != nil {
			return err
		}
		return w.bytes(b)
	default:
		return fmt.Errorf(
			"redis: can't marshal %T (implement encoding.BinaryMarshaler)", v)
	}
}

func (w *Writer) bytes(b []byte) error {
	//参数先写入$
	if err := w.WriteByte(StringReply); err != nil {
		return err
	}

	//写入参数长度
	if err := w.writeLen(len(b)); err != nil {
		return err
	}

	//写入具体的参数
	if _, err := w.Write(b); err != nil {
		return err
	}

	return w.crlf()
}

func (w *Writer) string(s string) error {
	//通过unsafe包将string转为[]byte 然后统一写入
	return w.bytes(util.StringToBytes(s))
}

func (w *Writer) uint(n uint64) error {
	w.numBuf = strconv.AppendUint(w.numBuf[:0], n, 10)
	return w.bytes(w.numBuf)
}

func (w *Writer) int(n int64) error {
	//将n转为string 追加到w.numBuf
	w.numBuf = strconv.AppendInt(w.numBuf[:0], n, 10)
	return w.bytes(w.numBuf)
}

func (w *Writer) float(f float64) error {
	w.numBuf = strconv.AppendFloat(w.numBuf[:0], f, 'f', -1, 64)
	return w.bytes(w.numBuf)
}

func (w *Writer) crlf() error {
	if err := w.WriteByte('\r'); err != nil {
		return err
	}
	return w.WriteByte('\n')
}
