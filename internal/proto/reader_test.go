package proto_test

import (
	"bytes"
	"io"
	"testing"

	"github.com/go-redis/redis/v8/internal/proto"
)

func BenchmarkReader_ParseReply_Status(b *testing.B) {
	benchmarkParseReply(b, "+OK\r\n", nil, false)
}

func BenchmarkReader_ParseReply_Int(b *testing.B) {
	benchmarkParseReply(b, ":1\r\n", nil, false)
}

func BenchmarkReader_ParseReply_Float(b *testing.B) {
	benchmarkParseReply(b, ",123.456\r\n", nil, false)
}

func BenchmarkReader_ParseReply_Bool(b *testing.B) {
	benchmarkParseReply(b, "#t\r\n", nil, false)
}

func BenchmarkReader_ParseReply_BigInt(b *testing.B) {
	benchmarkParseReply(b, "(3492890328409238509324850943850943825024385\r\n", nil, false)
}

func BenchmarkReader_ParseReply_Error(b *testing.B) {
	benchmarkParseReply(b, "-Error message\r\n", nil, true)
}

func BenchmarkReader_ParseReply_Nil(b *testing.B) {
	benchmarkParseReply(b, "_\r\n", nil, true)
}

func BenchmarkReader_ParseReply_BlobError(b *testing.B) {
	benchmarkParseReply(b, "!21\r\nSYNTAX invalid syntax", nil, true)
}

func BenchmarkReader_ParseReply_String(b *testing.B) {
	benchmarkParseReply(b, "$5\r\nhello\r\n", nil, false)
}

func BenchmarkReader_ParseReply_Verb(b *testing.B) {
	benchmarkParseReply(b, "$9\r\ntxt:hello\r\n", nil, false)
}

func BenchmarkReader_ParseReply_Slice(b *testing.B) {
	benchmarkParseReply(b, "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", aggregateBulkParse, false)
}

func BenchmarkReader_ParseReply_Set(b *testing.B) {
	benchmarkParseReply(b, "~2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", aggregateBulkParse, false)
}

func BenchmarkReader_ParseReply_Push(b *testing.B) {
	benchmarkParseReply(b, ">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", aggregateBulkParse, false)
}

func BenchmarkReader_ParseReply_Map(b *testing.B) {
	benchmarkParseReply(b, "%2\r\n$5\r\nhello\r\n$5\r\nworld\r\n+key\r\n+value\r\n", aggregateBulkParse, false)
}

func BenchmarkReader_ParseReply_Attr(b *testing.B) {
	benchmarkParseReply(b, "%1\r\n+key\r\n+value\r\n+hello\r\n", aggregateBulkParse, false)
}

func TestReader_ReadLine(t *testing.T) {
	original := bytes.Repeat([]byte("a"), 8192)
	original[len(original)-2] = '\r'
	original[len(original)-1] = '\n'
	r := proto.NewReader(bytes.NewReader(original))
	read, err := r.ReadLine()
	if err != nil && err != io.EOF {
		t.Errorf("Should be able to read the full buffer: %v", err)
	}

	if bytes.Compare(read, original[:len(original)-2]) != 0 {
		t.Errorf("Values must be equal: %d expected %d", len(read), len(original[:len(original)-2]))
	}
}

func benchmarkParseReply(b *testing.B, reply string, m proto.AggregateBulkParse, wanterr bool) {
	buf := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		buf.WriteString(reply)
	}
	p := proto.NewReader(buf)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := p.ReadReply(m)
		if !wanterr && err != nil {
			b.Fatal(err)
		}
	}
}

func aggregateBulkParse(p *proto.Reader, typ byte, n int64) (interface{}, error) {
	vv := make([]interface{}, 0, n)
	for i := int64(0); i < n; i++ {
		v, err := p.ReadReply(aggregateBulkParse)
		if err != nil {
			return nil, err
		}
		vv = append(vv, v)
	}
	return vv, nil
}
