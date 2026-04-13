package proto_test

import (
	"bytes"
	"fmt"
	"io"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func BenchmarkReader_ParseReply_Status(b *testing.B) {
	benchmarkParseReply(b, "+OK\r\n", false)
}

func BenchmarkReader_ParseReply_Int(b *testing.B) {
	benchmarkParseReply(b, ":1\r\n", false)
}

func BenchmarkReader_ParseReply_Error(b *testing.B) {
	benchmarkParseReply(b, "-Error message\r\n", true)
}

func BenchmarkReader_ParseReply_String(b *testing.B) {
	benchmarkParseReply(b, "$5\r\nhello\r\n", false)
}

func BenchmarkReader_ParseReply_Bool(b *testing.B) {
	benchmarkParseReply(b, "#t\r\n", false)
}

func BenchmarkReader_ParseReply_Float(b *testing.B) {
	benchmarkParseReply(b, ",1.23\r\n", false)
}

func BenchmarkReader_ParseReply_BigInt(b *testing.B) {
	benchmarkParseReply(b, "(12345678901234567890\r\n", false)
}

func BenchmarkReader_ParseReply_Array(b *testing.B) {
	benchmarkParseReply(b, "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Map(b *testing.B) {
	benchmarkParseReply(b, "%1\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Set(b *testing.B) {
	benchmarkParseReply(b, "~2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Attr(b *testing.B) {
	benchmarkParseReply(b, "|1\r\n$5\r\nkey\r\n$5\r\nvalue\r\n+OK\r\n", false)
}

func BenchmarkReader_ParseReply_Push(b *testing.B) {
	benchmarkParseReply(b, ">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func benchmarkParseReply(b *testing.B, reply string, expectErr bool) {
	data := []byte(reply)
	rd := bytes.NewReader(data)
	r := proto.NewReader(rd)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rd.Reset(data)
		r.Reset(rd)
		_, err := r.ReadReply()
		if expectErr {
			if err == nil {
				b.Fatal("expect error")
			}
		} else {
			if err != nil {
				b.Fatal(err)
			}
		}
	}
}

func BenchmarkReader_ReadRawReply(b *testing.B) {
	data := []byte("*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n")
	rd := bytes.NewReader(data)
	r := proto.NewReader(rd)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rd.Reset(data)
		r.Reset(rd)
		_, err := r.ReadRawReply()
		if err != nil {
			b.Fatal(err)
		}
	}
}
