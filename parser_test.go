package redis

import (
	"bufio"
	"bytes"
	"testing"
)

func BenchmarkParseReplyStatus(b *testing.B) {
	benchmarkParseReply(b, "+OK\r\n", nil, false)
}

func BenchmarkParseReplyInt(b *testing.B) {
	benchmarkParseReply(b, ":1\r\n", nil, false)
}

func BenchmarkParseReplyError(b *testing.B) {
	benchmarkParseReply(b, "-Error message\r\n", nil, true)
}

func BenchmarkParseReplyString(b *testing.B) {
	benchmarkParseReply(b, "$5\r\nhello\r\n", nil, false)
}

func BenchmarkParseReplySlice(b *testing.B) {
	benchmarkParseReply(b, "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", sliceParser, false)
}

func benchmarkParseReply(b *testing.B, reply string, p multiBulkParser, wanterr bool) {
	buf := &bytes.Buffer{}
	for i := 0; i < b.N; i++ {
		buf.WriteString(reply)
	}
	cn := &conn{
		rd:  bufio.NewReader(buf),
		buf: make([]byte, 0, defaultBufSize),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := readReply(cn, p)
		if !wanterr && err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAppendArgs(b *testing.B) {
	buf := make([]byte, 0, 64)
	args := []interface{}{"hello", "world", "foo", "bar"}
	for i := 0; i < b.N; i++ {
		appendArgs(buf, args)
	}
}
