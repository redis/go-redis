// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package bufio2

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/jay-wlj/redis/utils/assert"
)

func newReader(n int, input string) *Reader {
	return &Reader{rd: strings.NewReader(input), buf: make([]byte, n)}
}

func TestRead(t *testing.T) {
	var b bytes.Buffer
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&b, "hello world %d", i)
	}
	var input = b.String()
	for n := 1; n < len(input); n++ {
		r := newReader(n, input)
		b := make([]byte, len(input))
		_, err := io.ReadFull(r, b)
		assert.MustNoError(err)
		assert.Must(string(b) == input)
	}
}

func TestReadByte(t *testing.T) {
	var input = "hello world"
	for n := 1; n < len(input); n++ {
		r := newReader(n, input)
		var s string
		for i := 0; i < len(input); i++ {
			b1, err := r.PeekByte()
			assert.MustNoError(err)
			b2, err := r.ReadByte()
			assert.MustNoError(err)
			assert.Must(b1 == b2)
			s += string(b1)
		}
		assert.Must(s == input)
	}
}

func TestReadBytes(t *testing.T) {
	var b bytes.Buffer
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&b, "hello world %d ", i)
	}
	var input = b.String()
	for n := 1; n < len(input); n++ {
		r := newReader(n, input)
		var s string
		for i := 0; i < 30; i++ {
			b, err := r.ReadBytes(' ')
			assert.MustNoError(err)
			s += string(b)
		}
		assert.Must(s == input)
	}
}

func TestReadFull(t *testing.T) {
	var b bytes.Buffer
	for i := 0; i < 10; i++ {
		fmt.Fprintf(&b, "hello world %d ", i)
	}
	var input = b.String()
	for n := 1; n < len(input); n++ {
		r := newReader(n, input)
		b, err := r.ReadFull(len(input))
		assert.MustNoError(err)
		assert.Must(string(b) == input)
	}
}

func newWriter(n int, b *bytes.Buffer) *Writer {
	return &Writer{wr: b, buf: make([]byte, n)}
}

func TestWrite(t *testing.T) {
	for n := 1; n < 20; n++ {
		var input string
		var b bytes.Buffer
		var w = newWriter(n, &b)
		for i := 0; i < 10; i++ {
			s := fmt.Sprintf("hello world %d", i)
			_, err := w.Write([]byte(s))
			assert.MustNoError(err)
			input += s
		}
		assert.MustNoError(w.Flush())
		assert.Must(b.String() == input)
	}
}

func TestWriteBytes(t *testing.T) {
	var input = "hello world!!"
	for n := 1; n < 20; n++ {
		var b bytes.Buffer
		var w = newWriter(n, &b)
		for i := 0; i < len(input); i++ {
			assert.MustNoError(w.WriteByte(input[i]))
		}
		assert.MustNoError(w.Flush())
		assert.Must(b.String() == input)
	}
}

func TestWriteString(t *testing.T) {
	for n := 1; n < 20; n++ {
		var input string
		var b bytes.Buffer
		var w = newWriter(n, &b)
		for i := 0; i < 10; i++ {
			s := fmt.Sprintf("hello world %d", i)
			_, err := w.WriteString(s)
			assert.MustNoError(err)
			input += s
		}
		assert.MustNoError(w.Flush())
		assert.Must(b.String() == input)
	}
}
