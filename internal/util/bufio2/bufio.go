// Copyright 2016 CodisLabs. All Rights Reserved.
// Licensed under the MIT (MIT-LICENSE.txt) license.

package bufio2

import (
	"bufio"
	"bytes"
	"io"
)

const DefaultBufferSize = 1024

type Reader struct {
	err error
	buf []byte

	rd   io.Reader
	rpos int
	wpos int

	slice sliceAlloc
}

func NewReader(rd io.Reader) *Reader {
	return NewReaderSize(rd, DefaultBufferSize)
}

func NewReaderSize(rd io.Reader, size int) *Reader {
	if size <= 0 {
		size = DefaultBufferSize
	}
	return &Reader{rd: rd, buf: make([]byte, size)}
}

func NewReaderBuffer(rd io.Reader, buf []byte) *Reader {
	if len(buf) == 0 {
		buf = make([]byte, DefaultBufferSize)
	}
	return &Reader{rd: rd, buf: buf}
}

func (b *Reader) fill() error {
	if b.err != nil {
		return b.err
	}
	if b.rpos > 0 {
		n := copy(b.buf, b.buf[b.rpos:b.wpos])
		b.rpos = 0
		b.wpos = n
	}
	n, err := b.rd.Read(b.buf[b.wpos:])
	if err != nil {
		b.err = err
	} else if n == 0 {
		b.err = io.ErrNoProgress
	} else {
		b.wpos += n
	}
	return b.err
}

func (b *Reader) buffered() int {
	return b.wpos - b.rpos
}

func (b *Reader) Read(p []byte) (int, error) {
	if b.err != nil || len(p) == 0 {
		return 0, b.err
	}
	if b.buffered() == 0 {
		if len(p) >= len(b.buf) {
			n, err := b.rd.Read(p)
			if err != nil {
				b.err = err
			}
			return n, b.err
		}
		if b.fill() != nil {
			return 0, b.err
		}
	}
	n := copy(p, b.buf[b.rpos:b.wpos])
	b.rpos += n
	return n, nil
}

func (b *Reader) ReadByte() (byte, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.buffered() == 0 {
		if b.fill() != nil {
			return 0, b.err
		}
	}
	c := b.buf[b.rpos]
	b.rpos += 1
	return c, nil
}

func (b *Reader) PeekByte() (byte, error) {
	if b.err != nil {
		return 0, b.err
	}
	if b.buffered() == 0 {
		if b.fill() != nil {
			return 0, b.err
		}
	}
	c := b.buf[b.rpos]
	return c, nil
}

func (b *Reader) ReadSlice(delim byte) ([]byte, error) {
	if b.err != nil {
		return nil, b.err
	}
	for {
		var index = bytes.IndexByte(b.buf[b.rpos:b.wpos], delim)
		if index >= 0 {
			limit := b.rpos + index + 1
			slice := b.buf[b.rpos:limit]
			b.rpos = limit
			return slice, nil
		}
		if b.buffered() == len(b.buf) {
			b.rpos = b.wpos
			return b.buf, bufio.ErrBufferFull
		}
		if b.fill() != nil {
			return nil, b.err
		}
	}
}

func (b *Reader) ReadBytes(delim byte) ([]byte, error) {
	var full [][]byte
	var last []byte
	var size int
	for last == nil {
		f, err := b.ReadSlice(delim)
		if err != nil {
			if err != bufio.ErrBufferFull {
				return nil, b.err
			}
			dup := b.slice.Make(len(f))
			copy(dup, f)
			full = append(full, dup)
		} else {
			last = f
		}
		size += len(f)
	}
	var n int
	var buf = b.slice.Make(size)
	for _, frag := range full {
		n += copy(buf[n:], frag)
	}
	copy(buf[n:], last)
	return buf, nil
}

func (b *Reader) ReadFull(n int) ([]byte, error) {
	if b.err != nil || n == 0 {
		return nil, b.err
	}
	var buf = b.slice.Make(n)
	if _, err := io.ReadFull(b, buf); err != nil {
		return nil, err
	}
	return buf, nil
}

type Writer struct {
	err error
	buf []byte

	wr   io.Writer
	wpos int
}

func NewWriter(wr io.Writer) *Writer {
	return NewWriterSize(wr, DefaultBufferSize)
}

func NewWriterSize(wr io.Writer, size int) *Writer {
	if size <= 0 {
		size = DefaultBufferSize
	}
	return &Writer{wr: wr, buf: make([]byte, size)}
}

func NewWriterBuffer(wr io.Writer, buf []byte) *Writer {
	if len(buf) == 0 {
		buf = make([]byte, DefaultBufferSize)
	}
	return &Writer{wr: wr, buf: buf}
}

func (b *Writer) Flush() error {
	return b.flush()
}

func (b *Writer) flush() error {
	if b.err != nil {
		return b.err
	}
	if b.wpos == 0 {
		return nil
	}
	n, err := b.wr.Write(b.buf[:b.wpos])
	if err != nil {
		b.err = err
	} else if n < b.wpos {
		b.err = io.ErrShortWrite
	} else {
		b.wpos = 0
	}
	return b.err
}

func (b *Writer) available() int {
	return len(b.buf) - b.wpos
}

func (b *Writer) Write(p []byte) (nn int, err error) {
	for b.err == nil && len(p) > b.available() {
		var n int
		if b.wpos == 0 {
			n, b.err = b.wr.Write(p)
		} else {
			n = copy(b.buf[b.wpos:], p)
			b.wpos += n
			b.flush()
		}
		nn, p = nn+n, p[n:]
	}
	if b.err != nil || len(p) == 0 {
		return nn, b.err
	}
	n := copy(b.buf[b.wpos:], p)
	b.wpos += n
	return nn + n, nil
}

func (b *Writer) WriteByte(c byte) error {
	if b.err != nil {
		return b.err
	}
	if b.available() == 0 && b.flush() != nil {
		return b.err
	}
	b.buf[b.wpos] = c
	b.wpos += 1
	return nil
}

func (b *Writer) WriteString(s string) (nn int, err error) {
	for b.err == nil && len(s) > b.available() {
		n := copy(b.buf[b.wpos:], s)
		b.wpos += n
		b.flush()
		nn, s = nn+n, s[n:]
	}
	if b.err != nil || len(s) == 0 {
		return nn, b.err
	}
	n := copy(b.buf[b.wpos:], s)
	b.wpos += n
	return nn + n, nil
}
