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

func BenchmarkReader_ParseReply_Float(b *testing.B) {
	benchmarkParseReply(b, ",123.456\r\n", false)
}

func BenchmarkReader_ParseReply_Bool(b *testing.B) {
	benchmarkParseReply(b, "#t\r\n", false)
}

func BenchmarkReader_ParseReply_BigInt(b *testing.B) {
	benchmarkParseReply(b, "(3492890328409238509324850943850943825024385\r\n", false)
}

func BenchmarkReader_ParseReply_Error(b *testing.B) {
	benchmarkParseReply(b, "-Error message\r\n", true)
}

func BenchmarkReader_ParseReply_Nil(b *testing.B) {
	benchmarkParseReply(b, "_\r\n", true)
}

func BenchmarkReader_ParseReply_BlobError(b *testing.B) {
	benchmarkParseReply(b, "!21\r\nSYNTAX invalid syntax", true)
}

func BenchmarkReader_ParseReply_String(b *testing.B) {
	benchmarkParseReply(b, "$5\r\nhello\r\n", false)
}

func BenchmarkReader_ParseReply_Verb(b *testing.B) {
	benchmarkParseReply(b, "$9\r\ntxt:hello\r\n", false)
}

func BenchmarkReader_ParseReply_Slice(b *testing.B) {
	benchmarkParseReply(b, "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Set(b *testing.B) {
	benchmarkParseReply(b, "~2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Push(b *testing.B) {
	benchmarkParseReply(b, ">2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", false)
}

func BenchmarkReader_ParseReply_Map(b *testing.B) {
	benchmarkParseReply(b, "%2\r\n$5\r\nhello\r\n$5\r\nworld\r\n+key\r\n+value\r\n", false)
}

func BenchmarkReader_ParseReply_Attr(b *testing.B) {
	benchmarkParseReply(b, "%1\r\n+key\r\n+value\r\n+hello\r\n", false)
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

	if !bytes.Equal(read, original[:len(original)-2]) {
		t.Errorf("Values must be equal: %d expected %d", len(read), len(original[:len(original)-2]))
	}
}

func BenchmarkReader_ReadFloat(b *testing.B) {
	buf := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		fmt.Fprint(buf, ",123.456\r\n")
	}
	p := proto.NewReader(buf)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := p.ReadFloat()
		if err != nil {
			b.Fatal(err)
		}
	}
}

// loopFrameReader is an io.Reader that returns the same RESP frame over and
// over without ever returning io.EOF. It lets the benchmark helpers below
// run for any b.N with O(1) memory — earlier versions materialised
// bytes.Repeat(frame, b.N) up front and OOM'd on CI when the test framework
// escalated b.N to the tens of thousands at 1 MiB payloads.
type loopFrameReader struct {
	frame []byte
	pos   int
}

func (r *loopFrameReader) Read(p []byte) (int, error) {
	n := 0
	for n < len(p) {
		if r.pos == len(r.frame) {
			r.pos = 0
		}
		c := copy(p[n:], r.frame[r.pos:])
		r.pos += c
		n += c
	}
	return n, nil
}

func makeBulkStringFrame(payloadSize int) []byte {
	payload := bytes.Repeat([]byte{'a'}, payloadSize)
	header := []byte(fmt.Sprintf("$%d\r\n", payloadSize))
	frame := make([]byte, 0, len(header)+payloadSize+2)
	frame = append(frame, header...)
	frame = append(frame, payload...)
	frame = append(frame, '\r', '\n')
	return frame
}

// benchmarkReadStringInto and benchmarkReadString compare the allocation
// behaviour of reading a bulk string into a pre-allocated buffer versus the
// standard ReadString call that returns a fresh string each time.
func benchmarkReadStringInto(b *testing.B, payloadSize int) {
	frame := makeBulkStringFrame(payloadSize)
	p := proto.NewReader(&loopFrameReader{frame: frame})
	buf := make([]byte, payloadSize)

	b.SetBytes(int64(payloadSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := p.ReadStringInto(buf); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkReadString(b *testing.B, payloadSize int) {
	frame := makeBulkStringFrame(payloadSize)
	p := proto.NewReader(&loopFrameReader{frame: frame})

	b.SetBytes(int64(payloadSize))
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := p.ReadString(); err != nil {
			b.Fatal(err)
		}
	}
}

// CI-friendly wrappers. Sizes capped at 64 KiB to keep peak memory under
// the GitHub Actions ubuntu-latest budget. The benchmark* helpers above
// can be invoked directly with larger sizes for off-CI analysis.
func BenchmarkReader_ReadStringInto_64B(b *testing.B)   { benchmarkReadStringInto(b, 64) }
func BenchmarkReader_ReadStringInto_4KiB(b *testing.B)  { benchmarkReadStringInto(b, 4*1024) }
func BenchmarkReader_ReadStringInto_64KiB(b *testing.B) { benchmarkReadStringInto(b, 64*1024) }

func BenchmarkReader_ReadString_64B(b *testing.B)   { benchmarkReadString(b, 64) }
func BenchmarkReader_ReadString_4KiB(b *testing.B)  { benchmarkReadString(b, 4*1024) }
func BenchmarkReader_ReadString_64KiB(b *testing.B) { benchmarkReadString(b, 64*1024) }

func benchmarkParseReply(b *testing.B, reply string, wanterr bool) {
	buf := new(bytes.Buffer)
	for i := 0; i < b.N; i++ {
		fmt.Fprint(buf, reply)
	}
	p := proto.NewReader(buf)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		_, err := p.ReadReply()
		if !wanterr && err != nil {
			b.Fatal(err)
		}
	}
}

func TestReader_ReadStringInto_BulkString(t *testing.T) {
	r := proto.NewReader(bytes.NewReader([]byte("$5\r\nhello\r\n")))
	buf := make([]byte, 16)
	n, err := r.ReadStringInto(buf)
	if err != nil {
		t.Fatalf("ReadStringInto: %v", err)
	}
	if n != 5 {
		t.Fatalf("n = %d, want 5", n)
	}
	if string(buf[:n]) != "hello" {
		t.Fatalf("got %q, want %q", buf[:n], "hello")
	}
}

func TestReader_ReadStringInto_Status(t *testing.T) {
	r := proto.NewReader(bytes.NewReader([]byte("+OK\r\n")))
	buf := make([]byte, 16)
	n, err := r.ReadStringInto(buf)
	if err != nil {
		t.Fatalf("ReadStringInto: %v", err)
	}
	if string(buf[:n]) != "OK" {
		t.Fatalf("got %q, want %q", buf[:n], "OK")
	}
}

func TestReader_ReadStringInto_Int(t *testing.T) {
	r := proto.NewReader(bytes.NewReader([]byte(":42\r\n")))
	buf := make([]byte, 16)
	n, err := r.ReadStringInto(buf)
	if err != nil {
		t.Fatalf("ReadStringInto: %v", err)
	}
	if string(buf[:n]) != "42" {
		t.Fatalf("got %q, want %q", buf[:n], "42")
	}
}

func TestReader_ReadStringInto_Float(t *testing.T) {
	r := proto.NewReader(bytes.NewReader([]byte(",3.14\r\n")))
	buf := make([]byte, 16)
	n, err := r.ReadStringInto(buf)
	if err != nil {
		t.Fatalf("ReadStringInto: %v", err)
	}
	if string(buf[:n]) != "3.14" {
		t.Fatalf("got %q, want %q", buf[:n], "3.14")
	}
}

func TestReader_ReadStringInto_VerbatimNotSupported(t *testing.T) {
	// Verbatim strings are not handled by ReadStringInto — see the godoc
	// on ReadStringInto for the rationale (avoiding a stale-line[0] hazard
	// after bufio refill). A verbatim reply must surface as an error rather
	// than being silently parsed.
	r := proto.NewReader(bytes.NewReader([]byte("=9\r\ntxt:hello\r\n")))
	buf := make([]byte, 16)
	if _, err := r.ReadStringInto(buf); err == nil {
		t.Fatal("expected error for verbatim reply, got nil")
	}
}

// TestReader_ReadStringInto_BulkStringStartingWithEqRefill exercises a
// large bulk-string payload of '=' bytes sized to exactly bufio's internal
// buffer, which forces bufio.fill() to run during the final io.ReadFull
// step and place '=' bytes at position 0 of bufio.buf — the same slot
// that line[0] points at.
//
// Historically a `if line[0] == RespVerbatim` check ran AFTER the refill,
// causing the regular bulk string to be misclassified as verbatim and
// 4 bytes silently stripped from the caller's payload. The hazard class
// has since been removed structurally — ReadStringInto no longer handles
// RespVerbatim at all — so this test now also acts as a guard against
// re-introducing verbatim support without re-introducing the bug.
func TestReader_ReadStringInto_BulkStringStartingWithEqRefill(t *testing.T) {
	const size = proto.DefaultBufferSize // exactly bufio buffer size
	payload := bytes.Repeat([]byte{'='}, size)
	src := make([]byte, 0, size+16)
	src = append(src, []byte(fmt.Sprintf("$%d\r\n", size))...)
	src = append(src, payload...)
	src = append(src, '\r', '\n')

	r := proto.NewReader(bytes.NewReader(src))
	buf := make([]byte, size)
	n, err := r.ReadStringInto(buf)
	if err != nil {
		t.Fatalf("ReadStringInto: %v", err)
	}
	if n != size {
		t.Fatalf("n = %d, want %d (4 bytes would be stripped on regression)", n, size)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Fatal("payload mismatch — possible verbatim-misdetection regression")
	}
}

func TestReader_ReadStringInto_Nil(t *testing.T) {
	// RESP3 nil
	r := proto.NewReader(bytes.NewReader([]byte("_\r\n")))
	buf := make([]byte, 16)
	if _, err := r.ReadStringInto(buf); err != proto.Nil {
		t.Fatalf("got err=%v, want proto.Nil", err)
	}
	// RESP2 nil bulk
	r = proto.NewReader(bytes.NewReader([]byte("$-1\r\n")))
	if _, err := r.ReadStringInto(buf); err != proto.Nil {
		t.Fatalf("got err=%v, want proto.Nil for $-1", err)
	}
}

func TestReader_ReadStringInto_Error(t *testing.T) {
	r := proto.NewReader(bytes.NewReader([]byte("-ERR something bad\r\n")))
	buf := make([]byte, 16)
	_, err := r.ReadStringInto(buf)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if err == proto.Nil {
		t.Fatalf("got proto.Nil, want ERR")
	}
}

func TestReader_ReadStringInto_BufferTooSmall(t *testing.T) {
	// Feed two consecutive bulk-string replies. The first read uses a
	// too-small buf and must error; the second must succeed cleanly. This
	// guards the drain-on-error behaviour — without it, the unread payload
	// of the first reply would leak into the stream and the second read
	// would parse garbage.
	src := []byte("$5\r\nhello\r\n$5\r\nworld\r\n")
	r := proto.NewReader(bytes.NewReader(src))

	smallBuf := make([]byte, 3)
	n, err := r.ReadStringInto(smallBuf)
	if err == nil {
		t.Fatal("expected buffer-too-small error, got nil")
	}
	if n != 0 {
		t.Fatalf("got n=%d on error, want 0", n)
	}

	// After the drain, the reader is aligned at the start of the second
	// reply. A normal read must parse it as "world".
	bigBuf := make([]byte, 16)
	n, err = r.ReadStringInto(bigBuf)
	if err != nil {
		t.Fatalf("follow-up ReadStringInto: %v (connection left misaligned by the buffer-too-small drain)", err)
	}
	if n != 5 || string(bigBuf[:n]) != "world" {
		t.Fatalf("follow-up: got n=%d %q, want 5 \"world\"", n, bigBuf[:n])
	}
}

func TestReader_Discard_NilReplies(t *testing.T) {
	// Discard must consume only the header line of a nil reply, leaving the
	// following reply byte-aligned. A nil bulk string ($-1), verbatim (=-1)
	// and blob error (!-1) each have no payload; discarding an extra 2 bytes
	// (the phantom payload CRLF) eats into the next reply and desyncs a
	// pooled connection so one command's data bleeds into another's.
	nilHeaders := []string{"$-1", "=-1", "!-1", "*-1", "%-1"}
	for _, h := range nilHeaders {
		src := h + "\r\n$5\r\nhello\r\n"
		r := proto.NewReader(bytes.NewReader([]byte(src)))
		if err := r.DiscardNext(); err != nil {
			t.Fatalf("%s: DiscardNext: %v", h, err)
		}
		s, err := r.ReadString()
		if err != nil {
			t.Fatalf("%s: follow-up ReadString: %v (stream left misaligned by Discard)", h, err)
		}
		if s != "hello" {
			t.Fatalf("%s: follow-up got %q, want \"hello\"", h, s)
		}
	}
}

func TestReader_Discard_AttributeWithNilValue(t *testing.T) {
	// A RESP3 attribute carrying a nil bulk value is skipped via Discard on
	// the read path (ReadLine -> RespAttr -> Discard). The real reply that
	// follows the attribute must parse unchanged.
	src := "|1\r\n$3\r\nttl\r\n$-1\r\n$6\r\nsecond\r\n"
	r := proto.NewReader(bytes.NewReader([]byte(src)))
	s, err := r.ReadString()
	if err != nil {
		t.Fatalf("ReadString after nil-valued attribute: %v", err)
	}
	if s != "second" {
		t.Fatalf("got %q, want \"second\"", s)
	}
}

func TestReader_Discard_MapCountOverflow(t *testing.T) {
	// A valid map is discarded pair-by-pair and leaves the next reply aligned.
	src := "%2\r\n+a\r\n+1\r\n+b\r\n+2\r\n$5\r\nhello\r\n"
	r := proto.NewReader(bytes.NewReader([]byte(src)))
	if err := r.DiscardNext(); err != nil {
		t.Fatalf("DiscardNext on valid map: %v", err)
	}
	if s, err := r.ReadString(); err != nil || s != "hello" {
		t.Fatalf("follow-up ReadString = %q, %v; want \"hello\", nil", s, err)
	}

	// A map (and an attribute) advertising a count above MaxInt/2 used to make
	// the n*2 element loop overflow int to a negative bound, skip the body and
	// return nil, leaving the map bytes to be read as the next reply. Iterating
	// over pairs instead means the oversized frame is rejected with an error
	// rather than silently desyncing a pooled connection.
	for _, h := range []string{"%4611686018427387904", "|4611686018427387904"} {
		src := h + "\r\n+k\r\n+v\r\n:42\r\n"
		r := proto.NewReader(bytes.NewReader([]byte(src)))
		if err := r.DiscardNext(); err == nil {
			t.Fatalf("%s: DiscardNext returned nil; oversized count was silently skipped", h)
		}
	}
}

func TestReader_ReadStringInto_Large(t *testing.T) {
	// Payload deliberately larger than the default bufio buffer (32 KiB)
	// so we exercise the path where bufio.Reader drains its internal
	// buffer first and then reads remaining bytes directly into buf.
	const size = proto.DefaultBufferSize * 4
	payload := bytes.Repeat([]byte{'a'}, size)
	src := make([]byte, 0, size+32)
	src = append(src, []byte(fmt.Sprintf("$%d\r\n", size))...)
	src = append(src, payload...)
	src = append(src, '\r', '\n')

	r := proto.NewReader(bytes.NewReader(src))
	buf := make([]byte, size)
	n, err := r.ReadStringInto(buf)
	if err != nil {
		t.Fatalf("ReadStringInto: %v", err)
	}
	if n != size {
		t.Fatalf("n = %d, want %d", n, size)
	}
	if !bytes.Equal(buf[:n], payload) {
		t.Fatal("payload mismatch")
	}
}
