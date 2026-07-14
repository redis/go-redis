package proto_test

import (
	"bytes"
	"io"
	"math"
	"math/big"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func newReader(s string) *proto.Reader {
	return proto.NewReader(strings.NewReader(s))
}

func TestReader_ReadReplyTypes(t *testing.T) {
	bigVal, ok := new(big.Int).SetString("3492890328409238509324850943850943825024385", 10)
	if !ok {
		t.Fatal("failed to build expected big.Int value")
	}
	tests := []struct {
		name  string
		input string
		want  interface{}
	}{
		{"status", "+OK\r\n", "OK"},
		{"int", ":42\r\n", int64(42)},
		{"float", ",3.14\r\n", 3.14},
		{"float-inf", ",inf\r\n", math.Inf(1)},
		{"float-neg-inf", ",-inf\r\n", math.Inf(-1)},
		{"float-nan", ",nan\r\n", math.NaN()},
		{"bool-true", "#t\r\n", true},
		{"bool-false", "#f\r\n", false},
		{"bigint", "(3492890328409238509324850943850943825024385\r\n", bigVal},
		{"string", "$5\r\nhello\r\n", "hello"},
		{"verbatim", "=9\r\ntxt:hello\r\n", "hello"},
		{"array", "*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n", []interface{}{"hello", "world"}},
		{"set", "~2\r\n+a\r\n+b\r\n", []interface{}{"a", "b"}},
		{"push", ">2\r\n+a\r\n+b\r\n", []interface{}{"a", "b"}},
		{"map", "%1\r\n+key\r\n+value\r\n", map[interface{}]interface{}{"key": "value"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := newReader(tt.input).ReadReply()
			if err != nil {
				t.Fatalf("ReadReply(%q) unexpected error: %v", tt.input, err)
			}
			// NaN never compares equal to itself; check it explicitly.
			if want, ok := tt.want.(float64); ok && math.IsNaN(want) {
				if f, ok := got.(float64); !ok || !math.IsNaN(f) {
					t.Fatalf("ReadReply(%q) = %v (%T), want NaN", tt.input, got, got)
				}
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadReply(%q) = %#v (%T), want %#v (%T)", tt.input, got, got, tt.want, tt.want)
			}
		})
	}
}

func TestReader_ReadReplyErrors(t *testing.T) {
	if _, err := newReader("#x\r\n").ReadReply(); err == nil {
		t.Error("expected error for invalid bool")
	}
	if _, err := newReader("(notanumber\r\n").ReadReply(); err == nil {
		t.Error("expected error for invalid bigint")
	}
	if _, err := newReader("?unknown\r\n").ReadReply(); err == nil {
		t.Error("expected error for unknown type")
	}
}

func TestReader_ReadInt(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{":42\r\n", 42},
		{"+7\r\n", 7},
		{"$2\r\n13\r\n", 13},
		{"(100\r\n", 100},
	}
	for _, tt := range tests {
		got, err := newReader(tt.input).ReadInt()
		if err != nil {
			t.Fatalf("ReadInt(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ReadInt(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
	if _, err := newReader("#t\r\n").ReadInt(); err == nil {
		t.Error("expected error for non-int reply")
	}
	if _, err := newReader("(99999999999999999999999999999\r\n").ReadInt(); err == nil {
		t.Error("expected out-of-range error for bigint")
	}
}

func TestReader_ReadUint(t *testing.T) {
	tests := []struct {
		input string
		want  uint64
	}{
		{":42\r\n", 42},
		{"+7\r\n", 7},
		{"$2\r\n13\r\n", 13},
		{"(100\r\n", 100},
	}
	for _, tt := range tests {
		got, err := newReader(tt.input).ReadUint()
		if err != nil {
			t.Fatalf("ReadUint(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ReadUint(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
	if _, err := newReader("#t\r\n").ReadUint(); err == nil {
		t.Error("expected error for non-uint reply")
	}
}

func TestReader_ReadFloat(t *testing.T) {
	tests := []struct {
		input string
		want  float64
	}{
		{",3.14\r\n", 3.14},
		{"+2.5\r\n", 2.5},
		{"$3\r\n1.5\r\n", 1.5},
	}
	for _, tt := range tests {
		got, err := newReader(tt.input).ReadFloat()
		if err != nil {
			t.Fatalf("ReadFloat(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ReadFloat(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
	if _, err := newReader(":1\r\n").ReadFloat(); err == nil {
		t.Error("expected error for non-float reply")
	}
}

func TestReader_ReadString(t *testing.T) {
	tests := []struct {
		input string
		want  string
	}{
		{"+OK\r\n", "OK"},
		{":42\r\n", "42"},
		{",3.14\r\n", "3.14"},
		{"$5\r\nhello\r\n", "hello"},
		{"#t\r\n", "true"},
		{"#f\r\n", "false"},
		{"=9\r\ntxt:hello\r\n", "hello"},
		{"(123\r\n", "123"},
	}
	for _, tt := range tests {
		got, err := newReader(tt.input).ReadString()
		if err != nil {
			t.Fatalf("ReadString(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ReadString(%q) = %q, want %q", tt.input, got, tt.want)
		}
	}
	if _, err := newReader("*1\r\n+a\r\n").ReadString(); err == nil {
		t.Error("expected error for array reply")
	}
}

func TestReader_ReadBool(t *testing.T) {
	tests := []struct {
		input string
		want  bool
	}{
		{"+OK\r\n", true},
		{":1\r\n", true},
		{"#t\r\n", true},
		{":0\r\n", false},
		{"$5\r\nhello\r\n", false},
	}
	for _, tt := range tests {
		got, err := newReader(tt.input).ReadBool()
		if err != nil {
			t.Fatalf("ReadBool(%q) error: %v", tt.input, err)
		}
		if got != tt.want {
			t.Errorf("ReadBool(%q) = %v, want %v", tt.input, got, tt.want)
		}
	}
	if _, err := newReader("*1\r\n+a\r\n").ReadBool(); err == nil {
		t.Error("expected error for array reply")
	}
}

func TestReader_ReadSliceAndLens(t *testing.T) {
	slice, err := newReader("*2\r\n+a\r\n+b\r\n").ReadSlice()
	if err != nil || len(slice) != 2 {
		t.Fatalf("ReadSlice = %v, %v", slice, err)
	}

	if err := newReader("*3\r\n+a\r\n+b\r\n+c\r\n").ReadFixedArrayLen(3); err != nil {
		t.Errorf("ReadFixedArrayLen(3) error: %v", err)
	}
	if err := newReader("*2\r\n+a\r\n+b\r\n").ReadFixedArrayLen(3); err == nil {
		t.Error("expected mismatch error from ReadFixedArrayLen")
	}

	n, err := newReader("*4\r\n").ReadArrayLen()
	if err != nil || n != 4 {
		t.Errorf("ReadArrayLen = %d, %v", n, err)
	}
	if _, err := newReader(":1\r\n").ReadArrayLen(); err == nil {
		t.Error("expected error from ReadArrayLen on non-array")
	}

	m, err := newReader("%2\r\n").ReadMapLen()
	if err != nil || m != 2 {
		t.Errorf("ReadMapLen(map) = %d, %v", m, err)
	}
	m, err = newReader("*4\r\n").ReadMapLen()
	if err != nil || m != 2 {
		t.Errorf("ReadMapLen(array) = %d, %v", m, err)
	}
	if _, err := newReader("*3\r\n").ReadMapLen(); err == nil {
		t.Error("expected odd-length error from ReadMapLen")
	}
	if _, err := newReader(":1\r\n").ReadMapLen(); err == nil {
		t.Error("expected error from ReadMapLen on non-map")
	}

	if err := newReader("%2\r\n").ReadFixedMapLen(2); err != nil {
		t.Errorf("ReadFixedMapLen(2) error: %v", err)
	}
	if err := newReader("%2\r\n").ReadFixedMapLen(3); err == nil {
		t.Error("expected mismatch error from ReadFixedMapLen")
	}
}

func TestReader_Discard(t *testing.T) {
	inputs := []string{
		":1\r\n",
		"$5\r\nhello\r\n",
		"*2\r\n+a\r\n+b\r\n",
		"%1\r\n+k\r\n+v\r\n",
		"_\r\n",
	}
	for _, in := range inputs {
		if err := newReader(in).DiscardNext(); err != nil {
			t.Errorf("DiscardNext(%q) error: %v", in, err)
		}
	}
	if err := newReader("").Discard(nil); err == nil {
		t.Error("expected error discarding empty line")
	}
}

func TestReader_ReadRawReply(t *testing.T) {
	inputs := []string{
		"+OK\r\n",
		"$5\r\nhello\r\n",
		"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
		"%1\r\n+k\r\n+v\r\n",
		"|1\r\n+k\r\n+v\r\n+actual\r\n",
	}
	for _, in := range inputs {
		raw, err := newReader(in).ReadRawReply()
		if err != nil {
			t.Errorf("ReadRawReply(%q) error: %v", in, err)
			continue
		}
		if len(raw) == 0 {
			t.Errorf("ReadRawReply(%q) returned empty", in)
		}
	}
}

func TestReader_ReadRawReplyWriteTo(t *testing.T) {
	inputs := []string{
		"+OK\r\n",
		"$5\r\nhello\r\n",
		"*2\r\n$1\r\na\r\n$1\r\nb\r\n",
		"%1\r\n+k\r\n+v\r\n",
		"|1\r\n+k\r\n+v\r\n+actual\r\n",
	}
	for _, in := range inputs {
		var buf bytes.Buffer
		n, err := newReader(in).ReadRawReplyWriteTo(&buf)
		if err != nil {
			t.Errorf("ReadRawReplyWriteTo(%q) error: %v", in, err)
			continue
		}
		if n != int64(buf.Len()) {
			t.Errorf("ReadRawReplyWriteTo(%q) wrote %d but reported %d", in, buf.Len(), n)
		}
	}
}

func TestReader_PeekAndBuffered(t *testing.T) {
	r := newReader("+OK\r\n")
	typ, err := r.PeekReplyType()
	if err != nil || typ != '+' {
		t.Errorf("PeekReplyType = %q, %v", typ, err)
	}
	if r.Buffered() == 0 {
		t.Error("expected buffered bytes")
	}
	b, err := r.Peek(1)
	if err != nil || b[0] != '+' {
		t.Errorf("Peek = %v, %v", b, err)
	}
}

func TestReader_PeekReplyTypeSkipsAttr(t *testing.T) {
	r := newReader("|1\r\n+k\r\n+v\r\n+OK\r\n")
	typ, err := r.PeekReplyType()
	if err != nil || typ != '+' {
		t.Errorf("PeekReplyType after attr = %q, %v", typ, err)
	}
}

func TestReader_ResetAndNewReaderSize(t *testing.T) {
	r := proto.NewReaderSize(strings.NewReader("+A\r\n"), 64)
	if _, err := r.ReadString(); err != nil {
		t.Fatalf("ReadString error: %v", err)
	}
	r.Reset(strings.NewReader("+B\r\n"))
	got, err := r.ReadString()
	if err != nil || got != "B" {
		t.Errorf("after Reset ReadString = %q, %v", got, err)
	}
}

func TestParseErrorReply(t *testing.T) {
	err := proto.ParseErrorReply([]byte("-ERR something went wrong"))
	if err == nil {
		t.Fatal("expected error from ParseErrorReply")
	}
	if !strings.Contains(err.Error(), "ERR something went wrong") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestReader_EOF(t *testing.T) {
	if _, err := proto.NewReader(strings.NewReader("")).ReadReply(); err != io.EOF {
		t.Errorf("expected io.EOF, got %v", err)
	}
}
