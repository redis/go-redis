package proto_test

import (
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestScan_AllTypes(t *testing.T) {
	var (
		s   string
		bs  []byte
		i   int
		i8  int8
		i16 int16
		i32 int32
		i64 int64
		u   uint
		u8  uint8
		u16 uint16
		u32 uint32
		u64 uint64
		f32 float32
		f64 float64
		b   bool
		d   time.Duration
		ip  net.IP
	)

	check := func(in string, v interface{}) {
		t.Helper()
		if err := proto.Scan([]byte(in), v); err != nil {
			t.Errorf("Scan(%q, %T) error: %v", in, v, err)
		}
	}

	check("hello", &s)
	if s != "hello" {
		t.Errorf("string = %q", s)
	}
	check("raw", &bs)
	if string(bs) != "raw" {
		t.Errorf("bytes = %q", bs)
	}
	check("42", &i)
	check("-8", &i8)
	check("-16", &i16)
	check("-32", &i32)
	check("-64", &i64)
	check("7", &u)
	check("8", &u8)
	check("16", &u16)
	check("32", &u32)
	check("64", &u64)
	check("3.5", &f32)
	check("6.25", &f64)
	check("1", &b)
	if !b {
		t.Error("bool should be true for '1'")
	}
	check("1000", &d)
	if d != time.Duration(1000) {
		t.Errorf("duration = %v", d)
	}
	check("1.2.3.4", &ip)

	var tm time.Time
	check("2020-01-02T15:04:05Z", &tm)
	if tm.Year() != 2020 {
		t.Errorf("time = %v", tm)
	}
}

func TestScan_BoolFalse(t *testing.T) {
	var b bool
	if err := proto.Scan([]byte("0"), &b); err != nil || b {
		t.Errorf("Scan bool '0' = %v, %v", b, err)
	}
}

func TestScan_Errors(t *testing.T) {
	if err := proto.Scan([]byte("x"), nil); err == nil {
		t.Error("expected error for Scan(nil)")
	}
	var i int
	if err := proto.Scan([]byte("notanumber"), &i); err == nil {
		t.Error("expected parse error for invalid int")
	}
	var unsupported chan int
	if err := proto.Scan([]byte("x"), &unsupported); err == nil {
		t.Error("expected unsupported type error")
	}
}

type binUnmarshaler struct{ data string }

func (b *binUnmarshaler) UnmarshalBinary(data []byte) error {
	b.data = string(data)
	return nil
}

func TestScan_BinaryUnmarshaler(t *testing.T) {
	var bu binUnmarshaler
	if err := proto.Scan([]byte("payload"), &bu); err != nil {
		t.Fatalf("Scan BinaryUnmarshaler error: %v", err)
	}
	if bu.data != "payload" {
		t.Errorf("BinaryUnmarshaler data = %q", bu.data)
	}
}

func TestScanSlice_Errors(t *testing.T) {
	if err := proto.ScanSlice(nil, nil); err == nil {
		t.Error("expected error for ScanSlice(nil)")
	}
	var notPtr []int
	if err := proto.ScanSlice([]string{"1"}, notPtr); err == nil {
		t.Error("expected non-pointer error")
	}
	var notSlice int
	if err := proto.ScanSlice([]string{"1"}, &notSlice); err == nil {
		t.Error("expected non-slice error")
	}
	var ints []int
	if err := proto.ScanSlice([]string{"1", "2"}, &ints); err != nil {
		t.Errorf("ScanSlice([]int) error: %v", err)
	}
	if len(ints) != 2 || ints[0] != 1 || ints[1] != 2 {
		t.Errorf("ScanSlice result = %v", ints)
	}
	if err := proto.ScanSlice([]string{"bad"}, &ints); err == nil {
		t.Error("expected element scan error")
	}
}
