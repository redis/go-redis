package proto_test

import (
	"bytes"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// TestReadReplyKeepsTypedErrorElements: an array or map element that parses to
// a typed Redis error (MOVED, ASK, LOADING, ...) is kept as a value like a plain
// -ERR element, and the whole aggregate is consumed.
func TestReadReplyKeepsTypedErrorElements(t *testing.T) {
	tests := []struct {
		name   string
		stream string
	}{
		{"array", "*2\r\n-MOVED 1 h:1\r\n:5\r\n+OK\r\n"},
		{"push", ">2\r\n-ASK 1 h:1\r\n:5\r\n+OK\r\n"},
		{"map", "%1\r\n+k\r\n-LOADING busy\r\n+OK\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rd := proto.NewReader(bytes.NewReader([]byte(tt.stream)))
			if _, err := rd.ReadReply(); err != nil {
				t.Fatalf("ReadReply: %v, want the typed error kept as an element", err)
			}
			next, err := rd.ReadReply()
			if err != nil || next != "OK" {
				t.Fatalf("next reply = %#v, %v; want \"OK\" (aggregate only partly consumed)", next, err)
			}
		})
	}
}

// TestReadReplyTypedErrorElementValue: the kept element is the typed error.
func TestReadReplyTypedErrorElementValue(t *testing.T) {
	rd := proto.NewReader(bytes.NewReader([]byte("*1\r\n-MOVED 1 h:1\r\n")))
	v, err := rd.ReadReply()
	if err != nil {
		t.Fatal(err)
	}
	elems, ok := v.([]interface{})
	if !ok || len(elems) != 1 {
		t.Fatalf("reply = %#v, want a 1-element array", v)
	}
	e, ok := elems[0].(error)
	if !ok {
		t.Fatalf("element = %#v, want an error", elems[0])
	}
	if _, ok := proto.IsMovedError(e); !ok {
		t.Fatalf("element = %v (%T), want a MOVED error", e, e)
	}
}
