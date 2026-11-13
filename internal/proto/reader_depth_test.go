package proto_test

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestReader_ReadReply_NestingDepth(t *testing.T) {
	const limit = 1000
	for _, tc := range []struct {
		name   string
		prefix string
	}{
		{"array", "*1\r\n"},
		{"set", "~1\r\n"},
		{"push", ">1\r\n"},
		{"map", "%1\r\n+k\r\n"},
		{"mixed", "*1\r\n%1\r\n+k\r\n"},
	} {
		for _, depth := range []int{limit, limit + 1, 6000} {
			t.Run(fmt.Sprintf("%s/%d", tc.name, depth), func(t *testing.T) {
				var frame strings.Builder
				for i := 0; i < depth; i++ {
					if tc.name == "mixed" {
						if i%2 == 0 {
							frame.WriteString("*1\r\n")
						} else {
							frame.WriteString("%1\r\n+k\r\n")
						}
					} else {
						frame.WriteString(tc.prefix)
					}
				}
				frame.WriteString("+leaf\r\n+next\r\n")
				r := proto.NewReader(strings.NewReader(frame.String()))
				_, err := r.ReadReply()
				if depth > limit {
					if err == nil || err.Error() != "redis: maximum nesting depth exceeded" {
						t.Fatalf("expected nesting depth error, got %v", err)
					}
					// An over-deep reply is only partially consumed. Reset onto
					// a new stream to check that the depth counter unwinds.
					r.Reset(strings.NewReader("*1\r\n+leaf\r\n+next\r\n"))
					if _, err := r.ReadReply(); err != nil {
						t.Fatalf("ReadReply after Reset: %v", err)
					}
				} else if err != nil {
					t.Fatalf("reply at depth limit: %v", err)
				}
				if v, err := r.ReadReply(); err != nil || v != "next" {
					t.Fatalf("following reply = %v, %v; want next, nil", v, err)
				}
			})
		}
	}
}

func TestReader_ReadReply_AttributeNestingDepth(t *testing.T) {
	// ReadReply skips attributes using Discard, which must share the same
	// depth budget as the arrays that surround the attribute.
	frame := strings.Repeat("*1\r\n", 500) + "|1\r\n+k\r\n" +
		strings.Repeat("*1\r\n", 500) + "+leaf\r\n+reply\r\n"
	r := proto.NewReader(strings.NewReader(frame))
	if _, err := r.ReadReply(); err == nil || err.Error() != "redis: maximum nesting depth exceeded" {
		t.Fatalf("expected nesting depth error, got %v", err)
	}
}

func TestReader_ReadSlice_NestingDepth(t *testing.T) {
	r := proto.NewReader(strings.NewReader(strings.Repeat("*1\r\n", 1001) + "+leaf\r\n"))
	if _, err := r.ReadSlice(); err == nil || err.Error() != "redis: maximum nesting depth exceeded" {
		t.Fatalf("expected nesting depth error, got %v", err)
	}
}

func TestReader_NestingDepthErrorIsTerminalUntilReset(t *testing.T) {
	const overDeep = 1001
	for _, tc := range []struct {
		name  string
		frame string
		read  func(*proto.Reader) error
	}{
		{
			name:  "reply",
			frame: strings.Repeat("*1\r\n", overDeep) + "+leaf\r\n+next\r\n",
			read:  func(r *proto.Reader) error { _, err := r.ReadReply(); return err },
		},
		{
			name:  "attribute peek",
			frame: "|1\r\n+meta\r\n" + strings.Repeat("*1\r\n", overDeep) + "+leaf\r\n+next\r\n",
			read:  func(r *proto.Reader) error { _, err := r.PeekReplyType(); return err },
		},
		{
			name:  "discard",
			frame: strings.Repeat("*1\r\n", overDeep) + "+leaf\r\n+next\r\n",
			read:  (*proto.Reader).DiscardNext,
		},
		{
			name:  "raw reply",
			frame: strings.Repeat("*1\r\n", overDeep) + "+leaf\r\n+next\r\n",
			read:  func(r *proto.Reader) error { _, err := r.ReadRawReply(); return err },
		},
		{
			name:  "raw reply writer",
			frame: strings.Repeat("*1\r\n", overDeep) + "+leaf\r\n+next\r\n",
			read:  func(r *proto.Reader) error { _, err := r.ReadRawReplyWriteTo(io.Discard); return err },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := proto.NewReader(strings.NewReader(tc.frame))
			fatalErr := tc.read(r)
			if fatalErr == nil || fatalErr.Error() != "redis: maximum nesting depth exceeded" {
				t.Fatalf("expected nesting depth error, got %v", fatalErr)
			}

			// The rejected frame is still in the stream, including a trailing
			// reply. No read API may consume any of those remaining bytes.
			for _, check := range []struct {
				name string
				read func() error
			}{
				{"ReadReply", func() error { _, err := r.ReadReply(); return err }},
				{"PeekReplyType", func() error { _, err := r.PeekReplyType(); return err }},
				{"Peek", func() error { _, err := r.Peek(1); return err }},
				{"PeekPushNotificationName", func() error { _, err := r.PeekPushNotificationName(); return err }},
				{"DiscardNext", r.DiscardNext},
				{"Discard", func() error { return r.Discard([]byte("+item")) }},
				{"ReadRawReply", func() error { _, err := r.ReadRawReply(); return err }},
				{"ReadRawReplyWriteTo", func() error { _, err := r.ReadRawReplyWriteTo(io.Discard); return err }},
			} {
				if err := check.read(); err != fatalErr {
					t.Errorf("%s after depth limit = %v; want %v", check.name, err, fatalErr)
				}
			}

			r.Reset(strings.NewReader("+reset\r\n+next\r\n"))
			for _, want := range []string{"reset", "next"} {
				if got, err := r.ReadReply(); err != nil || got != want {
					t.Fatalf("ReadReply after Reset = %v, %v; want %q", got, err, want)
				}
			}
		})
	}
}

func TestReader_RawReplyNestingAtLimit(t *testing.T) {
	frame := strings.Repeat("*1\r\n", 1000) + "+leaf\r\n"
	for _, writeTo := range []bool{false, true} {
		r := proto.NewReader(strings.NewReader(frame + "+next\r\n"))
		var raw []byte
		if writeTo {
			var buf bytes.Buffer
			if _, err := r.ReadRawReplyWriteTo(&buf); err != nil {
				t.Fatalf("ReadRawReplyWriteTo at limit: %v", err)
			}
			raw = buf.Bytes()
		} else {
			var err error
			raw, err = r.ReadRawReply()
			if err != nil {
				t.Fatalf("ReadRawReply at limit: %v", err)
			}
		}
		if string(raw) != frame {
			t.Fatalf("raw reply differs at depth limit (writer=%t)", writeTo)
		}
		if got, err := r.ReadReply(); err != nil || got != "next" {
			t.Fatalf("following reply = %v, %v; want next", got, err)
		}
	}
}
