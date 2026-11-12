package proto_test

import (
	"fmt"
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
