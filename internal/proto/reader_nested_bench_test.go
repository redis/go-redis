package proto_test

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// nestedReplyFrame builds a valid RESP aggregate with a single nested value.
// Maps contribute a key at every level; mixed frames alternate maps and arrays.
func nestedReplyFrame(kind string, depth int) []byte {
	var buf bytes.Buffer
	for i := 0; i < depth; i++ {
		switch kind {
		case "Map":
			buf.WriteString("%1\r\n+key\r\n")
		case "Mixed":
			if i%2 == 0 {
				buf.WriteString("*1\r\n")
			} else {
				buf.WriteString("%1\r\n+key\r\n")
			}
		default:
			buf.WriteString("*1\r\n")
		}
	}
	buf.WriteString("+OK\r\n")
	return buf.Bytes()
}

func BenchmarkReader_NestedReply(b *testing.B) {
	for _, kind := range []string{"Array", "Map", "Mixed"} {
		for _, depth := range []int{2, 8, 32, 128} {
			b.Run(fmt.Sprintf("%s/depth%d", kind, depth), func(b *testing.B) {
				frame := nestedReplyFrame(kind, depth)
				r := proto.NewReader(&loopFrameReader{frame: frame})
				b.SetBytes(int64(len(frame)))
				b.ReportAllocs()
				b.ResetTimer()
				for i := 0; i < b.N; i++ {
					result, err := r.ReadReply()
					if err != nil || result == nil {
						b.Fatalf("ReadReply: value=%v error=%v", result, err)
					}
				}
			})
		}
	}
}
