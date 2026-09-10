package redis

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// The probabilistic *.INFO replies are open-ended maps; newer servers add
// fields (e.g. CMS.INFO "cell_size" in Redis 8.12). Unknown fields must be
// drained so the reader stays aligned with the following reply instead of
// failing the whole command.

func TestCMSInfoCmdParsesCellSizeAndIgnoresUnknownField(t *testing.T) {
	reply := "%5\r\n" +
		"+width\r\n:2000\r\n" +
		"+depth\r\n:5\r\n" +
		"+count\r\n:0\r\n" +
		"+cell_size\r\n:4\r\n" +
		"+new-cms-field\r\n$5\r\nhello\r\n" // unknown field, string value

	cmd := NewCMSInfoCmd(context.Background())
	rd := proto.NewReader(strings.NewReader(reply + "+NEXT\r\n"))
	if err := cmd.readReply(rd); err != nil {
		t.Fatalf("readReply() returned unexpected error: %v", err)
	}
	want := CMSInfo{Width: 2000, Depth: 5, Count: 0, CellSize: 4}
	if got := cmd.Val(); got != want {
		t.Fatalf("CMSInfo = %+v, want %+v", got, want)
	}
	if next, err := rd.ReadString(); err != nil || next != "NEXT" {
		t.Fatalf("stream desynced: next=%q err=%v", next, err)
	}
}

func TestProbabilisticInfoCmdsIgnoreUnknownField(t *testing.T) {
	// unknown field with a nested (array) value, to exercise a full discard
	unknown := "+new-field\r\n*2\r\n:1\r\n$3\r\nfoo\r\n"

	tests := []struct {
		name  string
		reply string
		cmd   interface {
			Cmder
			readReply(rd *proto.Reader) error
		}
	}{
		{
			name:  "BF.INFO",
			reply: "%2\r\n+Capacity\r\n:100\r\n" + unknown,
			cmd:   NewBFInfoCmd(context.Background(), "bf.info", "key"),
		},
		{
			name:  "CF.INFO",
			reply: "%2\r\n+Size\r\n:100\r\n" + unknown,
			cmd:   NewCFInfoCmd(context.Background()),
		},
		{
			name:  "TOPK.INFO",
			reply: "%2\r\n+k\r\n:3\r\n" + unknown,
			cmd:   NewTopKInfoCmd(context.Background()),
		},
		{
			name:  "TDIGEST.INFO",
			reply: "%2\r\n+Compression\r\n:100\r\n" + unknown,
			cmd:   NewTDigestInfoCmd(context.Background()),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			rd := proto.NewReader(strings.NewReader(tc.reply + "+NEXT\r\n"))
			if err := tc.cmd.readReply(rd); err != nil {
				t.Fatalf("readReply() returned unexpected error: %v", err)
			}
			if next, err := rd.ReadString(); err != nil || next != "NEXT" {
				t.Fatalf("stream desynced: next=%q err=%v", next, err)
			}
		})
	}
}

// Asking BF.INFO for a specific unknown attribute is a caller error and must
// still be reported rather than silently skipped.
func TestBFInfoCmdSingleUnknownAttributeStillErrors(t *testing.T) {
	cmd := NewBFInfoCmd(context.Background(), "bf.info", "key", "BOGUS")
	rd := proto.NewReader(strings.NewReader("*1\r\n:1\r\n"))
	if err := cmd.readReply(rd); err == nil {
		t.Fatal("readReply() = nil, want error for unknown attribute")
	}
}
