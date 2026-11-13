package push

import (
	"context"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// A blocking push drain can swallow PeekReplyType errors, notably while
// discarding an attribute. The following command must still fail rather than
// decoding the leftover leaf as its own reply.
func TestProcessor_OverdeepFramesCannotShiftReplies(t *testing.T) {
	for _, tc := range []struct {
		name  string
		frame string
	}{
		{
			name:  "attribute",
			frame: "|1\r\n+meta\r\n" + strings.Repeat("*1\r\n", 1000) + "+leaf\r\n+actual\r\n",
		},
		{
			name:  "push",
			frame: ">1\r\n" + strings.Repeat("*1\r\n", 1000) + "+leaf\r\n+actual\r\n",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := proto.NewReader(strings.NewReader(tc.frame))
			_ = NewProcessor().ProcessPendingNotifications(context.Background(), NotificationHandlerContext{}, r)
			if reply, err := r.ReadReply(); err == nil || err.Error() != "redis: maximum nesting depth exceeded" {
				t.Fatalf("subsequent command read = %v, %v; want terminal depth error", reply, err)
			}
			r.Reset(strings.NewReader("+new reply\r\n"))
			if reply, err := r.ReadReply(); err != nil || reply != "new reply" {
				t.Fatalf("reply after Reset = %v, %v", reply, err)
			}
		})
	}
}
