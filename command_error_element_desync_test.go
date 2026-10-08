package redis

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// TS.MADD and CMS.INCRBY answer item by item: a rejected item becomes an error
// element and the items after it are still reported. IntSliceCmd used to
// return on the error element with the rest of the array unread. Callers take
// a Redis error from readReply as a fully read reply, so in a pipeline the
// next command was handed the leftover element and every later reply shifted
// by one. The first two replies are the ones Redis 8.10 sends.
func TestIntSliceCmdErrorElementNoDesync(t *testing.T) {
	tests := []struct {
		name    string
		reply   string
		wantVal []int64
		wantErr string
	}{
		{
			name:    "TS.MADD sample older than retention",
			reply:   "*3\r\n:10000\r\n-ERR TSDB: Timestamp is older than retention\r\n:10001\r\n",
			wantVal: []int64{10000, 0, 10001},
			wantErr: "ERR TSDB: Timestamp is older than retention",
		},
		{
			name:    "CMS.INCRBY counter overflow",
			reply:   "*4\r\n:4294967294\r\n:1\r\n-CMS: INCRBY overflow\r\n:2\r\n",
			wantVal: []int64{4294967294, 1, 0, 2},
			wantErr: "CMS: INCRBY overflow",
		},
		{
			name:    "first of several errors is reported",
			reply:   "*4\r\n-ERR first\r\n:7\r\n-ERR second\r\n:8\r\n",
			wantVal: []int64{0, 7, 0, 8},
			wantErr: "ERR first",
		},
		{
			name:    "RESP3 blob error element",
			reply:   "*3\r\n:1\r\n!9\r\nERR boom!\r\n:3\r\n",
			wantVal: []int64{1, 0, 3},
			wantErr: "ERR boom!",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// The reply is followed by the next command's reply, :999.
			rd := proto.NewReader(strings.NewReader(tt.reply + ":999\r\n"))
			cmd := NewIntSliceCmd(context.Background())
			err := cmd.readReply(rd)
			if err == nil || err.Error() != tt.wantErr {
				t.Fatalf("readReply err = %v, want %q", err, tt.wantErr)
			}
			if !isRedisError(err) {
				t.Fatalf("readReply err = %T, want a Redis error", err)
			}
			if !slices.Equal(cmd.val, tt.wantVal) {
				t.Fatalf("unexpected val: %+v, want %+v", cmd.val, tt.wantVal)
			}
			assertNextReplyInt(t, rd, 999)
		})
	}
}

// A read error in the middle of the array is not an error element: the reply
// is incomplete and readReply must fail with it so the connection is dropped.
func TestIntSliceCmdErrorElementTruncatedReply(t *testing.T) {
	rd := proto.NewReader(strings.NewReader("*3\r\n:10000\r\n-ERR TSDB: Timestamp is older than retention\r\n"))
	cmd := NewIntSliceCmd(context.Background())
	err := cmd.readReply(rd)
	if err == nil {
		t.Fatal("readReply returned nil for a truncated reply")
	}
	if isRedisError(err) {
		t.Fatalf("readReply err = %v, want the read error, not the element error", err)
	}
}
