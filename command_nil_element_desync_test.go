package redis

import (
	"context"
	"slices"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// A flat scalar array can carry a nil in a non-final position. BITFIELD with
// OVERFLOW FAIL returns nil for an operation that would overflow, e.g.
// `BITFIELD k OVERFLOW FAIL INCRBY u8 0 255 INCRBY u8 0 255 GET u8 0` replies
// [255, nil, 255]. IntSliceCmd, UintSliceCmd and BoolSliceCmd used to return on
// the nil element, leaving the trailing elements on the wire and desyncing the
// pooled connection so the next command read them as its reply. They must
// consume the whole array like FloatSliceCmd and StringSliceCmd already do.
func TestSliceCmdNilElementNoDesync(t *testing.T) {
	// Each reply is [ v, nil, v ] followed by the next command's reply :999.
	// A correct parser consumes all three elements and leaves :999 for the next read.
	t.Run("IntSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*3\r\n:255\r\n_\r\n:100\r\n:999\r\n"))
		cmd := NewIntSliceCmd(context.Background())
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("readReply: %v", err)
		}
		if got := cmd.val; len(got) != 3 || got[0] != 255 || got[1] != 0 || got[2] != 100 {
			t.Fatalf("unexpected val: %+v", got)
		}
		assertNextReplyInt(t, rd, 999)
	})

	t.Run("UintSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*3\r\n:255\r\n_\r\n:100\r\n:999\r\n"))
		cmd := NewUintSliceCmd(context.Background())
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("readReply: %v", err)
		}
		if got := cmd.val; len(got) != 3 || got[0] != 255 || got[1] != 0 || got[2] != 100 {
			t.Fatalf("unexpected val: %+v", got)
		}
		assertNextReplyInt(t, rd, 999)
	})

	t.Run("BoolSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*3\r\n:1\r\n_\r\n:1\r\n:999\r\n"))
		cmd := NewBoolSliceCmd(context.Background())
		if err := cmd.readReply(rd); err != nil {
			t.Fatalf("readReply: %v", err)
		}
		if got := cmd.val; len(got) != 3 || !got[0] || got[1] || !got[2] {
			t.Fatalf("unexpected val: %+v", got)
		}
		assertNextReplyInt(t, rd, 999)
	})
}

// An error element in a non-final position is a complete frame, e.g. EVAL with
// `return {'a', redis.error_reply('boom'), 'c'}`. UintSliceCmd, FloatSliceCmd,
// StringSliceCmd and BoolSliceCmd used to return on it with the trailing
// elements still on the wire, so in a pipeline the next command read them as
// its reply. They must consume the whole array and return the first error.
func TestSliceCmdErrorElementNoDesync(t *testing.T) {
	t.Run("UintSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*3\r\n:255\r\n-ERR item failed\r\n:100\r\n:999\r\n"))
		cmd := NewUintSliceCmd(context.Background())
		if err := cmd.readReply(rd); err == nil || err.Error() != "ERR item failed" {
			t.Fatalf("readReply err = %v, want ERR item failed", err)
		}
		if got := cmd.val; len(got) != 3 || got[0] != 255 || got[1] != 0 || got[2] != 100 {
			t.Fatalf("unexpected val: %+v", got)
		}
		assertNextReplyInt(t, rd, 999)
	})

	t.Run("FloatSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*3\r\n,1.5\r\n!15\r\nERR item failed\r\n,2.5\r\n:999\r\n"))
		cmd := NewFloatSliceCmd(context.Background())
		if err := cmd.readReply(rd); err == nil || err.Error() != "ERR item failed" {
			t.Fatalf("readReply err = %v, want ERR item failed", err)
		}
		if got := cmd.val; len(got) != 3 || got[0] != 1.5 || got[1] != 0 || got[2] != 2.5 {
			t.Fatalf("unexpected val: %+v", got)
		}
		assertNextReplyInt(t, rd, 999)
	})

	t.Run("StringSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*4\r\n$1\r\na\r\n-ERR first\r\n-ERR second\r\n$1\r\nc\r\n:999\r\n"))
		cmd := NewStringSliceCmd(context.Background())
		if err := cmd.readReply(rd); err == nil || err.Error() != "ERR first" {
			t.Fatalf("readReply err = %v, want ERR first", err)
		}
		if got := cmd.val; len(got) != 4 || got[0] != "a" || got[1] != "" || got[2] != "" || got[3] != "c" {
			t.Fatalf("unexpected val: %+v", got)
		}
		assertNextReplyInt(t, rd, 999)
	})

	t.Run("BoolSliceCmd", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*3\r\n:1\r\n-ERR item failed\r\n:1\r\n:999\r\n"))
		cmd := NewBoolSliceCmd(context.Background())
		if err := cmd.readReply(rd); err == nil || err.Error() != "ERR item failed" {
			t.Fatalf("readReply err = %v, want ERR item failed", err)
		}
		if got := cmd.val; len(got) != 3 || !got[0] || got[1] || !got[2] {
			t.Fatalf("unexpected val: %+v", got)
		}
		assertNextReplyInt(t, rd, 999)
	})

	t.Run("TruncatedReply", func(t *testing.T) {
		rd := proto.NewReader(strings.NewReader("*3\r\n$1\r\na\r\n-ERR item failed\r\n"))
		cmd := NewStringSliceCmd(context.Background())
		err := cmd.readReply(rd)
		if err == nil || isRedisError(err) {
			t.Fatalf("readReply err = %v, want the read error", err)
		}
	})
}

func assertNextReplyInt(t *testing.T, rd *proto.Reader, want int64) {
	t.Helper()
	got, err := rd.ReadInt()
	if err != nil {
		t.Fatalf("next reply read failed (connection desynced): %v", err)
	}
	if got != want {
		t.Fatalf("connection desynced: next reply = %d, want %d", got, want)
	}
}

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
