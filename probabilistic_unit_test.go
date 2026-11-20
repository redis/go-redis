package redis

import (
	"context"
	"reflect"
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

// BF.INFO matches the requested attribute case-insensitively and, in RESP2,
// replies with the bare value, so the reply is read by the option name.
func TestBFInfoCmdOptionCaseInsensitive(t *testing.T) {
	for _, opt := range []string{"CAPACITY", "capacity", "Capacity"} {
		t.Run(opt, func(t *testing.T) {
			cmd := NewBFInfoCmd(context.Background(), "bf.info", "key", opt)
			rd := proto.NewReader(strings.NewReader("*1\r\n:100\r\n+NEXT\r\n"))
			if err := cmd.readReply(rd); err != nil {
				t.Fatalf("readReply() error = %v", err)
			}
			if want := (BFInfo{Capacity: 100}); cmd.val != want {
				t.Fatalf("val = %+v, want %+v", cmd.val, want)
			}
		})
	}
}

// A filter created with NONSCALING has no expansion rate and the server
// reports it as null. The rest of the reply must still be parsed.
func TestBFInfoCmdNonScalingFilter(t *testing.T) {
	fields := "+Capacity\r\n:100\r\n" +
		"+Size\r\n:216\r\n" +
		"+Number of filters\r\n:1\r\n" +
		"+Number of items inserted\r\n:2\r\n" +
		"+Expansion rate\r\n"
	want := BFInfo{Capacity: 100, Size: 216, Filters: 1, ItemsInserted: 2}

	tests := []struct {
		name  string
		reply string
		args  []interface{}
		want  BFInfo
	}{
		{
			name:  "resp2",
			reply: "*10\r\n" + fields + "$-1\r\n",
			args:  []interface{}{"bf.info", "key"},
			want:  want,
		},
		{
			name:  "resp3",
			reply: "%5\r\n" + fields + "_\r\n",
			args:  []interface{}{"bf.info", "key"},
			want:  want,
		},
		{
			name:  "resp2_expansion_only",
			reply: "*1\r\n$-1\r\n",
			args:  []interface{}{"bf.info", "key", "EXPANSION"},
		},
		{
			name:  "resp3_expansion_only",
			reply: "%1\r\n+Expansion rate\r\n_\r\n",
			args:  []interface{}{"bf.info", "key", "EXPANSION"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewBFInfoCmd(context.Background(), tc.args...)
			rd := proto.NewReader(strings.NewReader(tc.reply + "+NEXT\r\n"))
			if err := cmd.readReply(rd); err != nil {
				t.Fatalf("readReply() error = %v", err)
			}
			if cmd.val != tc.want {
				t.Fatalf("val = %+v, want %+v", cmd.val, tc.want)
			}
			if next, err := rd.ReadString(); err != nil || next != "NEXT" {
				t.Fatalf("next reply = %q, %v; reader is out of sync", next, err)
			}
		})
	}
}

// A nil options pointer means "no options", as in BFReserveWithArgs: the
// command is still built and the server reports what is missing, instead of
// the client dereferencing the pointer and panicking.
func TestCFReserveWithArgsNilOptions(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name    string
		options *CFReserveOptions
		want    []interface{}
	}{
		{
			name: "nil_options",
			want: []interface{}{"CF.RESERVE", "cf"},
		},
		{
			name:    "capacity",
			options: &CFReserveOptions{Capacity: 1000},
			want:    []interface{}{"CF.RESERVE", "cf", int64(1000)},
		},
		{
			name:    "all_options",
			options: &CFReserveOptions{Capacity: 1000, BucketSize: 2, MaxIterations: 20, Expansion: 1},
			want: []interface{}{
				"CF.RESERVE", "cf", int64(1000),
				"BUCKETSIZE", int64(2),
				"MAXITERATIONS", int64(20),
				"EXPANSION", int64(1),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			cmd := captureCmdable(&captured).CFReserveWithArgs(ctx, "cf", tt.options)
			if !reflect.DeepEqual(cmd.Args(), tt.want) {
				t.Errorf("args mismatch\n got: %#v\nwant: %#v", cmd.Args(), tt.want)
			}
		})
	}
}
