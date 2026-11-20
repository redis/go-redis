package redis

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestLatencyHistogram_Args(t *testing.T) {
	tests := []struct {
		name     string
		commands []string
		want     []any
	}{
		{name: "all commands", want: []any{"latency", "histogram"}},
		{name: "one command", commands: []string{"ping"}, want: []any{"latency", "histogram", "ping"}},
		{
			name:     "two commands",
			commands: []string{"ping", "hello"},
			want:     []any{"latency", "histogram", "ping", "hello"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var captured Cmder
			c := captureCmdable(&captured)

			c.LatencyHistogram(context.Background(), tt.commands...)
			if !reflect.DeepEqual(captured.Args(), tt.want) {
				t.Errorf("args = %#v, want %#v", captured.Args(), tt.want)
			}
		})
	}
}

func TestLatencyHistogramCmd_readReply(t *testing.T) {
	// The replies of Redis 8.10 to LATENCY HISTOGRAM ping hello.
	const (
		pingBuckets = ":1\r\n:251\r\n:2\r\n:785\r\n:4\r\n:939\r\n:16\r\n:942\r\n" +
			":33\r\n:947\r\n:66\r\n:951\r\n:132\r\n:954\r\n:264\r\n:956\r\n"
		helloBuckets = ":4\r\n:1\r\n:8\r\n:50\r\n:16\r\n:335\r\n:33\r\n:1816\r\n:66\r\n:2122\r\n" +
			":132\r\n:2237\r\n:264\r\n:2254\r\n:528\r\n:2259\r\n:1056\r\n:2263\r\n:2113\r\n:2265\r\n"

		resp2 = "*4\r\n" +
			"$4\r\nping\r\n*4\r\n$5\r\ncalls\r\n:956\r\n$14\r\nhistogram_usec\r\n*16\r\n" + pingBuckets +
			"$5\r\nhello\r\n*4\r\n$5\r\ncalls\r\n:2265\r\n$14\r\nhistogram_usec\r\n*20\r\n" + helloBuckets
		resp3 = "%2\r\n" +
			"$4\r\nping\r\n%2\r\n$5\r\ncalls\r\n:956\r\n$14\r\nhistogram_usec\r\n%8\r\n" + pingBuckets +
			"$5\r\nhello\r\n%2\r\n$5\r\ncalls\r\n:2265\r\n$14\r\nhistogram_usec\r\n%10\r\n" + helloBuckets
	)
	// The fixture is rebuilt for every case because a clone that shares a
	// map with its source would change it.
	want := func() map[string]LatencyHistogram {
		return map[string]LatencyHistogram{
			"ping": {
				Calls: 956,
				HistogramUsec: map[int64]int64{
					1: 251, 2: 785, 4: 939, 16: 942, 33: 947, 66: 951, 132: 954, 264: 956,
				},
			},
			"hello": {
				Calls: 2265,
				HistogramUsec: map[int64]int64{
					4: 1, 8: 50, 16: 335, 33: 1816, 66: 2122,
					132: 2237, 264: 2254, 528: 2259, 1056: 2263, 2113: 2265,
				},
			},
		}
	}

	tests := []struct {
		name  string
		reply string
		want  map[string]LatencyHistogram
	}{
		{name: "RESP2", reply: resp2, want: want()},
		{name: "RESP3", reply: resp3, want: want()},
		// The replies of Redis 8.10 for a command that was never called.
		{name: "RESP2 no histograms", reply: "*0\r\n", want: map[string]LatencyHistogram{}},
		{name: "RESP3 no histograms", reply: "%0\r\n", want: map[string]LatencyHistogram{}},
		{
			name: "unknown field",
			reply: "%1\r\n$4\r\nping\r\n%3\r\n$3\r\nnew\r\n%1\r\n:1\r\n:2\r\n" +
				"$5\r\ncalls\r\n:3\r\n$14\r\nhistogram_usec\r\n%1\r\n:1\r\n:3\r\n",
			want: map[string]LatencyHistogram{
				"ping": {Calls: 3, HistogramUsec: map[int64]int64{1: 3}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewLatencyHistogramCmd(context.Background(), "latency", "histogram")
			if err := cmd.readReply(proto.NewReader(strings.NewReader(tt.reply))); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cmd.Val(), tt.want) {
				t.Errorf("val = %+v, want %+v", cmd.Val(), tt.want)
			}

			clone := cmd.Clone().(*LatencyHistogramCmd)
			for name, hist := range clone.Val() {
				for usec := range hist.HistogramUsec {
					hist.HistogramUsec[usec] = -1
				}
				clone.Val()[name] = LatencyHistogram{}
			}
			clone.Val()["added"] = LatencyHistogram{}
			if !reflect.DeepEqual(cmd.Val(), tt.want) {
				t.Errorf("val changed with its clone: %+v", cmd.Val())
			}
		})
	}

	t.Run("odd RESP2 buckets", func(t *testing.T) {
		const reply = "*2\r\n$4\r\nping\r\n*4\r\n$5\r\ncalls\r\n:1\r\n$14\r\nhistogram_usec\r\n*1\r\n:1\r\n"
		cmd := NewLatencyHistogramCmd(context.Background(), "latency", "histogram")
		if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err == nil {
			t.Errorf("reply accepted: %+v", cmd.Val())
		}
	})
}
