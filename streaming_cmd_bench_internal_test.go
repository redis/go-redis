package redis

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

var benchClientInfoSink *ClientInfo

// Compares the memory profile of the two ways to consume CLIENT LIST: the
// buffered ClientList path (StringCmd reads the whole bulk reply, caller
// splits and parses it) versus the ClientListStreaming path (StreamingCmd
// parses entries chunk by chunk without materializing the full reply).
func BenchmarkClientListVsStream(b *testing.B) {
	ctx := context.Background()

	for _, entries := range []int{100, 1_000, 10_000, 100_000} {
		wire := clientListWire(b, false, entries)

		b.Run(fmt.Sprintf("ClientList/entries=%d", entries), func(b *testing.B) {
			src := strings.NewReader(wire)
			rd := proto.NewReader(src)

			b.SetBytes(int64(len(wire)))
			b.ReportAllocs()
			for b.Loop() {
				src.Reset(wire)
				rd.Reset(src)

				cmd := NewStringCmd(ctx, "client", "list")
				if err := cmd.readReply(rd); err != nil {
					b.Fatalf("readReply: %v", err)
				}
				for line := range strings.SplitSeq(strings.TrimSpace(cmd.val), "\n") {
					info, err := parseClientInfo(line)
					if err != nil {
						b.Fatalf("parseClientInfo: %v", err)
					}
					benchClientInfoSink = info
				}
			}
		})

		b.Run(fmt.Sprintf("ClientListStreaming/entries=%d", entries), func(b *testing.B) {
			src := strings.NewReader(wire)
			rd := proto.NewReader(src)

			b.SetBytes(int64(len(wire)))
			b.ReportAllocs()
			for b.Loop() {
				src.Reset(wire)
				rd.Reset(src)

				cmd := NewStreamingCmd(ctx, "\n", StreamingParser(
					func(line []byte) (*ClientInfo, error) {
						return parseClientInfo(strings.TrimSpace(string(line)))
					},
					func(info *ClientInfo) error {
						benchClientInfoSink = info
						return nil
					},
				), 0, "client", "list")
				if err := cmd.readReply(rd); err != nil {
					b.Fatalf("readReply: %v", err)
				}
				if cmd.entriesCount != entries {
					b.Fatalf("streamed %d entries, want %d", cmd.entriesCount, entries)
				}
			}
		})
	}
}

// liveHeap forces a GC and returns the bytes still reachable, so deltas
// between two calls measure retained (not cumulative) memory.
func liveHeap() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// Measures peak live heap — the metric the churn benchmark above can't show.
// ClientList materializes the entire reply as one string before any parsing;
// ClientListStreaming never holds more than one read chunk plus a pending
// partial line. Each variant samples the live heap at its high-water point:
// right after readReply for the buffered path, mid-stream inside the callback
// for the streaming path. Only the peak-B metric is meaningful; ns/op is
// dominated by the forced GCs.
func BenchmarkClientListVsStreamPeakMem(b *testing.B) {
	ctx := context.Background()

	for _, entries := range []int{1_000, 100_000} {
		wire := clientListWire(b, false, entries)

		b.Run(fmt.Sprintf("ClientList/entries=%d", entries), func(b *testing.B) {
			src := strings.NewReader(wire)
			rd := proto.NewReader(src)

			var peak uint64
			for b.Loop() {
				benchClientInfoSink = nil
				src.Reset(wire)
				rd.Reset(src)
				base := liveHeap()

				cmd := NewStringCmd(ctx, "client", "list")
				if err := cmd.readReply(rd); err != nil {
					b.Fatalf("readReply: %v", err)
				}
				inflight := liveHeap()

				for line := range strings.SplitSeq(strings.TrimSpace(cmd.val), "\n") {
					info, err := parseClientInfo(line)
					if err != nil {
						b.Fatalf("parseClientInfo: %v", err)
					}
					benchClientInfoSink = info
				}
				peak = max(peak, inflight-base)
			}
			b.ReportMetric(float64(peak), "peak-B")
		})

		b.Run(fmt.Sprintf("ClientListStreaming/entries=%d", entries), func(b *testing.B) {
			src := strings.NewReader(wire)
			rd := proto.NewReader(src)
			mid := entries / 2

			var peak uint64
			for b.Loop() {
				benchClientInfoSink = nil
				src.Reset(wire)
				rd.Reset(src)
				base := liveHeap()

				var inflight uint64
				n := 0
				cmd := NewStreamingCmd(ctx, "\n", StreamingParser(
					func(line []byte) (*ClientInfo, error) {
						return parseClientInfo(strings.TrimSpace(string(line)))
					},
					func(info *ClientInfo) error {
						benchClientInfoSink = info
						if n == mid {
							// The chunk buffer and pending slice are live on
							// readReply's stack here — this is the high-water
							// point of the streaming path.
							inflight = liveHeap()
						}
						n++
						return nil
					},
				), 0, "client", "list")
				if err := cmd.readReply(rd); err != nil {
					b.Fatalf("readReply: %v", err)
				}
				if inflight > base {
					peak = max(peak, inflight-base)
				}
			}
			b.ReportMetric(float64(peak), "peak-B")
		})
	}
}
