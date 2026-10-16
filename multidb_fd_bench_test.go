package redis

import (
	"context"
	"strconv"
	"testing"
	"time"
)

// FD on a MultiDBClient vs FD on a plain *Client, same backend: the difference
// is MultiDB's per-command admission (AllowReserve, detector check) and outcome
// accounting on the fast path.
func benchmarkFDSet(b *testing.B, ap *AutoPipeliner) {
	ctx := context.Background()
	if err := ap.Set(ctx, "bench:fd:warm", "v", 0).Err(); err != nil {
		b.Skipf("no redis: %v", err)
	}
	const window = 64
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		cmds := make([]*StatusCmd, 0, window)
		i := 0
		for pb.Next() {
			cmds = append(cmds, ap.Set(ctx, "bench:fd:"+strconv.Itoa(i&1023), "v", 0))
			i++
			if len(cmds) == window {
				for _, c := range cmds {
					if err := c.Err(); err != nil {
						b.Error(err)
						return
					}
				}
				cmds = cmds[:0]
			}
		}
		for _, c := range cmds {
			_ = c.Err()
		}
	})
}

func BenchmarkFDSetPlainClient(b *testing.B) {
	c := NewClient(&Options{Addr: ":6379"})
	defer c.Close()
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		b.Fatal(err)
	}
	defer ap.Close()
	benchmarkFDSet(b, ap)
}

func BenchmarkFDSetMultiDB(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, &MultiDBOptions{
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: ":6379"}, Weight: 2},
			{Options: &Options{Addr: ":6379"}, Weight: 1},
		},
		AutoFallbackInterval: -1,
	})
	if err != nil {
		b.Skipf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()
	ap, err := mdb.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		b.Fatal(err)
	}
	if ap.mdbFD == nil {
		b.Fatal("MultiDB full duplex not engaged")
	}
	benchmarkFDSet(b, ap)
}

// Reference: MultiDB on the half-duplex autopipeline path.
func BenchmarkFDSetMultiDBHalfDuplex(b *testing.B) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	mdb, err := NewMultiDBClient(ctx, &MultiDBOptions{
		Clients: []MultiDBClientConfig{
			{Options: &Options{Addr: ":6379"}, Weight: 2},
			{Options: &Options{Addr: ":6379"}, Weight: 1},
		},
		AutoFallbackInterval: -1,
	})
	if err != nil {
		b.Skipf("NewMultiDBClient: %v", err)
	}
	defer mdb.Close()
	ap, err := mdb.AsyncAutoPipeline()
	if err != nil {
		b.Fatal(err)
	}
	benchmarkFDSet(b, ap)
}
