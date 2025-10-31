package proto

import (
	"bytes"
	"testing"
)

// BenchmarkWriteArgs benchmarks writing command arguments
func BenchmarkWriteArgs(b *testing.B) {
	testCases := []struct {
		name string
		args []interface{}
	}{
		{
			name: "simple_get",
			args: []interface{}{"GET", "key"},
		},
		{
			name: "simple_set",
			args: []interface{}{"SET", "key", "value"},
		},
		{
			name: "set_with_expiry",
			args: []interface{}{"SET", "key", "value", "EX", 3600},
		},
		{
			name: "zadd",
			args: []interface{}{"ZADD", "myset", 1.0, "member1", 2.0, "member2", 3.0, "member3"},
		},
		{
			name: "large_command",
			args: make([]interface{}, 100),
		},
	}

	// Initialize large command
	for i := range testCases[len(testCases)-1].args {
		testCases[len(testCases)-1].args[i] = "arg"
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			buf := &bytes.Buffer{}
			wr := NewWriter(buf)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				buf.Reset()
				if err := wr.WriteArgs(tc.args); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkWriteArgsParallel benchmarks concurrent writes
func BenchmarkWriteArgsParallel(b *testing.B) {
	args := []interface{}{"SET", "key", "value", "EX", 3600}

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		buf := &bytes.Buffer{}
		wr := NewWriter(buf)

		for pb.Next() {
			buf.Reset()
			if err := wr.WriteArgs(args); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkWriteCmds benchmarks writing multiple commands (pipeline)
func BenchmarkWriteCmds(b *testing.B) {
	sizes := []int{1, 10, 50, 100, 500}

	for _, size := range sizes {
		b.Run(string(rune('0'+size/10)), func(b *testing.B) {
			// Create commands
			cmds := make([][]interface{}, size)
			for i := range cmds {
				cmds[i] = []interface{}{"SET", "key", i}
			}

			buf := &bytes.Buffer{}
			wr := NewWriter(buf)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				buf.Reset()
				for _, args := range cmds {
					if err := wr.WriteArgs(args); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}

