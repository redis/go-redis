package proto

import (
	"bytes"
	"fmt"
	"testing"
)

// BenchmarkReadStringReply benchmarks the optimized readStringReply with buffer pooling
func BenchmarkReadStringReply(b *testing.B) {
	sizes := []int{10, 50, 100, 500, 1000, 5000}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("size_%d", size), func(b *testing.B) {
			// Create a RESP bulk string reply
			value := bytes.Repeat([]byte("x"), size)
			reply := fmt.Sprintf("$%d\r\n%s\r\n", size, value)
			data := []byte(reply)

			// Reuse reader (realistic usage pattern)
			reader := bytes.NewReader(data)
			r := NewReader(reader)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				reader.Reset(data)
				r.Reset(reader)

				line, err := r.readLine()
				if err != nil {
					b.Fatal(err)
				}
				_, err = r.readStringReply(line)
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReadString benchmarks the optimized ReadString with BytesToString
func BenchmarkReadString(b *testing.B) {
	testCases := []struct {
		name  string
		reply string
	}{
		{"status", "+OK\r\n"},
		{"int", ":42\r\n"},
		{"small_string", "$5\r\nhello\r\n"},
		{"medium_string", "$100\r\n" + string(bytes.Repeat([]byte("x"), 100)) + "\r\n"},
		{"large_string", "$1000\r\n" + string(bytes.Repeat([]byte("x"), 1000)) + "\r\n"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			// Create reader once and reuse it (realistic usage pattern)
			data := []byte(tc.reply)
			reader := bytes.NewReader(data)
			r := NewReader(reader)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				// Reset reader to beginning for each iteration
				reader.Reset(data)
				r.Reset(reader)

				_, err := r.ReadString()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReadStringParallel benchmarks concurrent ReadString calls
func BenchmarkReadStringParallel(b *testing.B) {
	reply := "$100\r\n" + string(bytes.Repeat([]byte("x"), 100)) + "\r\n"
	data := []byte(reply)

	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		// Each goroutine gets its own reader (realistic usage)
		reader := bytes.NewReader(data)
		r := NewReader(reader)

		for pb.Next() {
			reader.Reset(data)
			r.Reset(reader)

			_, err := r.ReadString()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkReadInt benchmarks integer parsing
func BenchmarkReadInt(b *testing.B) {
	testCases := []struct {
		name  string
		reply string
	}{
		{"small_int", ":42\r\n"},
		{"large_int", ":9223372036854775807\r\n"},
		{"negative_int", ":-12345\r\n"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			data := []byte(tc.reply)
			reader := bytes.NewReader(data)
			r := NewReader(reader)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				reader.Reset(data)
				r.Reset(reader)

				_, err := r.ReadInt()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkReadArrayLen benchmarks array length parsing
func BenchmarkReadArrayLen(b *testing.B) {
	testCases := []struct {
		name  string
		reply string
	}{
		{"small_array", "*3\r\n"},
		{"large_array", "*1000\r\n"},
	}

	for _, tc := range testCases {
		b.Run(tc.name, func(b *testing.B) {
			data := []byte(tc.reply)
			reader := bytes.NewReader(data)
			r := NewReader(reader)

			b.ResetTimer()
			b.ReportAllocs()

			for i := 0; i < b.N; i++ {
				reader.Reset(data)
				r.Reset(reader)

				_, err := r.ReadArrayLen()
				if err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

