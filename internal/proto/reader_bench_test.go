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
			
			b.ResetTimer()
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				r := NewReader(bytes.NewReader([]byte(reply)))
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
			b.ReportAllocs()
			
			for i := 0; i < b.N; i++ {
				r := NewReader(bytes.NewReader([]byte(tc.reply)))
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
	
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			r := NewReader(bytes.NewReader([]byte(reply)))
			_, err := r.ReadString()
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

