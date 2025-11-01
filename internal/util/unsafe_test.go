package util

import (
	"reflect"
	"runtime"
	"sync"
	"testing"
)

var (
	_tmpBytes  []byte
	_tmpString string
)

func TestBytesToString(t *testing.T) {
	tests := []struct {
		input  string
		expect string
	}{
		{
			input:  "string",
			expect: "string",
		},
		{
			input:  "",
			expect: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			input := []byte(tt.input)
			if result := BytesToString(input); !reflect.DeepEqual(tt.expect, result) {
				t.Errorf("BytesToString: Expected = %v, Got = %v", tt.expect, result)
			}

			if len(tt.input) == 0 {
				return
			}

			input[0] = 'x'
			if result := BytesToString(input); reflect.DeepEqual(tt.expect, result) {
				t.Errorf("BytesToString: expected not equal: %v", tt.expect)
			}
		})
	}
}

func TestStringToBytes(t *testing.T) {
	tests := []struct {
		input  string
		expect []byte
	}{
		{
			input:  "string",
			expect: []byte("string"),
		},
		{
			input:  "",
			expect: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			if result := StringToBytes(tt.input); !reflect.DeepEqual(tt.expect, result) {
				t.Errorf("StringToBytes: Expected = %v, Got = %v", tt.expect, result)
			}
		})
	}
}

func TestBytesToStringGC(t *testing.T) {
	var (
		expect = t.Name()
		x      string
		wg     sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		tmp := append([]byte(nil), t.Name()...)
		x = BytesToString(tmp)
	}()
	wg.Wait()

	for i := 0; i < 100; i++ {
		runtime.GC()
	}

	if !reflect.DeepEqual(expect, x) {
		t.Errorf("Expected = %v, Got = %v", expect, x)
	}
}

func TestStringToBytesGC(t *testing.T) {
	var (
		expect = []byte(t.Name())
		x      []byte
		wg     sync.WaitGroup
	)

	wg.Add(1)
	go func() {
		defer wg.Done()
		tmp := append([]byte(nil), t.Name()...)
		x = StringToBytes(string(tmp))
	}()
	wg.Wait()

	for i := 0; i < 100; i++ {
		runtime.GC()
	}
	if !reflect.DeepEqual(expect, x) {
		t.Errorf("Expected = %v, Got = %v", expect, x)
	}
}

func BenchmarkStringToBytes(b *testing.B) {
	input := b.Name()

	b.Run("copy", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_tmpBytes = []byte(input)
		}
	})

	b.Run("unsafe", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_tmpBytes = StringToBytes(input)
		}
	})
}

func BenchmarkBytesToString(b *testing.B) {
	input := []byte(b.Name())

	b.Run("copy", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_tmpString = string(input)
		}
	})

	b.Run("unsafe", func(b *testing.B) {
		b.ReportAllocs()

		for i := 0; i < b.N; i++ {
			_tmpString = BytesToString(input)
		}
	})
}
