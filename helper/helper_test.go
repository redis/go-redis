package helper

import "testing"

// Golden values from Redis DIGEST command:
// redis-cli SET testkey myvalue && redis-cli DIGEST testkey
// Returns: "5a32b091fa5dafe7" (hex) = 6499451353266237415 (decimal)
//
// Redis source (t_string.c):
//
//	hash = XXH3_64bits(o->ptr, sdslen(o->ptr));  // No seed parameter!
const (
	goldenTestValue   = "myvalue"
	goldenRedisDigest = uint64(0x5a32b091fa5dafe7) // 6499451353266237415
)

func TestDigestString_RedisCompatibility(t *testing.T) {
	digest := DigestString(goldenTestValue)

	if digest != goldenRedisDigest {
		t.Errorf("DigestString(%q) = 0x%016x, want 0x%016x (Redis DIGEST)",
			goldenTestValue, digest, goldenRedisDigest)
	}
}

func TestDigestBytes_RedisCompatibility(t *testing.T) {
	digest := DigestBytes([]byte(goldenTestValue))

	if digest != goldenRedisDigest {
		t.Errorf("DigestBytes(%q) = 0x%016x, want 0x%016x (Redis DIGEST)",
			goldenTestValue, digest, goldenRedisDigest)
	}
}

func TestDigestString_EqualsDigestBytes(t *testing.T) {
	testCases := []string{
		"",
		"a",
		"hello",
		"hello world",
		"The quick brown fox jumps over the lazy dog",
		string(make([]byte, 1024)),
	}

	for _, s := range testCases {
		stringDigest := DigestString(s)
		bytesDigest := DigestBytes([]byte(s))

		if stringDigest != bytesDigest {
			t.Errorf("DigestString(%q) = 0x%016x, DigestBytes = 0x%016x, mismatch!",
				s, stringDigest, bytesDigest)
		}
	}
}

// Benchmark to verify performance characteristics
func BenchmarkDigestString(b *testing.B) {
	sizes := []struct {
		name string
		data string
	}{
		{"8B", "12345678"},
		{"64B", "0123456789012345678901234567890123456789012345678901234567890123"},
		{"1KB", string(make([]byte, 1024))},
	}

	for _, tc := range sizes {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(tc.data)))
			for i := 0; i < b.N; i++ {
				_ = DigestString(tc.data)
			}
		})
	}
}

func BenchmarkDigestBytes(b *testing.B) {
	sizes := []struct {
		name string
		data []byte
	}{
		{"8B", make([]byte, 8)},
		{"64B", make([]byte, 64)},
		{"1KB", make([]byte, 1024)},
	}

	for _, tc := range sizes {
		b.Run(tc.name, func(b *testing.B) {
			b.SetBytes(int64(len(tc.data)))
			for i := 0; i < b.N; i++ {
				_ = DigestBytes(tc.data)
			}
		})
	}
}

