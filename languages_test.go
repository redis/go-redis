package redis_test

import (
	"context"
	"fmt"
	"testing"
	"unicode/utf8"

	"github.com/redis/go-redis/v9"
)

func newLanguagesTestClient(t *testing.T) *redis.Client {
	t.Helper()
	client := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})
	if err := client.Ping(context.Background()).Err(); err != nil {
		client.Close()
		t.Skipf("Redis not available: %v", err)
	}
	return client
}

// TestLanguagesSetGet verifies that Indonesian Bahasa and Thai strings
// round-trip through Redis SET/GET without any data loss or corruption.
func TestLanguagesSetGet(t *testing.T) {
	ctx := context.Background()
	client := newLanguagesTestClient(t)
	defer client.Close()

	cases := []struct {
		name  string
		key   string
		value string
	}{
		// Indonesian Bahasa — Latin script (ASCII-compatible)
		{"indonesian_greeting", "lang-test-id-1", "Halo, dunia! Selamat pagi, apa kabar?"},
		{"indonesian_thanks", "lang-test-id-2", "Terima kasih banyak"},
		{"indonesian_welcome", "lang-test-id-3", "Selamat datang di toko kami"},

		// Thai — multi-byte Unicode (U+0E00–U+0E7F, 3 bytes per rune in UTF-8)
		{"thai_hello_world", "lang-test-th-1", "สวัสดีชาวโลก"},
		{"thai_welcome", "lang-test-th-2", "ยินดีต้อนรับ"},
		{"thai_thanks", "lang-test-th-3", "ขอบคุณมาก สบายดีไหม"},

		// Thai string used as the Redis key itself
		{"thai_key", "สวัสดี-key", "value stored under a Thai key"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client.Del(ctx, tc.key)

			if err := client.Set(ctx, tc.key, tc.value, 0).Err(); err != nil {
				t.Fatalf("SET failed: %v", err)
			}
			got, err := client.Get(ctx, tc.key).Result()
			if err != nil {
				t.Fatalf("GET failed: %v", err)
			}
			if got != tc.value {
				t.Errorf("round-trip mismatch:\n  want: %q\n   got: %q", tc.value, got)
			}
		})
	}
}

// TestLanguagesStrLen verifies that Redis STRLEN returns the byte count
// (not the character/rune count) for multi-byte Unicode strings.
// Thai characters are 3 bytes each in UTF-8.
func TestLanguagesStrLen(t *testing.T) {
	ctx := context.Background()
	client := newLanguagesTestClient(t)
	defer client.Close()

	cases := []struct {
		name  string
		value string
	}{
		{"indonesian", "Halo, dunia!"},
		{"thai_hello", "สวัสดี"},        // 6 runes × 3 bytes = 18 bytes
		{"thai_world", "ชาวโลก"},        // 6 runes × 3 bytes = 18 bytes
		{"thai_sentence", "สวัสดีชาวโลก"}, // 12 runes × 3 bytes = 36 bytes
		{"mixed", "Hi สวัสดี"},          // 3 ASCII + 1 space + 6×3 Thai = 22 bytes
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := fmt.Sprintf("lang-strlen-%s", tc.name)
			client.Del(ctx, key)

			if err := client.Set(ctx, key, tc.value, 0).Err(); err != nil {
				t.Fatalf("SET failed: %v", err)
			}

			wantBytes := int64(len(tc.value))
			wantRunes := utf8.RuneCountInString(tc.value)

			gotBytes, err := client.StrLen(ctx, key).Result()
			if err != nil {
				t.Fatalf("STRLEN failed: %v", err)
			}
			if gotBytes != wantBytes {
				t.Errorf("STRLEN byte count: want %d, got %d (rune count is %d, value=%q)",
					wantBytes, gotBytes, wantRunes, tc.value)
			}
		})
	}
}

// TestLanguagesAppend verifies that APPEND works correctly with multi-byte
// Unicode strings: the result is the exact concatenation and the returned
// byte length matches.
func TestLanguagesAppend(t *testing.T) {
	ctx := context.Background()
	client := newLanguagesTestClient(t)
	defer client.Close()

	cases := []struct {
		name   string
		first  string
		second string
	}{
		{"indonesian", "Selamat ", "pagi"},
		{"thai", "สวัสดี", "ชาวโลก"},
		{"thai_mixed", "สวัสดี ", "world"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			key := fmt.Sprintf("lang-append-%s", tc.name)
			client.Del(ctx, key)

			if err := client.Set(ctx, key, tc.first, 0).Err(); err != nil {
				t.Fatalf("SET failed: %v", err)
			}

			wantFull := tc.first + tc.second
			wantBytes := int64(len(wantFull))

			gotBytes, err := client.Append(ctx, key, tc.second).Result()
			if err != nil {
				t.Fatalf("APPEND failed: %v", err)
			}
			if gotBytes != wantBytes {
				t.Errorf("APPEND returned length %d, want %d", gotBytes, wantBytes)
			}

			got, err := client.Get(ctx, key).Result()
			if err != nil {
				t.Fatalf("GET after APPEND failed: %v", err)
			}
			if got != wantFull {
				t.Errorf("after APPEND:\n  want: %q\n   got: %q", wantFull, got)
			}
		})
	}
}

// TestLanguagesMSetMGet verifies that bulk SET/GET operations preserve
// Indonesian Bahasa and Thai strings exactly.
func TestLanguagesMSetMGet(t *testing.T) {
	ctx := context.Background()
	client := newLanguagesTestClient(t)
	defer client.Close()

	keyValues := map[string]string{
		"lang-mset-id-1": "Halo, dunia!",
		"lang-mset-id-2": "Terima kasih banyak",
		"lang-mset-th-1": "สวัสดีชาวโลก",
		"lang-mset-th-2": "ยินดีต้อนรับ",
		"lang-mset-th-3": "ขอบคุณมาก",
	}

	keys := make([]string, 0, len(keyValues))
	args := make([]interface{}, 0, len(keyValues)*2)
	for k, v := range keyValues {
		keys = append(keys, k)
		args = append(args, k, v)
		client.Del(ctx, k)
	}

	if err := client.MSet(ctx, args...).Err(); err != nil {
		t.Fatalf("MSET failed: %v", err)
	}

	results, err := client.MGet(ctx, keys...).Result()
	if err != nil {
		t.Fatalf("MGET failed: %v", err)
	}
	if len(results) != len(keys) {
		t.Fatalf("MGET returned %d results, want %d", len(results), len(keys))
	}

	for i, key := range keys {
		want := keyValues[key]
		got, ok := results[i].(string)
		if !ok {
			t.Errorf("key %q: MGET returned nil or non-string", key)
			continue
		}
		if got != want {
			t.Errorf("key %q:\n  want: %q\n   got: %q", key, want, got)
		}
	}
}
