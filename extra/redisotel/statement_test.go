package redisotel

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"testing"

	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	semconv "go.opentelemetry.io/otel/semconv/v1.7.0"

	"github.com/redis/go-redis/v9"
)

func getDBStatement(spanData tracetest.SpanStub) string {
	for _, attr := range spanData.Attributes {
		if attr.Key == semconv.DBStatementKey {
			return attr.Value.AsString()
		}
	}
	return ""
}

func TestTracingHook_StatementTruncation_Default(t *testing.T) {
	imsb := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
	hook := newTracingHook(
		"redis://localhost:6379",
		WithTracerProvider(provider),
	)

	payload1MB := strings.Repeat("x", 1024*1024) // 1MB

	t.Run("large 1MB payload is truncated by default", func(t *testing.T) {
		defer imsb.Reset()

		cmd := redis.NewCmd(context.Background(), "SET", "mykey", payload1MB)
		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])

		remaining := DefaultDBStatementMaxLen - len("SET mykey ")
		expectedPrefix := "SET mykey " + strings.Repeat("x", remaining)
		expectedSuffix := fmt.Sprintf("…(+%d bytes)", 1024*1024-remaining)
		expected := expectedPrefix + expectedSuffix

		if stmt != expected {
			t.Fatalf("expected statement length %d, got %d: %q", len(expected), len(stmt), stmt[:100]+"...")
		}
	})

	t.Run("short payload is not truncated", func(t *testing.T) {
		defer imsb.Reset()

		cmd := redis.NewCmd(context.Background(), "SET", "mykey", "hello world")
		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])
		assertEqual(t, "SET mykey hello world", stmt)
	})

	t.Run("payload at exact limit is not truncated", func(t *testing.T) {
		defer imsb.Reset()

		exactVal := strings.Repeat("b", DefaultDBStatementMaxLen-len("SET mykey "))
		cmd := redis.NewCmd(context.Background(), "SET", "mykey", exactVal)
		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])
		assertEqual(t, "SET mykey "+exactVal, stmt)
	})

	t.Run("payload one byte above limit is truncated", func(t *testing.T) {
		defer imsb.Reset()

		remaining := DefaultDBStatementMaxLen - len("SET mykey ")
		val := strings.Repeat("c", remaining+1)
		cmd := redis.NewCmd(context.Background(), "SET", "mykey", val)
		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])
		expected := "SET mykey " + strings.Repeat("c", remaining) + "…(+1 bytes)"
		assertEqual(t, expected, stmt)
	})
}

func TestTracingHook_StatementTruncation_CustomLimit(t *testing.T) {
	payload1MB := strings.Repeat("y", 1024*1024)

	t.Run("custom limit 64 bytes matches ndyakov prefix and marker", func(t *testing.T) {
		imsb := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
		hook := newTracingHook(
			"redis://localhost:6379",
			WithTracerProvider(provider),
			WithDBStatementMaxLen(64),
		)

		cmd := redis.NewCmd(context.Background(), "SET", "mykey", payload1MB)
		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])
		remaining := 64 - len("SET mykey ")
		expected := "SET mykey " + strings.Repeat("y", remaining) + fmt.Sprintf("…(+%d bytes)", 1024*1024-remaining)
		assertEqual(t, expected, stmt)
	})

	t.Run("custom limit 128 bytes", func(t *testing.T) {
		imsb := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
		hook := newTracingHook(
			"redis://localhost:6379",
			WithTracerProvider(provider),
			WithDBStatementMaxLen(128),
		)

		val200 := strings.Repeat("z", 200)
		cmd := redis.NewCmd(context.Background(), "SET", "k", val200)
		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])
		remaining := 128 - len("SET k ")
		expected := "SET k " + strings.Repeat("z", remaining) + fmt.Sprintf("…(+%d bytes)", 200-remaining)
		assertEqual(t, expected, stmt)
	})

	t.Run("custom limit 256 does not truncate 200 bytes", func(t *testing.T) {
		imsb := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
		hook := newTracingHook(
			"redis://localhost:6379",
			WithTracerProvider(provider),
			WithDBStatementMaxLen(256),
		)

		val200 := strings.Repeat("z", 200)
		cmd := redis.NewCmd(context.Background(), "SET", "k", val200)
		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])
		assertEqual(t, "SET k "+val200, stmt)
	})

	t.Run("custom limit 0 disables truncation (full data opt-in)", func(t *testing.T) {
		imsb := tracetest.NewInMemoryExporter()
		provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
		hook := newTracingHook(
			"redis://localhost:6379",
			WithTracerProvider(provider),
			WithDBStatementMaxLen(0),
		)

		val100K := strings.Repeat("w", 100000)
		cmd := redis.NewCmd(context.Background(), "SET", "mykey", val100K)
		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])
		assertEqual(t, "SET mykey "+val100K, stmt)
	})
}

func TestTracingHook_StatementTruncation_Credentials(t *testing.T) {
	imsb := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
	hook := newTracingHook(
		"redis://localhost:6379",
		WithTracerProvider(provider),
		WithDBStatementMaxLen(128),
		WithCommandFilter(func(cmd redis.Cmder) bool {
			// Do not filter out AUTH for this test so we can verify redaction
			return false
		}),
	)

	tests := []struct {
		name     string
		args     []interface{}
		expected string
	}{
		{
			name:     "AUTH password",
			args:     []interface{}{"AUTH", "supersecretpassword"},
			expected: "AUTH <redacted>",
		},
		{
			name:     "AUTH username password",
			args:     []interface{}{"AUTH", "alice", "supersecretpassword"},
			expected: "AUTH alice <redacted>",
		},
		{
			name:     "AUTH large 1MB password",
			args:     []interface{}{"AUTH", strings.Repeat("p", 1024*1024)},
			expected: "AUTH <redacted>",
		},
		{
			name:     "HELLO with AUTH",
			args:     []interface{}{"HELLO", 3, "AUTH", "alice", "supersecret", "SETNAME", "app"},
			expected: "HELLO 3 AUTH alice <redacted> SETNAME app",
		},
		{
			name:     "CONFIG SET requirepass",
			args:     []interface{}{"CONFIG", "SET", "requirepass", "supersecret"},
			expected: "CONFIG SET requirepass <redacted>",
		},
		{
			name:     "CONFIG SET masterauth",
			args:     []interface{}{"CONFIG", "SET", "masterauth", "supersecret"},
			expected: "CONFIG SET masterauth <redacted>",
		},
		{
			name:     "ACL SETUSER with password rule",
			args:     []interface{}{"ACL", "SETUSER", "bob", "on", ">s3cret", "~app:*", "+get"},
			expected: "ACL SETUSER bob on <redacted> ~app:* +get",
		},
		{
			name:     "MIGRATE with AUTH",
			args:     []interface{}{"MIGRATE", "127.0.0.1", 6379, "key", 0, 100, "AUTH", "s3cret"},
			expected: "MIGRATE 127.0.0.1 6379 key 0 100 AUTH <redacted>",
		},
		{
			name:     "SENTINEL SET auth-pass",
			args:     []interface{}{"SENTINEL", "SET", "mymaster", "auth-pass", "s3cret"},
			expected: "SENTINEL SET mymaster auth-pass <redacted>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defer imsb.Reset()

			cmd := redis.NewCmd(context.Background(), tt.args...)
			processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
				return nil
			})

			if err := processHook(context.Background(), cmd); err != nil {
				t.Fatal(err)
			}

			assertEqual(t, 1, len(imsb.GetSpans()))
			stmt := getDBStatement(imsb.GetSpans()[0])
			assertEqual(t, tt.expected, stmt)
		})
	}
}

func TestTracingHook_StatementTruncation_Pipeline(t *testing.T) {
	imsb := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
	hook := newTracingHook(
		"redis://localhost:6379",
		WithTracerProvider(provider),
		WithDBStatementMaxLen(100),
	)

	t.Run("pipeline total statement cap cuts subsequent commands", func(t *testing.T) {
		defer imsb.Reset()

		cmds := []redis.Cmder{
			redis.NewCmd(context.Background(), "SET", "key1", "val1_12345678901234567890"),
			redis.NewCmd(context.Background(), "SET", "key2", "val2_12345678901234567890"),
			redis.NewCmd(context.Background(), "SET", "key3", "val3_12345678901234567890"),
			redis.NewCmd(context.Background(), "SET", "key4", "val4_12345678901234567890"),
			redis.NewCmd(context.Background(), "SET", "key5", "val5_12345678901234567890"),
		}

		pipelineHook := hook.ProcessPipelineHook(func(ctx context.Context, cmds []redis.Cmder) error {
			return nil
		})

		if err := pipelineHook(context.Background(), cmds); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])

		if !strings.HasSuffix(stmt, "\n…") {
			t.Fatalf("expected pipeline statement to be capped with trailing '\\n…', got: %q", stmt)
		}
	})

	t.Run("pipeline command with 1MB argument is truncated", func(t *testing.T) {
		defer imsb.Reset()

		payload1MB := strings.Repeat("p", 1024*1024)
		cmds := []redis.Cmder{
			redis.NewCmd(context.Background(), "SET", "mykey", payload1MB),
			redis.NewCmd(context.Background(), "GET", "mykey"),
		}

		pipelineHook := hook.ProcessPipelineHook(func(ctx context.Context, cmds []redis.Cmder) error {
			return nil
		})

		if err := pipelineHook(context.Background(), cmds); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])

		remaining := 100 - len("SET mykey ")
		expectedCmd0 := "SET mykey " + strings.Repeat("p", remaining) + fmt.Sprintf("…(+%d bytes)", 1024*1024-remaining)
		expected := expectedCmd0 + "\n…"
		assertEqual(t, expected, stmt)
	})
}

func TestTracingHook_StatementTruncation_Binary(t *testing.T) {
	imsb := tracetest.NewInMemoryExporter()
	provider := sdktrace.NewTracerProvider(sdktrace.WithSyncer(imsb))
	hook := newTracingHook(
		"redis://localhost:6379",
		WithTracerProvider(provider),
		WithDBStatementMaxLen(64),
	)

	t.Run("large binary byte slice is hex encoded prefix and truncated", func(t *testing.T) {
		defer imsb.Reset()

		binaryData := bytes.Repeat([]byte{0x00, 0x01, 0x02, 0xff}, 250*1024) // 1MB binary
		cmd := redis.NewCmd(context.Background(), "SET", "bin", binaryData)
		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])

		if !strings.HasPrefix(stmt, "SET bin ") {
			t.Fatalf("expected prefix 'SET bin ', got %q", stmt[:20])
		}
		remainingBytes := (64 - len("SET bin ")) / 2
		expectedSuffix := fmt.Sprintf("…(+%d bytes)", len(binaryData)-remainingBytes)
		if !strings.HasSuffix(stmt, expectedSuffix) {
			t.Fatalf("expected suffix %q, got %q", expectedSuffix, stmt[len(stmt)-30:])
		}
	})

	t.Run("binary payload with remaining budget of 1 byte is not hex expanded", func(t *testing.T) {
		defer imsb.Reset()

		hook1 := newTracingHook(
			"redis://localhost:6379",
			WithTracerProvider(provider),
			WithDBStatementMaxLen(9),
		)

		binaryData := bytes.Repeat([]byte{0x00, 0x01, 0x02, 0xff}, 250*1024)
		cmd := redis.NewCmd(context.Background(), "SET", "key", binaryData)
		processHook := hook1.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])

		expected := fmt.Sprintf("SET key …(+%d bytes)", len(binaryData))
		assertEqual(t, expected, stmt)
	})

	t.Run("large 5MB JSON string with small limit does not copy entire payload", func(t *testing.T) {
		defer imsb.Reset()

		largeJSON := `{"data":"` + strings.Repeat("a", 5*1024*1024) + `"}`
		cmd := redis.NewCmd(context.Background(), "SET", "json", largeJSON)
		processHook := hook.ProcessHook(func(ctx context.Context, cmd redis.Cmder) error {
			return nil
		})

		if err := processHook(context.Background(), cmd); err != nil {
			t.Fatal(err)
		}

		assertEqual(t, 1, len(imsb.GetSpans()))
		stmt := getDBStatement(imsb.GetSpans()[0])

		remaining := 64 - len("SET json ")
		expected := "SET json " + largeJSON[:remaining] + fmt.Sprintf("…(+%d bytes)", len(largeJSON)-remaining)
		assertEqual(t, expected, stmt)
	})
}
