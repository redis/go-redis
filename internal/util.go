package internal

import (
	"context"
	"fmt"
	"strconv"
	"time"
	"unicode/utf8"

	"github.com/go-redis/redis/v8/internal/proto"
	"github.com/go-redis/redis/v8/internal/util"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/trace"
)

func Sleep(ctx context.Context, dur time.Duration) error {
	return WithSpan(ctx, "sleep", func(ctx context.Context) error {
		t := time.NewTimer(dur)
		defer t.Stop()

		select {
		case <-t.C:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	})
}

func ToLower(s string) string {
	if isLower(s) {
		return s
	}

	b := make([]byte, len(s))
	for i := range b {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			c += 'a' - 'A'
		}
		b[i] = c
	}
	return util.BytesToString(b)
}

func isLower(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c >= 'A' && c <= 'Z' {
			return false
		}
	}
	return true
}

func Unwrap(err error) error {
	u, ok := err.(interface {
		Unwrap() error
	})
	if !ok {
		return nil
	}
	return u.Unwrap()
}

func AppendArg(b []byte, v interface{}) []byte {
	switch v := v.(type) {
	case nil:
		return append(b, "<nil>"...)
	case string:
		return appendUTF8String(b, v)
	case []byte:
		return appendUTF8String(b, String(v))
	case int:
		return strconv.AppendInt(b, int64(v), 10)
	case int8:
		return strconv.AppendInt(b, int64(v), 10)
	case int16:
		return strconv.AppendInt(b, int64(v), 10)
	case int32:
		return strconv.AppendInt(b, int64(v), 10)
	case int64:
		return strconv.AppendInt(b, v, 10)
	case uint:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint8:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint16:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint32:
		return strconv.AppendUint(b, uint64(v), 10)
	case uint64:
		return strconv.AppendUint(b, v, 10)
	case float32:
		return strconv.AppendFloat(b, float64(v), 'f', -1, 64)
	case float64:
		return strconv.AppendFloat(b, v, 'f', -1, 64)
	case bool:
		if v {
			return append(b, "true"...)
		}
		return append(b, "false"...)
	case time.Time:
		return v.AppendFormat(b, time.RFC3339Nano)
	default:
		return append(b, fmt.Sprint(v)...)
	}
}

func appendUTF8String(b []byte, s string) []byte {
	for _, r := range s {
		b = appendRune(b, r)
	}
	return b
}

func appendRune(b []byte, r rune) []byte {
	if r < utf8.RuneSelf {
		switch c := byte(r); c {
		case '\n':
			return append(b, "\\n"...)
		case '\r':
			return append(b, "\\r"...)
		default:
			return append(b, c)
		}
	}

	l := len(b)
	b = append(b, make([]byte, utf8.UTFMax)...)
	n := utf8.EncodeRune(b[l:l+utf8.UTFMax], r)
	b = b[:l+n]

	return b
}

//------------------------------------------------------------------------------

func WithSpan(ctx context.Context, name string, fn func(context.Context) error) error {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return fn(ctx)
	}

	ctx, span := global.Tracer("github.com/go-redis/redis").Start(ctx, name)
	defer span.End()

	return fn(ctx)
}

func RecordError(ctx context.Context, err error) error {
	if err != proto.Nil {
		trace.SpanFromContext(ctx).RecordError(ctx, err)
	}
	return err
}
