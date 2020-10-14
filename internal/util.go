package internal

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/go-redis/redis/v8/internal/proto"
	"github.com/go-redis/redis/v8/internal/util"
	"go.opentelemetry.io/otel/api/global"
)

func Sleep(ctx context.Context, dur time.Duration) error {
	return WithSpan(ctx, "sleep", func(ctx context.Context, span otel.Span) error {
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

//------------------------------------------------------------------------------

func WithSpan(ctx context.Context, name string, fn func(context.Context, otel.Span) error) error {
	if span := otel.SpanFromContext(ctx); !span.IsRecording() {
		return fn(ctx, span)
	}

	ctx, span := global.Tracer("github.com/go-redis/redis").Start(ctx, name)
	defer span.End()

	return fn(ctx, span)
}

func RecordError(ctx context.Context, err error) error {
	if err != proto.Nil {
		otel.SpanFromContext(ctx).RecordError(ctx, err)
	}
	return err
}
