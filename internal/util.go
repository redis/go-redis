package internal

import (
	"context"
	"reflect"
	"time"

	"github.com/go-redis/redis/v8/internal/util"
	"go.opentelemetry.io/otel/api/core"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/trace"
	"google.golang.org/grpc/codes"
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

var (
	logTypeKey    = core.Key("log.type")
	logMessageKey = core.Key("log.message")
)

func WithSpan(ctx context.Context, name string, fn func(context.Context) error) error {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return fn(ctx)
	}

	ctx, span := global.TraceProvider().Tracer("go-redis").Start(ctx, name)
	defer span.End()

	if err := fn(ctx); err != nil {
		span.SetStatus(codes.Internal)
		span.AddEvent(ctx, "error",
			logTypeKey.String(reflect.TypeOf(err).String()),
			logMessageKey.String(err.Error()),
		)
		return err
	}
	return nil
}
