package redisext

import (
	"context"
	"strings"

	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis/v8/internal"
	"go.opentelemetry.io/otel/api/global"
	"go.opentelemetry.io/otel/api/kv"
	"go.opentelemetry.io/otel/api/trace"
)

type OpenTelemetryHook struct{}

var _ redis.Hook = OpenTelemetryHook{}

func (OpenTelemetryHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}

	b := make([]byte, 32)
	b = appendCmd(b, cmd)

	tracer := global.Tracer("github.com/go-redis/redis")
	ctx, span := tracer.Start(ctx, cmd.FullName())
	span.SetAttributes(
		kv.String("db.system", "redis"),
		kv.String("redis.cmd", internal.String(b)),
	)

	return ctx, nil
}

func (OpenTelemetryHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	span := trace.SpanFromContext(ctx)
	if err := cmd.Err(); err != nil {
		internal.RecordError(ctx, err)
	}
	span.End()
	return nil
}

func (OpenTelemetryHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	if !trace.SpanFromContext(ctx).IsRecording() {
		return ctx, nil
	}

	const numCmdLimit = 100
	const numNameLimit = 10

	seen := make(map[string]struct{}, len(cmds))
	unqNames := make([]string, 0, len(cmds))

	b := make([]byte, 0, 32*len(cmds))

	for i, cmd := range cmds {
		if i > numCmdLimit {
			break
		}

		if i > 0 {
			b = append(b, '\n')
		}
		b = appendCmd(b, cmd)

		if len(unqNames) >= numNameLimit {
			continue
		}

		name := cmd.FullName()
		if _, ok := seen[name]; !ok {
			seen[name] = struct{}{}
			unqNames = append(unqNames, name)
		}
	}

	tracer := global.Tracer("github.com/go-redis/redis")
	ctx, span := tracer.Start(ctx, "pipeline "+strings.Join(unqNames, " "))
	span.SetAttributes(
		kv.String("db.system", "redis"),
		kv.Int("redis.num_cmd", len(cmds)),
		kv.String("redis.cmds", internal.String(b)),
	)

	return ctx, nil
}

func (OpenTelemetryHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	span := trace.SpanFromContext(ctx)
	if err := cmds[0].Err(); err != nil {
		internal.RecordError(ctx, err)
	}
	span.End()
	return nil
}

func appendCmd(b []byte, cmd redis.Cmder) []byte {
	const lenLimit = 64

	for i, arg := range cmd.Args() {
		if i > 0 {
			b = append(b, ' ')
		}

		start := len(b)
		b = internal.AppendArg(b, arg)
		if len(b)-start > lenLimit {
			b = append(b[:start+lenLimit], "..."...)
		}
	}

	if err := cmd.Err(); err != nil {
		b = append(b, ": "...)
		b = append(b, err.Error()...)
	}

	return b
}
