package rediscensus

import (
	"context"
	"net"

	"go.opencensus.io/trace"

	"github.com/redis/go-redis/extra/rediscmd/v9"
	"github.com/redis/go-redis/v9"
)

type TracingHook struct{}

var _ redis.Hook = (*TracingHook)(nil)

func NewTracingHook() *TracingHook {
	return new(TracingHook)
}

func (TracingHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		ctx, span := trace.StartSpan(ctx, "dial")
		defer span.End()

		span.AddAttributes(
			trace.StringAttribute("db.system", "redis"),
			trace.StringAttribute("network", network),
			trace.StringAttribute("addr", addr),
		)

		conn, err := next(ctx, network, addr)
		if err != nil {
			recordErrorOnOCSpan(ctx, span, err)

			return nil, err
		}

		return conn, nil
	}
}

func (TracingHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		ctx, span := trace.StartSpan(ctx, cmd.FullName())
		defer span.End()

		span.AddAttributes(
			trace.StringAttribute("db.system", "redis"),
			trace.StringAttribute("redis.cmd", rediscmd.CmdString(cmd)),
		)

		err := next(ctx, cmd)
		if err != nil {
			recordErrorOnOCSpan(ctx, span, err)
			return err
		}

		if err = cmd.Err(); err != nil {
			recordErrorOnOCSpan(ctx, span, err)
		}

		return nil
	}
}

func (TracingHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return next
}

func recordErrorOnOCSpan(ctx context.Context, span *trace.Span, err error) {
	if err != redis.Nil {
		span.AddAttributes(trace.BoolAttribute("error", true))
		span.Annotate([]trace.Attribute{trace.StringAttribute("Error", "redis error")}, err.Error())
	}
}
