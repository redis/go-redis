package redisotel

import (
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.24.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/redis/go-redis/extra/rediscmd/v9"
	"github.com/redis/go-redis/v9"
)

const (
	instrumName = "github.com/redis/go-redis/extra/redisotel"
)

func InstrumentTracing(rdb redis.UniversalClient, opts ...TracingOption) error {
	switch rdb := rdb.(type) {
	case *redis.Client:
		opt := rdb.Options()
		connString := formatDBConnString(opt.Network, opt.Addr)
		opts = addServerAttributes(opts, opt.Addr)
		rdb.AddHook(newTracingHook(connString, opts...))
		return nil
	case *redis.ClusterClient:
		rdb.OnNewNode(func(rdb *redis.Client) {
			opt := rdb.Options()
			opts = addServerAttributes(opts, opt.Addr)
			connString := formatDBConnString(opt.Network, opt.Addr)
			rdb.AddHook(newTracingHook(connString, opts...))
		})
		return nil
	case *redis.Ring:
		rdb.OnNewNode(func(rdb *redis.Client) {
			opt := rdb.Options()
			opts = addServerAttributes(opts, opt.Addr)
			connString := formatDBConnString(opt.Network, opt.Addr)
			rdb.AddHook(newTracingHook(connString, opts...))
		})
		return nil
	default:
		return fmt.Errorf("redisotel: %T not supported", rdb)
	}
}

type tracingHook struct {
	conf *config

	spanOpts []trace.SpanStartOption
}

var _ redis.Hook = (*tracingHook)(nil)

func newTracingHook(connString string, opts ...TracingOption) *tracingHook {
	baseOpts := make([]baseOption, len(opts))
	for i, opt := range opts {
		baseOpts[i] = opt
	}
	conf := newConfig(baseOpts...)

	if conf.tracer == nil {
		conf.tracer = conf.tp.Tracer(
			instrumName,
			trace.WithInstrumentationVersion("semver:"+redis.Version()),
		)
	}
	if connString != "" {
		conf.attrs = append(conf.attrs, semconv.DBConnectionString(connString))
	}

	return &tracingHook{
		conf: conf,

		spanOpts: []trace.SpanStartOption{
			trace.WithSpanKind(trace.SpanKindClient),
			trace.WithAttributes(conf.attrs...),
		},
	}
}

func (th *tracingHook) shouldTrace(cmd redis.Cmder) bool {
	if _, excluded := th.conf.excludedCommands[strings.ToUpper(cmd.Name())]; excluded {
		return false
	}

	if th.conf.unifiedProcessFilter != nil {
		return !th.conf.unifiedProcessFilter(cmd)
	}

	if th.conf.filterProcess != nil {
		return !th.conf.filterProcess(cmd)
	}

	return true
}

func (th *tracingHook) shouldTracePipeline(cmds []redis.Cmder) bool {
	for _, cmd := range cmds {
		if _, excluded := th.conf.excludedCommands[strings.ToUpper(cmd.Name())]; excluded {
			return false
		}
	}

	if th.conf.unifiedProcessPipelineFilter != nil {
		return !th.conf.unifiedProcessPipelineFilter(cmds)
	}

	if th.conf.filterProcessPipeline != nil {
		return !th.conf.filterProcessPipeline(cmds)
	}

	return true
}

func (th *tracingHook) shouldTraceDial(network, addr string) bool {
	if th.conf.unifiedDialFilter != nil {
		return !th.conf.unifiedDialFilter(network, addr)
	}

	return !th.conf.filterDial
}

func (th *tracingHook) DialHook(hook redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {

		if !th.shouldTraceDial(network, addr) {
			return hook(ctx, network, addr)
		}

		ctx, span := th.conf.tracer.Start(ctx, "redis.dial", th.spanOpts...)
		defer span.End()

		conn, err := hook(ctx, network, addr)
		if err != nil {
			recordError(span, err)
			return nil, err
		}
		return conn, nil
	}
}

func (th *tracingHook) ProcessHook(hook redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {

		if !th.shouldTrace(cmd) {
			return hook(ctx, cmd)
		}

		attrs := make([]attribute.KeyValue, 0, 8)
		if th.conf.callerEnabled {
			if fn, file, line := funcFileLine("github.com/redis/go-redis"); fn != "" {
				attrs = append(attrs,
					semconv.CodeFunction(fn),
					semconv.CodeFilepath(file),
					semconv.CodeLineNumber(line),
				)
			}
		}

		if th.conf.dbStmtEnabled {
			if cmdString := rediscmd.CmdString(cmd); cmdString != "" {
				attrs = append(attrs, semconv.DBStatement(cmdString))
			}
		}

		opts := th.spanOpts
		opts = append(opts, trace.WithAttributes(attrs...))

		ctx, span := th.conf.tracer.Start(ctx, cmd.FullName(), opts...)
		defer span.End()

		if err := hook(ctx, cmd); err != nil {
			recordError(span, err)
			return err
		}
		return nil
	}
}

func (th *tracingHook) ProcessPipelineHook(
	hook redis.ProcessPipelineHook,
) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {

		if !th.shouldTracePipeline(cmds) {
			return hook(ctx, cmds)
		}

		attrs := make([]attribute.KeyValue, 0, 8)
		attrs = append(attrs,
			attribute.Int("db.redis.num_cmd", len(cmds)),
		)

		if th.conf.callerEnabled {
			fn, file, line := funcFileLine("github.com/redis/go-redis")
			attrs = append(attrs,
				semconv.CodeFunction(fn),
				semconv.CodeFilepath(file),
				semconv.CodeLineNumber(line),
			)
		}

		summary, cmdsString := rediscmd.CmdsString(cmds)
		if th.conf.dbStmtEnabled {
			attrs = append(attrs, semconv.DBStatement(cmdsString))
		}

		opts := th.spanOpts
		opts = append(opts, trace.WithAttributes(attrs...))

		ctx, span := th.conf.tracer.Start(ctx, "redis.pipeline "+summary, opts...)
		defer span.End()

		if err := hook(ctx, cmds); err != nil {
			recordError(span, err)
			return err
		}
		return nil
	}
}

func recordError(span trace.Span, err error) {
	if err != redis.Nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}

func formatDBConnString(network, addr string) string {
	if network == "tcp" {
		network = "redis"
	}
	return fmt.Sprintf("%s://%s", network, addr)
}

func funcFileLine(pkg string) (string, string, int) {
	const depth = 16
	var pcs [depth]uintptr
	n := runtime.Callers(3, pcs[:])
	ff := runtime.CallersFrames(pcs[:n])

	var fn, file string
	var line int
	for {
		f, ok := ff.Next()
		if !ok {
			break
		}
		fn, file, line = f.Function, f.File, f.Line
		if !strings.Contains(fn, pkg) {
			break
		}
	}

	if ind := strings.LastIndexByte(fn, '/'); ind != -1 {
		fn = fn[ind+1:]
	}

	return fn, file, line
}

// addServerAttributes adds database span attributes following semantic conventions
// for server address and port as recommended by OpenTelemetry.
// https://opentelemetry.io/docs/specs/semconv/database/database-spans/#connection-level-attributes
func addServerAttributes(opts []TracingOption, addr string) []TracingOption {
	host, portString, err := net.SplitHostPort(addr)
	if err != nil {
		return opts
	}

	if host != "" {
		opts = append(opts, WithAttributes(
			semconv.ServerAddress(host),
		))
	}

	if portString != "" {
		if port, err := strconv.Atoi(portString); err == nil && port > 0 {
			opts = append(opts, WithAttributes(
				semconv.ServerPort(port),
			))
		}
	}

	return opts
}
