module github.com/go-redis/redis/example/otel

go 1.14

replace github.com/go-redis/redis/v8 => ../..

replace github.com/go-redis/redis/extra/redisotel/v8 => ../../extra/redisotel

replace github.com/go-redis/redis/extra/rediscmd/v8 => ../../extra/rediscmd

require (
	github.com/go-redis/redis/extra/redisotel/v8 v8.11.4
	github.com/go-redis/redis/v8 v8.11.4
	github.com/uptrace/opentelemetry-go-extra/otelplay v0.1.3
	github.com/uptrace/uptrace-go v1.1.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.26.1 // indirect
	go.opentelemetry.io/otel v1.1.0
	go.opentelemetry.io/proto/otlp v0.10.0 // indirect
	golang.org/x/net v0.0.0-20211104170005-ce137452f963 // indirect
	golang.org/x/sys v0.0.0-20211103235746-7861aae1554b // indirect
	google.golang.org/genproto v0.0.0-20211104193956-4c6863e31247 // indirect
	google.golang.org/grpc v1.42.0 // indirect
)
