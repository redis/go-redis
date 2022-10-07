module github.com/go-redis/redis/example/otel

go 1.14

replace github.com/go-redis/redis/v9 => ../..

replace github.com/go-redis/redis/extra/redisotel/v9 => ../../extra/redisotel

replace github.com/go-redis/redis/extra/rediscmd/v9 => ../../extra/rediscmd

require (
	github.com/go-redis/redis/extra/redisotel/v9 v9.0.0-beta.4
	github.com/go-redis/redis/v9 v9.0.0-beta.4
	github.com/uptrace/opentelemetry-go-extra/otelplay v0.1.16
	github.com/uptrace/uptrace-go v1.10.0 // indirect
	go.opentelemetry.io/otel v1.10.0
	go.opentelemetry.io/otel/exporters/jaeger v1.10.0 // indirect
)
