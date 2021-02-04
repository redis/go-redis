module github.com/go-redis/redis/example/otel

go 1.14

require (
	github.com/go-redis/redis/v8 v8.4.4
	github.com/go-redis/redis/v8/extra/redisotel v0.0.0-00010101000000-000000000000
	go.opentelemetry.io/otel v0.16.0
	go.opentelemetry.io/otel/exporters/stdout v0.16.0
	go.opentelemetry.io/otel/sdk v0.16.0
)

replace github.com/go-redis/redis/v8 => ../../

replace github.com/go-redis/redis/v8/extra/redisotel => ../../extra/redisotel
