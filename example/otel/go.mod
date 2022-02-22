module github.com/go-redis/redis/example/otel

go 1.14

replace github.com/go-redis/redis/v8 => ../..

replace github.com/go-redis/redis/extra/redisotel/v8 => ../../extra/redisotel

replace github.com/go-redis/redis/extra/rediscmd/v8 => ../../extra/rediscmd

require (
	github.com/go-redis/redis/extra/redisotel/v8 v8.11.4
	github.com/go-redis/redis/v8 v8.11.4
	github.com/uptrace/opentelemetry-go-extra/otelplay v0.1.8
	github.com/uptrace/uptrace-go v1.4.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/runtime v0.29.0 // indirect
	go.opentelemetry.io/otel v1.4.1
	go.opentelemetry.io/otel/exporters/jaeger v1.4.1 // indirect
	go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc v1.4.1 // indirect
	go.opentelemetry.io/otel/exporters/stdout/stdouttrace v1.4.1 // indirect
)
