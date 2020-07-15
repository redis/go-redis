module github.com/go-redis/redis/example

go 1.14

require (
	github.com/go-redis/redis/v8 v8.0.0-beta.5
	go.opentelemetry.io/otel v0.7.0
)

replace github.com/go-redis/redis/v8 => ../../
