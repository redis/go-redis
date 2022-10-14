module github.com/go-redis/redis/extra/redisotel/v9

go 1.15

replace github.com/go-redis/redis/v9 => ../..

replace github.com/go-redis/redis/extra/rediscmd/v9 => ../rediscmd

require (
	github.com/go-redis/redis/extra/rediscmd/v9 v9.0.0-rc.1
	github.com/go-redis/redis/v9 v9.0.0-rc.1
	go.opentelemetry.io/otel v1.10.0
	go.opentelemetry.io/otel/metric v0.32.1
	go.opentelemetry.io/otel/sdk v1.9.0
	go.opentelemetry.io/otel/trace v1.10.0
)
