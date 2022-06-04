module github.com/go-redis/redis/extra/redisotel/v9

go 1.15

replace github.com/go-redis/redis/v9 => ../..

replace github.com/go-redis/redis/extra/rediscmd/v9 => ../rediscmd

require (
	go.opentelemetry.io/otel v1.5.0
	go.opentelemetry.io/otel/sdk v1.4.1
	go.opentelemetry.io/otel/trace v1.5.0
)
