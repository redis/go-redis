module github.com/go-redis/redis/example/otel

go 1.14

replace github.com/go-redis/redis/v9 => ../..

replace github.com/go-redis/redis/extra/redisotel/v9 => ../../extra/redisotel

replace github.com/go-redis/redis/extra/rediscmd/v9 => ../../extra/rediscmd

require (
	github.com/go-redis/redis/extra/redisotel/v9 v9.0.0-rc.2
	github.com/go-redis/redis/v9 v9.0.0-rc.2
	github.com/uptrace/uptrace-go v1.11.6
	go.opentelemetry.io/otel v1.11.1
)
