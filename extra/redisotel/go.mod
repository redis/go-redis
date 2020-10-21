module github.com/go-redis/redis/extra/rediscensus

go 1.15

replace github.com/go-redis/redis/extra/rediscmd => ../rediscmd

require (
	github.com/go-redis/redis/extra/rediscmd v0.0.0-00010101000000-000000000000
	github.com/go-redis/redis/v8 v8.3.2
	go.opentelemetry.io/otel v0.13.0
)
