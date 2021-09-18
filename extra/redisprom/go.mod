module github.com/go-redis/redis/extra/redisprom/v8

go 1.15

replace github.com/go-redis/redis/v8 => ../..

require (
	github.com/go-redis/redis/v8 v8.11.3
	github.com/prometheus/client_golang v1.11.0
)
