module github.com/go-redis/redis/extra/rediscensus/v8

go 1.15

replace github.com/go-redis/redis/v8 => ../..

replace github.com/go-redis/redis/extra/rediscmd/v8 => ../rediscmd

require (
	github.com/go-redis/redis/extra/rediscmd/v8 v8.11.4
	github.com/go-redis/redis/v8 v8.11.4
	go.opencensus.io v0.23.0
)
