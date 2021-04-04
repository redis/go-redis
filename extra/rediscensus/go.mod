module github.com/go-redis/redis/extra/rediscensus

go 1.15

replace github.com/go-redis/redis/extra/rediscmd => ../rediscmd

require (
	github.com/go-redis/redis/extra/rediscmd v0.2.0
	github.com/go-redis/redis/v8 v8.4.4
	go.opencensus.io v0.22.6
)
