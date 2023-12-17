module github.com/redis/go-redis/extra/rediscensus/v9

go 1.15

replace github.com/redis/go-redis/v9 => ../..

replace github.com/redis/go-redis/extra/rediscmd/v9 => ../rediscmd

require (
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/redis/go-redis/extra/rediscmd/v9 v9.3.1
	github.com/redis/go-redis/v9 v9.3.1
	go.opencensus.io v0.24.0
)
