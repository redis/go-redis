module github.com/go-redis/redis/extra/rediscensus/v9

go 1.15

replace github.com/go-redis/redis/v9 => ../..

replace github.com/go-redis/redis/extra/rediscmd/v9 => ../rediscmd

require (
	github.com/go-redis/redis/extra/rediscmd/v9 v9.0.0-rc.2
	github.com/go-redis/redis/v9 v9.0.0-rc.2
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	go.opencensus.io v0.23.0
)
