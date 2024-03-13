module github.com/redis/go-redis/extra/rediscmd/v9

go 1.15

replace github.com/redis/go-redis/v9 => ../..

require (
	github.com/bsm/ginkgo/v2 v2.12.0
	github.com/bsm/gomega v1.27.10
	github.com/redis/go-redis/v9 v9.5.1
)
