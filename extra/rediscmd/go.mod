module github.com/go-redis/redis/extra/rediscmd/v8

go 1.15

replace github.com/go-redis/redis/v8 => ../..

require (
	github.com/go-redis/redis/v8 v8.11.3
	github.com/onsi/ginkgo v1.16.4
	github.com/onsi/gomega v1.15.0
)
