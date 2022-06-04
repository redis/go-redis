module github.com/go-redis/redis/extra/rediscmd/v9

go 1.15

replace github.com/go-redis/redis/v9 => ../..

require (
	github.com/go-redis/redis/v9 v9.0.0-beta.1
	github.com/onsi/ginkgo v1.16.5
	github.com/onsi/gomega v1.19.0
)
