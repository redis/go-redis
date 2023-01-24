module github.com/redis/go-redis/extra/rediscmd/v9

go 1.15

replace github.com/redis/go-redis/v9 => ../..

require (
	github.com/onsi/ginkgo v1.16.5
	github.com/onsi/gomega v1.25.0
	github.com/redis/go-redis/v9 v9.0.0-rc.2
)
