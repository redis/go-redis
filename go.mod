module github.com/redis/go-redis/v9

go 1.22

require (
	github.com/bsm/ginkgo/v2 v2.12.0
	github.com/bsm/gomega v1.27.10
	github.com/cespare/xxhash/v2 v2.3.0
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f
	github.com/zeebo/xxh3 v1.1.0
)

require go.uber.org/atomic v1.11.0

require (
	github.com/klauspost/cpuid/v2 v2.2.10 // indirect
	golang.org/x/sys v0.30.0 // indirect
)

retract (
	v9.15.1 // This version is used to retract v9.15.0
	v9.15.0 // This version was accidentally released. It is identical to 9.15.0-beta.2
	v9.7.2 // This version was accidentally released. Please use version 9.7.3 instead.
	v9.5.4 // This version was accidentally released. Please use version 9.6.0 instead.
	v9.5.3 // This version was accidentally released. Please use version 9.6.0 instead.
)
