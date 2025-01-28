module entra_id

go 1.22.0

toolchain go1.23.1

replace github.com/go-redis/entra_id => ./

require (
	github.com/bsm/ginkgo/v2 v2.12.0
	github.com/bsm/gomega v1.27.10
	github.com/go-redis/entra_id v0.0.0-00010101000000-000000000000
)
