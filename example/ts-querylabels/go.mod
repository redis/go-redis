module github.com/redis/go-redis/example/ts-querylabels

go 1.26.0

replace github.com/redis/go-redis/v9 => ../..

require github.com/redis/go-redis/v9 v9.23.0-beta.1

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.12.0 // indirect
	golang.org/x/sys v0.48.0 // indirect
)
