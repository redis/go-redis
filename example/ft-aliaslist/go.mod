module github.com/redis/go-redis/example/ft-aliaslist

go 1.24

replace github.com/redis/go-redis/v9 => ../..

require github.com/redis/go-redis/v9 v9.22.0-beta.1

require (
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	go.uber.org/atomic v1.11.0 // indirect
)
