module github.com/redis/go-redis/example/otel

go 1.14

replace github.com/redis/go-redis/v9 => ../..

replace github.com/redis/go-redis/extra/redisotel/v9 => ../../extra/redisotel

replace github.com/redis/go-redis/extra/rediscmd/v9 => ../../extra/rediscmd

require (
	github.com/redis/go-redis/extra/redisotel/v9 v9.0.0-rc.2
	github.com/redis/go-redis/v9 v9.0.0-rc.2
	github.com/uptrace/uptrace-go v1.11.8
	go.opentelemetry.io/otel v1.11.2
	google.golang.org/genproto v0.0.0-20230119192704-9d59e20e5cd1 // indirect
	google.golang.org/grpc v1.52.0 // indirect
)
