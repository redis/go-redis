module github.com/go-redis/redis/example/otel

go 1.14

replace github.com/go-redis/redis/v9 => ../..

replace github.com/go-redis/redis/extra/redisotel/v9 => ../../extra/redisotel

replace github.com/go-redis/redis/extra/rediscmd/v9 => ../../extra/rediscmd

require (
	github.com/go-redis/redis/extra/rediscmd/v9 v9.0.0-beta.1 // indirect
	github.com/go-redis/redis/extra/redisotel/v9 v9.0.0-beta.1
	github.com/go-redis/redis/v9 v9.0.0-beta.1
	github.com/grpc-ecosystem/grpc-gateway/v2 v2.10.3 // indirect
	github.com/uptrace/opentelemetry-go-extra/otelplay v0.1.14
	go.opentelemetry.io/otel v1.7.0
	golang.org/x/net v0.0.0-20220531201128-c960675eff93 // indirect
	google.golang.org/genproto v0.0.0-20220602131408-e326c6e8e9c8 // indirect
	google.golang.org/grpc v1.47.0 // indirect
)
