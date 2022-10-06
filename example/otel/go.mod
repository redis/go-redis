module github.com/go-redis/redis/example/otel

go 1.14

replace github.com/go-redis/redis/v9 => ../..

replace github.com/go-redis/redis/extra/redisotel/v9 => ../../extra/redisotel

replace github.com/go-redis/redis/extra/rediscmd/v9 => ../../extra/rediscmd

require (
	github.com/go-redis/redis/extra/redisotel/v9 v9.0.0-beta.2
	github.com/go-redis/redis/v9 v9.0.0-beta.2
	github.com/uptrace/opentelemetry-go-extra/otelplay v0.1.15
	go.opentelemetry.io/otel v1.8.0
	golang.org/x/net v0.0.0-20220728030405-41545e8bf201 // indirect
	google.golang.org/genproto v0.0.0-20220725144611-272f38e5d71b // indirect
)
