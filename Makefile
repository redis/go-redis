GO_MOD_DIRS := $(shell find . -type f -name 'go.mod' -exec dirname {} \; | sort)
REDIS_PORT ?= 6379

redis:
	docker run --name redis-stack -d -p ${REDIS_PORT}:6379 -it redis/redis-stack-server:edge

test: redis
	set -e; for dir in $(GO_MOD_DIRS); do \
	  echo "go test in $${dir}"; \
	  (cd "$${dir}" && \
	    go mod tidy -compat=1.19 && \
	    go test && \
	    go test ./... -short -race && \
	    go test ./... -run=NONE -bench=. -benchmem && \
	    env GOOS=linux GOARCH=386 go test && \
	    go vet); \
	done
	cd internal/customvet && go build .
	go vet -vettool ./internal/customvet/customvet

# testdeps: testdata/redis/src/redis-server

bench: redis
	go test ./... -test.run=NONE -test.bench=. -test.benchmem

.PHONY: all test redis bench

fmt:
	gofmt -w -s ./
	goimports -w  -local github.com/redis/go-redis ./

go_mod_tidy:
	set -e; for dir in $(GO_MOD_DIRS); do \
	  echo "go mod tidy in $${dir}"; \
	  (cd "$${dir}" && \
	    go get -u ./... && \
	    go mod tidy -compat=1.19); \
	done
