GO_MOD_DIRS := $(shell find . -type f -name 'go.mod' -exec dirname {} \; | sort)
export REDIS_VERSION := "7.4"

docker.start:
	docker compose --profile all up -d

docker.stop:
	docker compose --profile all down

test:
	$(MAKE) docker.start
	$(MAKE) test.ci
	$(MAKE) docker.stop

test.ci:
	$(eval GO_VERSION := $(shell go version | cut -d " " -f 3 | cut -d. -f2))
	set -e; for dir in $(GO_MOD_DIRS); do \
	  if echo "$${dir}" | grep -q "./example" && [ "$(GO_VERSION)" = "19" ]; then \
	    echo "Skipping go test in $${dir} due to Go version 1.19 and dir contains ./example"; \
	    continue; \
	  fi; \
	  echo "go test in $${dir}"; \
	  (cd "$${dir}" && \
	    go mod tidy -compat=1.18 && \
	    go test ./... -short -race -v && \
	    go test -v -coverprofile=coverage.txt -covermode=atomic ./... && \
	    go vet); \
	done
	cd internal/customvet && go build .
	go vet -vettool ./internal/customvet/customvet

bench:
	go test ./... -test.run=NONE -test.bench=. -test.benchmem

.PHONY: all test bench fmt

build:
	go build .

fmt:
	gofumpt -w ./
	goimports -w  -local github.com/redis/go-redis ./

go_mod_tidy:
	set -e; for dir in $(GO_MOD_DIRS); do \
	  echo "go mod tidy in $${dir}"; \
	  (cd "$${dir}" && \
	    go get -u ./... && \
	    go mod tidy -compat=1.18); \
	done
