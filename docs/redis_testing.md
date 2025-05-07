# Redis Testing Guide

## Running Tests

### 1. Setup Test Environment

```bash
# Start Docker containers for testing
make docker.start

# Stop Docker containers when done
make docker.stop
```

### 2. Environment Variables

```bash
# Redis version and image configuration
CLIENT_LIBS_TEST_IMAGE=redislabs/client-libs-test:rs-7.4.0-v2  # Default Redis Stack image
REDIS_VERSION=7.2  # Default Redis version

# Cluster configuration
RE_CLUSTER=false  # Set to true for RE testing
RCE_DOCKER=false  # Set to true for Docker-based Redis CE testing

```

### 3. Running Tests

```bash

# Run tests with race detection, as executed in the CI
make test.ci


### 4. Test Coverage

```bash
# Generate coverage report
go test -coverprofile=coverage.out

# View coverage report in browser
go tool cover -html=coverage.out
```

## Writing Tests

### 1. Basic Test Structure

```go
package redis_test

import (
    . "github.com/bsm/ginkgo/v2"
    . "github.com/bsm/gomega"
    "github.com/redis/go-redis/v9"
)

var _ = Describe("Redis Client", func() {
    var client *redis.Client
    var ctx = context.Background()

    BeforeEach(func() {
        client = redis.NewClient(&redis.Options{
            Addr: ":6379",
        })
    })

    AfterEach(func() {
        client.Close()
    })

    It("should handle basic operations", func() {
        err := client.Set(ctx, "key", "value", 0).Err()
        Expect(err).NotTo(HaveOccurred())

        val, err := client.Get(ctx, "key").Result()
        Expect(err).NotTo(HaveOccurred())
        Expect(val).To(Equal("value"))
    })
})
```

### 2. Test Organization

```go
// Use Describe for test groups
Describe("Redis Client", func() {
    // Use Context for different scenarios
    Context("when connection is established", func() {
        // Use It for individual test cases
        It("should handle basic operations", func() {
            // Test implementation
        })
    })
})
```

### 3. Common Test Patterns

#### Testing Success Cases
```go
It("should succeed", func() {
    err := client.Set(ctx, "key", "value", 0).Err()
    Expect(err).NotTo(HaveOccurred())
})
```

#### Testing Error Cases
```go
It("should return error", func() {
    _, err := client.Get(ctx, "nonexistent").Result()
    Expect(err).To(Equal(redis.Nil))
})
```

#### Testing Timeouts
```go
It("should timeout", func() {
    ctx, cancel := context.WithTimeout(ctx, time.Millisecond)
    defer cancel()

    err := client.Ping(ctx).Err()
    Expect(err).To(HaveOccurred())
})
```

### 4. Best Practices

1. **Test Structure**
   - Use descriptive test names
   - Group related tests together
   - Keep tests focused and simple
   - Clean up resources in AfterEach

2. **Assertions**
   - Use Gomega's Expect syntax
   - Be specific in assertions
   - Test both success and failure cases
   - Include error checking

3. **Resource Management**
   - Close connections in AfterEach
   - Clean up test data
   - Handle timeouts properly
   - Manage test isolation 