# Command examples for redis.io

These examples appear on the [Redis documentation](https://redis.io) site as part of the tabbed examples interface.

## How to add examples

- Create a Go test file with a meaningful name in the current folder. 
- Create a single method prefixed with `Example` and write your test in it.
- Find out what's the example id for the example you're creating and add it as the first line of the file `// EXAMPLE: set_and_get`. See more info below under the [Example](#example) section.
- We're using the [Testable Examples](https://go.dev/blog/examples) feature of Go to test the desired output has been written to stdout.
- To run the tests start a Redis server locally on port 6379 and run `go test`. (This will be improved and aligned with the way we run the main test suite).

### Special markup

#### EXAMPLE:
Every file should contain an **example id**, so it can be matched to the documentation page it should show up on. You should be able to find this id in the command's [documentation page](https://github.com/redis/redis-doc/tree/master/commands). If one hasn't been added yet, reach out to the `redis-doc` repo maintainers and agree on a new id.


#### HIDE_START and HIDE_END
Should be used to hide imports, connection creation and other bootstrapping code that is not the focal part of the example.

Example:

```go
// EXAMPLE: set_and_get
// HIDE_START
package example_commands_test

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func ExampleSetGet() {
	ctx := context.Background()

	rdb := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "", // no password docs
		DB:       0,  // use default DB
	})

	// HIDE_END
	err := rdb.Set(ctx, "bike:1", "Process 134", 0).Err()
	if err != nil {
		panic(err)
	}

	fmt.Println("OK")

	value, err := rdb.Get(ctx, "bike:1").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println(value)
	// HIDE_START

	// Output: OK
	// Process 134
}

// HIDE_END
```

#### REMOVE_START and REMOVE_END
Should be used to **remove** code from the resulting code snippet published on redis.io.
This markup can be useful to remove assertions, and any eventual other testing blocks.

## How to test the examples

- Start a Redis server locally on port 6379
- CD into the `example/docs` directory
- Run `go test` to test all examples in the current folder.
- Run `go test filename.go` to test a single file

## Missing

- We need to set up a way to automatically bring up a Redis server on port 6379 before the test suite, flush the db between tests and stop the server after the test suite.
- We should be able to test with more than one Redis version
