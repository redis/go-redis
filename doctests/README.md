# Command examples for redis.io

These examples appear on the [Redis documentation](https://redis.io) site as part of the tabbed examples interface.

## How to add examples

- Create a Go test file with a meaningful name in the current folder. 
- Create a single method prefixed with `Example` and write your test in it.
- Find out what's the example id for the example you're creating and add it as the first line of the file `// EXAMPLE: set_and_get`. See more info below under the [Example](#example) section.
- We're using the [Testable Examples](https://go.dev/blog/examples) feature of Go to test the desired output has been written to stdout.
- To run the tests start a Redis server locally on port 6379 and run `go test`. (This will be improved and aligned with the way we run the main test suite).

### Special markup

See https://github.com/redis-stack/redis-stack-website#readme for more details.

## How to test the examples

- Start a Redis server locally on port 6379
- CD into the `example/docs` directory
- Run `go test` to test all examples in the current folder.
- Run `go test filename.go` to test a single file

