package example_commands_test

import (
	"fmt"
	"os"
	"strconv"
	"strings"
)

var RedisVersion float64

func init() {
	// read REDIS_VERSION from env
	RedisVersion, _ = strconv.ParseFloat(strings.Trim(os.Getenv("REDIS_VERSION"), "\""), 64)
	fmt.Printf("REDIS_VERSION: %.1f\n", RedisVersion)
}
