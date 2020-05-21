package internal

import (
	"log"
	"os"
)

type Logging interface {
	Printf(format string, v ...interface{})
}

var Logger Logging = log.New(os.Stderr, "redis: ", log.LstdFlags|log.Lshortfile)
