package internal

import (
	"fmt"
	"log"
	"os"
)

type Logging interface {
	Printf(format string, v ...interface{})
}

type logger struct {
	log *log.Logger
}

func (l *logger) Printf(format string, v ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintf(format, v...))
}

// Logger calls Output to print to the stderr.
// Arguments are handled in the manner of fmt.Print.
var Logger Logging = &logger{
	log: log.New(os.Stderr, "redis: ", log.LstdFlags|log.Lshortfile),
}
