package internal

import (
	"context"
	"fmt"
	"log"
	"os"
)

// TODO (ned): Revisit logging
// Add more standardized approach with log levels and configurability

type Logging interface {
	Printf(ctx context.Context, format string, v ...interface{})
}

type DefaultLogger struct {
	log *log.Logger
}

func (l *DefaultLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintf(format, v...))
}

func NewDefaultLogger() Logging {
	return &DefaultLogger{
		log: log.New(os.Stderr, "redis: ", log.LstdFlags|log.Lshortfile),
	}
}

// Logger calls Output to print to the stderr.
// Arguments are handled in the manner of fmt.Print.
var Logger Logging = NewDefaultLogger()
