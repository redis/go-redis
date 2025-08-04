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

type logger struct {
	log *log.Logger
}

func (l *logger) Printf(ctx context.Context, format string, v ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintf(format, v...))
}

// Logger calls Output to print to the stderr.
// Arguments are handled in the manner of fmt.Print.
var Logger Logging = &logger{
	log: log.New(os.Stderr, "redis: ", log.LstdFlags|log.Lshortfile),
}

// VoidLogger is a logger that does nothing.
// Used to disable logging and thus speed up the library.
type VoidLogger struct{}

func (v *VoidLogger) Printf(_ context.Context, _ string, _ ...interface{}) {
	// do nothing
}

var _ Logging = (*VoidLogger)(nil)
