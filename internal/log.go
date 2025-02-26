package internal

import (
	"context"
	"fmt"
	"log"
	"os"
)

// msg is the main log message.
// keysAndValues are the extra context you want to add to your log entries.
// To provide additional information for the context in which the log entry was created.
// These are passed as variadic arguments so you can pass any number of them.

type Logging interface {
	Printf(ctx context.Context, format string, v ...interface{})
	Info(ctx context.Context, msg string, keysAndValues ...interface{})
	Error(ctx context.Context, msg string, keysAndValues ...interface{})
}

type logger struct {
	log *log.Logger
}

func (l *logger) Printf(ctx context.Context, format string, v ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintf(format, v...))
}
func (l *logger) Info(ctx context.Context, msg string, keysAndValues ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintf(msg, keysAndValues...))
}

func (l *logger) Error(ctx context.Context, msg string, keysAndValues ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintf("ERROR: "+msg, keysAndValues...))
}

// Logger calls Output to print to the stderr.
// Arguments are handled in the manner of fmt.Print.
var Logger Logging = &logger{
	log: log.New(os.Stderr, "redis: ", log.LstdFlags|log.Lshortfile),
}
