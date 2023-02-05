package internal

import (
	"context"
	"fmt"
	"log"
	"os"
)

type Logging interface {
	Printf(ctx context.Context, format string, v ...interface{})
	Println(ctx context.Context, v ...interface{})
	Fatalf(ctx context.Context, format string, v ...interface{})
	Fatalln(ctx context.Context, v ...interface{})
}

type logger struct {
	log *log.Logger
}

func (l *logger) Printf(ctx context.Context, format string, v ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintf(format, v...))
}

func (l *logger) Println(ctx context.Context, v ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintln(v...))
}

func (l *logger) Fatalf(ctx context.Context, format string, v ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintf(format, v...))
	os.Exit(1)
}

func (l *logger) Fatalln(ctx context.Context, v ...interface{}) {
	_ = l.log.Output(2, fmt.Sprintln(v...))
	os.Exit(1)
}

// Logger calls Output to print to the stderr.
// Arguments are handled in the manner of fmt.Print.
var Logger Logging = &logger{
	log: log.New(os.Stderr, "redis: ", log.LstdFlags|log.Lshortfile),
}
