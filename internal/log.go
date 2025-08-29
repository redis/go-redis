package internal

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/redis/go-redis/v9/logging"
)

// LogLevel is an alias to the public logging.LogLevel for internal use
type LogLevel = logging.LogLevel

// Log level constants for internal use
const (
	LogLevelError = logging.LogLevelError
	LogLevelWarn  = logging.LogLevelWarn
	LogLevelInfo  = logging.LogLevelInfo
	LogLevelDebug = logging.LogLevelDebug
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

func NewFilterLogger(substr []string) Logging {
	l := &logger{
		log: log.New(os.Stderr, "redis: ", log.LstdFlags|log.Lshortfile),
	}
	return &filterLogger{logger: l, substr: substr}
}

type filterLogger struct {
	logger Logging
	substr []string
}

func (l *filterLogger) Printf(ctx context.Context, format string, v ...interface{}) {
	msg := fmt.Sprintf(format, v...)
	for _, substr := range l.substr {
		if strings.Contains(msg, substr) {
			return
		}
	}
	if l.logger != nil {
		l.logger.Printf(ctx, format, v...)
		return
	}
}
