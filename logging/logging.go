package logging

// LogLevel represents the logging level
type LogLevel int

// Log level constants for the entire go-redis library
const (
	LogLevelError LogLevel = iota // 0 - errors only
	LogLevelWarn                  // 1 - warnings and errors
	LogLevelInfo                  // 2 - info, warnings, and errors
	LogLevelDebug                 // 3 - debug, info, warnings, and errors
)

// String returns the string representation of the log level
func (l LogLevel) String() string {
	switch l {
	case LogLevelError:
		return "ERROR"
	case LogLevelWarn:
		return "WARN"
	case LogLevelInfo:
		return "INFO"
	case LogLevelDebug:
		return "DEBUG"
	default:
		return "UNKNOWN"
	}
}

// IsValid returns true if the log level is valid
func (l LogLevel) IsValid() bool {
	return l >= LogLevelError && l <= LogLevelDebug
}
