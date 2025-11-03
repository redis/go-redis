package redisotel

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

const (
	// Library name for redis.client.library attribute
	libraryName = "go-redis"
)

// metricsRecorder implements the otel.Recorder interface
type metricsRecorder struct {
	operationDuration    metric.Float64Histogram
	connectionCount      metric.Int64UpDownCounter
	connectionCreateTime metric.Float64Histogram

	// Client configuration for attributes (used for operation metrics only)
	serverAddr string
	serverPort string
	dbIndex    string
}

// RecordOperationDuration records db.client.operation.duration metric
func (r *metricsRecorder) RecordOperationDuration(
	ctx context.Context,
	duration time.Duration,
	cmd redis.Cmder,
	attempts int,
	cn redis.ConnInfo,
) {
	if r.operationDuration == nil {
		return
	}

	// Convert duration to seconds (OTel convention for duration metrics)
	durationSeconds := duration.Seconds()

	// Build attributes
	attrs := []attribute.KeyValue{
		// Required attributes
		attribute.String("db.operation.name", cmd.FullName()),
		attribute.String("redis.client.library", fmt.Sprintf("%s:%s", libraryName, redis.Version())),
		attribute.Int("redis.client.operation.retry_attempts", attempts-1), // attempts-1 = retry count
		attribute.Bool("redis.client.operation.blocking", isBlockingCommand(cmd)),

		// Recommended attributes
		attribute.String("db.system", "redis"),
		attribute.String("server.address", r.serverAddr),
	}

	// Add server.port if not default
	if r.serverPort != "" && r.serverPort != "6379" {
		attrs = append(attrs, attribute.String("server.port", r.serverPort))
	}

	// Add db.namespace (database index) if available
	if r.dbIndex != "" {
		attrs = append(attrs, attribute.String("db.namespace", r.dbIndex))
	}

	// Add network.peer.address and network.peer.port from connection
	if cn != nil {
		remoteAddr := cn.RemoteAddr()
		if remoteAddr != nil {
			peerAddr, peerPort := splitHostPort(remoteAddr.String())
			if peerAddr != "" {
				attrs = append(attrs, attribute.String("network.peer.address", peerAddr))
			}
			if peerPort != "" {
				attrs = append(attrs, attribute.String("network.peer.port", peerPort))
			}
		}
	}

	// Add error.type if command failed
	if err := cmd.Err(); err != nil {
		attrs = append(attrs, attribute.String("error.type", classifyError(err)))
	}

	// Add db.response.status_code if error is a Redis error
	if err := cmd.Err(); err != nil {
		if statusCode := extractRedisErrorPrefix(err); statusCode != "" {
			attrs = append(attrs, attribute.String("db.response.status_code", statusCode))
		}
	}

	// Record the histogram
	r.operationDuration.Record(ctx, durationSeconds, metric.WithAttributes(attrs...))
}

// isBlockingCommand checks if a command is a blocking operation
// Blocking commands have a timeout parameter and include: BLPOP, BRPOP, BRPOPLPUSH, BLMOVE,
// BZPOPMIN, BZPOPMAX, BZMPOP, BLMPOP, XREAD with BLOCK, XREADGROUP with BLOCK
func isBlockingCommand(cmd redis.Cmder) bool {
	name := strings.ToLower(cmd.Name())

	// Commands that start with 'b' and are blocking
	if strings.HasPrefix(name, "b") {
		switch name {
		case "blpop", "brpop", "brpoplpush", "blmove", "bzpopmin", "bzpopmax", "bzmpop", "blmpop":
			return true
		}
	}

	// XREAD and XREADGROUP with BLOCK option
	if name == "xread" || name == "xreadgroup" {
		args := cmd.Args()
		for i, arg := range args {
			if argStr, ok := arg.(string); ok {
				if strings.ToLower(argStr) == "block" && i+1 < len(args) {
					return true
				}
			}
		}
	}

	return false
}

// classifyError returns the error.type attribute value
// Format: <category>:<subcategory>:<error_name>
func classifyError(err error) string {
	if err == nil {
		return ""
	}

	errStr := err.Error()

	// Network errors
	if isNetworkError(err) {
		return fmt.Sprintf("network:%s", errStr)
	}

	// Timeout errors
	if isTimeoutError(err) {
		return "timeout"
	}

	// Redis errors (start with error prefix like ERR, WRONGTYPE, etc.)
	if prefix := extractRedisErrorPrefix(err); prefix != "" {
		return fmt.Sprintf("redis:%s", prefix)
	}

	// Generic error
	return errStr
}

// extractRedisErrorPrefix extracts the Redis error prefix (e.g., "ERR", "WRONGTYPE")
// Redis errors typically start with an uppercase prefix followed by a space
func extractRedisErrorPrefix(err error) string {
	if err == nil {
		return ""
	}

	errStr := err.Error()

	// Redis errors typically start with an uppercase prefix
	// Examples: "ERR ...", "WRONGTYPE ...", "CLUSTERDOWN ..."
	parts := strings.SplitN(errStr, " ", 2)
	if len(parts) > 0 {
		prefix := parts[0]
		// Check if it's all uppercase (Redis error convention)
		if prefix == strings.ToUpper(prefix) && len(prefix) > 0 {
			return prefix
		}
	}

	return ""
}

// isNetworkError checks if an error is a network-related error
func isNetworkError(err error) bool {
	if err == nil {
		return false
	}

	// Check for net.Error interface
	if _, ok := err.(net.Error); ok {
		return true
	}

	// Check error message for common network error patterns
	errStr := strings.ToLower(err.Error())
	networkPatterns := []string{
		"connection refused",
		"connection reset",
		"broken pipe",
		"no route to host",
		"network is unreachable",
		"i/o timeout",
		"eof",
	}

	for _, pattern := range networkPatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}

// isTimeoutError checks if an error is a timeout error
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	// Check for net.Error with Timeout() method
	if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
		return true
	}

	// Check error message
	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "timeout") || strings.Contains(errStr, "deadline exceeded")
}

// splitHostPort splits a host:port string into host and port
func splitHostPort(addr string) (host, port string) {
	// Handle Unix sockets
	if strings.HasPrefix(addr, "/") || strings.HasPrefix(addr, "@") {
		return addr, ""
	}

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// If split fails, return the whole address as host
		return addr, ""
	}

	return host, port
}

// parseAddr parses a Redis address into host and port
func parseAddr(addr string) (host, port string) {
	// Handle Unix sockets
	if strings.HasPrefix(addr, "/") || strings.HasPrefix(addr, "unix://") {
		return addr, ""
	}

	// Remove protocol prefix if present
	addr = strings.TrimPrefix(addr, "redis://")
	addr = strings.TrimPrefix(addr, "rediss://")

	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		// No port specified, use default
		return addr, "6379"
	}

	return host, port
}

// formatDBIndex formats the database index as a string
func formatDBIndex(db int) string {
	if db < 0 {
		return ""
	}
	return strconv.Itoa(db)
}

// RecordConnectionStateChange records a change in connection state
// This is called from the pool when connections transition between states
func (r *metricsRecorder) RecordConnectionStateChange(
	ctx context.Context,
	cn redis.ConnInfo,
	fromState, toState string,
) {
	if r.connectionCount == nil {
		return
	}

	// Extract server address from connection
	serverAddr, serverPort := extractServerInfo(cn)

	// Build base attributes
	attrs := []attribute.KeyValue{
		attribute.String("db.system", "redis"),
		attribute.String("server.address", serverAddr),
	}

	// Add server.port if not default
	if serverPort != "" && serverPort != "6379" {
		attrs = append(attrs, attribute.String("server.port", serverPort))
	}

	// Decrement old state (if not empty)
	if fromState != "" {
		fromAttrs := append([]attribute.KeyValue{}, attrs...)
		fromAttrs = append(fromAttrs, attribute.String("state", fromState))
		r.connectionCount.Add(ctx, -1, metric.WithAttributes(fromAttrs...))
	}

	// Increment new state
	if toState != "" {
		toAttrs := append([]attribute.KeyValue{}, attrs...)
		toAttrs = append(toAttrs, attribute.String("state", toState))
		r.connectionCount.Add(ctx, 1, metric.WithAttributes(toAttrs...))
	}
}

// extractServerInfo extracts server address and port from connection info
func extractServerInfo(cn redis.ConnInfo) (addr, port string) {
	if cn == nil {
		return "", ""
	}

	remoteAddr := cn.RemoteAddr()
	if remoteAddr == nil {
		return "", ""
	}

	addrStr := remoteAddr.String()
	host, portStr := parseAddr(addrStr)
	return host, portStr
}

// RecordConnectionCreateTime records the time it took to create a new connection
func (r *metricsRecorder) RecordConnectionCreateTime(
	ctx context.Context,
	duration time.Duration,
	cn redis.ConnInfo,
) {
	if r.connectionCreateTime == nil {
		return
	}

	// Convert duration to seconds (OTel convention)
	durationSeconds := duration.Seconds()

	// Extract server address from connection
	serverAddr, serverPort := extractServerInfo(cn)

	// Build attributes
	attrs := []attribute.KeyValue{
		attribute.String("db.system", "redis"),
		attribute.String("server.address", serverAddr),
		attribute.String("redis.client.library", fmt.Sprintf("%s:%s", libraryName, redis.Version())),
	}

	// Add server.port if not default
	if serverPort != "" && serverPort != "6379" {
		attrs = append(attrs, attribute.String("server.port", serverPort))
	}

	// Add pool name (using server.address:server.port format)
	poolName := serverAddr
	if serverPort != "" && serverPort != "6379" {
		poolName = fmt.Sprintf("%s:%s", serverAddr, serverPort)
	}
	attrs = append(attrs, attribute.String("db.client.connection.pool.name", poolName))

	// Record the histogram
	r.connectionCreateTime.Record(ctx, durationSeconds, metric.WithAttributes(attrs...))
}
