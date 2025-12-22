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

// getLibraryVersionAttr returns the redis.client.library attribute
// This is computed once and reused to avoid repeated string formatting
func getLibraryVersionAttr() attribute.KeyValue {
	return attribute.String("redis.client.library", fmt.Sprintf("%s:%s", libraryName, redis.Version()))
}

// addServerPortIfNonDefault adds server.port attribute if port is not the default (6379)
func addServerPortIfNonDefault(attrs []attribute.KeyValue, serverPort string) []attribute.KeyValue {
	if serverPort != "" && serverPort != "6379" {
		return append(attrs, attribute.String("server.port", serverPort))
	}
	return attrs
}

// formatPoolName formats the pool name according to OpenTelemetry semantic conventions
func formatPoolName(serverAddr, serverPort, dbIndex string) string {
	poolName := serverAddr

	if serverPort != "" && serverPort != "6379" {
		poolName = fmt.Sprintf("%s:%s", poolName, serverPort)
	}

	if dbIndex != "" {
		poolName = fmt.Sprintf("%s/%s", poolName, dbIndex)
	}

	return poolName
}

// metricsRecorder implements the otel.Recorder interface
type metricsRecorder struct {
	operationDuration        metric.Float64Histogram
	connectionCountGauge     metric.Int64ObservableGauge
	connectionCreateTime     metric.Float64Histogram
	connectionRelaxedTimeout metric.Int64UpDownCounter
	connectionHandoff        metric.Int64Counter
	clientErrors             metric.Int64Counter
	maintenanceNotifications metric.Int64Counter

	connectionWaitTime         metric.Float64Histogram
	connectionUseTime          metric.Float64Histogram
	connectionClosed           metric.Int64Counter
	connectionPendingReqsGauge metric.Int64ObservableGauge

	pubsubMessages metric.Int64Counter

	streamLag metric.Float64Histogram

	// Configuration
	cfg *config

	// Client configuration for attributes (used for operation metrics only)
	serverAddr string
	serverPort string
	dbIndex    string

	// Client reference for accessing pool stats (used for async gauges)
	client redis.UniversalClient
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

	// Check if command should be included
	if r.cfg != nil && !r.cfg.isCommandIncluded(cmd.Name()) {
		return
	}

	// Convert duration to seconds (OTel convention for duration metrics)
	durationSeconds := duration.Seconds()

	// Build attributes
	attrs := []attribute.KeyValue{
		// Required attributes
		attribute.String("db.operation.name", cmd.FullName()),
		getLibraryVersionAttr(),
		attribute.Int("redis.client.operation.retry_attempts", attempts-1), // attempts-1 = retry count
		attribute.Bool("redis.client.operation.blocking", isBlockingCommand(cmd)),

		// Recommended attributes
		attribute.String("db.system.name", "redis"),
		attribute.String("server.address", r.serverAddr),
	}

	// Add server.port if not default
	attrs = addServerPortIfNonDefault(attrs, r.serverPort)

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

	// Check for net.Error interface (standard way to detect network errors)
	_, ok := err.(net.Error)
	return ok
}

// isTimeoutError checks if an error is a timeout error
func isTimeoutError(err error) bool {
	if err == nil {
		return false
	}

	// Check for net.Error with Timeout() method (standard way)
	if netErr, ok := err.(net.Error); ok {
		return netErr.Timeout()
	}

	return false
}

// splitHostPort splits a host:port string into host and port
// This is a simplified version that handles the common cases
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

// extractServerInfo extracts server address and port from connection info
// For client connections, this is the remote endpoint (server address)
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
		attribute.String("db.system.name", "redis"),
		getLibraryVersionAttr(),
	}

	// Add pool name
	poolName := formatPoolName(serverAddr, serverPort, r.dbIndex)
	attrs = append(attrs, attribute.String("db.client.connection.pool.name", poolName))

	// Record the histogram
	r.connectionCreateTime.Record(ctx, durationSeconds, metric.WithAttributes(attrs...))
}

// RecordConnectionRelaxedTimeout records when connection timeout is relaxed/unrelaxed
func (r *metricsRecorder) RecordConnectionRelaxedTimeout(
	ctx context.Context,
	delta int,
	cn redis.ConnInfo,
	poolName, notificationType string,
) {
	if r.connectionRelaxedTimeout == nil {
		return
	}

	// Extract server address from connection
	serverAddr, serverPort := extractServerInfo(cn)

	// Build attributes
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		getLibraryVersionAttr(),
	}

	// Add pool name (computed from server info, not using the passed poolName parameter)
	computedPoolName := formatPoolName(serverAddr, serverPort, r.dbIndex)
	attrs = append(attrs, attribute.String("db.client.connection.pool.name", computedPoolName))

	// Add notification type
	attrs = append(attrs, attribute.String("redis.client.connection.notification", notificationType))

	// Record the counter (delta can be +1 or -1)
	r.connectionRelaxedTimeout.Add(ctx, int64(delta), metric.WithAttributes(attrs...))
}

// RecordConnectionHandoff records when a connection is handed off to another node
func (r *metricsRecorder) RecordConnectionHandoff(
	ctx context.Context,
	cn redis.ConnInfo,
	poolName string,
) {
	if r.connectionHandoff == nil {
		return
	}

	// Extract server address from connection
	serverAddr, serverPort := extractServerInfo(cn)

	// Build attributes (connection-basic: NO peer info)
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		getLibraryVersionAttr(),
	}

	// Add pool name (computed from server info, not using the passed poolName parameter)
	// The passed poolName is just "main" or "pubsub" which is not unique enough
	computedPoolName := formatPoolName(serverAddr, serverPort, r.dbIndex)
	attrs = append(attrs, attribute.String("db.client.connection.pool.name", computedPoolName))

	// Record the counter
	r.connectionHandoff.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordError records client errors (ASK, MOVED, handshake failures, etc.)
func (r *metricsRecorder) RecordError(
	ctx context.Context,
	errorType string,
	cn redis.ConnInfo,
	statusCode string,
	isInternal bool,
	retryAttempts int,
) {
	if r.clientErrors == nil {
		return
	}

	// Extract server address and peer address from connection (may be nil for some errors)
	// For client connections, peer address is the same as server address (remote endpoint)
	var serverAddr, serverPort, peerAddr, peerPort string
	if cn != nil {
		serverAddr, serverPort = extractServerInfo(cn)
		peerAddr, peerPort = serverAddr, serverPort // Peer is same as server for client connections
	}

	// Build attributes
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		attribute.String("error.type", errorType),
		attribute.String("db.response.status_code", statusCode),
		attribute.Bool("redis.client.errors.internal", isInternal),
		attribute.Int("redis.client.operation.retry_attempts", retryAttempts),
		getLibraryVersionAttr(),
	}

	// Add server info if available
	if serverAddr != "" {
		attrs = append(attrs, attribute.String("server.address", serverAddr))
		attrs = addServerPortIfNonDefault(attrs, serverPort)
	}

	// Add peer info if available
	if peerAddr != "" {
		attrs = append(attrs, attribute.String("network.peer.address", peerAddr))
		if peerPort != "" {
			attrs = append(attrs, attribute.String("network.peer.port", peerPort))
		}
	}

	// Record the counter
	r.clientErrors.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordMaintenanceNotification records when a maintenance notification is received
func (r *metricsRecorder) RecordMaintenanceNotification(
	ctx context.Context,
	cn redis.ConnInfo,
	notificationType string,
) {
	if r.maintenanceNotifications == nil {
		return
	}

	// Extract server address and peer address from connection
	// For client connections, peer address is the same as server address (remote endpoint)
	serverAddr, serverPort := extractServerInfo(cn)
	peerAddr, peerPort := serverAddr, serverPort // Peer is same as server for client connections

	// Build attributes
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		attribute.String("server.address", serverAddr),
		getLibraryVersionAttr(),
		attribute.String("redis.client.connection.notification", notificationType),
	}

	// Add server.port if not default
	attrs = addServerPortIfNonDefault(attrs, serverPort)

	// Add peer info if available
	if peerAddr != "" {
		attrs = append(attrs, attribute.String("network.peer.address", peerAddr))
		if peerPort != "" {
			attrs = append(attrs, attribute.String("network.peer.port", peerPort))
		}
	}

	// Record the counter
	r.maintenanceNotifications.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordConnectionWaitTime records db.client.connection.wait_time metric
func (r *metricsRecorder) RecordConnectionWaitTime(
	ctx context.Context,
	duration time.Duration,
	cn redis.ConnInfo,
) {
	if r.connectionWaitTime == nil {
		return
	}

	// Extract server address from connection
	serverAddr, serverPort := extractServerInfo(cn)

	// Build attributes (connection-advanced: NO peer info, NO server.address/port)
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		getLibraryVersionAttr(),
	}

	// Add pool name
	poolName := formatPoolName(serverAddr, serverPort, r.dbIndex)
	attrs = append(attrs, attribute.String("db.client.connection.pool.name", poolName))

	// Record the histogram (duration in seconds)
	r.connectionWaitTime.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

// RecordConnectionUseTime records db.client.connection.use_time metric
func (r *metricsRecorder) RecordConnectionUseTime(
	ctx context.Context,
	duration time.Duration,
	cn redis.ConnInfo,
) {
	if r.connectionUseTime == nil {
		return
	}

	// Extract server address from connection
	serverAddr, serverPort := extractServerInfo(cn)

	// Build attributes (connection-advanced: NO peer info, NO server.address/port)
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		getLibraryVersionAttr(),
	}

	// Add pool name
	poolName := formatPoolName(serverAddr, serverPort, r.dbIndex)
	attrs = append(attrs, attribute.String("db.client.connection.pool.name", poolName))

	// Record the histogram (duration in seconds)
	r.connectionUseTime.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

// RecordConnectionClosed records redis.client.connection.closed metric
func (r *metricsRecorder) RecordConnectionClosed(
	ctx context.Context,
	cn redis.ConnInfo,
	reason string,
) {
	if r.connectionClosed == nil {
		return
	}

	// Extract server address from connection
	serverAddr, serverPort := extractServerInfo(cn)

	// Build attributes (connection-advanced: NO peer info, NO server.address/port)
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		getLibraryVersionAttr(),
	}

	// Add pool name
	poolName := formatPoolName(serverAddr, serverPort, r.dbIndex)
	attrs = append(attrs, attribute.String("db.client.connection.pool.name", poolName))

	// Add close reason
	attrs = append(attrs, attribute.String("redis.client.connection.close.reason", reason))

	// Record the counter
	r.connectionClosed.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordPubSubMessage records redis.client.pubsub.messages metric
func (r *metricsRecorder) RecordPubSubMessage(
	ctx context.Context,
	cn redis.ConnInfo,
	direction string,
	channel string,
	sharded bool,
) {
	if r.pubsubMessages == nil {
		return
	}

	// Extract server address and peer address from connection
	serverAddr, serverPort := extractServerInfo(cn)
	peerAddr, peerPort := serverAddr, serverPort

	// Build attributes
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		attribute.String("server.address", serverAddr),
		attribute.String("redis.client.pubsub.direction", direction), // "sent" or "received"
		attribute.Bool("redis.client.pubsub.sharded", sharded),
		getLibraryVersionAttr(),
	}

	// Add channel name if not hidden for cardinality reduction
	if !r.cfg.hidePubSubChannelNames && channel != "" {
		attrs = append(attrs, attribute.String("redis.client.pubsub.channel", channel))
	}

	// Add server.port if not default
	attrs = addServerPortIfNonDefault(attrs, serverPort)

	// Add peer info
	if peerAddr != "" {
		attrs = append(attrs, attribute.String("network.peer.address", peerAddr))
		if peerPort != "" {
			attrs = append(attrs, attribute.String("network.peer.port", peerPort))
		}
	}

	// Record the counter
	r.pubsubMessages.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordStreamLag records redis.client.stream.lag metric
func (r *metricsRecorder) RecordStreamLag(
	ctx context.Context,
	lag time.Duration,
	cn redis.ConnInfo,
	streamName string,
	consumerGroup string,
	consumerName string,
) {
	if r.streamLag == nil {
		return
	}

	// Extract server address and peer address from connection
	serverAddr, serverPort := extractServerInfo(cn)
	peerAddr, peerPort := serverAddr, serverPort

	// Build attributes
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		attribute.String("server.address", serverAddr),
		attribute.String("redis.client.stream.consumer_group", consumerGroup),
		attribute.String("redis.client.stream.consumer_name", consumerName),
		getLibraryVersionAttr(),
	}

	// Add stream name if not hidden for cardinality reduction
	if !r.cfg.hideStreamNames && streamName != "" {
		attrs = append(attrs, attribute.String("redis.client.stream.name", streamName))
	}

	// Add server.port if not default
	attrs = addServerPortIfNonDefault(attrs, serverPort)

	// Add peer info
	if peerAddr != "" {
		attrs = append(attrs, attribute.String("network.peer.address", peerAddr))
		if peerPort != "" {
			attrs = append(attrs, attribute.String("network.peer.port", peerPort))
		}
	}

	// Record the histogram (lag in seconds)
	r.streamLag.Record(ctx, lag.Seconds(), metric.WithAttributes(attrs...))
}
