package redisotel

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
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
func formatPoolName(serverAddr, serverPort string) string {
	poolName := serverAddr

	if serverPort != "" && serverPort != "6379" {
		poolName = fmt.Sprintf("%s:%s", poolName, serverPort)
	}

	return poolName
}

// poolInfo stores information about a registered main connection pool
type poolInfo struct {
	name string
	pool redis.Pooler
}

// pubsubPoolInfo stores information about a registered PubSub pool
type pubsubPoolInfo struct {
	pool redis.PubSubPooler
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
	connectionClosed           metric.Int64Counter
	connectionPendingReqsGauge metric.Int64ObservableGauge

	pubsubMessages metric.Int64Counter

	streamLag metric.Float64Histogram

	// Configuration
	cfg *config

	// Pool registry for tracking multiple pools
	poolsMu     sync.RWMutex
	pools       []poolInfo
	pubsubPools []pubsubPoolInfo
}

// RecordOperationDuration records db.client.operation.duration metric
func (r *metricsRecorder) RecordOperationDuration(
	ctx context.Context,
	duration time.Duration,
	cmd redis.Cmder,
	attempts int,
	err error,
	cn redis.ConnInfo,
	dbIndex int,
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

	serverAddr, serverPort := extractServerInfo(cn)

	// Build attributes
	attrs := []attribute.KeyValue{
		// Required attributes
		attribute.String("db.operation.name", cmd.FullName()),
		getLibraryVersionAttr(),
		attribute.Int("redis.client.operation.retry_attempts", attempts-1), // attempts-1 = retry count

		// Recommended attributes
		attribute.String("db.system.name", "redis"),
		attribute.String("server.address", serverAddr),
		attribute.String("db.namespace", strconv.Itoa(dbIndex)),
	}

	// Add server.port if not default
	attrs = addServerPortIfNonDefault(attrs, serverPort)

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

	if err != nil {
		attrs = append(attrs, attribute.String("error.type", classifyError(err)))
		attrs = append(attrs, attribute.String("redis.client.errors.category", getErrorCategory(err)))
		if statusCode := extractRedisErrorPrefix(err); statusCode != "" {
			attrs = append(attrs, attribute.String("db.response.status_code", statusCode))
		}
	}

	// Record the histogram
	r.operationDuration.Record(ctx, durationSeconds, metric.WithAttributes(attrs...))
}

// RecordPipelineOperationDuration records db.client.operation.duration metric for pipelines/transactions.
// operationName should be "PIPELINE" for regular pipelines or "MULTI" for transactions.
func (r *metricsRecorder) RecordPipelineOperationDuration(
	ctx context.Context,
	duration time.Duration,
	operationName string,
	cmdCount int,
	attempts int,
	err error,
	cn redis.ConnInfo,
	dbIndex int,
) {
	if r.operationDuration == nil {
		return
	}

	// Convert duration to seconds (OTel convention for duration metrics)
	durationSeconds := duration.Seconds()

	// Extract server info from connection
	serverAddr, serverPort := extractServerInfo(cn)

	// Build attributes
	attrs := []attribute.KeyValue{
		attribute.String("db.operation.name", operationName),
		getLibraryVersionAttr(),
		attribute.Int("redis.client.operation.retry_attempts", attempts-1), // attempts-1 = retry count
		attribute.Int("db.operation.batch.size", cmdCount),                 // number of commands in pipeline
		attribute.String("db.system.name", "redis"),
		attribute.String("server.address", serverAddr),
		attribute.String("db.namespace", strconv.Itoa(dbIndex)),
	}

	// Add server.port if not default
	attrs = addServerPortIfNonDefault(attrs, serverPort)

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

	// Add error attributes if pipeline failed
	if err != nil {
		attrs = append(attrs, attribute.String("error.type", classifyError(err)))
		attrs = append(attrs, attribute.String("redis.client.errors.category", getErrorCategory(err)))
		if statusCode := extractRedisErrorPrefix(err); statusCode != "" {
			attrs = append(attrs, attribute.String("db.response.status_code", statusCode))
		}
	}

	// Record the histogram
	r.operationDuration.Record(ctx, durationSeconds, metric.WithAttributes(attrs...))
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

func getErrorCategory(err error) string {
	if err == nil {
		return ""
	}

	errStr := strings.ToLower(err.Error())

	// Check for TLS errors
	if strings.Contains(errStr, "tls") || strings.Contains(errStr, "certificate") ||
		strings.Contains(errStr, "x509") || strings.Contains(errStr, "ssl") {
		return "tls"
	}

	// Check for auth errors
	if strings.Contains(errStr, "auth") || strings.Contains(errStr, "noauth") ||
		strings.Contains(errStr, "wrongpass") || strings.Contains(errStr, "noperm") ||
		strings.Contains(errStr, "permission") {
		return "auth"
	}

	// Check for network errors (transport/DNS/socket issues)
	if isNetworkError(err) || isTimeoutError(err) ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "connection reset") ||
		strings.Contains(errStr, "no route to host") ||
		strings.Contains(errStr, "network is unreachable") ||
		strings.Contains(errStr, "dns") ||
		strings.Contains(errStr, "lookup") ||
		strings.Contains(errStr, "dial") ||
		strings.Contains(errStr, "eof") ||
		strings.Contains(errStr, "broken pipe") {
		return "network"
	}

	// Check for Redis server errors (including cluster errors)
	if prefix := extractRedisErrorPrefix(err); prefix != "" {
		return "server"
	}

	// Uncategorized errors
	return "other"
}

func getErrorCategoryFromType(errorType string) string {
	if errorType == "" {
		return ""
	}

	errLower := strings.ToLower(errorType)

	// Check for TLS errors
	if strings.Contains(errLower, "tls") || strings.Contains(errLower, "certificate") ||
		strings.Contains(errLower, "x509") || strings.Contains(errLower, "ssl") {
		return "tls"
	}

	// Check for auth errors
	if strings.Contains(errLower, "auth") || strings.Contains(errLower, "noauth") ||
		strings.Contains(errLower, "wrongpass") || strings.Contains(errLower, "noperm") ||
		strings.Contains(errLower, "permission") {
		return "auth"
	}

	// Check for network errors
	if strings.Contains(errLower, "network") || strings.Contains(errLower, "timeout") ||
		strings.Contains(errLower, "connection refused") || strings.Contains(errLower, "connection reset") ||
		strings.Contains(errLower, "dns") || strings.Contains(errLower, "dial") ||
		strings.Contains(errLower, "eof") || strings.Contains(errLower, "broken pipe") ||
		strings.Contains(errLower, "i/o") {
		return "network"
	}

	// Check for Redis server errors (including cluster errors)
	// Common Redis error prefixes: ERR, WRONGTYPE, CLUSTERDOWN, MOVED, ASK, READONLY, etc.
	serverErrors := []string{"err", "wrongtype", "clusterdown", "moved", "ask", "readonly",
		"crossslot", "tryagain", "loading", "busy", "noscript", "oom", "execabort", "noquorum"}
	for _, prefix := range serverErrors {
		if strings.Contains(errLower, prefix) {
			return "server"
		}
	}

	// If it looks like an uppercase Redis error prefix, it's a server error
	if len(errorType) > 0 && errorType == strings.ToUpper(errorType) {
		return "server"
	}

	return "other"
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
	poolName := formatPoolName(serverAddr, serverPort)
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
	computedPoolName := formatPoolName(serverAddr, serverPort)
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

	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		getLibraryVersionAttr(),
	}

	// Add pool name (computed from server info, not using the passed poolName parameter)
	computedPoolName := formatPoolName(serverAddr, serverPort)
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
		attribute.String("redis.client.errors.category", getErrorCategoryFromType(errorType)),
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

	// Build attributes
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		getLibraryVersionAttr(),
	}

	// Add pool name
	poolName := formatPoolName(serverAddr, serverPort)
	attrs = append(attrs, attribute.String("db.client.connection.pool.name", poolName))

	// Record the histogram (duration in seconds)
	r.connectionWaitTime.Record(ctx, duration.Seconds(), metric.WithAttributes(attrs...))
}

// RecordConnectionClosed records redis.client.connection.closed metric
func (r *metricsRecorder) RecordConnectionClosed(
	ctx context.Context,
	cn redis.ConnInfo,
	reason string,
	err error,
) {
	if r.connectionClosed == nil {
		return
	}

	// Extract server address from connection
	serverAddr, serverPort := extractServerInfo(cn)

	// Build attributes
	attrs := []attribute.KeyValue{
		attribute.String("db.system.name", "redis"),
		getLibraryVersionAttr(),
	}

	// Add pool name
	poolName := formatPoolName(serverAddr, serverPort)
	attrs = append(attrs, attribute.String("db.client.connection.pool.name", poolName))

	// Add close reason
	attrs = append(attrs, attribute.String("redis.client.connection.close.reason", reason))

	// Add error type and category (always required per spec)
	if err != nil {
		attrs = append(attrs, attribute.String("error.type", err.Error()))
		attrs = append(attrs, attribute.String("redis.client.errors.category", getErrorCategory(err)))
	} else {
		// For non-error closures, use reason as error type and derive category
		attrs = append(attrs, attribute.String("error.type", reason))
		attrs = append(attrs, attribute.String("redis.client.errors.category", getErrorCategoryFromType(reason)))
	}

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

// RegisterPool implements the OTelPoolRegistrar interface.
// The pools are used by async gauge callbacks to pull statistics.
func (r *metricsRecorder) RegisterPool(poolName string, pool redis.Pooler) {
	r.poolsMu.Lock()
	defer r.poolsMu.Unlock()

	// Add pool to registry
	r.pools = append(r.pools, poolInfo{
		name: poolName,
		pool: pool,
	})
}

// UnregisterPool implements the OTelPoolRegistrar interface.
// This ensures async gauge callbacks don't try to access closed pools.
func (r *metricsRecorder) UnregisterPool(pool redis.Pooler) {
	r.poolsMu.Lock()
	defer r.poolsMu.Unlock()

	// Find and remove the pool from registry
	for i, p := range r.pools {
		if p.pool == pool {
			// Remove by swapping with last element and truncating
			r.pools[i] = r.pools[len(r.pools)-1]
			r.pools = r.pools[:len(r.pools)-1]
			return
		}
	}
}

// RegisterPubSubPool implements the OTelPoolRegistrar interface.
// The pools are used by async gauge callbacks to pull statistics.
func (r *metricsRecorder) RegisterPubSubPool(pool redis.PubSubPooler) {
	r.poolsMu.Lock()
	defer r.poolsMu.Unlock()

	// Add PubSub pool to registry
	r.pubsubPools = append(r.pubsubPools, pubsubPoolInfo{
		pool: pool,
	})
}

// UnregisterPubSubPool implements the OTelPoolRegistrar interface.
// This ensures async gauge callbacks don't try to access closed pools.
func (r *metricsRecorder) UnregisterPubSubPool(pool redis.PubSubPooler) {
	r.poolsMu.Lock()
	defer r.poolsMu.Unlock()

	// Find and remove the pool from registry
	for i, p := range r.pubsubPools {
		if p.pool == pool {
			// Remove by swapping with last element and truncating
			r.pubsubPools[i] = r.pubsubPools[len(r.pubsubPools)-1]
			r.pubsubPools = r.pubsubPools[:len(r.pubsubPools)-1]
			return
		}
	}
}
