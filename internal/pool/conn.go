package pool

import (
	"bufio"
	"context"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

var noDeadline = time.Time{}

// Global atomic counter for connection IDs
var connIDCounter uint64

// generateConnID generates a fast unique identifier for a connection with zero allocations
func generateConnID() uint64 {
	return atomic.AddUint64(&connIDCounter, 1)
}

type Conn struct {
	usedAt int64 // atomic

	// Mutex to protect netConn and related fields from concurrent access
	mu      sync.RWMutex
	netConn net.Conn

	rd *proto.Reader
	bw *bufio.Writer
	wr *proto.Writer

	Inited    bool
	pooled    bool
	usable    bool // Flag indicating if connection is safe to use for new commands
	createdAt time.Time

	// Separate mutex for timeout management to avoid contention with connection operations
	timeoutMu sync.RWMutex

	// Hitless upgrade support: relaxed timeouts during migrations/failovers
	relaxedReadTimeout  time.Duration
	relaxedWriteTimeout time.Duration
	relaxedDeadline     time.Time // Deadline when relaxed timeouts should expire

	// Connection initialization function for reconnections
	initConnFunc func(context.Context, *Conn) error

	// Connection identifier for unique tracking across handoffs
	id uint64 // Unique numeric identifier for this connection

	shouldHandoff  bool   // Flag indicating connection needs handoff
	newEndpoint    string // New endpoint from MOVING notification
	movingSeqID    int64  // Sequence ID from MOVING notification
	handoffRetries int    // Retry counter for handoff attempts

	onClose func() error
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		netConn:   netConn,
		createdAt: time.Now(),
		usable:    false,            // Will be set to true after initialization
		id:        generateConnID(), // Generate unique ID for this connection
	}
	cn.rd = proto.NewReader(netConn)
	cn.bw = bufio.NewWriter(netConn)
	cn.wr = proto.NewWriter(cn.bw)
	cn.SetUsedAt(time.Now())
	return cn
}

func (cn *Conn) UsedAt() time.Time {
	unix := atomic.LoadInt64(&cn.usedAt)
	return time.Unix(unix, 0)
}

func (cn *Conn) SetUsedAt(tm time.Time) {
	atomic.StoreInt64(&cn.usedAt, tm.Unix())
}

// IsUsable returns true if the connection is safe to use for new commands.
func (cn *Conn) IsUsable() bool {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.usable
}

// SetUsable sets the usable flag for the connection.
func (cn *Conn) SetUsable(usable bool) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.usable = usable
}

// SetRelaxedTimeout sets relaxed timeouts for this connection during hitless upgrades.
// These timeouts will be used for all subsequent commands until the deadline expires.
func (cn *Conn) SetRelaxedTimeout(readTimeout, writeTimeout time.Duration) {
	cn.timeoutMu.Lock()
	defer cn.timeoutMu.Unlock()
	cn.relaxedReadTimeout = readTimeout
	cn.relaxedWriteTimeout = writeTimeout
	// No deadline set - timeouts remain until explicitly cleared
	cn.relaxedDeadline = time.Time{}
}

// SetRelaxedTimeoutWithDeadline sets relaxed timeouts with an expiration deadline.
// After the deadline, timeouts automatically revert to normal values.
func (cn *Conn) SetRelaxedTimeoutWithDeadline(readTimeout, writeTimeout time.Duration, deadline time.Time) {
	cn.timeoutMu.Lock()
	defer cn.timeoutMu.Unlock()
	cn.relaxedReadTimeout = readTimeout
	cn.relaxedWriteTimeout = writeTimeout
	cn.relaxedDeadline = deadline
}

// ClearRelaxedTimeout removes relaxed timeouts, returning to normal timeout behavior.
func (cn *Conn) ClearRelaxedTimeout() {
	cn.timeoutMu.Lock()
	defer cn.timeoutMu.Unlock()
	cn.relaxedReadTimeout = 0
	cn.relaxedWriteTimeout = 0
	cn.relaxedDeadline = time.Time{}
}

// HasRelaxedTimeout returns true if relaxed timeouts are currently active on this connection.
// This checks both the timeout values and the deadline (if set).
func (cn *Conn) HasRelaxedTimeout() bool {
	cn.timeoutMu.RLock()
	defer cn.timeoutMu.RUnlock()

	// If no relaxed timeouts are set, return false
	if cn.relaxedReadTimeout <= 0 && cn.relaxedWriteTimeout <= 0 {
		return false
	}

	// If no deadline is set, relaxed timeouts are active
	if cn.relaxedDeadline.IsZero() {
		return true
	}

	// If deadline is set, check if it's still in the future
	return time.Now().Before(cn.relaxedDeadline)
}

// getEffectiveTimeout returns the timeout to use for operations.
// If relaxed timeout is set and not expired, it takes precedence over the provided timeout.
// This method automatically clears expired relaxed timeouts.
func (cn *Conn) getEffectiveReadTimeout(normalTimeout time.Duration) time.Duration {
	cn.timeoutMu.Lock()
	defer cn.timeoutMu.Unlock()

	// Check if relaxed timeout is set
	if cn.relaxedReadTimeout > 0 {
		// If no deadline is set, use relaxed timeout
		if cn.relaxedDeadline.IsZero() {
			return cn.relaxedReadTimeout
		}

		// Check if deadline has passed
		if time.Now().Before(cn.relaxedDeadline) {
			// Deadline is in the future, use relaxed timeout
			return cn.relaxedReadTimeout
		} else {
			// Deadline has passed, clear relaxed timeouts and use normal timeout
			cn.relaxedReadTimeout = 0
			cn.relaxedWriteTimeout = 0
			cn.relaxedDeadline = time.Time{}
			return normalTimeout
		}
	}
	return normalTimeout
}

func (cn *Conn) getEffectiveWriteTimeout(normalTimeout time.Duration) time.Duration {
	cn.timeoutMu.Lock()
	defer cn.timeoutMu.Unlock()

	// Check if relaxed timeout is set
	if cn.relaxedWriteTimeout > 0 {
		// If no deadline is set, use relaxed timeout
		if cn.relaxedDeadline.IsZero() {
			return cn.relaxedWriteTimeout
		}

		// Check if deadline has passed
		if time.Now().Before(cn.relaxedDeadline) {
			// Deadline is in the future, use relaxed timeout
			return cn.relaxedWriteTimeout
		} else {
			// Deadline has passed, clear relaxed timeouts and use normal timeout
			cn.relaxedReadTimeout = 0
			cn.relaxedWriteTimeout = 0
			cn.relaxedDeadline = time.Time{}
			return normalTimeout
		}
	}
	return normalTimeout
}

func (cn *Conn) SetOnClose(fn func() error) {
	cn.onClose = fn
}

// SetInitConnFunc sets the connection initialization function to be called on reconnections.
func (cn *Conn) SetInitConnFunc(fn func(context.Context, *Conn) error) {
	cn.initConnFunc = fn
}

// ExecuteInitConn runs the stored connection initialization function if available.
func (cn *Conn) ExecuteInitConn(ctx context.Context) error {
	if cn.initConnFunc != nil {
		err := cn.initConnFunc(ctx, cn)
		if err == nil {
			cn.Inited = true
			cn.usable = true
		}
		return err
	}
	return nil
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	cn.mu.Lock()
	defer cn.mu.Unlock()

	cn.netConn = netConn
	cn.rd.Reset(netConn)
	cn.bw.Reset(netConn)
}

// GetNetConn safely returns the current network connection.
// This method is used by the pool for health checks to avoid data races.
func (cn *Conn) GetNetConn() net.Conn {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.netConn
}

// SetNetConnWithInitConn replaces the underlying connection and executes the initialization.
func (cn *Conn) SetNetConnWithInitConn(ctx context.Context, netConn net.Conn) error {
	// New connection is not initialized yet
	cn.Inited = false
	// Replace the underlying connection
	cn.SetNetConn(netConn)
	return cn.ExecuteInitConn(ctx)
}

// MarkForHandoff marks the connection for handoff due to MOVING notification.
func (cn *Conn) MarkForHandoff(newEndpoint string, seqID int64) {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.shouldHandoff = true
	cn.newEndpoint = newEndpoint
	cn.movingSeqID = seqID
	cn.usable = false // Connection is not safe to use until handoff completes
}

// ShouldHandoff returns true if the connection needs to be handed off.
func (cn *Conn) ShouldHandoff() bool {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.shouldHandoff
}

// GetHandoffEndpoint returns the new endpoint for handoff.
func (cn *Conn) GetHandoffEndpoint() string {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.newEndpoint
}

// GetMovingSeqID returns the sequence ID from the MOVING notification.
func (cn *Conn) GetMovingSeqID() int64 {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.movingSeqID
}

// GetID returns the unique identifier for this connection.
func (cn *Conn) GetID() uint64 {
	return cn.id
}

// ClearHandoffState clears the handoff state after successful handoff.
func (cn *Conn) ClearHandoffState() {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.shouldHandoff = false
	cn.newEndpoint = ""
	cn.movingSeqID = 0
	cn.handoffRetries = 0
	cn.usable = true // Connection is safe to use again after handoff completes
}

func (cn *Conn) IncrementAndGetHandoffRetries(n int) int {
	cn.mu.Lock()
	defer cn.mu.Unlock()
	cn.handoffRetries += n
	return cn.handoffRetries
}

// Rd returns the connection's reader for protocol-specific processing
func (cn *Conn) Rd() *proto.Reader {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.rd
}

// Reader returns the connection's proto reader for processing notifications
func (cn *Conn) Reader() *proto.Reader {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.rd
}

// HasBufferedData safely checks if the connection has buffered data.
// This method is used to avoid data races when checking for push notifications.
func (cn *Conn) HasBufferedData() bool {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.rd.Buffered() > 0
}

// PeekReplyTypeSafe safely peeks at the reply type.
// This method is used to avoid data races when checking for push notifications.
func (cn *Conn) PeekReplyTypeSafe() (byte, error) {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.rd.PeekReplyType()
}

func (cn *Conn) Write(b []byte) (int, error) {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.netConn.Write(b)
}

func (cn *Conn) RemoteAddr() net.Addr {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	if cn.netConn != nil {
		return cn.netConn.RemoteAddr()
	}
	return nil
}

func (cn *Conn) WithReader(
	ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error,
) error {
	if timeout >= 0 {
		// Use relaxed timeout if set, otherwise use provided timeout
		effectiveTimeout := cn.getEffectiveReadTimeout(timeout)

		cn.mu.RLock()
		err := cn.netConn.SetReadDeadline(cn.deadline(ctx, effectiveTimeout))
		cn.mu.RUnlock()

		if err != nil {
			return err
		}
	}

	return fn(cn.rd)
}

func (cn *Conn) WithWriter(
	ctx context.Context, timeout time.Duration, fn func(wr *proto.Writer) error,
) error {
	if timeout >= 0 {
		// Use relaxed timeout if set, otherwise use provided timeout
		effectiveTimeout := cn.getEffectiveWriteTimeout(timeout)

		cn.mu.RLock()
		err := cn.netConn.SetWriteDeadline(cn.deadline(ctx, effectiveTimeout))
		cn.mu.RUnlock()

		if err != nil {
			return err
		}
	}

	cn.mu.RLock()
	if cn.bw.Buffered() > 0 {
		cn.bw.Reset(cn.netConn)
	}
	cn.mu.RUnlock()

	if err := fn(cn.wr); err != nil {
		return err
	}

	return cn.bw.Flush()
}

func (cn *Conn) Close() error {
	if cn.onClose != nil {
		// ignore error
		_ = cn.onClose()
	}

	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return cn.netConn.Close()
}

// MaybeHasData tries to peek at the next byte in the socket without consuming it
// This is used to check if there are push notifications available
// Important: This will work on Linux, but not on Windows
func (cn *Conn) MaybeHasData() bool {
	cn.mu.RLock()
	defer cn.mu.RUnlock()
	return maybeHasData(cn.netConn)
}

func (cn *Conn) deadline(ctx context.Context, timeout time.Duration) time.Time {
	tm := time.Now()
	cn.SetUsedAt(tm)

	if timeout > 0 {
		tm = tm.Add(timeout)
	}

	if ctx != nil {
		deadline, ok := ctx.Deadline()
		if ok {
			if timeout == 0 {
				return deadline
			}
			if deadline.Before(tm) {
				return deadline
			}
			return tm
		}
	}

	if timeout > 0 {
		return tm
	}

	return noDeadline
}
