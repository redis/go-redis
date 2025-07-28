package pool

import (
	"bufio"
	"context"
	"fmt"
	"net"
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

	// Lock-free netConn access using atomic.Value
	// Contains net.Conn, accessed atomically for better performance
	netConnAtomic atomic.Value // stores net.Conn

	rd *proto.Reader
	bw *bufio.Writer
	wr *proto.Writer

	Inited    bool
	pooled    bool
	createdAt time.Time
	expiresAt time.Time

	// Hitless upgrade support: relaxed timeouts during migrations/failovers
	// Using atomic operations for lock-free access to avoid mutex contention
	relaxedReadTimeoutNs  int64 // time.Duration as nanoseconds, accessed atomically
	relaxedWriteTimeoutNs int64 // time.Duration as nanoseconds, accessed atomically
	relaxedDeadlineNs     int64 // time.Time as nanoseconds since epoch, accessed atomically

	// Connection initialization function for reconnections
	initConnFunc func(context.Context, *Conn) error

	// Connection identifier for unique tracking across handoffs
	id uint64 // Unique numeric identifier for this connection

	// Handoff state - using atomic operations for lock-free access
	usableAtomic         int32 // 1 if usable, 0 if not (atomic bool)
	shouldHandoffAtomic  int32 // 1 if should handoff, 0 if not (atomic bool)
	movingSeqIDAtomic    int64 // Sequence ID from MOVING notification (atomic)
	handoffRetriesAtomic int32 // Retry counter for handoff attempts (atomic)

	// newEndpoint needs special handling as it's a string
	// We'll use atomic.Value for this
	newEndpointAtomic atomic.Value // stores string

	onClose func() error
}

func NewConn(netConn net.Conn) *Conn {
	cn := &Conn{
		createdAt: time.Now(),
		id:        generateConnID(), // Generate unique ID for this connection
	}

	// Store netConn atomically for lock-free access
	cn.netConnAtomic.Store(netConn)

	// Initialize atomic handoff state
	atomic.StoreInt32(&cn.usableAtomic, 0)         // false initially, set to true after initialization
	atomic.StoreInt32(&cn.shouldHandoffAtomic, 0)  // false initially
	atomic.StoreInt64(&cn.movingSeqIDAtomic, 0)    // 0 initially
	atomic.StoreInt32(&cn.handoffRetriesAtomic, 0) // 0 initially
	cn.newEndpointAtomic.Store("")                 // empty string initially

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

// getNetConn returns the current network connection using atomic load (lock-free).
// This is the fast path for accessing netConn without mutex overhead.
func (cn *Conn) getNetConn() net.Conn {
	if conn := cn.netConnAtomic.Load(); conn != nil {
		return conn.(net.Conn)
	}
	return nil
}

// setNetConn stores the network connection atomically (lock-free).
// This is used for the fast path of connection replacement.
func (cn *Conn) setNetConn(netConn net.Conn) {
	cn.netConnAtomic.Store(netConn)
}

// Lock-free helper methods for handoff state management

// isUsable returns true if the connection is safe to use (lock-free).
func (cn *Conn) isUsable() bool {
	return atomic.LoadInt32(&cn.usableAtomic) == 1
}

// setUsable sets the usable flag atomically (lock-free).
func (cn *Conn) setUsable(usable bool) {
	var val int32
	if usable {
		val = 1
	}
	atomic.StoreInt32(&cn.usableAtomic, val)
}

// shouldHandoff returns true if connection needs handoff (lock-free).
func (cn *Conn) shouldHandoff() bool {
	return atomic.LoadInt32(&cn.shouldHandoffAtomic) == 1
}

// setShouldHandoff sets the handoff flag atomically (lock-free).
func (cn *Conn) setShouldHandoff(should bool) {
	var val int32
	if should {
		val = 1
	}
	atomic.StoreInt32(&cn.shouldHandoffAtomic, val)
}

// getMovingSeqID returns the sequence ID atomically (lock-free).
func (cn *Conn) getMovingSeqID() int64 {
	return atomic.LoadInt64(&cn.movingSeqIDAtomic)
}

// setMovingSeqID sets the sequence ID atomically (lock-free).
func (cn *Conn) setMovingSeqID(seqID int64) {
	atomic.StoreInt64(&cn.movingSeqIDAtomic, seqID)
}

// getNewEndpoint returns the new endpoint atomically (lock-free).
func (cn *Conn) getNewEndpoint() string {
	if endpoint := cn.newEndpointAtomic.Load(); endpoint != nil {
		return endpoint.(string)
	}
	return ""
}

// setNewEndpoint sets the new endpoint atomically (lock-free).
func (cn *Conn) setNewEndpoint(endpoint string) {
	cn.newEndpointAtomic.Store(endpoint)
}

// getHandoffRetries returns the retry count atomically (lock-free).
func (cn *Conn) getHandoffRetries() int {
	return int(atomic.LoadInt32(&cn.handoffRetriesAtomic))
}

// setHandoffRetries sets the retry count atomically (lock-free).
func (cn *Conn) setHandoffRetries(retries int) {
	atomic.StoreInt32(&cn.handoffRetriesAtomic, int32(retries))
}

// incrementHandoffRetries atomically increments and returns the new retry count (lock-free).
func (cn *Conn) incrementHandoffRetries(delta int) int {
	return int(atomic.AddInt32(&cn.handoffRetriesAtomic, int32(delta)))
}

// IsUsable returns true if the connection is safe to use for new commands (lock-free).
func (cn *Conn) IsUsable() bool {
	return cn.isUsable()
}

// SetUsable sets the usable flag for the connection (lock-free).
func (cn *Conn) SetUsable(usable bool) {
	cn.setUsable(usable)
}

// SetRelaxedTimeout sets relaxed timeouts for this connection during hitless upgrades.
// These timeouts will be used for all subsequent commands until the deadline expires.
// Uses atomic operations for lock-free access.
func (cn *Conn) SetRelaxedTimeout(readTimeout, writeTimeout time.Duration) {
	atomic.StoreInt64(&cn.relaxedReadTimeoutNs, int64(readTimeout))
	atomic.StoreInt64(&cn.relaxedWriteTimeoutNs, int64(writeTimeout))
	// No deadline set - timeouts remain until explicitly cleared
	atomic.StoreInt64(&cn.relaxedDeadlineNs, 0)
}

// SetRelaxedTimeoutWithDeadline sets relaxed timeouts with an expiration deadline.
// After the deadline, timeouts automatically revert to normal values.
// Uses atomic operations for lock-free access.
func (cn *Conn) SetRelaxedTimeoutWithDeadline(readTimeout, writeTimeout time.Duration, deadline time.Time) {
	atomic.StoreInt64(&cn.relaxedReadTimeoutNs, int64(readTimeout))
	atomic.StoreInt64(&cn.relaxedWriteTimeoutNs, int64(writeTimeout))
	atomic.StoreInt64(&cn.relaxedDeadlineNs, deadline.UnixNano())
}

// ClearRelaxedTimeout removes relaxed timeouts, returning to normal timeout behavior.
// Uses atomic operations for lock-free access.
func (cn *Conn) ClearRelaxedTimeout() {
	atomic.StoreInt64(&cn.relaxedReadTimeoutNs, 0)
	atomic.StoreInt64(&cn.relaxedWriteTimeoutNs, 0)
	atomic.StoreInt64(&cn.relaxedDeadlineNs, 0)
}

// HasRelaxedTimeout returns true if relaxed timeouts are currently active on this connection.
// This checks both the timeout values and the deadline (if set).
// Uses atomic operations for lock-free access.
func (cn *Conn) HasRelaxedTimeout() bool {
	readTimeoutNs := atomic.LoadInt64(&cn.relaxedReadTimeoutNs)
	writeTimeoutNs := atomic.LoadInt64(&cn.relaxedWriteTimeoutNs)

	// If no relaxed timeouts are set, return false
	if readTimeoutNs <= 0 && writeTimeoutNs <= 0 {
		return false
	}

	deadlineNs := atomic.LoadInt64(&cn.relaxedDeadlineNs)
	// If no deadline is set, relaxed timeouts are active
	if deadlineNs == 0 {
		return true
	}

	// If deadline is set, check if it's still in the future
	return time.Now().UnixNano() < deadlineNs
}

// getEffectiveReadTimeout returns the timeout to use for read operations.
// If relaxed timeout is set and not expired, it takes precedence over the provided timeout.
// This method automatically clears expired relaxed timeouts using atomic operations.
func (cn *Conn) getEffectiveReadTimeout(normalTimeout time.Duration) time.Duration {
	readTimeoutNs := atomic.LoadInt64(&cn.relaxedReadTimeoutNs)

	// Fast path: no relaxed timeout set
	if readTimeoutNs <= 0 {
		return normalTimeout
	}

	deadlineNs := atomic.LoadInt64(&cn.relaxedDeadlineNs)
	// If no deadline is set, use relaxed timeout
	if deadlineNs == 0 {
		return time.Duration(readTimeoutNs)
	}

	nowNs := time.Now().UnixNano()
	// Check if deadline has passed
	if nowNs < deadlineNs {
		// Deadline is in the future, use relaxed timeout
		return time.Duration(readTimeoutNs)
	} else {
		// Deadline has passed, clear relaxed timeouts atomically and use normal timeout
		atomic.StoreInt64(&cn.relaxedReadTimeoutNs, 0)
		atomic.StoreInt64(&cn.relaxedWriteTimeoutNs, 0)
		atomic.StoreInt64(&cn.relaxedDeadlineNs, 0)
		return normalTimeout
	}
}

// getEffectiveWriteTimeout returns the timeout to use for write operations.
// If relaxed timeout is set and not expired, it takes precedence over the provided timeout.
// This method automatically clears expired relaxed timeouts using atomic operations.
func (cn *Conn) getEffectiveWriteTimeout(normalTimeout time.Duration) time.Duration {
	writeTimeoutNs := atomic.LoadInt64(&cn.relaxedWriteTimeoutNs)

	// Fast path: no relaxed timeout set
	if writeTimeoutNs <= 0 {
		return normalTimeout
	}

	deadlineNs := atomic.LoadInt64(&cn.relaxedDeadlineNs)
	// If no deadline is set, use relaxed timeout
	if deadlineNs == 0 {
		return time.Duration(writeTimeoutNs)
	}

	nowNs := time.Now().UnixNano()
	// Check if deadline has passed
	if nowNs < deadlineNs {
		// Deadline is in the future, use relaxed timeout
		return time.Duration(writeTimeoutNs)
	} else {
		// Deadline has passed, clear relaxed timeouts atomically and use normal timeout
		atomic.StoreInt64(&cn.relaxedReadTimeoutNs, 0)
		atomic.StoreInt64(&cn.relaxedWriteTimeoutNs, 0)
		atomic.StoreInt64(&cn.relaxedDeadlineNs, 0)
		return normalTimeout
	}
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
			cn.setUsable(true) // Use atomic operation
		}
		return err
	}
	return nil
}

func (cn *Conn) SetNetConn(netConn net.Conn) {
	// Store the new connection atomically first (lock-free)
	cn.setNetConn(netConn)
	cn.rd.Reset(netConn)
	cn.bw.Reset(netConn)
}

// GetNetConn safely returns the current network connection using atomic load (lock-free).
// This method is used by the pool for health checks and provides better performance.
func (cn *Conn) GetNetConn() net.Conn {
	return cn.getNetConn()
}

// SetNetConnWithInitConn replaces the underlying connection and executes the initialization.
func (cn *Conn) SetNetConnWithInitConn(ctx context.Context, netConn net.Conn) error {
	// New connection is not initialized yet
	cn.Inited = false
	// Replace the underlying connection
	cn.SetNetConn(netConn)
	return cn.ExecuteInitConn(ctx)
}

// MarkForHandoff marks the connection for handoff due to MOVING notification (lock-free).
func (cn *Conn) MarkForHandoff(newEndpoint string, seqID int64) {
	cn.setShouldHandoff(true)
	cn.setNewEndpoint(newEndpoint)
	cn.setMovingSeqID(seqID)
	cn.setUsable(false) // Connection is not safe to use until handoff completes
}

// ShouldHandoff returns true if the connection needs to be handed off (lock-free).
func (cn *Conn) ShouldHandoff() bool {
	return cn.shouldHandoff()
}

// GetHandoffEndpoint returns the new endpoint for handoff (lock-free).
func (cn *Conn) GetHandoffEndpoint() string {
	return cn.getNewEndpoint()
}

// GetMovingSeqID returns the sequence ID from the MOVING notification (lock-free).
func (cn *Conn) GetMovingSeqID() int64 {
	return cn.getMovingSeqID()
}

// GetID returns the unique identifier for this connection.
func (cn *Conn) GetID() uint64 {
	return cn.id
}

// ClearHandoffState clears the handoff state after successful handoff (lock-free).
func (cn *Conn) ClearHandoffState() {
	cn.setShouldHandoff(false)
	cn.setNewEndpoint("")
	cn.setMovingSeqID(0)
	cn.setHandoffRetries(0)
	cn.setUsable(true) // Connection is safe to use again after handoff completes
}

// IncrementAndGetHandoffRetries atomically increments and returns handoff retries (lock-free).
func (cn *Conn) IncrementAndGetHandoffRetries(n int) int {
	return cn.incrementHandoffRetries(n)
}

// Rd returns the connection's reader for protocol-specific processing
func (cn *Conn) Rd() *proto.Reader {
	return cn.rd
}

// Reader returns the connection's proto reader for processing notifications
func (cn *Conn) Reader() *proto.Reader {
	return cn.rd
}

// HasBufferedData safely checks if the connection has buffered data.
// This method is used to avoid data races when checking for push notifications.
func (cn *Conn) HasBufferedData() bool {
	return cn.rd.Buffered() > 0
}

// PeekReplyTypeSafe safely peeks at the reply type.
// This method is used to avoid data races when checking for push notifications.
func (cn *Conn) PeekReplyTypeSafe() (byte, error) {
	if !cn.HasBufferedData() {
		return 0, fmt.Errorf("redis: can't peek reply type, no data available")
	}
	return cn.rd.PeekReplyType()
}

func (cn *Conn) Write(b []byte) (int, error) {
	// Lock-free netConn access for better performance
	if netConn := cn.getNetConn(); netConn != nil {
		return netConn.Write(b)
	}
	return 0, net.ErrClosed
}

func (cn *Conn) RemoteAddr() net.Addr {
	// Lock-free netConn access for better performance
	if netConn := cn.getNetConn(); netConn != nil {
		return netConn.RemoteAddr()
	}
	return nil
}

func (cn *Conn) WithReader(
	ctx context.Context, timeout time.Duration, fn func(rd *proto.Reader) error,
) error {
	if timeout >= 0 {
		// Use relaxed timeout if set, otherwise use provided timeout
		effectiveTimeout := cn.getEffectiveReadTimeout(timeout)

		// Lock-free netConn access for better performance
		if netConn := cn.getNetConn(); netConn != nil {
			if err := netConn.SetReadDeadline(cn.deadline(ctx, effectiveTimeout)); err != nil {
				return err
			}
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

		// Lock-free netConn access for better performance
		if netConn := cn.getNetConn(); netConn != nil {
			if err := netConn.SetWriteDeadline(cn.deadline(ctx, effectiveTimeout)); err != nil {
				return err
			}
		}
	}

	if cn.bw.Buffered() > 0 {
		if netConn := cn.getNetConn(); netConn != nil {
			cn.bw.Reset(netConn)
		}
	}

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

	// Lock-free netConn access for better performance
	if netConn := cn.getNetConn(); netConn != nil {
		return netConn.Close()
	}
	return nil
}

// MaybeHasData tries to peek at the next byte in the socket without consuming it
// This is used to check if there are push notifications available
// Important: This will work on Linux, but not on Windows
func (cn *Conn) MaybeHasData() bool {
	// Lock-free netConn access for better performance
	if netConn := cn.getNetConn(); netConn != nil {
		return maybeHasData(netConn)
	}
	return false
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
