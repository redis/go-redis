package redis

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/proto"
)

// broadcastSidecar is the single long-lived out-of-pool connection that issues
// CLIENT TRACKING ON BCAST and routes all incoming `invalidate` push frames
// to the shared client-side cache attached to the parent baseClient.
//
// Invariants:
//   - The sidecar connection is never returned to the pool. Functional
//     commands never share it.
//   - The read loop runs in its own goroutine, owns the proto.Reader, and
//     has exclusive ownership of the underlying net.Conn once Start returns.
//   - DB namespacing of incoming keys matches the shared invalidateHandler
//     so the sidecar evicts from the same namespaced key-space the
//     functional connections populate.
//
// What the sidecar does NOT do: it never participates in normal command
// dispatch, never reserves a cache slot, never reads command replies.
// The only frames it ever consumes are RESP3 push frames (`>`).
//
// Authentication: the sidecar authenticates once at HELLO using the
// non-streaming credential sources (Options.resolveCredentials:
// CredentialsProviderContext → CredentialsProvider → static Username/Password).
// The StreamingCredentialsProvider (rotating-token) path is intentionally not
// wired here — it requires the per-connection reauth/listener plumbing this
// push-only connection deliberately avoids. Deployments relying solely on
// rotating tokens are therefore unsupported for Broadcast CSC; a sidecar auth
// failure is logged and retried with backoff by the read loop.
type broadcastSidecar struct {
	opt   *Options
	cache Cache
	db    int

	// connMu protects conn/reader/writer during reconnects. The read loop
	// goroutine reads its own snapshot of these under the mutex; outside
	// the loop, callers also take the mutex to publish a new connection.
	connMu sync.Mutex
	conn   net.Conn
	reader *proto.Reader
	writer *proto.Writer
	// bw is the bufio.Writer beneath writer; retained so the liveness probe
	// can flush PING onto the socket.
	bw *bufio.Writer

	// done is closed by Shutdown(); the read loop watches it.
	done     chan struct{}
	doneOnce sync.Once

	// loopDone is closed by the read-loop goroutine on exit.
	loopDone chan struct{}

	// ready is set to 1 once the initial BCAST handshake completes
	// successfully. Tests gate on this so they don't race the dial.
	ready atomic.Bool

	// Reconnect backoff parameters.
	backoffInitial time.Duration
	backoffMax     time.Duration

	// healthCheckInterval bounds each blocking read: on expiry the loop probes
	// the server with PING, and a second consecutive expiry (probe unanswered)
	// tears the connection down. Detects silent server-side drops (no FIN/RST)
	// without waiting for OS TCP keepalive.
	healthCheckInterval time.Duration
}

func newBroadcastSidecar(opt *Options, cache Cache, db int) *broadcastSidecar {
	return &broadcastSidecar{
		opt:                 opt,
		cache:               cache,
		db:                  db,
		done:                make(chan struct{}),
		loopDone:            make(chan struct{}),
		backoffInitial:      100 * time.Millisecond,
		backoffMax:          30 * time.Second,
		healthCheckInterval: 30 * time.Second,
	}
}

// Start attempts the initial dial and BCAST handshake synchronously, then
// spawns the read-loop goroutine. A failed first connect is NOT fatal: the
// read loop owns reconnection with backoff, exactly as it would after a
// mid-life disconnect, and the cache stays bypassed (ready=false) until the
// subscription is live. Start returns an error only for configuration
// problems that no amount of retrying can fix.
func (s *broadcastSidecar) Start(ctx context.Context) error {
	if s.opt.Dialer == nil {
		return errors.New("csc sidecar: nil dialer")
	}
	if err := s.dialAndHandshake(ctx); err != nil {
		// Same handling as a runtime disconnect: keep ready=false (the data
		// path bypasses the cache) and let readLoop's reconnect-with-backoff
		// bring the subscription up when the server becomes reachable.
		internal.Logger.Printf(ctx,
			"csc sidecar: initial connect failed (retrying in background): %v", err)
		go s.readLoop()
		return nil
	}
	s.ready.Store(true)
	go s.readLoop()
	return nil
}

// Shutdown signals the read loop to exit and closes the underlying socket.
// Bounded to 5s to satisfy the spec's no-block guarantee.
func (s *broadcastSidecar) Shutdown() {
	s.doneOnce.Do(func() { close(s.done) })

	s.connMu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.connMu.Unlock()

	select {
	case <-s.loopDone:
	case <-time.After(5 * time.Second):
		internal.Logger.Printf(context.Background(),
			"csc sidecar: shutdown timed out waiting for read loop to exit")
	}
}

// dialAndHandshake opens a fresh TCP connection, completes the RESP3 HELLO
// handshake (authenticating if credentials are configured) and issues
// CLIENT TRACKING ON BCAST. On success it publishes the new conn/reader/
// writer atomically under connMu and returns nil.
//
// The handshake commands are built with the same Cmder constructors and
// argument builders the normal client uses (writeCmd + appendClientTrackingOptions),
// so RESP3 framing, auth, and tracking-option encoding live in one place; only
// the dedicated socket and the push-frame read loop are bespoke.
func (s *broadcastSidecar) dialAndHandshake(ctx context.Context) error {
	if s.opt.Dialer == nil {
		return errors.New("csc sidecar: nil dialer")
	}

	nc, err := s.opt.Dialer(ctx, s.opt.Network, s.opt.Addr)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}

	// Apply DialTimeout to handshake reads/writes so a hung server cannot
	// stall Start forever. The deadline is cleared once handshake completes.
	// opt.DialTimeout is defaulted to 5s by Options.init() before the sidecar
	// is constructed, so it is always positive here.
	_ = nc.SetDeadline(time.Now().Add(s.opt.DialTimeout))

	bw := bufio.NewWriterSize(nc, proto.DefaultBufferSize)
	wr := proto.NewWriter(bw)
	rd := proto.NewReader(nc)

	// writeAndRead serializes one command, flushes, and parses its reply,
	// reusing the client's writeCmd and the command's own readReply.
	writeAndRead := func(cmd Cmder) error {
		if err := writeCmd(wr, cmd); err != nil {
			return err
		}
		if err := bw.Flush(); err != nil {
			return err
		}
		return cmd.readReply(rd)
	}

	// --- HELLO 3 [AUTH ...] [SETNAME ...] ---
	// CSC requires RESP3, so HELLO 3 is mandatory (no RESP2/legacy-AUTH fallback).
	username, password, err := s.opt.resolveCredentials(ctx)
	if err != nil {
		_ = nc.Close()
		return fmt.Errorf("resolve credentials: %w", err)
	}
	helloArgs := []interface{}{"hello", 3}
	if password != "" {
		if username == "" {
			username = "default"
		}
		helloArgs = append(helloArgs, "auth", username, password)
	}
	if s.opt.ClientName != "" {
		helloArgs = append(helloArgs, "setname", s.opt.ClientName+"-csc-sidecar")
	}
	if err := writeAndRead(NewMapStringInterfaceCmd(ctx, helloArgs...)); err != nil {
		_ = nc.Close()
		return fmt.Errorf("HELLO: %w", err)
	}

	// --- SELECT db if non-zero ---
	if s.db > 0 {
		if err := writeAndRead(NewStatusCmd(ctx, "select", s.db)); err != nil {
			_ = nc.Close()
			return fmt.Errorf("SELECT: %w", err)
		}
	}

	// --- CLIENT TRACKING ON BCAST ---
	trackingArgs := appendClientTrackingOptions(
		[]interface{}{"client", "tracking", "on"}, &ClientTrackingOptions{Bcast: true})
	if err := writeAndRead(NewStatusCmd(ctx, trackingArgs...)); err != nil {
		_ = nc.Close()
		return fmt.Errorf("CLIENT TRACKING ON BCAST: %w", err)
	}

	// Clear the handshake deadline. The read loop manages its own timeouts.
	_ = nc.SetDeadline(time.Time{})

	s.connMu.Lock()
	// Re-check shutdown under the mutex: Shutdown closes s.conn under connMu,
	// but a dial in flight at that moment would otherwise publish a fresh conn
	// afterwards that nothing ever closes.
	if s.isShutdown() {
		s.connMu.Unlock()
		_ = nc.Close()
		return errors.New("csc sidecar: shut down during handshake")
	}
	s.conn = nc
	s.reader = rd
	s.writer = wr
	s.bw = bw
	s.connMu.Unlock()

	internal.Logger.Printf(ctx,
		"csc sidecar: connected and subscribed to BCAST invalidations (db=%d)", s.db)
	return nil
}

// readLoop owns the proto.Reader and dispatches incoming push frames until
// Shutdown closes s.done.
//
// Lifecycle:
//   - If a read returns an error we treat it as a disconnect, close the
//     current socket and reconnect with exponential backoff.
//   - During reconnect, ready is flipped to false so tests can observe it.
//   - After Shutdown, we exit immediately without attempting reconnect.
func (s *broadcastSidecar) readLoop() {
	defer close(s.loopDone)

	backoff := s.backoffInitial
	pingOutstanding := false
	for {
		select {
		case <-s.done:
			return
		default:
		}

		s.connMu.Lock()
		rd := s.reader
		cn := s.conn
		s.connMu.Unlock()
		if rd == nil {
			// Lost connection — reconnect.
			s.ready.Store(false)
			if !s.sleepWithDone(backoff) {
				return
			}
			if err := s.dialAndHandshake(context.Background()); err != nil {
				internal.Logger.Printf(context.Background(),
					"csc sidecar: reconnect failed: %v", err)
				backoff = nextBackoff(backoff, s.backoffMax)
				continue
			}
			// Flush AFTER the new BCAST subscription is live. tearDownConn
			// already flushed at disconnect, but the data path keeps
			// fulfilling cache entries WHILE we were disconnected — those
			// entries have no invalidate coverage (BCAST only emits for
			// writes after subscription). Dropping them here bounds
			// staleness to the outage window; everything cached after this
			// point is covered by the new subscription.
			if s.cache != nil {
				s.cache.Flush()
			}
			backoff = s.backoffInitial
			pingOutstanding = false
			s.ready.Store(true)
			continue
		}

		// Block on the next push frame, bounded by the health-check window:
		// a server that pushes nothing for that long gets probed with PING
		// (below); a silently dead socket therefore surfaces within two
		// windows instead of waiting for OS TCP keepalive.
		_ = cn.SetReadDeadline(time.Now().Add(s.healthCheckInterval))
		replyType, err := rd.PeekReplyType()
		if err != nil {
			if s.isShutdown() {
				return
			}
			var netErr net.Error
			if errors.As(err, &netErr) && netErr.Timeout() && !pingOutstanding {
				// Quiet window, no probe in flight: PING the server. The
				// PONG arrives as a non-push frame and is discarded below,
				// resetting pingOutstanding.
				if perr := s.writePing(); perr == nil {
					pingOutstanding = true
					continue
				}
			}
			// Disconnect / EOF / unanswered probe / probe write failure —
			// invalidate the conn and let the next iteration reconnect.
			internal.Logger.Printf(context.Background(),
				"csc sidecar: read error, will reconnect: %v", err)
			s.tearDownConn()
			pingOutstanding = false
			continue
		}
		// Any inbound traffic proves liveness.
		pingOutstanding = false

		if replyType != proto.RespPush {
			// A non-push frame is either the PONG from our liveness probe or
			// a protocol violation by the server (we issue no other commands
			// after handshake). Drop the frame either way.
			if _, err := rd.ReadReply(); err != nil {
				if !s.isShutdown() {
					internal.Logger.Printf(context.Background(),
						"csc sidecar: discard non-push reply failed: %v", err)
					s.tearDownConn()
				}
			}
			continue
		}

		reply, err := rd.ReadReply()
		if err != nil {
			if s.isShutdown() {
				return
			}
			internal.Logger.Printf(context.Background(),
				"csc sidecar: read push reply failed: %v", err)
			s.tearDownConn()
			continue
		}

		notif, ok := reply.([]interface{})
		if !ok || len(notif) == 0 {
			continue
		}

		name, _ := notif[0].(string)
		if name != invalidatePushName {
			// Some other push (e.g. pubsub) — ignore.
			continue
		}
		s.handleInvalidate(notif)
	}
}

// handleInvalidate mirrors invalidateHandler.HandlePushNotification, but
// operates without the push.NotificationHandlerContext indirection because
// the sidecar owns its own dispatch path.
func (s *broadcastSidecar) handleInvalidate(notif []interface{}) {
	if len(notif) < 2 || s.cache == nil {
		return
	}
	switch payload := notif[1].(type) {
	case nil:
		s.cache.Flush()
	case []interface{}:
		var bytesConsumed int64
		for _, k := range payload {
			var name string
			switch v := k.(type) {
			case string:
				name = v
			case []byte:
				name = string(v)
			default:
				continue
			}
			bytesConsumed += int64(len(name))
			s.cache.DeleteByRedisKey(dbNamespacedKey(s.db, name))
		}
		if bytesConsumed > 0 {
			proto.InvalidationBytesRead.Add(bytesConsumed)
		}
	}
}

// writePing writes a PING on the sidecar socket to probe liveness. The PONG
// comes back as a normal (non-push) frame and is discarded by the read loop.
func (s *broadcastSidecar) writePing() error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn == nil || s.writer == nil || s.bw == nil {
		return errors.New("csc sidecar: no connection")
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(s.opt.DialTimeout))
	defer func() { _ = s.conn.SetWriteDeadline(time.Time{}) }()
	if err := writeCmd(s.writer, NewStatusCmd(context.Background(), "ping")); err != nil {
		return err
	}
	return s.bw.Flush()
}

func (s *broadcastSidecar) isShutdown() bool {
	select {
	case <-s.done:
		return true
	default:
		return false
	}
}

// tearDownConn closes the current sidecar connection and flushes the entire
// shared cache. The flush is the correctness-critical half of this function:
// while the sidecar is disconnected, the Redis server has dropped its
// CLIENT TRACKING table for that connection (server-side state is bound to the
// TCP connection). Any writes that happen during the gap — including writes
// from this same client's functional pool — will NOT generate invalidations
// we observe, so every entry already in the cache is potentially stale the
// moment the sidecar disconnects.
func (s *broadcastSidecar) tearDownConn() {
	s.connMu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
		s.reader = nil
		s.writer = nil
		s.bw = nil
	}
	s.connMu.Unlock()
	s.ready.Store(false)
	if s.cache != nil {
		s.cache.Flush()
	}
}

// sleepWithDone sleeps for d but returns false if Shutdown fires during
// the sleep so the caller can exit promptly.
func (s *broadcastSidecar) sleepWithDone(d time.Duration) bool {
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-s.done:
		return false
	}
}

// nextBackoff doubles the current backoff up to max.
func nextBackoff(current, max time.Duration) time.Duration {
	next := current * 2
	if next > max {
		return max
	}
	if next <= 0 {
		return 100 * time.Millisecond
	}
	return next
}
