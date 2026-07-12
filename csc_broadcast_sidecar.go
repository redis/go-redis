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

	"github.com/redis/go-redis/v9/auth"
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
// Authentication: the sidecar authenticates at HELLO using the non-streaming
// credential sources (Options.resolveCredentials: CredentialsProviderContext →
// CredentialsProvider → static Username/Password). It ALSO supports
// Options.StreamingCredentialsProvider (rotating tokens): it subscribes for
// updates, uses the latest credentials for each HELLO, and on a rotation issues
// AUTH in place on the live connection (see sidecarCredsListener) so the new
// token takes effect WITHOUT dropping the BCAST subscription or flushing the
// cache. It falls back to a reconnect only when no live connection exists or
// the AUTH write fails.
//
// Not yet wired: maintnotifications (cluster maintenance push events). The
// sidecar reads push frames but only acts on `invalidate`; participating in
// MOVING/MIGRATING handoffs is a follow-up.
type broadcastSidecar struct {
	opt   *Options
	cache Cache
	db    int

	// credsMu guards latestCreds and unsubscribe.
	credsMu sync.Mutex
	// latestCreds is the most recent credential from a StreamingCredentialsProvider
	// (nil when none is configured, falling back to opt.resolveCredentials).
	latestCreds auth.Credentials
	// unsubscribe cancels the streaming-credentials subscription; called on
	// Shutdown to avoid leaking a listener in the provider.
	unsubscribe auth.UnsubscribeFunc
	// credsGen is bumped on every credential rotation (atomic so the handshake
	// can capture-and-recheck without nesting credsMu under connMu). A dial that
	// authenticated with a superseded token is detected at publish and redialed.
	credsGen atomic.Uint64

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
		opt:            opt,
		cache:          cache,
		db:             db,
		done:           make(chan struct{}),
		loopDone:       make(chan struct{}),
		backoffInitial: 100 * time.Millisecond,
		backoffMax:     30 * time.Second,
		// A silent (no FIN/RST) drop is detected within ~2 of these windows
		// (probe, then unanswered probe), during which stale hits are served;
		// kept short to bound that. PING on a quiet sidecar is negligible.
		healthCheckInterval: 10 * time.Second,
	}
}

// subscribeCredentials subscribes to Options.StreamingCredentialsProvider (if
// set), storing the current credentials and the unsubscribe func. On failure it
// logs and falls back to the static credential sources.
func (s *broadcastSidecar) subscribeCredentials() {
	if s.opt.StreamingCredentialsProvider == nil {
		return
	}
	creds, unsub, err := s.opt.StreamingCredentialsProvider.Subscribe(&sidecarCredsListener{s: s})
	if err != nil {
		internal.Logger.Printf(context.Background(),
			"csc sidecar: streaming credentials subscribe failed (falling back to static credentials): %v", err)
		return
	}
	s.credsMu.Lock()
	// Some providers deliver the first credential via a synchronous OnNext
	// inside Subscribe; if that already ran, don't clobber it with the older
	// return value.
	if s.latestCreds == nil {
		s.latestCreds = creds
	}
	s.unsubscribe = unsub
	s.credsMu.Unlock()
}

// handshakeCredentials returns the HELLO AUTH credentials (latest streaming
// credential, else static sources) plus the rotation generation observed, which
// lets dialAndHandshake detect a rotation that lands mid-handshake and redial.
func (s *broadcastSidecar) handshakeCredentials(ctx context.Context) (username, password string, gen uint64, err error) {
	gen = s.credsGen.Load()
	s.credsMu.Lock()
	creds := s.latestCreds
	s.credsMu.Unlock()
	if creds != nil {
		username, password = creds.BasicAuth()
		return username, password, gen, nil
	}
	username, password, err = s.opt.resolveCredentials(ctx)
	return username, password, gen, err
}

// sidecarCredsListener bridges a StreamingCredentialsProvider to the sidecar:
// a new credential is applied via in-place re-AUTH (see OnNext).
type sidecarCredsListener struct {
	s *broadcastSidecar
}

func (l *sidecarCredsListener) OnNext(creds auth.Credentials) {
	if creds == nil {
		// A nil credential can't authenticate; reconnect to re-resolve rather
		// than store a nil that would panic in BasicAuth.
		l.s.tearDownConn()
		return
	}
	l.s.credsMu.Lock()
	l.s.latestCreds = creds
	l.s.credsMu.Unlock()
	// Bump the generation so an in-flight handshake (which captured the old gen)
	// redials instead of publishing a conn on the old token.
	l.s.credsGen.Add(1)
	// Prefer in-place re-AUTH so the new token takes effect without dropping the
	// BCAST subscription (which would flush the cache). Fall back to a reconnect
	// when there's no live conn (mid-handshake; the gen bump handles it) or the
	// AUTH write fails.
	if username, password := creds.BasicAuth(); password != "" {
		if err := l.s.reauthConn(username, password); err == nil {
			return
		}
	}
	l.s.tearDownConn()
}

func (l *sidecarCredsListener) OnError(err error) {
	internal.Logger.Printf(context.Background(), "csc sidecar: streaming credentials error: %v", err)
}

// Start attempts the initial dial and BCAST handshake, then spawns the read
// loop. A failed first connect is not fatal: the read loop reconnects with
// backoff (cache stays bypassed via ready=false until live). Returns an error
// only for unrecoverable configuration problems.
func (s *broadcastSidecar) Start(ctx context.Context) error {
	if s.opt.Dialer == nil {
		return errors.New("csc sidecar: nil dialer")
	}
	// Subscribe BEFORE the first handshake so it authenticates with the current
	// token; later rotations are handled by sidecarCredsListener.OnNext.
	s.subscribeCredentials()
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

// signalShutdown requests the read loop to exit WITHOUT joining it, so it is
// safe from a runtime.AddCleanup finalizer (which must not block): unsubscribe,
// close done (the loop exits on its own), flip ready=false, close the socket.
// Idempotent.
func (s *broadcastSidecar) signalShutdown() {
	// Unsubscribe first so no rotation triggers a reconnect during teardown and
	// the listener is released.
	s.credsMu.Lock()
	unsub := s.unsubscribe
	s.unsubscribe = nil
	s.credsMu.Unlock()
	if unsub != nil {
		_ = unsub()
	}

	s.doneOnce.Do(func() { close(s.done) })
	// Stop serving hits now: after shutdown no invalidations flow, so a stale
	// ready flag would let a closing client keep serving un-evictable hits.
	s.ready.Store(false)

	s.connMu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.connMu.Unlock()
}

// Shutdown signals the read loop to exit and JOINS it (bounded to 5s) so the
// caller knows the goroutine is gone. Used on explicit client Close; the GC
// safety net uses signalShutdown instead (must not block).
func (s *broadcastSidecar) Shutdown() {
	s.signalShutdown()

	select {
	case <-s.loopDone:
	case <-time.After(5 * time.Second):
		internal.Logger.Printf(context.Background(),
			"csc sidecar: shutdown timed out waiting for read loop to exit")
	}
	// A reconnect racing the first Store(false) could set ready=true; re-clear
	// after the loop exits so the terminal state is always false.
	s.ready.Store(false)
}

// dialAndHandshake dials a fresh connection, runs the RESP3 HELLO handshake and
// CLIENT TRACKING ON BCAST, and on success publishes conn/reader/writer under
// connMu. Handshake commands reuse the normal client's Cmder constructors so
// framing/auth/tracking-option encoding live in one place.
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
	username, password, credsGen, err := s.handshakeCredentials(ctx)
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
	// Re-check shutdown under the mutex: a dial racing Shutdown would otherwise
	// publish a conn nothing ever closes.
	if s.isShutdown() {
		s.connMu.Unlock()
		_ = nc.Close()
		return errors.New("csc sidecar: shut down during handshake")
	}
	// Re-check the credential generation: a rotation that landed after we read
	// the credentials would leave this conn on the superseded token. Abandon it;
	// the read loop redials with the current one.
	if s.credsGen.Load() != credsGen {
		s.connMu.Unlock()
		_ = nc.Close()
		return errors.New("csc sidecar: credentials rotated during handshake")
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

		// Give the frame body a fresh read window rather than the leftover
		// pre-peek deadline, so a slow body isn't mistaken for a dead conn.
		_ = cn.SetReadDeadline(time.Now().Add(s.healthCheckInterval))

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
			s.cache.DeleteByRedisKey(dbNamespacedKey(s.db, name))
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

// reauthConn issues AUTH on the live connection so a rotated token applies
// without dropping the BCAST subscription (which would flush the cache). The
// +OK reply is discarded by the read loop's non-push branch, like the PING
// probe. Returns an error (no live conn / write failed) so the caller can
// fall back to a reconnect.
func (s *broadcastSidecar) reauthConn(username, password string) error {
	s.connMu.Lock()
	defer s.connMu.Unlock()
	if s.conn == nil || s.writer == nil || s.bw == nil {
		return errors.New("csc sidecar: no connection")
	}
	var cmd Cmder
	if username != "" {
		cmd = NewStatusCmd(context.Background(), "auth", username, password)
	} else {
		cmd = NewStatusCmd(context.Background(), "auth", password)
	}
	_ = s.conn.SetWriteDeadline(time.Now().Add(s.opt.DialTimeout))
	defer func() { _ = s.conn.SetWriteDeadline(time.Time{}) }()
	if err := writeCmd(s.writer, cmd); err != nil {
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
