package pubsub

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
)

// fakeServer hands out net.Pipe-backed connections to the manager under
// test. Each dial yields a fakeServerConn holding the server end of the
// pipe: a read loop parses the RESP commands the manager writes into
// cmds, and tests inject inbound frames by writing raw RESP to the pipe
// (net.Pipe is synchronous, so a write returns once the manager's read
// loop consumed the frame).
type fakeServer struct {
	mu      sync.Mutex
	dialErr error // when set, dials fail until cleared
	// autoConfirm, when set before dialing, arms autoConfirm on every
	// connection the server hands out.
	autoConfirm bool
	// dialGate, when set, parks every dial (after signaling gateHit)
	// until the gate channel is closed — how tests hold an operation
	// mid-flight to interleave a concurrent call deterministically. The
	// park honors the dial context, like a real dialer: a deadline-bound
	// dial (e.g. reconnect's ReconnectTimeout) fails instead of parking
	// past its deadline.
	dialGate chan struct{}

	dialCh  chan *fakeServerConn
	gateHit chan struct{}
}

type fakeServerConn struct {
	addr string
	conn net.Conn // server end of the pipe
	// poolConn is the *pool.Conn handed to the manager, kept so tests
	// can mark it (e.g. MarkForHandoff).
	poolConn *pool.Conn

	cmds chan []string
	done chan struct{} // closed when the read loop exits (conn closed)
	// paused stops the read loop between frames; with net.Pipe unread,
	// the manager's next write then blocks until its write deadline —
	// how tests force a deterministic write timeout.
	paused atomic.Bool
	// autoConfirm makes the read loop answer every (p/s)(un)subscribe
	// command with confirmation frames, like a real server. The manager
	// completes deferred unsubscribes only on those confirmations, so
	// most teardown flows need it.
	autoConfirm atomic.Bool
	subCount    int // read-loop only: the running per-conn count

	// frames feeds the single writer goroutine that serializes test
	// injections and auto-confirmations onto the pipe. Writing from the
	// read loop directly can deadlock the synchronous pipe: the manager
	// may block in a write while holding its lock, which the listen
	// loop needs before it can consume our frame.
	frames chan string
}

func newFakeServer() *fakeServer {
	return &fakeServer{
		dialCh:  make(chan *fakeServerConn, 16),
		gateHit: make(chan struct{}, 16),
	}
}

func (s *fakeServer) dial(ctx context.Context, addr string) (*pool.Conn, error) {
	s.mu.Lock()
	err := s.dialErr
	gate := s.dialGate
	s.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if gate != nil {
		select {
		case s.gateHit <- struct{}{}:
		default:
		}
		select {
		case <-gate:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	client, server := net.Pipe()
	fsc := &fakeServerConn{
		addr:   addr,
		conn:   server,
		cmds:   make(chan []string, 100),
		done:   make(chan struct{}),
		frames: make(chan string, 256),
	}
	s.mu.Lock()
	fsc.autoConfirm.Store(s.autoConfirm)
	s.mu.Unlock()
	fsc.poolConn = pool.NewConn(client)
	go fsc.readLoop()
	go fsc.writeLoop()

	s.dialCh <- fsc
	return fsc.poolConn, nil
}

func (s *fakeServer) setDialErr(err error) {
	s.mu.Lock()
	s.dialErr = err
	s.mu.Unlock()
}

// setDialGate arms (or, with nil, disarms) the dial gate. Closing the
// gate channel releases every parked and future dial.
func (s *fakeServer) setDialGate(gate chan struct{}) {
	s.mu.Lock()
	s.dialGate = gate
	s.mu.Unlock()
}

// waitGateHit blocks until a dial parks at the gate.
func (s *fakeServer) waitGateHit(t *testing.T) {
	t.Helper()
	select {
	case <-s.gateHit:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a dial to reach the gate")
	}
}

// waitDial returns the next dialed connection.
func (s *fakeServer) waitDial(t *testing.T) *fakeServerConn {
	t.Helper()
	select {
	case fsc := <-s.dialCh:
		return fsc
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a dial")
		return nil
	}
}

// readLoop parses the RESP commands the manager writes.
func (c *fakeServerConn) readLoop() {
	defer close(c.done)
	rd := proto.NewReader(c.conn)
	for {
		for c.paused.Load() {
			time.Sleep(time.Millisecond)
		}
		reply, err := rd.ReadReply()
		if err != nil {
			return
		}
		args, ok := reply.([]any)
		if !ok {
			continue
		}
		cmd := make([]string, 0, len(args))
		for _, a := range args {
			cmd = append(cmd, fmt.Sprint(a))
		}
		c.cmds <- cmd

		if c.autoConfirm.Load() {
			c.confirm(cmd)
		}
	}
}

// confirm answers a (p/s)(un)subscribe command with one confirmation
// frame per name, mirroring a real server.
func (c *fakeServerConn) confirm(cmd []string) {
	switch cmd[0] {
	case "subscribe", "psubscribe", "ssubscribe":
		for _, name := range cmd[1:] {
			c.subCount++
			c.writeRaw(confirmFrame(cmd[0], name, c.subCount))
		}
	case "unsubscribe", "punsubscribe", "sunsubscribe":
		for _, name := range cmd[1:] {
			if c.subCount > 0 {
				c.subCount--
			}
			c.writeRaw(confirmFrame(cmd[0], name, c.subCount))
		}
	}
}

// writeLoop is the single pipe writer; it exits when the read loop
// observes the connection closed.
func (c *fakeServerConn) writeLoop() {
	for {
		select {
		case frame := <-c.frames:
			_ = c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
			_, _ = c.conn.Write([]byte(frame))
		case <-c.done:
			return
		}
	}
}

// writeRaw enqueues frame bytes without a testing.T (read-loop side);
// a closed conn just drops them like a broken socket would.
func (c *fakeServerConn) writeRaw(frame string) {
	select {
	case c.frames <- frame:
	case <-c.done:
	}
}

func confirmFrame(kind, channel string, count int) string {
	return fmt.Sprintf("*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n:%d\r\n",
		len(kind), kind, len(channel), channel, count)
}

// waitCmd returns the next command the manager wrote on this conn.
func (c *fakeServerConn) waitCmd(t *testing.T) []string {
	t.Helper()
	select {
	case cmd := <-c.cmds:
		return cmd
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a command")
		return nil
	}
}

// expectCmd asserts the next command matches want exactly.
func (c *fakeServerConn) expectCmd(t *testing.T, want ...string) {
	t.Helper()
	if got := c.waitCmd(t); strings.Join(got, " ") != strings.Join(want, " ") {
		t.Fatalf("command = %v, want %v", got, want)
	}
}

// expectNoCmd asserts no command arrives within d.
func (c *fakeServerConn) expectNoCmd(t *testing.T, d time.Duration) {
	t.Helper()
	select {
	case cmd := <-c.cmds:
		t.Fatalf("unexpected command %v", cmd)
	case <-time.After(d):
	}
}

// expectClosed asserts the manager closed this connection.
func (c *fakeServerConn) expectClosed(t *testing.T) {
	t.Helper()
	select {
	case <-c.done:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for the conn to %q to close", c.addr)
	}
}

func (c *fakeServerConn) write(t *testing.T, data string) {
	t.Helper()
	select {
	case c.frames <- data:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out enqueueing a server frame")
	}
}

// sendMessage delivers a message/pmessage/smessage frame built from
// bulk strings (kind first).
func (c *fakeServerConn) sendMessage(t *testing.T, kind string, fields ...string) {
	t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "*%d\r\n", 1+len(fields))
	fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(kind), kind)
	for _, f := range fields {
		fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(f), f)
	}
	c.write(t, b.String())
}

// sendConfirm delivers a subscription confirmation frame; count is the
// trailing integer element.
func (c *fakeServerConn) sendConfirm(t *testing.T, kind, channel string, count int) {
	t.Helper()
	c.write(t, fmt.Sprintf("*3\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n:%d\r\n",
		len(kind), kind, len(channel), channel, count))
}

// sendError delivers a RESP error reply (an error on a healthy conn).
func (c *fakeServerConn) sendError(t *testing.T, msg string) {
	t.Helper()
	c.write(t, "-"+msg+"\r\n")
}

// testConfig returns a Config suited to unit tests: tiny backoffs so
// reconnect loops converge fast, and the health checker disabled so no
// background pings interfere with command assertions.
func testConfig(addr string) Config {
	return Config{
		Addr:                addr,
		WriteTimeout:        time.Second,
		MinRetryBackoff:     time.Millisecond,
		ReconnectMaxBackoff: 10 * time.Millisecond,
		HealthCheckInterval: -1,
		PingTimeout:         time.Second,
		ReconnectTimeout:    time.Second,
		LogInterval:         time.Minute,
		ChanSize:            16,
	}
}

// testIsBadConn mirrors the root package's classification closely
// enough for tests: a RESP error reply — proto.RedisError or one of the
// typed errors carrying the RedisError() marker — leaves the connection
// healthy, anything else (EOF, closed pipe) means broken.
func testIsBadConn(err error, _ bool) bool {
	var redisErr interface {
		error
		RedisError()
	}
	return !errors.As(err, &redisErr)
}

func newTestManager(t *testing.T, srv *fakeServer, cfg Config) *Manager {
	t.Helper()
	return newTestManagerReload(t, srv, cfg, nil)
}

// newTestManagerReload is newTestManager with an observable
// onReconnectFailure hook.
func newTestManagerReload(t *testing.T, srv *fakeServer, cfg Config, onReconnectFailure func()) *Manager {
	t.Helper()
	m := NewManager(
		cfg,
		srv.dial,
		func(cn *pool.Conn) error { return cn.Close() },
		func(ctx context.Context, cn *pool.Conn, rd *proto.Reader) error { return nil },
		testIsBadConn,
		onReconnectFailure,
	)
	t.Cleanup(func() { _ = m.Close() })
	return m
}

// recvMsg receives one message from a delivery channel with a timeout.
func recvMsg(t *testing.T, ch <-chan *Message) *Message {
	t.Helper()
	select {
	case msg, ok := <-ch:
		if !ok {
			t.Fatal("delivery channel closed while waiting for a message")
		}
		return msg
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for a message")
		return nil
	}
}

// expectChanClosed drains ch until it closes or the timeout expires.
func expectChanClosed(t *testing.T, ch <-chan *Message) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for the delivery channel to close")
		}
	}
}

// recvEvent receives one event from a ChannelWithSubscriptions channel.
func recvEvent(t *testing.T, ch <-chan any) any {
	t.Helper()
	select {
	case ev, ok := <-ch:
		if !ok {
			t.Fatal("event channel closed while waiting for an event")
		}
		return ev
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an event")
		return nil
	}
}

// expectEventChanClosed drains ch until it closes or the timeout
// expires.
func expectEventChanClosed(t *testing.T, ch <-chan any) {
	t.Helper()
	deadline := time.After(5 * time.Second)
	for {
		select {
		case _, ok := <-ch:
			if !ok {
				return
			}
		case <-deadline:
			t.Fatal("timed out waiting for the event channel to close")
		}
	}
}
