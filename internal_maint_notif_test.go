package redis

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestInitConnNilMaintNotificationsConfig is a regression test for
// https://github.com/redis/go-redis/issues/3675
//
// initConn previously accessed MaintNotificationsConfig.EndpointType
// unconditionally, even though the preceding line correctly nil-checked
// MaintNotificationsConfig. This caused a nil pointer dereference panic
// and left optLock.RLock held, leading to a subsequent deadlock.
func TestInitConnNilMaintNotificationsConfig(t *testing.T) {
	// Start a minimal TCP server that speaks enough RESP to let
	// initConn get past the HELLO / AUTH / pipeline phases and reach
	// the MaintNotificationsConfig code path.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	defer ln.Close()

	// mockRedis responds to every RESP command with a Redis-protocol
	// error. This lets initConn fall through HELLO (Redis errors are
	// not fatal when there is no password) and the empty pipeline
	// succeeds trivially.
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func(c net.Conn) {
				defer c.Close()
				scanner := bufio.NewScanner(c)
				for scanner.Scan() {
					line := scanner.Text()
					if strings.HasPrefix(line, "*") {
						_, _ = c.Write([]byte("-ERR unknown command\r\n"))
					}
				}
			}(conn)
		}
	}()

	opt := &Options{
		Addr: ln.Addr().String(),
	}
	opt.init()

	// Force MaintNotificationsConfig to nil after init() to reproduce
	// the scenario from issue #3675.
	opt.MaintNotificationsConfig = nil

	c := &baseClient{
		opt: opt,
	}
	c.initHooks(hooks{
		dial:       c.dial,
		process:    c.process,
		pipeline:   c.processPipeline,
		txPipeline: c.processTxPipeline,
	})

	// Dial a real connection to the mock server.
	netConn, err := net.DialTimeout("tcp", ln.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial mock server: %v", err)
	}
	defer netConn.Close()

	cn := pool.NewConn(netConn)
	// Put the connection into INITIALIZING state so initConn proceeds
	// with the full initialization logic.
	cn.GetStateMachine().Transition(pool.StateInitializing)

	// initConn must not panic. Any returned error is acceptable (the
	// mock server does not implement a full Redis protocol), but a
	// nil-pointer panic on MaintNotificationsConfig is the bug we
	// guard against.
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("initConn panicked with nil MaintNotificationsConfig: %v", r)
		}
	}()

	_ = c.initConn(context.Background(), cn)
}


// mockRESP2Server is a minimal RESP server used to exercise the HELLO
// fallback path in initConn. It replies with a Redis protocol error to
// HELLO (simulating a server that only speaks RESP2) and with +OK to every
// other command. It records every command received so tests can assert
// which commands were (or were not) sent on the wire.
type mockRESP2Server struct {
	ln       net.Listener
	mu       sync.Mutex
	commands []string
}

func (s *mockRESP2Server) Addr() string { return s.ln.Addr().String() }
func (s *mockRESP2Server) Close()       { _ = s.ln.Close() }

func (s *mockRESP2Server) Commands() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.commands))
	copy(out, s.commands)
	return out
}

// readRESPCommand reads a single RESP array command from r and returns its
// arguments as strings.
func readRESPCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array header, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		if !strings.HasPrefix(hdr, "$") {
			return nil, fmt.Errorf("expected bulk header, got %q", hdr)
		}
		length, err := strconv.Atoi(hdr[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, length)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		if _, err := r.Discard(2); err != nil {
			return nil, err
		}
		args = append(args, string(buf))
	}
	return args, nil
}

func (s *mockRESP2Server) handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)
	for {
		args, err := readRESPCommand(r)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		name := strings.ToUpper(args[0])
		full := name
		if len(args) > 1 {
			full = name + " " + strings.ToUpper(args[1])
		}
		s.mu.Lock()
		s.commands = append(s.commands, full)
		s.mu.Unlock()

		if name == "HELLO" {
			_, _ = c.Write([]byte("-ERR unknown command 'hello'\r\n"))
			continue
		}
		_, _ = c.Write([]byte("+OK\r\n"))
	}
}

func startMockRESP2Server(t *testing.T) *mockRESP2Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := &mockRESP2Server{ln: ln}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go s.handle(conn)
		}
	}()
	return s
}


// assertNoMaintNotifications fails the test if a CLIENT MAINT_NOTIFICATIONS
// command was observed by srv.
func assertNoMaintNotifications(t *testing.T, srv *mockRESP2Server) {
	t.Helper()
	for _, cmd := range srv.Commands() {
		if cmd == "CLIENT MAINT_NOTIFICATIONS" {
			t.Fatalf("CLIENT MAINT_NOTIFICATIONS must not be sent when HELLO falls back to RESP2; commands observed: %v", srv.Commands())
		}
	}
}

// initConnOnMockServer dials srv, puts the connection into INITIALIZING and
// runs c.initConn against it. It returns the error from initConn (if any).
func initConnOnMockServer(t *testing.T, c *baseClient, srv *mockRESP2Server) error {
	t.Helper()
	netConn, err := net.DialTimeout("tcp", srv.Addr(), 2*time.Second)
	if err != nil {
		t.Fatalf("failed to dial mock server: %v", err)
	}
	t.Cleanup(func() { _ = netConn.Close() })
	cn := pool.NewConn(netConn)
	cn.GetStateMachine().Transition(pool.StateInitializing)
	return c.initConn(context.Background(), cn)
}

// newTestBaseClient builds a baseClient wired up with hooks for use in
// tests that exercise initConn directly.
func newTestBaseClient(opt *Options) *baseClient {
	c := &baseClient{opt: opt}
	c.initHooks(hooks{
		dial:       c.dial,
		process:    c.process,
		pipeline:   c.processPipeline,
		txPipeline: c.processTxPipeline,
	})
	return c
}

// TestInitConn_HelloFallback_ModeEnabled verifies that when the server
// rejects HELLO (so the connection falls back to RESP2) and the user has
// explicitly requested ModeEnabled with Protocol: 3, initConn fails with a
// clear RESP3-related error instead of silently sending a meaningless
// CLIENT MAINT_NOTIFICATIONS command on a RESP2 connection.
func TestInitConn_HelloFallback_ModeEnabled(t *testing.T) {
	srv := startMockRESP2Server(t)
	defer srv.Close()

	opt := &Options{
		Addr:     srv.Addr(),
		Protocol: 3,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeEnabled,
		},
	}
	opt.init()

	c := newTestBaseClient(opt)
	err := initConnOnMockServer(t, c, srv)
	if err == nil {
		t.Fatalf("expected initConn to fail when HELLO falls back to RESP2 with ModeEnabled, got nil")
	}
	if !strings.Contains(err.Error(), "RESP3") {
		t.Fatalf("expected error to mention RESP3, got %v", err)
	}
	assertNoMaintNotifications(t, srv)
}

// TestInitConn_HelloFallback_ModeAuto verifies that when HELLO is rejected
// and ModeAuto is configured, initConn succeeds without sending CLIENT
// MAINT_NOTIFICATIONS and the feature is silently disabled on the client.
func TestInitConn_HelloFallback_ModeAuto(t *testing.T) {
	srv := startMockRESP2Server(t)
	defer srv.Close()

	opt := &Options{
		Addr:     srv.Addr(),
		Protocol: 3,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeAuto,
		},
	}
	opt.init()

	c := newTestBaseClient(opt)
	if err := initConnOnMockServer(t, c, srv); err != nil {
		t.Fatalf("initConn returned error in ModeAuto (expected silent disable and success): %v", err)
	}
	assertNoMaintNotifications(t, srv)
	if opt.MaintNotificationsConfig.Mode != maintnotifications.ModeDisabled {
		t.Fatalf("expected mode to be silently set to Disabled after RESP2 fallback, got %q", opt.MaintNotificationsConfig.Mode)
	}
}
