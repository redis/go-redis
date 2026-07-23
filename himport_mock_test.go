package redis_test

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

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// himportMockServer is a minimal RESP2 server implementing the HIMPORT
// session semantics: fieldsets live per connection, RESET wipes them, and a
// BOOM command drops the connection. It lets the lazy-replay logic be
// exercised end to end without a Redis 8.10 server.
type himportMockServer struct {
	ln net.Listener

	// resp3 makes HELLO succeed so the client negotiates RESP3.
	resp3 bool

	mu           sync.Mutex
	hashes       map[string]map[string]string
	prepareCount int
	// sessions tracks the per-connection fieldset maps of live connections,
	// so tests can observe fieldsets surviving on sessions the client is not
	// currently using.
	sessions map[*himportMockSession]struct{}
	// pushBeforeSetReply, when armed, makes the next HIMPORT SET reply be
	// preceded by an out-of-band RESP3 push frame.
	pushBeforeSetReply bool
}

// himportMockSession is one connection's fieldset state; guarded by the
// server mutex.
type himportMockSession struct {
	fieldsets map[string][]string
}

func newHImportMockServer(t *testing.T) *himportMockServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := &himportMockServer{
		ln:       ln,
		hashes:   make(map[string]map[string]string),
		sessions: make(map[*himportMockSession]struct{}),
	}
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go srv.serve(conn)
		}
	}()
	t.Cleanup(func() { _ = ln.Close() })
	return srv
}

func (s *himportMockServer) addr() string { return s.ln.Addr().String() }

func (s *himportMockServer) prepares() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.prepareCount
}

func (s *himportMockServer) hash(key string) map[string]string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.hashes[key]
}

// totalSessionFieldsets counts fieldsets across all live connection
// sessions — the server-side footprint the lazy discard replay must clean up.
func (s *himportMockServer) totalSessionFieldsets() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for session := range s.sessions {
		n += len(session.fieldsets)
	}
	return n
}

func (s *himportMockServer) armPushBeforeSetReply() {
	s.mu.Lock()
	s.pushBeforeSetReply = true
	s.mu.Unlock()
}

func (s *himportMockServer) consumePushBeforeSetReply() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	armed := s.pushBeforeSetReply
	s.pushBeforeSetReply = false
	return armed
}

// readCommand parses one RESP2 array-of-bulk-strings request.
func readCommand(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	if len(line) == 0 || line[0] != '*' {
		return nil, fmt.Errorf("unexpected request line %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		sizeLine, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		sizeLine = strings.TrimSuffix(strings.TrimSuffix(sizeLine, "\n"), "\r")
		if len(sizeLine) == 0 || sizeLine[0] != '$' {
			return nil, fmt.Errorf("unexpected bulk header %q", sizeLine)
		}
		size, err := strconv.Atoi(sizeLine[1:])
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

func (s *himportMockServer) serve(conn net.Conn) {
	defer conn.Close()
	rd := bufio.NewReader(conn)

	// Per-connection session state, visible to tests through the server.
	session := &himportMockSession{fieldsets: make(map[string][]string)}
	s.mu.Lock()
	s.sessions[session] = struct{}{}
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.sessions, session)
		s.mu.Unlock()
	}()

	reply := func(msg string) bool {
		_, err := conn.Write([]byte(msg))
		return err == nil
	}

	for {
		args, err := readCommand(rd)
		if err != nil {
			return
		}
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			if s.resp3 {
				if !reply("%3\r\n$6\r\nserver\r\n$5\r\nredis\r\n$7\r\nversion\r\n$6\r\n8.10.0\r\n$5\r\nproto\r\n:3\r\n") {
					return
				}
				continue
			}
			// Pre-RESP3 server: the client falls back to RESP2.
			if !reply("-ERR unknown command 'HELLO'\r\n") {
				return
			}
		case "RESET":
			s.mu.Lock()
			session.fieldsets = make(map[string][]string)
			s.mu.Unlock()
			if !reply("+RESET\r\n") {
				return
			}
		case "BOOM":
			// Simulate a dropped connection: close without replying.
			return
		case "HOLD":
			// Occupy this connection long enough for a concurrent caller to
			// be forced onto another pooled connection.
			time.Sleep(150 * time.Millisecond)
			if !reply("+OK\r\n") {
				return
			}
		case "HIMPORT":
			if !s.handleHImport(args, session, reply) {
				return
			}
		default:
			if !reply("+OK\r\n") {
				return
			}
		}
	}
}

func (s *himportMockServer) handleHImport(args []string, session *himportMockSession, reply func(string) bool) bool {
	switch strings.ToUpper(args[1]) {
	case "PREPARE":
		name, fields := args[2], args[3:]
		seen := make(map[string]struct{}, len(fields))
		for _, f := range fields {
			if _, dup := seen[f]; dup {
				return reply("-ERR duplicate field name in fieldset\r\n")
			}
			seen[f] = struct{}{}
		}
		s.mu.Lock()
		session.fieldsets[name] = append([]string(nil), fields...)
		s.prepareCount++
		s.mu.Unlock()
		return reply("+OK\r\n")
	case "SET":
		key, name, values := args[2], args[3], args[4:]
		s.mu.Lock()
		fields, ok := session.fieldsets[name]
		s.mu.Unlock()
		if !ok {
			return reply("-ERR no such fieldset\r\n")
		}
		if len(values) != len(fields) {
			return reply("-ERR value count does not match fieldset field count\r\n")
		}
		hash := make(map[string]string, len(fields))
		for i, f := range fields {
			hash[f] = values[i]
		}
		s.mu.Lock()
		s.hashes[key] = hash
		s.mu.Unlock()
		if s.consumePushBeforeSetReply() {
			// Out-of-band push frame squeezed in right before the SET reply
			// (after the injected PREPARE's reply): the client must drain it
			// instead of consuming it as the SET reply. The payload is sized
			// like a real notification so PeekPushNotificationName's initial
			// peek window (36 bytes) is satisfied without waiting for the
			// read deadline.
			payload := strings.Repeat("x", 32)
			return reply(">2\r\n$8\r\ntestpush\r\n$32\r\n" + payload + "\r\n+OK\r\n")
		}
		return reply("+OK\r\n")
	case "DISCARD":
		s.mu.Lock()
		_, ok := session.fieldsets[args[2]]
		delete(session.fieldsets, args[2])
		s.mu.Unlock()
		if ok {
			return reply(":1\r\n")
		}
		return reply(":0\r\n")
	case "DISCARDALL":
		s.mu.Lock()
		n := len(session.fieldsets)
		session.fieldsets = make(map[string][]string)
		s.mu.Unlock()
		return reply(":" + strconv.Itoa(n) + "\r\n")
	}
	return reply("-ERR Unknown subcommand\r\n")
}

// TestHImportLazyReplay drives the client against the mock server and pins
// the lazy-replay contract: PREPARE runs at most once per connection session
// (NF.2), a session that lost its fieldsets ("no such fieldset" after RESET)
// is transparently re-prepared and retried once, and a brand-new connection
// is prepared on first use.
func TestHImportLazyReplay(t *testing.T) {
	srv := newHImportMockServer(t)
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:            srv.addr(),
		Protocol:        2,
		PoolSize:        1, // deterministic: every command runs on the same connection
		DisableIdentity: true,
	})
	defer client.Close()

	if err := client.HImportPrepare(ctx, "fs", "a", "b").Err(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := client.HImportSet(ctx, "k1", "fs", "1", "2").Err(); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := client.HImportSet(ctx, "k2", "fs", "3", "4").Err(); err != nil {
		t.Fatalf("set k2: %v", err)
	}
	if got := srv.prepares(); got != 1 {
		t.Errorf("prepares after two sets on one session = %d, want 1 (NF.2: replay at most once)", got)
	}
	if h := srv.hash("k1"); h["a"] != "1" || h["b"] != "2" {
		t.Errorf("k1 = %v, want a=1 b=2", h)
	}

	// RESET wipes the server session behind the client's back; the next SET
	// gets "no such fieldset", which must trigger one re-prepare + retry.
	if err := client.Do(ctx, "reset").Err(); err != nil {
		t.Fatalf("reset: %v", err)
	}
	if err := client.HImportSet(ctx, "k3", "fs", "5", "6").Err(); err != nil {
		t.Fatalf("set k3 after reset: %v", err)
	}
	if got := srv.prepares(); got != 2 {
		t.Errorf("prepares after reset recovery = %d, want 2", got)
	}
	if h := srv.hash("k3"); h["a"] != "5" || h["b"] != "6" {
		t.Errorf("k3 = %v, want a=5 b=6", h)
	}

	// BOOM drops the connection; the pool dials a fresh one whose empty
	// session must be prepared before its first SET.
	if err := client.Do(ctx, "boom").Err(); err == nil {
		t.Fatal("boom should surface a connection error")
	}
	prepBefore := srv.prepares()
	if err := client.HImportSet(ctx, "k4", "fs", "7", "8").Err(); err != nil {
		t.Fatalf("set k4 on fresh connection: %v", err)
	}
	if got := srv.prepares(); got != prepBefore+1 {
		t.Errorf("prepares after fresh connection = %d, want %d", got, prepBefore+1)
	}

	// Pipeline on another fresh connection: one injected PREPARE covers all
	// SETs of the same fieldset in the batch.
	if err := client.Do(ctx, "boom").Err(); err == nil {
		t.Fatal("boom should surface a connection error")
	}
	prepBefore = srv.prepares()
	pipe := client.Pipeline()
	set5 := pipe.HImportSet(ctx, "k5", "fs", "9", "10")
	set6 := pipe.HImportSet(ctx, "k6", "fs", "11", "12")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipeline: %v", err)
	}
	if set5.Err() != nil || set6.Err() != nil {
		t.Fatalf("pipeline sets: %v, %v", set5.Err(), set6.Err())
	}
	if got := srv.prepares(); got != prepBefore+1 {
		t.Errorf("prepares after pipeline on fresh connection = %d, want %d", got, prepBefore+1)
	}
	if h := srv.hash("k6"); h["a"] != "11" || h["b"] != "12" {
		t.Errorf("k6 = %v, want a=11 b=12", h)
	}

	// Discard drops the client-side registry entry: the next SET is a raw
	// pass-through and surfaces the server error unchanged.
	if err := client.HImportDiscard(ctx, "fs").Err(); err != nil {
		t.Fatalf("discard: %v", err)
	}
	err := client.HImportSet(ctx, "k7", "fs", "13", "14").Err()
	if err == nil || !strings.Contains(err.Error(), "no such fieldset") {
		t.Errorf("set after discard = %v, want no-such-fieldset pass-through", err)
	}
}

// TestHImportPipelineRetryAfterSessionLoss pins the pipeline recovery
// contract: a pipeline whose HIMPORT SETs fail with "no such fieldset" after
// the session was wiped (RESET) is not auto-retried, but it invalidates the
// connection's prepared flags, so the caller's retry of the pipeline replays
// the PREPARE and succeeds.
func TestHImportPipelineRetryAfterSessionLoss(t *testing.T) {
	srv := newHImportMockServer(t)
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:            srv.addr(),
		Protocol:        2,
		PoolSize:        1,
		DisableIdentity: true,
	})
	defer client.Close()

	if err := client.HImportPrepare(ctx, "fs", "a", "b").Err(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := client.HImportSet(ctx, "k1", "fs", "1", "2").Err(); err != nil {
		t.Fatalf("set k1: %v", err)
	}

	// Wipe the server session behind the client's back; the connection's
	// prepared flag is now stale.
	if err := client.Do(ctx, "reset").Err(); err != nil {
		t.Fatalf("reset: %v", err)
	}

	pipe := client.Pipeline()
	set2 := pipe.HImportSet(ctx, "k2", "fs", "3", "4")
	set3 := pipe.HImportSet(ctx, "k3", "fs", "5", "6")
	_, _ = pipe.Exec(ctx)
	if err := set2.Err(); err == nil || !strings.Contains(err.Error(), "no such fieldset") {
		t.Fatalf("first pipeline exec = %v, want no-such-fieldset (pipelines are not auto-retried)", err)
	}
	if err := set3.Err(); err == nil {
		t.Fatal("first pipeline exec: set3 should fail alongside set2")
	}

	// The failed exec must have invalidated the stale flag: retrying the
	// pipeline on the same connection replays the PREPARE and succeeds.
	prepBefore := srv.prepares()
	pipe = client.Pipeline()
	set2 = pipe.HImportSet(ctx, "k2", "fs", "3", "4")
	set3 = pipe.HImportSet(ctx, "k3", "fs", "5", "6")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipeline retry: %v", err)
	}
	if set2.Err() != nil || set3.Err() != nil {
		t.Fatalf("pipeline retry sets: %v, %v", set2.Err(), set3.Err())
	}
	if got := srv.prepares(); got != prepBefore+1 {
		t.Errorf("prepares after pipeline retry = %d, want %d (single replay)", got, prepBefore+1)
	}
	if h := srv.hash("k3"); h["a"] != "5" || h["b"] != "6" {
		t.Errorf("k3 = %v, want a=5 b=6", h)
	}
}

// TestHImportLazyDiscardPropagation pins the lazy discard contract: a
// DISCARD/DISCARDALL executes on one pooled connection, and other sessions
// still holding the fieldset replay the discard before their next HIMPORT
// command, so post-discard behavior is deterministic and no zombie fieldsets
// survive on connections the client keeps using.
func TestHImportLazyDiscardPropagation(t *testing.T) {
	srv := newHImportMockServer(t)
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:            srv.addr(),
		Protocol:        2,
		PoolSize:        2,
		DisableIdentity: true,
	})
	defer client.Close()

	// runBoth occupies both pooled connections concurrently (HOLD keeps each
	// busy long enough for the other goroutine to be forced onto the second
	// connection) and runs one HIMPORT SET on each; it returns the SET
	// errors.
	runBoth := func(fieldset, keyPrefix string) [2]error {
		var wg sync.WaitGroup
		var errs [2]error
		for i := 0; i < 2; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				pipe := client.Pipeline()
				pipe.Do(ctx, "hold")
				set := pipe.HImportSet(ctx, fmt.Sprintf("%s:%d", keyPrefix, i), fieldset, "v")
				_, _ = pipe.Exec(ctx)
				errs[i] = set.Err()
			}(i)
		}
		wg.Wait()
		return errs
	}

	// Prepare the fieldset on both pooled connections.
	if err := client.HImportPrepare(ctx, "fs", "f").Err(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	for i, err := range runBoth("fs", "seed") {
		if err != nil {
			t.Fatalf("seed set %d: %v", i, err)
		}
	}
	if got := srv.totalSessionFieldsets(); got != 2 {
		t.Fatalf("sessions holding fs = %d, want 2", got)
	}

	// DISCARD executes on one borrowed connection; the other session still
	// holds the fieldset for now.
	if err := client.HImportDiscard(ctx, "fs").Err(); err != nil {
		t.Fatalf("discard: %v", err)
	}
	if got := srv.totalSessionFieldsets(); got != 1 {
		t.Fatalf("sessions holding fs right after discard = %d, want 1", got)
	}

	// The next HIMPORT command on each connection replays the DISCARD: the
	// leftover session copy is removed and every SET fails deterministically.
	for i, err := range runBoth("fs", "late") {
		if err == nil || !strings.Contains(err.Error(), "no such fieldset") {
			t.Errorf("set %d after discard = %v, want no-such-fieldset", i, err)
		}
	}
	if got := srv.totalSessionFieldsets(); got != 0 {
		t.Errorf("sessions holding fs after replayed discard = %d, want 0", got)
	}

	// Same for DISCARDALL, via the epoch: prepare on both connections, wipe
	// through one, and let the other session replay the wipe.
	if err := client.HImportPrepare(ctx, "fs2", "g").Err(); err != nil {
		t.Fatalf("prepare fs2: %v", err)
	}
	for i, err := range runBoth("fs2", "seed2") {
		if err != nil {
			t.Fatalf("seed2 set %d: %v", i, err)
		}
	}
	if got := srv.totalSessionFieldsets(); got != 2 {
		t.Fatalf("sessions holding fs2 = %d, want 2", got)
	}
	if err := client.HImportDiscardAll(ctx).Err(); err != nil {
		t.Fatalf("discardall: %v", err)
	}
	if got := srv.totalSessionFieldsets(); got != 1 {
		t.Fatalf("sessions right after discardall = %d, want 1", got)
	}
	for i, err := range runBoth("fs2", "late2") {
		if err == nil || !strings.Contains(err.Error(), "no such fieldset") {
			t.Errorf("set %d after discardall = %v, want no-such-fieldset", i, err)
		}
	}
	if got := srv.totalSessionFieldsets(); got != 0 {
		t.Errorf("sessions after replayed discardall = %d, want 0", got)
	}
}

// TestHImportInjectedPrepareWithPushNotification pins the RESP3 read
// sequence: a push frame arriving between the injected PREPARE's reply and
// the SET's reply must be drained as a notification, not consumed as the
// SET's reply.
func TestHImportInjectedPrepareWithPushNotification(t *testing.T) {
	srv := newHImportMockServer(t)
	srv.resp3 = true
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:            srv.addr(),
		Protocol:        3,
		PoolSize:        1,
		MaxRetries:      -1, // fail BOOM fast; the injected PREPARE needs no retries
		DisableIdentity: true,
		// The mock is not a real cluster; keep maintenance-notification
		// machinery out of the connection lifecycle.
		MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	defer client.Close()

	if err := client.HImportPrepare(ctx, "fs", "a", "b").Err(); err != nil {
		t.Fatalf("prepare: %v", err)
	}

	// Drop the connection so the next SET runs on a fresh session and gets
	// the PREPARE injected in front of it.
	if err := client.Do(ctx, "boom").Err(); err == nil {
		t.Fatal("boom should surface a connection error")
	}

	prepBefore := srv.prepares()
	srv.armPushBeforeSetReply()
	if err := client.HImportSet(ctx, "k1", "fs", "1", "2").Err(); err != nil {
		t.Fatalf("set with interleaved push notification: %v", err)
	}
	if got := srv.prepares(); got != prepBefore+1 {
		t.Errorf("prepares = %d, want %d (PREPARE must be injected on the fresh connection)", got, prepBefore+1)
	}
	if h := srv.hash("k1"); h["a"] != "1" || h["b"] != "2" {
		t.Errorf("k1 = %v, want a=1 b=2", h)
	}

	// The connection must stay aligned for subsequent commands.
	if err := client.HImportSet(ctx, "k2", "fs", "3", "4").Err(); err != nil {
		t.Fatalf("follow-up set: %v", err)
	}
	if h := srv.hash("k2"); h["a"] != "3" || h["b"] != "4" {
		t.Errorf("k2 = %v, want a=3 b=4", h)
	}
}
