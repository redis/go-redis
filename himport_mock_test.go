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
	// boomBeforeNextPrepare, when armed, drops the connection when the next
	// HIMPORT PREPARE arrives, before replying.
	boomBeforeNextPrepare bool
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

func (s *himportMockServer) hashKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.hashes))
	for k := range s.hashes {
		keys = append(keys, k)
	}
	return keys
}

func (s *himportMockServer) armPushBeforeSetReply() {
	s.mu.Lock()
	s.pushBeforeSetReply = true
	s.mu.Unlock()
}

func (s *himportMockServer) armBoomBeforeNextPrepare() {
	s.mu.Lock()
	s.boomBeforeNextPrepare = true
	s.mu.Unlock()
}

func (s *himportMockServer) consumeBoomBeforeNextPrepare() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	armed := s.boomBeforeNextPrepare
	s.boomBeforeNextPrepare = false
	return armed
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

	var (
		inMulti bool
		queued  [][]string
	)

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
		case "MULTI":
			inMulti = true
			queued = queued[:0]
			if !reply("+OK\r\n") {
				return
			}
		case "EXEC":
			inMulti = false
			var b strings.Builder
			fmt.Fprintf(&b, "*%d\r\n", len(queued))
			for _, q := range queued {
				b.WriteString(s.commandReply(q, session))
			}
			queued = queued[:0]
			if !reply(b.String()) {
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
		default:
			if inMulti {
				queued = append(queued, args)
				if !reply("+QUEUED\r\n") {
					return
				}
				continue
			}
			resp := s.commandReply(args, session)
			if resp == "" {
				// Sentinel from an armed connection drop.
				return
			}
			if strings.ToUpper(args[0]) == "HIMPORT" && strings.ToUpper(args[1]) == "SET" &&
				strings.HasPrefix(resp, "+OK") && s.consumePushBeforeSetReply() {
				// Out-of-band push frame squeezed in right before the SET
				// reply (after the injected PREPARE's reply): the client
				// must drain it instead of consuming it as the SET reply.
				// The payload is sized like a real notification so
				// PeekPushNotificationName's initial peek window (36 bytes)
				// is satisfied without waiting for the read deadline.
				resp = ">2\r\n$8\r\ntestpush\r\n$32\r\n" + strings.Repeat("x", 32) + "\r\n" + resp
			}
			if !reply(resp) {
				return
			}
		}
	}
}

// commandReply produces the RESP reply for one command; used both for direct
// dispatch and for commands queued inside MULTI/EXEC.
func (s *himportMockServer) commandReply(args []string, session *himportMockSession) string {
	if strings.ToUpper(args[0]) == "HIMPORT" {
		return s.himportReply(args, session)
	}
	return "+OK\r\n"
}

func (s *himportMockServer) himportReply(args []string, session *himportMockSession) string {
	switch strings.ToUpper(args[1]) {
	case "PREPARE":
		if s.consumeBoomBeforeNextPrepare() {
			// Simulate the connection dying mid-replay: the caller closes
			// the conn when it sees the empty sentinel.
			return ""
		}
		name, fields := args[2], args[3:]
		seen := make(map[string]struct{}, len(fields))
		for _, f := range fields {
			if _, dup := seen[f]; dup {
				return "-ERR duplicate field name in fieldset\r\n"
			}
			seen[f] = struct{}{}
		}
		s.mu.Lock()
		session.fieldsets[name] = append([]string(nil), fields...)
		s.prepareCount++
		s.mu.Unlock()
		return "+OK\r\n"
	case "SET":
		key, name, values := args[2], args[3], args[4:]
		s.mu.Lock()
		fields, ok := session.fieldsets[name]
		s.mu.Unlock()
		if !ok {
			return "-ERR no such fieldset\r\n"
		}
		if len(values) != len(fields) {
			return "-ERR value count does not match fieldset field count\r\n"
		}
		hash := make(map[string]string, len(fields))
		for i, f := range fields {
			hash[f] = values[i]
		}
		s.mu.Lock()
		s.hashes[key] = hash
		s.mu.Unlock()
		return "+OK\r\n"
	case "DISCARD":
		s.mu.Lock()
		_, ok := session.fieldsets[args[2]]
		delete(session.fieldsets, args[2])
		s.mu.Unlock()
		if ok {
			return ":1\r\n"
		}
		return ":0\r\n"
	case "DISCARDALL":
		s.mu.Lock()
		n := len(session.fieldsets)
		session.fieldsets = make(map[string][]string)
		s.mu.Unlock()
		return ":" + strconv.Itoa(n) + "\r\n"
	}
	return "-ERR Unknown subcommand\r\n"
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

// TestHImportPipelineRecoversAfterSessionLoss pins the pipeline recovery
// contract (NF.4): HIMPORT SETs of a registered fieldset that fail with "no
// such fieldset" after the session was wiped (RESET) are re-prepared and
// re-issued once within the same Exec — the error never reaches the caller,
// and only the SETs run again.
func TestHImportPipelineRecoversAfterSessionLoss(t *testing.T) {
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

	prepBefore := srv.prepares()
	pipe := client.Pipeline()
	other := pipe.Set(ctx, "plain", "x", 0)
	set2 := pipe.HImportSet(ctx, "k2", "fs", "3", "4")
	set3 := pipe.HImportSet(ctx, "k3", "fs", "5", "6")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("pipeline exec: %v", err)
	}
	if other.Err() != nil || set2.Err() != nil || set3.Err() != nil {
		t.Fatalf("pipeline cmds after transparent recovery: %v, %v, %v",
			other.Err(), set2.Err(), set3.Err())
	}
	if got := srv.prepares(); got != prepBefore+1 {
		t.Errorf("prepares after recovery = %d, want %d (single replay)", got, prepBefore+1)
	}
	if h := srv.hash("k3"); h["a"] != "5" || h["b"] != "6" {
		t.Errorf("k3 = %v, want a=5 b=6", h)
	}
}

// TestHImportPipelineReissueTransportErrorScoped pins the failure scoping of
// the pipeline re-issue: when the retry round trip itself dies on a
// transport error, the batch is neither re-executed nor failed wholesale —
// commands whose results were delivered in the first round trip keep them,
// the failed SETs keep their errors, and the suspect connection is discarded.
func TestHImportPipelineReissueTransportErrorScoped(t *testing.T) {
	srv := newHImportMockServer(t)
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:            srv.addr(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1,
		DisableIdentity: true,
	})
	defer client.Close()

	if err := client.HImportPrepare(ctx, "fs", "a").Err(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := client.HImportSet(ctx, "k1", "fs", "1").Err(); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := client.Do(ctx, "reset").Err(); err != nil {
		t.Fatalf("reset: %v", err)
	}

	// The first round trip delivers: plain SET OK, HIMPORT SET fails with
	// "no such fieldset". The re-issue round trip then dies (the mock drops
	// the connection on the replayed PREPARE).
	srv.armBoomBeforeNextPrepare()
	pipe := client.Pipeline()
	plain := pipe.Set(ctx, "plain", "x", 0)
	set2 := pipe.HImportSet(ctx, "k2", "fs", "2")
	_, _ = pipe.Exec(ctx)

	if plain.Err() != nil || plain.Val() != "OK" {
		t.Errorf("delivered plain SET = %q, %v; must keep its first-round result", plain.Val(), plain.Err())
	}
	if err := set2.Err(); err == nil || !strings.Contains(err.Error(), "no such fieldset") {
		t.Errorf("failed HIMPORT SET = %v, want its own no-such-fieldset error", err)
	}

	// The half-read connection was discarded; the next command dials fresh
	// and the registered fieldset replays.
	if err := client.HImportSet(ctx, "k3", "fs", "3").Err(); err != nil {
		t.Fatalf("set k3 on fresh connection: %v", err)
	}
	if h := srv.hash("k3"); h["a"] != "3" {
		t.Errorf("k3 = %v, want a=3", h)
	}
}

// TestHImportTxSurfacesSessionLoss pins the documented transaction
// limitation: an executed MULTI/EXEC cannot be partially re-run, so the "no
// such fieldset" error surfaces — but the stale flag is invalidated, and the
// caller's retry of the transaction succeeds.
func TestHImportTxSurfacesSessionLoss(t *testing.T) {
	srv := newHImportMockServer(t)
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:            srv.addr(),
		Protocol:        2,
		PoolSize:        1,
		DisableIdentity: true,
	})
	defer client.Close()

	if err := client.HImportPrepare(ctx, "fs", "a").Err(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if err := client.HImportSet(ctx, "k1", "fs", "1").Err(); err != nil {
		t.Fatalf("set k1: %v", err)
	}
	if err := client.Do(ctx, "reset").Err(); err != nil {
		t.Fatalf("reset: %v", err)
	}

	tx := client.TxPipeline()
	set := tx.HImportSet(ctx, "k2", "fs", "2")
	_, _ = tx.Exec(ctx)
	if err := set.Err(); err == nil || !strings.Contains(err.Error(), "no such fieldset") {
		t.Fatalf("tx exec after session loss = %v, want no-such-fieldset", err)
	}

	// The stale flag was invalidated: the retried transaction replays the
	// PREPARE and succeeds.
	tx = client.TxPipeline()
	set = tx.HImportSet(ctx, "k2", "fs", "2")
	if _, err := tx.Exec(ctx); err != nil {
		t.Fatalf("tx retry: %v", err)
	}
	if set.Err() != nil {
		t.Fatalf("tx retry set: %v", set.Err())
	}
	if h := srv.hash("k2"); h["a"] != "2" {
		t.Errorf("k2 = %v, want a=2", h)
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

// TestHImportRingFanOut drives the ring (whose shards are plain standalone
// servers, so two mock servers suffice) through the fan-out API: PREPARE
// registers once and eagerly prepares one connection per shard, SETs on any
// shard need no further replay, DISCARD/DISCARDALL clean every shard, and a
// deterministic PREPARE rejection withdraws the registration.
func TestHImportRingFanOut(t *testing.T) {
	srv1 := newHImportMockServer(t)
	srv2 := newHImportMockServer(t)
	ctx := context.Background()

	ring := redis.NewRing(&redis.RingOptions{
		Addrs: map[string]string{
			"shard1": srv1.addr(),
			"shard2": srv2.addr(),
		},
		PoolSize:        1,
		DisableIdentity: true,
	})
	defer ring.Close()

	// Fan-out PREPARE: exactly one connection per shard is prepared.
	if err := ring.HImportPrepare(ctx, "fs", "a", "b").Err(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	if p1, p2 := srv1.prepares(), srv2.prepares(); p1 != 1 || p2 != 1 {
		t.Fatalf("prepares after fan-out = %d/%d, want 1/1", p1, p2)
	}

	// SETs route by key hash to some shard; the fan-out already prepared
	// those connections, so no further PREPARE is sent.
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("ring:%d", i)
		if err := ring.HImportSet(ctx, key, "fs", "1", "2").Err(); err != nil {
			t.Fatalf("set %s: %v", key, err)
		}
	}
	if p1, p2 := srv1.prepares(), srv2.prepares(); p1 != 1 || p2 != 1 {
		t.Errorf("prepares after sets = %d/%d, want 1/1 (no replay needed)", p1, p2)
	}
	if total := len(srv1.hashKeys()) + len(srv2.hashKeys()); total != 20 {
		t.Errorf("hashes across shards = %d, want 20", total)
	}

	// Fan-out DISCARD cleans both shard sessions; the return value reports
	// the registry lifecycle.
	removed, err := ring.HImportDiscard(ctx, "fs").Result()
	if err != nil {
		t.Fatalf("discard: %v", err)
	}
	if removed != 1 {
		t.Errorf("discard = %d, want 1", removed)
	}
	if got := srv1.totalSessionFieldsets() + srv2.totalSessionFieldsets(); got != 0 {
		t.Errorf("session fieldsets after fan-out discard = %d, want 0", got)
	}
	err = ring.HImportSet(ctx, "ring:after", "fs", "1", "2").Err()
	if err == nil || !strings.Contains(err.Error(), "no such fieldset") {
		t.Errorf("set after discard = %v, want no-such-fieldset pass-through", err)
	}

	// A deterministic server rejection (duplicate field) withdraws the
	// registration: no replay is attempted afterwards.
	err = ring.HImportPrepare(ctx, "dup", "f", "f").Err()
	if err == nil || !strings.Contains(err.Error(), "duplicate field name") {
		t.Fatalf("duplicate prepare = %v, want duplicate-field error", err)
	}
	err = ring.HImportSet(ctx, "ring:dup", "dup", "v").Err()
	if err == nil || !strings.Contains(err.Error(), "no such fieldset") {
		t.Errorf("set after rejected prepare = %v, want raw no-such-fieldset", err)
	}

	// Fan-out DISCARDALL reports the registry count and cleans both shards.
	if err := ring.HImportPrepare(ctx, "fs2", "g").Err(); err != nil {
		t.Fatalf("prepare fs2: %v", err)
	}
	count, err := ring.HImportDiscardAll(ctx).Result()
	if err != nil {
		t.Fatalf("discardall: %v", err)
	}
	if count != 1 {
		t.Errorf("discardall = %d, want 1", count)
	}
	if got := srv1.totalSessionFieldsets() + srv2.totalSessionFieldsets(); got != 0 {
		t.Errorf("session fieldsets after fan-out discardall = %d, want 0", got)
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

// TestHImportPipelineInjectedReplyFailureStampsBatch pins the error stamping
// when the injected-reply read dies on a transport error before any batch
// reply was consumed: Exec surfaces the error and every command of the batch
// carries it. The outer retry loop stamps errors only on its exit branch,
// not when the attempt budget runs out mid-loop, so the pipeline path must
// stamp before returning — otherwise a batch that keeps dying here reports
// Err() == nil on every command while Exec errors.
func TestHImportPipelineInjectedReplyFailureStampsBatch(t *testing.T) {
	srv := newHImportMockServer(t)
	ctx := context.Background()

	client := redis.NewClient(&redis.Options{
		Addr:     srv.addr(),
		Protocol: 2,
		PoolSize: 1,
		// Exhaust the budget on the first attempt: stamping must not
		// depend on a later attempt reaching the read path.
		MaxRetries:      -1,
		DisableIdentity: true,
	})
	defer client.Close()

	if err := client.HImportPrepare(ctx, "fs", "a").Err(); err != nil {
		t.Fatalf("prepare: %v", err)
	}
	// Drop the connection so the next batch runs on a fresh session and
	// gets the PREPARE injected ahead of it.
	if err := client.Do(ctx, "boom").Err(); err == nil {
		t.Fatal("boom should surface a connection error")
	}

	// The mock drops the connection when the injected PREPARE arrives: the
	// batch was fully written, not one reply arrives.
	srv.armBoomBeforeNextPrepare()
	pipe := client.Pipeline()
	plain := pipe.Set(ctx, "plain", "x", 0)
	set := pipe.HImportSet(ctx, "k1", "fs", "1")
	if _, err := pipe.Exec(ctx); err == nil {
		t.Fatal("exec must surface the transport error")
	}
	if plain.Err() == nil || set.Err() == nil {
		t.Errorf("batch commands must carry the exec error, got %v / %v",
			plain.Err(), set.Err())
	}

	// Same contract on the transaction path.
	if err := client.Do(ctx, "boom").Err(); err == nil {
		t.Fatal("boom should surface a connection error")
	}
	srv.armBoomBeforeNextPrepare()
	tx := client.TxPipeline()
	txSet := tx.HImportSet(ctx, "k2", "fs", "2")
	if _, err := tx.Exec(ctx); err == nil {
		t.Fatal("tx exec must surface the transport error")
	}
	if txSet.Err() == nil {
		t.Error("tx command must carry the exec error")
	}
}
