package redis

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// partialRetryServer is a scripted RESP server for pipeline retry tests.
// The first connection answers the first pipeline command and then drops
// mid-pipeline; later connections answer everything. It records every
// command name it receives so tests can assert what a retry resent.
type partialRetryServer struct {
	ln       net.Listener
	conns    atomic.Int32
	received atomic.Int32 // total commands seen, all connections
	incrs    atomic.Int32 // INCR commands seen, all connections
}

func newPartialRetryServer(t *testing.T) *partialRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &partialRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

// readPartialRetryCommand reads one RESP array command and returns its
// lower-cased name, or "" when the stream is done.
func readPartialRetryCommand(scanner *bufio.Scanner) string {
	if !scanner.Scan() {
		return ""
	}
	header := scanner.Text()
	if !strings.HasPrefix(header, "*") {
		return ""
	}
	n, err := strconv.Atoi(strings.TrimPrefix(header, "*"))
	if err != nil || n <= 0 {
		return ""
	}
	name := ""
	for i := 0; i < n; i++ {
		if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "$") || !scanner.Scan() {
			return ""
		}
		if i == 0 {
			name = strings.ToLower(scanner.Text())
		}
	}
	return name
}

func (s *partialRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	answered := false
	for {
		name := readPartialRetryCommand(scanner)
		if name == "" {
			return
		}
		s.received.Add(1)
		if name == "incr" {
			s.incrs.Add(1)
		}
		// Connection setup commands (HELLO and friends) are not part of
		// the pipeline: always answer them and never drop on them.
		isPipelineCmd := name == "incr" || name == "set" || name == "get"
		if first && answered && isPipelineCmd {
			// Drop the connection mid-pipeline: this command and the
			// ones after it get no reply on this connection.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "incr":
			reply = ":1\r\n"
		case "get":
			reply = "$1\r\n1\r\n"
		case "ping":
			reply = "+PONG\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
		if isPipelineCmd {
			answered = true
		}
	}
}

func newPartialRetryClient(t *testing.T, addr string) *Client {
	t.Helper()
	client := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestPipelineRetryDoesNotReplayAnsweredCommands: when a connection drops
// after the first pipeline reply was read, the retry must resend only the
// commands that were not answered. Before the fix the whole batch was
// resent, so the server executed the first INCR twice.
// connStateRetryServer is a scripted RESP server for connection-state
// pipeline retry tests. It tracks per-connection state (selected DB,
// MULTI) so tests can assert a retry did not silently drop it. The first
// connection answers the first pipeline command and then drops
// mid-pipeline; later connections answer everything.
type connStateRetryServer struct {
	ln               net.Listener
	conns            atomic.Int32
	setDB            atomic.Int32 // DB in effect when SET last ran, -1 = never
	sets             atomic.Int32
	setWhileWatched  atomic.Int32
	multiCount       atomic.Int32
	execWithMulti    atomic.Int32
	execWithoutMulti atomic.Int32
	execAborted      atomic.Int32 // EXECs that aborted because a watched key was modified
	incrs            atomic.Int32
	selects          atomic.Int32
	// dropAfter is how many pipeline commands the first connection
	// answers before dropping (1 = the historical behaviour).
	dropAfter int
	// secondMaxDB is the highest DB index the replacement (second and
	// later) connections accept; the first connection always accepts
	// 0-15. Tests that need a reissued SELECT to fail on the retry
	// connection set it below 15.
	secondMaxDB int
}

func newConnStateRetryServer(t *testing.T) *connStateRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &connStateRetryServer{ln: ln, dropAfter: 1, secondMaxDB: 15}
	s.setDB.Store(-1)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

// readConnStateRetryCommand reads one RESP array command and returns its
// lower-cased name and arguments, or "" when the stream is done.
func readConnStateRetryCommand(scanner *bufio.Scanner) (string, []string) {
	if !scanner.Scan() {
		return "", nil
	}
	header := scanner.Text()
	if !strings.HasPrefix(header, "*") {
		return "", nil
	}
	n, err := strconv.Atoi(strings.TrimPrefix(header, "*"))
	if err != nil || n <= 0 {
		return "", nil
	}
	var args []string
	for i := 0; i < n; i++ {
		if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "$") || !scanner.Scan() {
			return "", nil
		}
		args = append(args, scanner.Text())
	}
	if len(args) == 0 {
		return "", nil
	}
	return strings.ToLower(args[0]), args[1:]
}

func (s *connStateRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	db := 0
	inMulti := false
	watched := false
	// watchedKeys tracks the keys this connection watches, and dirty
	// whether one of them was modified since the WATCH: like a real
	// server, an EXEC then aborts (nil array) instead of committing.
	watchedKeys := map[string]bool{}
	dirty := false
	answered := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		// Connection setup commands (HELLO and friends) are not part of
		// the pipeline: always answer them and never drop on them.
		isPipelineCmd := name == "select" || name == "set" || name == "get" ||
			name == "multi" || name == "exec" || name == "watch" || name == "unwatch" ||
			name == "incr"
		if first && answered >= s.dropAfter && isPipelineCmd {
			// Drop the connection mid-pipeline: this command and the
			// ones after it get no reply on this connection.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "select":
			s.selects.Add(1)
			if len(args) > 0 {
				if n, err := strconv.Atoi(args[0]); err == nil {
					maxDB := 15
					if !first {
						maxDB = s.secondMaxDB
					}
					if n < 0 || n > maxDB {
						reply = "-ERR DB index is out of range\r\n"
					} else {
						db = n
					}
				}
			}
		case "set":
			s.sets.Add(1)
			s.setDB.Store(int32(db))
			if watched {
				s.setWhileWatched.Add(1)
			}
			if len(args) > 0 && watchedKeys[args[0]] {
				dirty = true
			}
			if inMulti {
				reply = "+QUEUED\r\n"
			}
		case "incr":
			s.incrs.Add(1)
			if len(args) > 0 && watchedKeys[args[0]] {
				dirty = true
			}
			reply = ":1\r\n"
		case "watch":
			watched = true
			for _, key := range args {
				watchedKeys[key] = true
			}
		case "unwatch":
			watched = false
			clear(watchedKeys)
			dirty = false
		case "get":
			reply = "$1\r\nv\r\n"
		case "multi":
			s.multiCount.Add(1)
			inMulti = true
		case "exec":
			if inMulti {
				inMulti = false
				clear(watchedKeys)
				watched = false
				if dirty {
					dirty = false
					s.execAborted.Add(1)
					reply = "*-1\r\n"
				} else {
					s.execWithMulti.Add(1)
					reply = "*1\r\n+OK\r\n"
				}
			} else {
				s.execWithoutMulti.Add(1)
				reply = "-ERR EXEC without MULTI\r\n"
			}
		case "ping":
			reply = "+PONG\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
		if isPipelineCmd {
			answered++
		}
	}
}

// TestPipelineRetryKeepsSelectDB: a pipeline whose first command SELECTs a
// database must not retry its remaining commands on a fresh connection
// that is still on the default database. Regression test for the review
// finding on #4351 (SELECT dropped by the suffix retry, SET lands in DB 0).
func TestPipelineRetryKeepsSelectDB(t *testing.T) {
	server := newConnStateRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.setDB.Load(); got != 2 {
		t.Fatalf("SET ran on DB %d, want 2 (the answered SELECT must not be dropped by the retry)", got)
	}
}

// TestPipelineRetryKeepsMultiTransaction: a MULTI issued in a regular
// pipeline must not be dropped by the suffix retry, or the retried EXEC
// runs on a fresh connection with no transaction open.
func TestPipelineRetryKeepsMultiTransaction(t *testing.T) {
	server := newConnStateRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "k", "v", 0)
	exec := pipe.Do(ctx, "exec")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got := server.execWithoutMulti.Load(); got != 0 {
		t.Fatalf("EXEC ran %d time(s) on a connection with no MULTI open", got)
	}
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("EXEC ran %d time(s) inside MULTI, want 1", got)
	}
}

// TestPipelineRetryKeepsWatch: a WATCH issued in a regular pipeline must
// not be dropped by the suffix retry, or the retried write runs unwatched
// and loses its optimistic-concurrency guard.
func TestPipelineRetryKeepsWatch(t *testing.T) {
	server := newConnStateRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.setWhileWatched.Load(); got != 1 {
		t.Fatalf("SET ran %d time(s) while watched, want 1 (the answered WATCH must not be dropped by the retry)", got)
	}
}

// TestPipelineRetryDirtyWatchAbortsExec: a WATCH followed by an answered
// write that dirtied it must keep the whole batch on retry. Reissuing
// the WATCH alone on a fresh connection would give it a clean watch, so
// the retried EXEC would commit a transaction the original ordering
// would have aborted (the client's own INCR invalidates the watch).
// Regression test for the review finding on #4351.
func TestPipelineRetryDirtyWatchAbortsExec(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	incr := pipe.Incr(ctx, "k")
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "other", "v", 0)
	exec := pipe.Do(ctx, "exec")
	// The aborted EXEC surfaces as redis.Nil, exactly as the same
	// batch would without any disconnect: the transaction aborted
	// because the client's own INCR invalidated the watch.
	if _, err := pipe.Exec(ctx); !errors.Is(err, Nil) {
		t.Fatalf("Exec: %v, want redis.Nil (the retried EXEC must abort, like the same batch without a disconnect)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := exec.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EXEC: %v, want redis.Nil (the aborted EXEC reply)", err)
	}
	// The retried transaction must abort: the answered INCR dirtied the
	// watch, so a clean reissued WATCH would silently commit the SET.
	if got := server.execAborted.Load(); got != 1 {
		t.Fatalf("EXEC aborted %d time(s), want 1 (the retried transaction must abort, not silently commit)", got)
	}
	if got := server.execWithMulti.Load(); got != 0 {
		t.Fatalf("EXEC committed %d time(s), want 0 (a reissued clean WATCH must not let the transaction commit)", got)
	}
	// Whole-batch retry re-runs the answered INCR (the pre-existing
	// whole-batch cost): it ran once on the first connection and once
	// on the retry, which is what dirties the retried watch.
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the answered INCR must be re-run with the whole batch)", got)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil (the retried INCR overwrites the first attempt's reply)", got, err)
	}
}

// syntheticPrefixRetryServer is a scripted RESP server for the synthetic
// prefix retry test. The first connection answers INCR and SELECT and
// then drops before SET's reply; the next connection answers the
// synthetic SELECT and rejects the retried SET with READONLY (failover
// in progress); later connections answer everything, so a correct retry
// eventually applies the SET.
type syntheticPrefixRetryServer struct {
	ln           net.Listener
	conns        atomic.Int32
	setsReceived atomic.Int32
	readonlySets atomic.Int32
	setsApplied  atomic.Int32
	setDB        atomic.Int32 // DB in effect when SET last ran, -1 = never
}

func newSyntheticPrefixRetryServer(t *testing.T) *syntheticPrefixRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &syntheticPrefixRetryServer{ln: ln}
	s.setDB.Store(-1)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *syntheticPrefixRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	db := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			if len(args) > 0 {
				if n, err := strconv.Atoi(args[0]); err == nil {
					db = n
				}
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "incr":
			_, _ = conn.Write([]byte(":1\r\n"))
		case "set":
			s.setsReceived.Add(1)
			s.setDB.Store(int32(db))
			if first {
				// Drop before replying: the SET must be retried
				// with a synthetic SELECT ahead of it.
				return
			}
			if s.readonlySets.Load() == 0 {
				s.readonlySets.Add(1)
				_, _ = conn.Write([]byte("-READONLY You can't write against a read only replica.\r\n"))
				continue
			}
			s.setsApplied.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryClassifiesOriginalErrorAfterSyntheticPrefix: when a
// retry opens with synthetic commands that succeed, a retryable error on
// a later original command must still be classified for retry. Here the
// retried SET answers READONLY during failover behind a synthetic SELECT
// that succeeded: without the fix the attempt reports success, the outer
// loop exits with retries left, and Exec returns the READONLY too late
// to retry it. Regression test for the review finding on #4351.
func TestPipelineRetryClassifiesOriginalErrorAfterSyntheticPrefix(t *testing.T) {
	server := newSyntheticPrefixRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	incr := pipe.Incr(ctx, "counter")
	sel := pipe.Select(ctx, 2)
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the READONLY reply on the retried SET must be retried, not reported as success)", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (it must be retried after the failover, not left with the READONLY error)", err)
	}
	if got := server.setDB.Load(); got != 2 {
		t.Fatalf("SET ran on DB %d, want 2 (the retried SET must run on the re-established database)", got)
	}
	if got := server.readonlySets.Load(); got != 1 {
		t.Fatalf("server returned READONLY %d time(s), want 1", got)
	}
	if got := server.setsApplied.Load(); got != 1 {
		t.Fatalf("server applied %d SET commands, want 1 (the SET rejected READONLY must be applied by a later retry)", got)
	}
	if got := server.setsReceived.Load(); got != 3 {
		t.Fatalf("server received %d SET commands, want 3 (dropped once, READONLY once, applied once)", got)
	}
}

// syntheticPrefixTrimServer is a scripted RESP server for the
// synthetic-prefix trim test. The first connection answers SELECT and
// then drops before INCR's reply; the second answers the synthetic
// SELECT and the INCR, then rejects the SET with READONLY (failover in
// progress); later connections answer everything. It counts INCRs the
// server actually answered, so a retry that re-runs an answered INCR
// shows up as a second increment.
type syntheticPrefixTrimServer struct {
	ln            net.Listener
	conns         atomic.Int32
	incrsAnswered atomic.Int32
	setsReceived  atomic.Int32
	readonlySets  atomic.Int32
	setsApplied   atomic.Int32
	setDB         atomic.Int32 // DB in effect when SET last ran, -1 = never
}

func newSyntheticPrefixTrimServer(t *testing.T) *syntheticPrefixTrimServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &syntheticPrefixTrimServer{ln: ln}
	s.setDB.Store(-1)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *syntheticPrefixTrimServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	db := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			if len(args) > 0 {
				if v, err := strconv.Atoi(args[0]); err == nil {
					db = v
				}
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "incr":
			if n == 1 {
				// Drop before replying: the INCR must be
				// retried with a synthetic SELECT ahead of it.
				return
			}
			s.incrsAnswered.Add(1)
			_, _ = conn.Write([]byte(":1\r\n"))
		case "set":
			s.setsReceived.Add(1)
			s.setDB.Store(int32(db))
			if n == 2 && s.readonlySets.Load() == 0 {
				s.readonlySets.Add(1)
				_, _ = conn.Write([]byte("-READONLY You can't write against a read only replica.\r\n"))
				continue
			}
			s.setsApplied.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryTrimsAnsweredWritesAfterSyntheticPrefixError: when a
// retry opens with a synthetic SELECT that succeeds, a retryable error
// on a later original command must narrow the next retry to that
// command, not return the whole batch. Here the retried SET answers
// READONLY behind a synthetic SELECT: the whole-batch return keeps the
// already-answered INCR in pending, so the next healthy attempt
// increments the counter a second time and Exec still reports success.
// Regression test for the review finding on #4351.
func TestPipelineRetryTrimsAnsweredWritesAfterSyntheticPrefixError(t *testing.T) {
	server := newSyntheticPrefixTrimServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	incr := pipe.Incr(ctx, "counter")
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the READONLY reply on the retried SET must be retried, not reported as success)", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (it must be retried after the failover, not left with the READONLY error)", err)
	}
	if got := server.setDB.Load(); got != 2 {
		t.Fatalf("SET ran on DB %d, want 2 (the retried SET must run on the re-established database)", got)
	}
	if got := server.readonlySets.Load(); got != 1 {
		t.Fatalf("server returned READONLY %d time(s), want 1", got)
	}
	if got := server.incrsAnswered.Load(); got != 1 {
		t.Fatalf("server answered %d INCR commands, want 1 (the answered INCR must not be re-run by the retry after the READONLY SET)", got)
	}
	if got := server.setsApplied.Load(); got != 1 {
		t.Fatalf("server applied %d SET commands, want 1 (the SET rejected READONLY must be applied by a later retry)", got)
	}
}

// failedRestoreRetryServer is a scripted RESP server for the failed
// state-restoration test. The first connection answers SELECT and then
// drops before either SET's reply; the second connection rejects the
// synthetic SELECT (the DB does not exist there) and then answers the
// first SET in DB 0 before dropping before the second SET's reply; a
// third connection would accept everything. It records which DB each
// SET ran in, so a retry that keeps the DB-0 SET and only re-runs the
// second SET shows up as a success with the first SET missing from
// DB 2.
type failedRestoreRetryServer struct {
	ln           net.Listener
	conns        atomic.Int32
	setsA        atomic.Int32
	setsB        atomic.Int32
	setsAApplied atomic.Int32 // SET a commands answered +OK
	setsBApplied atomic.Int32 // SET b commands answered +OK
	setADB       atomic.Int32 // DB in effect when SET a last ran, -1 = never
	selectErrs   atomic.Int32
}

func newFailedRestoreRetryServer(t *testing.T) *failedRestoreRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &failedRestoreRetryServer{ln: ln}
	s.setADB.Store(-1)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *failedRestoreRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	db := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			if n == 2 {
				// The replacement server has no DB 2: the
				// required re-establishment fails, so the
				// SETs that follow run in DB 0.
				s.selectErrs.Add(1)
				_, _ = conn.Write([]byte("-ERR DB index is out of range\r\n"))
				continue
			}
			if len(args) > 0 {
				if v, err := strconv.Atoi(args[0]); err == nil {
					db = v
				}
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "set":
			if len(args) > 0 && args[0] == "a" {
				s.setsA.Add(1)
				s.setADB.Store(int32(db))
			} else {
				s.setsB.Add(1)
			}
			if n == 1 || (n == 2 && len(args) > 0 && args[0] == "b") {
				// Drop before replying: the SET must be retried.
				return
			}
			if len(args) > 0 && args[0] == "a" {
				s.setsAApplied.Add(1)
			} else {
				s.setsBApplied.Add(1)
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryKeepsFailedStateRestoration: when a retry's required
// synthetic SELECT fails, the writes that followed it ran under the
// wrong session state and must not be retained as the batch's outcome.
// Here the second connection rejects SELECT 2, SET a answers OK in
// DB 0, and the connection drops before SET b's reply: without the fix
// the transport return bypasses required-synthetic validation, the
// narrowing drops SET a and reissues a cleared SELECT, and a third
// connection runs only SET b in DB 2 while Exec reports success even
// though SET a never landed in DB 2. Regression test for the review
// finding on #4351.
func TestPipelineRetryKeepsFailedStateRestoration(t *testing.T) {
	server := newFailedRestoreRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	setA := pipe.Set(ctx, "a", "v", 0)
	setB := pipe.Set(ctx, "b", "v", 0)
	if _, err := pipe.Exec(ctx); err == nil {
		t.Fatalf("Exec: nil, want an error (the failed SELECT 2 restoration must not be erased by a retry that only re-runs SET b)")
	}
	// The original SELECT was answered OK on the first connection; the
	// failure belongs to the synthetic reissue, so it surfaces as the
	// Exec error, not on the original command.
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered OK on the first connection; the failed reissue is reported on Exec)", err)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (no third attempt may run after the restoration failure)", got)
	}
	if got := server.selectErrs.Load(); got != 1 {
		t.Fatalf("server rejected SELECT %d time(s), want 1", got)
	}
	if got := server.setsA.Load(); got != 2 {
		t.Fatalf("server received %d SET a commands, want 2 (dropped once on the first connection, answered once in the wrong DB)", got)
	}
	if got := server.setsAApplied.Load(); got != 1 {
		t.Fatalf("server applied %d SET a commands, want 1 (only the DB-0 run was answered; it must not be re-run in DB 2 as a silent success)", got)
	}
	if got := server.setADB.Load(); got != 0 {
		t.Fatalf("SET a ran on DB %d, want 0 (it only ever ran under the failed SELECT, never in DB 2)", got)
	}
	if got := server.setsB.Load(); got != 1 {
		t.Fatalf("server received %d SET b commands, want 1 (it must not be re-run on a third connection as if the batch had succeeded)", got)
	}
	if got := server.setsBApplied.Load(); got != 0 {
		t.Fatalf("server applied %d SET b commands, want 0 (its reply was never read)", got)
	}
	if err := setA.Err(); err != nil {
		t.Fatalf("SET a: %v, want nil (it was answered OK in DB 0; the failure is reported on Exec, not by rewriting its reply)", err)
	}
	if err := setB.Err(); err == nil {
		t.Fatalf("SET b: nil, want an error (its reply was never read)")
	}
}

func TestPipelineRetryDoesNotReplayAnsweredCommands(t *testing.T) {
	server := newPartialRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	incr := pipe.Incr(ctx, "counter")
	set := pipe.Set(ctx, "other", "v", 0)
	get := pipe.Get(ctx, "counter")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "1" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "1")
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be replayed)", got)
	}
}

// waitRetryServer is a scripted RESP server for WAIT pipeline retry tests.
// The first connection answers SET and then drops before WAIT's reply;
// later connections answer everything. It counts WAITs that arrive on a
// connection which never ran a SET, i.e. a barrier retried without the
// write whose replication it is supposed to confirm.
type waitRetryServer struct {
	ln             net.Listener
	conns          atomic.Int32
	sets           atomic.Int32
	waits          atomic.Int32
	waitWithoutSet atomic.Int32
}

func newWaitRetryServer(t *testing.T) *waitRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &waitRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *waitRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	seenSet := false
	answered := false
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
			continue
		case "set":
			s.sets.Add(1)
			seenSet = true
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "wait":
			s.waits.Add(1)
			if !seenSet {
				s.waitWithoutSet.Add(1)
			}
			if first && answered {
				// Drop the connection before replying: the WAIT
				// must be retried together with its SET.
				return
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
		answered = true
	}
}

// TestPipelineRetryKeepsWaitWithWrite: WAIT reads the replication offset of
// the connection that runs it (the server's per-connection woff), so a WAIT
// retried alone on a fresh connection would report success at once while
// the answered SET is still unreplicated. Regression test for the review
// finding on #4351 (SET + WAIT trimmed to WAIT-only on retry).
func TestPipelineRetryKeepsWaitWithWrite(t *testing.T) {
	server := newWaitRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	// Pipeliner has no Wait helper; issue the barrier as a raw command.
	wait := pipe.Do(ctx, "wait", 1, 1000)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := wait.Result(); err != nil || got != int64(1) {
		t.Fatalf("WAIT = %v, %v; want 1, nil", got, err)
	}
	if got := server.waitWithoutSet.Load(); got != 0 {
		t.Fatalf("WAIT ran %d time(s) on a connection that never ran the SET it follows", got)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the answered SET must be re-run with its WAIT)", got)
	}
}

// himportSetRecord records one HIMPORT SET received by the scripted server:
// the connection number and the fields of the last HIMPORT PREPARE for that
// fieldset on the same connection.
type himportSetRecord struct {
	conn   int32
	fields []string
}

// himportRetryServer is a scripted RESP server for HIMPORT pipeline retry
// tests. The first connection answers the user-issued HIMPORT PREPARE and
// then drops before the SET's reply; later connections answer everything.
type himportRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	mu    sync.Mutex
	sets  []himportSetRecord
}

func newHImportRetryServer(t *testing.T) *himportRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &himportRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *himportRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	prepared := map[string][]string{}
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "himport":
			// readConnStateRetryCommand strips the command name, so
			// args[0] is the subcommand (prepare/set), args[1] the
			// fieldset for PREPARE and the key for SET.
			if len(args) < 1 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				continue
			}
			switch strings.ToLower(args[0]) {
			case "prepare":
				if len(args) >= 2 {
					prepared[args[1]] = append([]string(nil), args[2:]...)
				}
				_, _ = conn.Write([]byte("+OK\r\n"))
			case "set":
				var fields []string
				if len(args) >= 3 {
					fields = append([]string(nil), prepared[args[2]]...)
				}
				s.mu.Lock()
				s.sets = append(s.sets, himportSetRecord{conn: n, fields: fields})
				s.mu.Unlock()
				if n == 1 {
					// Drop before replying: the SET must be retried
					// together with its PREPARE.
					return
				}
				_, _ = conn.Write([]byte("+OK\r\n"))
			default:
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryKeepsHImportPrepare: a pipeline HIMPORT PREPARE followed
// by HIMPORT SET must not be trimmed to the SET on retry. The client-side
// registry still holds the old fields (himportAfterBatch never ran on the
// dropped connection), so a trimmed retry would inject a PREPARE with the
// stale fields and write under the wrong field names. Regression test for
// the review finding on #4351.
func TestPipelineRetryKeepsHImportPrepare(t *testing.T) {
	server := newHImportRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	// Seed the client-side registry as if fs had been registered with
	// field "old" by an earlier HImportPrepare.
	client.himport.register("fs", []string{"old"})

	pipe := client.Pipeline()
	prep := pipe.HImportPrepare(ctx, "fs", "new")
	set := pipe.HImportSet(ctx, "k", "fs", "v")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := prep.Err(); err != nil {
		t.Fatalf("PREPARE: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	var retry []himportSetRecord
	for _, rec := range server.sets {
		if rec.conn == 2 {
			retry = append(retry, rec)
		}
	}
	if len(retry) != 1 {
		t.Fatalf("server received %d SET commands on the retry connection, want 1", len(retry))
	}
	if got := retry[0].fields; len(got) != 1 || got[0] != "new" {
		t.Fatalf("SET on the retry connection ran under PREPARE fields %v, want [new] (the answered PREPARE must not be trimmed)", got)
	}
}

// staleErrRetryServer is a scripted RESP server for stale-error pipeline
// retry tests. The first connection answers the first batch (SET with a
// LOADING error, GET with a value) and then stops reading forever, so the
// retry's read stalls until the read timeout; later connections answer
// everything.
type staleErrRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	sets  atomic.Int32
	done  chan struct{}
}

func newStaleErrRetryServer(t *testing.T) *staleErrRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &staleErrRetryServer{ln: ln, done: make(chan struct{})}
	t.Cleanup(func() { close(s.done) })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

// readStaleErrRESPCommand reads one RESP array command from rd and returns its
// lower-cased name. Unlike the bufio.Scanner helpers above it reads bulk
// strings by their declared length, so a multi-megabyte argument does not
// depend on scanner token buffering.
func readStaleErrRESPCommand(rd *bufio.Reader) (string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return "", io.EOF
	}
	n, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
	if err != nil || n <= 0 {
		return "", io.EOF
	}
	var args []string
	for i := 0; i < n; i++ {
		hline, err := rd.ReadString('\n')
		if err != nil {
			return "", err
		}
		hline = strings.TrimRight(hline, "\r\n")
		if !strings.HasPrefix(hline, "$") {
			return "", io.EOF
		}
		size, err := strconv.Atoi(strings.TrimPrefix(hline, "$"))
		if err != nil || size < 0 {
			return "", io.EOF
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return "", err
		}
		args = append(args, string(buf[:size]))
	}
	if len(args) == 0 {
		return "", io.EOF
	}
	return strings.ToLower(args[0]), nil
}

func (s *staleErrRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	answered := 0
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
			continue
		case "set":
			s.sets.Add(1)
			if first {
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "get":
			_, _ = conn.Write([]byte("$1\r\nv\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
		if first {
			answered++
			if answered >= 2 {
				// Stop reading: the retry's read must stall until
				// the read timeout instead of being answered by
				// this connection.
				<-s.done
				return
			}
		}
	}
}

// TestPipelineRetryDoesNotReportStaleError: when the first attempt reads a
// retryable LOADING error for SET and a successful GET, and the next
// attempt fails during its read, the retry must re-execute SET — not drop
// it as though the earlier LOADING had been this attempt's answer and
// report the stale error after the server has recovered. (LOADING, not
// READONLY, seeds the stale error: a READONLY batch error makes the pool
// drop the connection outright, see #790, so the read failure this test
// needs would never happen.) The stalled retry read is made deterministic
// with a small payload and a short ReadTimeout: the retry's write lands in
// the kernel buffers of a connection that stopped reading, and its read
// then stalls until the timeout — instead of a 32 MiB payload racing a
// 100ms WriteTimeout, whose outcome depends on runner speed (CI 8.4.x /
// 8.6.x). Regression test for the review finding on #4351.
func TestPipelineRetryDoesNotReportStaleError(t *testing.T) {
	server := newStaleErrRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())
	client.opt.ReadTimeout = 100 * time.Millisecond

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "x", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v (a LOADING reply from the first attempt must not survive a retry that never re-executed it)", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the SET must be re-executed once the server has recovered)", got)
	}
}

// firstErrRetryServer is a scripted RESP server for first-failure
// aggregation tests. The first connection answers HGET with a WRONGTYPE
// error and then drops before INCR's reply; later connections answer INCR
// with a Redis error (retryErr=true) or a value.
type firstErrRetryServer struct {
	ln       net.Listener
	conns    atomic.Int32
	hgets    atomic.Int32
	incrs    atomic.Int32
	retryErr bool
}

func newFirstErrRetryServer(t *testing.T, retryErr bool) *firstErrRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &firstErrRetryServer{ln: ln, retryErr: retryErr}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *firstErrRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "hget":
			s.hgets.Add(1)
			if n == 1 {
				_, _ = conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
			} else {
				_, _ = conn.Write([]byte("$1\r\nv\r\n"))
			}
		case "incr":
			s.incrs.Add(1)
			if n == 1 {
				// Drop before replying: INCR must be retried on its own.
				return
			}
			if s.retryErr {
				_, _ = conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
			} else {
				_, _ = conn.Write([]byte(":1\r\n"))
			}
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryReportsFirstErr: when the first command of a pipeline is
// answered with a Redis error and a later command is retried on its own and
// fails with a different Redis error, Exec must report the first failed
// command's error, per its documented contract — not the retried tail's
// error. Regression test for the review finding on #4351.
func TestPipelineRetryReportsFirstErr(t *testing.T) {
	server := newFirstErrRetryServer(t, true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	hget := pipe.HGet(ctx, "str", "f")
	incr := pipe.Incr(ctx, "text")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("Exec: %v, want the first failed command's WRONGTYPE error, not the retried INCR's error", err)
	}
	if err := hget.Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("HGET: %v, want WRONGTYPE", err)
	}
	if err := incr.Err(); err == nil || !strings.Contains(err.Error(), "ERR") {
		t.Fatalf("INCR: %v, want the retry's ERR", err)
	}
	if got := server.hgets.Load(); got != 1 {
		t.Fatalf("server received %d HGET commands, want 1 (the answered HGET must not be re-executed)", got)
	}
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the unanswered INCR retried once)", got)
	}
}

// TestPipelineRetryKeepsAnsweredResultsOnNonRetryableExit: when one attempt
// answers the first command and the connection then drops before the next
// reply, a non-retryable failure must not stamp the terminal error onto the
// command that already succeeded. The batch holds a NoRetry command (as a
// RawWriteToCmd would), so the early-exit branch runs instead of a retry:
// stamping the whole pending set would overwrite the answered SET's success
// with the transport error, so callers would observe failure for a command
// that already applied. Regression test for the review finding on #4351.
func TestPipelineRetryKeepsAnsweredResultsOnNonRetryableExit(t *testing.T) {
	server := newPartialRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "text")
	incr.setNoRetry(true) // stands in for a RawWriteToCmd in the batch
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the transport error for the unanswered INCR")
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (the answered SET must keep its success, not inherit the terminal error)", err)
	}
	if got, err := set.Result(); err != nil || got != "OK" {
		t.Fatalf("SET = %q, %v; want OK, nil", got, err)
	}
	if err := incr.Err(); err == nil {
		t.Fatalf("INCR: nil, want the transport error for the unanswered command")
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (no retry on a non-retryable exit)", got)
	}
}

// firstErrMetricRecorder captures the error passed to
// RecordPipelineOperationDuration.
type firstErrMetricRecorder struct {
	fdOtelRecorder
	calls   atomic.Int64
	lastErr atomic.Value
}

func (r *firstErrMetricRecorder) RecordPipelineOperationDuration(_ context.Context, _ time.Duration, _ string, _ int, _ int, err error, _ *pool.Conn, _ int) {
	r.calls.Add(1)
	if err != nil {
		r.lastErr.Store(err)
	}
}

// TestPipelineRetryRecordsFirstErrInMetrics: when a pipeline's first command
// keeps a Redis error and the retried tail then succeeds, Exec still
// reports the first error — but the pipeline duration metric must also
// record it as a failure, not as a success. Regression test for the
// review finding on #4351.
func TestPipelineRetryRecordsFirstErrInMetrics(t *testing.T) {
	server := newFirstErrRetryServer(t, false)
	client := newPartialRetryClient(t, server.ln.Addr().String())
	rec := &firstErrMetricRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	pipe := client.Pipeline()
	hget := pipe.HGet(ctx, "str", "f")
	incr := pipe.Incr(ctx, "text")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("Exec: %v, want WRONGTYPE (the first failed command)", err)
	}
	if err := hget.Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("HGET: %v, want WRONGTYPE", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %v, %v; want 1, nil", got, err)
	}
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("%d pipeline metrics for one operation, want 1", n)
	}
	e, _ := rec.lastErr.Load().(error)
	if e == nil || !strings.Contains(e.Error(), "WRONGTYPE") {
		t.Fatalf("metric error %v, want WRONGTYPE (a retained prefix error must not record as success)", e)
	}
}

// loadingRetryServer is a scripted RESP server for retryable-reply pipeline
// retry tests. The first connection answers SET with a LOADING error and then
// drops before INCR's reply; later connections answer everything.
type loadingRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	sets  atomic.Int32
}

func newLoadingRetryServer(t *testing.T) *loadingRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &loadingRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *loadingRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			s.sets.Add(1)
			if first {
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "incr":
			if first {
				// Drop before replying: the INCR must be retried
				// together with the SET the LOADING reply never executed.
				return
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryReexecutesLoadingCommand: when one command of a pipeline is
// answered with a retryable LOADING error and a later command then fails at
// the transport layer, the retry must re-execute the LOADING command — it
// never applied — not drop it as though the LOADING reply had been a
// definitive answer and keep reporting the stale error after the server has
// recovered. Regression test for the review finding on #4351.
func TestPipelineRetryReexecutesLoadingCommand(t *testing.T) {
	server := newLoadingRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the LOADING reply never executed the SET, so the retry must re-execute it)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %v, %v; want 1, nil", got, err)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the SET answered LOADING must be re-executed once the server has recovered)", got)
	}
}

// scriptRetryServer is a scripted RESP server for SCRIPT LOAD pipeline retry
// tests. Each connection holds its own (initially empty) script cache, like a
// fresh server instance after a restart: SCRIPT LOAD records the digest,
// EVALSHA answers only when the digest was loaded on that same connection.
// The first connection answers the LOAD and then drops before EVALSHA's reply;
// later connections answer everything.
//
// dropBefore names the command before whose reply the first connection
// drops ("evalsha" by default); denyLoad answers SCRIPT LOAD with NOPERM,
// modelling an ACL user without that permission; dropRetryConn closes a
// retry connection once its last command arrived, without replying to any
// of it, so the retry fails while re-reading its first command.
type scriptRetryServer struct {
	ln            net.Listener
	conns         atomic.Int32
	loads         atomic.Int32
	flushes       atomic.Int32
	evals         atomic.Int32
	evalshas      atomic.Int32
	noscripts     atomic.Int32
	dropBefore    string
	denyLoad      bool
	dropRetryConn bool
	// loadUser, when set, is the only user allowed to SCRIPT LOAD;
	// other users get NOPERM. Tests that need a LOAD to run under a
	// specific AUTH use it.
	loadUser string
	// persistCache makes the script cache survive across connections
	// (a still-running server), instead of a fresh cache per
	// connection (a restarted server).
	persistCache bool
	cacheMu      sync.Mutex
	sharedLoaded map[string]bool
}

func (s *scriptRetryServer) cacheHas(sha string) bool {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	return s.sharedLoaded[sha]
}

func (s *scriptRetryServer) cachePut(sha string) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if s.sharedLoaded == nil {
		s.sharedLoaded = map[string]bool{}
	}
	s.sharedLoaded[sha] = true
}

func (s *scriptRetryServer) cacheClear() {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.sharedLoaded = map[string]bool{}
}

func newScriptRetryServer(t *testing.T) *scriptRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &scriptRetryServer{ln: ln, dropBefore: "evalsha"}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *scriptRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	loaded := map[string]bool{}
	user := "default"
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		if first && s.dropBefore != "" && name == s.dropBefore {
			// Drop before replying: the command must be retried.
			return
		}
		if !first && s.dropRetryConn {
			// The connection handshake still needs its HELLO reply;
			// only the pipeline batch goes unanswered.
			if name == "hello" {
				_, _ = conn.Write([]byte("%0\r\n"))
				continue
			}
			// Read the retry batch, then close without replying to
			// any of it, so the retry fails while re-reading its
			// first command.
			if name == "script" && len(args) >= 2 && strings.EqualFold(args[0], "load") {
				s.loads.Add(1)
			}
			if name == "evalsha" {
				return
			}
			continue
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "auth":
			if len(args) >= 1 {
				user = args[0]
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "script":
			if len(args) >= 2 && strings.EqualFold(args[0], "load") {
				s.loads.Add(1)
				if s.denyLoad {
					_, _ = conn.Write([]byte("-NOPERM this user has no permissions to run the 'script|load' command\r\n"))
					continue
				}
				if s.loadUser != "" && user != s.loadUser {
					_, _ = conn.Write([]byte("-NOPERM this user has no permissions to run the 'script|load' command\r\n"))
					continue
				}
				sum := sha1.Sum([]byte(args[1]))
				digest := hex.EncodeToString(sum[:])
				if s.persistCache {
					s.cachePut(digest)
				} else {
					loaded[digest] = true
				}
				_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(digest), digest)
			} else if len(args) >= 1 && strings.EqualFold(args[0], "flush") {
				s.flushes.Add(1)
				if s.persistCache {
					s.cacheClear()
				} else {
					loaded = map[string]bool{}
				}
				_, _ = conn.Write([]byte("+OK\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "eval", "eval_ro":
			s.evals.Add(1)
			// A rejected EVAL never runs, so it populates nothing: a
			// negative key count is refused before the script is
			// cached. Only a successful EVAL counts as a cache entry.
			// A CROSSSLOT rejection is the same kind of pre-execution
			// refusal: the script never ran, so it cached nothing.
			rejected := false
			if len(args) >= 2 {
				if n, err := strconv.Atoi(args[1]); err == nil && n < 0 {
					rejected = true
				}
			}
			if rejected {
				_, _ = conn.Write([]byte("-ERR wrong number of keys for 'eval' command\r\n"))
				continue
			}
			if len(args) >= 1 && args[0] == "crossslot" {
				_, _ = conn.Write([]byte("-CROSSSLOT Keys in request don't hash to the same slot\r\n"))
				continue
			}
			if len(args) >= 1 {
				sum := sha1.Sum([]byte(args[0]))
				digest := hex.EncodeToString(sum[:])
				if s.persistCache {
					s.cachePut(digest)
				} else {
					loaded[digest] = true
				}
			}
			// A script that ran but returned nil, or failed at runtime,
			// still populated the cache before producing its reply.
			switch {
			case len(args) >= 1 && args[0] == "if ARGV[1] then return 42 end":
				_, _ = conn.Write([]byte("$-1\r\n"))
			case len(args) >= 1 && args[0] == "return redis.call('BADCMD')":
				_, _ = conn.Write([]byte("-ERR Error running script (call to f_0123456789abcdef): @user_script:1: Unknown Redis command called from Lua script\r\n"))
			default:
				_, _ = conn.Write([]byte(":1\r\n"))
			}
		case "evalsha":
			s.evalshas.Add(1)
			cached := false
			if len(args) >= 1 {
				if s.persistCache {
					cached = s.cacheHas(args[0])
				} else {
					cached = loaded[args[0]]
				}
			}
			if cached {
				_, _ = conn.Write([]byte(":1\r\n"))
			} else {
				s.noscripts.Add(1)
				_, _ = conn.Write([]byte("-NOSCRIPT No matching script. Please use EVAL.\r\n"))
			}
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryReissuesScriptLoad: a pipeline SCRIPT LOAD followed by
// EVALSHA must not be trimmed to the EVALSHA on retry. The LOAD populates
// the server's volatile script cache, which a restarted server does not
// have, so the retried EVALSHA would fail NOSCRIPT without it. Regression
// test for the review finding on #4351.
func TestPipelineRetryReissuesScriptLoad(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	load := pipe.ScriptLoad(ctx, src)
	eval := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must reissue the SCRIPT LOAD before the EVALSHA)", err)
	}
	if got := load.Val(); got != sha {
		t.Fatalf("SCRIPT LOAD = %q, want %q", got, sha)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil", got, err)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must be reissued on the retry connection)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryReissuesScriptFlush: a pipeline SCRIPT LOAD, SCRIPT FLUSH,
// EVALSHA must not be rebuilt as LOAD, EVALSHA on retry. The flush
// invalidated the script the LOAD populated, so a retry that drops it
// executes a script the flush had removed — the previous whole-batch retry
// repeated the flush and returned NOSCRIPT instead. Regression test for the
// review finding on #4351.
func TestPipelineRetryReissuesScriptFlush(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	pipe.ScriptLoad(ctx, src)
	pipe.ScriptFlush(ctx)
	eval := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the retry must replay the FLUSH after the LOAD, so the flushed script stays gone)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT", err)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must be reissued on the retry connection)", got)
	}
	if got := server.flushes.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT FLUSH commands, want 2 (the answered FLUSH must be reissued after the LOAD)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryBackoffCancelKeepsAnsweredResults: cancelling the context
// while the retry backoff sleeps must not overwrite commands a prior attempt
// already answered. The first attempt answers SELECT (a connection-local
// state command, so the retry is whole-batch) and then drops before SET's
// reply; the cancel lands in the backoff before the second attempt. The
// SELECT keeps its +OK, while the unanswered SET and INCR take the cancel
// error. Regression test for the review finding on #4351.
func TestPipelineRetryBackoffCancelKeepsAnsweredResults(t *testing.T) {
	server := newConnStateRetryServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: 5 * time.Second,
		MaxRetryBackoff: 5 * time.Second,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 1)
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "counter")
	errCh := make(chan error, 1)
	go func() {
		_, err := pipe.Exec(ctx)
		errCh <- err
	}()
	// The first attempt fails in milliseconds; the backoff lasts 5s, so
	// cancelling now lands inside the backoff sleep.
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Exec err=%v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Exec did not return after the context was cancelled")
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered before the drop and must keep its result)", err)
	}
	if err := set.Err(); err == nil {
		t.Fatal("SET: nil, want the attempt's transport error (it was never answered)")
	}
	// INCR keeps the read path's transport error: it was never answered
	// either, and setCmdsErr only stamps commands with a nil error.
	if err := incr.Err(); err == nil {
		t.Fatal("INCR: nil, want the attempt's transport error (it was never answered)")
	}
}

// TestPipelineRetryReportsTransportErrOverFirstRedisErr: when the first
// command of a pipeline is answered with a Redis error and a later command
// fails with a transport error on the final attempt, Exec must report the
// transport error, not the first command's Redis error. The transport error
// means the batch never completed and the connection may hold an unread
// reply: hiding it behind the earlier Redis error lets callers that branch
// on the error kind — notably initConn's identity setup, which deliberately
// tolerates Redis errors — accept a desynchronized connection. Regression
// test for the review finding on #4351.
func TestPipelineRetryReportsTransportErrOverFirstRedisErr(t *testing.T) {
	server := newFirstErrRetryServer(t, true)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1, // a single attempt: the transport error is final
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	pipe := client.Pipeline()
	hget := pipe.HGet(ctx, "str", "f")
	incr := pipe.Incr(ctx, "text")
	_, err := pipe.Exec(ctx)
	if err == nil || isRedisError(err) {
		t.Fatalf("Exec: %v, want the transport error for the unanswered INCR, not the first command's WRONGTYPE", err)
	}
	if err := hget.Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("HGET: %v, want WRONGTYPE (its own reply is preserved)", err)
	}
	if err := incr.Err(); err == nil || isRedisError(err) {
		t.Fatalf("INCR: %v, want the transport error (it was never answered)", err)
	}
}

// dialFailRetryServer is a scripted RESP server for retry-acquisition tests.
// The first connection answers SELECT and then drops mid-pipeline; the
// listener is closed as soon as that connection drops, so every later dial
// fails with connection refused.
type dialFailRetryServer struct {
	ln       net.Listener
	conns    atomic.Int32
	closeLn  sync.Once
	dials    atomic.Int32 // connections accepted after the first
	answered atomic.Int32 // SELECT replies sent
}

func newDialFailRetryServer(t *testing.T) *dialFailRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &dialFailRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			if n > 1 {
				s.dials.Add(1)
			}
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *dialFailRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			s.answered.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			if first {
				// Drop mid-pipeline and stop listening: the retry's
				// dial must fail, not reach a fresh server.
				s.closeLn.Do(func() { _ = s.ln.Close() })
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryDialFailureKeepsAnsweredResults: after a whole-batch retry
// is narrowed (the first attempt answered SELECT, then dropped before SET),
// the retry's dial failure must not stamp the dial error onto SELECT. The
// pre-attempt error clear used to wipe the tail transport errors that
// pipelineUnansweredSuffix uses as the answered/unanswered boundary, so a
// dial failure after the clear stamped the whole batch — including the
// command a prior attempt had already answered. The backoff-cancel test only
// covers a cancel during the sleep that runs before that clear, not a
// failure after it. Regression test for the review finding on #4351.
func TestPipelineRetryDialFailureKeepsAnsweredResults(t *testing.T) {
	server := newDialFailRetryServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 1)
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the dial error (the listener is closed after the first drop)")
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered before the drop and must keep its result)", err)
	}
	if err := set.Err(); err == nil {
		t.Fatal("SET: nil, want the attempt's transport error (it was never answered)")
	}
	if err := incr.Err(); err == nil {
		t.Fatal("INCR: nil, want the attempt's transport error (it was never answered)")
	}
}

// identityTransportServer is a scripted RESP server for initConn identity
// setup tests. It answers HELLO, rejects the first CLIENT SETINFO with a
// Redis error (as a server older than 7.2 would), and withholds the second
// SETINFO reply until it reads the next command — only then does it send
// the late +OK. A client that accepts the connection anyway reads that
// late reply as its next command's reply.
type identityTransportServer struct {
	ln    net.Listener
	conns atomic.Int32
}

func newIdentityTransportServer(t *testing.T) *identityTransportServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &identityTransportServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.conns.Add(1)
			go s.serve(conn)
		}
	}()
	return s
}

func (s *identityTransportServer) serve(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	setinfo := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "client":
			// readConnStateRetryCommand strips the command name, so
			// args[0] is "setinfo" and args[1] is LIB-NAME / LIB-VER.
			if len(args) >= 2 && strings.EqualFold(args[0], "setinfo") {
				setinfo++
				if setinfo == 1 {
					_, _ = conn.Write([]byte("-ERR unknown command 'client', with args beginning with: 'setinfo'\r\n"))
				}
				// The second SETINFO reply is withheld: the read
				// times out on the client, and the late reply is
				// only sent after the next command arrives.
				continue
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "ping":
			// Send the withheld SETINFO reply first: an accepted
			// connection consumes it as PING's reply.
			if setinfo >= 2 {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
			_, _ = conn.Write([]byte("+PONG\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestInitConnRejectsIdentityPipelineTransportFailure: with MaxRetries -1 and
// identity setup enabled, a CLIENT SETINFO Redis error on the first setup
// command must not hide a transport timeout on the second: initConn
// deliberately ignores Redis errors, so if Exec reported only the Redis
// error the connection would be accepted with a pending SETINFO reply in
// its stream, and the next user command would consume that reply instead
// of its own. Regression test for the review finding on #4351.
func TestInitConnRejectsIdentityPipelineTransportFailure(t *testing.T) {
	server := newIdentityTransportServer(t)
	client := NewClient(&Options{
		Addr:         server.ln.Addr().String(),
		Protocol:     2,
		PoolSize:     1,
		MaxRetries:   -1, // a single attempt: the timeout is final
		DialTimeout:  5 * time.Second,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	val, err := client.Ping(ctx).Result()
	if err == nil {
		t.Fatalf("Ping = %q, nil; want a transport error (the identity pipeline's SETINFO reply timed out, so init must reject the connection)", val)
	}
	if isRedisError(err) {
		t.Fatalf("Ping err = %v, want a transport error, not a Redis error", err)
	}
}

// dialFailLoadingServer is a scripted RESP server for rejected-command
// retry tests. The first connection answers SET with a retryable LOADING
// error and then drops before INCR's reply, and closes the listener, so
// every later dial fails instead of reaching a fresh server.
type dialFailLoadingServer struct {
	ln      net.Listener
	conns   atomic.Int32
	closeLn sync.Once
}

func newDialFailLoadingServer(t *testing.T) *dialFailLoadingServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &dialFailLoadingServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *dialFailLoadingServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			if first {
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "incr":
			if first {
				// Drop before replying and stop listening: the
				// retry's dial must fail, not reach a fresh server.
				s.closeLn.Do(func() { _ = s.ln.Close() })
				return
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryDialFailureKeepsRejectedCommandError: when SET is answered
// with a retryable LOADING error (the server rejected it without executing
// it) and INCR's reply is then lost, a later dial failure must not make SET
// look successful: the SET was never executed, so its error must survive
// until a retry can dispatch it again. Regression test for the review
// finding on #4351.
func TestPipelineRetryDialFailureKeepsRejectedCommandError(t *testing.T) {
	server := newDialFailLoadingServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the dial error (the listener is closed after the first drop)")
	}
	if err := set.Err(); err == nil {
		t.Fatal("SET: nil, want the LOADING rejection (it was never executed, so it must not look successful)")
	}
	if err := incr.Err(); err == nil {
		t.Fatal("INCR: nil, want the attempt's transport error (it was never answered)")
	}
}

// relistenLoadingServer is a scripted RESP server for the cleared-reply
// retry test. The first connection answers SET with LOADING and drops
// before INCR's reply; the listener then closes for reopenDelay and
// re-opens on the same address. reopenDelay is chosen so that one retry
// attempt's whole dial sequence is exhausted while the listener is down
// (the pool retries a failed dial up to 5 times, 100ms apart, inside a
// single getConn) and a later attempt's dial sequence reaches the fresh
// server that answers everything.
type relistenLoadingServer struct {
	ln          net.Listener
	addr        string
	conns       atomic.Int32
	sets        atomic.Int32
	closeLn     sync.Once
	reopenOnce  sync.Once
	reopenDelay time.Duration
	reopenErr   atomic.Value // error, if re-listening failed
}

func newRelistenLoadingServer(t *testing.T, reopenDelay time.Duration) *relistenLoadingServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &relistenLoadingServer{ln: ln, addr: ln.Addr().String(), reopenDelay: reopenDelay}
	t.Cleanup(func() { _ = ln.Close() })
	go s.acceptLoop(ln)
	return s
}

func (s *relistenLoadingServer) acceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		n := s.conns.Add(1)
		go s.serve(conn, n == 1)
	}
}

func (s *relistenLoadingServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			s.sets.Add(1)
			if first {
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "incr":
			if first {
				// Drop before replying, close the listener, and
				// re-open it after reopenDelay: the next dial
				// fails, the one after it reaches a fresh server.
				s.closeLn.Do(func() { _ = s.ln.Close() })
				s.reopenOnce.Do(func() {
					go func() {
						time.Sleep(s.reopenDelay)
						ln, err := net.Listen("tcp", s.addr)
						if err != nil {
							s.reopenErr.Store(err)
							return
						}
						s.ln = ln
						go s.acceptLoop(ln)
					}()
				})
				return
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryClearedLoadingCommandIsNotDropped: when SET is answered
// with a retryable LOADING error and INCR's reply is then lost, a later
// dial failure must not drop SET from the retry: the LOADING reply never
// executed it, so a succeeding retry must re-execute it — Exec must not
// report success for a write that never applied. Regression test for the
// review finding on #4351.
func TestPipelineRetryClearedLoadingCommandIsNotDropped(t *testing.T) {
	// Timeline (client backoff fixed at 50ms): attempt 0 answers SET
	// with LOADING and loses INCR's reply. Attempt 1 dispatches ~50ms
	// later; one dispatch runs TWO dial sequences back to back — the
	// pipeline pool's TryGet dials (5 dials, 100ms apart, ~400ms) and,
	// on failure, spills to the main pool which dials again (~400ms) —
	// so the retry loop only sees a dial failure at ~850ms. Attempt 2
	// dispatches ~900ms in; its dial sequence reaches the reopened
	// listener (~1250ms, the ~1300ms dial) and dispatches the pending
	// commands. PoolSize 2 keeps the pool's dial breaker (which trips
	// when dialErrorsNum reaches PoolSize) from short-circuiting
	// attempt 2's dial sequence after attempt 1's sequences were
	// exhausted.
	server := newRelistenLoadingServer(t, 1250*time.Millisecond)
	client := NewClient(&Options{
		Addr:     server.addr,
		Protocol: 2,
		// A custom Dialer makes this a non-fixed-endpoint client
		// (fixedDialEndpoint is opt.Dialer == nil): only then does the
		// retry loop keep going after a dial failure. With the default
		// dialer, an exhausted dial sequence sets the dialExhausted
		// marker and the loop exits at once, so no later attempt could
		// ever succeed — the dropped write would surface only as a
		// dial error, never as a silent success.
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
		},
		PoolSize:        2,
		MaxRetries:      3,
		MinRetryBackoff: 50 * time.Millisecond,
		MaxRetryBackoff: 50 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry after the failed dial must succeed)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (it must have been re-executed once the server recovered)", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %v, %v; want 1, nil", got, err)
	}
	if err, ok := server.reopenErr.Load().(error); ok && err != nil {
		t.Fatalf("re-listening failed: %v", err)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the SET answered LOADING must be re-executed after the failed dial, not dropped as though it had been answered)", got)
	}
}

// TestScriptCmdRawArgs: the SCRIPT LOAD / FLUSH predicates must match the
// subcommand however the caller spelled it. Writer.WriteArg and
// baseCmd.stringArg both accept string, *string and []byte, so a raw command
// built with Do(ctx, "SCRIPT", []byte("FLUSH")) is a real SCRIPT FLUSH on
// the wire — the predicates must not only recognise the string form.
// Regression test for the review finding on #4351.
func TestScriptCmdRawArgs(t *testing.T) {
	ctx := context.Background()
	flush := "FLUSH"
	load := "LOAD"
	cases := []struct {
		name            string
		args            []interface{}
		isLoad, isFlush bool
	}{
		{"string LOAD", []interface{}{"SCRIPT", "LOAD", "return 1"}, true, false},
		{"string FLUSH", []interface{}{"SCRIPT", "FLUSH"}, false, true},
		{"bytes LOAD", []interface{}{"SCRIPT", []byte("LOAD"), "return 1"}, true, false},
		{"bytes FLUSH", []interface{}{"SCRIPT", []byte("FLUSH")}, false, true},
		{"stringptr LOAD", []interface{}{"SCRIPT", &load, "return 1"}, true, false},
		{"stringptr FLUSH", []interface{}{"SCRIPT", &flush}, false, true},
		{"lowercase bytes flush", []interface{}{"SCRIPT", []byte("flush")}, false, true},
		{"string EXISTS", []interface{}{"SCRIPT", "EXISTS", "abc"}, false, false},
		{"bytes EXISTS", []interface{}{"SCRIPT", []byte("EXISTS"), "abc"}, false, false},
		{"not script", []interface{}{"EVALSHA", "abc"}, false, false},
		{"no subcommand", []interface{}{"SCRIPT"}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewCmd(ctx, tc.args...)
			if got := isScriptLoadCmd(cmd); got != tc.isLoad {
				t.Fatalf("isScriptLoadCmd = %v, want %v", got, tc.isLoad)
			}
			if got := isScriptFlushCmd(cmd); got != tc.isFlush {
				t.Fatalf("isScriptFlushCmd = %v, want %v", got, tc.isFlush)
			}
		})
	}
}

// TestPipelineRetryReissuesScriptFlushRawArgs: the same LOAD / FLUSH /
// EVALSHA batch as TestPipelineRetryReissuesScriptFlush, but the FLUSH is a
// raw Do command carrying the subcommand as []byte. The retry must still
// replay the FLUSH after the LOAD: replaying the LOAD without it executes a
// script the batch had already removed. Regression test for the review
// finding on #4351.
func TestPipelineRetryReissuesScriptFlushRawArgs(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	pipe.ScriptLoad(ctx, src)
	pipe.Do(ctx, "SCRIPT", []byte("FLUSH"))
	eval := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the retry must replay the raw []byte FLUSH after the LOAD, so the flushed script stays gone)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT", err)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must be reissued on the retry connection)", got)
	}
	if got := server.flushes.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT FLUSH commands, want 2 (the answered []byte FLUSH must be reissued after the LOAD)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryStaleReusedCommandIsStampedOnDialFailure: a Cmder reused
// from an earlier pipeline keeps its old io.EOF, and that stale error must
// not act as the answered/unanswered boundary when the next pipeline never
// acquires a connection. A fresh SET queued before the reused command was
// never dispatched, so it must be stamped with the dial error instead of
// keeping Err() == nil while Exec reports the failure. Regression test for
// the review finding on #4351.
func TestPipelineRetryStaleReusedCommandIsStampedOnDialFailure(t *testing.T) {
	// First pipeline: stamp io.EOF onto the shared command. A single
	// attempt (MaxRetries -1 normalises to 0) answers the warmup SET and
	// drops before the shared SET's reply, so no retry heals it.
	server := newPartialRetryServer(t)
	first := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = first.Close() })

	ctx := context.Background()
	pipe := first.Pipeline()
	pipe.Set(ctx, "warmup", "v", 0)
	shared := pipe.Set(ctx, "shared", "v", 0)
	if _, err := pipe.Exec(ctx); err == nil {
		t.Fatalf("first Exec: nil, want the drop's transport error")
	}
	if err := shared.Err(); !errors.Is(err, io.EOF) {
		t.Fatalf("shared SET after first Exec: %v, want io.EOF (the drop must leave the transport error on it)", err)
	}

	// Second pipeline: the same shared command, behind a fresh SET, against
	// an address nothing listens on — every dial is refused, so no command
	// is ever dispatched.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	dead := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = dead.Close() })

	pipe2 := dead.Pipeline()
	fresh := pipe2.Set(ctx, "fresh", "v", 0)
	_ = pipe2.BatchProcess(ctx, shared)
	_, err = pipe2.Exec(ctx)
	if err == nil {
		t.Fatalf("second Exec: nil, want the dial error (nothing listens on %s)", addr)
	}
	if err := fresh.Err(); err == nil {
		t.Fatal("fresh SET: nil, want the dial error (it was never dispatched, so it must not look successful)")
	}
	if err := shared.Err(); err == nil {
		t.Fatal("shared SET: nil, want an error (stale EOF or the dial error)")
	}
}

// staleTransportRetryServer is a scripted RESP server for the stale
// transport-error pipeline retry test. The first connection reads the whole
// batch and then closes without replying, so the first attempt leaves
// io.EOF on every command. Later connections answer HELLO and then stop
// reading forever, so the retry's write blocks until the caller's context
// deadline.
type staleTransportRetryServer struct {
	ln       net.Listener
	conns    atomic.Int32
	sets     atomic.Int32
	wantSets int32
	done     chan struct{}
}

func newStaleTransportRetryServer(t *testing.T, wantSets int) *staleTransportRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &staleTransportRetryServer{ln: ln, wantSets: int32(wantSets), done: make(chan struct{})}
	t.Cleanup(func() { close(s.done) })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *staleTransportRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	if first {
		for {
			name, err := readStaleErrRESPCommand(rd)
			if err != nil {
				return
			}
			switch name {
			case "hello":
				_, _ = conn.Write([]byte("%0\r\n"))
			case "set":
				if s.sets.Add(1) >= s.wantSets {
					// Whole batch read: close without replying, so
					// the first attempt's read fails with io.EOF.
					return
				}
			default:
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		}
	}
	// Later connections: answer the setup HELLO, then never read again.
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		if name == "hello" {
			_, _ = conn.Write([]byte("%0\r\n"))
			<-s.done
			return
		}
	}
}

// TestPipelineRetryReportsLatestTransportErrAfterRetryWriteFails: when the
// first attempt leaves io.EOF on the pending commands and the retry's write
// then hits the context deadline, Exec must report the latest transport
// error — errors.Is(err, context.DeadlineExceeded) — not the stale EOF, and
// the pipeline duration metric must describe the deadline failure, not the
// earlier EOF. Regression test for the review finding on #4351.
func TestPipelineRetryReportsLatestTransportErrAfterRetryWriteFails(t *testing.T) {
	const nsets = 128
	server := newStaleTransportRetryServer(t, nsets)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		// The retry write must fail with the caller's context deadline, not
		// the socket WriteTimeout: c.context only forwards the caller's ctx
		// when ContextTimeoutEnabled is set.
		ContextTimeoutEnabled: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	value := strings.Repeat("x", 256*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pipe := client.Pipeline()
	var sets []*StatusCmd
	for i := 0; i < nsets; i++ {
		sets = append(sets, pipe.Set(ctx, fmt.Sprintf("k%d", i), value, 0))
	}
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the retry write's context deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exec: %v, want context.DeadlineExceeded (the retry write hit the deadline; the stale io.EOF from the first attempt must not mask it)", err)
	}
	if errors.Is(err, io.EOF) {
		t.Fatalf("Exec: %v, want the latest transport error, not the stale io.EOF", err)
	}
	if err := sets[0].Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SET[0]: %v, want context.DeadlineExceeded", err)
	}
	if got := server.sets.Load(); got != int32(nsets) {
		t.Fatalf("server received %d SET commands on the first connection, want %d (the whole batch must have been read before the drop)", got, nsets)
	}
}

// TestPipelineRetryReissuesEvalAsScriptLoad: a pipeline EVAL followed by
// EVALSHA must not be trimmed to the EVALSHA on retry. EVAL populates the
// server's volatile script cache like SCRIPT LOAD, so a retry on a
// restarted server must reissue the answered EVAL's source as a SCRIPT LOAD
// ahead of the EVALSHA — without executing the answered script again.
// Regression test for the review finding on #4351.
func TestPipelineRetryReissuesEvalAsScriptLoad(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must reissue the answered EVAL's source as SCRIPT LOAD before the EVALSHA)", err)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVAL = %v, %v; want 1, nil", got, err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (the answered EVAL's source must be reissued once as a LOAD on the retry connection)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryRejectedEvalIsNotPreloaded: an EVAL the server rejected
// (negative key count) never ran, so it cached nothing. The retry must not
// turn that rejection into a successful SCRIPT LOAD: the retried EVALSHA
// would then execute writes where the original pipeline and the previous
// whole-batch retry returned NOSCRIPT. Regression test for the review
// finding on #4351.
func TestPipelineRetryRejectedEvalIsNotPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Do(ctx, "eval", src, -1)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "wrong number of keys") {
		t.Fatalf("Exec: %v, want the rejected EVAL's error (the first failed command wins; the retry must not turn the rejection into a successful SCRIPT LOAD)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "wrong number of keys") {
		t.Fatalf("EVAL: %v, want the wrong-number-of-keys rejection", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to reissue)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryCrossslotEvalIsNotPreloaded: a CROSSSLOT rejection is a
// pre-execution cluster refusal — the script never ran, so it cached
// nothing. The retry must not turn it into a successful SCRIPT LOAD: the
// retried EVALSHA would then execute writes the original batch refused.
// Regression test for the Cursor Bugbot finding on #4351.
func TestPipelineRetryCrossslotEvalIsNotPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "crossslot"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Do(ctx, "eval", src, 0)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "CROSSSLOT") {
		t.Fatalf("Exec: %v, want the CROSSSLOT rejection (the first failed command wins; the retry must not turn the rejection into a successful SCRIPT LOAD)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "CROSSSLOT") {
		t.Fatalf("EVAL: %v, want the CROSSSLOT rejection", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to reissue)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetrySkipsScriptPreloadWithoutEvalSha: when no command in the
// retry suffix needs the script cache, no SCRIPT LOAD may be prepended. An
// ACL user allowed EVAL and GET but not SCRIPT LOAD would otherwise fail a
// batch whose own commands all succeed, on the synthetic LOAD's NOPERM.
// Regression test for the review finding on #4351.
func TestPipelineRetrySkipsScriptPreloadWithoutEvalSha(t *testing.T) {
	server := newScriptRetryServer(t)
	server.denyLoad = true
	server.dropBefore = "get"
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (no command in the retry suffix needs the script cache, so no SCRIPT LOAD may be prepended)", err)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVAL = %v, %v; want 1, nil", got, err)
	}
	if err := get.Err(); err != nil {
		t.Fatalf("GET: %v, want nil", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (the retry suffix has no EVALSHA)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
}

// TestPipelineRetryReissuedScriptLoadKeepsOriginalResult: a SCRIPT LOAD the
// first attempt answered must keep its digest when the retry that reissues
// it fails while reading. The reissued LOAD is a fresh command; stamping
// the retry's transport error onto the original would replace its digest
// with a failure for work that already succeeded. Regression test for the
// review finding on #4351.
func TestPipelineRetryReissuedScriptLoadKeepsOriginalResult(t *testing.T) {
	server := newScriptRetryServer(t)
	server.dropRetryConn = true
	client := newMaxRetriesClient(t, server.ln.Addr().String(), 1)

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	load := pipe.ScriptLoad(ctx, src)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the retry's read failure")
	}
	if got := load.Val(); got != sha {
		t.Fatalf("SCRIPT LOAD = %q, want %q (the digest was answered on the first attempt; the retry's read failure must not overwrite it)", got, sha)
	}
	if err := load.Err(); err != nil {
		t.Fatalf("SCRIPT LOAD: %v, want nil", err)
	}
	if err := evalsha.Err(); err == nil {
		t.Fatalf("EVALSHA: nil, want the retry's read failure (it was never answered)")
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must be reissued on the retry connection)", got)
	}
}

// readStampRetryServer is a scripted RESP server for retry-read-stamping
// tests. The first connection answers SET and GET, then drops before WAIT's
// reply, so the retry re-runs the whole batch; the retry connection reads
// the batch and closes without replying, so the retry fails while
// re-reading its first command.
type readStampRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	sets  atomic.Int32
	gets  atomic.Int32
	waits atomic.Int32
}

func newReadStampRetryServer(t *testing.T) *readStampRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &readStampRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *readStampRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			s.sets.Add(1)
			if first {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "get":
			s.gets.Add(1)
			if first {
				_, _ = conn.Write([]byte("$1\r\nv\r\n"))
			}
		case "wait":
			s.waits.Add(1)
			// First connection: drop before replying, so the retry
			// re-runs the whole batch. Retry connection: close once
			// the batch arrived, without replying to any of it.
			return
		default:
			if first {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		}
	}
}

// TestPipelineRetryReadFailureKeepsAnsweredResults: when a whole-batch retry
// (a WAIT follows the answered writes) dies while re-reading its first
// command, the read failure must not stamp over commands a prior attempt
// already answered. The GET keeps its value; only the re-dispatched SET
// and the never-answered WAIT take the transport error. Regression test
// for the review finding on #4351.
func TestPipelineRetryReadFailureKeepsAnsweredResults(t *testing.T) {
	server := newReadStampRetryServer(t)
	client := newMaxRetriesClient(t, server.ln.Addr().String(), 1)

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	wait := pipe.Do(ctx, "wait", 1, 1000)
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the retry's read failure")
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil (the value was answered on the first attempt and the retry's read failure must not overwrite it)", got, err, "v")
	}
	if err := set.Err(); err == nil {
		t.Fatalf("SET: nil, want the retry's read failure (it was re-dispatched and never answered on the retry)")
	}
	if err := wait.Err(); err == nil {
		t.Fatalf("WAIT: nil, want the retry's read failure (it was never answered)")
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the answered SET must be re-run with its WAIT)", got)
	}
	if got := server.waits.Load(); got != 2 {
		t.Fatalf("server received %d WAIT commands, want 2", got)
	}
}

// newMaxRetriesClient is newPartialRetryClient with an explicit retry budget:
// a budget of 1 makes the second attempt the last, so an exit-path test can
// pin what a failed retry leaves behind.
func newMaxRetriesClient(t *testing.T, addr string, maxRetries int) *Client {
	t.Helper()
	client := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      maxRetries,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// seedStaleEOFIncr runs one pipeline that leaves io.EOF on a shared INCR
// and returns it: a warmup SET is answered, then the connection drops
// before the INCR's reply. The returned command carries the stale EOF a
// later pipeline must not mistake for its own progress.
func seedStaleEOFIncr(t *testing.T) *IntCmd {
	t.Helper()
	server := newPartialRetryServer(t)
	seeder := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = seeder.Close() })

	ctx := context.Background()
	pipe := seeder.Pipeline()
	pipe.Set(ctx, "warmup", "v", 0)
	shared := pipe.Incr(ctx, "counter")
	if _, err := pipe.Exec(ctx); err == nil {
		t.Fatalf("seeding Exec: nil, want the drop's transport error")
	}
	if err := shared.Err(); !errors.Is(err, io.EOF) {
		t.Fatalf("shared INCR after seeding: %v, want io.EOF", err)
	}
	return shared
}

// TestPipelineRetryReusedCommandFreshFailureIsRetried: a reused INCR that
// carries a stale io.EOF must still be retried when THIS attempt records a
// fresh EOF on it. The batch is a fresh SET, the reused INCR, and a fresh
// GET; the first connection answers the SET and then drops. The stale
// marker must be retired once the attempt dispatches the command, so the
// retry starts at the INCR instead of skipping it and leaving it with EOF
// after a healthy reconnection. Regression test for the review finding on
// #4351.
func TestPipelineRetryReusedCommandFreshFailureIsRetried(t *testing.T) {
	shared := seedStaleEOFIncr(t)

	server := newPartialRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	_ = pipe.BatchProcess(ctx, shared)
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must start at the reused INCR, whose fresh EOF is this attempt's progress)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil", err)
	}
	if got, err := shared.Result(); err != nil || got != int64(1) {
		t.Fatalf("reused INCR = %v, %v; want 1, nil (it must be retried, not left with the stale EOF)", got, err)
	}
	if got, err := get.Result(); err != nil || got != "1" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "1")
	}
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the reused INCR must be dispatched once per attempt)", got)
	}
}

// freshLoadingRetryServer is a scripted RESP server for the reused-command
// LOADING test. The first connection answers INCR with LOADING and then
// drops before SET's reply; later connections answer everything.
type freshLoadingRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	incrs atomic.Int32
	sets  atomic.Int32
}

func newFreshLoadingRetryServer(t *testing.T) *freshLoadingRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &freshLoadingRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *freshLoadingRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "incr":
			s.incrs.Add(1)
			if first {
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
			} else {
				_, _ = conn.Write([]byte(":1\r\n"))
			}
		case "set":
			s.sets.Add(1)
			if first {
				// Drop before replying: the SET gets no reply on
				// this connection.
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryReusedCommandFreshLoadingIsRetried: a reused INCR that
// carries a stale io.EOF must still be retried when THIS attempt records a
// fresh LOADING reply on it. The batch is the reused INCR and a fresh SET;
// the first connection answers the INCR with LOADING and then drops before
// the SET's reply. The stale marker must be retired once the attempt
// dispatches the command, so the retry starts at the INCR's retryable
// LOADING instead of treating the command as an answered prefix and
// dropping it. Regression test for the review finding on #4351.
func TestPipelineRetryReusedCommandFreshLoadingIsRetried(t *testing.T) {
	shared := seedStaleEOFIncr(t)

	server := newFreshLoadingRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	_ = pipe.BatchProcess(ctx, shared)
	set := pipe.Set(ctx, "k", "v", 0)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must start at the reused INCR's fresh LOADING, not drop it as an answered prefix)", err)
	}
	if got, err := shared.Result(); err != nil || got != int64(1) {
		t.Fatalf("reused INCR = %v, %v; want 1, nil (its fresh LOADING must be retried, not kept)", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil", err)
	}
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the reused INCR must be dispatched once per attempt)", got)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the SET must be retried with the INCR)", got)
	}
}

// answeredWriteFailRetryServer is a scripted RESP server for the
// answered-results-on-write-failure test. The first connection answers
// SELECT and then drops before the next pipeline command's reply; later
// connections answer HELLO and then stop reading forever, so the retry's
// write blocks until the caller's context deadline.
type answeredWriteFailRetryServer struct {
	ln      net.Listener
	conns   atomic.Int32
	selects atomic.Int32
	sets    atomic.Int32
	done    chan struct{}
}

func newAnsweredWriteFailRetryServer(t *testing.T) *answeredWriteFailRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &answeredWriteFailRetryServer{ln: ln, done: make(chan struct{})}
	t.Cleanup(func() { close(s.done) })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *answeredWriteFailRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	if !first {
		// Later connections: answer the setup HELLO, then never
		// read again, so the retry's write blocks until the
		// caller's context deadline.
		for {
			name, err := readStaleErrRESPCommand(rd)
			if err != nil {
				return
			}
			if name == "hello" {
				_, _ = conn.Write([]byte("%0\r\n"))
				<-s.done
				return
			}
		}
	}
	answered := false
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			s.selects.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
			answered = true
		case "set":
			s.sets.Add(1)
			if answered {
				// Drop before replying: the rest of the batch
				// gets no reply on this connection.
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			if answered {
				// Drop before replying: the rest of the batch
				// gets no reply on this connection.
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryWriteFailureKeepsAnsweredResults: when the first attempt
// answers SELECT (a connection-local state command, so the retry is
// whole-batch) and then drops, and the retry's write fails on the
// context deadline, the answered SELECT must keep its +OK. A batch-level
// transport stamp must not overwrite results a prior attempt already
// read. Regression test for the review finding on #4351.
func TestPipelineRetryWriteFailureKeepsAnsweredResults(t *testing.T) {
	server := newAnsweredWriteFailRetryServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		// The retry write must fail with the caller's context deadline, not
		// the socket WriteTimeout: c.context only forwards the caller's ctx
		// when ContextTimeoutEnabled is set.
		ContextTimeoutEnabled: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 1)
	set := pipe.Set(ctx, "k", strings.Repeat("x", 32*1024*1024), 0)
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the retry write's context deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exec: %v, want context.DeadlineExceeded", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered by the first attempt and the retry never re-executed it)", err)
	}
	if err := set.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SET: %v, want context.DeadlineExceeded (it was never answered)", err)
	}
	if err := incr.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("INCR: %v, want context.DeadlineExceeded (it was never answered)", err)
	}
	if got := server.selects.Load(); got != 1 {
		t.Fatalf("server received %d SELECT commands, want 1 (the retry's write must have blocked before re-executing it)", got)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (the retry's write must have blocked before re-executing it)", got)
	}
}

// loadingWriteFailRetryServer is a scripted RESP server for the
// write-failure fallback test. The first connection answers SET with
// LOADING and GET with a value, then closes; later connections answer
// HELLO and then stop reading forever, so a retry write that reaches a
// fresh connection blocks until the caller's context deadline.
type loadingWriteFailRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	sets  atomic.Int32
	done  chan struct{}
}

func newLoadingWriteFailRetryServer(t *testing.T) *loadingWriteFailRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &loadingWriteFailRetryServer{ln: ln, done: make(chan struct{})}
	t.Cleanup(func() { close(s.done) })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *loadingWriteFailRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	if !first {
		// Later connections: answer the setup HELLO, then never
		// read again, so a retry write that reaches a fresh
		// connection blocks until the caller's context deadline.
		for {
			name, err := readStaleErrRESPCommand(rd)
			if err != nil {
				return
			}
			if name == "hello" {
				_, _ = conn.Write([]byte("%0\r\n"))
				<-s.done
				return
			}
		}
	}
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			s.sets.Add(1)
			_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
		case "get":
			_, _ = conn.Write([]byte("$1\r\nv\r\n"))
			// The batch is fully answered: close, so the retry
			// cannot silently re-run it on this connection.
			return
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryWriteFailureKeepsAnsweredResultsOnFallback: the same
// answered-results guarantee as TestPipelineRetryWriteFailureKeepsAnsweredResults,
// but on the exit path where no command carries a transport error. The
// first attempt answers SET with LOADING and GET with a value, so the
// retry's write fails with nothing stamped; the exit stamping must still
// leave the answered GET alone instead of falling back to stamping the
// whole batch. Regression test for the review finding on #4351.
func TestPipelineRetryWriteFailureKeepsAnsweredResultsOnFallback(t *testing.T) {
	server := newLoadingWriteFailRetryServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		// The retry write must fail with the caller's context deadline, not
		// the socket WriteTimeout: c.context only forwards the caller's ctx
		// when ContextTimeoutEnabled is set.
		ContextTimeoutEnabled: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", strings.Repeat("x", 32*1024*1024), 0)
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "LOADING") {
		t.Fatalf("Exec: %v, want the retained LOADING (the retry write failed, but the answered GET must keep its value and the SET its LOADING reply)", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil (it was answered by the first attempt)", got, err, "v")
	}
	if err := set.Err(); err == nil || !strings.Contains(err.Error(), "LOADING") {
		t.Fatalf("SET: %v, want LOADING (its first-attempt reply must survive the failed retry write)", err)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (the retry's write must have failed before re-executing it)", got)
	}
}

// writeFailRetryServer is a scripted RESP server for write-failure retry
// tests. The first connection answers the HELLO handshake and then closes
// immediately, so the pipeline batch's write fails mid-batch; later
// connections answer everything. It counts the commands it actually
// answers, so tests can assert what a retry resent.
type writeFailRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	incrs atomic.Int32
	sets  atomic.Int32
}

func newWriteFailRetryServer(t *testing.T) *writeFailRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &writeFailRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *writeFailRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	if first {
		// Answer the handshake, then close before the batch is
		// written: the client's write must fail, not its read.
		name, err := readStaleErrRESPCommand(rd)
		if err != nil || name != "hello" {
			return
		}
		_, _ = conn.Write([]byte("%0\r\n"))
		return
	}
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "incr":
			s.incrs.Add(1)
			_, _ = conn.Write([]byte(":1\r\n"))
		case "set":
			s.sets.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "get":
			_, _ = conn.Write([]byte("$1\r\n1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryNilEvalIsPreloaded: an EVAL that ran but returned nil
// (redis.Nil) still populated the script cache, so the retry must preload
// its source before the retried EVALSHA. The previous guard only preloaded
// an EVAL with a nil error, so the retried EVALSHA returned NOSCRIPT
// instead of executing. Regression test for the review finding on #4351.
func TestPipelineRetryNilEvalIsPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "if ARGV[1] then return 42 end"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	evalsha := pipe.EvalSha(ctx, sha, nil, "x")
	_, err := pipe.Exec(ctx)
	// The EVAL's own nil reply is the first command error, so Exec
	// reports redis.Nil; what must hold is that the retried EVALSHA
	// executed instead of returning NOSCRIPT.
	if err == nil || !errors.Is(err, Nil) {
		t.Fatalf("Exec: %v, want redis.Nil (the EVAL's own nil reply is the first command error)", err)
	}
	if err := eval.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EVAL: %v, want redis.Nil (the script ran and returned nil)", err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil (the answered EVAL ran and cached the script, so the retry must preload it)", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (the nil-returning EVAL populated the cache)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryRuntimeErrorEvalIsPreloaded: an EVAL that failed at
// runtime (a Lua error) still populated the script cache before failing,
// so the retry must preload its source before the retried EVALSHA. The
// previous guard only preloaded an EVAL with a nil error, so the retried
// EVALSHA returned NOSCRIPT instead of executing. Regression test for the
// review finding on #4351.
func TestPipelineRetryRuntimeErrorEvalIsPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return redis.call('BADCMD')"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "Error running script") {
		t.Fatalf("Exec: %v, want the EVAL's Lua runtime error (it is the first command error)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "Error running script") {
		t.Fatalf("EVAL: %v, want the Lua runtime error", err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil (the EVAL ran and cached the script before failing, so the retry must preload it)", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (a runtime-failing EVAL populated the cache)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryPreloadFailureDoesNotFailBatch: when the server is still
// running, the script an answered EVAL cached is still there, so a
// synthetic SCRIPT LOAD that fails (an ACL user without SCRIPT LOAD
// permission) must not fail a batch whose own commands all succeed. The
// retry still attempts the preload; only its failure is ignored when the
// suffix it was built for succeeds. Regression test for the review finding
// on #4351.
func TestPipelineRetryPreloadFailureDoesNotFailBatch(t *testing.T) {
	server := newScriptRetryServer(t)
	server.denyLoad = true
	server.persistCache = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the script is still cached on the live server, so the failed synthetic SCRIPT LOAD must not fail a batch whose own commands succeeded)", err)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVAL = %v, %v; want 1, nil", got, err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil (it reads the cache the live server still holds)", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (the retry still attempts the preload; only its failure is ignored)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryConnStateReissueDoesNotReplayAnsweredWrites: a pipeline
// whose answered prefix established connection-local state must not replay
// its answered writes on retry. SELECT is re-established on the retry
// connection by reissuing a fresh copy, and only the unanswered suffix
// runs. Regression test for the review finding on #4351.
func TestPipelineRetryConnStateReissueDoesNotReplayAnsweredWrites(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	incr := pipe.Incr(ctx, "counter")
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be replayed when the retry only re-establishes the SELECT)", got)
	}
	if got := server.selects.Load(); got != 2 {
		t.Fatalf("server received %d SELECT commands, want 2 (the answered SELECT must be reissued on the retry connection)", got)
	}
	if got := server.setDB.Load(); got != 2 {
		t.Fatalf("SET ran on DB %d, want 2 (the retried SET must run on the re-established database)", got)
	}
}

// TestPipelineRetryWriteFailureRetiresStaleReusedCommand: a reused INCR
// that entered with a stale io.EOF must still be retried when THIS
// attempt's write fails on it. The write failure stamps a fresh transport
// error via setCmdsTransportErr; the attempt must retire the stale marker
// the way the read path already does, so the retry starts at the INCR
// instead of trimming it out of the batch and leaving it with the fresh
// error after a healthy reconnection. Regression test for the review
// finding on #4351.
func TestPipelineRetryWriteFailureRetiresStaleReusedCommand(t *testing.T) {
	shared := seedStaleEOFIncr(t)

	server := newWriteFailRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	value := strings.Repeat("x", 256*1024)
	ctx := context.Background()
	pipe := client.Pipeline()
	_ = pipe.BatchProcess(ctx, shared)
	var sets []*StatusCmd
	for i := 0; i < 128; i++ {
		sets = append(sets, pipe.Set(ctx, fmt.Sprintf("k%d", i), value, 0))
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must resend the reused INCR whose stale EOF the failed write replaced)", err)
	}
	if got, err := shared.Result(); err != nil || got != int64(1) {
		t.Fatalf("reused INCR = %v, %v; want 1, nil (it must be retried, not trimmed out of the batch)", got, err)
	}
	for i, set := range sets {
		if err := set.Err(); err != nil {
			t.Fatalf("SET[%d]: %v, want nil", i, err)
		}
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server answered %d INCR commands, want 1 (the reused INCR must be dispatched on the retry connection)", got)
	}
	if got := server.sets.Load(); got != 128 {
		t.Fatalf("server answered %d SET commands, want 128", got)
	}
}

// TestPipelineRetryPreloadFailureSurfacesSuffixError: the mirror of
// TestPipelineRetryPreloadFailureDoesNotFailBatch. When the server did
// restart, the script is gone, so the retried EVALSHA fails NOSCRIPT —
// and that suffix error, not the synthetic LOAD's NOPERM, is what Exec
// must report. A failed preload is ignored only when the suffix it was
// built for succeeds. Regression test for the review finding on #4351.
func TestPipelineRetryPreloadFailureSurfacesSuffixError(t *testing.T) {
	server := newScriptRetryServer(t)
	server.denyLoad = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the restarted server lost the script, so the retried EVALSHA fails; the LOAD's NOPERM must not mask it)", err)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVAL = %v, %v; want 1, nil", got, err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT", err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (the retry still attempts the preload)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryFailedConnStateReissueFailsBatch: a reissued
// connection-state command is required, not best-effort. When the
// reissued SELECT fails (a database index the server rejects), the suffix
// must not silently run on the wrong database and report success: Exec
// fails with the re-establishment error. Regression test for the review
// finding on #4351.
func TestPipelineRetryFailedConnStateReissueFailsBatch(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 99)
	incr := pipe.Incr(ctx, "counter")
	set := pipe.Set(ctx, "k", "v", 0)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "DB index is out of range") {
		t.Fatalf("Exec: %v, want the DB-index error (the reissued SELECT failed, so the batch must fail loudly instead of running the SET on the wrong database)", err)
	}
	if err := sel.Err(); err == nil || !strings.Contains(err.Error(), "DB index is out of range") {
		t.Fatalf("SELECT: %v, want the DB-index error", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil (it was answered by the first attempt and must not be replayed)", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (it ran on the default database; the batch failure is reported through Exec, not the SET)", err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be replayed)", got)
	}
	if got := server.selects.Load(); got != 2 {
		t.Fatalf("server received %d SELECT commands, want 2 (the answered SELECT must be reissued on the retry connection)", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (the reissued SELECT failed, so the retried SET could only run on the default database — and Exec fails loudly for it)", got)
	}
}

// TestPipelineRetryReportsLaterConnStateReissueFailure: the mirror of
// TestPipelineRetryFailedConnStateReissueFailsBatch where the FIRST
// reissued SELECT succeeds. The read loop reports only the first
// command's error, so when the replacement server rejects a LATER
// reissued SELECT, the batch must still fail: Exec must not report
// success while the suffix ran on the wrong database. Regression test
// for the review finding on #4351.
func TestPipelineRetryReportsLaterConnStateReissueFailure(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	server.secondMaxDB = 0
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel0 := pipe.Select(ctx, 0)
	sel2 := pipe.Select(ctx, 2)
	set := pipe.Set(ctx, "k", "v", 0)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "DB index is out of range") {
		t.Fatalf("Exec: %v, want the DB-index error (the reissued SELECT 2 failed on the replacement server, so the batch must fail loudly instead of reporting success with the SET on the wrong database)", err)
	}
	if err := sel0.Err(); err != nil {
		t.Fatalf("SELECT 0: %v, want nil (it was answered by the first attempt)", err)
	}
	if err := sel2.Err(); err != nil {
		t.Fatalf("SELECT 2: %v, want nil (it was answered by the first attempt; the failure belongs to the reissued copy, not the original)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (it ran on the default database; the batch failure is reported through Exec, not the SET)", err)
	}
	if got := server.selects.Load(); got != 4 {
		t.Fatalf("server received %d SELECT commands, want 4 (two answered, two reissued)", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (the reissued SELECT 2 failed, so the retried SET could only run on the default database — and Exec fails loudly for it)", got)
	}
}

// helloRetryServer is a scripted RESP server for the HELLO reissue test.
// The first connection answers the pipeline HELLO (a RESP3 map, as a
// real server would) and then drops before the GET; later connections
// answer everything.
type helloRetryServer struct {
	ln     net.Listener
	conns  atomic.Int32
	hellos atomic.Int32
	gets   atomic.Int32
}

func newHelloRetryServer(t *testing.T) *helloRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &helloRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *helloRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			s.hellos.Add(1)
			_, _ = conn.Write([]byte("%0\r\n"))
		case "get":
			s.gets.Add(1)
			if first {
				// Drop before replying: the GET must be retried, and
				// the retry must reissue the HELLO ahead of it.
				return
			}
			_, _ = conn.Write([]byte("$1\r\nv\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryReissuesHelloWithMapReply: a pipeline HELLO answered
// on the first attempt must be reissued on the retry with the same reply
// decoder. HELLO returns a map (RESP3) or an array (RESP2), which a
// fresh StringCmd cannot read, so reissuing it as one fails every retry
// attempt and the GET reply is never read. Regression test for the
// review finding on #4351.
func TestPipelineRetryReissuesHelloWithMapReply(t *testing.T) {
	server := newHelloRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	hello := pipe.Hello(ctx, 2, "", "", "")
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must reissue the HELLO with its map decoder, then read the GET)", err)
	}
	if err := hello.Err(); err != nil {
		t.Fatalf("HELLO: %v, want nil", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want \"v\", nil (the retried GET must be read after the reissued HELLO)", got, err)
	}
	if got := server.hellos.Load(); got < 2 {
		t.Fatalf("server received %d HELLO commands, want at least 2 (the answered HELLO must be reissued on the retry connection)", got)
	}
	if got := server.gets.Load(); got != 2 {
		t.Fatalf("server received %d GET commands, want 2 (one dropped, one answered)", got)
	}
}

// TestPipelineRetryPreservesSessionAndScriptPrerequisiteOrder: when the
// answered prefix mixes session commands and script prerequisites, the
// retry must replay them in the original prefix order. Grouping all
// session reissues before all script preloads runs the SCRIPT LOAD as
// the wrong user (AUTH runner) when only the loader may load scripts,
// so the LOAD gets NOPERM and the retried EVALSHA fails NOSCRIPT.
// Regression test for the review finding on #4351.
func TestPipelineRetryPreservesSessionAndScriptPrerequisiteOrder(t *testing.T) {
	server := newScriptRetryServer(t)
	server.dropBefore = "evalsha"
	server.loadUser = "loader"
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	authLoader := pipe.Do(ctx, "auth", "loader", "pass")
	load := pipe.ScriptLoad(ctx, src)
	authRunner := pipe.Do(ctx, "auth", "runner", "pass")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must replay AUTH loader, SCRIPT LOAD, AUTH runner in order, so the LOAD runs as loader and the EVALSHA finds the script)", err)
	}
	if err := authLoader.Err(); err != nil {
		t.Fatalf("AUTH loader: %v, want nil", err)
	}
	if got, err := load.Result(); err != nil || got != sha {
		t.Fatalf("SCRIPT LOAD = %q, %v; want %q, nil", got, err, sha)
	}
	if err := authRunner.Err(); err != nil {
		t.Fatalf("AUTH runner: %v, want nil", err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil", got, err)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (one per attempt)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0 (the LOAD must run as loader, before the runner AUTH)", got)
	}
}

// oneShotRetryServer is a scripted RESP server for one-shot
// connection-state commands (CLIENT CACHING, ASKING). The first
// connection answers the one-shot command and its follower, then drops
// before the next command's reply; later connections answer everything.
// It records the command sequence per connection so tests can assert
// which command a reissued one-shot flag ended up attached to.
type oneShotRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	incrs atomic.Int32
	mu    sync.Mutex
	seqs  map[int32][]string
}

func newOneShotRetryServer(t *testing.T) *oneShotRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &oneShotRetryServer{ln: ln, seqs: make(map[int32][]string)}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *oneShotRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	answered := 0
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		s.mu.Lock()
		s.seqs[n] = append(s.seqs[n], name)
		s.mu.Unlock()
		isPipelineCmd := name == "client" || name == "asking" || name == "get" || name == "incr"
		if n == 1 && answered >= 2 && isPipelineCmd {
			// Drop the connection mid-pipeline: this command and the
			// ones after it get no reply on this connection.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "get":
			reply = "$1\r\n1\r\n"
		case "incr":
			s.incrs.Add(1)
			reply = ":1\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
		if isPipelineCmd {
			answered++
		}
	}
}

func (s *oneShotRetryServer) sequence(n int32) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seqs[n]...)
}

// TestPipelineRetryDoesNotReissueOneShotClientCaching: CLIENT CACHING
// applies only to the next command, so reissuing an answered one ahead
// of a retry suffix attaches the flag to a later command instead of its
// original follower. Here the first connection answers CLIENT CACHING
// YES and GET k, then drops before INCR's reply: without the fix the
// retry resends [CLIENT CACHING YES, INCR], so the flag lands on the
// INCR instead of the GET. Regression test for the review finding on
// #4351.
func TestPipelineRetryDoesNotReissueOneShotClientCaching(t *testing.T) {
	server := newOneShotRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	caching := pipe.Do(ctx, "client", "caching", "yes")
	get := pipe.Get(ctx, "k")
	incr := pipe.Incr(ctx, "counter")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := caching.Err(); err != nil {
		t.Fatalf("CLIENT CACHING: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "1" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "1")
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1", got)
	}
	seq := server.sequence(2)
	// The second connection must replay the whole batch, so the
	// one-shot flag still lands on the GET it was issued for.
	idx := -1
	for i, name := range seq {
		if name == "client" {
			idx = i
		}
	}
	if idx < 0 || idx+1 >= len(seq) || seq[idx+1] != "get" {
		t.Fatalf("second connection ran %v, want CLIENT CACHING immediately before GET", seq)
	}
}

// TestPipelineRetryDoesNotReissueOneShotAsking: ASKING lasts for one
// command only, so the same reissue fault applies: an answered ASKING
// reissued ahead of a retry suffix would steer the first suffix
// command instead of the GET it was issued for. Same shape as the
// CLIENT CACHING test above. Regression test for the review finding on
// #4351.
func TestPipelineRetryDoesNotReissueOneShotAsking(t *testing.T) {
	server := newOneShotRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	asking := pipe.Do(ctx, "asking")
	get := pipe.Get(ctx, "k")
	incr := pipe.Incr(ctx, "counter")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := asking.Err(); err != nil {
		t.Fatalf("ASKING: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "1" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "1")
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1", got)
	}
	seq := server.sequence(2)
	idx := -1
	for i, name := range seq {
		if name == "asking" {
			idx = i
		}
	}
	if idx < 0 || idx+1 >= len(seq) || seq[idx+1] != "get" {
		t.Fatalf("second connection ran %v, want ASKING immediately before GET", seq)
	}
}

// optionalPreloadRetryServer is a scripted RESP server for the optional
// preload retry test. The first connection answers EVAL and drops before
// INCR's reply; the second answers the retry's SCRIPT LOAD with LOADING,
// answers INCR, and drops before EVALSHA's reply; later connections
// answer everything. It counts INCRs so a retry that re-runs the
// answered INCR shows up as a double increment.
type optionalPreloadRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	incrs atomic.Int32
	loads atomic.Int32
}

func newOptionalPreloadRetryServer(t *testing.T) *optionalPreloadRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &optionalPreloadRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *optionalPreloadRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "eval":
			_, _ = conn.Write([]byte(":1\r\n"))
		case "incr":
			if n == 1 {
				// Drop before replying: the INCR must be retried
				// with a script preload ahead of it.
				return
			}
			s.incrs.Add(1)
			_, _ = conn.Write([]byte(":1\r\n"))
		case "script":
			s.loads.Add(1)
			if n == 2 {
				// The dataset is still loading on the retry: a
				// retryable error on the best-effort preload.
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
				continue
			}
			// SCRIPT LOAD answers the script SHA.
			sha := ""
			if len(args) > 1 {
				sum := sha1.Sum([]byte(args[1]))
				sha = hex.EncodeToString(sum[:])
			}
			_, _ = conn.Write([]byte("$" + strconv.Itoa(len(sha)) + "\r\n" + sha + "\r\n"))
		case "evalsha":
			if n == 2 {
				// Drop before replying: the EVALSHA must be retried
				// without re-running the answered INCR.
				return
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryIgnoresOptionalPreloadErrorForSuffix: when a retry's
// best-effort SCRIPT LOAD fails with a retryable error, that failure
// must not start the retry suffix: the LOAD is not an original command,
// and starting there re-runs the answered originals behind it. Here the
// second attempt's LOAD answers LOADING, INCR answers, and the
// connection drops before EVALSHA's reply: without the fix the next
// retry resends [LOAD, INCR, EVALSHA] and the counter increments twice
// while Exec reports success. Regression test for the review finding on
// #4351.
func TestPipelineRetryIgnoresOptionalPreloadErrorForSuffix(t *testing.T) {
	server := newOptionalPreloadRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	src := "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, []string{"k"})
	incr := pipe.Incr(ctx, "counter")
	evalsha := pipe.EvalSha(ctx, sha, []string{"k"})
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := evalsha.Err(); err != nil {
		t.Fatalf("EVALSHA: %v", err)
	}
	if got := server.conns.Load(); got != 3 {
		t.Fatalf("server saw %d connections, want 3", got)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (one per retry attempt)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be re-run when only the optional preload failed)", got)
	}
}

// lateRestoreRetryServer is a scripted RESP server for the failed
// restoration connection-retirement test. The first connection answers
// SELECT and drops before SET a's reply; the second rejects the
// synthetic SELECT, answers SET a, and answers SET b only after a delay
// past the client's read timeout, so the late reply is still
// outstanding when the attempt returns the restoration error. A third
// connection would accept everything.
type lateRestoreRetryServer struct {
	ln         net.Listener
	conns      atomic.Int32
	setsA      atomic.Int32
	selectErrs atomic.Int32
}

func newLateRestoreRetryServer(t *testing.T) *lateRestoreRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &lateRestoreRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *lateRestoreRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "ping":
			_, _ = conn.Write([]byte("+PONG\r\n"))
		case "select":
			if n == 2 {
				// The replacement server has no DB 2: the
				// required re-establishment fails, so the SETs
				// that follow run in DB 0.
				s.selectErrs.Add(1)
				_, _ = conn.Write([]byte("-ERR DB index is out of range\r\n"))
				continue
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "set":
			if len(args) > 0 && args[0] == "a" {
				s.setsA.Add(1)
			}
			if n == 1 {
				// Drop before replying: the SET must be retried.
				return
			}
			if n == 2 && len(args) > 0 && args[0] == "b" {
				// Answer late, past the client's read timeout:
				// the reply is still outstanding when the
				// attempt reports the restoration failure.
				time.Sleep(500 * time.Millisecond)
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

func newLateRestoreRetryClient(t *testing.T, addr string) *Client {
	t.Helper()
	client := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     100 * time.Millisecond,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestPipelineRetryRetiresConnAfterFailedRequiredReissue: when a retry's
// required synthetic SELECT fails and the read loop then dies partway
// with a transport error, the attempt reports the restoration error —
// but the connection may still hold unread replies, so it must be
// removed, not returned to the pool for the next pipeline to reuse.
// Here the second connection's SET b reply arrives after the client's
// read timeout: without the fix the connection is Put back with the
// Redis error and the next pipeline reuses it, consuming the late +OK
// as its PING reply instead of PONG. Regression test for the review
// finding on #4351.
func TestPipelineRetryRetiresConnAfterFailedRequiredReissue(t *testing.T) {
	server := newLateRestoreRetryServer(t)
	client := newLateRestoreRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	setA := pipe.Set(ctx, "a", "v", 0)
	setB := pipe.Set(ctx, "b", "v", 0)
	if _, err := pipe.Exec(ctx); err == nil {
		t.Fatalf("Exec: nil, want an error (the failed SELECT 2 restoration must fail the batch)")
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered OK on the first connection)", err)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2", got)
	}
	if got := server.selectErrs.Load(); got != 1 {
		t.Fatalf("server rejected SELECT %d time(s), want 1", got)
	}
	if got := server.setsA.Load(); got != 2 {
		t.Fatalf("server received %d SET a commands, want 2", got)
	}
	if err := setA.Err(); err != nil {
		t.Fatalf("SET a: %v, want nil (it was answered OK in DB 0)", err)
	}
	if err := setB.Err(); err == nil {
		t.Fatalf("SET b: nil, want an error (its reply was never read)")
	}
	// The second connection may still hold SET b's late reply when it
	// is released with the restoration error: it must be removed, not
	// returned to the pipeline pool for the next pipeline to reuse. The
	// next pipeline runs before the late reply arrives (so the pool
	// health check cannot see it yet) and reads past it: without the
	// fix it consumes the late +OK as the PING reply instead of PONG.
	client.opt.MaxRetries = 0
	client.opt.ReadTimeout = 2 * time.Second
	pipe2 := client.Pipeline()
	pong := pipe2.Ping(ctx)
	if _, err := pipe2.Exec(ctx); err != nil {
		t.Fatalf("second pipeline Exec: %v, want nil (it must run on a fresh connection)", err)
	}
	if got := pong.Val(); got != "PONG" {
		t.Fatalf("PING = %q, want %q (the late reply of the failed attempt must not be consumed as this pipeline's reply)", got, "PONG")
	}
	if got := server.conns.Load(); got != 3 {
		t.Fatalf("server saw %d connections after the next pipeline, want 3 (the connection released with replies outstanding must not be reused)", got)
	}
}
