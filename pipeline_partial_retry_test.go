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
}

func newConnStateRetryServer(t *testing.T) *connStateRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &connStateRetryServer{ln: ln}
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
	answered := false
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		// Connection setup commands (HELLO and friends) are not part of
		// the pipeline: always answer them and never drop on them.
		isPipelineCmd := name == "select" || name == "set" || name == "get" ||
			name == "multi" || name == "exec" || name == "watch" || name == "unwatch"
		if first && answered && isPipelineCmd {
			// Drop the connection mid-pipeline: this command and the
			// ones after it get no reply on this connection.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "select":
			if len(args) > 0 {
				if n, err := strconv.Atoi(args[0]); err == nil {
					db = n
				}
			}
		case "set":
			s.sets.Add(1)
			s.setDB.Store(int32(db))
			if watched {
				s.setWhileWatched.Add(1)
			}
			if inMulti {
				reply = "+QUEUED\r\n"
			}
		case "watch":
			watched = true
		case "unwatch":
			watched = false
		case "get":
			reply = "$1\r\nv\r\n"
		case "multi":
			s.multiCount.Add(1)
			inMulti = true
		case "exec":
			if inMulti {
				s.execWithMulti.Add(1)
				inMulti = false
				reply = "*1\r\n+OK\r\n"
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
			answered = true
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
// retry's write blocks until the write timeout; later connections answer
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
				// Stop reading: the retry's write must block
				// until the write timeout instead of being
				// answered by this connection.
				<-s.done
				return
			}
		}
	}
}

// TestPipelineRetryDoesNotReportStaleError: when the first attempt reads a
// retryable LOADING error for SET and a successful GET, and the next
// attempt fails during its write, the retry must re-execute SET — not drop
// it as though the earlier LOADING had been this attempt's answer and
// report the stale error after the server has recovered. (LOADING, not
// READONLY, seeds the stale error: a READONLY batch error makes the pool
// drop the connection outright, see #790, so the write failure this test
// needs would never happen.) Regression test for the review finding on
// #4351.
func TestPipelineRetryDoesNotReportStaleError(t *testing.T) {
	server := newStaleErrRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())
	client.opt.WriteTimeout = 100 * time.Millisecond

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", strings.Repeat("x", 32*1024*1024), 0)
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
type scriptRetryServer struct {
	ln        net.Listener
	conns     atomic.Int32
	loads     atomic.Int32
	flushes   atomic.Int32
	evalshas  atomic.Int32
	noscripts atomic.Int32
}

func newScriptRetryServer(t *testing.T) *scriptRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &scriptRetryServer{ln: ln}
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
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "script":
			if len(args) >= 2 && strings.EqualFold(args[0], "load") {
				s.loads.Add(1)
				sum := sha1.Sum([]byte(args[1]))
				digest := hex.EncodeToString(sum[:])
				loaded[digest] = true
				_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(digest), digest)
			} else if len(args) >= 1 && strings.EqualFold(args[0], "flush") {
				s.flushes.Add(1)
				loaded = map[string]bool{}
				_, _ = conn.Write([]byte("+OK\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "evalsha":
			s.evalshas.Add(1)
			if first {
				// Drop before replying: the EVALSHA must be retried
				// together with the SCRIPT LOAD that populates the
				// restarted server's script cache.
				return
			}
			if len(args) >= 1 && loaded[args[0]] {
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
