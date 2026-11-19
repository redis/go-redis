package redis

import (
	"bufio"
	"context"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

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
