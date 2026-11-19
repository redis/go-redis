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
