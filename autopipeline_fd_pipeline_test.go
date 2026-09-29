package redis

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fdStateServer is a minimal stateful RESP2 server for the FD pipeline tests.
// SET stores, GET reads ($-1 when missing), HELLO gets a minimal map, anything
// else +OK. It can answer the FIRST SET with -LOADING (not applied), and delay
// every GET/SET reply. Commands are parsed with readRESPCommand from
// internal_maint_notif_test.go.
type fdStateServer struct {
	ln          net.Listener
	mu          sync.Mutex
	kv          map[string]string
	loadingOnce atomic.Bool
	delay       time.Duration
	wg          sync.WaitGroup
	conns       []net.Conn
}

func newFDStateServer(t *testing.T) *fdStateServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fdStateServer{ln: ln, kv: map[string]string{}}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			s.mu.Lock()
			s.conns = append(s.conns, c)
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				s.serve(c)
			}()
		}
	}()
	t.Cleanup(s.close)
	return s
}

func (s *fdStateServer) addr() string { return s.ln.Addr().String() }

func (s *fdStateServer) close() {
	_ = s.ln.Close()
	s.mu.Lock()
	for _, c := range s.conns {
		_ = c.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
}

func (s *fdStateServer) serve(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)
	for {
		args, err := readRESPCommand(rd)
		if err != nil {
			return
		}
		name := ""
		if len(args) > 0 {
			name = strings.ToLower(args[0])
		}
		var reply string
		switch {
		case name == "hello":
			reply = "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:2\r\n"
		case name == "set" && len(args) >= 3:
			if s.loadingOnce.CompareAndSwap(true, false) {
				reply = "-LOADING Redis is loading the dataset in memory\r\n"
				break
			}
			s.mu.Lock()
			s.kv[args[1]] = args[2]
			s.mu.Unlock()
			reply = "+OK\r\n"
		case name == "get" && len(args) == 2:
			s.mu.Lock()
			v, ok := s.kv[args[1]]
			s.mu.Unlock()
			if ok {
				reply = "$" + itoa(len(v)) + "\r\n" + v + "\r\n"
			} else {
				reply = "$-1\r\n"
			}
		default:
			reply = "+OK\r\n"
		}
		if (name == "get" || name == "set") && s.delay > 0 {
			time.Sleep(s.delay)
		}
		if _, err := io.WriteString(c, reply); err != nil {
			return
		}
	}
}

func fdPipelineTestAP(t *testing.T, opt *Options) *AutoPipeliner {
	t.Helper()
	opt.Protocol = 2
	opt.DisableIdentity = true
	if opt.PipelinePoolSize == 0 {
		opt.PipelinePoolSize = 2
	}
	c := NewClient(opt)
	t.Cleanup(func() { _ = c.Close() })
	ap, err := c.AsyncAutoPipelineWithOptions(&AutoPipelineOptions{FullDuplex: true})
	if err != nil {
		t.Fatalf("AsyncAutoPipeline: %v", err)
	}
	t.Cleanup(func() { _ = ap.Close() })
	if ap.fd == nil {
		t.Fatal("full-duplex engine not active")
	}
	return ap
}

// Pipeline.Exec returns the first command error, redis.Nil included. The FD
// path used to drop Nil and return nil for a pipeline whose only error is a
// missing key.
func TestFDPipelineExecReturnsNil(t *testing.T) {
	srv := newFDStateServer(t)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr()})

	ctx := context.Background()
	pipe := ap.Pipeline()
	get := pipe.Get(ctx, "missing")
	if _, err := pipe.Exec(ctx); !errors.Is(err, Nil) {
		t.Fatalf("Exec err=%v, want redis.Nil", err)
	}
	if !errors.Is(get.Err(), Nil) {
		t.Fatalf("get err=%v, want redis.Nil", get.Err())
	}
}

// A retryable reply on the first command retries the whole pipeline, in
// order, as an ordinary pipeline does. The FD path used to retry the SET alone
// on another connection while the GET completed from the first attempt, so
// the GET returned the value from before the SET.
func TestFDPipelineExecRetriesWholeBatchInOrder(t *testing.T) {
	srv := newFDStateServer(t)
	srv.loadingOnce.Store(true)
	ap := fdPipelineTestAP(t, &Options{Addr: srv.addr(), MaxRetries: 3})

	ctx := context.Background()
	pipe := ap.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("set: %v", err)
	}
	if v, err := get.Result(); err != nil || v != "v" {
		t.Fatalf("get: v=%q err=%v, want the value the pipeline's SET wrote", v, err)
	}
	if srv.loadingOnce.Load() {
		t.Fatal("the LOADING reply was never sent; the test did not exercise the retry")
	}
}

// With ContextTimeoutEnabled an ordinary pipeline is bounded by the caller's
// deadline. The FD path used to wait for the replies regardless.
func TestFDPipelineExecHonorsContextTimeout(t *testing.T) {
	srv := newFDStateServer(t)
	srv.delay = time.Second
	ap := fdPipelineTestAP(t, &Options{
		Addr:                  srv.addr(),
		ContextTimeoutEnabled: true,
		ReadTimeout:           5 * time.Second,
		MaxRetries:            -1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	pipe := ap.Pipeline()
	pipe.Get(ctx, "k")
	start := time.Now()
	_, err := pipe.Exec(ctx)
	if el := time.Since(start); el > 700*time.Millisecond {
		t.Fatalf("Exec took %v with a 100ms deadline (err=%v): the deadline was ignored", el, err)
	}
	if err == nil {
		t.Fatal("Exec returned nil although its deadline passed before the reply")
	}
}
