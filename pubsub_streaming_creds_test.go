package redis_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/auth"
	"github.com/redis/go-redis/v9/maintnotifications"
)

type testStreamingProvider struct {
	mu          sync.Mutex
	credentials auth.Credentials
	listeners   []auth.CredentialsListener
}

func newTestStreamingProvider(username, password string) *testStreamingProvider {
	return &testStreamingProvider{
		credentials: auth.NewBasicCredentials(username, password),
	}
}

func (p *testStreamingProvider) Subscribe(listener auth.CredentialsListener) (auth.Credentials, auth.UnsubscribeFunc, error) {
	p.mu.Lock()
	p.listeners = append(p.listeners, listener)
	creds := p.credentials
	p.mu.Unlock()

	unsub := func() error {
		p.mu.Lock()
		defer p.mu.Unlock()
		for i, l := range p.listeners {
			if l == listener {
				p.listeners = append(p.listeners[:i], p.listeners[i+1:]...)
				break
			}
		}
		return nil
	}
	return creds, unsub, nil
}

func (p *testStreamingProvider) Update(username, password string) {
	p.mu.Lock()
	newCreds := auth.NewBasicCredentials(username, password)
	p.credentials = newCreds
	listeners := append([]auth.CredentialsListener(nil), p.listeners...)
	p.mu.Unlock()

	for _, l := range listeners {
		l.OnNext(newCreds)
	}
}

type mockRESP3Server struct {
	ln         net.Listener
	mu         sync.Mutex
	conns      []net.Conn
	commands   [][]string
	authErr    error
	subscribed chan struct{}
}

func newMockRESP3Server(t *testing.T) *mockRESP3Server {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen: %v", err)
	}
	s := &mockRESP3Server{
		ln:         ln,
		subscribed: make(chan struct{}, 10),
	}
	go s.acceptLoop()
	return s
}

func (s *mockRESP3Server) Close() {
	_ = s.ln.Close()
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, c := range s.conns {
		_ = c.Close()
	}
}

func (s *mockRESP3Server) acceptLoop() {
	for {
		c, err := s.ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conns = append(s.conns, c)
		s.mu.Unlock()
		go s.handleConn(c)
	}
}

func (s *mockRESP3Server) handleConn(c net.Conn) {
	defer c.Close()
	rd := bufio.NewReader(c)

	for {
		line, err := rd.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		if !strings.HasPrefix(line, "*") {
			continue
		}
		n, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
		if err != nil || n <= 0 {
			continue
		}

		args := make([]string, n)
		for i := 0; i < n; i++ {
			lenLine, err := rd.ReadString('\n')
			if err != nil {
				return
			}
			argLen, _ := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(lenLine, "$")))
			argBuf := make([]byte, argLen+2)
			if _, err := io.ReadFull(rd, argBuf); err != nil {
				return
			}
			args[i] = string(argBuf[:argLen])
		}

		cmdName := strings.ToUpper(args[0])

		s.mu.Lock()
		s.commands = append(s.commands, args)
		authErr := s.authErr
		s.mu.Unlock()

		switch cmdName {
		case "HELLO":
			_, _ = c.Write([]byte("%2\r\n+server\r\n+redis\r\n+proto\r\n:3\r\n"))
		case "CLIENT":
			_, _ = c.Write([]byte("+OK\r\n"))
		case "SUBSCRIBE":
			channel := "channel"
			if len(args) > 1 {
				channel = args[1]
			}
			resp := fmt.Sprintf(">3\r\n$9\r\nsubscribe\r\n$%d\r\n%s\r\n:1\r\n", len(channel), channel)
			_, _ = c.Write([]byte(resp))
			select {
			case s.subscribed <- struct{}{}:
			default:
			}
		case "AUTH":
			if authErr != nil {
				_, _ = c.Write([]byte(fmt.Sprintf("-WRONGPASS %s\r\n", authErr.Error())))
			} else {
				_, _ = c.Write([]byte("+OK\r\n"))
			}
		case "PING":
			_, _ = c.Write([]byte("+PONG\r\n"))
		default:
			_, _ = c.Write([]byte("+OK\r\n"))
		}
	}
}

func (s *mockRESP3Server) Publish(channel, payload string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	resp := fmt.Sprintf(">3\r\n$7\r\nmessage\r\n$%d\r\n%s\r\n$%d\r\n%s\r\n",
		len(channel), channel, len(payload), payload)
	for _, c := range s.conns {
		_, _ = c.Write([]byte(resp))
	}
}

func (s *mockRESP3Server) HasCommand(name string, matchArg string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, cmd := range s.commands {
		if strings.EqualFold(cmd[0], name) {
			if matchArg == "" {
				return true
			}
			for _, arg := range cmd[1:] {
				if strings.Contains(arg, matchArg) {
					return true
				}
			}
		}
	}
	return false
}

func TestPubSubRESP3StreamingCredsActiveReader(t *testing.T) {
	srv := newMockRESP3Server(t)
	defer srv.Close()

	provider := newTestStreamingProvider("init_user", "init_pass")
	client := redis.NewClient(&redis.Options{
		Addr:                         srv.ln.Addr().String(),
		Protocol:                     3,
		StreamingCredentialsProvider: provider,
		PoolSize:                     1,
		ReadTimeout:                  3 * time.Second,
		WriteTimeout:                 3 * time.Second,
		DisableIdentity:              true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := client.Subscribe(ctx, "alerts")
	defer ps.Close()

	// Wait for subscription confirmation
	msg, err := ps.Receive(ctx)
	if err != nil {
		t.Fatalf("ps.Receive subscribe confirmation failed: %v", err)
	}
	sub, ok := msg.(*redis.Subscription)
	if !ok || sub.Kind != "subscribe" || sub.Channel != "alerts" {
		t.Fatalf("unexpected subscription confirmation: %v", msg)
	}

	// Active reader goroutine waiting on messages
	msgCh := make(chan *redis.Message, 1)
	errCh := make(chan error, 1)
	go func() {
		m, e := ps.ReceiveMessage(context.Background())
		if e != nil {
			errCh <- e
			return
		}
		msgCh <- m
	}()

	// Allow reader goroutine to block in ReceiveTimeout
	time.Sleep(100 * time.Millisecond)

	// Trigger streaming credentials rotation while reader is actively waiting
	provider.Update("rotated_user", "rotated_pass")

	// Wait for AUTH rotated_user command to be processed
	deadline := time.Now().Add(2 * time.Second)
	for !srv.HasCommand("AUTH", "rotated_user") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !srv.HasCommand("AUTH", "rotated_user") {
		t.Fatal("expected server to receive AUTH rotated_user command")
	}

	// Server publishes a message to the active subscription
	srv.Publish("alerts", "critical_alert_payload")

	// Active reader should receive message despite in-place re-auth
	select {
	case m := <-msgCh:
		if m.Channel != "alerts" || m.Payload != "critical_alert_payload" {
			t.Fatalf("unexpected message: %v", m)
		}
	case e := <-errCh:
		t.Fatalf("reader failed with error: %v", e)
	case <-time.After(3 * time.Second):
		t.Fatal("timed out waiting for active reader to receive message after re-auth")
	}
}

func TestPubSubRESP3StreamingCredsIdleReader(t *testing.T) {
	srv := newMockRESP3Server(t)
	defer srv.Close()

	provider := newTestStreamingProvider("init_user", "init_pass")
	client := redis.NewClient(&redis.Options{
		Addr:                         srv.ln.Addr().String(),
		Protocol:                     3,
		StreamingCredentialsProvider: provider,
		PoolSize:                     1,
		ReadTimeout:                  3 * time.Second,
		WriteTimeout:                 3 * time.Second,
		DisableIdentity:              true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := client.Subscribe(ctx, "idle_channel")
	defer ps.Close()

	// Wait for subscription confirmation
	msg, err := ps.Receive(ctx)
	if err != nil {
		t.Fatalf("ps.Receive failed: %v", err)
	}
	if sub, ok := msg.(*redis.Subscription); !ok || sub.Kind != "subscribe" {
		t.Fatalf("expected subscribe confirmation, got %v", msg)
	}

	// Trigger credential rotation when reader is idle (no goroutine inside Receive)
	provider.Update("idle_user", "idle_pass")

	// Verify AUTH was issued inline
	deadline := time.Now().Add(2 * time.Second)
	for !srv.HasCommand("AUTH", "idle_user") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !srv.HasCommand("AUTH", "idle_user") {
		t.Fatal("expected server to receive AUTH idle_user command inline")
	}

	// Subsequent message reception succeeds
	srv.Publish("idle_channel", "payload_after_idle_reauth")

	m, err := ps.ReceiveMessage(ctx)
	if err != nil {
		t.Fatalf("failed to receive message after idle reauth: %v", err)
	}
	if m.Channel != "idle_channel" || m.Payload != "payload_after_idle_reauth" {
		t.Fatalf("unexpected message: %v", m)
	}
}

func TestPubSubRESP3StreamingCredsAuthError(t *testing.T) {
	srv := newMockRESP3Server(t)
	defer srv.Close()

	provider := newTestStreamingProvider("init_user", "init_pass")
	client := redis.NewClient(&redis.Options{
		Addr:                         srv.ln.Addr().String(),
		Protocol:                     3,
		StreamingCredentialsProvider: provider,
		PoolSize:                     1,
		ReadTimeout:                  2 * time.Second,
		WriteTimeout:                 2 * time.Second,
		DisableIdentity:              true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := client.Subscribe(ctx, "err_channel")
	defer ps.Close()

	_, err := ps.Receive(ctx)
	if err != nil {
		t.Fatalf("initial subscribe failed: %v", err)
	}

	// Set server to fail upcoming AUTH commands
	srv.mu.Lock()
	srv.authErr = errors.New("invalid credentials provided")
	srv.mu.Unlock()

	// Trigger credential rotation
	provider.Update("bad_user", "bad_pass")

	// Verify AUTH was received and failed
	deadline := time.Now().Add(2 * time.Second)
	for !srv.HasCommand("AUTH", "bad_user") && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if !srv.HasCommand("AUTH", "bad_user") {
		t.Fatal("expected server to receive AUTH bad_user command")
	}
}

func TestPubSubRESP2StreamingCredsNoOp(t *testing.T) {
	srv := newMockRESP3Server(t)
	defer srv.Close()

	provider := newTestStreamingProvider("init_user", "init_pass")
	client := redis.NewClient(&redis.Options{
		Addr:                         srv.ln.Addr().String(),
		Protocol:                     2,
		StreamingCredentialsProvider: provider,
		PoolSize:                     1,
		ReadTimeout:                  2 * time.Second,
		WriteTimeout:                 2 * time.Second,
		DisableIdentity:              true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	ps := client.Subscribe(ctx, "resp2_channel")
	defer ps.Close()

	_, err := ps.Receive(ctx)
	if err != nil {
		t.Fatalf("initial subscribe failed: %v", err)
	}

	// Trigger credential rotation under RESP2
	provider.Update("resp2_new_user", "resp2_new_pass")

	// Wait briefly and verify no AUTH was sent over pubsub socket
	time.Sleep(200 * time.Millisecond)
	if srv.HasCommand("AUTH", "resp2_new_user") {
		t.Fatal("expected RESP2 Pub/Sub socket NOT to issue in-place AUTH")
	}
}
