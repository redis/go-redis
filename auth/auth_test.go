package auth

import (
	"errors"
	"sync"
	"testing"
	"time"
)

type mockStreamingProvider struct {
	credentials Credentials
	err         error
	updates     chan Credentials
}

func newMockStreamingProvider(initialCreds Credentials) *mockStreamingProvider {
	return &mockStreamingProvider{
		credentials: initialCreds,
		updates:     make(chan Credentials, 10),
	}
}

func (m *mockStreamingProvider) Subscribe(listener CredentialsListener) (Credentials, UnsubscribeFunc, error) {
	if m.err != nil {
		return nil, nil, m.err
	}

	// Send initial credentials
	listener.OnNext(m.credentials)

	// Start goroutine to handle updates
	go func() {
		for creds := range m.updates {
			listener.OnNext(creds)
		}
	}()

	return m.credentials, func() error {
		close(m.updates)
		return nil
	}, nil
}

func TestStreamingCredentialsProvider(t *testing.T) {
	t.Run("successful subscription", func(t *testing.T) {
		initialCreds := NewBasicCredentials("user1", "pass1")
		provider := newMockStreamingProvider(initialCreds)

		var receivedCreds []Credentials
		var receivedErrors []error
		var mu sync.Mutex

		listener := NewReAuthCredentialsListener(
			func(creds Credentials) error {
				mu.Lock()
				receivedCreds = append(receivedCreds, creds)
				mu.Unlock()
				return nil
			},
			func(err error) {
				receivedErrors = append(receivedErrors, err)
			},
		)

		creds, cancel, err := provider.Subscribe(listener)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cancel == nil {
			t.Fatal("expected cancel function to be non-nil")
		}
		if creds != initialCreds {
			t.Fatalf("expected credentials %v, got %v", initialCreds, creds)
		}
		if len(receivedCreds) != 1 {
			t.Fatalf("expected 1 received credential, got %d", len(receivedCreds))
		}
		if receivedCreds[0] != initialCreds {
			t.Fatalf("expected received credential %v, got %v", initialCreds, receivedCreds[0])
		}
		if len(receivedErrors) != 0 {
			t.Fatalf("expected no errors, got %d", len(receivedErrors))
		}

		// Send an update
		newCreds := NewBasicCredentials("user2", "pass2")
		provider.updates <- newCreds

		// Wait for update to be processed
		time.Sleep(100 * time.Millisecond)
		mu.Lock()
		if len(receivedCreds) != 2 {
			t.Fatalf("expected 2 received credentials, got %d", len(receivedCreds))
		}
		if receivedCreds[1] != newCreds {
			t.Fatalf("expected received credential %v, got %v", newCreds, receivedCreds[1])
		}
		mu.Unlock()

		// Cancel subscription
		if err := cancel(); err != nil {
			t.Fatalf("unexpected error cancelling subscription: %v", err)
		}
	})

	t.Run("subscription error", func(t *testing.T) {
		provider := &mockStreamingProvider{
			err: errors.New("subscription failed"),
		}

		var receivedCreds []Credentials
		var receivedErrors []error

		listener := NewReAuthCredentialsListener(
			func(creds Credentials) error {
				receivedCreds = append(receivedCreds, creds)
				return nil
			},
			func(err error) {
				receivedErrors = append(receivedErrors, err)
			},
		)

		creds, cancel, err := provider.Subscribe(listener)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if cancel != nil {
			t.Fatal("expected cancel function to be nil")
		}
		if creds != nil {
			t.Fatalf("expected nil credentials, got %v", creds)
		}
		if len(receivedCreds) != 0 {
			t.Fatalf("expected no received credentials, got %d", len(receivedCreds))
		}
		if len(receivedErrors) != 0 {
			t.Fatalf("expected no errors, got %d", len(receivedErrors))
		}
	})

	t.Run("re-auth error", func(t *testing.T) {
		initialCreds := NewBasicCredentials("user1", "pass1")
		provider := newMockStreamingProvider(initialCreds)

		reauthErr := errors.New("re-auth failed")
		var receivedErrors []error

		listener := NewReAuthCredentialsListener(
			func(creds Credentials) error {
				return reauthErr
			},
			func(err error) {
				receivedErrors = append(receivedErrors, err)
			},
		)

		creds, cancel, err := provider.Subscribe(listener)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		if cancel == nil {
			t.Fatal("expected cancel function to be non-nil")
		}
		if creds != initialCreds {
			t.Fatalf("expected credentials %v, got %v", initialCreds, creds)
		}
		if len(receivedErrors) != 1 {
			t.Fatalf("expected 1 error, got %d", len(receivedErrors))
		}
		if receivedErrors[0] != reauthErr {
			t.Fatalf("expected error %v, got %v", reauthErr, receivedErrors[0])
		}

		if err := cancel(); err != nil {
			t.Fatalf("unexpected error cancelling subscription: %v", err)
		}
	})
}

func TestBasicCredentials(t *testing.T) {
	t.Run("basic auth", func(t *testing.T) {
		creds := NewBasicCredentials("user1", "pass1")
		username, password := creds.BasicAuth()
		if username != "user1" {
			t.Fatalf("expected username 'user1', got '%s'", username)
		}
		if password != "pass1" {
			t.Fatalf("expected password 'pass1', got '%s'", password)
		}
	})

	t.Run("raw credentials", func(t *testing.T) {
		creds := NewBasicCredentials("user1", "pass1")
		raw := creds.RawCredentials()
		expected := "user1:pass1"
		if raw != expected {
			t.Fatalf("expected raw credentials '%s', got '%s'", expected, raw)
		}
	})

	t.Run("empty username", func(t *testing.T) {
		creds := NewBasicCredentials("", "pass1")
		username, password := creds.BasicAuth()
		if username != "" {
			t.Fatalf("expected empty username, got '%s'", username)
		}
		if password != "pass1" {
			t.Fatalf("expected password 'pass1', got '%s'", password)
		}
	})
}

func TestReAuthCredentialsListener(t *testing.T) {
	t.Run("successful re-auth", func(t *testing.T) {
		var reAuthCalled bool
		var onErrCalled bool
		var receivedCreds Credentials

		listener := NewReAuthCredentialsListener(
			func(creds Credentials) error {
				reAuthCalled = true
				receivedCreds = creds
				return nil
			},
			func(err error) {
				onErrCalled = true
			},
		)

		creds := NewBasicCredentials("user1", "pass1")
		listener.OnNext(creds)

		if !reAuthCalled {
			t.Fatal("expected reAuth to be called")
		}
		if onErrCalled {
			t.Fatal("expected onErr not to be called")
		}
		if receivedCreds != creds {
			t.Fatalf("expected credentials %v, got %v", creds, receivedCreds)
		}
	})

	t.Run("re-auth error", func(t *testing.T) {
		var reAuthCalled bool
		var onErrCalled bool
		var receivedErr error
		expectedErr := errors.New("re-auth failed")

		listener := NewReAuthCredentialsListener(
			func(creds Credentials) error {
				reAuthCalled = true
				return expectedErr
			},
			func(err error) {
				onErrCalled = true
				receivedErr = err
			},
		)

		creds := NewBasicCredentials("user1", "pass1")
		listener.OnNext(creds)

		if !reAuthCalled {
			t.Fatal("expected reAuth to be called")
		}
		if !onErrCalled {
			t.Fatal("expected onErr to be called")
		}
		if receivedErr != expectedErr {
			t.Fatalf("expected error %v, got %v", expectedErr, receivedErr)
		}
	})

	t.Run("on error", func(t *testing.T) {
		var onErrCalled bool
		var receivedErr error
		expectedErr := errors.New("provider error")

		listener := NewReAuthCredentialsListener(
			func(creds Credentials) error {
				return nil
			},
			func(err error) {
				onErrCalled = true
				receivedErr = err
			},
		)

		listener.OnError(expectedErr)

		if !onErrCalled {
			t.Fatal("expected onErr to be called")
		}
		if receivedErr != expectedErr {
			t.Fatalf("expected error %v, got %v", expectedErr, receivedErr)
		}
	})

	t.Run("nil callbacks", func(t *testing.T) {
		listener := NewReAuthCredentialsListener(nil, nil)

		// Should not panic
		listener.OnNext(NewBasicCredentials("user1", "pass1"))
		listener.OnError(errors.New("test error"))
	})
}
