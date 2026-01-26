package redis_test

import (
	"crypto/tls"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestTLSStandalone tests TLS connection to standalone Redis
func TestTLSStandalone(t *testing.T) {
	// Use InsecureSkipVerify for testing with self-signed certificates
	tlsConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	client := redis.NewClient(&redis.Options{
		Addr:      "localhost:6666",
		TLSConfig: tlsConfig,
	})
	defer client.Close()

	// Test PING
	val, err := client.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("PING failed: %v", err)
	}
	if val != "PONG" {
		t.Fatalf("Expected PONG, got %s", val)
	}

	// Test SET/GET
	err = client.Set(ctx, "tls_test_key", "tls_test_value", 0).Err()
	if err != nil {
		t.Fatalf("SET failed: %v", err)
	}

	val, err = client.Get(ctx, "tls_test_key").Result()
	if err != nil {
		t.Fatalf("GET failed: %v", err)
	}
	if val != "tls_test_value" {
		t.Fatalf("Expected tls_test_value, got %s", val)
	}

	// Cleanup
	client.Del(ctx, "tls_test_key")

	t.Log("✅ TLS standalone test passed")
}

// TestTLSInsecureSkipVerify tests TLS with InsecureSkipVerify
func TestTLSInsecureSkipVerify(t *testing.T) {
	insecureTLSConfig := &tls.Config{
		InsecureSkipVerify: true,
	}

	client := redis.NewClient(&redis.Options{
		Addr:      "localhost:6666",
		TLSConfig: insecureTLSConfig,
	})
	defer client.Close()

	val, err := client.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("PING failed: %v", err)
	}
	if val != "PONG" {
		t.Fatalf("Expected PONG, got %s", val)
	}

	t.Log("✅ TLS InsecureSkipVerify test passed")
}

// TestTLSRedissURL tests rediss:// URL scheme
func TestTLSRedissURL(t *testing.T) {
	opt, err := redis.ParseURL("rediss://localhost:6666")
	if err != nil {
		t.Fatalf("ParseURL failed: %v", err)
	}

	// Override TLS config to skip verification for self-signed certs
	opt.TLSConfig = &tls.Config{
		InsecureSkipVerify: true,
	}

	client := redis.NewClient(opt)
	defer client.Close()

	val, err := client.Ping(ctx).Result()
	if err != nil {
		t.Fatalf("PING failed: %v", err)
	}
	if val != "PONG" {
		t.Fatalf("Expected PONG, got %s", val)
	}

	t.Log("✅ TLS rediss:// URL test passed")
}

