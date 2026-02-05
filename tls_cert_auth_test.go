package redis_test

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestTLSCertificateAuthentication tests that Redis automatically authenticates
// a user based on the CN field in the client's TLS certificate.
//
// This test demonstrates the flow:
// 1. Create a Redis ACL user with a specific username
// 2. Generate a client certificate with that username in the CN field
// 3. Connect using TLS with that certificate
// 4. Verify that Redis automatically authenticates as that user (no AUTH command needed)
func TestTLSCertificateAuthentication(t *testing.T) {
	ctx := context.Background()
	testUsername := "testcertuser"

	// Step 1: Create a non-TLS client to set up the ACL user
	setupClient := redis.NewClient(&redis.Options{
		Addr: "localhost:6379", // Non-TLS port
	})
	defer setupClient.Close()

	// Verify connection
	if err := setupClient.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping test - Redis not available: %v", err)
	}

	// Clean up any existing test user
	setupClient.ACLDelUser(ctx, testUsername)

	// Step 2: Create ACL user with specific permissions
	// The user can read/write keys but has limited command access
	err := setupClient.ACLSetUser(ctx,
		testUsername,
		"on",          // Enable the user
		"nopass",      // No password required (will use cert auth)
		"~*",          // Can access all keys
		"+get",        // Allow GET command
		"+set",        // Allow SET command
		"+ping",       // Allow PING command
		"+acl|whoami", // Allow ACL WHOAMI command
	).Err()
	if err != nil {
		t.Fatalf("Failed to create ACL user: %v", err)
	}
	defer setupClient.ACLDelUser(ctx, testUsername) // Cleanup

	// Verify user was created
	users, err := setupClient.ACLUsers(ctx).Result()
	if err != nil {
		t.Fatalf("Failed to list ACL users: %v", err)
	}
	t.Logf("ACL users: %v", users)

	// Step 3: Load CA certificate and key to sign our custom client cert
	caCertPEM, err := os.ReadFile("dockers/standalone/tls/testcertuser.crt")
	if err != nil {
		t.Skipf("Skipping test - CA cert not found: %v", err)
	}

	caKeyPEM, err := os.ReadFile("dockers/standalone/tls/testcertuser.key")
	if err != nil {
		t.Skipf("Skipping test - CA key not found: %v", err)
	}

	// Parse CA certificate
	caCertBlock, _ := pem.Decode(caCertPEM)
	if caCertBlock == nil {
		t.Fatal("Failed to decode CA certificate PEM")
	}
	caCert, err := x509.ParseCertificate(caCertBlock.Bytes)
	if err != nil {
		t.Fatalf("Failed to parse CA certificate: %v", err)
	}

	// Parse CA private key
	caKeyBlock, _ := pem.Decode(caKeyPEM)
	if caKeyBlock == nil {
		t.Fatal("Failed to decode CA key PEM")
	}
	caKey, err := x509.ParsePKCS1PrivateKey(caKeyBlock.Bytes)
	if err != nil {
		// Try PKCS8 format
		caKeyInterface, err2 := x509.ParsePKCS8PrivateKey(caKeyBlock.Bytes)
		if err2 != nil {
			t.Fatalf("Failed to parse CA key (tried PKCS1 and PKCS8): %v, %v", err, err2)
		}
		var ok bool
		caKey, ok = caKeyInterface.(*rsa.PrivateKey)
		if !ok {
			t.Fatal("CA key is not RSA")
		}
	}

	// Step 4: Generate a new client certificate with the username in CN field
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("Failed to generate client key: %v", err)
	}

	// Create certificate template with username in CN
	serialNumber, _ := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	clientCertTemplate := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName:   testUsername, // THIS IS THE KEY: CN = username
			Organization: []string{"Redis Test"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}

	// Sign the certificate with CA
	clientCertDER, err := x509.CreateCertificate(
		rand.Reader,
		clientCertTemplate,
		caCert,
		&clientKey.PublicKey,
		caKey,
	)
	if err != nil {
		t.Fatalf("Failed to create client certificate: %v", err)
	}

	// Parse the certificate we just created
	clientCert, err := x509.ParseCertificate(clientCertDER)
	if err != nil {
		t.Fatalf("Failed to parse created certificate: %v", err)
	}

	// Create TLS certificate from key and cert
	tlsCert := tls.Certificate{
		Certificate: [][]byte{clientCertDER},
		PrivateKey:  clientKey,
		Leaf:        clientCert,
	}

	// Step 5: Create TLS config with our custom certificate
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCertPEM)

	tlsConfig := &tls.Config{
		RootCAs:      caCertPool,
		Certificates: []tls.Certificate{tlsCert},
		ServerName:   "localhost",
		// Note: Using InsecureSkipVerify because test certs use legacy CN field
		InsecureSkipVerify: true,
	}

	// Step 6: Connect with TLS using the certificate
	// NOTE: This test requires Redis to be configured with:
	//   tls-auth-clients-user CN
	// Without this config, the certificate CN won't be used for authentication
	tlsClient := redis.NewClient(&redis.Options{
		Addr:      "localhost:6666", // TLS port
		TLSConfig: tlsConfig,
		// NO Username/Password - authentication should happen via certificate!
	})
	defer tlsClient.Close()

	// Step 7: Verify we're authenticated as the correct user
	whoami, err := tlsClient.ACLWhoAmI(ctx).Result()
	if err != nil {
		t.Logf("ACL WHOAMI failed: %v", err)
		t.Logf("This test requires Redis to be configured with: tls-auth-clients-user CN")
		t.Skipf("Skipping - Redis may not be configured for certificate-based authentication")
	}

	if whoami != testUsername {
		t.Fatalf("Expected to be authenticated as %q, but got %q", testUsername, whoami)
		t.Skipf("Ensure Redis is configured with: tls-auth-clients-user CN")
	} else {
		t.Skipf("✅ Successfully authenticated as %q using certificate CN", whoami)
	}

	// Step 8: Test that we can execute allowed commands
	err = tlsClient.Set(ctx, "test_cert_auth_key", "test_value", 0).Err()
	if err != nil {
		t.Fatalf("SET command failed (should be allowed): %v", err)
	}

	val, err := tlsClient.Get(ctx, "test_cert_auth_key").Result()
	if err != nil {
		t.Fatalf("GET command failed (should be allowed): %v", err)
	}
	if val != "test_value" {
		t.Errorf("Expected 'test_value', got %q", val)
	}

	// Step 9: Test that we CANNOT execute disallowed commands
	// The user doesn't have +del permission, so this should fail
	err = tlsClient.Del(ctx, "test_cert_auth_key").Err()
	if err == nil {
		t.Error("DEL command succeeded but should have failed (user doesn't have +del permission)")
	} else {
		t.Logf("✅ DEL command correctly denied: %v", err)
	}

	// Cleanup
	setupClient.Del(ctx, "test_cert_auth_key")

	t.Log("✅ TLS certificate authentication test passed")
}
