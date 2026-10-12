// TLS Certificate Authentication Example
//
// This example demonstrates how to use TLS client certificates for
// automatic authentication with Redis 8.6+.
//
// When Redis is configured with `tls-auth-clients-user CN`, it uses
// the Common Name (CN) field from the client certificate as the username,
// eliminating the need for password-based authentication.
//
// Prerequisites:
//   - Redis 8.6+ with TLS enabled
//   - Redis configured with: tls-auth-clients-user CN
//   - Client certificate with CN matching a Redis ACL user
//   - The ACL user must exist
//
// To run with the go-redis test environment:
//
//	docker compose --profile standalone up -d
//	go run main.go
package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"os"

	"github.com/redis/go-redis/v9"
)

func main() {
	ctx := context.Background()

	// Configuration - adjust paths as needed
	certDir := "../../dockers/standalone/tls"
	username := "testcertuser" // Must match CN in certificate and ACL user
	tlsPort := "6666"
	nonTLSPort := "6379"

	// Step 1: First, ensure the ACL user exists (using non-TLS connection)
	setupClient := redis.NewClient(&redis.Options{
		Addr: "localhost:" + nonTLSPort,
	})
	defer setupClient.Close()

	// Create the ACL user if it doesn't exist
	err := setupClient.ACLSetUser(ctx,
		username,
		"on",     // Enable the user
		"nopass", // No password - will use cert auth
		"~*",     // Access all keys
		"+@all",  // All commands (adjust as needed)
	).Err()
	if err != nil {
		log.Printf("Note: Could not create ACL user (may already exist): %v", err)
	}

	// Step 2: Load CA certificate
	caCert, err := os.ReadFile(certDir + "/ca.crt")
	if err != nil {
		log.Fatalf("Failed to load CA certificate: %v", err)
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	// Step 3: Load client certificate (CN must match the Redis ACL username)
	clientCert, err := tls.LoadX509KeyPair(
		certDir+"/"+username+".crt",
		certDir+"/"+username+".key",
	)
	if err != nil {
		log.Fatalf("Failed to load client certificate: %v", err)
	}

	// Step 4: Create TLS config
	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		Certificates:       []tls.Certificate{clientCert},
		ServerName:         "localhost",
		InsecureSkipVerify: true, // Only for self-signed certs in testing
	}

	// Step 5: Connect to Redis with TLS - NO username/password needed!
	client := redis.NewClient(&redis.Options{
		Addr:      "localhost:" + tlsPort,
		TLSConfig: tlsConfig,
		// Note: No Username or Password fields - auth happens via certificate
	})
	defer client.Close()

	// Step 6: Verify authentication
	whoami, err := client.ACLWhoAmI(ctx).Result()
	if err != nil {
		log.Fatalf("Failed to get current user: %v", err)
	}
	fmt.Printf("✅ Authenticated as: %s (via TLS certificate CN)\n", whoami)

	// Step 7: Test some commands
	err = client.Set(ctx, "tls-auth-example", "hello from cert auth!", 0).Err()
	if err != nil {
		log.Fatalf("SET failed: %v", err)
	}

	val, err := client.Get(ctx, "tls-auth-example").Result()
	if err != nil {
		log.Fatalf("GET failed: %v", err)
	}
	fmt.Printf("✅ SET/GET successful: %s\n", val)

	// Cleanup
	client.Del(ctx, "tls-auth-example")

	fmt.Println("\n🎉 TLS certificate authentication working!")
}
