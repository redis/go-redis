# TLS Certificate Authentication Example

This example demonstrates how to use TLS client certificates for automatic authentication with Redis 8.6+.

When Redis is configured with `tls-auth-clients-user CN`, it uses the Common Name (CN) field from the client certificate as the username, eliminating the need for password-based authentication.

## Prerequisites

- Redis 8.6+ with TLS enabled
- Redis configured with: `tls-auth-clients-user CN`
- Client certificate with CN matching a Redis ACL user
- The ACL user must exist and have `nopass` set

## How It Works

1. **Load CA certificate** - Used to verify the Redis server's certificate
2. **Load client certificate** - The CN field must match a Redis ACL username
3. **Connect with TLS** - No username/password needed in the connection options
4. **Redis authenticates automatically** - Based on the certificate's CN field

## Running the Example

```bash
# Start Redis with TLS (from the go-redis root directory)
docker compose --profile standalone up -d

# Run the example
cd example/tls-cert-auth
go run main.go
```

## Expected Output

```
✅ Authenticated as: testcertuser (via TLS certificate CN)
✅ SET/GET successful: hello from cert auth!

🎉 TLS certificate authentication working!
```

## Docker Configuration

The go-redis test environment is configured with these environment variables:

```yaml
environment:
  - TLS_ENABLED=yes
  - TLS_CLIENT_CNS=testcertuser      # Generates testcertuser.{crt,key}
  - TLS_AUTH_CLIENTS_USER=CN         # Enables CN-based authentication
```

## Key Code

```go
// Load client certificate (CN must match Redis ACL username)
clientCert, err := tls.LoadX509KeyPair(
    "testcertuser.crt",
    "testcertuser.key",
)

// Create TLS config
tlsConfig := &tls.Config{
    RootCAs:      caCertPool,
    Certificates: []tls.Certificate{clientCert},
}

// Connect - NO username/password needed!
client := redis.NewClient(&redis.Options{
    Addr:      "localhost:6666",
    TLSConfig: tlsConfig,
})
```

## Fallback Behavior

If the certificate CN doesn't match any existing ACL user, Redis falls back to the `default` user. See `tls_cert_auth_test.go` for tests covering both scenarios.

