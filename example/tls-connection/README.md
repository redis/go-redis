# TLS Connection Examples

Shows different ways to connect to Redis over TLS.

## Running

Start Redis with TLS:

```shell
cd ../..
docker compose --profile standalone up -d
```

Then run the example:

```shell
go run .
```

## Connection Methods

### 1. InsecureSkipVerify (for testing)

Quick way to test with self-signed certs:

```go
client := redis.NewClient(&redis.Options{
    Addr: "localhost:6666",
    TLSConfig: &tls.Config{
        InsecureSkipVerify: true,
    },
})
```

Don't use this in production.

### 2. With CA certificate

Proper way for production:

```go
caCert, _ := os.ReadFile("path/to/ca.crt")
caCertPool := x509.NewCertPool()
caCertPool.AppendCertsFromPEM(caCert)

client := redis.NewClient(&redis.Options{
    Addr: "localhost:6666",
    TLSConfig: &tls.Config{
        RootCAs:    caCertPool,
        ServerName: "localhost",
    },
})
```

### 3. Mutual TLS

If Redis requires client certs:

```go
caCert, _ := os.ReadFile("path/to/ca.crt")
caCertPool := x509.NewCertPool()
caCertPool.AppendCertsFromPEM(caCert)

cert, _ := tls.LoadX509KeyPair("path/to/client.crt", "path/to/client.key")

client := redis.NewClient(&redis.Options{
    Addr: "localhost:6666",
    TLSConfig: &tls.Config{
        RootCAs:      caCertPool,
        Certificates: []tls.Certificate{cert},
        ServerName:   "localhost",
    },
})
```

### 4. Using rediss:// URL

```go
opt, _ := redis.ParseURL("rediss://localhost:6666")
opt.TLSConfig = &tls.Config{
    InsecureSkipVerify: true, // for testing only
}
client := redis.NewClient(opt)
```

### 5. Certificate-based auth

Redis 6.2+ can authenticate users based on the certificate CN field. You need to configure Redis with:

```
tls-auth-clients optional
tls-auth-clients-user CN
```

Then the CN in your client cert becomes your username - no password needed.

Check `../../tls_cert_auth_test.go` for a working example that:
- Generates a client cert with a specific CN
- Connects to Redis with that cert
- Verifies auth based on the CN

Note: Current Redis test build doesn't support this yet, so the test skips gracefully.

## Tests

Run the TLS tests:

```shell
go test -v -run "^TestTLS" -timeout 30s
```

## Notes

- Always verify certs in production (don't use InsecureSkipVerify)
- Keep your private keys safe
- Use TLS 1.2 or higher

