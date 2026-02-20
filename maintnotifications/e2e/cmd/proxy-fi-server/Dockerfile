# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the proxy-fi-server binary
RUN cd maintnotifications/e2e/cmd/proxy-fi-server && \
    CGO_ENABLED=0 GOOS=linux go build -o /proxy-fi-server .

# Runtime stage
FROM alpine:latest

RUN apk --no-cache add ca-certificates

# Create a non-root user
RUN addgroup -g 1000 appuser && \
    adduser -D -u 1000 -G appuser appuser

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /proxy-fi-server .

# Change ownership of the app directory
RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser

# Expose the fault injector API port
EXPOSE 5000

# Run the server
ENTRYPOINT ["/app/proxy-fi-server"]
CMD ["--listen", "0.0.0.0:5000", "--proxy-api-url", "http://cae-resp-proxy:3000"]

