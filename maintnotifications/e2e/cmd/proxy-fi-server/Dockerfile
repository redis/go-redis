# Build stage
FROM golang:1.21-alpine AS builder

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

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /proxy-fi-server .

# Expose the fault injector API port
EXPOSE 5000

# Run the server
ENTRYPOINT ["/app/proxy-fi-server"]
CMD ["--listen", "0.0.0.0:5000", "--proxy-api-port", "3000"]

