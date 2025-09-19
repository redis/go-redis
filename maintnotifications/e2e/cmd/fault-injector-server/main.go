package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9/maintnotifications/e2e"
)

var (
	version = "dev"
	commit  = "unknown"
)

func main() {
	var (
		port     = flag.String("port", getEnv("FAULT_INJECTOR_PORT", "8080"), "Server port")
		host     = flag.String("host", getEnv("FAULT_INJECTOR_HOST", "0.0.0.0"), "Server host")
		logLevel = flag.String("log-level", getEnv("FAULT_INJECTOR_LOG", "info"), "Log level")
		showVer  = flag.Bool("version", false, "Show version information")
	)
	flag.Parse()

	if *showVer {
		fmt.Printf("Fault Injector Server\n")
		fmt.Printf("Version: %s\n", version)
		fmt.Printf("Commit: %s\n", commit)
		os.Exit(0)
	}

	// Validate port
	portNum, err := strconv.Atoi(*port)
	if err != nil || portNum < 1 || portNum > 65535 {
		log.Fatalf("Invalid port: %s", *port)
	}

	addr := fmt.Sprintf("%s:%s", *host, *port)

	// Create and configure server
	server := e2e.NewMockFaultInjectorServer(addr)

	// Register additional handlers if needed
	registerCustomHandlers(server)

	// Start server in a goroutine
	go func() {
		log.Printf("Starting Fault Injector Server v%s on %s", version, addr)
		log.Printf("Log level: %s", *logLevel)

		if err := server.Start(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server failed to start: %v", err)
		}
	}()

	// Wait for interrupt signal to gracefully shutdown the server
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down server...")

	// Give the server 30 seconds to finish handling requests
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Stop(ctx); err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	} else {
		log.Println("Server shutdown complete")
	}
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func registerCustomHandlers(server *e2e.MockFaultInjectorServer) {
	// Register Docker-based handlers for real infrastructure control
	server.RegisterHandler(e2e.ActionNodeRestart, &e2e.DockerNodeHandler{})
	server.RegisterHandler(e2e.ActionNodeStop, &e2e.DockerNodeHandler{})
	server.RegisterHandler(e2e.ActionNodeStart, &e2e.DockerNodeHandler{})
	server.RegisterHandler(e2e.ActionNodeKill, &e2e.DockerNodeHandler{})

	// Register network control handlers
	server.RegisterHandler(e2e.ActionNetworkPartition, &e2e.NetworkControlHandler{})
	server.RegisterHandler(e2e.ActionNetworkLatency, &e2e.NetworkControlHandler{})
	server.RegisterHandler(e2e.ActionNetworkPacketLoss, &e2e.NetworkControlHandler{})
	server.RegisterHandler(e2e.ActionNetworkBandwidth, &e2e.NetworkControlHandler{})
	server.RegisterHandler(e2e.ActionNetworkRestore, &e2e.NetworkControlHandler{})

	log.Println("Custom handlers registered successfully")
}
