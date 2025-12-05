package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	e2e "github.com/redis/go-redis/v9/maintnotifications/e2e"
)

func main() {
	listenAddr := flag.String("listen", "0.0.0.0:5000", "Address to listen on for fault injector API")
	proxyAPIURL := flag.String("proxy-api-url", "http://localhost:18100", "URL of the cae-resp-proxy API (updated to avoid macOS Control Center conflict)")
	flag.Parse()

	fmt.Printf("Starting Proxy Fault Injector Server...\n")
	fmt.Printf("  Listen address: %s\n", *listenAddr)
	fmt.Printf("  Proxy API URL: %s\n", *proxyAPIURL)

	server := e2e.NewProxyFaultInjectorServerWithURL(*listenAddr, *proxyAPIURL)
	if err := server.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to start server: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Proxy Fault Injector Server started successfully\n")
	fmt.Printf("Fault Injector API available at http://%s\n", *listenAddr)

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	<-sigChan

	fmt.Println("\nShutting down...")
	if err := server.Stop(); err != nil {
		fmt.Fprintf(os.Stderr, "Error during shutdown: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("Server stopped")
}

