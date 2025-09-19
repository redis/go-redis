package examples

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/maintnotifications/e2e"
)

// ConfigIntegrationExample demonstrates how to use the config parser with fault injection
func ConfigIntegrationExample() {
	ctx := context.Background()

	log.Println("=== Config Parser Integration Example ===")

	// Set up environment variables (normally these would be set externally)
	setupExampleEnvironment()

	// Example 1: Basic configuration parsing
	log.Println("\n1. Basic Configuration Parsing...")
	if err := demonstrateBasicConfigParsing(); err != nil {
		log.Printf("Basic config parsing failed: %v", err)
		return
	}

	// Example 2: Client factory usage
	log.Println("\n2. Client Factory Usage...")
	if err := demonstrateClientFactory(ctx); err != nil {
		log.Printf("Client factory demo failed: %v", err)
		return
	}

	// Example 3: Integration with fault injector
	log.Println("\n3. Fault Injector Integration...")
	if err := demonstrateFaultInjectorIntegration(ctx); err != nil {
		log.Printf("Fault injector integration failed: %v", err)
		return
	}

	// Example 4: Multi-database testing
	log.Println("\n4. Multi-Database Testing...")
	if err := demonstrateMultiDatabaseTesting(ctx); err != nil {
		log.Printf("Multi-database testing failed: %v", err)
		return
	}

	log.Println("\n=== All examples completed successfully ===")
}

func setupExampleEnvironment() {
	// Set environment variables for the example
	// In real usage, these would be set by the test runner or CI system

	// Get the current working directory and construct path to example config
	wd, _ := os.Getwd()
	configPath := fmt.Sprintf("%s/hitless/e2e/examples/endpoints.json", wd)

	os.Setenv("REDIS_ENDPOINTS_CONFIG_PATH", configPath)
	os.Setenv("FAULT_INJECTION_API_URL", "http://localhost:8080")

	log.Printf("Environment configured:")
	log.Printf("  REDIS_ENDPOINTS_CONFIG_PATH=%s", configPath)
	log.Printf("  FAULT_INJECTION_API_URL=http://localhost:8080")
}

func demonstrateBasicConfigParsing() error {
	// Get environment configuration
	envConfig, err := e2e.GetEnvConfig()
	if err != nil {
		return fmt.Errorf("failed to get environment config: %w", err)
	}

	log.Printf("Environment config loaded:")
	log.Printf("  Config path: %s", envConfig.RedisEndpointsConfigPath)
	log.Printf("  Fault injector URL: %s", envConfig.FaultInjectorURL)

	// Parse database configuration
	databasesConfig, err := e2e.GetDatabaseConfigFromEnv(envConfig.RedisEndpointsConfigPath)
	if err != nil {
		return fmt.Errorf("failed to parse database config: %w", err)
	}

	log.Printf("Found %d database configurations", len(databasesConfig))

	// List available databases
	databases, err := e2e.GetAvailableDatabases(envConfig.RedisEndpointsConfigPath)
	if err != nil {
		return fmt.Errorf("failed to get available databases: %w", err)
	}

	log.Printf("Available databases: %v", databases)

	// Parse specific database configurations
	for _, dbName := range []string{"standalone0", "cluster0", "enterprise-cluster"} {
		dbConfig, err := e2e.GetDatabaseConfig(databasesConfig, dbName)
		if err != nil {
			log.Printf("  Warning: Could not parse %s: %v", dbName, err)
			continue
		}

		log.Printf("Database '%s':", dbName)
		log.Printf("  Host: %s", dbConfig.Host)
		log.Printf("  Port: %d", dbConfig.Port)
		log.Printf("  Username: %s", dbConfig.Username)
		log.Printf("  TLS: %v", dbConfig.TLS)
		log.Printf("  Endpoints: %v", dbConfig.Endpoints)
		if dbConfig.BdbID > 0 {
			log.Printf("  BDB ID: %d", dbConfig.BdbID)
		}
	}

	return nil
}

func demonstrateClientFactory(ctx context.Context) error {
	// Create client factory for standalone Redis
	factory, err := e2e.CreateTestClientFactory("enterprise-cluster")
	if err != nil {
		return fmt.Errorf("failed to create client factory: %w", err)
	}
	defer factory.DestroyAll()

	log.Printf("Client factory created for database: standalone0")
	log.Printf("Configuration: %+v", factory.GetConfig())

	// Create multiple clients with different configurations
	clients := map[string]*e2e.CreateClientOptions{
		"basic-client": e2e.DefaultCreateClientOptions(),
		"hitless-client": {
			Protocol: 3,
			MaintNotificationsConfig: &maintnotifications.Config{
				Mode:           maintnotifications.ModeEnabled,
				HandoffTimeout: 45000, // 45 seconds
				RelaxedTimeout: 15000, // 15 seconds
				MaxWorkers:     30,
			},
			MaxRetries:   5,
			PoolSize:     20,
			MinIdleConns: 5,
			ClientName:   "hitless-test-client",
		},
		"performance-client": {
			Protocol:     3,
			MaxRetries:   10,
			PoolSize:     50,
			MinIdleConns: 10,
			ClientName:   "performance-test-client",
		},
	}

	for clientName, options := range clients {
		client, err := factory.Create(clientName, options)
		if err != nil {
			return fmt.Errorf("failed to create client %s: %w", clientName, err)
		}

		log.Printf("Created client '%s' with options:", clientName)
		log.Printf("  Protocol: %d", options.Protocol)
		log.Printf("  Pool size: %d", options.PoolSize)
		log.Printf("  Client name: %s", options.ClientName)

		// In a real test, you would connect and test the client
		// err = client.Ping(ctx).Err()
		// if err != nil {
		//     return fmt.Errorf("failed to ping with client %s: %w", clientName, err)
		// }

		_ = client // Suppress unused variable warning
	}

	// Demonstrate client retrieval
	basicClient := factory.Get("basic-client")
	if basicClient == nil {
		return fmt.Errorf("failed to retrieve basic-client")
	}

	allClients := factory.GetAll()
	log.Printf("Total clients created: %d", len(allClients))

	// Demonstrate individual client destruction
	if err := factory.Destroy("performance-client"); err != nil {
		log.Printf("Warning: Failed to destroy performance-client: %v", err)
	}

	log.Printf("Remaining clients after destroying performance-client: %d", len(factory.GetAll()))

	return nil
}

func demonstrateFaultInjectorIntegration(ctx context.Context) error {
	// Create fault injector client
	faultInjector, err := e2e.CreateTestFaultInjector()
	if err != nil {
		return fmt.Errorf("failed to create fault injector: %w", err)
	}

	log.Printf("Fault injector client created: %s", faultInjector.GetBaseURL())

	// Create Redis client factory for cluster testing
	factory, err := e2e.CreateTestClientFactory("enterprise-cluster")
	if err != nil {
		return fmt.Errorf("failed to create cluster client factory: %w", err)
	}
	defer factory.DestroyAll()

	// Create Redis client with hitless upgrade configuration
	client, err := factory.Create("cluster-client", &e2e.CreateClientOptions{
		Protocol: 3,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode:           maintnotifications.ModeEnabled,
			HandoffTimeout: 60000, // 60 seconds for cluster operations
			RelaxedTimeout: 20000, // 20 seconds
			MaxWorkers:     25,
		},
		ClientName: "fault-injection-test-client",
	})
	if err != nil {
		return fmt.Errorf("failed to create cluster client: %w", err)
	}

	log.Printf("Cluster client created with hitless upgrade configuration")

	// Simulate fault injection scenarios (these would work with actual infrastructure)
	scenarios := []struct {
		name        string
		description string
		action      func() error
	}{
		{
			name:        "cluster-failover",
			description: "Trigger cluster master failover",
			action: func() error {
				// resp, err := faultInjector.TriggerClusterFailover(ctx, "node-1", false)
				// if err != nil {
				//     return err
				// }
				// log.Printf("Failover triggered: %s", resp.ActionID)
				log.Printf("Simulated cluster failover trigger")
				return nil
			},
		},
		{
			name:        "network-latency",
			description: "Add network latency to cluster nodes",
			action: func() error {
				// nodes := factory.GetConfig().Endpoints
				// resp, err := faultInjector.SimulateNetworkLatency(ctx, nodes, 100*time.Millisecond, 20*time.Millisecond)
				// if err != nil {
				//     return err
				// }
				// log.Printf("Network latency applied: %s", resp.ActionID)
				log.Printf("Simulated network latency application")
				return nil
			},
		},
		{
			name:        "rolling-restart",
			description: "Simulate rolling cluster restart",
			action: func() error {
				// nodes := factory.GetConfig().Endpoints
				// resp, err := faultInjector.SimulateClusterUpgrade(ctx, nodes)
				// if err != nil {
				//     return err
				// }
				// log.Printf("Rolling restart triggered: %s", resp.ActionID)
				log.Printf("Simulated rolling restart trigger")
				return nil
			},
		},
	}

	for _, scenario := range scenarios {
		log.Printf("Executing scenario: %s - %s", scenario.name, scenario.description)

		if err := scenario.action(); err != nil {
			log.Printf("  Warning: Scenario %s failed: %v", scenario.name, err)
		} else {
			log.Printf("  ✓ Scenario %s completed successfully", scenario.name)
		}

		// In a real test, you would verify that the client continues to work
		// err = client.Ping(ctx).Err()
		// if err != nil {
		//     log.Printf("  Warning: Client ping failed after %s: %v", scenario.name, err)
		// } else {
		//     log.Printf("  ✓ Client remains responsive after %s", scenario.name)
		// }

		_ = client // Suppress unused variable warning
	}

	return nil
}

func demonstrateMultiDatabaseTesting(ctx context.Context) error {
	// Test multiple database configurations simultaneously
	databaseNames := []string{"standalone0", "cluster0"}

	factories := make(map[string]*e2e.ClientFactory)
	clients := make(map[string]interface{})

	// Create factories and clients for each database
	for _, dbName := range databaseNames {
		factory, err := e2e.CreateTestClientFactory("enterprise-cluster")
		if err != nil {
			log.Printf("Warning: Could not create factory for %s: %v", dbName, err)
			continue
		}

		factories[dbName] = factory

		// Create client with appropriate configuration
		var options *e2e.CreateClientOptions
		if dbName == "cluster0" {
			options = &e2e.CreateClientOptions{
				Protocol: 3,
				MaintNotificationsConfig: &maintnotifications.Config{
					Mode:           maintnotifications.ModeEnabled,
					HandoffTimeout: 60000,
					RelaxedTimeout: 20000,
					MaxWorkers:     30,
				},
				ClientName: fmt.Sprintf("multi-test-%s", dbName),
			}
		} else {
			options = e2e.DefaultCreateClientOptions()
			options.ClientName = fmt.Sprintf("multi-test-%s", dbName)
		}

		client, err := factory.Create("main", options)
		if err != nil {
			log.Printf("Warning: Could not create client for %s: %v", dbName, err)
			continue
		}

		clients[dbName] = client

		log.Printf("Created client for database '%s':", dbName)
		log.Printf("  Config: %+v", factory.GetConfig())
		log.Printf("  Client name: %s", options.ClientName)
	}

	// Clean up all factories
	defer func() {
		for dbName, factory := range factories {
			if err := factory.DestroyAll(); err != nil {
				log.Printf("Warning: Failed to destroy factory for %s: %v", dbName, err)
			}
		}
	}()

	// Simulate coordinated testing across multiple databases
	log.Printf("Simulating coordinated operations across %d databases", len(clients))

	for dbName := range clients {
		log.Printf("  Testing database: %s", dbName)

		// In a real test, you would perform operations on each client
		// client := clients[dbName]
		// err := client.Set(ctx, "test-key", "test-value", time.Minute).Err()
		// if err != nil {
		//     log.Printf("    Warning: Set operation failed: %v", err)
		// } else {
		//     log.Printf("    ✓ Set operation successful")
		// }

		log.Printf("    ✓ Simulated operations completed")
	}

	// Demonstrate fault injection across multiple databases
	faultInjector, err := e2e.CreateTestFaultInjector()
	if err != nil {
		return fmt.Errorf("failed to create fault injector: %w", err)
	}

	log.Printf("Simulating coordinated fault injection across databases")

	// Create a complex sequence that affects multiple database types
	sequence := []e2e.SequenceAction{
		{
			Type: e2e.ActionNetworkLatency,
			Parameters: map[string]interface{}{
				"nodes":   []string{"localhost:6379"}, // standalone
				"latency": "50ms",
			},
		},
		{
			Type: e2e.ActionNetworkLatency,
			Parameters: map[string]interface{}{
				"nodes":   []string{"localhost:7001", "localhost:7002"}, // cluster
				"latency": "100ms",
			},
			Delay: 10 * time.Second,
		},
		{
			Type: e2e.ActionNetworkRestore,
			Parameters: map[string]interface{}{
				"nodes": []string{"localhost:6379", "localhost:7001", "localhost:7002"},
			},
			Delay: 30 * time.Second,
		},
	}

	// resp, err := faultInjector.ExecuteSequence(ctx, sequence)
	// if err != nil {
	//     return fmt.Errorf("failed to execute coordinated sequence: %w", err)
	// }

	// log.Printf("Coordinated fault injection sequence triggered: %s", resp.ActionID)

	log.Printf("Simulated coordinated fault injection sequence")
	log.Printf("Sequence would include %d actions across multiple database types", len(sequence))

	// Use faultInjector to prevent unused variable error
	_ = faultInjector

	return nil
}

// Additional utility functions for advanced usage

func demonstrateAdvancedConfigParsing() error {
	log.Println("\n=== Advanced Configuration Parsing ===")

	// Example of parsing enterprise cluster configuration
	envConfig, err := e2e.GetEnvConfig()
	if err != nil {
		return err
	}

	databasesConfig, err := e2e.GetDatabaseConfigFromEnv(envConfig.RedisEndpointsConfigPath)
	if err != nil {
		return err
	}

	// Parse enterprise cluster with raw endpoints
	enterpriseConfig, err := e2e.GetDatabaseConfig(databasesConfig, "enterprise-cluster")
	if err != nil {
		log.Printf("Enterprise cluster config not available: %v", err)
		return nil
	}

	log.Printf("Enterprise cluster configuration:")
	log.Printf("  BDB ID: %d", enterpriseConfig.BdbID)
	log.Printf("  Host: %s", enterpriseConfig.Host)
	log.Printf("  Port: %d", enterpriseConfig.Port)
	log.Printf("  TLS: %v", enterpriseConfig.TLS)
	log.Printf("  Certificates: %s", enterpriseConfig.CertificatesLocation)
	log.Printf("  Endpoints: %v", enterpriseConfig.Endpoints)

	return nil
}

func demonstrateConfigValidation() error {
	log.Println("\n=== Configuration Validation ===")

	envConfig, err := e2e.GetEnvConfig()
	if err != nil {
		return err
	}

	databasesConfig, err := e2e.GetDatabaseConfigFromEnv(envConfig.RedisEndpointsConfigPath)
	if err != nil {
		return err
	}

	// Validate each database configuration
	for dbName := range databasesConfig {
		dbConfig, err := e2e.GetDatabaseConfig(databasesConfig, dbName)
		if err != nil {
			log.Printf("❌ Database %s: Invalid configuration - %v", dbName, err)
			continue
		}

		// Basic validation
		if dbConfig.Host == "" {
			log.Printf("❌ Database %s: Missing host", dbName)
			continue
		}

		if dbConfig.Port <= 0 || dbConfig.Port > 65535 {
			log.Printf("❌ Database %s: Invalid port %d", dbName, dbConfig.Port)
			continue
		}

		if len(dbConfig.Endpoints) == 0 {
			log.Printf("❌ Database %s: No endpoints defined", dbName)
			continue
		}

		log.Printf("✓ Database %s: Configuration valid", dbName)
	}

	return nil
}
