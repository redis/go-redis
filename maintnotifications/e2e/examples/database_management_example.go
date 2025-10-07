package examples

import (
	"context"
	"fmt"
	"time"

	e2e "github.com/redis/go-redis/v9/maintnotifications/e2e"
)

// ExampleCreateDatabase demonstrates how to create a database using the fault injector
func ExampleCreateDatabase() {
	ctx := context.Background()
	faultInjector := e2e.NewFaultInjectorClient("http://127.0.0.1:20324")

	// Example 1: Create a database using the DatabaseConfig struct
	dbConfig := e2e.DatabaseConfig{
		Name:           "ioredis-cluster",
		Port:           11112,
		MemorySize:     1273741824, // ~1.2GB
		Replication:    true,
		EvictionPolicy: "noeviction",
		Sharding:       true,
		AutoUpgrade:    true,
		ShardsCount:    3,
		ModuleList: []e2e.DatabaseModule{
			{ModuleArgs: "", ModuleName: "ReJSON"},
			{ModuleArgs: "", ModuleName: "search"},
			{ModuleArgs: "", ModuleName: "timeseries"},
			{ModuleArgs: "", ModuleName: "bf"},
		},
		OSSCluster:                   true,
		OSSClusterAPIPreferredIPType: "external",
		ProxyPolicy:                  "all-master-shards",
		ShardsPlacement:              "sparse",
		ShardKeyRegex: []e2e.ShardKeyRegexPattern{
			{Regex: ".*\\{(?<tag>.*)\\}.*"},
			{Regex: "(?<tag>.*)"},
		},
	}

	resp, err := faultInjector.CreateDatabase(ctx, 0, dbConfig)
	if err != nil {
		fmt.Printf("Failed to create database: %v\n", err)
		return
	}

	fmt.Printf("Database creation initiated. Action ID: %s\n", resp.ActionID)

	// Wait for the action to complete
	status, err := faultInjector.WaitForAction(ctx, resp.ActionID,
		e2e.WithMaxWaitTime(5*time.Minute),
		e2e.WithPollInterval(5*time.Second))
	if err != nil {
		fmt.Printf("Failed to wait for action: %v\n", err)
		return
	}

	fmt.Printf("Database creation status: %s\n", status.Status)
	if status.Error != nil {
		fmt.Printf("Error: %v\n", status.Error)
	}
}

// ExampleCreateDatabaseFromMap demonstrates how to create a database using a map
func ExampleCreateDatabaseFromMap() {
	ctx := context.Background()
	faultInjector := e2e.NewFaultInjectorClient("http://127.0.0.1:20324")

	// Example 2: Create a database using a map (more flexible)
	dbConfigMap := map[string]interface{}{
		"name":            "test-database",
		"port":            12000,
		"memory_size":     536870912, // 512MB
		"replication":     false,
		"eviction_policy": "volatile-lru",
		"sharding":        false,
		"auto_upgrade":    true,
		"shards_count":    1,
		"oss_cluster":     false,
	}

	resp, err := faultInjector.CreateDatabaseFromMap(ctx, 0, dbConfigMap)
	if err != nil {
		fmt.Printf("Failed to create database: %v\n", err)
		return
	}

	fmt.Printf("Database creation initiated. Action ID: %s\n", resp.ActionID)

	// Wait for the action to complete
	status, err := faultInjector.WaitForAction(ctx, resp.ActionID)
	if err != nil {
		fmt.Printf("Failed to wait for action: %v\n", err)
		return
	}

	fmt.Printf("Database creation status: %s\n", status.Status)
}

// ExampleDeleteDatabase demonstrates how to delete a database
func ExampleDeleteDatabase() {
	ctx := context.Background()
	faultInjector := e2e.NewFaultInjectorClient("http://127.0.0.1:20324")

	// Delete a database with cluster_index=0 and bdb_id=1
	resp, err := faultInjector.DeleteDatabase(ctx, 0, 1)
	if err != nil {
		fmt.Printf("Failed to delete database: %v\n", err)
		return
	}

	fmt.Printf("Database deletion initiated. Action ID: %s\n", resp.ActionID)

	// Wait for the action to complete
	status, err := faultInjector.WaitForAction(ctx, resp.ActionID,
		e2e.WithMaxWaitTime(2*time.Minute))
	if err != nil {
		fmt.Printf("Failed to wait for action: %v\n", err)
		return
	}

	fmt.Printf("Database deletion status: %s\n", status.Status)
	if status.Error != nil {
		fmt.Printf("Error: %v\n", status.Error)
	}
}

// ExampleCreateAndDeleteDatabase demonstrates a complete workflow
func ExampleCreateAndDeleteDatabase() {
	ctx := context.Background()
	faultInjector := e2e.NewFaultInjectorClient("http://127.0.0.1:20324")

	// Step 1: Create a database
	dbConfig := e2e.DatabaseConfig{
		Name:           "temp-test-db",
		Port:           13000,
		MemorySize:     268435456, // 256MB
		Replication:    false,
		EvictionPolicy: "noeviction",
		Sharding:       false,
		AutoUpgrade:    true,
		ShardsCount:    1,
		OSSCluster:     false,
	}

	createResp, err := faultInjector.CreateDatabase(ctx, 0, dbConfig)
	if err != nil {
		fmt.Printf("Failed to create database: %v\n", err)
		return
	}

	fmt.Printf("Creating database... Action ID: %s\n", createResp.ActionID)

	createStatus, err := faultInjector.WaitForAction(ctx, createResp.ActionID,
		e2e.WithMaxWaitTime(5*time.Minute))
	if err != nil {
		fmt.Printf("Failed to wait for database creation: %v\n", err)
		return
	}

	if createStatus.Status != e2e.StatusSuccess {
		fmt.Printf("Database creation failed: %v\n", createStatus.Error)
		return
	}

	fmt.Printf("Database created successfully!\n")

	// Extract the bdb_id from the output if available
	var bdbID int
	if output, ok := createStatus.Output["bdb_id"].(float64); ok {
		bdbID = int(output)
	} else {
		// If not in output, you might need to query or use a known ID
		bdbID = 1 // Example fallback
	}

	// Step 2: Do some work with the database
	fmt.Printf("Database is ready for testing (bdb_id: %d)\n", bdbID)
	time.Sleep(10 * time.Second) // Simulate some testing

	// Step 3: Delete the database
	deleteResp, err := faultInjector.DeleteDatabase(ctx, 0, bdbID)
	if err != nil {
		fmt.Printf("Failed to delete database: %v\n", err)
		return
	}

	fmt.Printf("Deleting database... Action ID: %s\n", deleteResp.ActionID)

	deleteStatus, err := faultInjector.WaitForAction(ctx, deleteResp.ActionID,
		e2e.WithMaxWaitTime(2*time.Minute))
	if err != nil {
		fmt.Printf("Failed to wait for database deletion: %v\n", err)
		return
	}

	if deleteStatus.Status != e2e.StatusSuccess {
		fmt.Printf("Database deletion failed: %v\n", deleteStatus.Error)
		return
	}

	fmt.Printf("Database deleted successfully!\n")
}

