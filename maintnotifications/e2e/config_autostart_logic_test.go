package e2e

import (
	"os"
	"testing"
)

// TestCreateTestFaultInjectorLogic_WithoutEnv verifies the auto-start logic
// when REDIS_ENDPOINTS_CONFIG_PATH is not set
func TestCreateTestFaultInjectorLogic_WithoutEnv(t *testing.T) {
	// Save original environment
	origConfigPath := os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH")
	origFIURL := os.Getenv("FAULT_INJECTION_API_URL")
	
	// Clear environment to simulate no setup
	os.Unsetenv("REDIS_ENDPOINTS_CONFIG_PATH")
	os.Unsetenv("FAULT_INJECTION_API_URL")
	
	// Restore environment after test
	defer func() {
		if origConfigPath != "" {
			os.Setenv("REDIS_ENDPOINTS_CONFIG_PATH", origConfigPath)
		}
		if origFIURL != "" {
			os.Setenv("FAULT_INJECTION_API_URL", origFIURL)
		}
	}()
	
	// Test GetEnvConfig - should fail when REDIS_ENDPOINTS_CONFIG_PATH is not set
	envConfig, err := GetEnvConfig()
	if err == nil {
		t.Fatal("Expected GetEnvConfig() to fail when REDIS_ENDPOINTS_CONFIG_PATH is not set")
	}
	if envConfig != nil {
		t.Fatal("Expected envConfig to be nil when GetEnvConfig() fails")
	}
	
	t.Log("✅ GetEnvConfig() correctly fails when REDIS_ENDPOINTS_CONFIG_PATH is not set")
	t.Log("✅ This means CreateTestFaultInjectorWithCleanup() will auto-start the proxy")
}

// TestCreateTestFaultInjectorLogic_WithEnv verifies the logic
// when REDIS_ENDPOINTS_CONFIG_PATH is set
func TestCreateTestFaultInjectorLogic_WithEnv(t *testing.T) {
	// Create a temporary config file
	tmpFile := "/tmp/test_endpoints.json"
	content := `{
		"standalone": {
			"endpoints": ["redis://localhost:6379"]
		}
	}`
	
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile)
	
	// Save original environment
	origConfigPath := os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH")
	origFIURL := os.Getenv("FAULT_INJECTION_API_URL")
	
	// Set environment
	os.Setenv("REDIS_ENDPOINTS_CONFIG_PATH", tmpFile)
	os.Setenv("FAULT_INJECTION_API_URL", "http://test-fi:9999")
	
	// Restore environment after test
	defer func() {
		if origConfigPath != "" {
			os.Setenv("REDIS_ENDPOINTS_CONFIG_PATH", origConfigPath)
		} else {
			os.Unsetenv("REDIS_ENDPOINTS_CONFIG_PATH")
		}
		if origFIURL != "" {
			os.Setenv("FAULT_INJECTION_API_URL", origFIURL)
		} else {
			os.Unsetenv("FAULT_INJECTION_API_URL")
		}
	}()
	
	// Test GetEnvConfig - should succeed when REDIS_ENDPOINTS_CONFIG_PATH is set
	envConfig, err := GetEnvConfig()
	if err != nil {
		t.Fatalf("Expected GetEnvConfig() to succeed when REDIS_ENDPOINTS_CONFIG_PATH is set, got error: %v", err)
	}
	if envConfig == nil {
		t.Fatal("Expected envConfig to be non-nil when GetEnvConfig() succeeds")
	}
	
	// Verify the fault injector URL is correct
	if envConfig.FaultInjectorURL != "http://test-fi:9999" {
		t.Errorf("Expected FaultInjectorURL to be 'http://test-fi:9999', got '%s'", envConfig.FaultInjectorURL)
	}
	
	t.Log("✅ GetEnvConfig() correctly succeeds when REDIS_ENDPOINTS_CONFIG_PATH is set")
	t.Log("✅ This means CreateTestFaultInjectorWithCleanup() will use the real fault injector")
	t.Logf("✅ Fault injector URL: %s", envConfig.FaultInjectorURL)
}

// TestCreateTestFaultInjectorLogic_DefaultFIURL verifies the default fault injector URL
func TestCreateTestFaultInjectorLogic_DefaultFIURL(t *testing.T) {
	// Create a temporary config file
	tmpFile := "/tmp/test_endpoints2.json"
	content := `{
		"standalone": {
			"endpoints": ["redis://localhost:6379"]
		}
	}`
	
	if err := os.WriteFile(tmpFile, []byte(content), 0644); err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(tmpFile)
	
	// Save original environment
	origConfigPath := os.Getenv("REDIS_ENDPOINTS_CONFIG_PATH")
	origFIURL := os.Getenv("FAULT_INJECTION_API_URL")
	
	// Set only config path, not fault injector URL
	os.Setenv("REDIS_ENDPOINTS_CONFIG_PATH", tmpFile)
	os.Unsetenv("FAULT_INJECTION_API_URL")
	
	// Restore environment after test
	defer func() {
		if origConfigPath != "" {
			os.Setenv("REDIS_ENDPOINTS_CONFIG_PATH", origConfigPath)
		} else {
			os.Unsetenv("REDIS_ENDPOINTS_CONFIG_PATH")
		}
		if origFIURL != "" {
			os.Setenv("FAULT_INJECTION_API_URL", origFIURL)
		}
	}()
	
	// Test GetEnvConfig - should succeed and use default FI URL
	envConfig, err := GetEnvConfig()
	if err != nil {
		t.Fatalf("Expected GetEnvConfig() to succeed, got error: %v", err)
	}
	
	// Verify the default fault injector URL
	if envConfig.FaultInjectorURL != "http://localhost:8080" {
		t.Errorf("Expected default FaultInjectorURL to be 'http://localhost:8080', got '%s'", envConfig.FaultInjectorURL)
	}
	
	t.Log("✅ GetEnvConfig() uses default fault injector URL when FAULT_INJECTION_API_URL is not set")
	t.Logf("✅ Default fault injector URL: %s", envConfig.FaultInjectorURL)
}

// TestFaultInjectorClientCreation verifies that FaultInjectorClient can be created
func TestFaultInjectorClientCreation(t *testing.T) {
	// Test creating client with different URLs
	testCases := []struct {
		name string
		url  string
	}{
		{"localhost", "http://localhost:15000"}, // Updated to avoid macOS Control Center conflict
		{"with port", "http://test:9999"},
		{"with trailing slash", "http://test:9999/"},
	}
	
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			client := NewFaultInjectorClient(tc.url)
			if client == nil {
				t.Fatal("Expected non-nil client")
			}
			
			// Verify the base URL (should have trailing slash removed)
			expectedURL := tc.url
			if expectedURL[len(expectedURL)-1] == '/' {
				expectedURL = expectedURL[:len(expectedURL)-1]
			}
			
			if client.GetBaseURL() != expectedURL {
				t.Errorf("Expected base URL '%s', got '%s'", expectedURL, client.GetBaseURL())
			}
			
			t.Logf("✅ Client created successfully with URL: %s", client.GetBaseURL())
		})
	}
}

