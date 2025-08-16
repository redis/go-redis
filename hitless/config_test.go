package hitless

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/util"
)

func TestConfig(t *testing.T) {
	t.Run("DefaultConfig", func(t *testing.T) {
		config := DefaultConfig()

		// MinWorkers and MaxWorkers should be 0 in default config (auto-calculated)
		if config.MinWorkers != 0 {
			t.Errorf("Expected MinWorkers to be 0 (auto-calculated), got %d", config.MinWorkers)
		}
		if config.MaxWorkers != 0 {
			t.Errorf("Expected MaxWorkers to be 0 (auto-calculated), got %d", config.MaxWorkers)
		}

		// HandoffQueueSize should be 0 in default config (auto-calculated)
		if config.HandoffQueueSize != 0 {
			t.Errorf("Expected HandoffQueueSize to be 0 (auto-calculated), got %d", config.HandoffQueueSize)
		}

		if config.RelaxedTimeout != 30*time.Second {
			t.Errorf("Expected RelaxedTimeout to be 30s, got %v", config.RelaxedTimeout)
		}

		// Test new configuration fields have proper defaults
		if config.MaxHandoffRetries != 3 {
			t.Errorf("Expected MaxHandoffRetries to be 3, got %d", config.MaxHandoffRetries)
		}
		if config.HandoffQueueTimeout != 5*time.Second {
			t.Errorf("Expected HandoffQueueTimeout to be 5s, got %v", config.HandoffQueueTimeout)
		}
		if config.HandoffRetryDelay != 1*time.Second {
			t.Errorf("Expected HandoffRetryDelay to be 1s, got %v", config.HandoffRetryDelay)
		}
		if config.WorkerScaleDownDelay != 30*time.Second {
			t.Errorf("Expected WorkerScaleDownDelay to be 30s, got %v", config.WorkerScaleDownDelay)
		}
		if config.MaxActiveOperations != 10000 {
			t.Errorf("Expected MaxActiveOperations to be 10000, got %d", config.MaxActiveOperations)
		}

		if config.HandoffTimeout != 15*time.Second {
			t.Errorf("Expected HandoffTimeout to be 15s, got %v", config.HandoffTimeout)
		}

		if config.PostHandoffRelaxedDuration != 0 {
			t.Errorf("Expected PostHandoffRelaxedDuration to be 0 (auto-calculated), got %v", config.PostHandoffRelaxedDuration)
		}

		// Test that defaults are applied correctly
		configWithDefaults := config.ApplyDefaultsWithPoolSize(100)
		if configWithDefaults.PostHandoffRelaxedDuration != 60*time.Second {
			t.Errorf("Expected PostHandoffRelaxedDuration to be 60s (2x RelaxedTimeout) after applying defaults, got %v", configWithDefaults.PostHandoffRelaxedDuration)
		}
	})

	t.Run("ConfigValidation", func(t *testing.T) {
		// Valid config with applied defaults
		config := DefaultConfig().ApplyDefaults()
		if err := config.Validate(); err != nil {
			t.Errorf("Default config with applied defaults should be valid: %v", err)
		}

		// Invalid worker configuration (negative MinWorkers)
		config = &Config{
			RelaxedTimeout:             30 * time.Second,
			HandoffTimeout:             15 * time.Second,
			MinWorkers:                 -1, // This should be invalid
			MaxWorkers:                 5,
			HandoffQueueSize:           100,
			PostHandoffRelaxedDuration: 10 * time.Second,
			LogLevel:                   1,
			MaxHandoffRetries:          3, // Add required field
		}
		if err := config.Validate(); err != ErrInvalidHandoffWorkers {
			t.Errorf("Expected ErrInvalidHandoffWorkers, got %v", err)
		}

		// Invalid worker range (MaxWorkers < MinWorkers)
		config = DefaultConfig()
		config.MinWorkers = 5
		config.MaxWorkers = 2
		if err := config.Validate(); err != ErrInvalidWorkerRange {
			t.Errorf("Expected ErrInvalidWorkerRange, got %v", err)
		}

		// Invalid HandoffQueueSize
		config = DefaultConfig().ApplyDefaults()
		config.HandoffQueueSize = -1
		if err := config.Validate(); err != ErrInvalidHandoffQueueSize {
			t.Errorf("Expected ErrInvalidHandoffQueueSize, got %v", err)
		}

		// Invalid PostHandoffRelaxedDuration
		config = DefaultConfig().ApplyDefaults()
		config.PostHandoffRelaxedDuration = -1 * time.Second
		if err := config.Validate(); err != ErrInvalidPostHandoffRelaxedDuration {
			t.Errorf("Expected ErrInvalidPostHandoffRelaxedDuration, got %v", err)
		}
	})

	t.Run("ConfigClone", func(t *testing.T) {
		original := DefaultConfig()
		original.MinWorkers = 5
		original.MaxWorkers = 20
		original.HandoffQueueSize = 200

		cloned := original.Clone()

		if cloned.MinWorkers != 5 {
			t.Errorf("Expected cloned MinWorkers to be 5, got %d", cloned.MinWorkers)
		}

		if cloned.MaxWorkers != 20 {
			t.Errorf("Expected cloned MaxWorkers to be 20, got %d", cloned.MaxWorkers)
		}

		if cloned.HandoffQueueSize != 200 {
			t.Errorf("Expected cloned HandoffQueueSize to be 200, got %d", cloned.HandoffQueueSize)
		}

		// Modify original to ensure clone is independent
		original.MinWorkers = 2
		if cloned.MinWorkers != 5 {
			t.Error("Clone should be independent of original")
		}
	})
}

func TestApplyDefaults(t *testing.T) {
	t.Run("NilConfig", func(t *testing.T) {
		var config *Config
		result := config.ApplyDefaultsWithPoolSize(100) // Use explicit pool size for testing

		// With nil config, should get default config with auto-calculated workers
		if result.MinWorkers <= 0 {
			t.Errorf("Expected MinWorkers to be > 0 after applying defaults, got %d", result.MinWorkers)
		}
		if result.MaxWorkers <= 0 {
			t.Errorf("Expected MaxWorkers to be > 0 after applying defaults, got %d", result.MaxWorkers)
		}
		if result.MaxWorkers < result.MinWorkers {
			t.Errorf("Expected MaxWorkers (%d) >= MinWorkers (%d)", result.MaxWorkers, result.MinWorkers)
		}

		// HandoffQueueSize should be auto-calculated (10 * MaxWorkers, capped by pool size)
		workerBasedSize := result.MaxWorkers * 10
		poolSize := 100 // Default pool size used in ApplyDefaults
		expectedQueueSize := util.Min(workerBasedSize, poolSize)
		if result.HandoffQueueSize != expectedQueueSize {
			t.Errorf("Expected HandoffQueueSize to be %d (util.Min(10*MaxWorkers=%d, poolSize=%d)), got %d",
				expectedQueueSize, workerBasedSize, poolSize, result.HandoffQueueSize)
		}
	})

	t.Run("PartialConfig", func(t *testing.T) {
		config := &Config{
			MinWorkers: 3,  // Set this field explicitly
			MaxWorkers: 12, // Set this field explicitly
			// Leave other fields as zero values
		}

		result := config.ApplyDefaultsWithPoolSize(100) // Use explicit pool size for testing

		// Should keep the explicitly set values
		if result.MinWorkers != 3 {
			t.Errorf("Expected MinWorkers to be 3 (explicitly set), got %d", result.MinWorkers)
		}
		if result.MaxWorkers != 12 {
			t.Errorf("Expected MaxWorkers to be 12 (explicitly set), got %d", result.MaxWorkers)
		}

		// Should apply default for unset fields (auto-calculated queue size, capped by pool size)
		workerBasedSize := result.MaxWorkers * 10
		poolSize := 100 // Default pool size used in ApplyDefaults
		expectedQueueSize := util.Min(workerBasedSize, poolSize)
		if result.HandoffQueueSize != expectedQueueSize {
			t.Errorf("Expected HandoffQueueSize to be %d (util.Min(10*MaxWorkers=%d, poolSize=%d)), got %d",
				expectedQueueSize, workerBasedSize, poolSize, result.HandoffQueueSize)
		}

		if result.RelaxedTimeout != 30*time.Second {
			t.Errorf("Expected RelaxedTimeout to be 30s (default), got %v", result.RelaxedTimeout)
		}

		if result.HandoffTimeout != 15*time.Second {
			t.Errorf("Expected HandoffTimeout to be 15s (default), got %v", result.HandoffTimeout)
		}
	})

	t.Run("ZeroValues", func(t *testing.T) {
		config := &Config{
			MinWorkers:       0, // Zero value should get auto-calculated defaults
			MaxWorkers:       0, // Zero value should get auto-calculated defaults
			HandoffQueueSize: 0, // Zero value should get default
			RelaxedTimeout:   0, // Zero value should get default
			LogLevel:         0, // Zero is valid for LogLevel (errors only)
		}

		result := config.ApplyDefaultsWithPoolSize(100) // Use explicit pool size for testing

		// Zero values should get auto-calculated defaults
		if result.MinWorkers <= 0 {
			t.Errorf("Expected MinWorkers to be > 0 (auto-calculated), got %d", result.MinWorkers)
		}
		if result.MaxWorkers <= 0 {
			t.Errorf("Expected MaxWorkers to be > 0 (auto-calculated), got %d", result.MaxWorkers)
		}

		// HandoffQueueSize should be auto-calculated (10 * MaxWorkers, capped by pool size)
		workerBasedSize := result.MaxWorkers * 10
		poolSize := 100 // Default pool size used in ApplyDefaults
		expectedQueueSize := util.Min(workerBasedSize, poolSize)
		if result.HandoffQueueSize != expectedQueueSize {
			t.Errorf("Expected HandoffQueueSize to be %d (util.Min(10*MaxWorkers=%d, poolSize=%d)), got %d",
				expectedQueueSize, workerBasedSize, poolSize, result.HandoffQueueSize)
		}

		if result.RelaxedTimeout != 30*time.Second {
			t.Errorf("Expected RelaxedTimeout to be 30s (default), got %v", result.RelaxedTimeout)
		}

		// LogLevel 0 should be preserved (it's a valid value)
		if result.LogLevel != 0 {
			t.Errorf("Expected LogLevel to be 0 (preserved), got %d", result.LogLevel)
		}
	})
}

func TestProcessorWithConfig(t *testing.T) {
	t.Run("ProcessorUsesConfigValues", func(t *testing.T) {
		config := &Config{
			MinWorkers:       2,
			MaxWorkers:       5,
			HandoffQueueSize: 50,
			RelaxedTimeout:   10 * time.Second,
			HandoffTimeout:   5 * time.Second,
		}

		baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return &mockNetConn{addr: addr}, nil
		}

		processor := NewPoolHook(baseDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		// The processor should be created successfully with custom config
		if processor == nil {
			t.Error("Processor should be created with custom config")
		}
	})

	t.Run("ProcessorWithPartialConfig", func(t *testing.T) {
		config := &Config{
			MinWorkers: 3, // Only set worker fields
			MaxWorkers: 7,
			// Other fields will get defaults
		}

		baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return &mockNetConn{addr: addr}, nil
		}

		processor := NewPoolHook(baseDialer, "tcp", config, nil)
		defer processor.Shutdown(context.Background())

		// Should work with partial config (defaults applied)
		if processor == nil {
			t.Error("Processor should be created with partial config")
		}
	})

	t.Run("ProcessorWithNilConfig", func(t *testing.T) {
		baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return &mockNetConn{addr: addr}, nil
		}

		processor := NewPoolHook(baseDialer, "tcp", nil, nil)
		defer processor.Shutdown(context.Background())

		// Should use default config when nil is passed
		if processor == nil {
			t.Error("Processor should be created with nil config (using defaults)")
		}
	})
}

func TestIntegrationWithApplyDefaults(t *testing.T) {
	t.Run("ProcessorWithPartialConfigAppliesDefaults", func(t *testing.T) {
		// Create a partial config with only some fields set
		partialConfig := &Config{
			MinWorkers: 3, // Custom value
			MaxWorkers: 7, // Custom value
			LogLevel:   2, // Custom value
			// Other fields left as zero values - should get defaults
		}

		baseDialer := func(ctx context.Context, network, addr string) (net.Conn, error) {
			return &mockNetConn{addr: addr}, nil
		}

		// Create processor - should apply defaults to missing fields
		processor := NewPoolHook(baseDialer, "tcp", partialConfig, nil)
		defer processor.Shutdown(context.Background())

		// Processor should be created successfully
		if processor == nil {
			t.Error("Processor should be created with partial config")
		}

		// Test that the ApplyDefaults method worked correctly by creating the same config
		// and applying defaults manually
		expectedConfig := partialConfig.ApplyDefaultsWithPoolSize(100) // Use explicit pool size for testing

		// Should preserve custom values
		if expectedConfig.MinWorkers != 3 {
			t.Errorf("Expected MinWorkers to be 3, got %d", expectedConfig.MinWorkers)
		}

		if expectedConfig.MaxWorkers != 7 {
			t.Errorf("Expected MaxWorkers to be 7, got %d", expectedConfig.MaxWorkers)
		}

		if expectedConfig.LogLevel != 2 {
			t.Errorf("Expected LogLevel to be 2, got %d", expectedConfig.LogLevel)
		}

		// Should apply defaults for missing fields (auto-calculated queue size, capped by pool size)
		workerBasedSize := expectedConfig.MaxWorkers * 10
		poolSize := 100 // Default pool size used in ApplyDefaults
		expectedQueueSize := util.Min(workerBasedSize, poolSize)
		if expectedConfig.HandoffQueueSize != expectedQueueSize {
			t.Errorf("Expected HandoffQueueSize to be %d (util.Min(10*MaxWorkers=%d, poolSize=%d)), got %d",
				expectedQueueSize, workerBasedSize, poolSize, expectedConfig.HandoffQueueSize)
		}

		if expectedConfig.RelaxedTimeout != 30*time.Second {
			t.Errorf("Expected RelaxedTimeout to be 30s (default), got %v", expectedConfig.RelaxedTimeout)
		}

		if expectedConfig.HandoffTimeout != 15*time.Second {
			t.Errorf("Expected HandoffTimeout to be 15s (default), got %v", expectedConfig.HandoffTimeout)
		}

		if expectedConfig.PostHandoffRelaxedDuration != 60*time.Second {
			t.Errorf("Expected PostHandoffRelaxedDuration to be 60s (2x RelaxedTimeout), got %v", expectedConfig.PostHandoffRelaxedDuration)
		}
	})
}

func TestEnhancedConfigValidation(t *testing.T) {
	t.Run("ValidateNewFields", func(t *testing.T) {
		config := DefaultConfig()
		config.ApplyDefaultsWithPoolSize(100) // Apply defaults with pool size 100

		// Should pass validation with default values
		if err := config.Validate(); err != nil {
			t.Errorf("Default config should be valid, got error: %v", err)
		}

		// Test invalid MaxHandoffRetries
		config.MaxHandoffRetries = 0
		if err := config.Validate(); err == nil {
			t.Error("Expected validation error for MaxHandoffRetries = 0")
		}
		config.MaxHandoffRetries = 11
		if err := config.Validate(); err == nil {
			t.Error("Expected validation error for MaxHandoffRetries = 11")
		}
		config.MaxHandoffRetries = 3 // Reset to valid value

		// Test invalid HandoffQueueTimeout
		config.HandoffQueueTimeout = 0
		if err := config.Validate(); err == nil {
			t.Error("Expected validation error for HandoffQueueTimeout = 0")
		}
		config.HandoffQueueTimeout = 2 * time.Minute
		if err := config.Validate(); err == nil {
			t.Error("Expected validation error for HandoffQueueTimeout = 2 minutes")
		}
		config.HandoffQueueTimeout = 5 * time.Second // Reset to valid value

		// Test invalid MaxActiveOperations
		config.MaxActiveOperations = 50
		if err := config.Validate(); err == nil {
			t.Error("Expected validation error for MaxActiveOperations = 50")
		}
		config.MaxActiveOperations = 200000
		if err := config.Validate(); err == nil {
			t.Error("Expected validation error for MaxActiveOperations = 200000")
		}
		config.MaxActiveOperations = 10000 // Reset to valid value

		// Should pass validation again
		if err := config.Validate(); err != nil {
			t.Errorf("Config should be valid after reset, got error: %v", err)
		}
	})
}

func TestConfigClone(t *testing.T) {
	original := DefaultConfig()
	original.MaxHandoffRetries = 7
	original.HandoffQueueTimeout = 8 * time.Second

	cloned := original.Clone()

	// Test that values are copied
	if cloned.MaxHandoffRetries != 7 {
		t.Errorf("Expected cloned MaxHandoffRetries to be 7, got %d", cloned.MaxHandoffRetries)
	}
	if cloned.HandoffQueueTimeout != 8*time.Second {
		t.Errorf("Expected cloned HandoffQueueTimeout to be 8s, got %v", cloned.HandoffQueueTimeout)
	}

	// Test that modifying clone doesn't affect original
	cloned.MaxHandoffRetries = 10
	if original.MaxHandoffRetries != 7 {
		t.Errorf("Modifying clone should not affect original, original MaxHandoffRetries changed to %d", original.MaxHandoffRetries)
	}
}
