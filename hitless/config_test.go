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

		// MaxWorkers should be 0 in default config (auto-calculated)
		if config.MaxWorkers != 0 {
			t.Errorf("Expected MaxWorkers to be 0 (auto-calculated), got %d", config.MaxWorkers)
		}

		// HandoffQueueSize should be 0 in default config (auto-calculated)
		if config.HandoffQueueSize != 0 {
			t.Errorf("Expected HandoffQueueSize to be 0 (auto-calculated), got %d", config.HandoffQueueSize)
		}

		if config.RelaxedTimeout != 10*time.Second {
			t.Errorf("Expected RelaxedTimeout to be 10s, got %v", config.RelaxedTimeout)
		}

		// Test configuration fields have proper defaults
		if config.MaxHandoffRetries != 3 {
			t.Errorf("Expected MaxHandoffRetries to be 3, got %d", config.MaxHandoffRetries)
		}

		if config.HandoffTimeout != 15*time.Second {
			t.Errorf("Expected HandoffTimeout to be 15s, got %v", config.HandoffTimeout)
		}

		if config.PostHandoffRelaxedDuration != 0 {
			t.Errorf("Expected PostHandoffRelaxedDuration to be 0 (auto-calculated), got %v", config.PostHandoffRelaxedDuration)
		}

		// Test that defaults are applied correctly
		configWithDefaults := config.ApplyDefaultsWithPoolSize(100)
		if configWithDefaults.PostHandoffRelaxedDuration != 20*time.Second {
			t.Errorf("Expected PostHandoffRelaxedDuration to be 20s (2x RelaxedTimeout) after applying defaults, got %v", configWithDefaults.PostHandoffRelaxedDuration)
		}
	})

	t.Run("ConfigValidation", func(t *testing.T) {
		// Valid config with applied defaults
		config := DefaultConfig().ApplyDefaults()
		if err := config.Validate(); err != nil {
			t.Errorf("Default config with applied defaults should be valid: %v", err)
		}

		// Invalid worker configuration (negative MaxWorkers)
		config = &Config{
			RelaxedTimeout:             30 * time.Second,
			HandoffTimeout:             15 * time.Second,
			MaxWorkers:                 -1, // This should be invalid
			HandoffQueueSize:           100,
			PostHandoffRelaxedDuration: 10 * time.Second,
			LogLevel:                   1,
			MaxHandoffRetries:          3, // Add required field
		}
		if err := config.Validate(); err != ErrInvalidHandoffWorkers {
			t.Errorf("Expected ErrInvalidHandoffWorkers, got %v", err)
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
		original.MaxWorkers = 20
		original.HandoffQueueSize = 200

		cloned := original.Clone()

		if cloned.MaxWorkers != 20 {
			t.Errorf("Expected cloned MaxWorkers to be 20, got %d", cloned.MaxWorkers)
		}

		if cloned.HandoffQueueSize != 200 {
			t.Errorf("Expected cloned HandoffQueueSize to be 200, got %d", cloned.HandoffQueueSize)
		}

		// Modify original to ensure clone is independent
		original.MaxWorkers = 2
		if cloned.MaxWorkers != 20 {
			t.Error("Clone should be independent of original")
		}
	})
}

func TestApplyDefaults(t *testing.T) {
	t.Run("NilConfig", func(t *testing.T) {
		var config *Config
		result := config.ApplyDefaultsWithPoolSize(100) // Use explicit pool size for testing

		// With nil config, should get default config with auto-calculated workers
		if result.MaxWorkers <= 0 {
			t.Errorf("Expected MaxWorkers to be > 0 after applying defaults, got %d", result.MaxWorkers)
		}

		// HandoffQueueSize should be auto-calculated with hybrid scaling
		workerBasedSize := result.MaxWorkers * 8
		poolSize := 100 // Default pool size used in ApplyDefaults
		poolBasedSize := util.Max(50, poolSize/2)
		expectedQueueSize := util.Max(workerBasedSize, poolBasedSize)
		expectedQueueSize = util.Min(expectedQueueSize, poolSize*2) // Cap by 2x pool size
		if result.HandoffQueueSize != expectedQueueSize {
			t.Errorf("Expected HandoffQueueSize to be %d (max(8*MaxWorkers=%d, max(50, poolSize/2=%d)) capped by 2*poolSize=%d), got %d",
				expectedQueueSize, workerBasedSize, poolBasedSize, poolSize*2, result.HandoffQueueSize)
		}
	})

	t.Run("PartialConfig", func(t *testing.T) {
		config := &Config{
			MaxWorkers: 12, // Set this field explicitly
			// Leave other fields as zero values
		}

		result := config.ApplyDefaultsWithPoolSize(100) // Use explicit pool size for testing

		// Should keep the explicitly set values
		if result.MaxWorkers != 12 {
			t.Errorf("Expected MaxWorkers to be 12 (explicitly set), got %d", result.MaxWorkers)
		}

		// Should apply default for unset fields (auto-calculated queue size with hybrid scaling)
		workerBasedSize := result.MaxWorkers * 8
		poolSize := 100 // Default pool size used in ApplyDefaults
		poolBasedSize := util.Max(50, poolSize/2)
		expectedQueueSize := util.Max(workerBasedSize, poolBasedSize)
		expectedQueueSize = util.Min(expectedQueueSize, poolSize*2) // Cap by 2x pool size
		if result.HandoffQueueSize != expectedQueueSize {
			t.Errorf("Expected HandoffQueueSize to be %d (max(8*MaxWorkers=%d, max(50, poolSize/2=%d)) capped by 2*poolSize=%d), got %d",
				expectedQueueSize, workerBasedSize, poolBasedSize, poolSize*2, result.HandoffQueueSize)
		}

		// Test explicit queue size capping by 2x pool size
		configWithLargeQueue := &Config{
			MaxWorkers:       5,
			HandoffQueueSize: 1000, // Much larger than 2x pool size
		}

		resultCapped := configWithLargeQueue.ApplyDefaultsWithPoolSize(20) // Small pool size
		expectedCap := 20 * 2 // 2x pool size = 40
		if resultCapped.HandoffQueueSize != expectedCap {
			t.Errorf("Expected HandoffQueueSize to be capped by 2x pool size (%d), got %d", expectedCap, resultCapped.HandoffQueueSize)
		}

		// Test explicit queue size minimum enforcement
		configWithSmallQueue := &Config{
			MaxWorkers:       5,
			HandoffQueueSize: 10, // Below minimum of 50
		}

		resultMinimum := configWithSmallQueue.ApplyDefaultsWithPoolSize(100) // Large pool size
		if resultMinimum.HandoffQueueSize != 50 {
			t.Errorf("Expected HandoffQueueSize to be enforced minimum (50), got %d", resultMinimum.HandoffQueueSize)
		}

		// Test that large explicit values are capped by 2x pool size
		configWithVeryLargeQueue := &Config{
			MaxWorkers:       5,
			HandoffQueueSize: 500, // Much larger than 2x pool size
		}

		resultVeryLarge := configWithVeryLargeQueue.ApplyDefaultsWithPoolSize(100) // Pool size 100
		expectedVeryLargeCap := 100 * 2 // 2x pool size = 200
		if resultVeryLarge.HandoffQueueSize != expectedVeryLargeCap {
			t.Errorf("Expected very large HandoffQueueSize to be capped by 2x pool size (%d), got %d", expectedVeryLargeCap, resultVeryLarge.HandoffQueueSize)
		}

		if result.RelaxedTimeout != 10*time.Second {
			t.Errorf("Expected RelaxedTimeout to be 10s (default), got %v", result.RelaxedTimeout)
		}

		if result.HandoffTimeout != 15*time.Second {
			t.Errorf("Expected HandoffTimeout to be 15s (default), got %v", result.HandoffTimeout)
		}
	})

	t.Run("ZeroValues", func(t *testing.T) {
		config := &Config{
			MaxWorkers:       0, // Zero value should get auto-calculated defaults
			HandoffQueueSize: 0, // Zero value should get default
			RelaxedTimeout:   0, // Zero value should get default
			LogLevel:         0, // Zero is valid for LogLevel (errors only)
		}

		result := config.ApplyDefaultsWithPoolSize(100) // Use explicit pool size for testing

		// Zero values should get auto-calculated defaults
		if result.MaxWorkers <= 0 {
			t.Errorf("Expected MaxWorkers to be > 0 (auto-calculated), got %d", result.MaxWorkers)
		}

		// HandoffQueueSize should be auto-calculated with hybrid scaling
		workerBasedSize := result.MaxWorkers * 8
		poolSize := 100 // Default pool size used in ApplyDefaults
		poolBasedSize := util.Max(50, poolSize/2)
		expectedQueueSize := util.Max(workerBasedSize, poolBasedSize)
		expectedQueueSize = util.Min(expectedQueueSize, poolSize*2) // Cap by 2x pool size
		if result.HandoffQueueSize != expectedQueueSize {
			t.Errorf("Expected HandoffQueueSize to be %d (max(8*MaxWorkers=%d, max(50, poolSize/2=%d)) capped by 2*poolSize=%d), got %d",
				expectedQueueSize, workerBasedSize, poolBasedSize, poolSize*2, result.HandoffQueueSize)
		}

		if result.RelaxedTimeout != 10*time.Second {
			t.Errorf("Expected RelaxedTimeout to be 10s (default), got %v", result.RelaxedTimeout)
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
			MaxWorkers: 7, // Only set worker field
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
			MaxWorkers: 15, // Custom value (>= 10 to test preservation)
			LogLevel:   2,  // Custom value
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

		// Should preserve custom values (when >= 10)
		if expectedConfig.MaxWorkers != 15 {
			t.Errorf("Expected MaxWorkers to be 15, got %d", expectedConfig.MaxWorkers)
		}

		if expectedConfig.LogLevel != 2 {
			t.Errorf("Expected LogLevel to be 2, got %d", expectedConfig.LogLevel)
		}

		// Should apply defaults for missing fields (auto-calculated queue size with hybrid scaling)
		workerBasedSize := expectedConfig.MaxWorkers * 8
		poolSize := 100 // Default pool size used in ApplyDefaults
		poolBasedSize := util.Max(50, poolSize/2)
		expectedQueueSize := util.Max(workerBasedSize, poolBasedSize)
		expectedQueueSize = util.Min(expectedQueueSize, poolSize*2) // Cap by 2x pool size
		if expectedConfig.HandoffQueueSize != expectedQueueSize {
			t.Errorf("Expected HandoffQueueSize to be %d (max(8*MaxWorkers=%d, max(50, poolSize/2=%d)) capped by 2*poolSize=%d), got %d",
				expectedQueueSize, workerBasedSize, poolBasedSize, poolSize*2, expectedConfig.HandoffQueueSize)
		}

		// Test that queue size is always capped by 2x pool size
		if expectedConfig.HandoffQueueSize > poolSize*2 {
			t.Errorf("HandoffQueueSize (%d) should never exceed 2x pool size (%d)",
				expectedConfig.HandoffQueueSize, poolSize*2)
		}

		if expectedConfig.RelaxedTimeout != 10*time.Second {
			t.Errorf("Expected RelaxedTimeout to be 10s (default), got %v", expectedConfig.RelaxedTimeout)
		}

		if expectedConfig.HandoffTimeout != 15*time.Second {
			t.Errorf("Expected HandoffTimeout to be 15s (default), got %v", expectedConfig.HandoffTimeout)
		}

		if expectedConfig.PostHandoffRelaxedDuration != 20*time.Second {
			t.Errorf("Expected PostHandoffRelaxedDuration to be 20s (2x RelaxedTimeout), got %v", expectedConfig.PostHandoffRelaxedDuration)
		}
	})
}

func TestEnhancedConfigValidation(t *testing.T) {
	t.Run("ValidateFields", func(t *testing.T) {
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

		// Should pass validation again
		if err := config.Validate(); err != nil {
			t.Errorf("Config should be valid after reset, got error: %v", err)
		}
	})
}

func TestConfigClone(t *testing.T) {
	original := DefaultConfig()
	original.MaxHandoffRetries = 7
	original.HandoffTimeout = 8 * time.Second

	cloned := original.Clone()

	// Test that values are copied
	if cloned.MaxHandoffRetries != 7 {
		t.Errorf("Expected cloned MaxHandoffRetries to be 7, got %d", cloned.MaxHandoffRetries)
	}
	if cloned.HandoffTimeout != 8*time.Second {
		t.Errorf("Expected cloned HandoffTimeout to be 8s, got %v", cloned.HandoffTimeout)
	}

	// Test that modifying clone doesn't affect original
	cloned.MaxHandoffRetries = 10
	if original.MaxHandoffRetries != 7 {
		t.Errorf("Modifying clone should not affect original, original MaxHandoffRetries changed to %d", original.MaxHandoffRetries)
	}
}

func TestMaxWorkersLogic(t *testing.T) {
	t.Run("AutoCalculatedMaxWorkers", func(t *testing.T) {
		testCases := []struct {
			poolSize        int
			expectedWorkers int
			description     string
		}{
			{6, 2, "Small pool: min(10, 6/3) = min(10, 2) = 2"},
			{15, 5, "Medium pool: min(10, 15/3) = min(10, 5) = 5"},
			{30, 10, "Large pool: min(10, 30/3) = min(10, 10) = 10"},
			{60, 10, "Very large pool: min(10, 60/3) = min(10, 20) = 10"},
			{120, 10, "Huge pool: min(10, 120/3) = min(10, 40) = 10"},
		}

		for _, tc := range testCases {
			config := &Config{} // MaxWorkers = 0 (not set)
			result := config.ApplyDefaultsWithPoolSize(tc.poolSize)

			if result.MaxWorkers != tc.expectedWorkers {
				t.Errorf("PoolSize=%d: expected MaxWorkers=%d, got %d (%s)",
					tc.poolSize, tc.expectedWorkers, result.MaxWorkers, tc.description)
			}
		}
	})

	t.Run("ExplicitlySetMaxWorkers", func(t *testing.T) {
		testCases := []struct {
			setValue        int
			expectedWorkers int
			description     string
		}{
			{1, 10, "Set 1: max(10, 1) = 10 (enforced minimum)"},
			{5, 10, "Set 5: max(10, 5) = 10 (enforced minimum)"},
			{8, 10, "Set 8: max(10, 8) = 10 (enforced minimum)"},
			{10, 10, "Set 10: max(10, 10) = 10 (exact minimum)"},
			{15, 15, "Set 15: max(10, 15) = 15 (respects user choice)"},
			{20, 20, "Set 20: max(10, 20) = 20 (respects user choice)"},
		}

		for _, tc := range testCases {
			config := &Config{
				MaxWorkers: tc.setValue, // Explicitly set
			}
			result := config.ApplyDefaultsWithPoolSize(100) // Pool size doesn't affect explicit values

			if result.MaxWorkers != tc.expectedWorkers {
				t.Errorf("Set MaxWorkers=%d: expected %d, got %d (%s)",
					tc.setValue, tc.expectedWorkers, result.MaxWorkers, tc.description)
			}
		}
	})
}
