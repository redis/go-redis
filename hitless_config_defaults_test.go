package redis

import (
	"testing"
	"time"
)

func TestHitlessUpgradeConfig_DefaultValues(t *testing.T) {
	tests := []struct {
		name           string
		inputConfig    *HitlessUpgradeConfig
		expectedConfig *HitlessUpgradeConfig
	}{
		{
			name:        "nil config should use all defaults",
			inputConfig: nil,
			expectedConfig: &HitlessUpgradeConfig{
				Enabled:           true,
				TransitionTimeout: 60 * time.Second,
				CleanupInterval:   30 * time.Second,
			},
		},
		{
			name: "zero TransitionTimeout should use default",
			inputConfig: &HitlessUpgradeConfig{
				Enabled:           false,
				TransitionTimeout: 0, // Zero value
				CleanupInterval:   45 * time.Second,
			},
			expectedConfig: &HitlessUpgradeConfig{
				Enabled:           false,
				TransitionTimeout: 60 * time.Second, // Should use default
				CleanupInterval:   45 * time.Second,
			},
		},
		{
			name: "zero CleanupInterval should use default",
			inputConfig: &HitlessUpgradeConfig{
				Enabled:           true,
				TransitionTimeout: 90 * time.Second,
				CleanupInterval:   0, // Zero value
			},
			expectedConfig: &HitlessUpgradeConfig{
				Enabled:           true,
				TransitionTimeout: 90 * time.Second,
				CleanupInterval:   30 * time.Second, // Should use default
			},
		},
		{
			name: "both timeouts zero should use defaults",
			inputConfig: &HitlessUpgradeConfig{
				Enabled:           true,
				TransitionTimeout: 0, // Zero value
				CleanupInterval:   0, // Zero value
			},
			expectedConfig: &HitlessUpgradeConfig{
				Enabled:           true,
				TransitionTimeout: 60 * time.Second, // Should use default
				CleanupInterval:   30 * time.Second, // Should use default
			},
		},
		{
			name: "all values set should be preserved",
			inputConfig: &HitlessUpgradeConfig{
				Enabled:           false,
				TransitionTimeout: 120 * time.Second,
				CleanupInterval:   60 * time.Second,
			},
			expectedConfig: &HitlessUpgradeConfig{
				Enabled:           false,
				TransitionTimeout: 120 * time.Second,
				CleanupInterval:   60 * time.Second,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Test with a mock client that has hitless upgrades enabled
			opt := &Options{
				Addr:                 "127.0.0.1:6379",
				Protocol:             3,
				HitlessUpgrades:      true,
				HitlessUpgradeConfig: tt.inputConfig,
				ReadTimeout:          5 * time.Second,
				WriteTimeout:         5 * time.Second,
			}

			// Test the integration creation using the internal method directly
			// since initializeHitlessIntegration requires a push processor
			integration := newHitlessIntegrationWithTimeouts(tt.inputConfig, opt.ReadTimeout, opt.WriteTimeout)
			if integration == nil {
				t.Fatal("Integration should not be nil")
			}

			// Get the config from the integration
			actualConfig := integration.GetConfig()
			if actualConfig == nil {
				t.Fatal("Config should not be nil")
			}

			// Verify all fields match expected values
			if actualConfig.Enabled != tt.expectedConfig.Enabled {
				t.Errorf("Enabled: expected %v, got %v", tt.expectedConfig.Enabled, actualConfig.Enabled)
			}
			if actualConfig.TransitionTimeout != tt.expectedConfig.TransitionTimeout {
				t.Errorf("TransitionTimeout: expected %v, got %v", tt.expectedConfig.TransitionTimeout, actualConfig.TransitionTimeout)
			}
			if actualConfig.CleanupInterval != tt.expectedConfig.CleanupInterval {
				t.Errorf("CleanupInterval: expected %v, got %v", tt.expectedConfig.CleanupInterval, actualConfig.CleanupInterval)
			}

			// Test UpdateConfig as well
			newConfig := &HitlessUpgradeConfig{
				Enabled:           !tt.expectedConfig.Enabled,
				TransitionTimeout: 0, // Zero value should use default
				CleanupInterval:   0, // Zero value should use default
			}

			err := integration.UpdateConfig(newConfig)
			if err != nil {
				t.Fatalf("Failed to update config: %v", err)
			}

			// Verify updated config has defaults applied
			updatedConfig := integration.GetConfig()
			if updatedConfig.Enabled == tt.expectedConfig.Enabled {
				t.Error("Enabled should have been toggled")
			}
			if updatedConfig.TransitionTimeout != 60*time.Second {
				t.Errorf("TransitionTimeout should use default (60s), got %v", updatedConfig.TransitionTimeout)
			}
			if updatedConfig.CleanupInterval != 30*time.Second {
				t.Errorf("CleanupInterval should use default (30s), got %v", updatedConfig.CleanupInterval)
			}
		})
	}
}

func TestDefaultHitlessUpgradeConfig(t *testing.T) {
	config := DefaultHitlessUpgradeConfig()
	
	if config == nil {
		t.Fatal("Default config should not be nil")
	}
	
	if !config.Enabled {
		t.Error("Default config should have Enabled=true")
	}
	
	if config.TransitionTimeout != 60*time.Second {
		t.Errorf("Default TransitionTimeout should be 60s, got %v", config.TransitionTimeout)
	}
	
	if config.CleanupInterval != 30*time.Second {
		t.Errorf("Default CleanupInterval should be 30s, got %v", config.CleanupInterval)
	}
}

func TestHitlessUpgradeConfig_ZeroValueHandling(t *testing.T) {
	// Test that zero values are properly handled in various scenarios
	
	// Test 1: Partial config with some zero values
	partialConfig := &HitlessUpgradeConfig{
		Enabled: true,
		// TransitionTimeout and CleanupInterval are zero values
	}
	
	integration := newHitlessIntegrationWithTimeouts(partialConfig, 3*time.Second, 3*time.Second)
	if integration == nil {
		t.Fatal("Integration should not be nil")
	}
	
	config := integration.GetConfig()
	if config.TransitionTimeout == 0 {
		t.Error("Zero TransitionTimeout should have been replaced with default")
	}
	if config.CleanupInterval == 0 {
		t.Error("Zero CleanupInterval should have been replaced with default")
	}
	
	// Test 2: Empty struct
	emptyConfig := &HitlessUpgradeConfig{}
	
	integration2 := newHitlessIntegrationWithTimeouts(emptyConfig, 3*time.Second, 3*time.Second)
	if integration2 == nil {
		t.Fatal("Integration should not be nil")
	}
	
	config2 := integration2.GetConfig()
	if config2.TransitionTimeout == 0 {
		t.Error("Zero TransitionTimeout in empty config should have been replaced with default")
	}
	if config2.CleanupInterval == 0 {
		t.Error("Zero CleanupInterval in empty config should have been replaced with default")
	}
}
