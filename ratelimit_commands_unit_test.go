package redis

import (
	"context"
	"testing"
	"time"
)

func TestGCRAWithArgs_TokensParameter(t *testing.T) {
	ctx := context.Background()

	tests := []struct {
		name           string
		tokens         int64
		expectedArgs   []interface{}
		expectTokens   bool
	}{
		{
			name:         "Tokens = 0 should send TOKENS subcommand",
			tokens:       0,
			expectedArgs: []interface{}{"gcra", "testkey", int64(5), int64(10), 1.0, "TOKENS", int64(0)},
			expectTokens: true,
		},
		{
			name:         "Tokens = 1 should NOT send TOKENS subcommand (default)",
			tokens:       1,
			expectedArgs: []interface{}{"gcra", "testkey", int64(5), int64(10), 1.0},
			expectTokens: false,
		},
		{
			name:         "Tokens = 2 should send TOKENS subcommand",
			tokens:       2,
			expectedArgs: []interface{}{"gcra", "testkey", int64(5), int64(10), 1.0, "TOKENS", int64(2)},
			expectTokens: true,
		},
		{
			name:         "Tokens = 5 should send TOKENS subcommand",
			tokens:       5,
			expectedArgs: []interface{}{"gcra", "testkey", int64(5), int64(10), 1.0, "TOKENS", int64(5)},
			expectTokens: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args := &GCRAArgs{
				MaxBurst:        5,
				TokensPerPeriod: 10,
				Period:          time.Second,
				Tokens:          tt.tokens,
			}

			cmd := NewGCRACmd(ctx, buildGCRAArgs("testkey", args)...)

			// Check that the command has the expected arguments
			if len(cmd.Args()) != len(tt.expectedArgs) {
				t.Errorf("Expected %d args, got %d. Args: %v", len(tt.expectedArgs), len(cmd.Args()), cmd.Args())
			}

			// Verify each argument matches
			for i, expectedArg := range tt.expectedArgs {
				if i >= len(cmd.Args()) {
					t.Errorf("Missing arg at index %d", i)
					continue
				}
				if cmd.Args()[i] != expectedArg {
					t.Errorf("Arg at index %d: expected %v, got %v", i, expectedArg, cmd.Args()[i])
				}
			}

			// Check if TOKENS is present when expected
			hasTokens := false
			for i, arg := range cmd.Args() {
				if arg == "TOKENS" {
					hasTokens = true
					// Verify the value after TOKENS matches
					if i+1 < len(cmd.Args()) && cmd.Args()[i+1] != tt.tokens {
						t.Errorf("Expected TOKENS value %d, got %v", tt.tokens, cmd.Args()[i+1])
					}
					break
				}
			}

			if hasTokens != tt.expectTokens {
				t.Errorf("Expected TOKENS subcommand present: %v, got: %v", tt.expectTokens, hasTokens)
			}
		})
	}
}

// Helper function to build GCRA args (extracted from GCRAWithArgs for testing)
func buildGCRAArgs(key string, args *GCRAArgs) []interface{} {
	cmdArgs := make([]interface{}, 0, 7)
	cmdArgs = append(cmdArgs, "gcra", key, args.MaxBurst, args.TokensPerPeriod)

	// Convert period to seconds as a float
	periodSeconds := float64(args.Period) / float64(time.Second)
	cmdArgs = append(cmdArgs, periodSeconds)

	// Add TOKENS if specified and not default
	if args.Tokens != 1 {
		cmdArgs = append(cmdArgs, "TOKENS", args.Tokens)
	}

	return cmdArgs
}
