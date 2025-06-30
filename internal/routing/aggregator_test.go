package routing

import (
	"errors"
	"testing"
)

// Mock command types for testing
type MockStringCmd struct {
	cmdType CmdType
	val     string
}

func (cmd *MockStringCmd) GetCmdType() CmdType {
	return cmd.cmdType
}

func (cmd *MockStringCmd) Val() string {
	return cmd.val
}

type MockIntCmd struct {
	cmdType CmdType
	val     int64
}

func (cmd *MockIntCmd) GetCmdType() CmdType {
	return cmd.cmdType
}

func (cmd *MockIntCmd) Val() int64 {
	return cmd.val
}

type MockBoolCmd struct {
	cmdType CmdType
	val     bool
}

func (cmd *MockBoolCmd) GetCmdType() CmdType {
	return cmd.cmdType
}

func (cmd *MockBoolCmd) Val() bool {
	return cmd.val
}

// Legacy command without GetCmdType for comparison
type LegacyStringCmd struct {
	val string
}

func (cmd *LegacyStringCmd) Val() string {
	return cmd.val
}

func BenchmarkExtractCommandValueOptimized(b *testing.B) {
	commands := []interface{}{
		&MockStringCmd{cmdType: CmdTypeString, val: "test-value"},
		&MockIntCmd{cmdType: CmdTypeInt, val: 42},
		&MockBoolCmd{cmdType: CmdTypeBool, val: true},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, cmd := range commands {
			ExtractCommandValue(cmd)
		}
	}
}

func BenchmarkExtractCommandValueLegacy(b *testing.B) {
	commands := []interface{}{
		&LegacyStringCmd{val: "test-value"},
		&MockIntCmd{cmdType: CmdTypeInt, val: 42},
		&MockBoolCmd{cmdType: CmdTypeBool, val: true},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for _, cmd := range commands {
			ExtractCommandValue(cmd)
		}
	}
}

func TestExtractCommandValue(t *testing.T) {
	tests := []struct {
		name     string
		cmd      interface{}
		expected interface{}
	}{
		{
			name:     "string command",
			cmd:      &MockStringCmd{cmdType: CmdTypeString, val: "hello"},
			expected: "hello",
		},
		{
			name:     "int command",
			cmd:      &MockIntCmd{cmdType: CmdTypeInt, val: 123},
			expected: int64(123),
		},
		{
			name:     "bool command",
			cmd:      &MockBoolCmd{cmdType: CmdTypeBool, val: true},
			expected: true,
		},
		{
			name:     "unsupported command",
			cmd:      &LegacyStringCmd{val: "test"},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractCommandValue(tt.cmd)
			if result != tt.expected {
				t.Errorf("ExtractCommandValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestExtractCommandValueIntegration(t *testing.T) {
	tests := []struct {
		name     string
		cmd      interface{}
		expected interface{}
	}{
		{
			name:     "optimized string command",
			cmd:      &MockStringCmd{cmdType: CmdTypeString, val: "hello"},
			expected: "hello",
		},
		{
			name:     "legacy string command returns nil (no GetCmdType)",
			cmd:      &LegacyStringCmd{val: "legacy"},
			expected: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ExtractCommandValue(tt.cmd)
			if result != tt.expected {
				t.Errorf("ExtractCommandValue() = %v, want %v", result, tt.expected)
			}
		})
	}
}

func TestAllSucceededAggregator(t *testing.T) {
	agg := &AllSucceededAggregator{}

	err := agg.Add("result1", nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add("result2", nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err := agg.Finish()
	if err != nil {
		t.Errorf("Finish failed: %v", err)
	}
	if result != "result1" {
		t.Errorf("Expected 'result1', got %v", result)
	}

	agg = &AllSucceededAggregator{}
	testErr := errors.New("test error")
	err = agg.Add("result1", nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add("result2", testErr)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err = agg.Finish()
	if err != testErr {
		t.Errorf("Expected test error, got %v", err)
	}
}

func TestOneSucceededAggregator(t *testing.T) {
	agg := &OneSucceededAggregator{}

	testErr := errors.New("test error")
	err := agg.Add("result1", testErr)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add("result2", nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err := agg.Finish()
	if err != nil {
		t.Errorf("Finish failed: %v", err)
	}
	if result != "result2" {
		t.Errorf("Expected 'result2', got %v", result)
	}

	agg = &OneSucceededAggregator{}
	err = agg.Add("result1", testErr)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add("result2", testErr)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err = agg.Finish()
	if err != testErr {
		t.Errorf("Expected test error, got %v", err)
	}
}

func TestAggSumAggregator(t *testing.T) {
	agg := &AggSumAggregator{}

	err := agg.Add(int64(10), nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(int64(20), nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(int64(30), nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err := agg.Finish()
	if err != nil {
		t.Errorf("Finish failed: %v", err)
	}
	if result != int64(60) {
		t.Errorf("Expected 60, got %v", result)
	}

	agg = &AggSumAggregator{}
	testErr := errors.New("test error")
	err = agg.Add(int64(10), nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(int64(20), testErr)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err = agg.Finish()
	if err != testErr {
		t.Errorf("Expected test error, got %v", err)
	}
}

func TestAggMinAggregator(t *testing.T) {
	agg := &AggMinAggregator{}

	err := agg.Add(int64(30), nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(int64(10), nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(int64(20), nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err := agg.Finish()
	if err != nil {
		t.Errorf("Finish failed: %v", err)
	}
	if result != int64(10) {
		t.Errorf("Expected 10, got %v", result)
	}
}

func TestAggMaxAggregator(t *testing.T) {
	agg := &AggMaxAggregator{}

	err := agg.Add(int64(10), nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(int64(30), nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(int64(20), nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err := agg.Finish()
	if err != nil {
		t.Errorf("Finish failed: %v", err)
	}
	if result != int64(30) {
		t.Errorf("Expected 30, got %v", result)
	}
}

func TestAggLogicalAndAggregator(t *testing.T) {
	agg := &AggLogicalAndAggregator{}

	err := agg.Add(true, nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(true, nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(false, nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err := agg.Finish()
	if err != nil {
		t.Errorf("Finish failed: %v", err)
	}
	if result != false {
		t.Errorf("Expected false, got %v", result)
	}
}

func TestAggLogicalOrAggregator(t *testing.T) {
	agg := &AggLogicalOrAggregator{}

	err := agg.Add(false, nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(true, nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add(false, nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err := agg.Finish()
	if err != nil {
		t.Errorf("Finish failed: %v", err)
	}
	if result != true {
		t.Errorf("Expected true, got %v", result)
	}
}

func TestDefaultKeylessAggregator(t *testing.T) {
	agg := &DefaultKeylessAggregator{}

	err := agg.Add("result1", nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add("result2", nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}
	err = agg.Add("result3", nil)
	if err != nil {
		t.Errorf("Add failed: %v", err)
	}

	result, err := agg.Finish()
	if err != nil {
		t.Errorf("Finish failed: %v", err)
	}

	results, ok := result.([]interface{})
	if !ok {
		t.Errorf("Expected []interface{}, got %T", result)
	}
	if len(results) != 3 {
		t.Errorf("Expected 3 results, got %d", len(results))
	}
	if results[0] != "result1" || results[1] != "result2" || results[2] != "result3" {
		t.Errorf("Unexpected results: %v", results)
	}
}

func TestNewResponseAggregator(t *testing.T) {
	tests := []struct {
		policy   ResponsePolicy
		cmdName  string
		expected string
	}{
		{RespAllSucceeded, "test", "*routing.AllSucceededAggregator"},
		{RespOneSucceeded, "test", "*routing.OneSucceededAggregator"},
		{RespAggSum, "test", "*routing.AggSumAggregator"},
		{RespAggMin, "test", "*routing.AggMinAggregator"},
		{RespAggMax, "test", "*routing.AggMaxAggregator"},
		{RespAggLogicalAnd, "test", "*routing.AggLogicalAndAggregator"},
		{RespAggLogicalOr, "test", "*routing.AggLogicalOrAggregator"},
		{RespSpecial, "test", "*routing.SpecialAggregator"},
	}

	for _, test := range tests {
		agg := NewResponseAggregator(test.policy, test.cmdName)
		if agg == nil {
			t.Errorf("NewResponseAggregator returned nil for policy %v", test.policy)
		}
		_, ok := agg.(ResponseAggregator)
		if !ok {
			t.Errorf("Aggregator does not implement ResponseAggregator interface")
		}
	}
}
