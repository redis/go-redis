package routing

import (
	"errors"
	"testing"
)

func TestAggLogicalAndAggregator(t *testing.T) {
	t.Run("all true values", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalAnd, "")

		err := agg.Add(true, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = agg.Add(int64(1), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = agg.Add(1, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := agg.Aggregate()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result != true {
			t.Errorf("expected true, got %v", result)
		}
	})

	t.Run("one false value", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalAnd, "")

		err := agg.Add(true, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = agg.Add(false, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = agg.Add(true, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := agg.Aggregate()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result != false {
			t.Errorf("expected false, got %v", result)
		}
	})

	t.Run("no results", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalAnd, "")

		_, err := agg.Aggregate()
		if err != ErrAndAggregation {
			t.Errorf("expected ErrAndAggregation, got %v", err)
		}
	})

	t.Run("with error", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalAnd, "")

		testErr := errors.New("test error")
		err := agg.Add(nil, testErr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = agg.Aggregate()
		if err != testErr {
			t.Errorf("expected test error, got %v", err)
		}
	})
}

func TestAggLogicalOrAggregator(t *testing.T) {
	t.Run("all false values", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalOr, "")

		err := agg.Add(false, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = agg.Add(int64(0), nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = agg.Add(0, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := agg.Aggregate()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result != false {
			t.Errorf("expected false, got %v", result)
		}
	})

	t.Run("one true value", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalOr, "")

		err := agg.Add(false, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = agg.Add(true, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		err = agg.Add(false, nil)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := agg.Aggregate()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result != true {
			t.Errorf("expected true, got %v", result)
		}
	})

	t.Run("no results", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalOr, "")

		_, err := agg.Aggregate()
		if err != ErrOrAggregation {
			t.Errorf("expected ErrOrAggregation, got %v", err)
		}
	})

	t.Run("with error", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalOr, "")

		testErr := errors.New("test error")
		err := agg.Add(nil, testErr)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		_, err = agg.Aggregate()
		if err != testErr {
			t.Errorf("expected test error, got %v", err)
		}
	})
}

func TestNumericAggregatorsPreserveExactIntegers(t *testing.T) {
	tests := []struct {
		name   string
		policy ResponsePolicy
		values []AggregatorResErr
		want   int64
	}{
		{
			name:   "sum above float precision",
			policy: RespAggSum,
			values: []AggregatorResErr{{Result: int64(1 << 53)}, {Result: int64(1)}},
			want:   int64(1<<53) + 1,
		},
		{
			name:   "minimum above float precision",
			policy: RespAggMin,
			values: []AggregatorResErr{{Result: int64(1<<53) + 1}, {Result: int64(1 << 53)}},
			want:   int64(1 << 53),
		},
		{
			name:   "maximum above float precision",
			policy: RespAggMax,
			values: []AggregatorResErr{{Result: int64(1<<53) + 1}, {Result: int64(1 << 53)}},
			want:   int64(1<<53) + 1,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			agg := NewResponseAggregator(tt.policy, "")
			if err := agg.BatchSlice(tt.values); err != nil {
				t.Fatal(err)
			}
			got, err := agg.Aggregate()
			if err != nil {
				t.Fatal(err)
			}
			if got != tt.want {
				t.Fatalf("result=%v (%T), want %d (int64)", got, got, tt.want)
			}
		})
	}
}

func TestAggLogicalAndBatchAdd(t *testing.T) {
	t.Run("batch add all true", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalAnd, "")

		results := map[string]AggregatorResErr{
			"key1": {Result: true, Err: nil},
			"key2": {Result: int64(1), Err: nil},
			"key3": {Result: 1, Err: nil},
		}

		err := agg.BatchAdd(results)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := agg.Aggregate()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result != true {
			t.Errorf("expected true, got %v", result)
		}
	})

	t.Run("batch add with false", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalAnd, "")

		results := map[string]AggregatorResErr{
			"key1": {Result: true, Err: nil},
			"key2": {Result: false, Err: nil},
			"key3": {Result: true, Err: nil},
		}

		err := agg.BatchAdd(results)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := agg.Aggregate()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result != false {
			t.Errorf("expected false, got %v", result)
		}
	})
}

func TestAggLogicalOrBatchAdd(t *testing.T) {
	t.Run("batch add all false", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalOr, "")

		results := map[string]AggregatorResErr{
			"key1": {Result: false, Err: nil},
			"key2": {Result: int64(0), Err: nil},
			"key3": {Result: 0, Err: nil},
		}

		err := agg.BatchAdd(results)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := agg.Aggregate()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result != false {
			t.Errorf("expected false, got %v", result)
		}
	})

	t.Run("batch add with true", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalOr, "")

		results := map[string]AggregatorResErr{
			"key1": {Result: false, Err: nil},
			"key2": {Result: true, Err: nil},
			"key3": {Result: false, Err: nil},
		}

		err := agg.BatchAdd(results)
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		result, err := agg.Aggregate()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}

		if result != true {
			t.Errorf("expected true, got %v", result)
		}
	})
}
