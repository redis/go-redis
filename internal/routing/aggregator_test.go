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
		
		result, err := agg.Result()
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
		
		result, err := agg.Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		
		if result != false {
			t.Errorf("expected false, got %v", result)
		}
	})
	
	t.Run("no results", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalAnd, "")
		
		_, err := agg.Result()
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
		
		_, err = agg.Result()
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
		
		result, err := agg.Result()
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
		
		result, err := agg.Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		
		if result != true {
			t.Errorf("expected true, got %v", result)
		}
	})
	
	t.Run("no results", func(t *testing.T) {
		agg := NewResponseAggregator(RespAggLogicalOr, "")
		
		_, err := agg.Result()
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
		
		_, err = agg.Result()
		if err != testErr {
			t.Errorf("expected test error, got %v", err)
		}
	})
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
		
		result, err := agg.Result()
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
		
		result, err := agg.Result()
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
		
		result, err := agg.Result()
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
		
		result, err := agg.Result()
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
		
		if result != true {
			t.Errorf("expected true, got %v", result)
		}
	})
}

