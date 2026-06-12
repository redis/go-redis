package routing

import (
	"errors"
	"testing"
)

func TestAggSumAggregator(t *testing.T) {
	agg := NewResponseAggregator(RespAggSum, "")
	for _, v := range []interface{}{1, int64(2), 3.0} {
		if err := agg.Add(v, nil); err != nil {
			t.Fatalf("Add(%v) error: %v", v, err)
		}
	}
	res, err := agg.Result()
	if err != nil {
		t.Fatalf("Result error: %v", err)
	}
	if res.(float64) != 6 {
		t.Errorf("sum = %v, want 6", res)
	}

	// Non-numeric value should surface an error.
	bad := NewResponseAggregator(RespAggSum, "")
	if err := bad.Add("nope", nil); err == nil {
		t.Errorf("Add(non-numeric) should error")
	}

	// BatchAdd path.
	batch := NewResponseAggregator(RespAggSum, "")
	if err := batch.BatchAdd(map[string]AggregatorResErr{
		"a": {Result: int64(4)}, "b": {Result: int64(6)},
	}); err != nil {
		t.Fatalf("BatchAdd error: %v", err)
	}
	res, _ = batch.Result()
	if res.(float64) != 10 {
		t.Errorf("batch sum = %v, want 10", res)
	}
}

func TestAggMinMaxAggregators(t *testing.T) {
	min := NewResponseAggregator(RespAggMin, "")
	for _, v := range []interface{}{5, 2, 8} {
		_ = min.Add(v, nil)
	}
	res, err := min.Result()
	if err != nil || res.(float64) != 2 {
		t.Errorf("min Result = %v, err %v, want 2", res, err)
	}

	max := NewResponseAggregator(RespAggMax, "")
	if err := max.BatchSlice([]AggregatorResErr{
		{Result: int64(5)}, {Result: int64(9)}, {Result: int64(1)},
	}); err != nil {
		t.Fatalf("BatchSlice error: %v", err)
	}
	res, err = max.Result()
	if err != nil || res.(float64) != 9 {
		t.Errorf("max Result = %v, err %v, want 9", res, err)
	}

	// Empty min/max aggregators surface their sentinel errors.
	emptyMin := NewResponseAggregator(RespAggMin, "")
	if _, err := emptyMin.Result(); err != ErrMinAggregation {
		t.Errorf("empty min err = %v, want ErrMinAggregation", err)
	}
	emptyMax := NewResponseAggregator(RespAggMax, "")
	if _, err := emptyMax.Result(); err != ErrMaxAggregation {
		t.Errorf("empty max err = %v, want ErrMaxAggregation", err)
	}

	// Error propagation.
	testErr := errors.New("boom")
	errMax := NewResponseAggregator(RespAggMax, "")
	_ = errMax.Add(nil, testErr)
	if _, err := errMax.Result(); err != testErr {
		t.Errorf("max err = %v, want boom", err)
	}
}

func TestAllSucceededAggregator(t *testing.T) {
	agg := NewResponseAggregator(RespAllSucceeded, "")
	_ = agg.Add("ok", nil)
	res, err := agg.Result()
	if err != nil || res != "ok" {
		t.Errorf("Result = %v, err %v, want ok", res, err)
	}

	withErr := NewResponseAggregator(RespAllSucceeded, "")
	testErr := errors.New("fail")
	_ = withErr.AddWithKey("k", nil, testErr)
	if _, err := withErr.Result(); err != testErr {
		t.Errorf("err = %v, want fail", err)
	}

	batch := NewResponseAggregator(RespAllSucceeded, "")
	if err := batch.BatchSlice([]AggregatorResErr{{Result: "x"}}); err != nil {
		t.Fatalf("BatchSlice error: %v", err)
	}
}

func TestOneSucceededAggregator(t *testing.T) {
	agg := NewResponseAggregator(RespOneSucceeded, "")
	_ = agg.Add(nil, errors.New("first"))
	_ = agg.Add("good", nil)
	res, err := agg.Result()
	if err != nil || res != "good" {
		t.Errorf("Result = %v, err %v, want good", res, err)
	}

	allErr := NewResponseAggregator(RespOneSucceeded, "")
	testErr := errors.New("only")
	if err := allErr.BatchAdd(map[string]AggregatorResErr{"k": {Err: testErr}}); err != nil {
		t.Fatalf("BatchAdd error: %v", err)
	}
	if _, err := allErr.Result(); err != testErr {
		t.Errorf("err = %v, want only", err)
	}
}

func TestDefaultAggregators(t *testing.T) {
	keyless := NewDefaultAggregator(false)
	_ = keyless.Add("a", nil)
	_ = keyless.Add("b", nil)
	res, err := keyless.Result()
	if err != nil || len(res.([]interface{})) != 2 {
		t.Errorf("keyless Result = %v, err %v", res, err)
	}

	keyed := NewDefaultKeyedAggregator([]string{"k1", "k2"})
	_ = keyed.AddWithKey("k1", "v1", nil)
	_ = keyed.AddWithKey("k2", "v2", nil)
	res, err = keyed.Result()
	ordered := res.([]interface{})
	if err != nil || ordered[0] != "v1" || ordered[1] != "v2" {
		t.Errorf("keyed Result = %v, err %v", res, err)
	}

	// Keyed aggregator without explicit key order.
	noOrder := NewResponseAggregator(RespDefaultHashSlot, "")
	_ = noOrder.AddWithKey("x", "vx", nil)
	res, _ = noOrder.Result()
	if len(res.([]interface{})) != 1 {
		t.Errorf("no-order keyed Result = %v", res)
	}
}

func TestSpecialAggregator(t *testing.T) {
	RegisterSpecialAggregator("CUSTOMCMD", func(results []interface{}, _ []error) (interface{}, error) {
		return len(results), nil
	})
	agg := NewResponseAggregator(RespSpecial, "CUSTOMCMD")
	_ = agg.Add("a", nil)
	_ = agg.Add("b", nil)
	res, err := agg.Result()
	if err != nil || res != 2 {
		t.Errorf("special Result = %v, err %v, want 2", res, err)
	}

	// Default behavior (no registered func) returns first non-error result.
	def := NewResponseAggregator(RespSpecial, "UNREGISTERED")
	_ = def.Add(nil, errors.New("e"))
	_ = def.Add("ok", nil)
	res, err = def.Result()
	if err != nil || res != "ok" {
		t.Errorf("default special Result = %v, err %v, want ok", res, err)
	}
}
