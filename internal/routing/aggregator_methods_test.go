package routing

import "testing"

// exerciseMethods drives all four ingestion methods of an aggregator so the
// repetitive AddWithKey/BatchAdd/BatchSlice wrappers are covered.
func exerciseMethods(t *testing.T, policy ResponsePolicy, ok interface{}) {
	t.Helper()

	mapResults := map[string]AggregatorResErr{"k1": {Result: ok}, "k2": {Result: ok}}
	sliceResults := []AggregatorResErr{{Result: ok}, {Result: ok}}

	agg := NewResponseAggregator(policy, "")
	if err := agg.Add(ok, nil); err != nil {
		t.Fatalf("policy %v Add error: %v", policy, err)
	}

	agg = NewResponseAggregator(policy, "")
	if err := agg.AddWithKey("key", ok, nil); err != nil {
		t.Fatalf("policy %v AddWithKey error: %v", policy, err)
	}

	agg = NewResponseAggregator(policy, "")
	if err := agg.BatchAdd(mapResults); err != nil {
		t.Fatalf("policy %v BatchAdd error: %v", policy, err)
	}
	if _, err := agg.Result(); err != nil {
		t.Fatalf("policy %v BatchAdd Result error: %v", policy, err)
	}

	agg = NewResponseAggregator(policy, "")
	if err := agg.BatchSlice(sliceResults); err != nil {
		t.Fatalf("policy %v BatchSlice error: %v", policy, err)
	}
}

func TestAllAggregatorMethods(t *testing.T) {
	numeric := []ResponsePolicy{RespAggSum, RespAggMin, RespAggMax}
	for _, p := range numeric {
		exerciseMethods(t, p, int64(3))
	}

	boolean := []ResponsePolicy{RespAggLogicalAnd, RespAggLogicalOr}
	for _, p := range boolean {
		exerciseMethods(t, p, true)
	}

	other := []ResponsePolicy{
		RespDefaultKeyless, RespDefaultHashSlot,
		RespAllSucceeded, RespOneSucceeded, RespSpecial,
	}
	for _, p := range other {
		exerciseMethods(t, p, "value")
	}
}

func TestDefaultKeyedAggregatorOrdering(t *testing.T) {
	agg := &DefaultKeyedAggregator{results: make(map[string]interface{})}

	if err := agg.BatchAddWithKeyOrder(map[string]AggregatorResErr{
		"a": {Result: "va"}, "b": {Result: "vb"},
	}, []string{"b", "a"}); err != nil {
		t.Fatalf("BatchAddWithKeyOrder error: %v", err)
	}

	res, err := agg.Result()
	if err != nil {
		t.Fatalf("Result error: %v", err)
	}
	ordered := res.([]interface{})
	if ordered[0] != "vb" || ordered[1] != "va" {
		t.Errorf("ordered result = %v, want [vb va]", ordered)
	}

	// SetKeyOrder then BatchSlice (non-keyed) path.
	agg2 := NewDefaultKeyedAggregator(nil)
	agg2.SetKeyOrder([]string{"x"})
	if err := agg2.BatchSlice([]AggregatorResErr{{Result: "v"}}); err != nil {
		t.Fatalf("BatchSlice error: %v", err)
	}
	if err := agg2.BatchAdd(map[string]AggregatorResErr{"y": {Result: "w"}}); err != nil {
		t.Fatalf("BatchAdd error: %v", err)
	}
}

func TestConvertHelpersViaSpecial(t *testing.T) {
	// toInt64 / toFloat64 / toBool nil handling through aggregators.
	sum := NewResponseAggregator(RespAggSum, "")
	if err := sum.Add(nil, nil); err != nil {
		t.Errorf("sum Add(nil) error: %v", err)
	}

	and := NewResponseAggregator(RespAggLogicalAnd, "")
	if err := and.Add(nil, nil); err != nil {
		t.Errorf("and Add(nil) error: %v", err)
	}

	// toInt64 with float that is not integral should error via BatchAdd.
	maxAgg := NewResponseAggregator(RespAggMax, "")
	if err := maxAgg.BatchAdd(map[string]AggregatorResErr{"k": {Result: int32(7)}}); err != nil {
		t.Errorf("max BatchAdd(int32) error: %v", err)
	}
}
