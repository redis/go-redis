package redis

import (
	"context"
	"reflect"
	"testing"
)

func TestSearchCollect_String(t *testing.T) {
	if got := SearchCollect.String(); got != "COLLECT" {
		t.Fatalf("SearchCollect.String() = %q, want %q", got, "COLLECT")
	}
}

// TestNewCollectReducer_Wire is the wire oracle: an options-struct COLLECT
// query must serialize to the exact REDUCE COLLECT clause, with the argument
// count computed from the assembled tokens and every name @-prefixed. Field
// names are supplied with mixed prefixes to exercise normalization.
func TestNewCollectReducer_Wire(t *testing.T) {
	m := &mockCmdable{}
	c := m.asCmdable()

	reducer, err := NewCollectReducer(FTAggregateCollect{
		Fields: []string{"title", "@rating", "year"}, // mixed @-prefix
		SortBy: []FTAggregateSortBy{{FieldName: "rating", Desc: true}},
		Limit:  &FTAggregateCollectLimit{Offset: 0, Count: 5},
		As:     "top_movies",
	})
	if err != nil {
		t.Fatalf("NewCollectReducer: %v", err)
	}

	opts := &FTAggregateOptions{
		GroupBy: []FTAggregateGroupBy{{
			Fields: []interface{}{"@genre"},
			Reduce: []FTAggregateReducer{reducer},
		}},
	}
	cmd := c.FTAggregateWithArgs(context.Background(), "idx", "*", opts)
	if cmd.Err() != nil {
		t.Fatalf("unexpected error: %v", cmd.Err())
	}

	want := []interface{}{
		"FT.AGGREGATE", "idx", "*",
		"GROUPBY", 1, "@genre",
		"REDUCE", "COLLECT", 12,
		"FIELDS", 3, "@title", "@rating", "@year",
		"SORTBY", 2, "@rating", "DESC",
		"LIMIT", 0, 5,
		"AS", "top_movies",
		"DIALECT", 2,
	}
	if !reflect.DeepEqual(cmd.args, want) {
		t.Fatalf("args mismatch:\n got:  %v\n want: %v", cmd.args, want)
	}
	// narg must equal the token count.
	if got := len(reducer.Args); got != 12 {
		t.Fatalf("narg = %d, want 12", got)
	}
}

func TestBuildCollectArgs(t *testing.T) {
	tests := []struct {
		name    string
		in      FTAggregateCollect
		want    []interface{}
		wantErr bool
	}{
		{
			name: "fields all",
			in:   FTAggregateCollect{FieldsAll: true},
			want: []interface{}{"FIELDS", "*"},
		},
		{
			name: "fields all wins over explicit list",
			in:   FTAggregateCollect{FieldsAll: true, Fields: []string{"x"}},
			want: []interface{}{"FIELDS", "*"},
		},
		{
			name: "explicit fields normalized",
			in:   FTAggregateCollect{Fields: []string{"a", "@b", "@@c"}},
			want: []interface{}{"FIELDS", 3, "@a", "@b", "@c"},
		},
		{
			name: "sortby default asc emits nothing",
			in: FTAggregateCollect{
				Fields: []string{"a"},
				SortBy: []FTAggregateSortBy{{FieldName: "a"}},
			},
			want: []interface{}{"FIELDS", 1, "@a", "SORTBY", 1, "@a"},
		},
		{
			name: "sortby explicit asc emits ASC",
			in: FTAggregateCollect{
				Fields: []string{"a"},
				SortBy: []FTAggregateSortBy{{FieldName: "a", Asc: true}},
			},
			want: []interface{}{"FIELDS", 1, "@a", "SORTBY", 2, "@a", "ASC"},
		},
		{
			name: "sortby multi keys mixed direction",
			in: FTAggregateCollect{
				Fields: []string{"a"},
				SortBy: []FTAggregateSortBy{{FieldName: "a", Desc: true}, {FieldName: "b"}},
			},
			want: []interface{}{"FIELDS", 1, "@a", "SORTBY", 3, "@a", "DESC", "@b"},
		},
		{
			name: "distinct emitted",
			in:   FTAggregateCollect{FieldsAll: true, Distinct: true},
			want: []interface{}{"FIELDS", "*", "DISTINCT"},
		},
		{
			name: "limit emitted",
			in:   FTAggregateCollect{FieldsAll: true, Limit: &FTAggregateCollectLimit{Offset: 2, Count: 7}},
			want: []interface{}{"FIELDS", "*", "LIMIT", 2, 7},
		},
		{
			name:    "no fields selector is an error",
			in:      FTAggregateCollect{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildCollectArgs(tt.in)
			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error, got args %v", got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("args mismatch:\n got:  %v\n want: %v", got, tt.want)
			}
		})
	}
}

func TestEnsureAtPrefix(t *testing.T) {
	cases := map[string]string{
		"name":    "@name",
		"@name":   "@name",
		"@@name":  "@name",
		"@@@name": "@name",
		"__key":   "@__key",
		"@__key":  "@__key",
		"":        "@",
	}
	for in, want := range cases {
		if got := ensureAtPrefix(in); got != want {
			t.Errorf("ensureAtPrefix(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNewCollectReducer_Error(t *testing.T) {
	if _, err := NewCollectReducer(FTAggregateCollect{}); err == nil {
		t.Fatal("expected error for missing FIELDS selector")
	}
}

// TestAggregateBuilder_Collect checks the builder appends a correctly rendered
// COLLECT reducer to the last GROUPBY step. The wire serialization itself is
// covered by TestNewCollectReducer_Wire (same serializer path).
func TestAggregateBuilder_Collect(t *testing.T) {
	b := &AggregateBuilder{options: &FTAggregateOptions{LimitOffset: -1}}
	b.GroupBy("@genre").Collect(FTAggregateCollect{
		Fields: []string{"title"},
		Limit:  &FTAggregateCollectLimit{Offset: 0, Count: 3},
		As:     "top",
	})
	if b.err != nil {
		t.Fatalf("unexpected builder error: %v", b.err)
	}
	if n := len(b.options.Steps); n != 1 {
		t.Fatalf("steps = %d, want 1", n)
	}
	g := b.options.Steps[0].GroupBy
	if g == nil || len(g.Reduce) != 1 {
		t.Fatalf("expected one reducer on the GroupBy step, got %+v", g)
	}
	r := g.Reduce[0]
	if r.Reducer != SearchCollect || r.As != "top" {
		t.Fatalf("reducer = %v as=%q, want COLLECT as=top", r.Reducer, r.As)
	}
	want := []interface{}{"FIELDS", 1, "@title", "LIMIT", 0, 3}
	if !reflect.DeepEqual(r.Args, want) {
		t.Fatalf("reducer args mismatch:\n got:  %v\n want: %v", r.Args, want)
	}
}

func TestAggregateBuilder_Collect_Errors(t *testing.T) {
	t.Run("without groupby", func(t *testing.T) {
		b := &AggregateBuilder{options: &FTAggregateOptions{LimitOffset: -1}}
		b.Collect(FTAggregateCollect{FieldsAll: true})
		if b.err == nil {
			t.Fatal("expected error: Collect must follow a GroupBy step")
		}
	})
	t.Run("invalid options", func(t *testing.T) {
		b := &AggregateBuilder{options: &FTAggregateOptions{LimitOffset: -1}}
		b.GroupBy("@genre").Collect(FTAggregateCollect{})
		if b.err == nil {
			t.Fatal("expected error: missing FIELDS selector")
		}
	})
}

// TestAggregateRow_Collect_RESP3 decodes the RESP3 map form, including a sparse
// entry that omits a projected field.
func TestAggregateRow_Collect_RESP3(t *testing.T) {
	row := AggregateRow{Fields: map[string]interface{}{
		"fruits": []interface{}{
			map[interface{}]interface{}{"name": "apple", "sweetness": "4"},
			map[interface{}]interface{}{"name": "strawberry"}, // sparse: no sweetness
		},
	}}

	col, err := row.Collect("fruits")
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(col) != 2 {
		t.Fatalf("len = %d, want 2", len(col))
	}
	if col[0]["name"] != "apple" || col[0]["sweetness"] != "4" {
		t.Fatalf("entry 0 = %v", col[0])
	}
	if col[1]["name"] != "strawberry" {
		t.Fatalf("entry 1 name = %v", col[1]["name"])
	}
	if _, ok := col[1]["sweetness"]; ok {
		t.Fatalf("entry 1 should be sparse, has sweetness: %v", col[1])
	}
}

// TestAggregateRow_Collect_RESP2 decodes the RESP2 flat key/value array form.
func TestAggregateRow_Collect_RESP2(t *testing.T) {
	row := AggregateRow{Fields: map[string]interface{}{
		"fruits": []interface{}{
			[]interface{}{"name", "apple", "sweetness", "4"},
			[]interface{}{"name", "strawberry"}, // sparse
		},
	}}

	col, err := row.Collect("fruits")
	if err != nil {
		t.Fatalf("Collect: %v", err)
	}
	if len(col) != 2 {
		t.Fatalf("len = %d, want 2", len(col))
	}
	if col[0]["name"] != "apple" || col[0]["sweetness"] != "4" {
		t.Fatalf("entry 0 = %v", col[0])
	}
	if _, ok := col[1]["sweetness"]; ok {
		t.Fatalf("entry 1 should be sparse: %v", col[1])
	}
}

func TestAggregateRow_Collect_EdgeCases(t *testing.T) {
	t.Run("absent alias", func(t *testing.T) {
		row := AggregateRow{Fields: map[string]interface{}{}}
		col, err := row.Collect("missing")
		if err != nil || col != nil {
			t.Fatalf("got (%v, %v), want (nil, nil)", col, err)
		}
	})
	t.Run("non-array value", func(t *testing.T) {
		row := AggregateRow{Fields: map[string]interface{}{"x": "scalar"}}
		if _, err := row.Collect("x"); err == nil {
			t.Fatal("expected error for non-array COLLECT value")
		}
	})
	t.Run("odd-length resp2 entry", func(t *testing.T) {
		row := AggregateRow{Fields: map[string]interface{}{
			"x": []interface{}{[]interface{}{"name", "apple", "dangling"}},
		}}
		if _, err := row.Collect("x"); err == nil {
			t.Fatal("expected error for odd-length key/value entry")
		}
	})
	t.Run("bad entry type", func(t *testing.T) {
		row := AggregateRow{Fields: map[string]interface{}{
			"x": []interface{}{42},
		}}
		if _, err := row.Collect("x"); err == nil {
			t.Fatal("expected error for non-map/non-array entry")
		}
	})
}
