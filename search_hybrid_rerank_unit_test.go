package redis

import (
	"context"
	"testing"
)

// TestFTHybridWithArgs_Rerank verifies RERANK (Redis 8.8.7+) is emitted inside
// the counted KNN/RANGE parameter list, and rejected without a method.
func TestFTHybridWithArgs_Rerank(t *testing.T) {
	build := func(expr FTHybridVectorExpression) (*FTHybridCmd, error) {
		m := &mockCmdable{}
		expr.VectorField = "embedding"
		expr.VectorData = &VectorFP32{Val: []byte{1, 2, 3, 4}}
		expr.VectorParamName = "vec"
		cmd := m.asCmdable().FTHybridWithArgs(context.Background(), "idx", &FTHybridOptions{
			SearchExpressions: []FTHybridSearchExpression{{Query: "*"}},
			VectorExpressions: []FTHybridVectorExpression{expr},
		})
		if cmd.Err() != nil {
			return nil, cmd.Err()
		}
		return m.lastCmd.(*FTHybridCmd), nil
	}

	cases := []struct {
		name string
		expr FTHybridVectorExpression
		want []interface{}
	}{
		{
			"knn true",
			FTHybridVectorExpression{Method: "KNN", MethodParams: []interface{}{"K", 10}, Rerank: FTRerankEnabled},
			[]interface{}{"KNN", 4, "K", 10, "RERANK", "TRUE"},
		},
		{
			"range explicit false",
			FTHybridVectorExpression{Method: "RANGE", MethodParams: []interface{}{"RADIUS", 5}, Rerank: FTRerankDisabled},
			[]interface{}{"RANGE", 4, "RADIUS", 5, "RERANK", "FALSE"},
		},
		{
			"rerank only",
			FTHybridVectorExpression{Method: "KNN", Rerank: FTRerankEnabled},
			[]interface{}{"KNN", 2, "RERANK", "TRUE"},
		},
		{
			"default omits rerank",
			FTHybridVectorExpression{Method: "KNN", MethodParams: []interface{}{"K", 10}},
			[]interface{}{"KNN", 2, "K", 10},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd, err := build(tc.expr)
			if err != nil {
				t.Fatal(err)
			}
			start := -1
			for i, a := range cmd.args {
				if s, ok := a.(string); ok && s == tc.want[0] {
					start = i
					break
				}
			}
			if start < 0 || start+len(tc.want) > len(cmd.args) {
				t.Fatalf("%v not found in %v", tc.want[0], cmd.args)
			}
			for i, w := range tc.want {
				if cmd.args[start+i] != w {
					t.Fatalf("got %v, want %v", cmd.args[start:start+len(tc.want)], tc.want)
				}
			}
		})
	}

	t.Run("requires method", func(t *testing.T) {
		if _, err := build(FTHybridVectorExpression{Rerank: FTRerankEnabled}); err == nil {
			t.Fatal("expected error")
		}
	})
}
