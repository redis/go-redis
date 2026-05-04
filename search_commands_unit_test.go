package redis

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

type legacyHybridVector struct {
	value []any
}

func (v legacyHybridVector) Value() []any {
	return v.value
}

func TestHybridVectorBlob(t *testing.T) {
	tests := []struct {
		name    string
		vector  Vector
		want    interface{}
		wantErr string
	}{
		{
			name:   "fp32",
			vector: &VectorFP32{Val: []byte{1, 2, 3, 4}},
			want:   []byte{1, 2, 3, 4},
		},
		{
			name:   "float16",
			vector: &VectorFloat16{Val: []byte{1, 2, 3, 4}},
			want:   []byte{1, 2, 3, 4},
		},
		{
			name:   "bfloat16",
			vector: &VectorBFloat16{Val: []byte{1, 2, 3, 4}},
			want:   []byte{1, 2, 3, 4},
		},
		{
			name:   "float64",
			vector: &VectorFloat64{Val: []byte{1, 2, 3, 4}},
			want:   []byte{1, 2, 3, 4},
		},
		{
			name:   "int8",
			vector: &VectorInt8{Val: []byte{1, 2, 3, 4}},
			want:   []byte{1, 2, 3, 4},
		},
		{
			name:   "uint8",
			vector: &VectorUint8{Val: []byte{1, 2, 3, 4}},
			want:   []byte{1, 2, 3, 4},
		},
		{
			name:   "custom legacy vector",
			vector: legacyHybridVector{value: []any{"CUSTOM", []byte{1, 2, 3, 4}}},
			want:   []byte{1, 2, 3, 4},
		},
		{
			name:    "nil",
			vector:  nil,
			wantErr: "vector data is required",
		},
		{
			name:    "empty fp32",
			vector:  &VectorFP32{},
			wantErr: "vector blob is required",
		},
		{
			name:    "values",
			vector:  &VectorValues{Val: []float64{1, 2}},
			wantErr: "unsupported vector type *redis.VectorValues",
		},
		{
			name:    "ref",
			vector:  &VectorRef{Name: "vec1"},
			wantErr: "unsupported vector type *redis.VectorRef",
		},
		{
			name:    "invalid custom legacy vector",
			vector:  legacyHybridVector{value: []any{"CUSTOM"}},
			wantErr: "vector Value must contain a blob at index 1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := hybridVectorBlob(tt.vector)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("expected error containing %q, got %q", tt.wantErr, err.Error())
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFTHybridWithArgsRejectsUnsupportedVectors(t *testing.T) {
	tests := []struct {
		name    string
		vector  Vector
		wantErr string
	}{
		{
			name:    "values",
			vector:  &VectorValues{Val: []float64{1, 2}},
			wantErr: "unsupported vector type *redis.VectorValues",
		},
		{
			name:    "ref",
			vector:  &VectorRef{Name: "vec1"},
			wantErr: "unsupported vector type *redis.VectorRef",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mockCmdable{}
			c := m.asCmdable()
			cmd := c.FTHybridWithArgs(context.Background(), "idx", &FTHybridOptions{
				SearchExpressions: []FTHybridSearchExpression{{Query: "*"}},
				VectorExpressions: []FTHybridVectorExpression{{
					VectorField: "embedding",
					VectorData:  tt.vector,
				}},
			})
			if cmd.Err() == nil {
				t.Fatalf("expected error containing %q, got nil", tt.wantErr)
			}
			if !strings.Contains(cmd.Err().Error(), tt.wantErr) {
				t.Fatalf("expected error containing %q, got %q", tt.wantErr, cmd.Err().Error())
			}
			if m.lastCmd != nil {
				t.Fatalf("expected command not to be executed")
			}
		})
	}
}

func TestFTHybridWithArgsAcceptsVectorFP32(t *testing.T) {
	tests := []struct {
		name   string
		vector Vector
	}{
		{name: "fp32", vector: &VectorFP32{Val: []byte{1, 2, 3, 4}}},
		{name: "float16", vector: &VectorFloat16{Val: []byte{1, 2, 3, 4}}},
		{name: "bfloat16", vector: &VectorBFloat16{Val: []byte{1, 2, 3, 4}}},
		{name: "float64", vector: &VectorFloat64{Val: []byte{1, 2, 3, 4}}},
		{name: "int8", vector: &VectorInt8{Val: []byte{1, 2, 3, 4}}},
		{name: "uint8", vector: &VectorUint8{Val: []byte{1, 2, 3, 4}}},
		{name: "custom legacy vector", vector: legacyHybridVector{value: []any{"CUSTOM", []byte{1, 2, 3, 4}}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m := &mockCmdable{}
			c := m.asCmdable()
			cmd := c.FTHybridWithArgs(context.Background(), "idx", &FTHybridOptions{
				SearchExpressions: []FTHybridSearchExpression{{Query: "*"}},
				VectorExpressions: []FTHybridVectorExpression{{
					VectorField: "embedding",
					VectorData:  tt.vector,
				}},
			})
			if cmd.Err() != nil {
				t.Fatalf("unexpected error: %v", cmd.Err())
			}
			if m.lastCmd == nil {
				t.Fatalf("expected command to be executed")
			}
			gotCmd, ok := m.lastCmd.(*FTHybridCmd)
			if !ok {
				t.Fatalf("expected FTHybridCmd, got %T", m.lastCmd)
			}
			foundBlob := false
			for _, arg := range gotCmd.args {
				if blob, ok := arg.([]byte); ok && string(blob) == string([]byte{1, 2, 3, 4}) {
					foundBlob = true
					break
				}
			}
			if !foundBlob {
				t.Fatalf("expected raw vector blob in args, got %v", gotCmd.args)
			}
		})
	}
}
