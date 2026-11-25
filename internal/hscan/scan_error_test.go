package hscan

import (
	"errors"
	"strconv"
	"testing"
)

func TestScanPreservesNumericErrors(t *testing.T) {
	for _, tc := range []struct {
		name, value string
		cause       error
	}{
		{"syntax", "invalid", strconv.ErrSyntax},
		{"range", "128", strconv.ErrRange},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var dst struct {
				Number int8   `redis:"number"`
				Valid  string `redis:"valid"`
			}
			err := Scan(&dst, []interface{}{"number", "valid"}, []interface{}{tc.value, "decoded"})
			if !errors.Is(err, tc.cause) {
				t.Fatalf("expected %v in error chain, got %v", tc.cause, err)
			}
			var numErr *strconv.NumError
			if !errors.As(err, &numErr) {
				t.Fatalf("expected *strconv.NumError, got %v", err)
			}
			if numErr.Num != tc.value {
				t.Fatalf("unexpected input: %q", numErr.Num)
			}
			if dst.Valid != "decoded" {
				t.Fatalf("valid field was not decoded: %q", dst.Valid)
			}
		})
	}
}

func TestScanPreservesJoinedNumericErrors(t *testing.T) {
	var dst struct {
		Syntax int8 `redis:"syntax"`
		Range  int8 `redis:"range"`
	}
	err := Scan(&dst, []interface{}{"syntax", "range"}, []interface{}{"invalid", "128"})
	for _, cause := range []error{strconv.ErrSyntax, strconv.ErrRange} {
		if !errors.Is(err, cause) {
			t.Errorf("expected %v in joined error chain, got %v", cause, err)
		}
	}
}
