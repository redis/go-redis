package redis

import "testing"

func TestOptionsCloneNil(t *testing.T) {
	var o *Options
	defer func() {
		if r := recover(); r != nil {
			t.Fatalf("panicked: %v", r)
		}
	}()
	if o.clone() != nil {
		t.Fatal("want nil")
	}
}
