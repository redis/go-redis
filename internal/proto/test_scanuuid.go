package proto

import (
	"github.com/google/uuid"
	"testing"
)

func TestScanUUID(t *testing.T) {
	u1 := uuid.New()
	var u2 uuid.UUID

	err := Scan([]byte(u1.String()), &u2)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if u1 != u2 {
		t.Errorf("UUID mismatch, got %v, want %v", u2, u1)
	}
}
