package hashtag

import (
	"fmt"
	"testing"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"
)

func TestGinkgoSuite(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "hashtag")
}

// https://redis.io/docs/latest/operate/oss_and_stack/reference/cluster-spec#key-distribution-model.
func TestCRC16(t *testing.T) {
	tests := []struct {
		s string
		n uint16
	}{
		{"123456789", 0x31C3},
		{string([]byte{83, 153, 134, 118, 229, 214, 244, 75, 140, 37, 215, 215}), 21847},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("CRC16_Case_%d", i+1), func(t *testing.T) {
			got := crc16sum(test.s)
			if got != test.n {
				t.Errorf("Expected %d, got %d", test.n, got)
			}
		})
	}
}

func TestHashSlot(t *testing.T) {
	tests := []struct {
		key  string
		slot int
	}{
		{"123456789", 12739},
		{"{}foo", 9500},
		{"foo{}", 5542},
		{"foo{}{bar}", 8363},
		{string([]byte{83, 153, 134, 118, 229, 214, 244, 75, 140, 37, 215, 215}), 5463},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("HashSlot_Case_%d", i+1), func(t *testing.T) {
			got := Slot(test.key)
			if got != test.slot {
				t.Errorf("Slot(%q) = %d, want %d", test.key, got, test.slot)
			}
		})
	}
	t.Run("EmptyKey", func(t *testing.T) {
		slot := Slot("")
		if slot < 0 || slot >= 16384 {
			t.Errorf("Slot(\"\") = %d, want value in [0, 16384)", slot)
		}
	})

}

func TestHashSlotTags(t *testing.T) {
	tests := []struct {
		one, two string
	}{
		{"foo{bar}", "bar"},
		{"{foo}bar", "foo"},
		{"{user1000}.following", "{user1000}.followers"},
		{"foo{{bar}}zap", "{bar"},
		{"foo{bar}{zap}", "bar"},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("HashSlotTags_Case_%d", i+1), func(t *testing.T) {
			got := Slot(test.one)
			want := Slot(test.two)
			if got != want {
				t.Errorf(
					"Slot(%q) = %d, Slot(%q) = %d; want equal slots",
					test.one, got, test.two, want,
				)
			}
		})
	}
}

func TestPresent(t *testing.T) {
	tests := []struct {
		key     string
		present bool
	}{
		{"123456789", false},
		{"{}foo", false},
		{"foo{}", false},
		{"foo{}{bar}", false},
		{"", false},
		{string([]byte{83, 153, 134, 118, 229, 214, 244, 75, 140, 37, 215, 215}), false},
		{"foo{bar}", true},
		{"{foo}bar", true},
		{"{user1000}.following", true},
		{"foo{{bar}}zap", true},
		{"foo{bar}{zap}", true},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("Present_Case_%d", i+1), func(t *testing.T) {
			got := Present(test.key)
			if got != test.present {
				t.Errorf("Present(%q) = %t, want %t", test.key, got, test.present)
			}
		})
	}
}
