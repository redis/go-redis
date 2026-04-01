package hashtag

import (
	"strconv"
	"testing"
)

func randomString(i int) string {
	return "key-" + strconv.Itoa(i)
}

func TestRendezvous_Empty(t *testing.T) {
	h := NewRendezvousHash(nil)

	if h.Get("any") != "" {
		t.Fatal("expected empty result")
	}
}

func TestRendezvous_SingleNode(t *testing.T) {
	h := NewRendezvousHash([]string{"only"})

	for i := 0; i < 100; i++ {
		if h.Get(randomString(i)) != "only" {
			t.Fatal("single node should always return itself")
		}
	}
}
