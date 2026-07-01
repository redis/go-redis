package proto

import (
    "bytes"
    "testing"
)

func FuzzReadReply(f *testing.F) {
    // Seed corpus with valid RESP3 replies
    for _, s := range []string{
        "+OK\r\n", ":123\r\n", "-ERR x\r\n",
        "$5\r\nhello\r\n", "*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n",
        "_\r\n", "#t\r\n", ",3.14\r\n",
        "%1\r\n+k\r\n+v\r\n", "~2\r\n+a\r\n+b\r\n",
        ">2\r\n+pub\r\n+msg\r\n",
    } {
        f.Add([]byte(s))
    }
    f.Fuzz(func(t *testing.T, data []byte) {
        r := NewReader(bytes.NewReader(data))
        _, _ = r.ReadReply()
    })
}

// Explicit test for the unhashable key fix
func TestReadMapRejectsUnhashableKey(t *testing.T) {
    // RESP3 map with unhashable key (nested map)
    data := []byte("%1\r\n%0\r\n+\r\n")
    r := NewReader(bytes.NewReader(data))
    _, err := r.ReadReply()
    if err == nil {
        t.Fatal("expected error for unhashable map key, got nil")
    }
    if !strings.Contains(err.Error(), "scalar type") {
        t.Fatalf("expected 'scalar type' in error, got: %v", err)
    }
}