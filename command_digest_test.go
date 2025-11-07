package redis

import (
	"context"
	"fmt"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestDigestCmd(t *testing.T) {
	tests := []struct {
		name     string
		hexStr   string
		expected uint64
		wantErr  bool
	}{
		{
			name:     "zero value",
			hexStr:   "0",
			expected: 0,
			wantErr:  false,
		},
		{
			name:     "small value",
			hexStr:   "ff",
			expected: 255,
			wantErr:  false,
		},
		{
			name:     "medium value",
			hexStr:   "1234abcd",
			expected: 0x1234abcd,
			wantErr:  false,
		},
		{
			name:     "large value",
			hexStr:   "ffffffffffffffff",
			expected: 0xffffffffffffffff,
			wantErr:  false,
		},
		{
			name:     "uppercase hex",
			hexStr:   "DEADBEEF",
			expected: 0xdeadbeef,
			wantErr:  false,
		},
		{
			name:     "mixed case hex",
			hexStr:   "DeAdBeEf",
			expected: 0xdeadbeef,
			wantErr:  false,
		},
		{
			name:     "typical xxh3 hash",
			hexStr:   "a1b2c3d4e5f67890",
			expected: 0xa1b2c3d4e5f67890,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Create a mock reader that returns the hex string in RESP format
			// Format: $<length>\r\n<data>\r\n
			respData := []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(tt.hexStr), tt.hexStr))

			rd := proto.NewReader(newMockConn(respData))

			cmd := NewDigestCmd(context.Background(), "digest", "key")
			err := cmd.readReply(rd)

			if (err != nil) != tt.wantErr {
				t.Errorf("DigestCmd.readReply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if !tt.wantErr && cmd.Val() != tt.expected {
				t.Errorf("DigestCmd.Val() = %d (0x%x), want %d (0x%x)", cmd.Val(), cmd.Val(), tt.expected, tt.expected)
			}
		})
	}
}

func TestDigestCmdResult(t *testing.T) {
	cmd := NewDigestCmd(context.Background(), "digest", "key")
	expected := uint64(0xdeadbeefcafebabe)
	cmd.SetVal(expected)

	val, err := cmd.Result()
	if err != nil {
		t.Errorf("DigestCmd.Result() error = %v", err)
	}

	if val != expected {
		t.Errorf("DigestCmd.Result() = %d (0x%x), want %d (0x%x)", val, val, expected, expected)
	}
}

// mockConn is a simple mock connection for testing
type mockConn struct {
	data []byte
	pos  int
}

func newMockConn(data []byte) *mockConn {
	return &mockConn{data: data}
}

func (c *mockConn) Read(p []byte) (n int, err error) {
	if c.pos >= len(c.data) {
		return 0, nil
	}
	n = copy(p, c.data[c.pos:])
	c.pos += n
	return n, nil
}

