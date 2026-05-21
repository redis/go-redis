package helper

import (
	"github.com/redis/go-redis/v9/internal/util"
	"github.com/zeebo/xxh3"
)

func ParseFloat(s string) (float64, error) {
	return util.ParseStringToFloat(s)
}

func MustParseFloat(s string) float64 {
	return util.MustParseFloat(s)
}

// DigestBytes computes the xxh3 hash of the given byte slice.
// This produces the same hash as the Redis DIGEST command, allowing you to
// calculate digests client-side without making a Redis call.
// This is useful for optimistic locking with SetIFDEQ, SetIFDNE, and DelExArgs.
func DigestBytes(data []byte) uint64 {
	return xxh3.Hash(data)
}

// DigestString computes the xxh3 hash of the given string.
// This produces the same hash as the Redis DIGEST command, allowing you to
// calculate digests client-side without making a Redis call.
// This is useful for optimistic locking with SetIFDEQ, SetIFDNE, and DelExArgs.
func DigestString(s string) uint64 {
	return xxh3.HashString(s)
}
