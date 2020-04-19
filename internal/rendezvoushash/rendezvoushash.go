package rendezvoushash

import (
	"github.com/cespare/xxhash"
	"github.com/go-redis/redis/v7/internal"
)

type Map struct {
	hash  internal.Hash
	sites []string
}

func New(fn internal.Hash) *Map {
	m := &Map{
		hash: fn,
	}
	if m.hash == nil {
		m.hash = xxhash.Sum64
	}
	return m
}

// Returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.sites) == 0
}

// Adds some keys to the hash.
func (m *Map) Add(sites ...string) {
	for _, site := range sites {
		m.sites = append(m.sites, site)
	}
}

// Gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	// find the site that, when hashed with the key, yields the largest weight
	var targetSite string

	var maxWeight uint64
	buf := make([]byte, len(key), 2*len(key))
	copy(buf, key)
	for _, site := range m.sites {
		buf = buf[:len(key)]
		buf = append(buf, site...)
		siteWeight := m.hash(buf)

		if siteWeight > maxWeight {
			maxWeight = siteWeight
			targetSite = site
		}
	}

	return targetSite
}
