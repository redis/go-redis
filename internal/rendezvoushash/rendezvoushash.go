package rendezvoushash

import (
	"crypto/sha1"
	"hash/crc32"

	"github.com/go-redis/redis/v7/internal"
)

type Map struct {
	hash     internal.Hash
	sites    []string
}

func New(fn internal.Hash) *Map {
	m := &Map{
		hash:     fn,
	}
	if m.hash == nil {
		m.hash = crc32.ChecksumIEEE
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
	maxWeight := uint32(0)
	targetSite := ""
	for _, site := range m.sites {
		hasher := sha1.New()
		hasher.Write([]byte(site + key))
		siteWeight := m.hash(hasher.Sum(nil))

		if siteWeight > maxWeight {
			maxWeight = siteWeight
			targetSite = site
		}
	}

	return targetSite
}
