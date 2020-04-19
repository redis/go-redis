/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

// Package consistenthash provides an implementation of a ring hash.
package consistenthash

import (
	"sort"
	"strconv"

	"github.com/cespare/xxhash"
	"github.com/go-redis/redis/v7/internal"
)

type Map struct {
	hash     internal.Hash
	replicas int
	keys     []uint64 // Sorted
	hashMap  map[uint64]string
}

func New(replicas int, fn internal.Hash) *Map {
	m := &Map{
		replicas: replicas,
		hash:     fn,
		hashMap:  make(map[uint64]string),
	}
	if m.hash == nil {
		m.hash = xxhash.Sum64
	}
	return m
}

// Returns true if there are no items available.
func (m *Map) IsEmpty() bool {
	return len(m.keys) == 0
}

// Adds some keys to the hash.
func (m *Map) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < m.replicas; i++ {
			hash := m.hash([]byte(strconv.Itoa(i) + key))
			m.keys = append(m.keys, hash)
			m.hashMap[hash] = key
		}
	}
	sort.Slice(m.keys, func(i, j int) bool {
		return m.keys[i] < m.keys[j]
	})
}

// Gets the closest item in the hash to the provided key.
func (m *Map) Get(key string) string {
	if m.IsEmpty() {
		return ""
	}

	hash := m.hash([]byte(key))

	// Binary search for appropriate replica.
	idx := sort.Search(len(m.keys), func(i int) bool { return m.keys[i] >= hash })

	// Means we have cycled back to the first replica.
	if idx == len(m.keys) {
		idx = 0
	}

	return m.hashMap[m.keys[idx]]
}
