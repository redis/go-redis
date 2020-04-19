package rendezvoushash

import (
	"fmt"
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {
	hash := New(nil)
	hash.Add("site1", "site2", "site3")

	verifyFn := func(cases map[string]string) {
		for k, v := range cases {
			site := hash.Get(k)
			if site != v {
				t.Errorf("Asking for %s, should have return site %s, returned site %s", k, v, site)
			}
		}
	}

	testCases := map[string]string{
		"key1":  "site2",
		"key2":  "site1",
		"key3":  "site2",
		"key4":  "site1",
		"key5":  "site2",
		"key6":  "site3",
		"key7":  "site1",
		"key8":  "site1",
		"key9":  "site3",
		"key10": "site2",
		"key11": "site3",
		"key12": "site1",
		"key13": "site2",
		"key14": "site2",
		"key15": "site3",
		"key16": "site2",
	}

	verifyFn(testCases)

	hash.Add("site4")

	// remaps existing keys to all sites
	testCases["key1"] = "site4"
	testCases["key2"] = "site4"
	testCases["key9"] = "site4"
	testCases["key10"] = "site4"
	testCases["key11"] = "site4"
	testCases["key12"] = "site4"
	testCases["key15"] = "site4"

	// add new keys
	testCases["key17"] = "site1"
	testCases["key18"] = "site2"
	testCases["key19"] = "site4"
	testCases["key20"] = "site4"
	testCases["key21"] = "site1"
	testCases["key22"] = "site2"

	verifyFn(testCases)
}

func BenchmarkGet8(b *testing.B)   { benchmarkGet(b, 8) }
func BenchmarkGet32(b *testing.B)  { benchmarkGet(b, 32) }
func BenchmarkGet128(b *testing.B) { benchmarkGet(b, 128) }
func BenchmarkGet512(b *testing.B) { benchmarkGet(b, 512) }

func benchmarkGet(b *testing.B, shards int) {

	hash := New(nil)

	var buckets []string
	for i := 0; i < shards; i++ {
		buckets = append(buckets, fmt.Sprintf("shard-%d", i))
	}

	hash.Add(buckets...)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		hash.Get(buckets[i&(shards-1)])
	}
}

func TestDistribution(t *testing.T) {
	hash := New(nil)
	hash.Add("1", "2", "3", "4", "5", "6")

	results := make(map[string]int, 10)

	for i := 0; i < 1000000; i++ {
		key := strconv.Itoa(i)

		site := hash.Get(key)
		if val, ok := results[site]; ok {
			results[site] = val + 1
		} else {
			results[site] = 1
		}
	}

	fmt.Println(results)
}
