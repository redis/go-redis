package redis

import (
	"testing"
)

// TestParseFTInfo tests the parseFTInfo function with a comprehensive FT.INFO response
// This test uses the actual response structure from Redis with vector fields
func TestParseFTInfo(t *testing.T) {
	// This is the data structure that would be returned by Redis for FT.INFO
	// Based on the redis-cli output provided by the user
	data := map[string]interface{}{
		"index_name":    "rand:42d1f2820b3048b6bc783d4dcdb9094a",
		"index_options": []interface{}{},
		"index_definition": []interface{}{
			"key_type", "HASH",
			"prefixes", []interface{}{"rand:42d1f2820b3048b6bc783d4dcdb9094a"},
			"default_score", "1",
			"indexes_all", false,
		},
		"attributes": []interface{}{
			// prompt field (TEXT)
			[]interface{}{
				"identifier", "prompt",
				"attribute", "prompt",
				"type", "TEXT",
				"WEIGHT", "1",
			},
			// response field (TEXT)
			[]interface{}{
				"identifier", "response",
				"attribute", "response",
				"type", "TEXT",
				"WEIGHT", "1",
			},
			// exact_digest field (TAG)
			[]interface{}{
				"identifier", "exact_digest",
				"attribute", "exact_digest",
				"type", "TAG",
				"SEPARATOR", ",",
			},
			// prompt_vector field (VECTOR)
			[]interface{}{
				"identifier", "prompt_vector",
				"attribute", "prompt_vector",
				"type", "VECTOR",
				"algorithm", "HNSW",
				"data_type", "FLOAT32",
				"dim", int64(1536),
				"distance_metric", "COSINE",
				"M", int64(16),
				"ef_construction", int64(64),
			},
		},
		"num_docs":                    int64(0),
		"max_doc_id":                  int64(0),
		"num_terms":                   int64(0),
		"num_records":                 int64(0),
		"inverted_sz_mb":              "0",
		"vector_index_sz_mb":          "0",
		"total_inverted_index_blocks": int64(0),
		"offset_vectors_sz_mb":        "0",
		"doc_table_size_mb":           "0.01532745361328125",
		"sortable_values_size_mb":     "0",
		"key_table_size_mb":           "2.288818359375e-5",
		"tag_overhead_sz_mb":          "0",
		"text_overhead_sz_mb":         "0",
		"total_index_memory_sz_mb":    "0.015350341796875",
		"geoshapes_sz_mb":             "0",
		"records_per_doc_avg":         "nan",
		"bytes_per_record_avg":        "nan",
		"offsets_per_term_avg":        "nan",
		"offset_bits_per_record_avg":  "nan",
		"hash_indexing_failures":      int64(0),
		"total_indexing_time":         "0",
		"indexing":                    int64(0),
		"percent_indexed":             "1",
		"number_of_uses":              int64(2),
		"cleaning":                    int64(0),
		"gc_stats": []interface{}{
			"bytes_collected", "0",
			"total_ms_run", "0",
			"total_cycles", "0",
			"average_cycle_time_ms", "nan",
			"last_run_time_ms", "0",
			"gc_numeric_trees_missed", "0",
			"gc_blocks_denied", "0",
		},
		"cursor_stats": []interface{}{
			"global_idle", int64(0),
			"global_total", int64(0),
			"index_capacity", int64(128),
			"index_total", int64(0),
		},
		"dialect_stats": []interface{}{
			"dialect_1", int64(0),
			"dialect_2", int64(0),
			"dialect_3", int64(0),
			"dialect_4", int64(0),
		},
		"Index Errors": []interface{}{
			"indexing failures", int64(0),
			"last indexing error", "N/A",
			"last indexing error key", "N/A",
			"background indexing status", "OK",
		},
		"field statistics": []interface{}{
			[]interface{}{
				"identifier", "prompt",
				"attribute", "prompt",
				"Index Errors", []interface{}{
					"indexing failures", int64(0),
					"last indexing error", "N/A",
					"last indexing error key", "N/A",
				},
			},
			[]interface{}{
				"identifier", "response",
				"attribute", "response",
				"Index Errors", []interface{}{
					"indexing failures", int64(0),
					"last indexing error", "N/A",
					"last indexing error key", "N/A",
				},
			},
			[]interface{}{
				"identifier", "exact_digest",
				"attribute", "exact_digest",
				"Index Errors", []interface{}{
					"indexing failures", int64(0),
					"last indexing error", "N/A",
					"last indexing error key", "N/A",
				},
			},
			[]interface{}{
				"identifier", "prompt_vector",
				"attribute", "prompt_vector",
				"Index Errors", []interface{}{
					"indexing failures", int64(0),
					"last indexing error", "N/A",
					"last indexing error key", "N/A",
				},
				"memory", int64(0),
				"marked_deleted", int64(0),
			},
		},
	}

	// Parse the data
	result, err := parseFTInfo(data)
	if err != nil {
		t.Fatalf("parseFTInfo failed: %v", err)
	}

	// Validate index name
	if result.IndexName != "rand:42d1f2820b3048b6bc783d4dcdb9094a" {
		t.Errorf("IndexName = %v, want %v", result.IndexName, "rand:42d1f2820b3048b6bc783d4dcdb9094a")
	}

	// Validate index definition
	if result.IndexDefinition.KeyType != "HASH" {
		t.Errorf("IndexDefinition.KeyType = %v, want HASH", result.IndexDefinition.KeyType)
	}
	if len(result.IndexDefinition.Prefixes) != 1 || result.IndexDefinition.Prefixes[0] != "rand:42d1f2820b3048b6bc783d4dcdb9094a" {
		t.Errorf("IndexDefinition.Prefixes = %v, want [rand:42d1f2820b3048b6bc783d4dcdb9094a]", result.IndexDefinition.Prefixes)
	}
	if result.IndexDefinition.DefaultScore != 1.0 {
		t.Errorf("IndexDefinition.DefaultScore = %v, want 1.0", result.IndexDefinition.DefaultScore)
	}

	// Validate attributes
	if len(result.Attributes) != 4 {
		t.Fatalf("len(Attributes) = %v, want 4", len(result.Attributes))
	}

	// Check prompt field (TEXT)
	promptAttr := result.Attributes[0]
	if promptAttr.Identifier != "prompt" {
		t.Errorf("Attributes[0].Identifier = %v, want prompt", promptAttr.Identifier)
	}
	if promptAttr.Attribute != "prompt" {
		t.Errorf("Attributes[0].Attribute = %v, want prompt", promptAttr.Attribute)
	}
	if promptAttr.Type != "TEXT" {
		t.Errorf("Attributes[0].Type = %v, want TEXT", promptAttr.Type)
	}
	if promptAttr.Weight != 1.0 {
		t.Errorf("Attributes[0].Weight = %v, want 1.0", promptAttr.Weight)
	}

	// Check response field (TEXT)
	responseAttr := result.Attributes[1]
	if responseAttr.Identifier != "response" {
		t.Errorf("Attributes[1].Identifier = %v, want response", responseAttr.Identifier)
	}
	if responseAttr.Attribute != "response" {
		t.Errorf("Attributes[1].Attribute = %v, want response", responseAttr.Attribute)
	}
	if responseAttr.Type != "TEXT" {
		t.Errorf("Attributes[1].Type = %v, want TEXT", responseAttr.Type)
	}

	// Check exact_digest field (TAG)
	tagAttr := result.Attributes[2]
	if tagAttr.Identifier != "exact_digest" {
		t.Errorf("Attributes[2].Identifier = %v, want exact_digest", tagAttr.Identifier)
	}
	if tagAttr.Attribute != "exact_digest" {
		t.Errorf("Attributes[2].Attribute = %v, want exact_digest", tagAttr.Attribute)
	}
	if tagAttr.Type != "TAG" {
		t.Errorf("Attributes[2].Type = %v, want TAG", tagAttr.Type)
	}

	// Check prompt_vector field (VECTOR)
	vectorAttr := result.Attributes[3]
	if vectorAttr.Identifier != "prompt_vector" {
		t.Errorf("Attributes[3].Identifier = %v, want prompt_vector", vectorAttr.Identifier)
	}
	if vectorAttr.Attribute != "prompt_vector" {
		t.Errorf("Attributes[3].Attribute = %v, want prompt_vector", vectorAttr.Attribute)
	}
	if vectorAttr.Type != "VECTOR" {
		t.Errorf("Attributes[3].Type = %v, want VECTOR", vectorAttr.Type)
	}
	if vectorAttr.Algorithm != "HNSW" {
		t.Errorf("Attributes[3].Algorithm = %v, want HNSW", vectorAttr.Algorithm)
	}
	if vectorAttr.DataType != "FLOAT32" {
		t.Errorf("Attributes[3].DataType = %v, want FLOAT32", vectorAttr.DataType)
	}
	if vectorAttr.Dim != 1536 {
		t.Errorf("Attributes[3].Dim = %v, want 1536", vectorAttr.Dim)
	}
	if vectorAttr.DistanceMetric != "COSINE" {
		t.Errorf("Attributes[3].DistanceMetric = %v, want COSINE", vectorAttr.DistanceMetric)
	}
	if vectorAttr.M != 16 {
		t.Errorf("Attributes[3].M = %v, want 16", vectorAttr.M)
	}
	if vectorAttr.EFConstruction != 64 {
		t.Errorf("Attributes[3].EFConstruction = %v, want 64", vectorAttr.EFConstruction)
	}

	// Validate numeric fields
	if result.NumDocs != 0 {
		t.Errorf("NumDocs = %v, want 0", result.NumDocs)
	}
	if result.MaxDocID != 0 {
		t.Errorf("MaxDocID = %v, want 0", result.MaxDocID)
	}
	if result.NumTerms != 0 {
		t.Errorf("NumTerms = %v, want 0", result.NumTerms)
	}
	if result.NumRecords != 0 {
		t.Errorf("NumRecords = %v, want 0", result.NumRecords)
	}
	if result.Indexing != 0 {
		t.Errorf("Indexing = %v, want 0", result.Indexing)
	}
	if result.PercentIndexed != 1.0 {
		t.Errorf("PercentIndexed = %v, want 1.0", result.PercentIndexed)
	}
	if result.HashIndexingFailures != 0 {
		t.Errorf("HashIndexingFailures = %v, want 0", result.HashIndexingFailures)
	}
	if result.Cleaning != 0 {
		t.Errorf("Cleaning = %v, want 0", result.Cleaning)
	}
	if result.NumberOfUses != 2 {
		t.Errorf("NumberOfUses = %v, want 2", result.NumberOfUses)
	}

	// Validate average stats (should be "nan" for empty index)
	if result.RecordsPerDocAvg != "nan" {
		t.Errorf("RecordsPerDocAvg = %v, want nan", result.RecordsPerDocAvg)
	}
	if result.BytesPerRecordAvg != "nan" {
		t.Errorf("BytesPerRecordAvg = %v, want nan", result.BytesPerRecordAvg)
	}
	if result.OffsetsPerTermAvg != "nan" {
		t.Errorf("OffsetsPerTermAvg = %v, want nan", result.OffsetsPerTermAvg)
	}
	if result.OffsetBitsPerRecordAvg != "nan" {
		t.Errorf("OffsetBitsPerRecordAvg = %v, want nan", result.OffsetBitsPerRecordAvg)
	}

	// Validate cursor stats
	if result.CursorStats.GlobalIdle != 0 {
		t.Errorf("CursorStats.GlobalIdle = %v, want 0", result.CursorStats.GlobalIdle)
	}
	if result.CursorStats.GlobalTotal != 0 {
		t.Errorf("CursorStats.GlobalTotal = %v, want 0", result.CursorStats.GlobalTotal)
	}
	if result.CursorStats.IndexCapacity != 128 {
		t.Errorf("CursorStats.IndexCapacity = %v, want 128", result.CursorStats.IndexCapacity)
	}
	if result.CursorStats.IndexTotal != 0 {
		t.Errorf("CursorStats.IndexTotal = %v, want 0", result.CursorStats.IndexTotal)
	}

	// Validate dialect stats
	if result.DialectStats["dialect_1"] != 0 {
		t.Errorf("DialectStats[dialect_1] = %v, want 0", result.DialectStats["dialect_1"])
	}
	if result.DialectStats["dialect_2"] != 0 {
		t.Errorf("DialectStats[dialect_2] = %v, want 0", result.DialectStats["dialect_2"])
	}
	if result.DialectStats["dialect_3"] != 0 {
		t.Errorf("DialectStats[dialect_3] = %v, want 0", result.DialectStats["dialect_3"])
	}
	if result.DialectStats["dialect_4"] != 0 {
		t.Errorf("DialectStats[dialect_4] = %v, want 0", result.DialectStats["dialect_4"])
	}

	// Validate GC stats
	if result.GCStats.BytesCollected != 0 {
		t.Errorf("GCStats.BytesCollected = %v, want 0", result.GCStats.BytesCollected)
	}
	if result.GCStats.TotalMsRun != 0 {
		t.Errorf("GCStats.TotalMsRun = %v, want 0", result.GCStats.TotalMsRun)
	}
	if result.GCStats.TotalCycles != 0 {
		t.Errorf("GCStats.TotalCycles = %v, want 0", result.GCStats.TotalCycles)
	}
	if result.GCStats.AverageCycleTimeMs != "nan" {
		t.Errorf("GCStats.AverageCycleTimeMs = %v, want nan", result.GCStats.AverageCycleTimeMs)
	}

	// Validate Index Errors
	if result.IndexErrors.IndexingFailures != 0 {
		t.Errorf("IndexErrors.IndexingFailures = %v, want 0", result.IndexErrors.IndexingFailures)
	}
	if result.IndexErrors.LastIndexingError != "N/A" {
		t.Errorf("IndexErrors.LastIndexingError = %v, want N/A", result.IndexErrors.LastIndexingError)
	}
	if result.IndexErrors.LastIndexingErrorKey != "N/A" {
		t.Errorf("IndexErrors.LastIndexingErrorKey = %v, want N/A", result.IndexErrors.LastIndexingErrorKey)
	}

	// Validate field statistics
	if len(result.FieldStatistics) != 4 {
		t.Fatalf("len(FieldStatistics) = %v, want 4", len(result.FieldStatistics))
	}

	expectedIdentifiers := map[string]bool{
		"prompt":        true,
		"response":      true,
		"exact_digest":  true,
		"prompt_vector": true,
	}

	for _, fieldStat := range result.FieldStatistics {
		if !expectedIdentifiers[fieldStat.Identifier] {
			t.Errorf("Unexpected field statistic identifier: %v", fieldStat.Identifier)
		}
		if fieldStat.IndexErrors.IndexingFailures != 0 {
			t.Errorf("FieldStatistic[%s].IndexErrors.IndexingFailures = %v, want 0", fieldStat.Identifier, fieldStat.IndexErrors.IndexingFailures)
		}
		if fieldStat.IndexErrors.LastIndexingError != "N/A" {
			t.Errorf("FieldStatistic[%s].IndexErrors.LastIndexingError = %v, want N/A", fieldStat.Identifier, fieldStat.IndexErrors.LastIndexingError)
		}
		if fieldStat.IndexErrors.LastIndexingErrorKey != "N/A" {
			t.Errorf("FieldStatistic[%s].IndexErrors.LastIndexingErrorKey = %v, want N/A", fieldStat.Identifier, fieldStat.IndexErrors.LastIndexingErrorKey)
		}
	}

	t.Logf("Successfully parsed FT.INFO response with %d attributes", len(result.Attributes))
}
