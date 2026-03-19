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

// TestParseFTSearchRESP3 tests the RESP3 parsing for FT.SEARCH results
func TestParseFTSearchRESP3(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		expected FTSearchResult
	}{
		{
			name: "empty result",
			input: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(0),
				"format":        "STRING",
				"results":       []interface{}{},
				"warning":       []interface{}{},
			},
			expected: FTSearchResult{
				Total:    0,
				Docs:     []Document{},
				Warnings: []string{},
			},
		},
		{
			name: "simple query result",
			input: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(2),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"id": "usr:david",
						"extra_attributes": map[string]interface{}{
							"first_name": "David",
							"last_name":  "Maier",
						},
						"values": []interface{}{},
					},
					map[string]interface{}{
						"id": "usr:shaya",
						"extra_attributes": map[string]interface{}{
							"first_name": "Shaya",
							"last_name":  "Potter",
						},
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{},
			},
			expected: FTSearchResult{
				Total: 2,
				Docs: []Document{
					{
						ID:     "usr:david",
						Fields: map[string]string{"first_name": "David", "last_name": "Maier"},
					},
					{
						ID:     "usr:shaya",
						Fields: map[string]string{"first_name": "Shaya", "last_name": "Potter"},
					},
				},
				Warnings: []string{},
			},
		},
		{
			name: "result with scores",
			input: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(1),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"id":    "bicycle:hash:1",
						"score": float64(1.5),
						"extra_attributes": map[string]interface{}{
							"price": "1200",
							"brand": "Bicyk",
						},
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{},
			},
			expected: FTSearchResult{
				Total: 1,
				Docs: []Document{
					{
						ID:     "bicycle:hash:1",
						Score:  ptrFloat64(1.5),
						Fields: map[string]string{"price": "1200", "brand": "Bicyk"},
					},
				},
				Warnings: []string{},
			},
		},
		{
			name: "result with warnings",
			input: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(1),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"id": "doc:1",
						"extra_attributes": map[string]interface{}{
							"field1": "value1",
						},
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{"Timeout limit was reached"},
			},
			expected: FTSearchResult{
				Total: 1,
				Docs: []Document{
					{
						ID:     "doc:1",
						Fields: map[string]string{"field1": "value1"},
					},
				},
				Warnings: []string{"Timeout limit was reached"},
			},
		},
		{
			name: "NOCONTENT result",
			input: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(10),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"id":     "bicycle:hash:2",
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{},
			},
			expected: FTSearchResult{
				Total: 10,
				Docs: []Document{
					{
						ID:     "bicycle:hash:2",
						Fields: map[string]string{},
					},
				},
				Warnings: []string{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := processFTSearchMapResult(tt.input)
			if err != nil {
				t.Fatalf("processFTSearchMapResult() error = %v", err)
			}

			if result.Total != tt.expected.Total {
				t.Errorf("Total = %v, want %v", result.Total, tt.expected.Total)
			}

			if len(result.Docs) != len(tt.expected.Docs) {
				t.Errorf("len(Docs) = %v, want %v", len(result.Docs), len(tt.expected.Docs))
			}

			for i, doc := range result.Docs {
				if doc.ID != tt.expected.Docs[i].ID {
					t.Errorf("Docs[%d].ID = %v, want %v", i, doc.ID, tt.expected.Docs[i].ID)
				}
				if tt.expected.Docs[i].Score != nil {
					if doc.Score == nil || *doc.Score != *tt.expected.Docs[i].Score {
						t.Errorf("Docs[%d].Score = %v, want %v", i, doc.Score, tt.expected.Docs[i].Score)
					}
				}
				for k, v := range tt.expected.Docs[i].Fields {
					if doc.Fields[k] != v {
						t.Errorf("Docs[%d].Fields[%s] = %v, want %v", i, k, doc.Fields[k], v)
					}
				}
			}

			if len(result.Warnings) != len(tt.expected.Warnings) {
				t.Errorf("len(Warnings) = %v, want %v", len(result.Warnings), len(tt.expected.Warnings))
			}
			for i, w := range result.Warnings {
				if w != tt.expected.Warnings[i] {
					t.Errorf("Warnings[%d] = %v, want %v", i, w, tt.expected.Warnings[i])
				}
			}
		})
	}
}

// TestParseFTAggregateRESP3 tests the RESP3 parsing for FT.AGGREGATE results
func TestParseFTAggregateRESP3(t *testing.T) {
	tests := []struct {
		name     string
		input    map[string]interface{}
		expected FTAggregateResult
	}{
		{
			name: "empty result",
			input: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(0),
				"format":        "STRING",
				"results":       []interface{}{},
				"warning":       []interface{}{},
			},
			expected: FTAggregateResult{
				Total:    0,
				Rows:     []AggregateRow{},
				Warnings: []string{},
			},
		},
		{
			name: "aggregation with groupby",
			input: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(3),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"extra_attributes": map[string]interface{}{
							"condition":      "refurbished",
							"num_affordable": "158",
						},
						"values": []interface{}{},
					},
					map[string]interface{}{
						"extra_attributes": map[string]interface{}{
							"condition":      "used",
							"num_affordable": "156",
						},
						"values": []interface{}{},
					},
					map[string]interface{}{
						"extra_attributes": map[string]interface{}{
							"condition":      "new",
							"num_affordable": "158",
						},
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{},
			},
			expected: FTAggregateResult{
				Total: 3,
				Rows: []AggregateRow{
					{Fields: map[string]interface{}{"condition": "refurbished", "num_affordable": "158"}},
					{Fields: map[string]interface{}{"condition": "used", "num_affordable": "156"}},
					{Fields: map[string]interface{}{"condition": "new", "num_affordable": "158"}},
				},
				Warnings: []string{},
			},
		},
		{
			name: "aggregation with warnings",
			input: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(1),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"extra_attributes": map[string]interface{}{
							"__key":      "bicycle:0",
							"price":      "270",
							"discounted": "243",
						},
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{"Timeout limit was reached"},
			},
			expected: FTAggregateResult{
				Total: 1,
				Rows: []AggregateRow{
					{Fields: map[string]interface{}{"__key": "bicycle:0", "price": "270", "discounted": "243"}},
				},
				Warnings: []string{"Timeout limit was reached"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := processFTAggregateMapResult(tt.input)
			if err != nil {
				t.Fatalf("processFTAggregateMapResult() error = %v", err)
			}

			if result.Total != tt.expected.Total {
				t.Errorf("Total = %v, want %v", result.Total, tt.expected.Total)
			}

			if len(result.Rows) != len(tt.expected.Rows) {
				t.Errorf("len(Rows) = %v, want %v", len(result.Rows), len(tt.expected.Rows))
			}

			for i, row := range result.Rows {
				for k, v := range tt.expected.Rows[i].Fields {
					if row.Fields[k] != v {
						t.Errorf("Rows[%d].Fields[%s] = %v, want %v", i, k, row.Fields[k], v)
					}
				}
			}

			if len(result.Warnings) != len(tt.expected.Warnings) {
				t.Errorf("len(Warnings) = %v, want %v", len(result.Warnings), len(tt.expected.Warnings))
			}
			for i, w := range result.Warnings {
				if w != tt.expected.Warnings[i] {
					t.Errorf("Warnings[%d] = %v, want %v", i, w, tt.expected.Warnings[i])
				}
			}
		})
	}
}

// Helper function to create a pointer to a float64
func ptrFloat64(f float64) *float64 {
	return &f
}

// TestFTSearchRESP2AndRESP3Equivalence validates that RESP2 and RESP3 parsing
// produce semantically equivalent results as per the spec requirement R.1.1
func TestFTSearchRESP2AndRESP3Equivalence(t *testing.T) {
	tests := []struct {
		name       string
		resp2Data  []interface{}
		resp3Data  map[string]interface{}
		noContent  bool
		withScores bool
	}{
		{
			name: "simple hash query",
			resp2Data: []interface{}{
				int64(2),
				"usr:david",
				[]interface{}{"first_name", "David", "last_name", "Maier"},
				"usr:shaya",
				[]interface{}{"first_name", "Shaya", "last_name", "Potter"},
			},
			resp3Data: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(2),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"id": "usr:david",
						"extra_attributes": map[string]interface{}{
							"first_name": "David",
							"last_name":  "Maier",
						},
						"values": []interface{}{},
					},
					map[string]interface{}{
						"id": "usr:shaya",
						"extra_attributes": map[string]interface{}{
							"first_name": "Shaya",
							"last_name":  "Potter",
						},
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{},
			},
			noContent:  false,
			withScores: false,
		},
		{
			name: "query with scores",
			resp2Data: []interface{}{
				int64(1),
				"bicycle:hash:1",
				"1.5",
				[]interface{}{"price", "1200", "brand", "Bicyk"},
			},
			resp3Data: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(1),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"id":    "bicycle:hash:1",
						"score": float64(1.5),
						"extra_attributes": map[string]interface{}{
							"price": "1200",
							"brand": "Bicyk",
						},
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{},
			},
			noContent:  false,
			withScores: true,
		},
		{
			name: "JSON document query",
			resp2Data: []interface{}{
				int64(1),
				"bicycle:1",
				[]interface{}{"$", `{"brand":"Bicyk","model":"Hillcraft","price":1200}`},
			},
			resp3Data: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(1),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"id": "bicycle:1",
						"extra_attributes": map[string]interface{}{
							"$": `{"brand":"Bicyk","model":"Hillcraft","price":1200}`,
						},
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{},
			},
			noContent:  false,
			withScores: false,
		},
		{
			name: "NOCONTENT query",
			resp2Data: []interface{}{
				int64(10),
				"bicycle:hash:2",
			},
			resp3Data: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(10),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"id":     "bicycle:hash:2",
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{},
			},
			noContent:  true,
			withScores: false,
		},
		{
			name: "empty result",
			resp2Data: []interface{}{
				int64(0),
			},
			resp3Data: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(0),
				"format":        "STRING",
				"results":       []interface{}{},
				"warning":       []interface{}{},
			},
			noContent:  false,
			withScores: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse RESP2
			resp2Result, err := parseFTSearch(tt.resp2Data, tt.noContent, tt.withScores, false, false)
			if err != nil {
				t.Fatalf("parseFTSearch() error = %v", err)
			}

			// Parse RESP3
			resp3Result, err := processFTSearchMapResult(tt.resp3Data)
			if err != nil {
				t.Fatalf("processFTSearchMapResult() error = %v", err)
			}

			// Compare Total
			if resp2Result.Total != resp3Result.Total {
				t.Errorf("Total mismatch: RESP2=%d, RESP3=%d", resp2Result.Total, resp3Result.Total)
			}

			// Compare number of documents
			if len(resp2Result.Docs) != len(resp3Result.Docs) {
				t.Errorf("Docs count mismatch: RESP2=%d, RESP3=%d", len(resp2Result.Docs), len(resp3Result.Docs))
				return
			}

			// Compare each document
			for i, resp2Doc := range resp2Result.Docs {
				resp3Doc := resp3Result.Docs[i]

				if resp2Doc.ID != resp3Doc.ID {
					t.Errorf("Docs[%d].ID mismatch: RESP2=%s, RESP3=%s", i, resp2Doc.ID, resp3Doc.ID)
				}

				// Compare scores (if present)
				if tt.withScores {
					if resp2Doc.Score == nil && resp3Doc.Score != nil {
						t.Errorf("Docs[%d].Score mismatch: RESP2=nil, RESP3=%v", i, *resp3Doc.Score)
					} else if resp2Doc.Score != nil && resp3Doc.Score == nil {
						t.Errorf("Docs[%d].Score mismatch: RESP2=%v, RESP3=nil", i, *resp2Doc.Score)
					} else if resp2Doc.Score != nil && resp3Doc.Score != nil && *resp2Doc.Score != *resp3Doc.Score {
						t.Errorf("Docs[%d].Score mismatch: RESP2=%v, RESP3=%v", i, *resp2Doc.Score, *resp3Doc.Score)
					}
				}

				// Compare fields
				if len(resp2Doc.Fields) != len(resp3Doc.Fields) {
					t.Errorf("Docs[%d].Fields count mismatch: RESP2=%d, RESP3=%d", i, len(resp2Doc.Fields), len(resp3Doc.Fields))
				}

				for k, v := range resp2Doc.Fields {
					if resp3Doc.Fields[k] != v {
						t.Errorf("Docs[%d].Fields[%s] mismatch: RESP2=%s, RESP3=%s", i, k, v, resp3Doc.Fields[k])
					}
				}
			}
		})
	}
}

// TestFTAggregateRESP2AndRESP3Equivalence validates that RESP2 and RESP3 parsing
// produce semantically equivalent results for FT.AGGREGATE
func TestFTAggregateRESP2AndRESP3Equivalence(t *testing.T) {
	tests := []struct {
		name      string
		resp2Data []interface{}
		resp3Data map[string]interface{}
	}{
		{
			name: "aggregation with groupby",
			resp2Data: []interface{}{
				int64(3),
				[]interface{}{"condition", "refurbished", "num_affordable", "158"},
				[]interface{}{"condition", "used", "num_affordable", "156"},
				[]interface{}{"condition", "new", "num_affordable", "158"},
			},
			resp3Data: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(3),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"extra_attributes": map[string]interface{}{
							"condition":      "refurbished",
							"num_affordable": "158",
						},
						"values": []interface{}{},
					},
					map[string]interface{}{
						"extra_attributes": map[string]interface{}{
							"condition":      "used",
							"num_affordable": "156",
						},
						"values": []interface{}{},
					},
					map[string]interface{}{
						"extra_attributes": map[string]interface{}{
							"condition":      "new",
							"num_affordable": "158",
						},
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{},
			},
		},
		{
			name: "aggregation with apply",
			resp2Data: []interface{}{
				int64(1),
				[]interface{}{"__key", "bicycle:0", "price", "270", "discounted", "243"},
			},
			resp3Data: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(1),
				"format":        "STRING",
				"results": []interface{}{
					map[string]interface{}{
						"extra_attributes": map[string]interface{}{
							"__key":      "bicycle:0",
							"price":      "270",
							"discounted": "243",
						},
						"values": []interface{}{},
					},
				},
				"warning": []interface{}{},
			},
		},
		{
			name: "empty result",
			resp2Data: []interface{}{
				int64(0),
			},
			resp3Data: map[string]interface{}{
				"attributes":    []interface{}{},
				"total_results": int64(0),
				"format":        "STRING",
				"results":       []interface{}{},
				"warning":       []interface{}{},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Parse RESP2
			resp2Result, err := ProcessAggregateResult(tt.resp2Data)
			if err != nil {
				t.Fatalf("ProcessAggregateResult() error = %v", err)
			}

			// Parse RESP3
			resp3Result, err := processFTAggregateMapResult(tt.resp3Data)
			if err != nil {
				t.Fatalf("processFTAggregateMapResult() error = %v", err)
			}

			// Compare Total
			if resp2Result.Total != resp3Result.Total {
				t.Errorf("Total mismatch: RESP2=%d, RESP3=%d", resp2Result.Total, resp3Result.Total)
			}

			// Compare number of rows
			if len(resp2Result.Rows) != len(resp3Result.Rows) {
				t.Errorf("Rows count mismatch: RESP2=%d, RESP3=%d", len(resp2Result.Rows), len(resp3Result.Rows))
				return
			}

			// Compare each row
			for i, resp2Row := range resp2Result.Rows {
				resp3Row := resp3Result.Rows[i]

				// Compare fields count
				if len(resp2Row.Fields) != len(resp3Row.Fields) {
					t.Errorf("Rows[%d].Fields count mismatch: RESP2=%d, RESP3=%d", i, len(resp2Row.Fields), len(resp3Row.Fields))
				}

				// Compare field values
				for k, v := range resp2Row.Fields {
					resp3Val, ok := resp3Row.Fields[k]
					if !ok {
						t.Errorf("Rows[%d].Fields[%s] missing in RESP3", i, k)
						continue
					}
					if v != resp3Val {
						t.Errorf("Rows[%d].Fields[%s] mismatch: RESP2=%v, RESP3=%v", i, k, v, resp3Val)
					}
				}
			}
		})
	}
}
