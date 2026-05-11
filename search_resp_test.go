package redis_test

import (
	"context"
	"testing"

	"github.com/redis/go-redis/v9"
)

// TestSearchCommandsRESP2AndRESP3Equivalence tests that search commands
// return equivalent results for both RESP2 and RESP3 protocols.
func TestSearchCommandsRESP2AndRESP3Equivalence(t *testing.T) {
	ctx := context.Background()

	// RESP2 client
	client2 := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Protocol: 2,
	})
	defer client2.Close()

	// RESP3 client
	client3 := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Protocol: 3,
	})
	defer client3.Close()

	// Check connection
	if err := client2.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	// Clean up before test
	client2.FTDropIndex(ctx, "test-idx")
	client2.Del(ctx, "doc1", "doc2", "doc3")

	t.Run("FTInfo", func(t *testing.T) {
		// Create index
		_, err := client2.FTCreate(ctx, "test-idx",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText, Sortable: true},
			&redis.FieldSchema{FieldName: "score", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		).Result()
		if err != nil {
			t.Fatalf("FTCreate failed: %v", err)
		}
		defer client2.FTDropIndex(ctx, "test-idx")

		// Add some documents
		client2.HSet(ctx, "doc1", "title", "hello world", "score", 100)
		client2.HSet(ctx, "doc2", "title", "foo bar", "score", 200)

		// Wait for indexing
		waitForIndexing(t, client2, "test-idx", 2)

		// Get FTInfo from both protocols
		info2, err := client2.FTInfo(ctx, "test-idx").Result()
		if err != nil {
			t.Fatalf("FTInfo RESP2 failed: %v", err)
		}

		info3, err := client3.FTInfo(ctx, "test-idx").Result()
		if err != nil {
			t.Fatalf("FTInfo RESP3 failed: %v", err)
		}

		// Compare key fields
		if info2.IndexName != info3.IndexName {
			t.Errorf("IndexName mismatch: RESP2=%q, RESP3=%q", info2.IndexName, info3.IndexName)
		}
		if info2.NumDocs != info3.NumDocs {
			t.Errorf("NumDocs mismatch: RESP2=%d, RESP3=%d", info2.NumDocs, info3.NumDocs)
		}
		if len(info2.Attributes) != len(info3.Attributes) {
			t.Errorf("Attributes length mismatch: RESP2=%d, RESP3=%d", len(info2.Attributes), len(info3.Attributes))
		}

		// Check IndexDefinition
		if info2.IndexDefinition.KeyType != info3.IndexDefinition.KeyType {
			t.Errorf("IndexDefinition.KeyType mismatch: RESP2=%q, RESP3=%q",
				info2.IndexDefinition.KeyType, info3.IndexDefinition.KeyType)
		}
		if info2.IndexDefinition.DefaultScore != info3.IndexDefinition.DefaultScore {
			t.Errorf("IndexDefinition.DefaultScore mismatch: RESP2=%v, RESP3=%v",
				info2.IndexDefinition.DefaultScore, info3.IndexDefinition.DefaultScore)
		}

		// Check CursorStats
		if info2.CursorStats != info3.CursorStats {
			t.Errorf("CursorStats mismatch: RESP2=%+v, RESP3=%+v", info2.CursorStats, info3.CursorStats)
		}

		// Check IndexErrors
		if info2.IndexErrors != info3.IndexErrors {
			t.Errorf("IndexErrors mismatch: RESP2=%+v, RESP3=%+v", info2.IndexErrors, info3.IndexErrors)
		}

		// Check DialectStats
		if len(info2.DialectStats) != len(info3.DialectStats) {
			t.Errorf("DialectStats length mismatch: RESP2=%d, RESP3=%d", len(info2.DialectStats), len(info3.DialectStats))
		}
		for k, v := range info2.DialectStats {
			if info3.DialectStats[k] != v {
				t.Errorf("DialectStats[%s] mismatch: RESP2=%d, RESP3=%d", k, v, info3.DialectStats[k])
			}
		}

		// Check GCStats
		if info2.GCStats != info3.GCStats {
			t.Errorf("GCStats mismatch: RESP2=%+v, RESP3=%+v", info2.GCStats, info3.GCStats)
		}

		// The four *_avg fields are emitted by RediSearch via REPLY_KVNUM
		// (RedisModule_ReplyWithDouble): bulk strings in RESP2, native doubles
		// in RESP3. The FTInfoResult fields are typed as string, so the RESP3
		// path must stringify the double; if it does not the field is "" and
		// the data is silently lost. Assert both that the RESP3 value is
		// non-empty and that it matches the RESP2 value.
		avgFields := []struct {
			name        string
			resp2Val    string
			resp3Val    string
		}{
			{"BytesPerRecordAvg", info2.BytesPerRecordAvg, info3.BytesPerRecordAvg},
			{"RecordsPerDocAvg", info2.RecordsPerDocAvg, info3.RecordsPerDocAvg},
			{"OffsetsPerTermAvg", info2.OffsetsPerTermAvg, info3.OffsetsPerTermAvg},
			{"OffsetBitsPerRecordAvg", info2.OffsetBitsPerRecordAvg, info3.OffsetBitsPerRecordAvg},
		}
		for _, f := range avgFields {
			if f.resp3Val == "" {
				t.Errorf("%s is empty under RESP3 (RESP2=%q) - silent data loss", f.name, f.resp2Val)
			}
			if f.resp2Val != f.resp3Val {
				t.Errorf("%s mismatch: RESP2=%q, RESP3=%q", f.name, f.resp2Val, f.resp3Val)
			}
		}

		// total_indexing_time is also emitted via REPLY_KVNUM (double) but the
		// FTInfoResult field is typed as int. Without explicit float-to-int
		// handling in the RESP3 parser, this field is silently 0.
		if info2.TotalIndexingTime != info3.TotalIndexingTime {
			t.Errorf("TotalIndexingTime mismatch: RESP2=%d, RESP3=%d",
				info2.TotalIndexingTime, info3.TotalIndexingTime)
		}

		// All gc_stats numeric fields are emitted via REPLY_KVNUM in RediSearch
		// (see src/fork_gc/fork_gc.c statsCb) but typed as int in GCStats.
		// Compare each individually so a regression surfaces with a precise message.
		gcFields := []struct {
			name             string
			resp2, resp3 int
		}{
			{"BytesCollected", info2.GCStats.BytesCollected, info3.GCStats.BytesCollected},
			{"TotalMsRun", info2.GCStats.TotalMsRun, info3.GCStats.TotalMsRun},
			{"TotalCycles", info2.GCStats.TotalCycles, info3.GCStats.TotalCycles},
			{"LastRunTimeMs", info2.GCStats.LastRunTimeMs, info3.GCStats.LastRunTimeMs},
			{"GCNumericTreesMissed", info2.GCStats.GCNumericTreesMissed, info3.GCStats.GCNumericTreesMissed},
			{"GCBlocksDenied", info2.GCStats.GCBlocksDenied, info3.GCStats.GCBlocksDenied},
		}
		for _, f := range gcFields {
			if f.resp2 != f.resp3 {
				t.Errorf("GCStats.%s mismatch: RESP2=%d, RESP3=%d", f.name, f.resp2, f.resp3)
			}
		}
	})

	t.Run("FTSearch", func(t *testing.T) {
		// Create index
		_, err := client2.FTCreate(ctx, "test-idx",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "score", FieldType: redis.SearchFieldTypeNumeric},
		).Result()
		if err != nil {
			t.Fatalf("FTCreate failed: %v", err)
		}
		defer client2.FTDropIndex(ctx, "test-idx")

		// Add documents
		client2.HSet(ctx, "doc1", "title", "hello world", "score", 100)
		client2.HSet(ctx, "doc2", "title", "hello redis", "score", 200)
		client2.HSet(ctx, "doc3", "title", "goodbye world", "score", 300)

		// Wait for indexing
		waitForIndexing(t, client2, "test-idx", 3)

		// Search from both protocols
		result2, err := client2.FTSearch(ctx, "test-idx", "hello").Result()
		if err != nil {
			t.Fatalf("FTSearch RESP2 failed: %v", err)
		}

		result3, err := client3.FTSearch(ctx, "test-idx", "hello").Result()
		if err != nil {
			t.Fatalf("FTSearch RESP3 failed: %v", err)
		}

		// Compare results
		if result2.Total != result3.Total {
			t.Errorf("Total mismatch: RESP2=%d, RESP3=%d", result2.Total, result3.Total)
		}
		if len(result2.Docs) != len(result3.Docs) {
			t.Errorf("Docs length mismatch: RESP2=%d, RESP3=%d", len(result2.Docs), len(result3.Docs))
		}

		t.Logf("RESP2 result: Total=%d, Docs=%d", result2.Total, len(result2.Docs))
		t.Logf("RESP3 result: Total=%d, Docs=%d", result3.Total, len(result3.Docs))

		for i, doc := range result2.Docs {
			t.Logf("RESP2 Doc[%d]: ID=%s, Fields=%v", i, doc.ID, doc.Fields)
		}
		for i, doc := range result3.Docs {
			t.Logf("RESP3 Doc[%d]: ID=%s, Fields=%v", i, doc.ID, doc.Fields)
		}
	})

	t.Run("FTAggregate", func(t *testing.T) {
		// Create index
		_, err := client2.FTCreate(ctx, "test-idx",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText},
			&redis.FieldSchema{FieldName: "score", FieldType: redis.SearchFieldTypeNumeric, Sortable: true},
		).Result()
		if err != nil {
			t.Fatalf("FTCreate failed: %v", err)
		}
		defer client2.FTDropIndex(ctx, "test-idx")

		// Add documents
		client2.HSet(ctx, "doc1", "title", "hello world", "score", 100)
		client2.HSet(ctx, "doc2", "title", "hello redis", "score", 200)

		// Wait for indexing
		waitForIndexing(t, client2, "test-idx", 2)

		// Aggregate from both protocols
		options := &redis.FTAggregateOptions{
			Load: []redis.FTAggregateLoad{{Field: "@score"}},
		}

		// Test that RawVal() is populated
		cmd2 := client2.FTAggregateWithArgs(ctx, "test-idx", "*", options)
		if cmd2.RawVal() == nil {
			t.Error("RESP2 RawVal() should not be nil")
		}
		cmd3 := client3.FTAggregateWithArgs(ctx, "test-idx", "*", options)
		if cmd3.RawVal() == nil {
			t.Error("RESP3 RawVal() should not be nil")
		}

		result2, err := cmd2.Result()
		if err != nil {
			t.Fatalf("FTAggregate RESP2 failed: %v", err)
		}

		result3, err := cmd3.Result()
		if err != nil {
			t.Fatalf("FTAggregate RESP3 failed: %v", err)
		}

		// Compare results
		// Note: FT.AGGREGATE total_results is unreliable from the server in RESP3
		// It should only match when both are 0 (no results)
		if result2.Total == 0 && result3.Total != 0 {
			t.Errorf("Total mismatch when RESP2 is 0: RESP2=%d, RESP3=%d", result2.Total, result3.Total)
		}
		if len(result2.Rows) != len(result3.Rows) {
			t.Errorf("Rows length mismatch: RESP2=%d, RESP3=%d", len(result2.Rows), len(result3.Rows))
		}

		t.Logf("RESP2 result: Total=%d, Rows=%d", result2.Total, len(result2.Rows))
		t.Logf("RESP3 result: Total=%d, Rows=%d (note: total_results is unreliable in RESP3)", result3.Total, len(result3.Rows))

		for i, row := range result2.Rows {
			t.Logf("RESP2 Row[%d]: Fields=%v", i, row.Fields)
		}
		for i, row := range result3.Rows {
			t.Logf("RESP3 Row[%d]: Fields=%v", i, row.Fields)
		}
	})

	t.Run("FTSpellCheck", func(t *testing.T) {
		// Create index
		_, err := client2.FTCreate(ctx, "test-idx",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText},
		).Result()
		if err != nil {
			t.Fatalf("FTCreate failed: %v", err)
		}
		defer client2.FTDropIndex(ctx, "test-idx")

		// Add documents
		client2.HSet(ctx, "doc1", "title", "hello world")
		client2.HSet(ctx, "doc2", "title", "hello redis")

		// Wait for indexing
		waitForIndexing(t, client2, "test-idx", 2)

		// SpellCheck from both protocols
		result2, err := client2.FTSpellCheck(ctx, "test-idx", "helo").Result()
		if err != nil {
			t.Fatalf("FTSpellCheck RESP2 failed: %v", err)
		}

		result3, err := client3.FTSpellCheck(ctx, "test-idx", "helo").Result()
		if err != nil {
			t.Fatalf("FTSpellCheck RESP3 failed: %v", err)
		}

		t.Logf("RESP2 result: %+v", result2)
		t.Logf("RESP3 result: %+v", result3)

		// Compare results
		if len(result2) != len(result3) {
			t.Errorf("Results length mismatch: RESP2=%d, RESP3=%d", len(result2), len(result3))
		}
	})

	t.Run("FTSynDump", func(t *testing.T) {
		// Create index
		_, err := client2.FTCreate(ctx, "test-idx",
			&redis.FTCreateOptions{},
			&redis.FieldSchema{FieldName: "title", FieldType: redis.SearchFieldTypeText},
		).Result()
		if err != nil {
			t.Fatalf("FTCreate failed: %v", err)
		}
		defer client2.FTDropIndex(ctx, "test-idx")

		// Add synonyms
		_, err = client2.FTSynUpdate(ctx, "test-idx", "group1", []interface{}{"hello", "hi", "hey"}).Result()
		if err != nil {
			t.Fatalf("FTSynUpdate failed: %v", err)
		}

		// SynDump from both protocols
		result2, err := client2.FTSynDump(ctx, "test-idx").Result()
		if err != nil {
			t.Fatalf("FTSynDump RESP2 failed: %v", err)
		}

		result3, err := client3.FTSynDump(ctx, "test-idx").Result()
		if err != nil {
			t.Fatalf("FTSynDump RESP3 failed: %v", err)
		}

		t.Logf("RESP2 result: %+v", result2)
		t.Logf("RESP3 result: %+v", result3)

		// Compare results
		if len(result2) != len(result3) {
			t.Errorf("Results length mismatch: RESP2=%d, RESP3=%d", len(result2), len(result3))
		}
	})
}

func waitForIndexing(t *testing.T, client *redis.Client, index string, expectedDocs int) {
	t.Helper()
	ctx := context.Background()
	for i := 0; i < 100; i++ {
		info, err := client.FTInfo(ctx, index).Result()
		if err != nil {
			t.Fatalf("FTInfo failed: %v", err)
		}
		if info.NumDocs >= expectedDocs {
			return
		}
	}
	t.Fatalf("Timeout waiting for indexing")
}
