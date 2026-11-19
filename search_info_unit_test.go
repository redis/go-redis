package redis

import "testing"

// The RESP2 index_definition array of an index created with FILTER and
// LANGUAGE, as Redis 8 returns it: the optional entries sit before
// default_score.
func TestParseFTInfoIndexDefinitionOptionalEntriesRESP2(t *testing.T) {
	res, err := parseFTInfo(map[string]interface{}{
		"index_definition": []interface{}{
			"key_type", "HASH",
			"prefixes", []interface{}{"doc:"},
			"filter", "@n>1",
			"default_language", "german",
			"default_score", "0.5",
			"indexes_all", "false",
		},
	})
	if err != nil {
		t.Fatalf("parseFTInfo() error = %v", err)
	}
	def := res.IndexDefinition
	if def.KeyType != "HASH" || len(def.Prefixes) != 1 || def.Prefixes[0] != "doc:" {
		t.Errorf("IndexDefinition = %+v, want KeyType HASH and Prefixes [doc:]", def)
	}
	if def.DefaultScore != 0.5 {
		t.Errorf("DefaultScore = %v, want 0.5", def.DefaultScore)
	}
}
