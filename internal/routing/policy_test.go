package routing

import "testing"

func TestRequestPolicyString(t *testing.T) {
	tests := map[RequestPolicy]string{
		ReqDefault:        "default",
		ReqAllNodes:       "all_nodes",
		ReqAllShards:      "all_shards",
		ReqMultiShard:     "multi_shard",
		ReqSpecial:        "special",
		RequestPolicy(99): "unknown_request_policy(99)",
	}
	for p, want := range tests {
		if got := p.String(); got != want {
			t.Errorf("RequestPolicy(%d).String() = %q, want %q", p, got, want)
		}
	}
}

func TestParseRequestPolicy(t *testing.T) {
	tests := []struct {
		raw     string
		want    RequestPolicy
		wantErr bool
	}{
		{"", ReqDefault, false},
		{"default", ReqDefault, false},
		{"none", ReqDefault, false},
		{"ALL_NODES", ReqAllNodes, false},
		{"all_shards", ReqAllShards, false},
		{"multi_shard", ReqMultiShard, false},
		{"special", ReqSpecial, false},
		{"bogus", ReqDefault, true},
	}
	for _, tt := range tests {
		got, err := ParseRequestPolicy(tt.raw)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseRequestPolicy(%q) err = %v, wantErr %v", tt.raw, err, tt.wantErr)
		}
		if got != tt.want {
			t.Errorf("ParseRequestPolicy(%q) = %v, want %v", tt.raw, got, tt.want)
		}
	}
}

func TestResponsePolicyString(t *testing.T) {
	tests := map[ResponsePolicy]string{
		RespDefaultKeyless:  "default(keyless)",
		RespDefaultHashSlot: "default(hashslot)",
		RespAllSucceeded:    "all_succeeded",
		RespOneSucceeded:    "one_succeeded",
		RespAggSum:          "agg_sum",
		RespAggMin:          "agg_min",
		RespAggMax:          "agg_max",
		RespAggLogicalAnd:   "agg_logical_and",
		RespAggLogicalOr:    "agg_logical_or",
		RespSpecial:         "special",
		ResponsePolicy(200): "all_succeeded",
	}
	for p, want := range tests {
		if got := p.String(); got != want {
			t.Errorf("ResponsePolicy(%d).String() = %q, want %q", p, got, want)
		}
	}
}

func TestParseResponsePolicy(t *testing.T) {
	tests := []struct {
		raw     string
		want    ResponsePolicy
		wantErr bool
	}{
		{"default(keyless)", RespDefaultKeyless, false},
		{"default(hashslot)", RespDefaultHashSlot, false},
		{"all_succeeded", RespAllSucceeded, false},
		{"one_succeeded", RespOneSucceeded, false},
		{"agg_sum", RespAggSum, false},
		{"agg_min", RespAggMin, false},
		{"agg_max", RespAggMax, false},
		{"AGG_LOGICAL_AND", RespAggLogicalAnd, false},
		{"agg_logical_or", RespAggLogicalOr, false},
		{"special", RespSpecial, false},
		{"bogus", RespDefaultKeyless, true},
	}
	for _, tt := range tests {
		got, err := ParseResponsePolicy(tt.raw)
		if (err != nil) != tt.wantErr {
			t.Errorf("ParseResponsePolicy(%q) err = %v, wantErr %v", tt.raw, err, tt.wantErr)
		}
		if got != tt.want {
			t.Errorf("ParseResponsePolicy(%q) = %v, want %v", tt.raw, got, tt.want)
		}
	}
}

func TestCommandPolicyCanBeUsedInPipeline(t *testing.T) {
	tests := []struct {
		req  RequestPolicy
		want bool
	}{
		{ReqDefault, true},
		{ReqSpecial, true},
		{ReqAllNodes, false},
		{ReqAllShards, false},
		{ReqMultiShard, false},
	}
	for _, tt := range tests {
		p := &CommandPolicy{Request: tt.req}
		if got := p.CanBeUsedInPipeline(); got != tt.want {
			t.Errorf("CanBeUsedInPipeline(%v) = %v, want %v", tt.req, got, tt.want)
		}
	}
}

func TestCommandPolicyIsReadOnly(t *testing.T) {
	p := &CommandPolicy{Tips: map[string]string{ReadOnlyCMD: ""}}
	if !p.IsReadOnly() {
		t.Errorf("expected IsReadOnly true")
	}
	p2 := &CommandPolicy{Tips: map[string]string{}}
	if p2.IsReadOnly() {
		t.Errorf("expected IsReadOnly false")
	}
}

func TestShardPickers(t *testing.T) {
	static := NewStaticShardPicker(2)
	if got := static.Next(5); got != 2 {
		t.Errorf("StaticShardPicker.Next(5) = %d, want 2", got)
	}
	if got := static.Next(0); got != 0 {
		t.Errorf("StaticShardPicker.Next(0) = %d, want 0", got)
	}
	if got := static.Next(1); got != 0 {
		t.Errorf("StaticShardPicker.Next(1) = %d, want 0 (index out of range)", got)
	}

	rr := &RoundRobinPicker{}
	for i := 0; i < 6; i++ {
		if got := rr.Next(3); got != i%3 {
			t.Errorf("RoundRobinPicker.Next iteration %d = %d, want %d", i, got, i%3)
		}
	}
	if got := rr.Next(0); got != 0 {
		t.Errorf("RoundRobinPicker.Next(0) = %d, want 0", got)
	}

	var random RandomPicker
	if got := random.Next(0); got != 0 {
		t.Errorf("RandomPicker.Next(0) = %d, want 0", got)
	}
	for i := 0; i < 20; i++ {
		if got := random.Next(4); got < 0 || got >= 4 {
			t.Errorf("RandomPicker.Next(4) = %d, out of range", got)
		}
	}
}
