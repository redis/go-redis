package redis

import "testing"

// TestParseClientInfoForwardCompatFlags pins that parseClientInfo (shared by
// CLIENT INFO and CLIENT LIST) tolerates client-flag characters it does not
// recognize instead of failing the whole reply. Previously an unrecognized
// flag character produced "redis: unexpected client info flags(...)" and broke
// CLIENT TRACKING tests. New flag characters can appear over time, so the
// parser must skip the ones it does not map while keeping the ones it does.
func TestParseClientInfoForwardCompatFlags(t *testing.T) {
	cases := []struct {
		name  string
		flags string
		want  ClientFlags // bits that must be set; unknown flags must not error
	}{
		{"single unknown", "m", 0},
		{"multiple unknown", "go", 0},
		{"known plus unknown", "tm", ClientTracking},
		{"known combo plus unknown", "Btm", ClientTrackingBCAST | ClientTracking},
		{"no-flag sentinel", "N", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			info, err := parseClientInfo("id=1 addr=127.0.0.1:6379 flags=" + tc.flags + " db=0")
			if err != nil {
				t.Fatalf("parseClientInfo(flags=%q) errored: %v", tc.flags, err)
			}
			if info.Flags&tc.want != tc.want {
				t.Fatalf("flags=%q: Flags=%b, want bits %b set", tc.flags, info.Flags, tc.want)
			}
		})
	}
}
