package redis

import (
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// classifyOutcome must treat typed server replies (the proto reader parses
// recognized prefixes into *proto.AuthError, *proto.MovedError, ... rather
// than the concrete proto.RedisError string) exactly like their string
// forms: application-level replies prove the database served the request,
// while availability replies and surfaced redirects are failures.
func TestClassifyOutcomeTypedReplies(t *testing.T) {
	for _, err := range []error{
		proto.NewAuthError("NOAUTH Authentication required"),
		proto.NewPermissionError("NOPERM this user has no permissions"),
		proto.NewExecAbortError("EXECABORT Transaction discarded because of previous errors"),
	} {
		if got := classifyOutcome(err, true); got != outcomeSuccess {
			t.Errorf("classifyOutcome(%q) = %v, want outcomeSuccess", err.Error(), got)
		}
	}
	for _, err := range []error{
		proto.NewLoadingError("LOADING Redis is loading the dataset in memory"),
		proto.NewClusterDownError("CLUSTERDOWN The cluster is down"),
		// A MOVED/ASK that surfaces to this layer means the cluster client
		// exhausted its redirect budget: an availability failure, not a
		// healthy reply (see classifyOutcome's isRedirectReply case).
		proto.NewMovedError("MOVED 3999 127.0.0.1:6381", "127.0.0.1:6381"),
		proto.NewAskError("ASK 3999 127.0.0.1:6381", "127.0.0.1:6381"),
	} {
		if got := classifyOutcome(err, true); got != outcomeFailure {
			t.Errorf("classifyOutcome(%q) = %v, want outcomeFailure", err.Error(), got)
		}
	}
	if got := classifyOutcome(proto.RedisError("WRONGTYPE Operation against a key"), true); got != outcomeSuccess {
		t.Errorf("classifyOutcome(WRONGTYPE) = %v, want outcomeSuccess", got)
	}
}
