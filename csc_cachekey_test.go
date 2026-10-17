package redis

// buildCacheKey renders a command's cache key with no namespace.
//
// Production code always namespaces the key, and builds it in one allocation
// via buildCacheKeyNS, so this un-namespaced form has no non-test caller. It
// lives here rather than in csc_commands.go so the production file has a
// single entry point and the linter's unused check stays clean (the repo runs
// golangci-lint with run.tests=false).
func buildCacheKey(cmd Cmder) (string, bool) {
	return buildCacheKeyNS(cmd, "")
}
