package redis

// extractRedisKeys returns the Redis key arguments from cmd. The result lets
// the cache map incoming invalidations back to affected entries. Returns nil
// (caller skips caching) when any key argument cannot be rendered in its wire
// form (see isWireKeyType).
//
// Production code reads the key span in processCached and builds the strings
// only on a miss (cscMissKeys), so this helper has no non-test caller.
func extractRedisKeys(cmd Cmder) []string {
	lo, hi, ok := cscKeySpan(cmd)
	if !ok || !cscKeysRenderable(cmd, lo, hi) {
		return nil
	}
	keys := make([]string, 0, hi-lo+1)
	for i := lo; i <= hi; i++ {
		keys = append(keys, cmd.stringArg(i))
	}
	return keys
}
