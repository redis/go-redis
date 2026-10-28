package redis

// extractRedisKeys applies the default metadata view to a command and returns
// every Redis key whose invalidation must evict its cached reply. Production
// code validates the layout first and renders namespaced keys only on a miss.
func extractRedisKeys(cmd Cmder) []string {
	return extractRedisKeysInView(defaultCommandMetadataView(), cmd)
}

// cscCollectKeys returns nil — never a partial list — if any key fails keyArg.
func cscCollectKeys(cmd Cmder, first, step, n int) []string {
	keys := make([]string, 0, n)
	for i, k := first, 0; k < n; i, k = i+step, k+1 {
		key, ok := keyArg(cmd, i)
		if !ok {
			return nil
		}
		keys = append(keys, key)
	}
	return keys
}
