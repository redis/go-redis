package redis

import "context"

// These keyless/script/fieldset helpers cannot cache replies. Delegate to the
// client's typed methods to preserve aggregation and registry ownership. The
// fallback command carries pre-dispatch errors; the native helper owns success.
func cacheViewHelper[T Cmder](v *CacheView, ctx context.Context, cmd T, run func() T) T {
	fail := func(err error) T { cmd.SetErr(err); return cmd }
	ap, isAP := v.client.(*AutoPipeliner)
	if isAP && ap.isClosed() {
		return fail(ErrClosed)
	}
	cmd.cacheState().mark = v.mark
	err := cscUnsupported(ctx, cmd, clientCachePolicy(v.client), "command is not cacheable")
	*cmd.cacheState() = cscCommandState{}
	if err != nil {
		return fail(err)
	}
	if isAP {
		// CacheView is synchronous, so join the existing diversion gate without
		// spawning a worker. Close waits for accepted helper calls as usual.
		ap.divertMu.Lock()
		if ap.isClosed() {
			ap.divertMu.Unlock()
			return fail(ErrClosed)
		}
		if !ap.admitQueued() {
			ap.divertMu.Unlock()
			return fail(ErrAutoPipelineQueueFull)
		}
		ap.divertWg.Add(1)
		ap.divertMu.Unlock()
		defer ap.divertWg.Done()
		defer ap.releaseQueued(1)
	}
	return run()
}

// DBSize preserves the underlying client's database-size aggregation.
func (v *CacheView) DBSize(ctx context.Context) *IntCmd {
	return cacheViewHelper(v, ctx, NewIntCmd(ctx, "dbsize"), func() *IntCmd { return v.client.DBSize(ctx) })
}

// ScriptLoad preserves the underlying client's script distribution.
func (v *CacheView) ScriptLoad(ctx context.Context, script string) *StringCmd {
	return cacheViewHelper(v, ctx, NewStringCmd(ctx, "script", "load", script), func() *StringCmd { return v.client.ScriptLoad(ctx, script) })
}

// ScriptFlush preserves the underlying client's shard coverage.
func (v *CacheView) ScriptFlush(ctx context.Context) *StatusCmd {
	return cacheViewHelper(v, ctx, NewStatusCmd(ctx, "script", "flush"), func() *StatusCmd { return v.client.ScriptFlush(ctx) })
}

// ScriptExists preserves the underlying client's result aggregation.
func (v *CacheView) ScriptExists(ctx context.Context, hashes ...string) *BoolSliceCmd {
	args := make([]interface{}, 2+len(hashes))
	args[0], args[1] = "script", "exists"
	for i, hash := range hashes {
		args[2+i] = hash
	}
	return cacheViewHelper(v, ctx, NewBoolSliceCmd(ctx, args...), func() *BoolSliceCmd { return v.client.ScriptExists(ctx, hashes...) })
}

// HImportPrepare preserves the underlying client's shared fieldset registry.
func (v *CacheView) HImportPrepare(ctx context.Context, fieldsetName string, fields ...string) *StatusCmd {
	cmd := NewHImportPrepareCmd(ctx, fieldsetName, fields...)
	return cacheViewHelper(v, ctx, &cmd.StatusCmd, func() *StatusCmd { return v.client.HImportPrepare(ctx, fieldsetName, fields...) })
}

// HImportDiscard preserves the underlying client's fieldset removal.
func (v *CacheView) HImportDiscard(ctx context.Context, fieldsetName string) *IntCmd {
	cmd := NewHImportDiscardCmd(ctx, fieldsetName)
	return cacheViewHelper(v, ctx, &cmd.IntCmd, func() *IntCmd { return v.client.HImportDiscard(ctx, fieldsetName) })
}

// HImportDiscardAll preserves the underlying client's fieldset removal.
func (v *CacheView) HImportDiscardAll(ctx context.Context) *IntCmd {
	cmd := NewHImportDiscardAllCmd(ctx)
	return cacheViewHelper(v, ctx, &cmd.IntCmd, func() *IntCmd { return v.client.HImportDiscardAll(ctx) })
}
