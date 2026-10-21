package redis

import (
	"context"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9/internal"
)

// TrackingMode selects the server-assisted cache tracking mode. It is fixed
// when a client is constructed and applied during each connection handshake.
type TrackingMode string

const (
	TrackingModePlain  TrackingMode = "plain"
	TrackingModeOptIn  TrackingMode = "optin"
	TrackingModeOptOut TrackingMode = "optout"
)

// CacheablePredicate selects among eligible, unmarked calls in every mode.
// command is the canonical metadata identifier; keys are actual Redis keys.
// The callback runs outside cache locks and may run concurrently. It must be
// fast, must not mutate keys, and must not call client control methods. The
// slice is owned by this invocation and may be retained by the callback.
type CacheablePredicate func(command string, keys []string) bool

type cscPolicy struct {
	mode      TrackingMode
	cacheable CacheablePredicate
	strict    bool
}

func cachePolicy(cfg *ClientSideCacheConfig) cscPolicy {
	if cfg == nil {
		return cscPolicy{}
	}
	return cscPolicy{cfg.TrackingMode, cfg.Cacheable, cfg.Strict}
}

func (p cscPolicy) validate() error {
	switch p.mode {
	case "", TrackingModePlain, TrackingModeOptIn, TrackingModeOptOut:
		return nil
	default:
		return fmt.Errorf("redis: invalid CSC tracking mode %q", p.mode)
	}
}

func (p cscPolicy) configured() bool {
	return (p.mode != "" && p.mode != TrackingModePlain) || p.cacheable != nil
}

type cacheMark uint8

const (
	cacheUnmarked cacheMark = iota
	cacheRequested
	cacheBypassed
)

// Invocation state is carried by the command, not a shared node or a context.
// Like command arguments/results it is owned by one execution at a time.
type cscIntent uint8

const (
	cscIntentUnresolved cscIntent = iota
	cscIntentCache
	cscIntentBypass
)

type cscCommandState struct {
	mark        cacheMark
	depth       uint8
	intent      cscIntent
	forceBypass bool
	noLookup    bool
	ask         bool
	warned      bool
	strict      bool
}

func cscEnter(cmd Cmder) { cmd.cacheState().depth++ }

func cscLeave(cmd Cmder) {
	s := cmd.cacheState()
	s.depth--
	if s.depth == 0 {
		*s = cscCommandState{}
	}
}

// CacheView is a synchronous, non-owning command view. It exposes typed
// commands and Do/Process, but no pools, Close, pipelines or transactions.
// Its immutable mark overrides the configured predicate for each invocation.
type CacheView struct{ cmdable }

func newCacheView(process func(context.Context, Cmder) error, cache bool) *CacheView {
	mark := cacheBypassed
	if cache {
		mark = cacheRequested
	}
	return &CacheView{cmdable: func(ctx context.Context, cmd Cmder) error {
		cmd.cacheState().mark = mark
		defer func() { *cmd.cacheState() = cscCommandState{} }()
		err := process(ctx, cmd)
		cmd.SetErr(err)
		return err
	}}
}

// WithCache returns a reusable view carrying an explicit cache or bypass mark.
// The client retains all resource ownership.
func (c *Client) WithCache(cache bool) *CacheView { return newCacheView(c.Process, cache) }

// WithCache returns a marked, non-owning view of this cluster client.
func (c *ClusterClient) WithCache(cache bool) *CacheView { return newCacheView(c.Process, cache) }

// WithCache explicitly diverts marked calls to the underlying client's normal
// synchronous execution path, independently of batch size and async mode.
func (ap *AutoPipeliner) WithCache(cache bool) *CacheView {
	return newCacheView(func(ctx context.Context, cmd Cmder) error {
		if ap.isClosed() {
			return ErrClosed
		}
		batch := ap.runOutsidePipeline(ctx, cmd)
		<-batch.done
		return cmd.rawErr()
	}, cache)
}

// Process executes a command with this view's mark. A command must not be
// executed concurrently; sequential reuse starts a new intent decision.
func (v *CacheView) Process(ctx context.Context, cmd Cmder) error { return v.cmdable(ctx, cmd) }

// Do executes a raw command with this view's mark and normal CSC validation.
func (v *CacheView) Do(ctx context.Context, args ...interface{}) *Cmd {
	cmd := NewCmd(ctx, args...)
	if len(args) == 0 {
		cmd.SetErr(errDoNoArgs)
		return cmd
	}
	_ = v.Process(ctx, cmd)
	return cmd
}

func (c *baseClient) cscUnsupported(ctx context.Context, cmd Cmder, reason string) error {
	s := cmd.cacheState()
	if s.mark != cacheRequested {
		return nil
	}
	err := fmt.Errorf("redis: cannot cache %s: %s", cmd.Name(), reason)
	if c.cscPolicy.strict || s.strict {
		return err
	}
	if !s.warned {
		internal.Logger.Printf(ctx, "%v; executing uncached", err)
		s.warned = true
	}
	return nil
}

func (c *baseClient) cscDecide(ctx context.Context, cmd Cmder, view *commandMetadataView) (cscCommandMeta, bool, error) {
	if err := c.cscPolicy.validate(); err != nil {
		return cscCommandMeta{}, false, err
	}
	s := cmd.cacheState()
	meta, eligible := cscEligibleMeta(view, cmd)
	if eligible {
		first, step, count := cscRedisKeyLayout(meta, cmd)
		eligible = count > 0 && commandArgsRepeatable(cmd)
		for i, pos := 0, first; eligible && i < count; i, pos = i+1, pos+step {
			eligible = keyArgOK(cmd, pos)
		}
	}
	capable := eligible && c.csc != nil && c.autopipelineCSCActive() && !s.forceBypass && !s.ask && !cscIsBypassed(ctx)
	if c.opt.cscPresenceOnly && clusterCSCCommandPolicy(cmd.Name()) == clusterCSCReplyBypass {
		capable = false
	}
	if !capable {
		if !s.ask {
			if err := c.cscUnsupported(ctx, cmd, "command or execution path is unsupported"); err != nil {
				return meta, false, err
			}
		}
		return meta, false, nil
	}
	if s.intent == cscIntentUnresolved {
		wantCache := s.mark == cacheRequested || (s.mark == cacheUnmarked && c.cscPolicy.mode != TrackingModeOptIn)
		if s.mark == cacheUnmarked && c.cscPolicy.cacheable != nil {
			first, step, count := cscRedisKeyLayout(meta, cmd)
			keys := make([]string, count)
			for i, pos := 0, first; i < count; i, pos = i+1, pos+step {
				keys[i], _ = keyArg(cmd, pos)
			}
			name := cmd.Name()
			if sub, ok := cscCommandToken(cmd, 1); ok {
				if _, ok := view.cscTable[name+"|"+strings.ToLower(sub)]; ok {
					name += "|" + strings.ToLower(sub)
				}
			}
			if len(keys) == 0 {
				return meta, false, nil
			}
			wantCache = c.cscPolicy.cacheable(name, keys)
		}
		s.intent = cscIntentBypass
		if wantCache {
			s.intent = cscIntentCache
		}
	}
	return meta, s.intent == cscIntentCache, nil
}

func (c *baseClient) cscTrackable(cmd Cmder) bool {
	meta, ok := cscLookupMeta(c.metadataView(), cmd)
	if !ok || meta.bits&cscFlagReadonly == 0 {
		return false
	}
	first, step, count := cscRedisKeyLayout(meta, cmd)
	if count == 0 {
		return false
	}
	for i, pos := 0, first; i < count; i, pos = i+1, pos+step {
		if !keyArgOK(cmd, pos) {
			return false
		}
	}
	return true
}

// Stage A deliberately leaves multi-target CSC intent unsupported. The routing
// algorithm remains unchanged; only cache participation is gated before dispatch.
func (c *ClusterClient) cscMultiTarget(ctx context.Context, cmd Cmder, targets int) error {
	p := cachePolicy(c.opt.ClientSideCacheConfig)
	if targets <= 1 || (!p.configured() && cmd.cacheState().mark == cacheUnmarked) {
		return nil
	}
	s := cmd.cacheState()
	if s.mark == cacheRequested {
		err := fmt.Errorf("redis: cannot cache %s: multi-target CSC awaits metadata routing integration", cmd.Name())
		if p.strict {
			return err
		}
		if !s.warned {
			internal.Logger.Printf(ctx, "%v; executing uncached", err)
			s.warned = true
		}
	}
	s.forceBypass = true
	return nil
}

// WithCache creates a command view for a UniversalClient without widening its
// lifecycle interface. AutoPipeline views execute marked calls synchronously.
func WithCache(client UniversalClient, cache bool) *CacheView {
	if c, ok := client.(interface{ WithCache(bool) *CacheView }); ok {
		return c.WithCache(cache)
	}
	return newCacheView(client.Process, cache)
}

// A custom factory must honor the requested policy. Fail closed for observable
// omissions instead of storing through an unexpectedly configured node cache.
func (c *ClusterClient) cscCheckNode(ctx context.Context, cmd Cmder, node *Client) error {
	if cmd.cacheState().ask {
		return nil
	}
	p := cachePolicy(c.opt.ClientSideCacheConfig)
	s := cmd.cacheState()
	if s.mark == cacheRequested && p.strict {
		s.strict = true
	}
	if !p.configured() {
		return nil
	}
	mode := func(m TrackingMode) TrackingMode {
		if m == "" {
			return TrackingModePlain
		}
		return m
	}
	if mode(p.mode) == mode(node.cscPolicy.mode) && (p.cacheable == nil) == (node.cscPolicy.cacheable == nil) {
		return nil
	}
	if s.mark == cacheRequested {
		err := fmt.Errorf("redis: cannot cache %s: node factory did not preserve CSC policy", cmd.Name())
		if p.strict {
			return err
		}
		if !s.warned {
			internal.Logger.Printf(ctx, "%v; executing uncached", err)
			s.warned = true
		}
	}
	s.forceBypass = true
	return nil
}
