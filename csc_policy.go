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

// After capturing the context mark, execution state is carried by the command.
// Like command arguments/results it is owned by one execution at a time.
type cscIntent uint8

const (
	cscIntentUnresolved cscIntent = iota
	cscIntentCache
	cscIntentBypass
)

type cscCommandState struct {
	// view pins the same immutable metadata used by the Cluster router. It is
	// separate from routing decisions, which may be shared or consumed at dispatch.
	view        *commandMetadataView
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

type cacheContextKey struct{}

// WithCache returns a context that requests caching for Redis commands using it.
// The mark overrides the cache predicate and tracking-mode default, but cannot
// make an ineligible command cacheable. The client must have CSC enabled.
// Deadlines, cancellation and other values are inherited from ctx. The context
// may be reused; each submitted command gets the mark. The most recent mark wins
// when WithCache and WithoutCache are nested, without changing their parent.
func WithCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheContextKey{}, cacheRequested)
}

// WithoutCache returns a context that bypasses caching for Redis commands using
// it. Reads go to the server without using or changing existing cache entries.
// The most recently applied cache mark wins; ctx itself is unchanged.
func WithoutCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, cacheContextKey{}, cacheBypassed)
}

// Internal operations inherit cancellation and values, but not cache intent.
// Preserve the original context without allocating when it is already unmarked.
func cscUnmarkedContext(ctx context.Context) context.Context {
	if mark, _ := ctx.Value(cacheContextKey{}).(cacheMark); mark != cacheUnmarked {
		return context.WithValue(ctx, cacheContextKey{}, cacheUnmarked)
	}
	return ctx
}

// Capture intent before hooks or asynchronous dispatch can replace the context.
// Consume the context mark so internal handshake/helper commands do not inherit
// it. Children and retries carry the captured command state instead.
func cscBegin(ctx context.Context, cmd Cmder) (context.Context, bool) {
	s := cmd.cacheState()
	mark, _ := ctx.Value(cacheContextKey{}).(cacheMark)
	if mark != cacheUnmarked {
		if s.depth == 0 {
			// A new explicit invocation overrides any decision copied by Clone.
			// Internal children/retries receive an unmarked context instead.
			*s = cscCommandState{mark: mark}
		}
		ctx = context.WithValue(ctx, cacheContextKey{}, cacheUnmarked)
	}
	if s.depth != 0 || s.mark != cacheUnmarked || s.forceBypass {
		cscEnter(cmd)
		return ctx, true
	}
	return ctx, false
}

// Native helpers and public pipelines remain uncached. Validate a positive
// mark before any helper fan-out, registry mutation or pipeline submission.
func cscUncacheable(ctx context.Context, cmd Cmder, policy cscPolicy) (context.Context, error) {
	ctx, active := cscBegin(ctx, cmd)
	if active {
		defer cscLeave(cmd)
	}
	return ctx, cscUnsupported(ctx, cmd, policy, "command or execution path is unsupported")
}

// cscAdmitMarked fixes capability at submission, not at batch-size-dependent
// dispatch. Ordered async callers must not overtake earlier queued commands.
func (ap *AutoPipeliner) cscAdmitMarked(ctx context.Context, cmd Cmder) (bool, error) {
	if ap.blocking || ap.config.Unordered {
		return true, nil
	}
	if err := cscUnsupported(ctx, cmd, clientCachePolicy(ap.pipeliner), "ordered AsyncAutoPipeline does not cache marked calls"); err != nil {
		return false, err
	}
	s := cmd.cacheState()
	s.forceBypass = true
	s.intent = cscIntentBypass
	return false, nil
}

func (c *baseClient) cscUnsupported(ctx context.Context, cmd Cmder, reason string) error {
	return cscUnsupported(ctx, cmd, c.cscPolicy, reason)
}

func cscUnsupported(ctx context.Context, cmd Cmder, policy cscPolicy, reason string) error {
	s := cmd.cacheState()
	if s.mark != cacheRequested {
		return nil
	}
	err := fmt.Errorf("redis: cannot cache %s: %s", cmd.Name(), reason)
	if policy.strict || s.strict {
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
	meta, eligible := cscInvocationMeta(view, cmd)
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
	cscResolveIntent(cmd, view, meta, c.cscPolicy)
	return meta, s.intent == cscIntentCache, nil
}

func cscInvocationMeta(view *commandMetadataView, cmd Cmder) (cscCommandMeta, bool) {
	meta, eligible := cscEligibleMeta(view, cmd)
	if !eligible || !commandArgsRepeatable(cmd) {
		return meta, false
	}
	first, step, count := cscRedisKeyLayout(meta, cmd)
	for i, pos := 0, first; i < count; i, pos = i+1, pos+step {
		if !keyArgOK(cmd, pos) {
			return meta, false
		}
	}
	return meta, count > 0
}

func cscResolveIntent(cmd Cmder, view *commandMetadataView, meta cscCommandMeta, policy cscPolicy) {
	s := cmd.cacheState()
	if s.intent == cscIntentUnresolved {
		wantCache := s.mark == cacheRequested || (s.mark == cacheUnmarked && policy.mode != TrackingModeOptIn)
		if s.mark == cacheUnmarked && policy.cacheable != nil {
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
			wantCache = policy.cacheable(name, keys)
		}
		s.intent = cscIntentBypass
		if wantCache {
			s.intent = cscIntentCache
		}
	}
}

func (c *baseClient) cscInvocationView(cmd Cmder) *commandMetadataView {
	if view := cmd.cacheState().view; view != nil {
		return view
	}
	return c.metadataView()
}

// Resolve application intent on the complete original call, before fragments
// change its arguments. Each target still validates eligibility and coverage.
func (c *ClusterClient) cscPrepareInvocation(ctx context.Context, cmd Cmder) error {
	s := cmd.cacheState()
	meta, eligible := cscInvocationMeta(s.view, cmd)
	capable := eligible && c.opt.Protocol == 3 &&
		(c.opt.ClientSideCacheConfig != nil || c.opt.cscCustomFactory) &&
		c.opt.StreamingCredentialsProvider == nil && c.opt.CredentialsProvider == nil && c.opt.CredentialsProviderContext == nil &&
		clusterCSCCommandPolicy(cmd.Name()) != clusterCSCReplyBypass
	if !capable {
		s.forceBypass = true
		return cscUnsupported(ctx, cmd, cachePolicy(c.opt.ClientSideCacheConfig), "command or execution path is unsupported")
	}
	cscResolveIntent(cmd, s.view, meta, cachePolicy(c.opt.ClientSideCacheConfig))
	return nil
}

func (c *baseClient) cscTrackable(cmd Cmder) bool {
	meta, ok := cscLookupMeta(c.cscInvocationView(cmd), cmd)
	if !ok || meta.bits&cscFlagReadonly == 0 {
		return false
	}
	first, step, count := cscRedisKeyLayout(meta, cmd)
	if count == 0 {
		return false
	}
	for i, pos := 0, first; i < count; i, pos = i+1, pos+step {
		if keyArgOK(cmd, pos) {
			continue
		}
		// Tracking needs a known wire key, not the narrower cache-key contract.
		// This conversion never invokes a user BinaryMarshaler.
		if _, ok := routingArgText(cmd, pos); !ok {
			return false
		}
	}
	return true
}

func clientCachePolicy(client UniversalClient) cscPolicy {
	switch c := client.(type) {
	case *Client:
		return c.cscPolicy
	case *ClusterClient:
		return cachePolicy(c.opt.ClientSideCacheConfig)
	case *AutoPipeliner:
		return clientCachePolicy(c.pipeliner)
	default:
		return cscPolicy{}
	}
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
	if !p.configured() && !c.opt.cscCustomFactory {
		return nil
	}
	mode := func(m TrackingMode) TrackingMode {
		if m == "" {
			return TrackingModePlain
		}
		return m
	}
	if mode(p.mode) == mode(node.cscPolicy.mode) && (p.cacheable == nil) == (node.cscPolicy.cacheable == nil) && node.metadataSource == c.cmdMeta {
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
