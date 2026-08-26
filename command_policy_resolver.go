package redis

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9/internal/routing"
)

type (
	module      = string
	commandName = string
)

var defaultPolicies = map[module]map[commandName]*routing.CommandPolicy{
	"ft": {
		"create": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"search": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"aggregate": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"dictadd": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"dictdump": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"dictdel": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"suglen": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultHashSlot,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"cursor": {
			Request:  routing.ReqSpecial,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"sugadd": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultHashSlot,
		},
		"sugget": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultHashSlot,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"sugdel": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultHashSlot,
		},
		"spellcheck": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"explain": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"explaincli": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"aliasadd": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"aliasupdate": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"aliasdel": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"aliaslist": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"info": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"tagvals": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"syndump": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"synupdate": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"profile": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
			Tips: map[string]string{
				routing.ReadOnlyCMD: "",
			},
		},
		"alter": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"dropindex": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"drop": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
	},
}

// CommandInfoResolveFunc resolves one command's Cluster routing policy.
// Nil delegates to the fallback resolver.
type CommandInfoResolveFunc func(ctx context.Context, cmd Cmder) *routing.CommandPolicy

type commandInfoResolver struct {
	resolveFunc      CommandInfoResolveFunc
	fallBackResolver *commandInfoResolver

	// metadataView marks a resolver backed by shared COMMAND metadata.
	metadataView   func() *commandMetadataView
	metadataEnsure func(context.Context) error
}

type commandRoutingResolution struct {
	policy             *routing.CommandPolicy
	policyFromMetadata bool
	meta               routingCommandMeta
	metaOK             bool
}

func NewCommandInfoResolver(resolveFunc CommandInfoResolveFunc) *commandInfoResolver {
	return &commandInfoResolver{resolveFunc: resolveFunc}
}

// NewDefaultCommandPolicyResolver derives policies from the shipped COMMAND
// metadata snapshot.
func NewDefaultCommandPolicyResolver() *commandInfoResolver {
	return newCommandMetadataPolicyResolver(func() *commandMetadataView {
		return defaultCommandMetadataView
	})
}

// defaultPolicyKeyless reports whether name (e.g. "ft.aliaslist") is registered
// in the static policy table as a plain keyless command: default request
// routing with a keyless response policy. Commands whose slot comes from a key
// (RespDefaultHashSlot, e.g. ft.suglen) or with special request routing
// (ReqSpecial, e.g. ft.cursor) are excluded — their key position must still be
// resolved. cmdFirstKeyPosWithInfo consults this so the initial slot
// computation on a cold command-info cache matches the policy the router
// applies once the command reaches routeAndRun.
func defaultPolicyKeyless(name string) bool {
	i := strings.IndexByte(name, '.')
	if i < 0 {
		return false
	}
	policy, ok := defaultPolicies[name[:i]][name[i+1:]]
	if !ok {
		return false
	}
	return policy.Request == routing.ReqDefault && policy.Response == routing.RespDefaultKeyless
}

// newCommandMetadataPolicyResolver loads the latest view for each invocation.
func newCommandMetadataPolicyResolver(view func() *commandMetadataView) *commandInfoResolver {
	return newCommandMetadataPolicyResolverWithEnsure(view, nil)
}

func newCommandMetadataPolicyResolverWithEnsure(
	view func() *commandMetadataView,
	ensure func(context.Context) error,
) *commandInfoResolver {
	r := &commandInfoResolver{metadataView: view, metadataEnsure: ensure}
	r.resolveFunc = func(ctx context.Context, cmd Cmder) *routing.CommandPolicy {
		// Direct dynamic resolvers fetch synchronously and fall back on failure.
		if ensure != nil {
			_ = ensure(ctx)
		}
		return r.commandPolicyInView(ctx, cmd, view())
	}
	return r
}

func (r *commandInfoResolver) GetCommandPolicy(ctx context.Context, cmd Cmder) *routing.CommandPolicy {
	if r == nil || r.resolveFunc == nil {
		return nil
	}
	if policy := r.resolveFunc(ctx, cmd); policy != nil {
		// Do not expose immutable view data to callers that may mutate it.
		if r.metadataView != nil {
			return cloneRoutingPolicy(policy)
		}
		return policy
	}
	if r.fallBackResolver != nil {
		return r.fallBackResolver.GetCommandPolicy(ctx, cmd)
	}
	return nil
}

func cloneRoutingPolicy(policy *routing.CommandPolicy) *routing.CommandPolicy {
	clone := *policy
	if policy.Tips != nil {
		clone.Tips = make(map[string]string, len(policy.Tips))
		for key, value := range policy.Tips {
			clone.Tips[key] = value
		}
	}
	return &clone
}

func (r *commandInfoResolver) SetFallbackResolver(fallbackResolver *commandInfoResolver) {
	r.fallBackResolver = fallbackResolver
}

// resolveCommandRoutingWithView resolves policy and metadata from one view.
func (r *commandInfoResolver) resolveCommandRoutingWithView(
	ctx context.Context,
	cmd Cmder,
	fallbackView func() *commandMetadataView,
) (commandRoutingResolution, *commandMetadataView, error) {
	var firstErr error
	var resolution commandRoutingResolution
	var view *commandMetadataView
	metaResolved := false
	resolveMeta := func() {
		if metaResolved || view == nil {
			return
		}
		resolution.meta, resolution.metaOK = routingLookupMeta(view, cmd)
		metaResolved = true
	}
	for current := r; current != nil; current = current.fallBackResolver {
		// A resolver without a function terminates the chain.
		if current.resolveFunc == nil {
			break
		}
		if current.metadataView != nil {
			if current.metadataEnsure != nil {
				if err := current.metadataEnsure(ctx); err != nil {
					// Keep routing with the current view and report the refresh error.
					if firstErr == nil {
						firstErr = err
					}
				}
			}
			// Prefer the client-owned view for a consistent invocation.
			view = nil
			if fallbackView != nil {
				view = fallbackView()
			}
			if view == nil {
				view = current.metadataView()
			}
			resolveMeta()
			if policy, ok := routingPolicyFor(resolution.meta); resolution.metaOK && ok {
				resolution.policy = policy
				resolution.policyFromMetadata = true
				return resolution, view, firstErr
			}
			continue
		}

		// Evaluate custom resolvers once, then capture key metadata.
		if policy := current.resolveFunc(ctx, cmd); policy != nil {
			view = nil
			if fallbackView != nil {
				view = fallbackView()
			}
			resolveMeta()
			resolution.policy = policy
			return resolution, view, firstErr
		}
	}

	view = nil
	if fallbackView != nil {
		view = fallbackView()
	}
	resolveMeta()
	return resolution, view, firstErr
}

func (r *commandInfoResolver) getCommandRoutingInView(
	ctx context.Context,
	cmd Cmder,
	view *commandMetadataView,
) commandRoutingResolution {
	meta, metaOK := routingLookupMeta(view, cmd)
	resolution := commandRoutingResolution{meta: meta, metaOK: metaOK}
	if r == nil || r.resolveFunc == nil {
		return resolution
	}
	for current := r; current != nil; current = current.fallBackResolver {
		if current.resolveFunc == nil {
			break
		}
		if current.metadataView != nil {
			if metaOK {
				resolution.policy, _ = routingPolicyFor(meta)
				resolution.policyFromMetadata = resolution.policy != nil
			}
		} else {
			resolution.policy = current.resolveFunc(ctx, cmd)
			resolution.policyFromMetadata = false
		}
		if resolution.policy != nil {
			break
		}
	}
	return resolution
}

// resolveCommandRoutingsWithView resolves a batch in one captured generation,
// invoking custom resolvers and metadata ensure hooks at most once as needed.
func (r *commandInfoResolver) resolveCommandRoutingsWithView(
	ctx context.Context,
	cmds []Cmder,
	fallbackView func() *commandMetadataView,
) ([]commandRoutingResolution, *commandMetadataView, error) {
	type pendingResolution struct {
		resolver *commandInfoResolver
		index    int
	}

	resolutions := make([]commandRoutingResolution, len(cmds))
	pending := make([]pendingResolution, 0, len(cmds))
	ensureResolvers := make([]*commandInfoResolver, 0, 1)
	ensureSeen := make(map[*commandInfoResolver]struct{})

	// First determine whether any command needs live metadata.
	for i, cmd := range cmds {
		for current := r; current != nil; current = current.fallBackResolver {
			if current.resolveFunc == nil {
				break
			}
			if current.metadataView != nil {
				pending = append(pending, pendingResolution{resolver: current, index: i})
				if current.metadataEnsure != nil {
					if _, seen := ensureSeen[current]; !seen {
						ensureSeen[current] = struct{}{}
						ensureResolvers = append(ensureResolvers, current)
					}
				}
				break
			}
			if policy := current.resolveFunc(ctx, cmd); policy != nil {
				resolutions[i].policy = policy
				break
			}
		}
	}

	var firstErr error
	for _, resolver := range ensureResolvers {
		if err := resolver.metadataEnsure(ctx); err != nil && firstErr == nil {
			firstErr = err
		}
	}

	var view *commandMetadataView
	if fallbackView != nil {
		view = fallbackView()
	}
	if view == nil && len(pending) > 0 {
		view = pending[0].resolver.metadataView()
	}
	for i, cmd := range cmds {
		resolutions[i].meta, resolutions[i].metaOK = routingLookupMeta(view, cmd)
	}

	// Resume unresolved chains in the captured view without another ensure.
	for _, item := range pending {
		cmd := cmds[item.index]
		for current := item.resolver; current != nil; current = current.fallBackResolver {
			if current.resolveFunc == nil {
				break
			}
			var policy *routing.CommandPolicy
			if current.metadataView != nil {
				if resolutions[item.index].metaOK {
					policy, _ = routingPolicyFor(resolutions[item.index].meta)
				}
			} else {
				policy = current.resolveFunc(ctx, cmd)
			}
			if policy != nil {
				resolutions[item.index].policy = policy
				resolutions[item.index].policyFromMetadata = current.metadataView != nil
				break
			}
		}
	}

	return resolutions, view, firstErr
}

func (r *commandInfoResolver) commandPolicyInView(
	_ context.Context,
	cmd Cmder,
	view *commandMetadataView,
) *routing.CommandPolicy {
	meta, ok := routingLookupMeta(view, cmd)
	if !ok {
		return nil
	}
	policy, ok := routingPolicyFor(meta)
	if !ok {
		return nil
	}
	return policy
}
