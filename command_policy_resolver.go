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
		},
		"aggregate": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"dictadd": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"dictdump": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"dictdel": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"suglen": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultHashSlot,
		},
		"cursor": {
			Request:  routing.ReqSpecial,
			Response: routing.RespDefaultKeyless,
		},
		"sugadd": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultHashSlot,
		},
		"sugget": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultHashSlot,
		},
		"sugdel": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultHashSlot,
		},
		"spellcheck": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"explain": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"explaincli": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
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
		"info": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"tagvals": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"syndump": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"synupdate": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
		},
		"profile": {
			Request:  routing.ReqDefault,
			Response: routing.RespDefaultKeyless,
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

type CommandInfoResolver interface {
	getCommandPolicy(ctx context.Context, cmdName string) *routing.CommandPolicy
	setFallbackResolver(fallback CommandInfoResolver)
}

type resolver struct {
	resolve          func(ctx context.Context, cmdName string) *routing.CommandPolicy
	fallBackResolver CommandInfoResolver
}

func NewDefaultCommandPolicyResolver() *resolver {
	return &resolver{
		resolve: func(ctx context.Context, cmdName string) *routing.CommandPolicy {
			module := "core"
			command := cmdName
			cmdParts := strings.Split(cmdName, ".")
			if len(cmdParts) == 2 {
				module = cmdParts[0]
				command = cmdParts[1]
			}

			if policy, ok := defaultPolicies[module][command]; ok {
				return policy
			}

			return nil
		},
	}
}

func (r *resolver) getCommandPolicy(ctx context.Context, cmdName string) *routing.CommandPolicy {
	policy := r.resolve(ctx, cmdName)
	if policy != nil {
		return policy
	}

	if r.fallBackResolver != nil {
		return r.fallBackResolver.getCommandPolicy(ctx, cmdName)
	}

	return nil
}

func (r *resolver) setFallbackResolver(fallbackResolver CommandInfoResolver) {
	r.fallBackResolver = fallbackResolver
}
