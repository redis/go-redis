package redis

import (
	"strings"
	"sync"

	"github.com/redis/go-redis/v9/internal/routing"
)

var defaultPolicies = map[string]*routing.CommandPolicy{
	"ft.create": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.search": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.aggregate": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.dictadd": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.dictdump": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.dictdel": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.suglen": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultHashSlot,
	},
	"ft.cursor": {
		Request:  routing.ReqSpecial,
		Response: routing.RespDefaultKeyless,
	},
	"ft.sugadd": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultHashSlot,
	},
	"ft.sugget": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultHashSlot,
	},
	"ft.sugdel": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultHashSlot,
	},
	"ft.spellcheck": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.explain": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.explaincli": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.aliasadd": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.aliasupdate": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.aliasdel": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.info": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.tagvals": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.syndump": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.synupdate": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.profile": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.alter": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.dropindex": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
	"ft.drop": {
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	},
}

type commandPolicyManager struct {
	rwmutex             *sync.RWMutex
	clientPolicies      map[string]*routing.CommandPolicy
	overwrittenPolicies map[string]*routing.CommandPolicy
}

func newCommandPolicyManager(overwrites interface{}) *commandPolicyManager {
	// TODO: To be implemented in the next req-resp development stage
	return &commandPolicyManager{
		rwmutex: &sync.RWMutex{},
	}
}

func (cpm *commandPolicyManager) updateClientPolicies(policies interface{}) {
	// TODO: To be implemented in the next req-resp development stage
	cpm.rwmutex.Lock()
	defer cpm.rwmutex.Unlock()
}

func (cpm *commandPolicyManager) getCmdPolicy(cmd Cmder) *routing.CommandPolicy {
	cpm.rwmutex.RLock()
	defer cpm.rwmutex.RUnlock()

	cmdName := strings.ToLower(cmd.Name())
	if policy, ok := cpm.overwrittenPolicies[cmdName]; ok {
		return policy
	}

	if policy, ok := cpm.clientPolicies[cmdName]; ok {
		return policy
	}

	if policy, ok := defaultPolicies[cmdName]; ok {
		return policy
	}

	return nil
}
