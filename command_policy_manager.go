package redis

import (
	"strings"
	"sync"

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

type commandPolicyManager struct {
	rwmutex             *sync.RWMutex
	clientPolicies      map[module]map[commandName]*routing.CommandPolicy
	overwrittenPolicies map[module]map[commandName]*routing.CommandPolicy
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

	module := "code"
	command := cmdName
	cmdParts := strings.Split(cmdName, ".")
	if len(cmdParts) == 2 {
		module = cmdParts[0]
		command = cmdParts[1]
	}

	if policy, ok := cpm.overwrittenPolicies[module][command]; ok {
		return policy
	}

	if policy, ok := cpm.clientPolicies[module][command]; ok {
		return policy
	}

	if policy, ok := defaultPolicies[module][command]; ok {
		return policy
	}

	return &routing.CommandPolicy{
		Request:  routing.ReqDefault,
		Response: routing.RespDefaultKeyless,
	}
}
