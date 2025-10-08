package redis

import (
	"sync"

	"github.com/redis/go-redis/v9/internal/routing"
)

var defaultPolicies = map[string]*routing.CommandPolicy{}

type commandPolicyManager struct {
	rwmutex             *sync.RWMutex
	clientPolicies      map[string]*routing.CommandPolicy
	overwrittenPolicies map[string]*routing.CommandPolicy
}

func newCommandPolicyManager(overwrites interface{}) *commandPolicyManager {
	return &commandPolicyManager{}
}

func (cpm *commandPolicyManager) updateClientPolicies(policies interface{}) {
	cpm.rwmutex.Lock()
	defer cpm.rwmutex.Unlock()
}

func (cpm *commandPolicyManager) getCmdPolicy(cmd Cmder) *routing.CommandPolicy {
	cpm.rwmutex.RLock()
	defer cpm.rwmutex.RUnlock()

	cmdName := cmd.Name()

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
