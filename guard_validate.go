package redis

import "os/exec"

// GuardValidateLookPath is a probe for the guard validation PR. Do not merge.
func GuardValidateLookPath() (string, error) { return exec.LookPath("redis-server") }
