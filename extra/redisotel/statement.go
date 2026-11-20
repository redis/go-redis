package redisotel

import (
	"github.com/redis/go-redis/extra/rediscmd/v9"
	"github.com/redis/go-redis/v9"
)

func buildCmdStatement(cmd redis.Cmder, maxLen int) string {
	return rediscmd.CmdStringLimit(cmd, maxLen)
}

func buildPipelineStatement(cmds []redis.Cmder, maxLen int) string {
	_, stmt := rediscmd.CmdsStringLimit(cmds, maxLen)
	return stmt
}

func pipelineSummary(cmds []redis.Cmder) string {
	return rediscmd.CmdsSummary(cmds)
}
