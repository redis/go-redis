package redis

import (
	"sync"
	"time"
)

// Cmd object pools to eliminate allocations
// These pools are used internally to reuse Cmd objects across operations

var (
	stringCmdPool = sync.Pool{
		New: func() interface{} {
			return &StringCmd{}
		},
	}

	intCmdPool = sync.Pool{
		New: func() interface{} {
			return &IntCmd{}
		},
	}

	boolCmdPool = sync.Pool{
		New: func() interface{} {
			return &BoolCmd{}
		},
	}

	floatCmdPool = sync.Pool{
		New: func() interface{} {
			return &FloatCmd{}
		},
	}

	statusCmdPool = sync.Pool{
		New: func() interface{} {
			return &StatusCmd{}
		},
	}

	sliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &SliceCmd{}
		},
	}

	intSliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &IntSliceCmd{}
		},
	}

	floatSliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &FloatSliceCmd{}
		},
	}

	stringSliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &StringSliceCmd{}
		},
	}

	boolSliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &BoolSliceCmd{}
		},
	}

	durationCmdPool = sync.Pool{
		New: func() interface{} {
			return &DurationCmd{}
		},
	}

	timeCmdPool = sync.Pool{
		New: func() interface{} {
			return &TimeCmd{}
		},
	}

	mapStringStringCmdPool = sync.Pool{
		New: func() interface{} {
			return &MapStringStringCmd{}
		},
	}

	mapStringIntCmdPool = sync.Pool{
		New: func() interface{} {
			return &MapStringIntCmd{}
		},
	}

	mapStringInterfaceCmdPool = sync.Pool{
		New: func() interface{} {
			return &MapStringInterfaceCmd{}
		},
	}

	zSliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &ZSliceCmd{}
		},
	}

	zWithKeyCmdPool = sync.Pool{
		New: func() interface{} {
			return &ZWithKeyCmd{}
		},
	}

	scanCmdPool = sync.Pool{
		New: func() interface{} {
			return &ScanCmd{}
		},
	}

	digestCmdPool = sync.Pool{
		New: func() interface{} {
			return &DigestCmd{}
		},
	}

	lcsCmdPool = sync.Pool{
		New: func() interface{} {
			return &LCSCmd{}
		},
	}

	keyValueSliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &KeyValueSliceCmd{}
		},
	}

	xMessageSliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &XMessageSliceCmd{}
		},
	}

	xStreamSliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &XStreamSliceCmd{}
		},
	}

	xPendingCmdPool = sync.Pool{
		New: func() interface{} {
			return &XPendingCmd{}
		},
	}

	xPendingExtCmdPool = sync.Pool{
		New: func() interface{} {
			return &XPendingExtCmd{}
		},
	}

	xAutoClaimCmdPool = sync.Pool{
		New: func() interface{} {
			return &XAutoClaimCmd{}
		},
	}

	xAutoClaimJustIDCmdPool = sync.Pool{
		New: func() interface{} {
			return &XAutoClaimJustIDCmd{}
		},
	}

	xInfoConsumersCmdPool = sync.Pool{
		New: func() interface{} {
			return &XInfoConsumersCmd{}
		},
	}

	xInfoGroupsCmdPool = sync.Pool{
		New: func() interface{} {
			return &XInfoGroupsCmd{}
		},
	}

	xInfoStreamCmdPool = sync.Pool{
		New: func() interface{} {
			return &XInfoStreamCmd{}
		},
	}

	geoLocationCmdPool = sync.Pool{
		New: func() interface{} {
			return &GeoLocationCmd{}
		},
	}

	geoSearchLocationCmdPool = sync.Pool{
		New: func() interface{} {
			return &GeoSearchLocationCmd{}
		},
	}

	geoPosCmdPool = sync.Pool{
		New: func() interface{} {
			return &GeoPosCmd{}
		},
	}

	commandsInfoCmdPool = sync.Pool{
		New: func() interface{} {
			return &CommandsInfoCmd{}
		},
	}

	stringStructMapCmdPool = sync.Pool{
		New: func() interface{} {
			return &StringStructMapCmd{}
		},
	}

	mapStringSliceInterfaceCmdPool = sync.Pool{
		New: func() interface{} {
			return &MapStringSliceInterfaceCmd{}
		},
	}

	mapStringStringSliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &MapStringStringSliceCmd{}
		},
	}

	mapStringInterfaceSliceCmdPool = sync.Pool{
		New: func() interface{} {
			return &MapStringInterfaceSliceCmd{}
		},
	}

	keyValuesCmdPool = sync.Pool{
		New: func() interface{} {
			return &KeyValuesCmd{}
		},
	}

	zSliceWithKeyCmdPool = sync.Pool{
		New: func() interface{} {
			return &ZSliceWithKeyCmd{}
		},
	}

	functionListCmdPool = sync.Pool{
		New: func() interface{} {
			return &FunctionListCmd{}
		},
	}

	functionStatsCmdPool = sync.Pool{
		New: func() interface{} {
			return &FunctionStatsCmd{}
		},
	}

	keyFlagsCmdPool = sync.Pool{
		New: func() interface{} {
			return &KeyFlagsCmd{}
		},
	}

	rankWithScoreCmdPool = sync.Pool{
		New: func() interface{} {
			return &RankWithScoreCmd{}
		},
	}
)

// Helper functions to get and put Cmd objects from/to pools

func getStringCmd() *StringCmd {
	return stringCmdPool.Get().(*StringCmd)
}

func putStringCmd(cmd *StringCmd) {
	cmd.val = ""
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	stringCmdPool.Put(cmd)
}

func getIntCmd() *IntCmd {
	return intCmdPool.Get().(*IntCmd)
}

func putIntCmd(cmd *IntCmd) {
	cmd.val = 0
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	intCmdPool.Put(cmd)
}

func getBoolCmd() *BoolCmd {
	return boolCmdPool.Get().(*BoolCmd)
}

func putBoolCmd(cmd *BoolCmd) {
	cmd.val = false
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	boolCmdPool.Put(cmd)
}

func getStatusCmd() *StatusCmd {
	return statusCmdPool.Get().(*StatusCmd)
}

func putStatusCmd(cmd *StatusCmd) {
	cmd.val = ""
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	statusCmdPool.Put(cmd)
}

func getSliceCmd() *SliceCmd {
	return sliceCmdPool.Get().(*SliceCmd)
}

func putSliceCmd(cmd *SliceCmd) {
	cmd.val = nil
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	sliceCmdPool.Put(cmd)
}

func getFloatCmd() *FloatCmd {
	return floatCmdPool.Get().(*FloatCmd)
}

func putFloatCmd(cmd *FloatCmd) {
	cmd.val = 0
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	floatCmdPool.Put(cmd)
}

func getStringSliceCmd() *StringSliceCmd {
	return stringSliceCmdPool.Get().(*StringSliceCmd)
}

func putStringSliceCmd(cmd *StringSliceCmd) {
	cmd.val = nil
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	stringSliceCmdPool.Put(cmd)
}

func getIntSliceCmd() *IntSliceCmd {
	return intSliceCmdPool.Get().(*IntSliceCmd)
}

func putIntSliceCmd(cmd *IntSliceCmd) {
	cmd.val = nil
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	intSliceCmdPool.Put(cmd)
}

func getFloatSliceCmd() *FloatSliceCmd {
	return floatSliceCmdPool.Get().(*FloatSliceCmd)
}

func putFloatSliceCmd(cmd *FloatSliceCmd) {
	cmd.val = nil
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	floatSliceCmdPool.Put(cmd)
}

func getBoolSliceCmd() *BoolSliceCmd {
	return boolSliceCmdPool.Get().(*BoolSliceCmd)
}

func putBoolSliceCmd(cmd *BoolSliceCmd) {
	cmd.val = nil
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	boolSliceCmdPool.Put(cmd)
}

func getDurationCmd() *DurationCmd {
	return durationCmdPool.Get().(*DurationCmd)
}

func putDurationCmd(cmd *DurationCmd) {
	cmd.val = 0
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	durationCmdPool.Put(cmd)
}

func getTimeCmd() *TimeCmd {
	return timeCmdPool.Get().(*TimeCmd)
}

func putTimeCmd(cmd *TimeCmd) {
	cmd.val = time.Time{}
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	timeCmdPool.Put(cmd)
}

func getMapStringStringCmd() *MapStringStringCmd {
	return mapStringStringCmdPool.Get().(*MapStringStringCmd)
}

func putMapStringStringCmd(cmd *MapStringStringCmd) {
	cmd.val = nil
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	mapStringStringCmdPool.Put(cmd)
}

func getMapStringIntCmd() *MapStringIntCmd {
	return mapStringIntCmdPool.Get().(*MapStringIntCmd)
}

func putMapStringIntCmd(cmd *MapStringIntCmd) {
	cmd.val = nil
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	mapStringIntCmdPool.Put(cmd)
}

func getMapStringInterfaceCmd() *MapStringInterfaceCmd {
	return mapStringInterfaceCmdPool.Get().(*MapStringInterfaceCmd)
}

func putMapStringInterfaceCmd(cmd *MapStringInterfaceCmd) {
	cmd.val = nil
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	mapStringInterfaceCmdPool.Put(cmd)
}

func getZSliceCmd() *ZSliceCmd {
	return zSliceCmdPool.Get().(*ZSliceCmd)
}

func putZSliceCmd(cmd *ZSliceCmd) {
	cmd.val = nil
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	zSliceCmdPool.Put(cmd)
}

func getDigestCmd() *DigestCmd {
	return digestCmdPool.Get().(*DigestCmd)
}

func putDigestCmd(cmd *DigestCmd) {
	cmd.val = 0
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	digestCmdPool.Put(cmd)
}

func getLCSCmd() *LCSCmd {
	return lcsCmdPool.Get().(*LCSCmd)
}

func putLCSCmd(cmd *LCSCmd) {
	cmd.val = nil
	cmd.readType = 0
	cmd.err = nil
	cmd.args = nil
	cmd.ctx = nil
	lcsCmdPool.Put(cmd)
}
