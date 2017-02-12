package redis

var cmdPartitionSupport = map[string]bool{
	"mget":   true,
	"mset":   true,
	"del":    true,
	"exists": true,
}

type ringSupport int

const (
	NO_KEY ringSupport = iota
	SINGLE_KEY
	MULTI_SAME_SHARD
	MULTI_PARTITION_AGGREGATE
)

type cmdPartitionable interface {
	createPartitioner(cmdInfo *CommandInfo) cmdPartitioner
}

type partitioner func(arg string) (interface{}, error)
type cmdFactory func(args ...interface{}) Cmder

type partition struct {
	shard interface{}
	cmd Cmder
}

type cmdPartitioner interface {
	partition(p partitioner) ([]partition, error)
	aggregate(results []partition, cmd Cmder) error
}

//------------------------------------------------------------------------------
// basePartitioner

type basePartitioner struct {
	cmdInfo *CommandInfo
	cmd     Cmder
}

func (agg basePartitioner) partition(p partitioner) ([]partition, error) {
	originalCmd := agg.cmd
	//argsPerShard

	lastKeyPos := cmdLastKeyPos(originalCmd, agg.cmdInfo)
	for keyPos := cmdFirstKeyPos(originalCmd, agg.cmdInfo); keyPos <= lastKeyPos; keyPos += int(agg.cmdInfo.StepCount) {
		_, err := p(originalCmd.arg(keyPos))
		if (err!=nil) {
			return nil, err
		}
	}

	return nil, nil
}

func (agg basePartitioner) newCmder(args ...interface{}) Cmder {
	panic("Not implemented")
}

//------------------------------------------------------------------------------
// Partitioner factories

func (cmd *IntCmd) createPartitioner(cmdInfo *CommandInfo) cmdPartitioner {
	return intAgg{basePartitioner: basePartitioner{cmdInfo, cmd }}
}

func (cmd *SliceCmd) createPartitioner(cmdInfo *CommandInfo) cmdPartitioner {
	return sliceAgg{basePartitioner: basePartitioner{cmdInfo, cmd}}
}

func (cmd *StatusCmd) createPartitioner(cmdInfo *CommandInfo) cmdPartitioner {
	return statusAgg{basePartitioner: basePartitioner{cmdInfo, cmd}}
}

//------------------------------------------------------------------------------
// Partitioners

type intAgg struct {
	basePartitioner
}

func (agg intAgg) newCmder(args ...interface{}) Cmder {
	return NewIntCmd(args)
}

func (agg intAgg) newIntCmd(args ...interface{}) Cmder {
	return NewIntCmd(args)
}

func (agg intAgg) aggregate(results []partition, cmd Cmder) error {
	//TODO: Implement
	return nil
}

//------------------------------------------------------------------------------

type sliceAgg struct {
	basePartitioner
}

func (agg sliceAgg) aggregate(results []partition, cmd Cmder) error {
	//TODO: Implement
	return nil
}

//------------------------------------------------------------------------------

type statusAgg struct {
	basePartitioner
}

func (agg statusAgg) aggregate(results []partition, cmd Cmder) error {
	//TODO: Implement
	return nil
}

//------------------------------------------------------------------------------
