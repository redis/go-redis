package redis

var cmdAggregationSupport = map[string]bool {
	"mget": true,
	"mset": true,
	"del": true,
	"exists": true,
}

type cmdAggregateable interface {
    createAggregator(cmdInfo *CommandInfo) cmdAggregator
}

type partitioner func(arg string) (interface {}, error)

type cmdAggregator interface {
    partition(cmd Cmder, p partitioner) (map[interface{}]Cmder, error)
    aggregate(results map[interface{}]Cmder, cmd Cmder) error
}

func (cmd *IntCmd) createAggregator(cmdInfo *CommandInfo) cmdAggregator {
    return IntAgg{cmdInfo}
}

type IntAgg struct {
    cmdInfo *CommandInfo
}

func (agg IntAgg) partition(cmd Cmder, p partitioner) (map[interface{}]Cmder, error) {
    return nil, nil
}

func (agg IntAgg) aggregate(results map[interface{}]Cmder, cmd Cmder)  error {
    return nil
}