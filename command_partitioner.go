package redis

import (
	"strings"
)

var cmdPartitionSupport = map[string]bool{
	"mget":   true,
	"mset":   true,
	"del":    true,
	"exists": true,
}

type ringSupport int

const (
	NO_KEY                    ringSupport = iota
	SINGLE_KEY
	MULTI_SAME_SHARD
	MULTI_PARTITION_AGGREGATE
)

type cmdPartitionable interface {
	createPartitioner(cmdInfo *CommandInfo) cmdPartitioner
}

type keyPartitioner func(key string) (interface{}, error)
type cmderFactory func(args []interface{}) Cmder

type partition struct {
	shard interface{}
	cmd   Cmder
}

type cmdPartitioner interface {
	partition(kp keyPartitioner) ([]partition, error)
	aggregate(results []partition, cmd Cmder) error
}

//------------------------------------------------------------------------------
// basePartitioner

type baseCmdPartitioner struct {
	cmdInfo *CommandInfo
	cmd     Cmder
}

//This is a generic function that takes a Cmder that has few "key" arguments and creates a
//Cmder per shard. It iterates generically the command arguments and find which ones are "keys" (shards are based on keys)
//and which are "regular" arguments. For example: MGET key1 key2 key3
//where each key resides in a different shard will create 3 commands, each with "MGET" as first argument and the relevant key
//as another argument (i.e. MGET key1, MGET key2, MGET key3)
//Let's take a more complicated command: MSET. It has a variable number of keys but each key also has a "value" argument which
//doesn't affect sharding. so when it creates a new command, it has to copy both the keys and the values.
//so, for example: MSET key1 val1 key2 val2 will be converted to MSET key1 val1 and MSET key2 val2 (we use StepCount to detect that)
//Altough no command currently have this behavior, it's possible that in the future we will also want to support commands like BLPOP
//BLPOP has a variable number of key arguments but a fixed last argument which doesn't affect sharding (we use lastKeyPo to detect that)
//The code below also detects that and will copy the additional arguments to each new command. So, for example, BLPOP key1 key2 timeout
//will be converted to BLPOP key1 timeout, BLPOP key2 timeout
func partitionImpl(kp keyPartitioner, originalCmd Cmder, cmdInfo *CommandInfo, createCmd cmderFactory) ([]partition, []interface{}, error) {
	argsPerShard := map[interface{}][]interface{}{}
	cmdArgs := originalCmd.args()

	firstKeyPos := cmdFirstKeyPos(originalCmd, cmdInfo)
	lastKeyPos := cmdLastKeyPos(originalCmd, cmdInfo)
	step := int(cmdInfo.StepCount)

	//sortInfo is saved for later use in aggregateResults. It basically allows to re-order results according to the original request
	sortInfo := make([]interface{}, 0, (lastKeyPos+1-firstKeyPos)/step)

	//Iterate over all the keys in the command
	for i := firstKeyPos; i <= lastKeyPos; i += step {
		//find the shard for this argument
		arg := cmdArgs[i]
		shard, err := kp(arg.(string))
		if err != nil {
			return nil, nil, err
		}
		sortInfo = append(sortInfo, shard)

		args, exists := argsPerShard[shard]
		if !exists {
			//Create with initial len to leave space for fixed arguments at start (like command name)
			args = make([]interface{}, firstKeyPos, lastKeyPos)
		}

		//Copy key argument to the new args array
		args = append(args, arg)
		//Copy the rest of the arguments (i.e. in MSET these are the values)
		for j := i + 1; j < i+step; j++ {
			args = append(args, cmdArgs[j])
		}
		//And keep that per shard
		argsPerShard[shard] = args
	}

	//now that we have all arguments liged up, lets create new commands for them and return as array
	result := make([]partition, 0, len(argsPerShard))
	for shard, args := range argsPerShard {
		//Copy fixed prefix params
		for i := 0; i < firstKeyPos; i++ {
			args[i] = cmdArgs[i]
		}
		//Copy fixed postfix params
		for i := lastKeyPos + 1; i < len(cmdArgs); i++ {
			args[i] = cmdArgs[i]
		}
		newCmd := createCmd(args)
		result = append(result, partition{shard, newCmd})
	}
	return result, sortInfo, nil
}

//------------------------------------------------------------------------------
// Partitioner factories

func (cmd *IntCmd) createPartitioner(cmdInfo *CommandInfo) cmdPartitioner {
	return &intCmdPartitioner{baseCmdPartitioner: baseCmdPartitioner{cmdInfo, cmd}}
}

func (cmd *SliceCmd) createPartitioner(cmdInfo *CommandInfo) cmdPartitioner {
	return &sliceCmdPartitioner{baseCmdPartitioner: baseCmdPartitioner{cmdInfo, cmd}}
}

func (cmd *StatusCmd) createPartitioner(cmdInfo *CommandInfo) cmdPartitioner {
	return &statusCmdPartitioner{baseCmdPartitioner: baseCmdPartitioner{cmdInfo, cmd}}
}

//------------------------------------------------------------------------------
// Partitioners

type intCmdPartitioner struct {
	baseCmdPartitioner
}

func (cmdPart intCmdPartitioner) partition(kp keyPartitioner) ([]partition, error) {
	p, _, err := partitionImpl(kp, cmdPart.cmd, cmdPart.cmdInfo, func(args []interface{}) Cmder {
		return NewIntCmd(args...)
	})
	return p, err
}

func (cmdPart intCmdPartitioner) aggregate(results []partition, cmd Cmder) error {
	result := cmd.(*IntCmd)

	for _, part := range results {
		if err := part.cmd.Err(); err != nil {
			cmd.setErr(err)
			return err
		}
		intCmd := part.cmd.(*IntCmd)
		result.val += intCmd.Val()
	}
	return nil
}

//------------------------------------------------------------------------------

type sliceCmdPartitioner struct {
	baseCmdPartitioner
	sortInfo []interface{}
}

func (cmdPart *sliceCmdPartitioner) partition(kp keyPartitioner) ([]partition, error) {
	p, sortInfo, err := partitionImpl(kp, cmdPart.cmd, cmdPart.cmdInfo, func(args []interface{}) Cmder {
		return NewSliceCmd(args...)
	})
	cmdPart.sortInfo = sortInfo
	return p, err
}

func (cmdPart sliceCmdPartitioner) aggregate(results []partition, cmd Cmder) error {
	result := cmd.(*SliceCmd)

	valuesByShard := make(map[interface{}][]interface{}) // command result by shard

	for _, part := range results {
		if err := part.cmd.Err(); err != nil {
			cmd.setErr(err)
			return err
		}
		statCmd := part.cmd.(*SliceCmd)
		valuesByShard[part.shard] = statCmd.Val()
	}

	//build result list according to the order in sortInfo
	resultValues := make([]interface{}, 0, len(cmdPart.sortInfo))
	for _, shard := range cmdPart.sortInfo {
		//take the values from the correct shard and slice that shard
		value := valuesByShard[shard][0]
		valuesByShard[shard] = valuesByShard[shard][1:]
		resultValues = append(resultValues, value)
	}

	result.val = resultValues
	return nil
}

//------------------------------------------------------------------------------

type statusCmdPartitioner struct {
	baseCmdPartitioner
}

func (cmdPart statusCmdPartitioner) partition(kp keyPartitioner) ([]partition, error) {
	p, _, err := partitionImpl(kp, cmdPart.cmd, cmdPart.cmdInfo, func(args []interface{}) Cmder {
		return NewStatusCmd(args...)
	})
	return p, err
}

func (cmdPart statusCmdPartitioner) aggregate(results []partition, cmd Cmder) error {
	result := cmd.(*StatusCmd)

	stats := []string{}
	for _, part := range results {
		if err := part.cmd.Err(); err != nil {
			cmd.setErr(err)
			return err
		}
		statCmd := part.cmd.(*StatusCmd)
		stats = append(stats, statCmd.Val())
	}
	result.val = strings.Join(stats, " ")
	return nil
}

//------------------------------------------------------------------------------
