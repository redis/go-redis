package redis

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/redis/go-redis/v9/internal"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/routing"
)

var (
	errInvalidCmdPointer         = errors.New("redis: invalid command pointer")
	errNoCmdsToAggregate         = errors.New("redis: no commands to aggregate")
	errNoResToAggregate          = errors.New("redis: no results to aggregate")
	errInvalidCursorCmdArgsCount = errors.New("redis: FT.CURSOR command requires at least 3 arguments")
	errInvalidCursorIdType       = errors.New("redis: invalid cursor ID type")
)

type clusterFanoutResponseHandler func(Cmder, []Cmder) (interface{}, error)

type clusterFanoutResponseHandlerKey struct {
	name     string
	response routing.ResponsePolicy
}

// These collection replies need command-specific aggregation.
// Unsupported RespSpecial policies fail closed.
var clusterFanoutResponseHandlers = map[clusterFanoutResponseHandlerKey]clusterFanoutResponseHandler{
	{name: "keys", response: routing.RespDefaultKeyless}:         aggregateClusterKeys,
	{name: "latency|reset", response: routing.RespAggSum}:        aggregateClusterLatencyReset,
	{name: "randomkey", response: routing.RespSpecial}:           aggregateClusterRandomKey,
	{name: "script|exists", response: routing.RespAggLogicalAnd}: aggregateClusterScriptExists,
	{name: "slowlog|get", response: routing.RespDefaultKeyless}:  aggregateClusterSlowLog,
	{name: "waitaof", response: routing.RespAggMin}:              aggregateClusterWaitAOF,
}

type clusterSpecialRequestHandler func(*ClusterClient, context.Context, Cmder, *clusterNode, clusterRoutingDecision) error

var clusterSpecialRequestHandlers = map[string]clusterSpecialRequestHandler{
	"ft.cursor|del": func(c *ClusterClient, ctx context.Context, cmd Cmder, node *clusterNode, decision clusterRoutingDecision) error {
		return c.executeCursorCommand(ctx, cmd, node, decision)
	},
	"ft.cursor|read": func(c *ClusterClient, ctx context.Context, cmd Cmder, node *clusterNode, decision clusterRoutingDecision) error {
		return c.executeCursorCommand(ctx, cmd, node, decision)
	},
}

// slotResult represents the result of executing a command on a specific slot
type slotResult struct {
	cmd  Cmder
	keys []string
	err  error
}

type clusterSlotArgs struct {
	args []interface{}
	keys []string
}

// clusterFanoutExecutionError prevents replaying successful siblings.
type clusterFanoutExecutionError struct {
	err error
}

func (e *clusterFanoutExecutionError) Error() string { return e.err.Error() }
func (e *clusterFanoutExecutionError) Unwrap() error { return e.err }

// routeAndRun routes and executes a command.
func (c *ClusterClient) routeAndRun(
	ctx context.Context,
	cmd Cmder,
	node *clusterNode,
	decision clusterRoutingDecision,
) error {
	policy := decision.policy
	if decision.policyErr != nil {
		return decision.policyErr
	}
	if policy == nil {
		return c.executeDefault(ctx, cmd, policy, node, decision)
	}
	switch policy.Request {
	case routing.ReqAllNodes:
		return c.executeOnAllNodes(ctx, cmd, policy, decision)
	case routing.ReqAllShards:
		return c.executeOnAllShards(ctx, cmd, policy, decision)
	case routing.ReqMultiShard:
		return c.executeMultiShard(ctx, cmd, policy, decision)
	case routing.ReqSpecial:
		return c.executeSpecialCommand(ctx, cmd, policy, node, decision)
	default:
		return c.executeDefault(ctx, cmd, policy, node, decision)
	}
}

// executeDefault handles standard command routing based on keys
func (c *ClusterClient) executeDefault(
	ctx context.Context,
	cmd Cmder,
	policy *routing.CommandPolicy,
	node *clusterNode,
	decision clusterRoutingDecision,
) error {
	return node.Client.Process(ctx, cmd)
}

// executeOnAllNodes executes command on all nodes (masters and replicas)
func (c *ClusterClient) executeOnAllNodes(
	ctx context.Context,
	cmd Cmder,
	policy *routing.CommandPolicy,
	decision clusterRoutingDecision,
) error {
	state, err := c.state.Get(ctx)
	if err != nil {
		return err
	}
	nodes := make([]*clusterNode, 0, len(state.Masters)+len(state.Slaves))
	if err := state.requireOnline(state.declaredMasters()); err != nil {
		return c.noteTopologySelectionError(err)
	}
	if err := state.requireOnline(state.declaredSlaves()); err != nil {
		return c.noteTopologySelectionError(err)
	}
	nodes = append(nodes, state.Masters...)
	nodes = append(nodes, state.Slaves...)
	if len(nodes) == 0 {
		return errClusterNoNodes
	}

	return c.executeParallel(ctx, cmd, nodes, nil, policy, decision)
}

// executeOnAllShards executes command on all master shards
func (c *ClusterClient) executeOnAllShards(
	ctx context.Context,
	cmd Cmder,
	policy *routing.CommandPolicy,
	decision clusterRoutingDecision,
) error {
	state, err := c.state.Get(ctx)
	if err != nil {
		return err
	}
	if err := state.requireOnline(state.declaredMasters()); err != nil {
		return c.noteTopologySelectionError(err)
	}
	if handled, err := c.executeHImportFanout(ctx, cmd); handled {
		if err != nil {
			return &clusterFanoutExecutionError{err: err}
		}
		return nil
	}

	if len(state.Masters) == 0 {
		return errClusterNoNodes
	}

	masterSlots := make(map[*clusterNode]int, len(state.Masters))
	for _, slot := range state.slots {
		if len(slot.nodes) > 0 {
			if _, exists := masterSlots[slot.nodes[0]]; !exists {
				masterSlots[slot.nodes[0]] = slot.start
			}
		}
	}
	return c.executeParallel(ctx, cmd, state.Masters, masterSlots, policy, decision)
}

func (c *ClusterClient) executeHImportFanout(ctx context.Context, cmd Cmder) (bool, error) {
	switch cmd := cmd.(type) {
	case *HImportPrepareCmd:
		himportFanOutPrepare(ctx, c.himport, c.ForEachMaster, cmd)
		return true, cmd.rawErr()
	case *HImportDiscardCmd:
		himportFanOutDiscard(ctx, c.himport, c.ForEachMaster, cmd)
		return true, cmd.rawErr()
	case *HImportDiscardAllCmd:
		himportFanOutDiscardAll(ctx, c.himport, c.ForEachMaster, cmd)
		return true, cmd.rawErr()
	default:
		return false, nil
	}
}

// executeMultiShard runs a command across its key slots.
func (c *ClusterClient) executeMultiShard(
	ctx context.Context,
	cmd Cmder,
	policy *routing.CommandPolicy,
	decision clusterRoutingDecision,
) error {
	plan := decision.plan
	if !decision.planOK || !plan.splittable || len(plan.positions) == 0 {
		return fmt.Errorf("redis: cannot determine all key arguments for multi-shard command %s", cmd.Name())
	}
	args := cmd.Args()
	slotMap := make(map[int]*clusterSlotArgs)
	keyOrder := make([]string, 0)
	for _, pos := range plan.positions {
		key, ok := routingArgText(cmd, pos)
		if !ok {
			return fmt.Errorf("redis: cannot encode key at position %d for command %s", pos, cmd.Name())
		}
		slot := clusterKeySlot(key)
		group := slotMap[slot]
		if group == nil {
			group = &clusterSlotArgs{}
			slotMap[slot] = group
		}
		groupEnd := pos + plan.step
		if groupEnd > plan.keyArgsEnd || groupEnd > len(args) {
			return fmt.Errorf("redis: incomplete key argument group at position %d for command %s", pos, cmd.Name())
		}
		group.args = append(group.args, args[pos:groupEnd]...)
		group.keys = append(group.keys, key)
		keyOrder = append(keyOrder, key)
	}
	if len(slotMap) > 1 && decision.meta.name == "msetex" && msetexHasCondition(cmd, plan) {
		// NX/XX covers all keys; splitting could update only some of them.
		return ErrCrossSlot
	}
	return c.executeMultiSlot(ctx, cmd, slotMap, keyOrder, policy, decision)
}

func msetexHasCondition(cmd Cmder, plan routingKeyPlan) bool {
	for pos := plan.keyArgsEnd; pos < len(cmd.Args()); pos++ {
		arg, ok := routingArgText(cmd, pos)
		if !ok {
			return true
		}
		if strings.EqualFold(arg, "nx") || strings.EqualFold(arg, "xx") {
			return true
		}
	}
	return false
}

// executeMultiSlot executes commands across multiple slots concurrently
func (c *ClusterClient) executeMultiSlot(
	ctx context.Context,
	cmd Cmder,
	slotMap map[int]*clusterSlotArgs,
	keyOrder []string,
	policy *routing.CommandPolicy,
	decision clusterRoutingDecision,
) error {
	if len(slotMap) == 1 {
		// Keep single-slot execution and retries local.
		for slot := range slotMap {
			if err := c.executeMultiSlotCommand(ctx, cmd, slot, decision); err != nil {
				return &clusterFanoutExecutionError{err: err}
			}
			return nil
		}
	}

	results := make(chan slotResult, len(slotMap))
	var wg sync.WaitGroup

	// Execute on each slot concurrently
	for slot, group := range slotMap {
		wg.Add(1)
		go func(slot int, group *clusterSlotArgs) {
			defer wg.Done()

			subCmd, err := c.createSlotSpecificCommand(ctx, cmd, group.args, len(group.keys), decision.plan)
			if err != nil {
				results <- slotResult{nil, group.keys, err}
				return
			}
			err = c.executeMultiSlotCommand(ctx, subCmd, slot, decision)
			results <- slotResult{subCmd, group.keys, err}
		}(slot, group)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	if err := c.aggregateMultiSlotResults(ctx, cmd, results, keyOrder, policy); err != nil {
		return &clusterFanoutExecutionError{err: err}
	}
	return nil
}

// executeMultiSlotCommand retries one subgroup without replaying siblings.
func (c *ClusterClient) executeMultiSlotCommand(
	ctx context.Context,
	cmd Cmder,
	slot int,
	decision clusterRoutingDecision,
) error {
	var node *clusterNode
	var moved, ask bool
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		if attempt > 0 && !moved && !ask {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				return err
			}
		}

		if node == nil {
			var err error
			if c.opt.ShardPicker != nil {
				node, err = c.cmdNodeWithShardPickerAndDecision(ctx, slot, c.opt.ShardPicker, decision)
			} else {
				node, err = c.cmdNodeWithDecision(ctx, slot, decision)
			}
			if err != nil {
				lastErr = err
				if c.reloadTopologyForRetry(ctx, err, attempt) {
					continue
				}
				return err
			}
		}

		if ask {
			ask = false
			pipe := node.Client.Pipeline()
			_ = pipe.Process(ctx, NewCmd(ctx, "asking"))
			_ = pipe.Process(ctx, cmd)
			_, lastErr = pipe.Exec(ctx)
		} else {
			lastErr = node.Client.Process(ctx, cmd)
		}
		if lastErr == nil {
			return nil
		}

		if readOnly := isReadOnlyError(lastErr); readOnly || lastErr == pool.ErrClosed {
			if readOnly {
				c.state.LazyReload()
			}
			node = nil
			continue
		}
		if c.opt.ReadOnly && isLoadingError(lastErr) {
			node.MarkAsFailing()
			node = nil
			continue
		}

		var addr string
		moved, ask, addr = isMovedError(lastErr)
		if moved || ask {
			c.state.LazyReload()
			recordClusterRedirectMetric(ctx, ask)
			redirected, err := c.nodes.GetOrCreate(addr)
			if err != nil {
				return err
			}
			node = redirected
			continue
		}

		if shouldRetry(lastErr, cmd.readTimeout() == nil) && !cmd.NoRetry() {
			if attempt > 0 {
				node.MarkAsFailing()
				node = nil
			}
			continue
		}
		return lastErr
	}
	return lastErr
}

func recordClusterRedirectMetric(ctx context.Context, ask bool) {
	if errorCallback := pool.GetMetricErrorCallback(); errorCallback != nil {
		errorType, statusCode := "MOVED", "MOVED"
		if ask {
			errorType, statusCode = "ASK", "ASK"
		}
		errorCallback(ctx, errorType, nil, statusCode, false, 0)
	}
}

// createSlotSpecificCommand replaces key groups and rewrites numkeys when needed.
func (c *ClusterClient) createSlotSpecificCommand(
	ctx context.Context,
	originalCmd Cmder,
	keyArgs []interface{},
	keyCount int,
	plan routingKeyPlan,
) (Cmder, error) {
	originalArgs := originalCmd.Args()
	if !plan.splittable || len(plan.positions) == 0 || plan.keyArgsEnd > len(originalArgs) {
		return nil, fmt.Errorf("redis: invalid key plan for multi-shard command %s", originalCmd.Name())
	}
	firstKey := plan.positions[0]
	newArgs := make([]interface{}, 0, firstKey+len(keyArgs)+len(originalArgs)-plan.keyArgsEnd)
	newArgs = append(newArgs, originalArgs[:firstKey]...)
	if plan.numKeysPos >= 0 {
		if plan.numKeysPos >= len(newArgs) {
			return nil, fmt.Errorf("redis: invalid numkeys position for multi-shard command %s", originalCmd.Name())
		}
		newArgs[plan.numKeysPos] = keyCount
	}
	newArgs = append(newArgs, keyArgs...)
	newArgs = append(newArgs, originalArgs[plan.keyArgsEnd:]...)
	return createCommandByType(ctx, originalCmd.GetCmdType(), newArgs...), nil
}

// createCommandByType creates a new command of the specified type with the given arguments
func createCommandByType(ctx context.Context, cmdType CmdType, args ...interface{}) Cmder {
	switch cmdType {
	case CmdTypeString:
		return NewStringCmd(ctx, args...)
	case CmdTypeInt:
		return NewIntCmd(ctx, args...)
	case CmdTypeBool:
		return NewBoolCmd(ctx, args...)
	case CmdTypeFloat:
		return NewFloatCmd(ctx, args...)
	case CmdTypeStringSlice:
		return NewStringSliceCmd(ctx, args...)
	case CmdTypeIntSlice:
		return NewIntSliceCmd(ctx, args...)
	case CmdTypeFloatSlice:
		return NewFloatSliceCmd(ctx, args...)
	case CmdTypeBoolSlice:
		return NewBoolSliceCmd(ctx, args...)
	case CmdTypeStatus:
		return NewStatusCmd(ctx, args...)
	case CmdTypeTime:
		return NewTimeCmd(ctx, args...)
	case CmdTypeMapStringString:
		return NewMapStringStringCmd(ctx, args...)
	case CmdTypeMapStringInt:
		return NewMapStringIntCmd(ctx, args...)
	case CmdTypeMapStringInterface:
		return NewMapStringInterfaceCmd(ctx, args...)
	case CmdTypeMapStringInterfaceSlice:
		return NewMapStringInterfaceSliceCmd(ctx, args...)
	case CmdTypeSlice:
		return NewSliceCmd(ctx, args...)
	case CmdTypeStringStructMap:
		return NewStringStructMapCmd(ctx, args...)
	case CmdTypeXMessageSlice:
		return NewXMessageSliceCmd(ctx, args...)
	case CmdTypeXStreamSlice:
		return NewXStreamSliceCmd(ctx, args...)
	case CmdTypeXPending:
		return NewXPendingCmd(ctx, args...)
	case CmdTypeXPendingExt:
		return NewXPendingExtCmd(ctx, args...)
	case CmdTypeXAutoClaim:
		return NewXAutoClaimCmd(ctx, args...)
	case CmdTypeXAutoClaimWithDeleted:
		return NewXAutoClaimWithDeletedCmd(ctx, args...)
	case CmdTypeXAutoClaimJustID:
		return NewXAutoClaimJustIDCmd(ctx, args...)
	case CmdTypeXInfoStreamFull:
		return NewXInfoStreamFullCmd(ctx, args...)
	case CmdTypeZSlice:
		return NewZSliceCmd(ctx, args...)
	case CmdTypeZWithKey:
		return NewZWithKeyCmd(ctx, args...)
	case CmdTypeClusterSlots:
		return NewClusterSlotsCmd(ctx, args...)
	case CmdTypeGeoPos:
		return NewGeoPosCmd(ctx, args...)
	case CmdTypeCommandsInfo:
		return NewCommandsInfoCmd(ctx, args...)
	case CmdTypeSlowLog:
		return NewSlowLogCmd(ctx, args...)
	case CmdTypeKeyValues:
		return NewKeyValuesCmd(ctx, args...)
	case CmdTypeZSliceWithKey:
		return NewZSliceWithKeyCmd(ctx, args...)
	case CmdTypeFunctionList:
		return NewFunctionListCmd(ctx, args...)
	case CmdTypeFunctionStats:
		return NewFunctionStatsCmd(ctx, args...)
	case CmdTypeKeyFlags:
		return NewKeyFlagsCmd(ctx, args...)
	case CmdTypeDuration:
		return NewDurationCmd(ctx, time.Millisecond, args...)
	}
	return NewCmd(ctx, args...)
}

// executeSpecialCommand handles commands with special routing requirements
func (c *ClusterClient) executeSpecialCommand(
	ctx context.Context,
	cmd Cmder,
	policy *routing.CommandPolicy,
	node *clusterNode,
	decision clusterRoutingDecision,
) error {
	if handler := clusterSpecialRequestHandlers[decision.meta.name]; handler != nil {
		return handler(c, ctx, cmd, node, decision)
	}
	return errUnsupportedRoutingPolicy
}

// executeCursorCommand handles FT.CURSOR commands with sticky routing
func (c *ClusterClient) executeCursorCommand(
	ctx context.Context,
	cmd Cmder,
	node *clusterNode,
	decision clusterRoutingDecision,
) error {
	if len(cmd.Args()) < 4 {
		return errInvalidCursorCmdArgsCount
	}

	cursorID, err := cursorRoutingKey(cmd)
	if err != nil {
		return err
	}

	if node == nil {
		// Reuse redirected nodes; otherwise route by cursor ID.
		slot := clusterKeySlot(cursorID)
		node, err = c.cmdNodeWithShardPickerAndDecision(ctx, slot, c.opt.ShardPicker, decision)
		if err != nil {
			return err
		}
	}

	return node.Client.Process(ctx, cmd)
}

func cursorRoutingKey(cmd Cmder) (string, error) {
	if len(cmd.Args()) < 4 {
		return "", errInvalidCursorCmdArgsCount
	}
	key, ok := routingArgText(cmd, 3)
	if !ok {
		return "", errInvalidCursorIdType
	}
	return key, nil
}

// executeParallel executes a command on multiple nodes concurrently
func (c *ClusterClient) executeParallel(
	ctx context.Context,
	cmd Cmder,
	nodes []*clusterNode,
	targetSlots map[*clusterNode]int,
	policy *routing.CommandPolicy,
	decision clusterRoutingDecision,
) error {
	if len(nodes) == 0 {
		return errClusterNoNodes
	}

	if len(nodes) == 1 {
		targetSlot := -1
		if targetSlots != nil {
			if slot, ok := targetSlots[nodes[0]]; ok {
				targetSlot = slot
			}
		}
		if err := c.executeParallelTarget(ctx, nodes[0], targetSlot, cmd); err != nil {
			return &clusterFanoutExecutionError{err: err}
		}
		return nil
	}

	type nodeResult struct {
		cmd Cmder
		err error
	}

	results := make(chan nodeResult, len(nodes))
	var wg sync.WaitGroup

	for _, node := range nodes {
		wg.Add(1)
		go func(n *clusterNode) {
			defer wg.Done()
			cmdCopy := cmd.Clone()
			targetSlot := -1
			if targetSlots != nil {
				if slot, ok := targetSlots[n]; ok {
					targetSlot = slot
				}
			}
			err := c.executeParallelTarget(ctx, n, targetSlot, cmdCopy)
			results <- nodeResult{cmdCopy, err}
		}(node)
	}

	go func() {
		wg.Wait()
		close(results)
	}()

	// Collect results and check for errors
	cmds := make([]Cmder, 0, len(nodes))
	var firstErr error

	for result := range results {
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
		cmds = append(cmds, result.cmd)
	}

	// If there was an error and no policy specified, fail fast
	if firstErr != nil && (policy == nil || policy.Response == routing.RespDefaultKeyless) {
		cmd.SetErr(firstErr)
		return &clusterFanoutExecutionError{err: firstErr}
	}

	if err := c.aggregateResponses(cmd, cmds, policy, decision); err != nil {
		return &clusterFanoutExecutionError{err: err}
	}
	return nil
}

func (c *ClusterClient) executeParallelTarget(
	ctx context.Context,
	node *clusterNode,
	targetSlot int,
	cmd Cmder,
) error {
	var lastErr error
	for attempt := 0; attempt <= c.opt.MaxRedirects; attempt++ {
		if attempt > 0 {
			if err := internal.Sleep(ctx, c.retryBackoff(attempt)); err != nil {
				return err
			}
		}
		lastErr = node.Client.Process(ctx, cmd)
		if lastErr == nil {
			return nil
		}
		if targetSlot >= 0 && (isReadOnlyError(lastErr) || lastErr == pool.ErrClosed) {
			state, err := c.state.ReloadOrGet(ctx)
			if err == nil {
				if replacement, routeErr := state.slotMasterNode(targetSlot); routeErr == nil && replacement != node {
					node = replacement
					continue
				} else if routeErr != nil {
					_ = c.noteTopologySelectionError(routeErr)
				}
			}
		}
		if moved, ask, _ := isMovedError(lastErr); moved || ask {
			// Do not redirect one keyless broadcast target into another.
			recordClusterRedirectMetric(ctx, ask)
			return lastErr
		}
		if !shouldRetry(lastErr, cmd.readTimeout() == nil) || cmd.NoRetry() {
			return lastErr
		}
	}
	return lastErr
}

// aggregateMultiSlotResults aggregates results from multi-slot execution
func (c *ClusterClient) aggregateMultiSlotResults(ctx context.Context, cmd Cmder, results <-chan slotResult, keyOrder []string, policy *routing.CommandPolicy) error {
	keyedResults := make(map[string]routing.AggregatorResErr)
	shardResults := make([]routing.AggregatorResErr, 0)
	keyedResponse := policy == nil || policy.Response == routing.RespDefaultHashSlot
	var firstErr error

	for result := range results {
		if result.err != nil && firstErr == nil {
			firstErr = result.err
		}
		if result.cmd != nil && result.err == nil {
			value, err := ExtractCommandValue(result.cmd)
			if !keyedResponse {
				shardResults = append(shardResults, routing.AggregatorResErr{Result: value, Err: err})
				continue
			}

			// Check if the result is a slice (e.g., from MGET)
			if sliceValue, ok := value.([]interface{}); ok {
				// Map each element to its corresponding key
				for i, key := range result.keys {
					if i < len(sliceValue) {
						keyedResults[key] = routing.AggregatorResErr{Result: sliceValue[i], Err: err}
					} else {
						keyedResults[key] = routing.AggregatorResErr{Result: nil, Err: err}
					}
				}
			} else {
				// For non-slice results, map the entire result to each key
				for _, key := range result.keys {
					keyedResults[key] = routing.AggregatorResErr{Result: value, Err: err}
				}
			}
		}

		// TODO: return multiple errors by order when we will implement multiple errors returning
		if result.err != nil {
			firstErr = result.err
			if keyedResponse {
				for _, key := range result.keys {
					keyedResults[key] = routing.AggregatorResErr{Err: result.err}
				}
			} else {
				shardResults = append(shardResults, routing.AggregatorResErr{Err: result.err})
			}
		}
	}

	if !keyedResponse {
		aggregator := c.createAggregator(policy, cmd, false)
		if err := aggregator.BatchSlice(shardResults); err != nil {
			return err
		}
		return c.finishAggregation(cmd, aggregator)
	}
	if len(keyedResults) == 0 && firstErr != nil {
		return firstErr
	}
	return c.aggregateKeyedValues(cmd, keyedResults, keyOrder, policy)
}

// aggregateKeyedValues aggregates individual key-value pairs while preserving key order
func (c *ClusterClient) aggregateKeyedValues(cmd Cmder, keyedResults map[string]routing.AggregatorResErr, keyOrder []string, policy *routing.CommandPolicy) error {
	if len(keyedResults) == 0 {
		return errNoResToAggregate
	}

	aggregator := c.createAggregator(policy, cmd, true)

	// Set key order for keyed aggregators
	var keyedAgg *routing.DefaultKeyedAggregator
	var isKeyedAgg bool
	var err error
	if keyedAgg, isKeyedAgg = aggregator.(*routing.DefaultKeyedAggregator); isKeyedAgg {
		err = keyedAgg.BatchAddWithKeyOrder(keyedResults, keyOrder)
	} else {
		err = aggregator.BatchAdd(keyedResults)
	}

	if err != nil {
		return err
	}

	return c.finishAggregation(cmd, aggregator)
}

// aggregateResponses aggregates multiple shard responses
func (c *ClusterClient) aggregateResponses(
	cmd Cmder,
	cmds []Cmder,
	policy *routing.CommandPolicy,
	decision clusterRoutingDecision,
) error {
	if len(cmds) == 0 {
		return errNoCmdsToAggregate
	}
	name := decision.name
	if decision.metaOK {
		name = decision.meta.name
	}
	response := routing.RespDefaultKeyless
	if policy != nil {
		response = policy.Response
	}
	if handler := clusterFanoutResponseHandlers[clusterFanoutResponseHandlerKey{name: name, response: response}]; handler != nil {
		value, err := handler(cmd, cmds)
		if err != nil {
			return err
		}
		return c.setCommandValue(cmd, value)
	}

	if len(cmds) == 1 {
		shardCmd := cmds[0]
		if err := shardCmd.Err(); err != nil {
			cmd.SetErr(err)
			return err
		}
		value, _ := ExtractCommandValue(shardCmd)
		return c.setCommandValue(cmd, value)
	}

	aggregator := c.createAggregator(policy, cmd, false)

	batchWithErrs := []routing.AggregatorResErr{}
	// Add all results to aggregator
	for _, shardCmd := range cmds {
		value, err := ExtractCommandValue(shardCmd)
		batchWithErrs = append(batchWithErrs, routing.AggregatorResErr{
			Result: value,
			Err:    err,
		})
	}

	err := aggregator.BatchSlice(batchWithErrs)
	if err != nil {
		return err
	}

	return c.finishAggregation(cmd, aggregator)
}

func aggregateClusterKeys(cmd Cmder, cmds []Cmder) (interface{}, error) {
	if cmd.GetCmdType() == CmdTypeGeneric {
		var result []interface{}
		for _, shardCmd := range cmds {
			value, err := ExtractCommandValue(shardCmd)
			if err != nil {
				return nil, err
			}
			switch keys := value.(type) {
			case []interface{}:
				result = append(result, keys...)
			case []string:
				for _, key := range keys {
					result = append(result, key)
				}
			default:
				return nil, fanoutTypeError(cmd, value, "array of keys")
			}
		}
		return result, nil
	}

	var result []string
	for _, shardCmd := range cmds {
		value, err := ExtractCommandValue(shardCmd)
		if err != nil {
			return nil, err
		}
		keys, ok := value.([]string)
		if !ok {
			return nil, fanoutTypeError(cmd, value, "[]string")
		}
		result = append(result, keys...)
	}
	return result, nil
}

// aggregateClusterRandomKey chooses among successful non-empty shard replies.
func aggregateClusterRandomKey(_ Cmder, cmds []Cmder) (interface{}, error) {
	values := make([]interface{}, 0, len(cmds))
	for _, shardCmd := range cmds {
		value, err := ExtractCommandValue(shardCmd)
		if err == Nil {
			continue
		}
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	if len(values) == 0 {
		return nil, Nil
	}
	return values[rand.Intn(len(values))], nil
}

func aggregateClusterSlowLog(cmd Cmder, cmds []Cmder) (interface{}, error) {
	if cmd.GetCmdType() == CmdTypeGeneric {
		var result []interface{}
		for _, shardCmd := range cmds {
			value, err := ExtractCommandValue(shardCmd)
			if err != nil {
				return nil, err
			}
			entries, ok := value.([]interface{})
			if !ok {
				return nil, fanoutTypeError(cmd, value, "array of slowlog entries")
			}
			result = append(result, entries...)
		}
		return result, nil
	}

	var result []SlowLog
	for _, shardCmd := range cmds {
		value, err := ExtractCommandValue(shardCmd)
		if err != nil {
			return nil, err
		}
		entries, ok := value.([]SlowLog)
		if !ok {
			return nil, fanoutTypeError(cmd, value, "[]redis.SlowLog")
		}
		result = append(result, entries...)
	}
	return result, nil
}

func aggregateClusterScriptExists(cmd Cmder, cmds []Cmder) (interface{}, error) {
	var result []bool
	for shardIndex, shardCmd := range cmds {
		value, err := ExtractCommandValue(shardCmd)
		if err != nil {
			return nil, err
		}
		values, err := clusterBoolSlice(value)
		if err != nil {
			return nil, fmt.Errorf("redis: cannot aggregate command %s shard %d: %w", cmd.Name(), shardIndex, err)
		}
		if shardIndex == 0 {
			result = append([]bool(nil), values...)
			continue
		}
		if len(values) != len(result) {
			return nil, fmt.Errorf(
				"redis: cannot aggregate command %s: shard result length %d does not match %d",
				cmd.Name(), len(values), len(result),
			)
		}
		for i := range result {
			result[i] = result[i] && values[i]
		}
	}
	if cmd.GetCmdType() != CmdTypeGeneric {
		return result, nil
	}
	raw := make([]interface{}, len(result))
	for i, exists := range result {
		if exists {
			raw[i] = int64(1)
		} else {
			raw[i] = int64(0)
		}
	}
	return raw, nil
}

func aggregateClusterWaitAOF(cmd Cmder, cmds []Cmder) (interface{}, error) {
	var result []int64
	for shardIndex, shardCmd := range cmds {
		value, err := ExtractCommandValue(shardCmd)
		if err != nil {
			return nil, err
		}
		values, err := clusterInt64Slice(value)
		if err != nil {
			return nil, fmt.Errorf("redis: cannot aggregate command %s shard %d: %w", cmd.Name(), shardIndex, err)
		}
		if shardIndex == 0 {
			result = append([]int64(nil), values...)
			continue
		}
		if len(values) != len(result) {
			return nil, fmt.Errorf(
				"redis: cannot aggregate command %s: shard result length %d does not match %d",
				cmd.Name(), len(values), len(result),
			)
		}
		for i := range result {
			if values[i] < result[i] {
				result[i] = values[i]
			}
		}
	}
	if cmd.GetCmdType() != CmdTypeGeneric {
		return result, nil
	}
	raw := make([]interface{}, len(result))
	for i, value := range result {
		raw[i] = value
	}
	return raw, nil
}

func aggregateClusterLatencyReset(cmd Cmder, cmds []Cmder) (interface{}, error) {
	var sum int64
	for _, shardCmd := range cmds {
		value, err := ExtractCommandValue(shardCmd)
		if err != nil {
			return nil, err
		}
		var count int64
		switch value := value.(type) {
		case string:
			count, err = strconv.ParseInt(value, 10, 64)
		case int64:
			count = value
		default:
			err = fanoutTypeError(cmd, value, "integer reset count")
		}
		if err != nil {
			return nil, err
		}
		if count > 0 && sum > math.MaxInt64-count || count < 0 && sum < math.MinInt64-count {
			return nil, fmt.Errorf("redis: cannot aggregate command %s: integer overflow", cmd.Name())
		}
		sum += count
	}
	if cmd.GetCmdType() == CmdTypeStatus {
		return strconv.FormatInt(sum, 10), nil
	}
	return sum, nil
}

func clusterBoolSlice(value interface{}) ([]bool, error) {
	switch values := value.(type) {
	case []bool:
		return values, nil
	case []interface{}:
		result := make([]bool, len(values))
		for i, value := range values {
			switch value := value.(type) {
			case bool:
				result[i] = value
			case int64:
				result[i] = value != 0
			default:
				return nil, fmt.Errorf("expected boolean array element, got %T", value)
			}
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected boolean array, got %T", value)
	}
}

func clusterInt64Slice(value interface{}) ([]int64, error) {
	switch values := value.(type) {
	case []int64:
		return values, nil
	case []interface{}:
		result := make([]int64, len(values))
		for i, value := range values {
			integer, ok := value.(int64)
			if !ok {
				return nil, fmt.Errorf("expected integer array element, got %T", value)
			}
			result[i] = integer
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected integer array, got %T", value)
	}
}

func fanoutTypeError(cmd Cmder, value interface{}, expected string) error {
	return fmt.Errorf(
		"redis: cannot aggregate command %s: expected %s, got %T",
		cmd.Name(), expected, value,
	)
}

// createAggregator returns the configured or default response aggregator.
func (c *ClusterClient) createAggregator(policy *routing.CommandPolicy, cmd Cmder, isKeyed bool) routing.ResponseAggregator {
	if policy != nil {
		return routing.NewResponseAggregator(policy.Response, cmd.Name())
	}
	return routing.NewDefaultAggregator(isKeyed)
}

// finishAggregation stores the aggregate result.
func (c *ClusterClient) finishAggregation(cmd Cmder, aggregator routing.ResponseAggregator) error {
	finalValue, finalErr := aggregator.Aggregate()
	if finalErr != nil {
		cmd.SetErr(finalErr)
		return finalErr
	}

	return c.setCommandValue(cmd, finalValue)
}

// setCommandValue sets the aggregated value on a command using the enum-based approach
func (c *ClusterClient) setCommandValue(cmd Cmder, value interface{}) error {
	// If value is nil, it might mean ExtractCommandValue couldn't extract the value
	// but the command might have executed successfully. In this case, don't set an error.
	if value == nil {
		// ExtractCommandValue returned nil - this means the command type is not supported
		// in the aggregation flow. This is a programming error, not a runtime error.
		if cmd.Err() != nil {
			// Command already has an error, preserve it
			return cmd.Err()
		}
		// Command executed successfully but we can't extract/set the aggregated value
		// This indicates the command type needs to be added to ExtractCommandValue
		return fmt.Errorf("redis: cannot aggregate command %s: unsupported command type %d",
			cmd.Name(), cmd.GetCmdType())
	}
	var err error
	value, err = normalizeAggregatedCommandValue(cmd, value)
	if err != nil {
		return err
	}

	switch cmd.GetCmdType() {
	case CmdTypeGeneric:
		if c, ok := cmd.(*Cmd); ok {
			c.SetVal(value)
		}
	case CmdTypeString:
		if c, ok := cmd.(*StringCmd); ok {
			if v, ok := value.(string); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeInt:
		if c, ok := cmd.(*IntCmd); ok {
			if v, ok := value.(int64); ok {
				c.SetVal(v)
			} else if v, ok := value.(float64); ok {
				c.SetVal(int64(v))
			}
		}
	case CmdTypeBool:
		if c, ok := cmd.(*BoolCmd); ok {
			if v, ok := value.(bool); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeFloat:
		if c, ok := cmd.(*FloatCmd); ok {
			if v, ok := value.(float64); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeStringSlice:
		if c, ok := cmd.(*StringSliceCmd); ok {
			if v, ok := value.([]string); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeIntSlice:
		if c, ok := cmd.(*IntSliceCmd); ok {
			if v, ok := value.([]int64); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeFloatSlice:
		if c, ok := cmd.(*FloatSliceCmd); ok {
			if v, ok := value.([]float64); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeBoolSlice:
		if c, ok := cmd.(*BoolSliceCmd); ok {
			if v, ok := value.([]bool); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeMapStringString:
		if c, ok := cmd.(*MapStringStringCmd); ok {
			if v, ok := value.(map[string]string); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeMapStringInt:
		if c, ok := cmd.(*MapStringIntCmd); ok {
			if v, ok := value.(map[string]int64); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeMapStringInterface:
		if c, ok := cmd.(*MapStringInterfaceCmd); ok {
			if v, ok := value.(map[string]interface{}); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeSlice:
		if c, ok := cmd.(*SliceCmd); ok {
			if v, ok := value.([]interface{}); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeStatus:
		if c, ok := cmd.(*StatusCmd); ok {
			if v, ok := value.(string); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeDuration:
		if c, ok := cmd.(*DurationCmd); ok {
			if v, ok := value.(time.Duration); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeTime:
		if c, ok := cmd.(*TimeCmd); ok {
			if v, ok := value.(time.Time); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeKeyValueSlice:
		if c, ok := cmd.(*KeyValueSliceCmd); ok {
			if v, ok := value.([]KeyValue); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeStringStructMap:
		if c, ok := cmd.(*StringStructMapCmd); ok {
			if v, ok := value.(map[string]struct{}); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeXMessageSlice:
		if c, ok := cmd.(*XMessageSliceCmd); ok {
			if v, ok := value.([]XMessage); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeXStreamSlice:
		if c, ok := cmd.(*XStreamSliceCmd); ok {
			if v, ok := value.([]XStream); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeXPending:
		if c, ok := cmd.(*XPendingCmd); ok {
			if v, ok := value.(*XPending); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeXPendingExt:
		if c, ok := cmd.(*XPendingExtCmd); ok {
			if v, ok := value.([]XPendingExt); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeXAutoClaim:
		if c, ok := cmd.(*XAutoClaimCmd); ok {
			if v, ok := value.(CmdTypeXAutoClaimValue); ok {
				c.SetVal(v.messages, v.start)
			}
		}
	case CmdTypeXAutoClaimWithDeleted:
		if c, ok := cmd.(*XAutoClaimWithDeletedCmd); ok {
			if v, ok := value.(CmdTypeXAutoClaimWithDeletedValue); ok {
				c.SetVal(v.messages, v.start, v.deletedIDs)
			}
		}
	case CmdTypeXAutoClaimJustID:
		if c, ok := cmd.(*XAutoClaimJustIDCmd); ok {
			if v, ok := value.(CmdTypeXAutoClaimJustIDValue); ok {
				c.SetVal(v.ids, v.start)
			}
		}
	case CmdTypeXInfoConsumers:
		if c, ok := cmd.(*XInfoConsumersCmd); ok {
			if v, ok := value.([]XInfoConsumer); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeXInfoGroups:
		if c, ok := cmd.(*XInfoGroupsCmd); ok {
			if v, ok := value.([]XInfoGroup); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeXInfoStream:
		if c, ok := cmd.(*XInfoStreamCmd); ok {
			if v, ok := value.(*XInfoStream); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeXInfoStreamFull:
		if c, ok := cmd.(*XInfoStreamFullCmd); ok {
			if v, ok := value.(*XInfoStreamFull); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeZSlice:
		if c, ok := cmd.(*ZSliceCmd); ok {
			if v, ok := value.([]Z); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeZWithKey:
		if c, ok := cmd.(*ZWithKeyCmd); ok {
			if v, ok := value.(*ZWithKey); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeScan:
		if c, ok := cmd.(*ScanCmd); ok {
			if v, ok := value.(CmdTypeScanValue); ok {
				c.SetVal(v.keys, v.cursor)
			}
		}
	case CmdTypeClusterSlots:
		if c, ok := cmd.(*ClusterSlotsCmd); ok {
			if v, ok := value.([]ClusterSlot); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeGeoLocation:
		if c, ok := cmd.(*GeoLocationCmd); ok {
			if v, ok := value.([]GeoLocation); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeGeoSearchLocation:
		if c, ok := cmd.(*GeoSearchLocationCmd); ok {
			if v, ok := value.([]GeoLocation); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeGeoPos:
		if c, ok := cmd.(*GeoPosCmd); ok {
			if v, ok := value.([]*GeoPos); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeCommandsInfo:
		if c, ok := cmd.(*CommandsInfoCmd); ok {
			if v, ok := value.(map[string]*CommandInfo); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeSlowLog:
		if c, ok := cmd.(*SlowLogCmd); ok {
			if v, ok := value.([]SlowLog); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeMapStringStringSlice:
		if c, ok := cmd.(*MapStringStringSliceCmd); ok {
			if v, ok := value.([]map[string]string); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeMapMapStringInterface:
		if c, ok := cmd.(*MapMapStringInterfaceCmd); ok {
			if v, ok := value.(map[string]interface{}); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeMapStringInterfaceSlice:
		if c, ok := cmd.(*MapStringInterfaceSliceCmd); ok {
			if v, ok := value.([]map[string]interface{}); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeKeyValues:
		if c, ok := cmd.(*KeyValuesCmd); ok {
			// KeyValuesCmd needs a key string and values slice
			if v, ok := value.(CmdTypeKeyValuesValue); ok {
				c.SetVal(v.key, v.values)
			}
		}
	case CmdTypeZSliceWithKey:
		if c, ok := cmd.(*ZSliceWithKeyCmd); ok {
			// ZSliceWithKeyCmd needs a key string and Z slice
			if v, ok := value.(CmdTypeZSliceWithKeyValue); ok {
				c.SetVal(v.key, v.zSlice)
			}
		}
	case CmdTypeFunctionList:
		if c, ok := cmd.(*FunctionListCmd); ok {
			if v, ok := value.([]Library); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeFunctionStats:
		if c, ok := cmd.(*FunctionStatsCmd); ok {
			if v, ok := value.(FunctionStats); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeLCS:
		if c, ok := cmd.(*LCSCmd); ok {
			if v, ok := value.(*LCSMatch); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeKeyFlags:
		if c, ok := cmd.(*KeyFlagsCmd); ok {
			if v, ok := value.([]KeyFlags); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeClusterLinks:
		if c, ok := cmd.(*ClusterLinksCmd); ok {
			if v, ok := value.([]ClusterLink); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeClusterShards:
		if c, ok := cmd.(*ClusterShardsCmd); ok {
			if v, ok := value.([]ClusterShard); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeRankWithScore:
		if c, ok := cmd.(*RankWithScoreCmd); ok {
			if v, ok := value.(RankScore); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeClientInfo:
		if c, ok := cmd.(*ClientInfoCmd); ok {
			if v, ok := value.(*ClientInfo); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeACLLog:
		if c, ok := cmd.(*ACLLogCmd); ok {
			if v, ok := value.([]*ACLLogEntry); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeInfo:
		if c, ok := cmd.(*InfoCmd); ok {
			if v, ok := value.(map[string]map[string]string); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeMonitor:
		// MonitorCmd doesn't have SetVal method
		// Skip setting value for MonitorCmd
	case CmdTypeJSON:
		if c, ok := cmd.(*JSONCmd); ok {
			if v, ok := value.(string); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeJSONSlice:
		if c, ok := cmd.(*JSONSliceCmd); ok {
			if v, ok := value.([]interface{}); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeIntPointerSlice:
		if c, ok := cmd.(*IntPointerSliceCmd); ok {
			if v, ok := value.([]*int64); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeScanDump:
		if c, ok := cmd.(*ScanDumpCmd); ok {
			if v, ok := value.(ScanDump); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeBFInfo:
		if c, ok := cmd.(*BFInfoCmd); ok {
			if v, ok := value.(BFInfo); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeCFInfo:
		if c, ok := cmd.(*CFInfoCmd); ok {
			if v, ok := value.(CFInfo); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeCMSInfo:
		if c, ok := cmd.(*CMSInfoCmd); ok {
			if v, ok := value.(CMSInfo); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeTopKInfo:
		if c, ok := cmd.(*TopKInfoCmd); ok {
			if v, ok := value.(TopKInfo); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeTDigestInfo:
		if c, ok := cmd.(*TDigestInfoCmd); ok {
			if v, ok := value.(TDigestInfo); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeFTSynDump:
		if c, ok := cmd.(*FTSynDumpCmd); ok {
			if v, ok := value.([]FTSynDumpResult); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeAggregate:
		if c, ok := cmd.(*AggregateCmd); ok {
			if v, ok := value.(*FTAggregateResult); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeFTInfo:
		if c, ok := cmd.(*FTInfoCmd); ok {
			if v, ok := value.(FTInfoResult); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeFTSpellCheck:
		if c, ok := cmd.(*FTSpellCheckCmd); ok {
			if v, ok := value.([]SpellCheckResult); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeFTSearch:
		if c, ok := cmd.(*FTSearchCmd); ok {
			if v, ok := value.(FTSearchResult); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeTSTimestampValue:
		if c, ok := cmd.(*TSTimestampValueCmd); ok {
			if v, ok := value.(TSTimestampValue); ok {
				c.SetVal(v)
			}
		}
	case CmdTypeTSTimestampValueSlice:
		if c, ok := cmd.(*TSTimestampValueSliceCmd); ok {
			if v, ok := value.([]TSTimestampValue); ok {
				c.SetVal(v)
			}
		}
	default:
		// Fallback to reflection for unknown types
		return c.setCommandValueReflection(cmd, value)
	}

	return nil
}

// normalizeAggregatedCommandValue rejects mismatched results before SetVal.
func normalizeAggregatedCommandValue(cmd Cmder, value interface{}) (interface{}, error) {
	switch cmd.GetCmdType() {
	case CmdTypeInt:
		if valueFloat, ok := value.(float64); ok {
			if valueFloat != math.Trunc(valueFloat) || valueFloat < math.MinInt64 || valueFloat >= math.MaxInt64 {
				return nil, fanoutTypeError(cmd, value, "exact int64")
			}
			value = int64(valueFloat)
		}
	case CmdTypeIntSlice:
		if values, ok := value.([]float64); ok {
			converted := make([]int64, len(values))
			for i, valueFloat := range values {
				if valueFloat != math.Trunc(valueFloat) || valueFloat < math.MinInt64 || valueFloat >= math.MaxInt64 {
					return nil, fanoutTypeError(cmd, value, "[]int64")
				}
				converted[i] = int64(valueFloat)
			}
			value = converted
		}
	case CmdTypeXAutoClaim:
		if _, ok := value.(CmdTypeXAutoClaimValue); !ok {
			return nil, fanoutTypeError(cmd, value, "XAutoClaim aggregate")
		}
		return value, nil
	case CmdTypeXAutoClaimWithDeleted:
		if _, ok := value.(CmdTypeXAutoClaimWithDeletedValue); !ok {
			return nil, fanoutTypeError(cmd, value, "XAutoClaimWithDeleted aggregate")
		}
		return value, nil
	case CmdTypeXAutoClaimJustID:
		if _, ok := value.(CmdTypeXAutoClaimJustIDValue); !ok {
			return nil, fanoutTypeError(cmd, value, "XAutoClaimJustID aggregate")
		}
		return value, nil
	case CmdTypeScan:
		if _, ok := value.(CmdTypeScanValue); !ok {
			return nil, fanoutTypeError(cmd, value, "scan aggregate")
		}
		return value, nil
	case CmdTypeKeyValues:
		if _, ok := value.(CmdTypeKeyValuesValue); !ok {
			return nil, fanoutTypeError(cmd, value, "key-values aggregate")
		}
		return value, nil
	case CmdTypeZSliceWithKey:
		if _, ok := value.(CmdTypeZSliceWithKeyValue); !ok {
			return nil, fanoutTypeError(cmd, value, "sorted-set aggregate")
		}
		return value, nil
	case CmdTypeMonitor:
		return nil, fanoutTypeError(cmd, value, "unsupported monitor result")
	}

	cmdValue := reflect.ValueOf(cmd)
	if cmdValue.Kind() != reflect.Ptr || cmdValue.IsNil() {
		return nil, errInvalidCmdPointer
	}
	setVal := cmdValue.MethodByName("SetVal")
	if !setVal.IsValid() || setVal.Type().NumIn() != 1 {
		return nil, fmt.Errorf("redis: command %T does not have a compatible SetVal method", cmd)
	}
	valueType := reflect.TypeOf(value)
	if valueType == nil || !valueType.AssignableTo(setVal.Type().In(0)) {
		return nil, fanoutTypeError(cmd, value, setVal.Type().In(0).String())
	}
	return value, nil
}

// setCommandValueReflection is a fallback function that uses reflection
func (c *ClusterClient) setCommandValueReflection(cmd Cmder, value interface{}) (retErr error) {
	cmdValue := reflect.ValueOf(cmd)
	if cmdValue.Kind() != reflect.Ptr || cmdValue.IsNil() {
		return errInvalidCmdPointer
	}

	setValMethod := cmdValue.MethodByName("SetVal")
	if !setValMethod.IsValid() {
		return fmt.Errorf("redis: command %T does not have SetVal method", cmd)
	}

	args := []reflect.Value{reflect.ValueOf(value)}

	switch cmd.(type) {
	case *XAutoClaimCmd, *XAutoClaimJustIDCmd:
		args = append(args, reflect.ValueOf(""))
	case *ScanCmd:
		args = append(args, reflect.ValueOf(uint64(0)))
	case *KeyValuesCmd, *ZSliceWithKeyCmd:
		if key, ok := value.(string); ok {
			args = []reflect.Value{reflect.ValueOf(key)}
			if _, ok := cmd.(*ZSliceWithKeyCmd); ok {
				args = append(args, reflect.ValueOf([]Z{}))
			} else {
				args = append(args, reflect.ValueOf([]string{}))
			}
		}
	}

	defer func() {
		if r := recover(); r != nil {
			retErr = fmt.Errorf("redis: failed to set command value: %v", r)
			cmd.SetErr(retErr)
		}
	}()

	setValMethod.Call(args)
	return nil
}
