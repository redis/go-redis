package routing

import (
	"fmt"
	"math"
	"sync"
)

type CmdTyper interface {
	GetCmdType() CmdType
}

type CmdType uint8

const (
	CmdTypeGeneric CmdType = iota
	CmdTypeString
	CmdTypeInt
	CmdTypeBool
	CmdTypeFloat
	CmdTypeStringSlice
	CmdTypeIntSlice
	CmdTypeFloatSlice
	CmdTypeBoolSlice
	CmdTypeMapStringString
	CmdTypeMapStringInt
	CmdTypeMapStringInterface
	CmdTypeMapStringInterfaceSlice
	CmdTypeSlice
	CmdTypeStatus
	CmdTypeDuration
	CmdTypeTime
	CmdTypeKeyValueSlice
	CmdTypeStringStructMap
	CmdTypeXMessageSlice
	CmdTypeXStreamSlice
	CmdTypeXPending
	CmdTypeXPendingExt
	CmdTypeXAutoClaim
	CmdTypeXAutoClaimJustID
	CmdTypeXInfoConsumers
	CmdTypeXInfoGroups
	CmdTypeXInfoStream
	CmdTypeXInfoStreamFull
	CmdTypeZSlice
	CmdTypeZWithKey
	CmdTypeScan
	CmdTypeClusterSlots
	CmdTypeGeoLocation
	CmdTypeGeoSearchLocation
	CmdTypeGeoPos
	CmdTypeCommandsInfo
	CmdTypeSlowLog
	CmdTypeMapStringStringSlice
	CmdTypeMapMapStringInterface
	CmdTypeKeyValues
	CmdTypeZSliceWithKey
	CmdTypeFunctionList
	CmdTypeFunctionStats
	CmdTypeLCS
	CmdTypeKeyFlags
	CmdTypeClusterLinks
	CmdTypeClusterShards
	CmdTypeRankWithScore
	CmdTypeClientInfo
	CmdTypeACLLog
	CmdTypeInfo
	CmdTypeMonitor
	CmdTypeJSON
	CmdTypeJSONSlice
	CmdTypeIntPointerSlice
	CmdTypeScanDump
	CmdTypeBFInfo
	CmdTypeCFInfo
	CmdTypeCMSInfo
	CmdTypeTopKInfo
	CmdTypeTDigestInfo
	CmdTypeFTSynDump
	CmdTypeAggregate
	CmdTypeFTInfo
	CmdTypeFTSpellCheck
	CmdTypeFTSearch
	CmdTypeTSTimestampValue
	CmdTypeTSTimestampValueSlice
)

// ResponseAggregator defines the interface for aggregating responses from multiple shards.
type ResponseAggregator interface {
	// Add processes a single shard response.
	Add(result interface{}, err error) error

	// AddWithKey processes a single shard response for a specific key (used by keyed aggregators).
	AddWithKey(key string, result interface{}, err error) error

	// Finish returns the final aggregated result and any error.
	Finish() (interface{}, error)
}

// NewResponseAggregator creates an aggregator based on the response policy.
func NewResponseAggregator(policy ResponsePolicy, cmdName string) ResponseAggregator {
	switch policy {
	case RespDefaultKeyless:
		return &DefaultKeylessAggregator{}
	case RespDefaultHashSlot:
		return &DefaultKeyedAggregator{}
	case RespAllSucceeded:
		return &AllSucceededAggregator{}
	case RespOneSucceeded:
		return &OneSucceededAggregator{}
	case RespAggSum:
		return &AggSumAggregator{}
	case RespAggMin:
		return &AggMinAggregator{}
	case RespAggMax:
		return &AggMaxAggregator{}
	case RespAggLogicalAnd:
		return &AggLogicalAndAggregator{}
	case RespAggLogicalOr:
		return &AggLogicalOrAggregator{}
	case RespSpecial:
		return NewSpecialAggregator(cmdName)
	default:
		return &AllSucceededAggregator{}
	}
}

func NewDefaultAggregator(isKeyed bool) ResponseAggregator {
	if isKeyed {
		return &DefaultKeyedAggregator{
			results: make(map[string]interface{}),
		}
	}
	return &DefaultKeylessAggregator{}
}

// AllSucceededAggregator returns one non-error reply if every shard succeeded,
// propagates the first error otherwise.
type AllSucceededAggregator struct {
	mu        sync.Mutex
	result    interface{}
	firstErr  error
	hasResult bool
}

func (a *AllSucceededAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil && !a.hasResult {
		a.result = result
		a.hasResult = true
	}
	return nil
}

func (a *AllSucceededAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AllSucceededAggregator) Finish() (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.firstErr != nil {
		return nil, a.firstErr
	}
	return a.result, nil
}

// OneSucceededAggregator returns the first non-error reply,
// if all shards errored, returns any one of those errors.
type OneSucceededAggregator struct {
	mu        sync.Mutex
	result    interface{}
	firstErr  error
	hasResult bool
}

func (a *OneSucceededAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil && !a.hasResult {
		a.result = result
		a.hasResult = true
	}
	return nil
}

func (a *OneSucceededAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *OneSucceededAggregator) Finish() (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.hasResult {
		return a.result, nil
	}
	return nil, a.firstErr
}

// AggSumAggregator sums numeric replies from all shards.
type AggSumAggregator struct {
	mu        sync.Mutex
	sum       int64
	hasResult bool
	firstErr  error
}

func (a *AggSumAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil {
		val, err := toInt64(result)
		if err != nil && a.firstErr == nil {
			a.firstErr = err
			return nil
		}
		if err == nil {
			a.sum += val
			a.hasResult = true
		}
	}
	return nil
}

func (a *AggSumAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AggSumAggregator) Finish() (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.firstErr != nil {
		return nil, a.firstErr
	}
	return a.sum, nil
}

// AggMinAggregator returns the minimum numeric value from all shards.
type AggMinAggregator struct {
	mu        sync.Mutex
	min       int64
	hasResult bool
	firstErr  error
}

func (a *AggMinAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil {
		val, err := toInt64(result)
		if err != nil && a.firstErr == nil {
			a.firstErr = err
			return nil
		}
		if err == nil {
			if !a.hasResult || val < a.min {
				a.min = val
				a.hasResult = true
			}
		}
	}
	return nil
}

func (a *AggMinAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AggMinAggregator) Finish() (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.firstErr != nil {
		return nil, a.firstErr
	}
	return a.min, nil
}

// AggMaxAggregator returns the maximum numeric value from all shards.
type AggMaxAggregator struct {
	mu        sync.Mutex
	max       int64
	hasResult bool
	firstErr  error
}

func (a *AggMaxAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil {
		val, err := toInt64(result)
		if err != nil && a.firstErr == nil {
			a.firstErr = err
			return nil
		}
		if err == nil {
			if !a.hasResult || val > a.max {
				a.max = val
				a.hasResult = true
			}
		}
	}
	return nil
}

func (a *AggMaxAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AggMaxAggregator) Finish() (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.firstErr != nil {
		return nil, a.firstErr
	}
	return a.max, nil
}

// AggLogicalAndAggregator performs logical AND on boolean values.
type AggLogicalAndAggregator struct {
	mu        sync.Mutex
	result    bool
	hasResult bool
	firstErr  error
}

func (a *AggLogicalAndAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil {
		val, err := toBool(result)
		if err != nil && a.firstErr == nil {
			a.firstErr = err
			return nil
		}
		if err == nil {
			if !a.hasResult {
				a.result = val
				a.hasResult = true
			} else {
				a.result = a.result && val
			}
		}
	}
	return nil
}

func (a *AggLogicalAndAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AggLogicalAndAggregator) Finish() (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.firstErr != nil {
		return nil, a.firstErr
	}
	return a.result, nil
}

// AggLogicalOrAggregator performs logical OR on boolean values.
type AggLogicalOrAggregator struct {
	mu        sync.Mutex
	result    bool
	hasResult bool
	firstErr  error
}

func (a *AggLogicalOrAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil {
		val, err := toBool(result)
		if err != nil && a.firstErr == nil {
			a.firstErr = err
			return nil
		}
		if err == nil {
			if !a.hasResult {
				a.result = val
				a.hasResult = true
			} else {
				a.result = a.result || val
			}
		}
	}
	return nil
}

func (a *AggLogicalOrAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AggLogicalOrAggregator) Finish() (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.firstErr != nil {
		return nil, a.firstErr
	}
	return a.result, nil
}

func toInt64(val interface{}) (int64, error) {
	switch v := val.(type) {
	case int64:
		return v, nil
	case int:
		return int64(v), nil
	case int32:
		return int64(v), nil
	case float64:
		if v != math.Trunc(v) {
			return 0, fmt.Errorf("cannot convert float %f to int64", v)
		}
		return int64(v), nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", val)
	}
}

func toBool(val interface{}) (bool, error) {
	switch v := val.(type) {
	case bool:
		return v, nil
	case int64:
		return v != 0, nil
	case int:
		return v != 0, nil
	default:
		return false, fmt.Errorf("cannot convert %T to bool", val)
	}
}

// DefaultKeylessAggregator collects all results in an array, order doesn't matter.
type DefaultKeylessAggregator struct {
	mu       sync.Mutex
	results  []interface{}
	firstErr error
}

func (a *DefaultKeylessAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil {
		a.results = append(a.results, result)
	}
	return nil
}

func (a *DefaultKeylessAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *DefaultKeylessAggregator) Finish() (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.firstErr != nil {
		return nil, a.firstErr
	}
	return a.results, nil
}

// DefaultKeyedAggregator reassembles replies in the exact key order of the original request.
type DefaultKeyedAggregator struct {
	mu       sync.Mutex
	results  map[string]interface{}
	keyOrder []string
	firstErr error
}

func NewDefaultKeyedAggregator(keyOrder []string) *DefaultKeyedAggregator {
	return &DefaultKeyedAggregator{
		results:  make(map[string]interface{}),
		keyOrder: keyOrder,
	}
}

func (a *DefaultKeyedAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	// For non-keyed Add, just collect the result without ordering
	if err == nil {
		a.results["__default__"] = result
	}
	return nil
}

func (a *DefaultKeyedAggregator) AddWithKey(key string, result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil {
		a.results[key] = result
	}
	return nil
}

func (a *DefaultKeyedAggregator) SetKeyOrder(keyOrder []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.keyOrder = keyOrder
}

func (a *DefaultKeyedAggregator) Finish() (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.firstErr != nil {
		return nil, a.firstErr
	}

	// If no explicit key order is set, return results in any order
	if len(a.keyOrder) == 0 {
		orderedResults := make([]interface{}, 0, len(a.results))
		for _, result := range a.results {
			orderedResults = append(orderedResults, result)
		}
		return orderedResults, nil
	}

	// Return results in the exact key order
	orderedResults := make([]interface{}, len(a.keyOrder))
	for i, key := range a.keyOrder {
		if result, exists := a.results[key]; exists {
			orderedResults[i] = result
		}
	}
	return orderedResults, nil
}

// SpecialAggregator provides a registry for command-specific aggregation logic.
type SpecialAggregator struct {
	mu             sync.Mutex
	aggregatorFunc func([]interface{}, []error) (interface{}, error)
	results        []interface{}
	errors         []error
}

func (a *SpecialAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.results = append(a.results, result)
	a.errors = append(a.errors, err)
	return nil
}

func (a *SpecialAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *SpecialAggregator) Finish() (interface{}, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if a.aggregatorFunc != nil {
		return a.aggregatorFunc(a.results, a.errors)
	}
	// Default behavior: return first non-error result or first error
	for i, err := range a.errors {
		if err == nil {
			return a.results[i], nil
		}
	}
	if len(a.errors) > 0 {
		return nil, a.errors[0]
	}
	return nil, nil
}

// SetAggregatorFunc allows setting custom aggregation logic for special commands.
func (a *SpecialAggregator) SetAggregatorFunc(fn func([]interface{}, []error) (interface{}, error)) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.aggregatorFunc = fn
}

// SpecialAggregatorRegistry holds custom aggregation functions for specific commands.
var SpecialAggregatorRegistry = make(map[string]func([]interface{}, []error) (interface{}, error))

// RegisterSpecialAggregator registers a custom aggregation function for a command.
func RegisterSpecialAggregator(cmdName string, fn func([]interface{}, []error) (interface{}, error)) {
	SpecialAggregatorRegistry[cmdName] = fn
}

// NewSpecialAggregator creates a special aggregator with command-specific logic if available.
func NewSpecialAggregator(cmdName string) *SpecialAggregator {
	agg := &SpecialAggregator{}
	if fn, exists := SpecialAggregatorRegistry[cmdName]; exists {
		agg.SetAggregatorFunc(fn)
	}
	return agg
}

// CmdTypeGetter interface for getting command type without circular imports
type CmdTypeGetter interface {
	GetCmdType() CmdType
}

// ExtractCommandValue extracts the value from a command result using the fast enum-based approach
func ExtractCommandValue(cmd interface{}) interface{} {
	// First try to get the command type using the interface
	if cmdTypeGetter, ok := cmd.(CmdTypeGetter); ok {
		cmdType := cmdTypeGetter.GetCmdType()

		// Use fast type-based extraction
		switch cmdType {
		case CmdTypeString:
			if stringCmd, ok := cmd.(interface{ Val() string }); ok {
				return stringCmd.Val()
			}
		case CmdTypeInt:
			if intCmd, ok := cmd.(interface{ Val() int64 }); ok {
				return intCmd.Val()
			}
		case CmdTypeBool:
			if boolCmd, ok := cmd.(interface{ Val() bool }); ok {
				return boolCmd.Val()
			}
		case CmdTypeFloat:
			if floatCmd, ok := cmd.(interface{ Val() float64 }); ok {
				return floatCmd.Val()
			}
		case CmdTypeDuration:
			if durationCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return durationCmd.Val()
			}
		case CmdTypeTime:
			if timeCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return timeCmd.Val()
			}
		case CmdTypeStringSlice:
			if stringSliceCmd, ok := cmd.(interface{ Val() []string }); ok {
				return stringSliceCmd.Val()
			}
		case CmdTypeIntSlice:
			if intSliceCmd, ok := cmd.(interface{ Val() []int64 }); ok {
				return intSliceCmd.Val()
			}
		case CmdTypeBoolSlice:
			if boolSliceCmd, ok := cmd.(interface{ Val() []bool }); ok {
				return boolSliceCmd.Val()
			}
		case CmdTypeFloatSlice:
			if floatSliceCmd, ok := cmd.(interface{ Val() []float64 }); ok {
				return floatSliceCmd.Val()
			}
		case CmdTypeMapStringString:
			if mapCmd, ok := cmd.(interface{ Val() map[string]string }); ok {
				return mapCmd.Val()
			}
		case CmdTypeMapStringInt:
			if mapCmd, ok := cmd.(interface{ Val() map[string]int64 }); ok {
				return mapCmd.Val()
			}
		case CmdTypeMapStringInterfaceSlice:
			if mapCmd, ok := cmd.(interface {
				Val() map[string][]interface{}
			}); ok {
				return mapCmd.Val()
			}
		case CmdTypeMapStringInterface:
			if mapCmd, ok := cmd.(interface{ Val() map[string]interface{} }); ok {
				return mapCmd.Val()
			}
		case CmdTypeMapStringStringSlice:
			if mapCmd, ok := cmd.(interface{ Val() map[string][]string }); ok {
				return mapCmd.Val()
			}
		case CmdTypeMapMapStringInterface:
			if mapCmd, ok := cmd.(interface {
				Val() map[string][]interface{}
			}); ok {
				return mapCmd.Val()
			}
		case CmdTypeStringStructMap:
			if mapCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return mapCmd.Val()
			}
		case CmdTypeXMessageSlice:
			if xMsgCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return xMsgCmd.Val()
			}
		case CmdTypeXStreamSlice:
			if xStreamCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return xStreamCmd.Val()
			}
		case CmdTypeXPending:
			if xPendingCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return xPendingCmd.Val()
			}
		case CmdTypeXPendingExt:
			if xPendingExtCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return xPendingExtCmd.Val()
			}
		case CmdTypeXAutoClaim:
			if xAutoClaimCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return xAutoClaimCmd.Val()
			}
		case CmdTypeXAutoClaimJustID:
			if xAutoClaimJustIDCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return xAutoClaimJustIDCmd.Val()
			}
		case CmdTypeXInfoConsumers:
			if xInfoConsumersCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return xInfoConsumersCmd.Val()
			}
		case CmdTypeXInfoGroups:
			if xInfoGroupsCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return xInfoGroupsCmd.Val()
			}
		case CmdTypeXInfoStream:
			if xInfoStreamCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return xInfoStreamCmd.Val()
			}
		case CmdTypeXInfoStreamFull:
			if xInfoStreamFullCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return xInfoStreamFullCmd.Val()
			}
		case CmdTypeZSlice:
			if zSliceCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return zSliceCmd.Val()
			}
		case CmdTypeZWithKey:
			if zWithKeyCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return zWithKeyCmd.Val()
			}
		case CmdTypeScan:
			if scanCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return scanCmd.Val()
			}
		case CmdTypeClusterSlots:
			if clusterSlotsCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return clusterSlotsCmd.Val()
			}
		case CmdTypeGeoSearchLocation:
			if geoSearchLocationCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return geoSearchLocationCmd.Val()
			}
		case CmdTypeGeoPos:
			if geoPosCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return geoPosCmd.Val()
			}
		case CmdTypeCommandsInfo:
			if commandsInfoCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return commandsInfoCmd.Val()
			}
		case CmdTypeSlowLog:
			if slowLogCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return slowLogCmd.Val()
			}

		case CmdTypeKeyValues:
			if keyValuesCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return keyValuesCmd.Val()
			}
		case CmdTypeZSliceWithKey:
			if zSliceWithKeyCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return zSliceWithKeyCmd.Val()
			}
		case CmdTypeFunctionList:
			if functionListCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return functionListCmd.Val()
			}
		case CmdTypeFunctionStats:
			if functionStatsCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return functionStatsCmd.Val()
			}
		case CmdTypeLCS:
			if lcsCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return lcsCmd.Val()
			}
		case CmdTypeKeyFlags:
			if keyFlagsCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return keyFlagsCmd.Val()
			}
		case CmdTypeClusterLinks:
			if clusterLinksCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return clusterLinksCmd.Val()
			}
		case CmdTypeClusterShards:
			if clusterShardsCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return clusterShardsCmd.Val()
			}
		case CmdTypeRankWithScore:
			if rankWithScoreCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return rankWithScoreCmd.Val()
			}
		case CmdTypeClientInfo:
			if clientInfoCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return clientInfoCmd.Val()
			}
		case CmdTypeACLLog:
			if aclLogCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return aclLogCmd.Val()
			}
		case CmdTypeInfo:
			if infoCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return infoCmd.Val()
			}
		case CmdTypeMonitor:
			if monitorCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return monitorCmd.Val()
			}
		case CmdTypeJSON:
			if jsonCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return jsonCmd.Val()
			}
		case CmdTypeJSONSlice:
			if jsonSliceCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return jsonSliceCmd.Val()
			}
		case CmdTypeIntPointerSlice:
			if intPointerSliceCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return intPointerSliceCmd.Val()
			}
		case CmdTypeScanDump:
			if scanDumpCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return scanDumpCmd.Val()
			}
		case CmdTypeBFInfo:
			if bfInfoCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return bfInfoCmd.Val()
			}
		case CmdTypeCFInfo:
			if cfInfoCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return cfInfoCmd.Val()
			}
		case CmdTypeCMSInfo:
			if cmsInfoCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return cmsInfoCmd.Val()
			}
		case CmdTypeTopKInfo:
			if topKInfoCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return topKInfoCmd.Val()
			}
		case CmdTypeTDigestInfo:
			if tDigestInfoCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return tDigestInfoCmd.Val()
			}
		case CmdTypeFTSearch:
			if ftSearchCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return ftSearchCmd.Val()
			}
		case CmdTypeFTInfo:
			if ftInfoCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return ftInfoCmd.Val()
			}
		case CmdTypeFTSpellCheck:
			if ftSpellCheckCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return ftSpellCheckCmd.Val()
			}
		case CmdTypeFTSynDump:
			if ftSynDumpCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return ftSynDumpCmd.Val()
			}
		case CmdTypeAggregate:
			if aggregateCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return aggregateCmd.Val()
			}
		case CmdTypeTSTimestampValue:
			if tsTimestampValueCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return tsTimestampValueCmd.Val()
			}
		case CmdTypeTSTimestampValueSlice:
			if tsTimestampValueSliceCmd, ok := cmd.(interface{ Val() interface{} }); ok {
				return tsTimestampValueSliceCmd.Val()
			}
		default:
			// For unknown command types, return nil
			return nil
		}
	}

	// If we can't get the command type, return nil
	return nil
}
