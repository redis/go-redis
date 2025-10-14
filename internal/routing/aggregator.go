package routing

import (
	"errors"
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/redis/go-redis/v9/internal/util"
)

var (
	ErrMaxAggregation = errors.New("redis: no valid results to aggregate for max operation")
	ErrMinAggregation = errors.New("redis: no valid results to aggregate for min operation")
	ErrAndAggregation = errors.New("redis: no valid results to aggregate for logical AND operation")
	ErrOrAggregation  = errors.New("redis: no valid results to aggregate for logical OR operation")
)

// ResponseAggregator defines the interface for aggregating responses from multiple shards.
type ResponseAggregator interface {
	// Add processes a single shard response.
	Add(result interface{}, err error) error

	// AddWithKey processes a single shard response for a specific key (used by keyed aggregators).
	AddWithKey(key string, result interface{}, err error) error

	BatchAdd(map[string]interface{}, error) error

	// Result returns the final aggregated result and any error.
	Result() (interface{}, error)
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
		return &AggMinAggregator{
			res: util.NewAtomicMin(),
		}
	case RespAggMax:
		return &AggMaxAggregator{
			res: util.NewAtomicMax(),
		}
	case RespAggLogicalAnd:
		andAgg := &AggLogicalAndAggregator{}
		andAgg.res.Add(1)

		return andAgg
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
	err atomic.Value
	res atomic.Value
}

func (a *AllSucceededAggregator) Add(result interface{}, err error) error {
	if err != nil {
		a.err.CompareAndSwap(nil, err)
		return nil
	}

	if result != nil {
		a.res.CompareAndSwap(nil, result)
	}

	return nil
}

func (a *AllSucceededAggregator) BatchAdd(results map[string]interface{}, err error) error {
	for _, res := range results {
		_ = a.Add(res, err)
	}

	return nil
}

func (a *AllSucceededAggregator) Result() (interface{}, error) {
	var err error
	res, e := a.res.Load(), a.err.Load()
	if e != nil {
		err = e.(error)
	}

	return res, err
}

func (a *AllSucceededAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

// OneSucceededAggregator returns the first non-error reply,
// if all shards errored, returns any one of those errors.
type OneSucceededAggregator struct {
	err atomic.Value
	res atomic.Value
}

func (a *OneSucceededAggregator) Add(result interface{}, err error) error {
	if err != nil {
		a.err.CompareAndSwap(nil, err)
		return nil
	}

	if result != nil {
		a.res.CompareAndSwap(nil, result)
	}

	return nil
}

func (a *OneSucceededAggregator) BatchAdd(results map[string]interface{}, err error) error {
	for _, res := range results {
		_ = a.Add(res, err)
	}

	return nil
}

func (a *OneSucceededAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *OneSucceededAggregator) Result() (interface{}, error) {
	res, e := a.res.Load(), a.err.Load()
	if res != nil {
		return nil, e.(error)
	}

	return res, nil
}

// AggSumAggregator sums numeric replies from all shards.
type AggSumAggregator struct {
	err atomic.Value
	res *int64
}

func (a *AggSumAggregator) Add(result interface{}, err error) error {
	if err != nil {
		a.err.CompareAndSwap(nil, err)
	}

	if result != nil {
		val, err := toInt64(result)
		if err != nil {
			return err
		}
		atomic.AddInt64(a.res, val)
	}

	return nil
}

func (a *AggSumAggregator) BatchAdd(results map[string]interface{}, err error) error {
	for _, res := range results {
		_ = a.Add(res, err)
	}

	return nil
}

func (a *AggSumAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AggSumAggregator) Result() (interface{}, error) {
	res, err := atomic.LoadInt64(a.res), a.err.Load()
	if err != nil {
		return nil, err.(error)
	}

	return res, nil
}

// AggMinAggregator returns the minimum numeric value from all shards.
type AggMinAggregator struct {
	err atomic.Value
	res *util.AtomicMin
}

func (a *AggMinAggregator) Add(result interface{}, err error) error {
	if err != nil {
		a.err.CompareAndSwap(nil, err)
		return nil
	}

	intVal, e := toInt64(result)
	if e != nil {
		a.err.CompareAndSwap(nil, err)
		return nil
	}

	a.res.Value(intVal)

	return nil
}

func (a *AggMinAggregator) BatchAdd(results map[string]interface{}, err error) error {
	for _, res := range results {
		_ = a.Add(res, err)
	}

	return nil
}

func (a *AggMinAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AggMinAggregator) Result() (interface{}, error) {
	err := a.err.Load()
	if err != nil {
		return nil, err.(error)
	}

	val, hasVal := a.res.Min()
	if !hasVal {
		return nil, ErrMinAggregation
	}
	return val, nil
}

// AggMaxAggregator returns the maximum numeric value from all shards.
type AggMaxAggregator struct {
	err atomic.Value
	res *util.AtomicMax
}

func (a *AggMaxAggregator) Add(result interface{}, err error) error {
	if err != nil {
		a.err.CompareAndSwap(nil, err)
		return nil
	}

	intVal, e := toInt64(result)
	if e != nil {
		a.err.CompareAndSwap(nil, err)
		return nil
	}

	a.res.Value(intVal)

	return nil
}

func (a *AggMaxAggregator) BatchAdd(results map[string]interface{}, err error) error {
	for _, res := range results {
		_ = a.Add(res, err)
	}

	return nil
}

func (a *AggMaxAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AggMaxAggregator) Result() (interface{}, error) {
	err := a.err.Load()
	if err != nil {
		return nil, err.(error)
	}

	val, hasVal := a.res.Max()
	if !hasVal {
		return nil, ErrMaxAggregation
	}
	return val, nil
}

// AggLogicalAndAggregator performs logical AND on boolean values.
type AggLogicalAndAggregator struct {
	err       atomic.Value
	res       atomic.Int64
	hasResult atomic.Bool
}

func (a *AggLogicalAndAggregator) Add(result interface{}, err error) error {
	if err != nil {
		a.err.CompareAndSwap(nil, err)
		return nil
	}

	val, e := toBool(result)
	if e != nil {
		a.err.CompareAndSwap(nil, e)
		return nil
	}

	if val {
		a.res.And(1)
	} else {
		a.res.And(0)
	}

	a.hasResult.Store(true)

	return nil
}

func (a *AggLogicalAndAggregator) BatchAdd(results map[string]interface{}, err error) error {
	for _, res := range results {
		_ = a.Add(res, err)
	}

	return nil
}

func (a *AggLogicalAndAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AggLogicalAndAggregator) Result() (interface{}, error) {
	err := a.err.Load()
	if err != nil {
		return nil, err.(error)
	}

	if !a.hasResult.Load() {
		return nil, ErrAndAggregation
	}
	return a.res.Load() != 0, nil
}

// AggLogicalOrAggregator performs logical OR on boolean values.
type AggLogicalOrAggregator struct {
	err       atomic.Value
	res       atomic.Int64
	hasResult atomic.Bool
}

func (a *AggLogicalOrAggregator) Add(result interface{}, err error) error {
	if err != nil {
		a.err.CompareAndSwap(nil, err)
		return nil
	}

	val, e := toBool(result)
	if e != nil {
		a.err.CompareAndSwap(nil, e)
		return nil
	}

	if val {
		a.res.Or(1)
	} else {
		a.res.Or(0)
	}

	a.hasResult.Store(true)

	return nil
}

func (a *AggLogicalOrAggregator) BatchAdd(results map[string]interface{}, err error) error {
	for _, res := range results {
		_ = a.Add(res, err)
	}

	return nil
}

func (a *AggLogicalOrAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *AggLogicalOrAggregator) Result() (interface{}, error) {
	err := a.err.Load()
	if err != nil {
		return nil, err.(error)
	}

	if !a.hasResult.Load() {
		return nil, ErrOrAggregation
	}
	return a.res.Load() != 0, nil
}

func toInt64(val interface{}) (int64, error) {
	if val == nil {
		return 0, nil
	}
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
	if val == nil {
		return false, nil
	}
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

func (a *DefaultKeylessAggregator) add(result interface{}, err error) error {
	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil {
		a.results = append(a.results, result)
	}
	return nil
}

func (a *DefaultKeylessAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.add(result, err)
}

func (a *DefaultKeylessAggregator) BatchAdd(results map[string]interface{}, err error) error {
	for _, res := range results {
		_ = a.add(res, err)
	}

	return nil
}

func (a *DefaultKeylessAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *DefaultKeylessAggregator) Result() (interface{}, error) {
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

func (a *DefaultKeyedAggregator) add(result interface{}, err error) error {
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

func (a *DefaultKeyedAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.add(result, err)
}

func (a *DefaultKeyedAggregator) BatchAdd(results map[string]interface{}, err error) error {
	for _, res := range results {
		_ = a.add(res, err)
	}

	return nil
}

func (a *DefaultKeyedAggregator) addWithKey(key string, result interface{}, err error) error {
	if err != nil && a.firstErr == nil {
		a.firstErr = err
		return nil
	}
	if err == nil {
		a.results[key] = result
	}
	return nil
}

func (a *DefaultKeyedAggregator) AddWithKey(key string, result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.addWithKey(key, result, err)
}

func (a *DefaultKeyedAggregator) BatchAddWithKeyOrder(results map[string]interface{}, keyOrder []string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.keyOrder = keyOrder
	for key, val := range results {
		_ = a.addWithKey(key, val, nil)
	}

	return nil
}

func (a *DefaultKeyedAggregator) SetKeyOrder(keyOrder []string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.keyOrder = keyOrder
}

func (a *DefaultKeyedAggregator) Result() (interface{}, error) {
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

func (a *SpecialAggregator) add(result interface{}, err error) error {
	a.results = append(a.results, result)
	a.errors = append(a.errors, err)
	return nil
}

func (a *SpecialAggregator) Add(result interface{}, err error) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.add(result, err)
}

func (a *SpecialAggregator) BatchAdd(results map[string]interface{}, err error) error {
	for _, res := range results {
		_ = a.add(res, err)
	}

	return nil
}

func (a *SpecialAggregator) AddWithKey(key string, result interface{}, err error) error {
	return a.Add(result, err)
}

func (a *SpecialAggregator) Result() (interface{}, error) {
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
		agg.aggregatorFunc = fn
	}
	return agg
}
