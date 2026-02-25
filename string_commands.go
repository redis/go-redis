package redis

import (
	"context"
	"fmt"
	"time"
)

type StringCmdable interface {
	Append(ctx context.Context, key, value string) (int64, error)
	Decr(ctx context.Context, key string) (int64, error)
	DecrBy(ctx context.Context, key string, decrement int64) (int64, error)
	DelExArgs(ctx context.Context, key string, a DelExArgs) (int64, error)
	Digest(ctx context.Context, key string) (uint64, error)
	Get(ctx context.Context, key string) (string, error)
	GetRange(ctx context.Context, key string, start, end int64) (string, error)
	GetSet(ctx context.Context, key string, value interface{}) (string, error)
	GetEx(ctx context.Context, key string, expiration time.Duration) (string, error)
	GetDel(ctx context.Context, key string) (string, error)
	Incr(ctx context.Context, key string) (int64, error)
	IncrBy(ctx context.Context, key string, value int64) (int64, error)
	IncrByFloat(ctx context.Context, key string, value float64) (float64, error)
	LCS(ctx context.Context, q *LCSQuery) (*LCSMatch, error)
	MGet(ctx context.Context, keys ...string) ([]interface{}, error)
	MSet(ctx context.Context, values ...interface{}) (string, error)
	MSetNX(ctx context.Context, values ...interface{}) (bool, error)
	MSetEX(ctx context.Context, args MSetEXArgs, values ...interface{}) (int64, error)
	Set(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error)
	SetArgs(ctx context.Context, key string, value interface{}, a SetArgs) (string, error)
	SetEx(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error)
	SetIFEQ(ctx context.Context, key string, value interface{}, matchValue interface{}, expiration time.Duration) (string, error)
	SetIFEQGet(ctx context.Context, key string, value interface{}, matchValue interface{}, expiration time.Duration) (string, error)
	SetIFNE(ctx context.Context, key string, value interface{}, matchValue interface{}, expiration time.Duration) (string, error)
	SetIFNEGet(ctx context.Context, key string, value interface{}, matchValue interface{}, expiration time.Duration) (string, error)
	SetIFDEQ(ctx context.Context, key string, value interface{}, matchDigest uint64, expiration time.Duration) (string, error)
	SetIFDEQGet(ctx context.Context, key string, value interface{}, matchDigest uint64, expiration time.Duration) (string, error)
	SetIFDNE(ctx context.Context, key string, value interface{}, matchDigest uint64, expiration time.Duration) (string, error)
	SetIFDNEGet(ctx context.Context, key string, value interface{}, matchDigest uint64, expiration time.Duration) (string, error)
	SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error)
	SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error)
	SetRange(ctx context.Context, key string, offset int64, value string) (int64, error)
	StrLen(ctx context.Context, key string) (int64, error)
}

func (c cmdable) Append(ctx context.Context, key, value string) (int64, error) {
	cmd := getIntCmd()
	cmd.ctx = ctx
	cmd.args = setArgs3(cmd.args, "append", key, value)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putIntCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}

func (c cmdable) Decr(ctx context.Context, key string) (int64, error) {
	cmd := getIntCmd()
	cmd.ctx = ctx
	cmd.args = setArgs2(cmd.args, "decr", key)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putIntCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}

func (c cmdable) DecrBy(ctx context.Context, key string, decrement int64) (int64, error) {
	cmd := getIntCmd()
	cmd.ctx = ctx
	cmd.args = setArgs3(cmd.args, "decrby", key, decrement)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putIntCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}

// DelExArgs provides arguments for the DelExArgs function.
type DelExArgs struct {
	// Mode can be `IFEQ`, `IFNE`, `IFDEQ`, or `IFDNE`.
	Mode string

	// MatchValue is used with IFEQ/IFNE modes for compare-and-delete operations.
	// - IFEQ: only delete if current value equals MatchValue
	// - IFNE: only delete if current value does not equal MatchValue
	MatchValue interface{}

	// MatchDigest is used with IFDEQ/IFDNE modes for digest-based compare-and-delete.
	// - IFDEQ: only delete if current value's digest equals MatchDigest
	// - IFDNE: only delete if current value's digest does not equal MatchDigest
	//
	// The digest is a uint64 xxh3 hash value.
	//
	// For examples of client-side digest generation, see:
	// example/digest-optimistic-locking/
	MatchDigest uint64
}

// DelExArgs Redis `DELEX key [IFEQ|IFNE|IFDEQ|IFDNE] match-value` command.
// Compare-and-delete with flexible conditions.
//
// Returns the number of keys that were removed (0 or 1).
//
// NOTE DelExArgs is still experimental
// it's signature and behaviour may change
func (c cmdable) DelExArgs(ctx context.Context, key string, a DelExArgs) (int64, error) {
	args := []interface{}{"delex", key}

	if a.Mode != "" {
		args = append(args, a.Mode)

		// Add match value/digest based on mode
		switch a.Mode {
		case "ifeq", "IFEQ", "ifne", "IFNE":
			if a.MatchValue != nil {
				args = append(args, a.MatchValue)
			}
		case "ifdeq", "IFDEQ", "ifdne", "IFDNE":
			if a.MatchDigest != 0 {
				args = append(args, fmt.Sprintf("%016x", a.MatchDigest))
			}
		}
	}

	cmd := getIntCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putIntCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}

// Digest returns the xxh3 hash (uint64) of the specified key's value.
//
// The digest is a 64-bit xxh3 hash that can be used for optimistic locking
// with SetIFDEQ, SetIFDNE, and DelExArgs commands.
//
// For examples of client-side digest generation and usage patterns, see:
// example/digest-optimistic-locking/
//
// Redis 8.4+. See https://redis.io/commands/digest/
//
// NOTE Digest is still experimental
// it's signature and behaviour may change
func (c cmdable) Digest(ctx context.Context, key string) (uint64, error) {
	cmd := getDigestCmd()
	cmd.ctx = ctx
	cmd.args = setArgs2(cmd.args, "digest", key)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putDigestCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}

// Get Redis `GET key` command. It returns redis.Nil error when key does not exist.
func (c cmdable) Get(ctx context.Context, key string) (string, error) {
	cmd := getStringCmd()
	cmd.ctx = ctx
	cmd.args = setArgs2(cmd.args, "get", key)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStringCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

func (c cmdable) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	cmd := getStringCmd()
	cmd.ctx = ctx
	cmd.args = setArgs4(cmd.args, "getrange", key, start, end)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStringCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// GetSet returns the old value stored at key and sets it to the new value.
//
// Deprecated: Use SetArgs with Get option instead as of Redis 6.2.0.
func (c cmdable) GetSet(ctx context.Context, key string, value interface{}) (string, error) {
	cmd := getStringCmd()
	cmd.ctx = ctx
	cmd.args = setArgs3(cmd.args, "getset", key, value)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStringCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// GetEx An expiration of zero removes the TTL associated with the key (i.e. GETEX key persist).
// Requires Redis >= 6.2.0.
func (c cmdable) GetEx(ctx context.Context, key string, expiration time.Duration) (string, error) {
	args := make([]interface{}, 0, 4)
	args = append(args, "getex", key)
	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == 0 {
		args = append(args, "persist")
	}

	cmd := getStringCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStringCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// GetDel redis-server version >= 6.2.0.
func (c cmdable) GetDel(ctx context.Context, key string) (string, error) {
	cmd := getStringCmd()
	cmd.ctx = ctx
	cmd.args = setArgs2(cmd.args, "getdel", key)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStringCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

func (c cmdable) Incr(ctx context.Context, key string) (int64, error) {
	cmd := getIntCmd()
	cmd.ctx = ctx
	cmd.args = setArgs2(cmd.args, "incr", key)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putIntCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}

func (c cmdable) IncrBy(ctx context.Context, key string, value int64) (int64, error) {
	cmd := getIntCmd()
	cmd.ctx = ctx
	cmd.args = setArgs3(cmd.args, "incrby", key, value)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putIntCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}

func (c cmdable) IncrByFloat(ctx context.Context, key string, value float64) (float64, error) {
	cmd := getFloatCmd()
	cmd.ctx = ctx
	cmd.args = setArgs3(cmd.args, "incrbyfloat", key, value)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putFloatCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}

type SetCondition string

const (
	// NX only set the keys and their expiration if none exist
	NX SetCondition = "NX"
	// XX only set the keys and their expiration if all already exist
	XX SetCondition = "XX"
)

type ExpirationMode string

const (
	// EX sets expiration in seconds
	EX ExpirationMode = "EX"
	// PX sets expiration in milliseconds
	PX ExpirationMode = "PX"
	// EXAT sets expiration as Unix timestamp in seconds
	EXAT ExpirationMode = "EXAT"
	// PXAT sets expiration as Unix timestamp in milliseconds
	PXAT ExpirationMode = "PXAT"
	// KEEPTTL keeps the existing TTL
	KEEPTTL ExpirationMode = "KEEPTTL"
)

type ExpirationOption struct {
	Mode  ExpirationMode
	Value int64
}

func (c cmdable) LCS(ctx context.Context, q *LCSQuery) (*LCSMatch, error) {
	cmd := NewLCSCmd(ctx, q)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	// Note: LCS is complex and doesn't use pooling yet due to custom initialization in NewLCSCmd
	if err != nil {
		return nil, err
	}
	return val, cmdErr
}

func (c cmdable) MGet(ctx context.Context, keys ...string) ([]interface{}, error) {
	args := make([]interface{}, 1+len(keys))
	args[0] = "mget"
	for i, key := range keys {
		args[1+i] = key
	}
	cmd := getSliceCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putSliceCmd(cmd)
	if err != nil {
		return nil, err
	}
	return val, cmdErr
}

// MSet is like Set but accepts multiple values:
//   - MSet("key1", "value1", "key2", "value2")
//   - MSet([]string{"key1", "value1", "key2", "value2"})
//   - MSet(map[string]interface{}{"key1": "value1", "key2": "value2"})
//   - MSet(struct), For struct types, see HSet description.
func (c cmdable) MSet(ctx context.Context, values ...interface{}) (string, error) {
	args := make([]interface{}, 1, 1+len(values))
	args[0] = "mset"
	args = appendArgs(args, values)
	cmd := getStatusCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStatusCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// MSetNX is like SetNX but accepts multiple values:
//   - MSetNX("key1", "value1", "key2", "value2")
//   - MSetNX([]string{"key1", "value1", "key2", "value2"})
//   - MSetNX(map[string]interface{}{"key1": "value1", "key2": "value2"})
//   - MSetNX(struct), For struct types, see HSet description.
func (c cmdable) MSetNX(ctx context.Context, values ...interface{}) (bool, error) {
	args := make([]interface{}, 1, 1+len(values))
	args[0] = "msetnx"
	args = appendArgs(args, values)
	cmd := getBoolCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putBoolCmd(cmd)
	if err != nil {
		return false, err
	}
	return val, cmdErr
}

type MSetEXArgs struct {
	Condition  SetCondition
	Expiration *ExpirationOption
}

// MSetEX sets the given keys to their respective values.
// This command is an extension of the MSETNX that adds expiration and XX options.
// Available since Redis 8.4
// Important: When this method is used with Cluster clients, all keys
// must be in the same hash slot, otherwise CROSSSLOT error will be returned.
// For more information, see https://redis.io/commands/msetex
func (c cmdable) MSetEX(ctx context.Context, args MSetEXArgs, values ...interface{}) (int64, error) {
	expandedArgs := appendArgs([]interface{}{}, values)
	numkeys := len(expandedArgs) / 2

	cmdArgs := make([]interface{}, 0, 2+len(expandedArgs)+3)
	cmdArgs = append(cmdArgs, "msetex", numkeys)
	cmdArgs = append(cmdArgs, expandedArgs...)

	if args.Condition != "" {
		cmdArgs = append(cmdArgs, string(args.Condition))
	}

	if args.Expiration != nil {
		switch args.Expiration.Mode {
		case EX:
			cmdArgs = append(cmdArgs, "ex", args.Expiration.Value)
		case PX:
			cmdArgs = append(cmdArgs, "px", args.Expiration.Value)
		case EXAT:
			cmdArgs = append(cmdArgs, "exat", args.Expiration.Value)
		case PXAT:
			cmdArgs = append(cmdArgs, "pxat", args.Expiration.Value)
		case KEEPTTL:
			cmdArgs = append(cmdArgs, "keepttl")
		}
	}

	cmd := getIntCmd()
	cmd.ctx = ctx
	cmd.args = cmdArgs
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putIntCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}

// Set Redis `SET key value [expiration]` command.
// Use expiration for `SETEx`-like behavior.
//
// Zero expiration means the key has no expiration time.
// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
func (c cmdable) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error) {
	args := make([]interface{}, 3, 5)
	args[0] = "set"
	args[1] = key
	args[2] = value
	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	cmd := getStatusCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStatusCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// SetArgs provides arguments for the SetArgs function.
type SetArgs struct {
	// Mode can be `NX`, `XX`, `IFEQ`, `IFNE`, `IFDEQ`, `IFDNE` or empty.
	Mode string

	// MatchValue is used with IFEQ/IFNE modes for compare-and-set operations.
	// - IFEQ: only set if current value equals MatchValue
	// - IFNE: only set if current value does not equal MatchValue
	MatchValue interface{}

	// MatchDigest is used with IFDEQ/IFDNE modes for digest-based compare-and-set.
	// - IFDEQ: only set if current value's digest equals MatchDigest
	// - IFDNE: only set if current value's digest does not equal MatchDigest
	//
	// The digest is a uint64 xxh3 hash value.
	//
	// For examples of client-side digest generation, see:
	// example/digest-optimistic-locking/
	MatchDigest uint64

	// Zero `TTL` or `Expiration` means that the key has no expiration time.
	TTL      time.Duration
	ExpireAt time.Time

	// When Get is true, the command returns the old value stored at key, or nil when key did not exist.
	Get bool

	// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
	// otherwise you will receive an error: (error) ERR syntax error.
	KeepTTL bool
}

// SetArgs supports all the options that the SET command supports.
// It is the alternative to the Set function when you want
// to have more control over the options.
func (c cmdable) SetArgs(ctx context.Context, key string, value interface{}, a SetArgs) (string, error) {
	args := []interface{}{"set", key, value}

	if a.KeepTTL {
		args = append(args, "keepttl")
	}

	if !a.ExpireAt.IsZero() {
		args = append(args, "exat", a.ExpireAt.Unix())
	}
	if a.TTL > 0 {
		if usePrecise(a.TTL) {
			args = append(args, "px", formatMs(ctx, a.TTL))
		} else {
			args = append(args, "ex", formatSec(ctx, a.TTL))
		}
	}

	if a.Mode != "" {
		args = append(args, a.Mode)

		// Add match value/digest for CAS modes
		switch a.Mode {
		case "ifeq", "IFEQ", "ifne", "IFNE":
			if a.MatchValue != nil {
				args = append(args, a.MatchValue)
			}
		case "ifdeq", "IFDEQ", "ifdne", "IFDNE":
			if a.MatchDigest != 0 {
				args = append(args, fmt.Sprintf("%016x", a.MatchDigest))
			}
		}
	}

	if a.Get {
		args = append(args, "get")
	}

	cmd := getStatusCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStatusCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// SetEx sets the value and expiration of a key.
//
// Deprecated: Use Set with expiration instead as of Redis 2.6.12.
func (c cmdable) SetEx(ctx context.Context, key string, value interface{}, expiration time.Duration) (string, error) {
	cmd := getStatusCmd()
	cmd.baseCmd = baseCmd{ctx: ctx, args: []interface{}{"setex", key, formatSec(ctx, expiration), value}}
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStatusCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// SetNX sets the value of a key only if the key does not exist.
//
// Deprecated: Use Set with NX option instead as of Redis 2.6.12.
//
// Zero expiration means the key has no expiration time.
// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
func (c cmdable) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	var args []interface{}
	switch expiration {
	case 0:
		// Use old `SETNX` to support old Redis versions.
		args = []interface{}{"setnx", key, value}
	case KeepTTL:
		args = []interface{}{"set", key, value, "keepttl", "nx"}
	default:
		if usePrecise(expiration) {
			args = []interface{}{"set", key, value, "px", formatMs(ctx, expiration), "nx"}
		} else {
			args = []interface{}{"set", key, value, "ex", formatSec(ctx, expiration), "nx"}
		}
	}

	cmd := getBoolCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putBoolCmd(cmd)
	if err != nil {
		return false, err
	}
	return val, cmdErr
}

// SetXX Redis `SET key value [expiration] XX` command.
//
// Zero expiration means the key has no expiration time.
// KeepTTL is a Redis KEEPTTL option to keep existing TTL, it requires your redis-server version >= 6.0,
// otherwise you will receive an error: (error) ERR syntax error.
func (c cmdable) SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) (bool, error) {
	var args []interface{}
	switch expiration {
	case 0:
		args = []interface{}{"set", key, value, "xx"}
	case KeepTTL:
		args = []interface{}{"set", key, value, "keepttl", "xx"}
	default:
		if usePrecise(expiration) {
			args = []interface{}{"set", key, value, "px", formatMs(ctx, expiration), "xx"}
		} else {
			args = []interface{}{"set", key, value, "ex", formatSec(ctx, expiration), "xx"}
		}
	}

	cmd := getBoolCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putBoolCmd(cmd)
	if err != nil {
		return false, err
	}
	return val, cmdErr
}

// SetIFEQ Redis `SET key value [expiration] IFEQ match-value` command.
// Compare-and-set: only sets the value if the current value equals matchValue.
//
// Returns "OK" on success.
// Returns nil if the operation was aborted due to condition not matching.
// Zero expiration means the key has no expiration time.
//
// NOTE SetIFEQ is still experimental
// it's signature and behaviour may change
func (c cmdable) SetIFEQ(ctx context.Context, key string, value interface{}, matchValue interface{}, expiration time.Duration) (string, error) {
	args := []interface{}{"set", key, value}

	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	args = append(args, "ifeq", matchValue)

	cmd := getStatusCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStatusCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// SetIFEQGet Redis `SET key value [expiration] IFEQ match-value GET` command.
// Compare-and-set with GET: only sets the value if the current value equals matchValue,
// and returns the previous value.
//
// Returns the previous value on success.
// Returns nil if the operation was aborted due to condition not matching.
// Zero expiration means the key has no expiration time.
//
// NOTE SetIFEQGet is still experimental
// it's signature and behaviour may change
func (c cmdable) SetIFEQGet(ctx context.Context, key string, value interface{}, matchValue interface{}, expiration time.Duration) (string, error) {
	args := []interface{}{"set", key, value}

	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	args = append(args, "ifeq", matchValue, "get")

	cmd := getStringCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStringCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// SetIFNE Redis `SET key value [expiration] IFNE match-value` command.
// Compare-and-set: only sets the value if the current value does not equal matchValue.
//
// Returns "OK" on success.
// Returns nil if the operation was aborted due to condition not matching.
// Zero expiration means the key has no expiration time.
//
// NOTE SetIFNE is still experimental
// it's signature and behaviour may change
func (c cmdable) SetIFNE(ctx context.Context, key string, value interface{}, matchValue interface{}, expiration time.Duration) (string, error) {
	args := []interface{}{"set", key, value}

	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	args = append(args, "ifne", matchValue)

	cmd := getStatusCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStatusCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// SetIFNEGet Redis `SET key value [expiration] IFNE match-value GET` command.
// Compare-and-set with GET: only sets the value if the current value does not equal matchValue,
// and returns the previous value.
//
// Returns the previous value on success.
// Returns nil if the operation was aborted due to condition not matching.
// Zero expiration means the key has no expiration time.
//
// NOTE SetIFNEGet is still experimental
// it's signature and behaviour may change
func (c cmdable) SetIFNEGet(ctx context.Context, key string, value interface{}, matchValue interface{}, expiration time.Duration) (string, error) {
	args := []interface{}{"set", key, value}

	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	args = append(args, "ifne", matchValue, "get")

	cmd := getStringCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStringCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// SetIFDEQ sets the value only if the current value's digest equals matchDigest.
//
// This is a compare-and-set operation using xxh3 digest for optimistic locking.
// The matchDigest parameter is a uint64 xxh3 hash value.
//
// Returns "OK" on success.
// Returns redis.Nil if the digest doesn't match (value was modified).
// Zero expiration means the key has no expiration time.
//
// For examples of client-side digest generation and usage patterns, see:
// example/digest-optimistic-locking/
//
// Redis 8.4+. See https://redis.io/commands/set/
//
// NOTE SetIFNEQ is still experimental
// it's signature and behaviour may change
func (c cmdable) SetIFDEQ(ctx context.Context, key string, value interface{}, matchDigest uint64, expiration time.Duration) (string, error) {
	args := []interface{}{"set", key, value}

	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	args = append(args, "ifdeq", fmt.Sprintf("%016x", matchDigest))

	cmd := getStatusCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStatusCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// SetIFDEQGet sets the value only if the current value's digest equals matchDigest,
// and returns the previous value.
//
// This is a compare-and-set operation using xxh3 digest for optimistic locking.
// The matchDigest parameter is a uint64 xxh3 hash value.
//
// Returns the previous value on success.
// Returns redis.Nil if the digest doesn't match (value was modified).
// Zero expiration means the key has no expiration time.
//
// For examples of client-side digest generation and usage patterns, see:
// example/digest-optimistic-locking/
//
// Redis 8.4+. See https://redis.io/commands/set/
//
// NOTE SetIFNEQGet is still experimental
// it's signature and behaviour may change
func (c cmdable) SetIFDEQGet(ctx context.Context, key string, value interface{}, matchDigest uint64, expiration time.Duration) (string, error) {
	args := []interface{}{"set", key, value}

	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	args = append(args, "ifdne", fmt.Sprintf("%016x", matchDigest), "get")

	cmd := getStringCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStringCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// SetIFDNE sets the value only if the current value's digest does NOT equal matchDigest.
//
// This is a compare-and-set operation using xxh3 digest for optimistic locking.
// The matchDigest parameter is a uint64 xxh3 hash value.
//
// Returns "OK" on success (digest didn't match, value was set).
// Returns redis.Nil if the digest matches (value was not modified).
// Zero expiration means the key has no expiration time.
//
// For examples of client-side digest generation and usage patterns, see:
// example/digest-optimistic-locking/
//
// Redis 8.4+. See https://redis.io/commands/set/
//
// NOTE SetIFDNE is still experimental
// it's signature and behaviour may change
func (c cmdable) SetIFDNE(ctx context.Context, key string, value interface{}, matchDigest uint64, expiration time.Duration) (string, error) {
	args := []interface{}{"set", key, value}

	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	args = append(args, "ifdne", fmt.Sprintf("%016x", matchDigest))

	cmd := getStatusCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStatusCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

// SetIFDNEGet sets the value only if the current value's digest does NOT equal matchDigest,
// and returns the previous value.
//
// This is a compare-and-set operation using xxh3 digest for optimistic locking.
// The matchDigest parameter is a uint64 xxh3 hash value.
//
// Returns the previous value on success (digest didn't match, value was set).
// Returns redis.Nil if the digest matches (value was not modified).
// Zero expiration means the key has no expiration time.
//
// For examples of client-side digest generation and usage patterns, see:
// example/digest-optimistic-locking/
//
// Redis 8.4+. See https://redis.io/commands/set/
//
// NOTE SetIFDNEGet is still experimental
// it's signature and behaviour may change
func (c cmdable) SetIFDNEGet(ctx context.Context, key string, value interface{}, matchDigest uint64, expiration time.Duration) (string, error) {
	args := []interface{}{"set", key, value}

	if expiration > 0 {
		if usePrecise(expiration) {
			args = append(args, "px", formatMs(ctx, expiration))
		} else {
			args = append(args, "ex", formatSec(ctx, expiration))
		}
	} else if expiration == KeepTTL {
		args = append(args, "keepttl")
	}

	args = append(args, "ifdne", fmt.Sprintf("%016x", matchDigest), "get")

	cmd := getStringCmd()
	cmd.ctx = ctx
	cmd.args = args
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putStringCmd(cmd)
	if err != nil {
		return "", err
	}
	return val, cmdErr
}

func (c cmdable) SetRange(ctx context.Context, key string, offset int64, value string) (int64, error) {
	cmd := getIntCmd()
	cmd.ctx = ctx
	cmd.args = setArgs4(cmd.args, "setrange", key, offset, value)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putIntCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}

func (c cmdable) StrLen(ctx context.Context, key string) (int64, error) {
	cmd := getIntCmd()
	cmd.ctx = ctx
	cmd.args = setArgs2(cmd.args, "strlen", key)
	err := c(ctx, cmd)
	val, cmdErr := cmd.Val(), cmd.Err()
	putIntCmd(cmd)
	if err != nil {
		return 0, err
	}
	return val, cmdErr
}
