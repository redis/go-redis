package redis

import (
	"context"
	"time"
)

type HashCmdable interface {
	HDel(ctx context.Context, key string, fields ...string) *IntCmd
	HExists(ctx context.Context, key, field string) *BoolCmd
	HGet(ctx context.Context, key, field string) *StringCmd
	HGetAll(ctx context.Context, key string) *MapStringStringCmd
	HIncrBy(ctx context.Context, key, field string, incr int64) *IntCmd
	HIncrByFloat(ctx context.Context, key, field string, incr float64) *FloatCmd
	HKeys(ctx context.Context, key string) *StringSliceCmd
	HLen(ctx context.Context, key string) *IntCmd
	HMGet(ctx context.Context, key string, fields ...string) *SliceCmd
	HSet(ctx context.Context, key string, values ...interface{}) *IntCmd
	HMSet(ctx context.Context, key string, values ...interface{}) *BoolCmd
	HSetNX(ctx context.Context, key, field string, value interface{}) *BoolCmd
	HScan(ctx context.Context, key string, cursor uint64, match string, count int64) *ScanCmd
	HVals(ctx context.Context, key string) *StringSliceCmd
	HRandField(ctx context.Context, key string, count int) *StringSliceCmd
	HRandFieldWithValues(ctx context.Context, key string, count int) *KeyValueSliceCmd
}

func (c cmdable) HDel(ctx context.Context, key string, fields ...string) *IntCmd {
	args := make([]interface{}, 2+len(fields))
	args[0] = "hdel"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HExists(ctx context.Context, key, field string) *BoolCmd {
	cmd := NewBoolCmd(ctx, "hexists", key, field)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HGet(ctx context.Context, key, field string) *StringCmd {
	cmd := NewStringCmd(ctx, "hget", key, field)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HGetAll(ctx context.Context, key string) *MapStringStringCmd {
	cmd := NewMapStringStringCmd(ctx, "hgetall", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HIncrBy(ctx context.Context, key, field string, incr int64) *IntCmd {
	cmd := NewIntCmd(ctx, "hincrby", key, field, incr)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HIncrByFloat(ctx context.Context, key, field string, incr float64) *FloatCmd {
	cmd := NewFloatCmd(ctx, "hincrbyfloat", key, field, incr)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HKeys(ctx context.Context, key string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "hkeys", key)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HLen(ctx context.Context, key string) *IntCmd {
	cmd := NewIntCmd(ctx, "hlen", key)
	_ = c(ctx, cmd)
	return cmd
}

// HMGet returns the values for the specified fields in the hash stored at key.
// It returns an interface{} to distinguish between empty string and nil value.
func (c cmdable) HMGet(ctx context.Context, key string, fields ...string) *SliceCmd {
	args := make([]interface{}, 2+len(fields))
	args[0] = "hmget"
	args[1] = key
	for i, field := range fields {
		args[2+i] = field
	}
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HSet accepts values in following formats:
//
//   - HSet("myhash", "key1", "value1", "key2", "value2")
//
//   - HSet("myhash", []string{"key1", "value1", "key2", "value2"})
//
//   - HSet("myhash", map[string]interface{}{"key1": "value1", "key2": "value2"})
//
//     Playing struct With "redis" tag.
//     type MyHash struct { Key1 string `redis:"key1"`; Key2 int `redis:"key2"` }
//
//   - HSet("myhash", MyHash{"value1", "value2"}) Warn: redis-server >= 4.0
//
//     For struct, can be a structure pointer type, we only parse the field whose tag is redis.
//     if you don't want the field to be read, you can use the `redis:"-"` flag to ignore it,
//     or you don't need to set the redis tag.
//     For the type of structure field, we only support simple data types:
//     string, int/uint(8,16,32,64), float(32,64), time.Time(to RFC3339Nano), time.Duration(to Nanoseconds ),
//     if you are other more complex or custom data types, please implement the encoding.BinaryMarshaler interface.
//
// Note that in older versions of Redis server(redis-server < 4.0), HSet only supports a single key-value pair.
// redis-docs: https://redis.io/commands/hset (Starting with Redis version 4.0.0: Accepts multiple field and value arguments.)
// If you are using a Struct type and the number of fields is greater than one,
// you will receive an error similar to "ERR wrong number of arguments", you can use HMSet as a substitute.
func (c cmdable) HSet(ctx context.Context, key string, values ...interface{}) *IntCmd {
	args := make([]interface{}, 2, 2+len(values))
	args[0] = "hset"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HMSet is a deprecated version of HSet left for compatibility with Redis 3.
func (c cmdable) HMSet(ctx context.Context, key string, values ...interface{}) *BoolCmd {
	args := make([]interface{}, 2, 2+len(values))
	args[0] = "hmset"
	args[1] = key
	args = appendArgs(args, values)
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HSetNX(ctx context.Context, key, field string, value interface{}) *BoolCmd {
	cmd := NewBoolCmd(ctx, "hsetnx", key, field, value)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HVals(ctx context.Context, key string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "hvals", key)
	_ = c(ctx, cmd)
	return cmd
}

// HRandField redis-server version >= 6.2.0.
func (c cmdable) HRandField(ctx context.Context, key string, count int) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "hrandfield", key, count)
	_ = c(ctx, cmd)
	return cmd
}

// HRandFieldWithValues redis-server version >= 6.2.0.
func (c cmdable) HRandFieldWithValues(ctx context.Context, key string, count int) *KeyValueSliceCmd {
	cmd := NewKeyValueSliceCmd(ctx, "hrandfield", key, count, "withvalues")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HScan(ctx context.Context, key string, cursor uint64, match string, count int64) *ScanCmd {
	args := []interface{}{"hscan", key, cursor}
	if match != "" {
		args = append(args, "match", match)
	}
	if count > 0 {
		args = append(args, "count", count)
	}
	cmd := NewScanCmd(ctx, c, args...)
	_ = c(ctx, cmd)
	return cmd
}

type HExpireArgs struct {
	NX bool
	XX bool
	GT bool
	LT bool
}

// HExpire - Sets the expiration time for specified fields in a hash in seconds.
// The command constructs an argument list starting with "HEXPIRE", followed by the key, duration, any conditional flags, and the specified fields.
// For more information - https://redis.io/commands/hexpire/
func (c cmdable) HExpire(ctx context.Context, key string, expiration time.Duration, fields ...string) *IntSliceCmd {
	args := []interface{}{"HEXPIRE", key, expiration, len(fields)}

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HExpire - Sets the expiration time for specified fields in a hash in seconds.
// It requires a key, an expiration duration, a struct with boolean flags for conditional expiration settings (NX, XX, GT, LT), and a list of fields.
// The command constructs an argument list starting with "HEXPIRE", followed by the key, duration, any conditional flags, and the specified fields.
// For more information - https://redis.io/commands/hexpire/
func (c cmdable) HExpireWithArgs(ctx context.Context, key string, expiration time.Duration, expirationArgs HExpireArgs, fields ...string) *IntSliceCmd {
	args := []interface{}{"HEXPIRE", key, expiration}

	// only if one argument is true, we can add it to the args
	// if more than one argument is true, it will cause an error
	if expirationArgs.NX {
		args = append(args, "NX")
	} else if expirationArgs.XX {
		args = append(args, "XX")
	} else if expirationArgs.GT {
		args = append(args, "GT")
	} else if expirationArgs.LT {
		args = append(args, "LT")
	}

	args = append(args, len(fields))

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HPExpire - Sets the expiration time for specified fields in a hash in milliseconds.
// Similar to HExpire, it accepts a key, an expiration duration in milliseconds, a struct with expiration condition flags, and a list of fields.
// The command modifies the standard time.Duration to milliseconds for the Redis command.
// For more information - https://redis.io/commands/hpexpire/
func (c cmdable) HPExpire(ctx context.Context, key string, expiration time.Duration, fields ...string) *IntSliceCmd {
	args := []interface{}{"HPEXPIRE", key, formatMs(ctx, expiration), len(fields)}

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HPExpireWithArgs(ctx context.Context, key string, expiration time.Duration, expirationArgs HExpireArgs, fields ...string) *IntSliceCmd {
	args := []interface{}{"HPEXPIRE", key, formatMs(ctx, expiration)}

	// only if one argument is true, we can add it to the args
	// if more than one argument is true, it will cause an error
	if expirationArgs.NX {
		args = append(args, "NX")
	} else if expirationArgs.XX {
		args = append(args, "XX")
	} else if expirationArgs.GT {
		args = append(args, "GT")
	} else if expirationArgs.LT {
		args = append(args, "LT")
	}

	args = append(args, len(fields))

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HExpireAt - Sets the expiration time for specified fields in a hash to a UNIX timestamp in seconds.
// Takes a key, a UNIX timestamp, a struct of conditional flags, and a list of fields.
// The command sets absolute expiration times based on the UNIX timestamp provided.
// For more information - https://redis.io/commands/hexpireat/
func (c cmdable) HExpireAt(ctx context.Context, key string, tm time.Time, fields ...string) *IntSliceCmd {

	args := []interface{}{"HEXPIREAT", key, tm.Unix(), len(fields)}

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HExpireAtWithArgs(ctx context.Context, key string, tm time.Time, expirationArgs HExpireArgs, fields ...string) *IntSliceCmd {
	args := []interface{}{"HEXPIREAT", key, tm.Unix()}

	// only if one argument is true, we can add it to the args
	// if more than one argument is true, it will cause an error
	if expirationArgs.NX {
		args = append(args, "NX")
	} else if expirationArgs.XX {
		args = append(args, "XX")
	} else if expirationArgs.GT {
		args = append(args, "GT")
	} else if expirationArgs.LT {
		args = append(args, "LT")
	}

	args = append(args, len(fields))

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HPExpireAt - Sets the expiration time for specified fields in a hash to a UNIX timestamp in milliseconds.
// Similar to HExpireAt but for timestamps in milliseconds. It accepts the same parameters and adjusts the UNIX time to milliseconds.
// For more information - https://redis.io/commands/hpexpireat/
func (c cmdable) HPExpireAt(ctx context.Context, key string, tm time.Time, fields ...string) *IntSliceCmd {
	args := []interface{}{"HPEXPIREAT", key, tm.UnixNano() / int64(time.Millisecond), len(fields)}

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) HPExpireAtWithArgs(ctx context.Context, key string, tm time.Time, expirationArgs HExpireArgs, fields ...string) *IntSliceCmd {
	args := []interface{}{"HPEXPIREAT", key, tm.UnixNano() / int64(time.Millisecond)}

	// only if one argument is true, we can add it to the args
	// if more than one argument is true, it will cause an error
	if expirationArgs.NX {
		args = append(args, "NX")
	} else if expirationArgs.XX {
		args = append(args, "XX")
	} else if expirationArgs.GT {
		args = append(args, "GT")
	} else if expirationArgs.LT {
		args = append(args, "LT")
	}

	args = append(args, len(fields))

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HPersist - Removes the expiration time from specified fields in a hash.
// Accepts a key and the fields themselves.
// This command ensures that each field specified will have its expiration removed if present.
// For more information - https://redis.io/commands/hpersist/
func (c cmdable) HPersist(ctx context.Context, key string, fields ...string) *IntSliceCmd {
	args := []interface{}{"HPERSIST", key, len(fields)}

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HExpireTime - Retrieves the expiration time for specified fields in a hash as a UNIX timestamp in seconds.
// Requires a key and the fields themselves to fetch their expiration timestamps.
// This command returns the expiration times for each field or error/status codes for each field as specified.
// For more information - https://redis.io/commands/hexpiretime/
func (c cmdable) HExpireTime(ctx context.Context, key string, fields ...string) *IntSliceCmd {
	args := []interface{}{"HEXPIRETIME", key, len(fields)}

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HPExpireTime - Retrieves the expiration time for specified fields in a hash as a UNIX timestamp in milliseconds.
// Similar to HExpireTime, adjusted for timestamps in milliseconds. It requires the same parameters.
// Provides the expiration timestamp for each field in milliseconds.
// For more information - https://redis.io/commands/hexpiretime/
func (c cmdable) HPExpireTime(ctx context.Context, key string, fields ...string) *IntSliceCmd {
	args := []interface{}{"HPEXPIRETIME", key, len(fields)}

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HTTL - Retrieves the remaining time to live for specified fields in a hash in seconds.
// Requires a key and the fields themselves. It returns the TTL for each specified field.
// This command fetches the TTL in seconds for each field or returns error/status codes as appropriate.
// For more information - https://redis.io/commands/httl/
func (c cmdable) HTTL(ctx context.Context, key string, fields ...string) *IntSliceCmd {
	args := []interface{}{"HTTL", key, len(fields)}

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// HPTTL - Retrieves the remaining time to live for specified fields in a hash in milliseconds.
// Similar to HTTL, but returns the TTL in milliseconds. It requires a key and the specified fields.
// This command provides the TTL in milliseconds for each field or returns error/status codes as needed.
// For more information - https://redis.io/commands/hpttl/
func (c cmdable) HPTTL(ctx context.Context, key string, fields ...string) *IntSliceCmd {
	args := []interface{}{"HPTTL", key, len(fields)}

	for _, field := range fields {
		args = append(args, field)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type HGetFArgs struct {
	Persist bool
	EX      time.Duration
	PX      time.Duration
	EXAT    time.Time
	PXAT    time.Time
}

// HGetF - Retrieves the value of a specified field in a hash and optionally updates its expiration time.
// Accepts a key, a single field, and a struct containing various expiration parameters such as:
// EX (seconds), PX (milliseconds), EXAT (UNIX time in seconds), and PXAT (UNIX time in milliseconds).
// It also allows for the removal of the expiration time with the 'PERSIST' option.
// The command constructs an argument list starting with "HGETF", followed by the key, the field, and any of the specified expiration settings.
// For more information - https://redis.io/commands/hgetf/
func (c cmdable) HGetF(ctx context.Context, key string, args HGetFArgs, field ...string) *StringSliceCmd {
	argsSlice := []interface{}{"HGETF", key}

	if args.Persist {
		argsSlice = append(argsSlice, "PERSIST")
	} else if args.EX > 0 {
		argsSlice = append(argsSlice, "EX", args.EX)
	} else if args.PX > 0 {
		argsSlice = append(argsSlice, "PX", formatMs(ctx, args.PX))
	} else if !args.EXAT.IsZero() {
		argsSlice = append(argsSlice, "EXAT", args.EXAT.Unix())
	} else if !args.PXAT.IsZero() {
		argsSlice = append(argsSlice, "PXAT", args.PXAT.UnixNano()/int64(time.Millisecond))
	}

	argsSlice = append(argsSlice, "FIELDS", len(field))
	for _, f := range field {
		argsSlice = append(argsSlice, f)
	}
	cmd := NewStringSliceCmd(ctx, argsSlice...)
	_ = c(ctx, cmd)
	return cmd
}

// HGetFWithArgs - Retrieves the value of a specified field in a hash with additional conditional expiration settings.
// Similar to HGetF but includes conditional flags (NX, XX, GT, LT) to determine when to apply the expiration settings.
// This function builds upon HGetF by adding these conditional flags, which control the setting of the expiration based on the current state of the field.
// For more information - https://redis.io/commands/hgetf/
func (c cmdable) HGetFWithArgs(ctx context.Context, key, field string, expirationArgs HExpireArgs, args HGetFArgs) *StringSliceCmd {
	argsSlice := []interface{}{"HGETF", key, field}

	// only if one argument is true, we can add it to the args
	// if more than one argument is true, it will cause an error
	if expirationArgs.NX {
		argsSlice = append(argsSlice, "NX")
	} else if expirationArgs.XX {
		argsSlice = append(argsSlice, "XX")
	} else if expirationArgs.GT {
		argsSlice = append(argsSlice, "GT")
	} else if expirationArgs.LT {
		argsSlice = append(argsSlice, "LT")
	}

	if args.Persist {
		argsSlice = append(argsSlice, "PERSIST")
	} else if args.EX > 0 {
		argsSlice = append(argsSlice, "EX", args.EX)
	} else if args.PX > 0 {
		argsSlice = append(argsSlice, "PX", formatMs(ctx, args.PX))
	} else if !args.EXAT.IsZero() {
		argsSlice = append(argsSlice, "EXAT", args.EXAT.Unix())
	} else if !args.PXAT.IsZero() {
		argsSlice = append(argsSlice, "PXAT", args.PXAT.UnixNano()/int64(time.Millisecond))
	}

	cmd := NewStringSliceCmd(ctx, argsSlice...)
	_ = c(ctx, cmd)
	return cmd
}

type HSetFArgs struct {
	DC      bool
	DCF     bool
	DOF     bool
	NX      bool
	XX      bool
	GT      bool
	LT      bool
	GETNEW  bool
	GETOLD  bool
	EX      time.Duration
	PX      time.Duration
	EXAT    time.Time
	PXAT    time.Time
	KEEPTTL bool
}

// HSetF - Sets field-value pairs in a hash with optional expiration and value retrieval settings.
// Accepts a key, a struct of options that includes directives for field creation and modification behavior (DC, DCF, DOF),
// expiration settings (EX, PX, EXAT, PXAT, KEEPTTL), and value retrieval options (GETNEW, GETOLD).
// The command supports conditional settings (NX, XX, GT, LT) to govern how and when fields are updated or expired based on their current state.
// It builds a command that can conditionally set values and expiration times, potentially returning the old or new values of fields.
// For more information - https://redis.io/commands/hsetf/
func (c cmdable) HSetF(ctx context.Context, key string, args HSetFArgs, fieldValues ...string) *StringSliceCmd {
	argsSlice := []interface{}{"HSETF", key}

	if args.DC {
		argsSlice = append(argsSlice, "DC")
	}

	if args.DCF {
		argsSlice = append(argsSlice, "DCF")
	} else if args.DOF {
		argsSlice = append(argsSlice, "DOF")
	}

	if args.NX {
		argsSlice = append(argsSlice, "NX")
	} else if args.XX {
		argsSlice = append(argsSlice, "XX")
	} else if args.GT {
		argsSlice = append(argsSlice, "GT")
	} else if args.LT {
		argsSlice = append(argsSlice, "LT")
	}

	if args.GETNEW {
		argsSlice = append(argsSlice, "GETNEW")
	} else if args.GETOLD {
		argsSlice = append(argsSlice, "GETOLD")
	}

	if args.KEEPTTL {
		argsSlice = append(argsSlice, "KEEPTTL")
	} else if args.EX > 0 {
		argsSlice = append(argsSlice, "EX", args.EX)
	} else if args.PX > 0 {
		argsSlice = append(argsSlice, "PX", formatMs(ctx, args.PX))
	} else if !args.EXAT.IsZero() {
		argsSlice = append(argsSlice, "EXAT", args.EXAT.Unix())
	} else if !args.PXAT.IsZero() {
		argsSlice = append(argsSlice, "PXAT", args.PXAT.UnixNano()/int64(time.Millisecond))
	}

	argsSlice = append(argsSlice, "FVS", len(fieldValues)/2)

	for _, fieldValue := range fieldValues {
		argsSlice = append(argsSlice, fieldValue)
	}

	cmd := NewStringSliceCmd(ctx, argsSlice...)
	_ = c(ctx, cmd)
	return cmd
}
