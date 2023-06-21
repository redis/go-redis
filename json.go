package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9/internal/proto"
)

// -------------------------------------------

type JSONCmdAble interface {
	JSONArrAppend(ctx context.Context, key, path string, values ...interface{}) *IntSliceCmd
	JSONArrIndex(ctx context.Context, key, path string, value interface{}) *IntSliceCmd
	JSONArrIndexStartStop(ctx context.Context, key, path string, value interface{}, start, stop int64) *IntSliceCmd
	JSONArrInsert(ctx context.Context, key, path string, index int64, values ...interface{}) *IntSliceCmd
	JSONArrLen(ctx context.Context, key, path string) *IntSliceCmd
	JSONArrPop(ctx context.Context, key, path string, index int) *StringSliceCmd
	JSONArrTrim(ctx context.Context, key, path string, start, stop int) *IntSliceCmd
	JSONClear(ctx context.Context, key, path string) *IntCmd
	JSONDel(ctx context.Context, key, path string) *IntCmd
	JSONForget(ctx context.Context, key, path string) *IntCmd
	JSONGet(ctx context.Context, key string, paths ...string) *JSONStringCmd
	JSONMGet(ctx context.Context, path string, keys ...string) *JSONSliceCmd
	JSONNumIncrBy(ctx context.Context, key, path string, value float64) *JSONStringCmd
	JSONObjKeys(ctx context.Context, key, path string) *SliceCmd
	JSONObjLen(ctx context.Context, key, path string) *IntPointerSliceCmd
	JSONSet(ctx context.Context, key, path string, value interface{}) *StatusCmd
	JSONSetMode(ctx context.Context, key, path string, value interface{}, mode string) *StatusCmd
	JSONStrAppend(ctx context.Context, key, path, value string) *IntPointerSliceCmd
	JSONStrLen(ctx context.Context, key, path string) *IntPointerSliceCmd
	JSONToggle(ctx context.Context, key, path string) *IntPointerSliceCmd
	JSONType(ctx context.Context, key, path string) *StringSliceCmd
}

type JSONStringCmd struct {
	baseCmd
	val []interface{}
	raw string
}

var _ Cmder = (*JSONStringCmd)(nil)

func NewJSONStringCmd(ctx context.Context, args ...interface{}) *JSONStringCmd {

	return &JSONStringCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *JSONStringCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *JSONStringCmd) SetVal(val []interface{}) {
	cmd.val = val
}

func (cmd *JSONStringCmd) Val() []interface{} {
	return cmd.val
}

func (cmd *JSONStringCmd) Result() ([]interface{}, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *JSONStringCmd) Scan(index int, dst interface{}) error {
	if cmd.Err() != nil {
		return cmd.Err()
	}

	if index < 0 || index >= len(cmd.val) {
		return fmt.Errorf("JSONCmd.Scan - %d is out of range (0..%d)", index, len(cmd.val))
	}

	results := []json.RawMessage{}
	if err := json.Unmarshal([]byte(cmd.raw), &results); err != nil {
		return err
	} else {
		return json.Unmarshal(results[index], dst)
	}
}

func (cmd *JSONStringCmd) readReply(rd *proto.Reader) error {

	// nil response from JSON.(M)GET (cmd.baseCmd.err will be "redis: nil")
	if cmd.baseCmd.Err() == Nil {
		cmd.val = nil
		cmd.SetErr(nil)
		return nil
	}

	var err error
	if cmd.raw, err = rd.ReadString(); err != nil && err != Nil {
		return err
	} else if cmd.raw == "" || err == Nil {
		cmd.val = nil
	} else {
		cmd.val = []interface{}{}
		if err := json.Unmarshal([]byte(cmd.raw), &cmd.val); err != nil {
			return err
		}
	}

	return nil
}

// -------------------------------------------

type JSONSliceCmd struct {
	baseCmd
	val [][]interface{}
}

func NewJSONSliceCmd(ctx context.Context, args ...interface{}) *JSONSliceCmd {
	return &JSONSliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *JSONSliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *JSONSliceCmd) SetVal(val [][]interface{}) {
	cmd.val = val
}

func (cmd *JSONSliceCmd) Val() [][]interface{} {
	return cmd.val
}

func (cmd *JSONSliceCmd) Result() ([][]interface{}, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *JSONSliceCmd) readReply(rd *proto.Reader) error {

	n, err := rd.ReadArrayLen()
	if err != nil {
		return err
	}
	cmd.val = make([][]interface{}, n)
	for i := 0; i < len(cmd.val); i++ {
		switch s, err := rd.ReadString(); {
		case err == Nil:
			cmd.val[i] = nil
		case err != nil:
			return err
		default:
			objects := []interface{}{}
			if err := json.Unmarshal([]byte(s), &objects); err != nil {
				return err
			} else {
				cmd.val[i] = objects
			}
		}
	}
	return nil
}

/*******************************************************************************
*
* IntPointerSliceCmd
* used to represent a RedisJSON response where the result is either an integer or nil
*
*******************************************************************************/

type IntPointerSliceCmd struct {
	baseCmd
	val []*int64
}

// NewIntPointerSliceCmd initialises an IntPointerSliceCmd
func NewIntPointerSliceCmd(ctx context.Context, args ...interface{}) *IntPointerSliceCmd {
	return &IntPointerSliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *IntPointerSliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *IntPointerSliceCmd) SetVal(val []*int64) {
	cmd.val = val
}

func (cmd *IntPointerSliceCmd) Val() []*int64 {
	return cmd.val
}

func (cmd *IntPointerSliceCmd) Result() ([]*int64, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *IntPointerSliceCmd) readReply(rd *proto.Reader) error {

	n, err := rd.ReadArrayLen()
	if err != nil {
		return err
	}
	cmd.val = make([]*int64, n)

	for i := 0; i < len(cmd.val); i++ {
		val, err := rd.ReadInt()
		if err != nil && err != Nil {
			return err
		} else if err != Nil {
			cmd.val[i] = &val
		}
	}

	return nil
}

//------------------------------------------------------------------------------

// JSONArrAppend adds the provided json values to the end of the at path
func (c cmdable) JSONArrAppend(ctx context.Context, key, path string, values ...interface{}) *IntSliceCmd {
	args := []interface{}{"json.arrappend", key, path}
	args = append(args, values...)
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONArrIndex searches for the first occurrence of a JSON value in an array
func (c cmdable) JSONArrIndex(ctx context.Context, key, path string, value interface{}) *IntSliceCmd {
	args := []interface{}{"json.arrindex", key, path, value}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONArrIndexFromTo searches for the first occurrence of a JSON value in an array whilst allowing the start and
// stop options to be provided.
func (c cmdable) JSONArrIndexStartStop(ctx context.Context, key, path string, value interface{}, start, stop int64) *IntSliceCmd {
	args := []interface{}{"json.arrindex", key, path, value, start, stop}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONArrInsert inserts the json values into the array at path before the index (shifts to the right)
func (c cmdable) JSONArrInsert(ctx context.Context, key, path string, index int64, values ...interface{}) *IntSliceCmd {
	args := []interface{}{"json.arrinsert", key, path, index}
	args = append(args, values...)
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONArrLen reports the length of the JSON array at path in key
func (c cmdable) JSONArrLen(ctx context.Context, key, path string) *IntSliceCmd {
	args := []interface{}{"json.arrlen", key, path}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONArrPop removes and returns an element from the index in the array
func (c cmdable) JSONArrPop(ctx context.Context, key, path string, index int) *StringSliceCmd {
	args := []interface{}{"json.arrpop", key, path, index}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONArrTrim trims an array so that it contains only the specified inclusive range of elements
func (c cmdable) JSONArrTrim(ctx context.Context, key, path string, start, stop int) *IntSliceCmd {
	args := []interface{}{"json.arrtrim", key, path, start, stop}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONClear clears container values (arrays/objects) and set numeric values to 0
func (c cmdable) JSONClear(ctx context.Context, key, path string) *IntCmd {
	args := []interface{}{"json.clear", key, path}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONDel deletes a value
func (c cmdable) JSONDel(ctx context.Context, key, path string) *IntCmd {
	args := []interface{}{"json.del", key, path}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONForget deletes a value
func (c cmdable) JSONForget(ctx context.Context, key, path string) *IntCmd {
	args := []interface{}{"json.forget", key, path}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONGet returns the value at path in JSON serialized form
func (c cmdable) JSONGet(ctx context.Context, key string, paths ...string) *JSONStringCmd {

	args := make([]interface{}, len(paths)+2)
	args[0] = "json.get"
	args[1] = key
	for n, path := range paths {
		args[n+2] = path
	}

	cmd := NewJSONStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONMGet returns the values at path from multiple key arguments
// Note - the arguments are reversed when compared with `JSON.MGET` as we want
// to follow the pattern of having the last argument be variable.
func (c cmdable) JSONMGet(ctx context.Context, path string, keys ...string) *JSONSliceCmd {

	args := make([]interface{}, len(keys)+1)
	args[0] = "json.mget"
	for n, keys := range keys {
		args[n+1] = keys
	}
	args = append(args, path)

	cmd := NewJSONSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONNumIncrBy increments the number value stored at path by number
func (c cmdable) JSONNumIncrBy(ctx context.Context, key, path string, value float64) *JSONStringCmd {
	args := []interface{}{"json.numincrby", key, path, value}
	cmd := NewJSONStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONObjKeys returns the keys in the object that's referenced by path
func (c cmdable) JSONObjKeys(ctx context.Context, key, path string) *SliceCmd {
	args := []interface{}{"json.objkeys", key, path}
	cmd := NewSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONObjLen reports the number of keys in the JSON object at path in key
func (c cmdable) JSONObjLen(ctx context.Context, key, path string) *IntPointerSliceCmd {
	args := []interface{}{"json.objlen", key, path}
	cmd := NewIntPointerSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONSet sets the JSON value at the given path in the given key. The value must be something that
// can be marshalled to JSON (using encoding/JSON) unless the argument is a string when we assume that
// it can be passed directly as JSON.
func (c cmdable) JSONSet(ctx context.Context, key, path string, value interface{}) *StatusCmd {
	return c.JSONSetMode(ctx, key, path, value, "")
}

// JSONSetMOde sets the JSON value at the given path in the given key allows the mode to be set
// as well (the mode value must be "XX" or "NX").  The value must be something that can be marshalled to JSON (using encoding/JSON) unless
// the argument is a string when we assume that  it can be passed directly as JSON.
func (c cmdable) JSONSetMode(ctx context.Context, key, path string, value interface{}, mode string) *StatusCmd {

	var bytes []byte
	var err error

	switch v := value.(type) {
	case string:
		bytes = []byte(v)
	default:
		bytes, err = json.Marshal(v)
	}

	args := []interface{}{"json.set", key, path, bytes}

	if mode != "" {
		args = append(args, mode)
	}

	cmd := NewStatusCmd(ctx, args...)

	if err != nil {
		cmd.SetErr(err)
	} else {
		_ = c(ctx, cmd)
	}

	return cmd
}

// JSONStrAppend appends the json-string values to the string at path
func (c cmdable) JSONStrAppend(ctx context.Context, key, path, value string) *IntPointerSliceCmd {
	args := []interface{}{"json.strappend", key, path, value}
	cmd := NewIntPointerSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONStrLen reports the length of the JSON String at path in key
func (c cmdable) JSONStrLen(ctx context.Context, key, path string) *IntPointerSliceCmd {
	args := []interface{}{"json.strlen", key, path}
	cmd := NewIntPointerSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONToggle toggles a Boolean value stored at path
func (c cmdable) JSONToggle(ctx context.Context, key, path string) *IntPointerSliceCmd {
	args := []interface{}{"json.toggle", key, path}
	cmd := NewIntPointerSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// JSONType reports the type of JSON value at path
func (c cmdable) JSONType(ctx context.Context, key, path string) *StringSliceCmd {
	args := []interface{}{"json.type", key, path}
	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}
