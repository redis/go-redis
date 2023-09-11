package redis

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/internal/util"
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
	JSONGet(ctx context.Context, key string, paths ...string) *JSONCmd
	JSONMGet(ctx context.Context, path string, keys ...string) *JSONSliceCmd
	JSONNumIncrBy(ctx context.Context, key, path string, value float64) *JSONCmd
	JSONObjKeys(ctx context.Context, key, path string) *SliceCmd
	JSONObjLen(ctx context.Context, key, path string) *IntPointerSliceCmd
	JSONSet(ctx context.Context, key, path string, value interface{}) *StatusCmd
	JSONSetMode(ctx context.Context, key, path string, value interface{}, mode string) *StatusCmd
	JSONStrAppend(ctx context.Context, key, path, value string) *IntPointerSliceCmd
	JSONStrLen(ctx context.Context, key, path string) *IntPointerSliceCmd
	JSONToggle(ctx context.Context, key, path string) *IntPointerSliceCmd
	JSONType(ctx context.Context, key, path string) *JSONSliceCmd
}

type JSONCmd struct {
	baseCmd
	val      string
	expanded []interface{}
}

var _ Cmder = (*JSONCmd)(nil)

func NewJSONCmd(ctx context.Context, args ...interface{}) *JSONCmd {

	return &JSONCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *JSONCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *JSONCmd) SetVal(val string) {
	cmd.val = val
}

func (cmd *JSONCmd) Val() string {
	if len(cmd.val) == 0 && cmd.expanded != nil {
		val, err := json.Marshal(cmd.expanded)
		if err != nil {
			cmd.SetErr(err)
			return ""
		}
		return string(val)

	} else {
		return cmd.val
	}

}

func (cmd *JSONCmd) Result() (string, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd JSONCmd) Expanded() (interface{}, error) {

	if len(cmd.val) != 0 && cmd.expanded == nil {
		err := json.Unmarshal([]byte(cmd.val), &cmd.expanded)
		if err != nil {
			return "", err
		}
	}

	return cmd.expanded, nil
}

func (cmd *JSONCmd) Scan(index int, dst interface{}) error {
	if cmd.Err() != nil {
		return cmd.Err()
	}

	if cmd.val != "" && cmd.expanded == nil {
		err := json.Unmarshal([]byte(cmd.val), &cmd.expanded)
		if err != nil {
			return err
		}
	}

	if index < 0 || index >= len(cmd.val) {
		return fmt.Errorf("JSONCmd.Scan - %d is out of range (0..%d)", index, len(cmd.val))
	}

	results := []json.RawMessage{}
	if err := json.Unmarshal([]byte(cmd.val), &results); err != nil {
		return err
	} else {
		return json.Unmarshal(results[index], dst)
	}
}

func (cmd *JSONCmd) readReply(rd *proto.Reader) error {

	// nil response from JSON.(M)GET (cmd.baseCmd.err will be "redis: nil")
	if cmd.baseCmd.Err() == Nil {
		cmd.val = ""
		return Nil
	}

	if readType, err := rd.PeekReplyType(); err != nil {
		return err
	} else if readType == proto.RespArray {

		size, err := rd.ReadArrayLen()
		if err != nil {
			return err
		}

		var expanded = make([]interface{}, size)

		for i := 0; i < size; i++ {
			if expanded[i], err = rd.ReadReply(); err != nil {
				return err
			}
		}
		cmd.expanded = expanded

	} else {
		if str, err := rd.ReadString(); err != nil && err != Nil {
			return err
		} else if str == "" || err == Nil {
			cmd.val = ""
		} else {
			cmd.val = str
		}
	}

	return nil
}

// -------------------------------------------

type JSONSliceCmd struct {
	baseCmd
	val []interface{}
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

func (cmd *JSONSliceCmd) SetVal(val []interface{}) {
	cmd.val = val
}

func (cmd *JSONSliceCmd) Val() []interface{} {
	return cmd.val
}

func (cmd *JSONSliceCmd) Result() ([]interface{}, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *JSONSliceCmd) readReply(rd *proto.Reader) error {

	if cmd.baseCmd.Err() == Nil {
		cmd.val = nil
		return Nil
	}

	if readType, err := rd.PeekReplyType(); err != nil {
		return err
	} else if readType == proto.RespArray {
		response, err := rd.ReadReply()
		if err != nil {
			return nil
		} else {
			cmd.val = response.([]interface{})
		}

	} else {
		n, err := rd.ReadArrayLen()
		if err != nil {
			return err
		}
		cmd.val = make([]interface{}, n)
		for i := 0; i < len(cmd.val); i++ {
			switch s, err := rd.ReadString(); {
			case err == Nil:
				cmd.val[i] = ""
			case err != nil:
				return err
			default:
				cmd.val[i] = s
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

// JSONGet returns the value at path in JSON serialized form. JSON.GET returns an
// array of strings. This function parses out the wrapping array but leaves the
// internal strings unprocessed by default (see Val())
func (c cmdable) JSONGet(ctx context.Context, key string, paths ...string) *JSONCmd {
	args := make([]interface{}, len(paths)+2)
	args[0] = "json.get"
	args[1] = key
	for n, path := range paths {
		args[n+2] = path
	}
	cmd := NewJSONCmd(ctx, args...)
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
func (c cmdable) JSONNumIncrBy(ctx context.Context, key, path string, value float64) *JSONCmd {
	args := []interface{}{"json.numincrby", key, path, value}
	cmd := NewJSONCmd(ctx, args...)
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
// can be marshalled to JSON (using encoding/JSON) unless the argument is a string or a []byte when we assume that
// it can be passed directly as JSON.
func (c cmdable) JSONSet(ctx context.Context, key, path string, value interface{}) *StatusCmd {
	return c.JSONSetMode(ctx, key, path, value, "")
}

// JSONSetMOde sets the JSON value at the given path in the given key allows the mode to be set
// as well (the mode value must be "XX" or "NX").  The value must be something that can be marshalled to JSON (using encoding/JSON) unless
// the argument is a string or []byte when we assume that  it can be passed directly as JSON.
func (c cmdable) JSONSetMode(ctx context.Context, key, path string, value interface{}, mode string) *StatusCmd {

	var bytes []byte
	var err error

	switch v := value.(type) {
	case string:
		bytes = []byte(v)
	case []byte:
		bytes = v
	default:
		bytes, err = json.Marshal(v)
	}

	args := []interface{}{"json.set", key, path, util.BytesToString(bytes)}

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
func (c cmdable) JSONType(ctx context.Context, key, path string) *JSONSliceCmd {
	args := []interface{}{"json.type", key, path}
	cmd := NewJSONSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}
