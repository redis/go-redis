package redis

import (
	"context"
	"fmt"
	"net"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8/internal"
	"github.com/go-redis/redis/v8/internal/hscan"
	"github.com/go-redis/redis/v8/internal/proto"
	"github.com/go-redis/redis/v8/internal/util"
)

type Cmder interface {
	Name() string
	FullName() string
	Args() []interface{}
	String() string
	stringArg(int) string
	firstKeyPos() int8
	setFirstKeyPos(int8)

	readTimeout() *time.Duration
	readReply(pv *proto.Value) error

	SetErr(error)
	Err() error
}

func setCmdsErr(cmds []Cmder, e error) {
	for _, cmd := range cmds {
		if cmd.Err() == nil {
			cmd.SetErr(e)
		}
	}
}

func cmdsFirstErr(cmds []Cmder) error {
	for _, cmd := range cmds {
		if err := cmd.Err(); err != nil {
			return err
		}
	}
	return nil
}

func writeCmds(wr *proto.Writer, cmds []Cmder) error {
	for _, cmd := range cmds {
		if err := writeCmd(wr, cmd); err != nil {
			return err
		}
	}
	return nil
}

func writeCmd(wr *proto.Writer, cmd Cmder) error {
	return wr.WriteArgs(cmd.Args())
}

func cmdFirstKeyPos(cmd Cmder, info *CommandInfo) int {
	if pos := cmd.firstKeyPos(); pos != 0 {
		return int(pos)
	}

	switch cmd.Name() {
	case "eval", "evalsha":
		if cmd.stringArg(2) != "0" {
			return 3
		}

		return 0
	case "publish":
		return 1
	case "memory":
		// https://github.com/redis/redis/issues/7493
		if cmd.stringArg(1) == "usage" {
			return 2
		}
	}

	if info != nil {
		return int(info.FirstKeyPos)
	}
	return 0
}

func cmdString(cmd Cmder, val interface{}) string {
	b := make([]byte, 0, 64)

	for i, arg := range cmd.Args() {
		if i > 0 {
			b = append(b, ' ')
		}
		b = internal.AppendArg(b, arg)
	}

	if err := cmd.Err(); err != nil {
		b = append(b, ": "...)
		b = append(b, err.Error()...)
	} else if val != nil {
		b = append(b, ": "...)
		b = internal.AppendArg(b, val)
	}

	return internal.String(b)
}

//------------------------------------------------------------------------------

type baseCmd struct {
	ctx    context.Context
	args   []interface{}
	err    error
	keyPos int8

	_readTimeout *time.Duration
}

var _ Cmder = (*Cmd)(nil)

func (cmd *baseCmd) Name() string {
	if len(cmd.args) == 0 {
		return ""
	}
	// Cmd name must be lower cased.
	return internal.ToLower(cmd.stringArg(0))
}

func (cmd *baseCmd) FullName() string {
	switch name := cmd.Name(); name {
	case "cluster", "command":
		if len(cmd.args) == 1 {
			return name
		}
		if s2, ok := cmd.args[1].(string); ok {
			return name + " " + s2
		}
		return name
	default:
		return name
	}
}

func (cmd *baseCmd) Args() []interface{} {
	return cmd.args
}

func (cmd *baseCmd) stringArg(pos int) string {
	if pos < 0 || pos >= len(cmd.args) {
		return ""
	}
	s, _ := cmd.args[pos].(string)
	return s
}

func (cmd *baseCmd) firstKeyPos() int8 {
	return cmd.keyPos
}

func (cmd *baseCmd) setFirstKeyPos(keyPos int8) {
	cmd.keyPos = keyPos
}

func (cmd *baseCmd) SetErr(e error) {
	cmd.err = e
}

func (cmd *baseCmd) Err() error {
	return cmd.err
}

func (cmd *baseCmd) readTimeout() *time.Duration {
	return cmd._readTimeout
}

func (cmd *baseCmd) setReadTimeout(d time.Duration) {
	cmd._readTimeout = &d
}

//------------------------------------------------------------------------------

type Cmd struct {
	baseCmd

	val *proto.Value
}

func NewCmd(ctx context.Context, args ...interface{}) *Cmd {
	return &Cmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *Cmd) String() string {
	return cmdString(cmd, cmd.val.Interface())
}

func (cmd *Cmd) Val() interface{} {
	if cmd.val != nil {
		return cmd.val.Interface()
	}
	return nil
}

func (cmd *Cmd) Result() (interface{}, error) {
	if cmd.err != nil {
		return nil, cmd.err
	}
	return cmd.val.Interface(), cmd.err
}

func (cmd *Cmd) Text() (string, error) {
	if cmd.err != nil {
		return "", cmd.err
	}
	return cmd.val.String()
}

func (cmd *Cmd) Int() (int, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	i, err := cmd.val.Int64()
	if err != nil {
		return 0, err
	}
	return int(i), nil
}

func (cmd *Cmd) Int64() (int64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return cmd.val.Int64()
}

func (cmd *Cmd) Uint64() (uint64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	i, err := cmd.val.Int64()
	if err != nil {
		return 0, err
	}
	return uint64(i), nil
}

func (cmd *Cmd) Float32() (float32, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	f, err := cmd.val.Float64()
	if err != nil {
		return 0, err
	}
	return float32(f), nil
}

func (cmd *Cmd) Float64() (float64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return cmd.val.Float64()
}

func (cmd *Cmd) Bool() (bool, error) {
	if cmd.err != nil {
		return false, cmd.err
	}
	return cmd.val.Bool()
}

func (cmd *Cmd) readReply(pv *proto.Value) error {
	if pv.RedisError == nil {
		cmd.val = pv
	}
	return pv.RedisError
}

//------------------------------------------------------------------------------

type SliceCmd struct {
	baseCmd

	val []interface{}
}

var _ Cmder = (*SliceCmd)(nil)

func NewSliceCmd(ctx context.Context, args ...interface{}) *SliceCmd {
	return &SliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *SliceCmd) Val() []interface{} {
	return cmd.val
}

func (cmd *SliceCmd) Result() ([]interface{}, error) {
	return cmd.val, cmd.err
}

func (cmd *SliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

// Scan scans the results from the map into a destination struct. The map keys
// are matched in the Redis struct fields by the `redis:"field"` tag.
func (cmd *SliceCmd) Scan(dst interface{}) error {
	if cmd.err != nil {
		return cmd.err
	}

	// Pass the list of keys and values.
	// Skip the first two args for: HMGET key
	var args []interface{}
	if cmd.args[0] == "hmget" {
		args = cmd.args[2:]
	} else {
		// Otherwise, it's: MGET field field ...
		args = cmd.args[1:]
	}

	return hscan.Scan(dst, args, cmd.val)
}

func (cmd *SliceCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.SliceInterface()
	return err
}

//------------------------------------------------------------------------------

type StatusCmd struct {
	baseCmd

	val string
}

var _ Cmder = (*StatusCmd)(nil)

func NewStatusCmd(ctx context.Context, args ...interface{}) *StatusCmd {
	return &StatusCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *StatusCmd) Val() string {
	return cmd.val
}

func (cmd *StatusCmd) Result() (string, error) {
	return cmd.val, cmd.err
}

func (cmd *StatusCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *StatusCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.String()
	return err
}

//------------------------------------------------------------------------------

type IntCmd struct {
	baseCmd

	val int64
}

var _ Cmder = (*IntCmd)(nil)

func NewIntCmd(ctx context.Context, args ...interface{}) *IntCmd {
	return &IntCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *IntCmd) Val() int64 {
	return cmd.val
}

func (cmd *IntCmd) Result() (int64, error) {
	return cmd.val, cmd.err
}

func (cmd *IntCmd) Uint64() (uint64, error) {
	return uint64(cmd.val), cmd.err
}

func (cmd *IntCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *IntCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.Int64()
	return err
}

//------------------------------------------------------------------------------

type IntSliceCmd struct {
	baseCmd

	val []int64
}

var _ Cmder = (*IntSliceCmd)(nil)

func NewIntSliceCmd(ctx context.Context, args ...interface{}) *IntSliceCmd {
	return &IntSliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *IntSliceCmd) Val() []int64 {
	return cmd.val
}

func (cmd *IntSliceCmd) Result() ([]int64, error) {
	return cmd.val, cmd.err
}

func (cmd *IntSliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *IntSliceCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.SliceInt64()
	return err
}

//------------------------------------------------------------------------------

type DurationCmd struct {
	baseCmd

	val       time.Duration
	precision time.Duration
}

var _ Cmder = (*DurationCmd)(nil)

func NewDurationCmd(ctx context.Context, precision time.Duration, args ...interface{}) *DurationCmd {
	return &DurationCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
		precision: precision,
	}
}

func (cmd *DurationCmd) Val() time.Duration {
	return cmd.val
}

func (cmd *DurationCmd) Result() (time.Duration, error) {
	return cmd.val, cmd.err
}

func (cmd *DurationCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *DurationCmd) readReply(pv *proto.Value) error {
	n, err := pv.Int64()
	if err != nil {
		return err
	}
	switch n {
	// -2 if the key does not exist
	// -1 if the key exists but has no associated expire
	case -2, -1:
		cmd.val = time.Duration(n)
	default:
		cmd.val = time.Duration(n) * cmd.precision
	}
	return nil
}

//------------------------------------------------------------------------------

type TimeCmd struct {
	baseCmd

	val time.Time
}

var _ Cmder = (*TimeCmd)(nil)

func NewTimeCmd(ctx context.Context, args ...interface{}) *TimeCmd {
	return &TimeCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *TimeCmd) Val() time.Time {
	return cmd.val
}

func (cmd *TimeCmd) Result() (time.Time, error) {
	return cmd.val, cmd.err
}

func (cmd *TimeCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *TimeCmd) readReply(pv *proto.Value) error {
	times, err := pv.SliceInt64()
	if err != nil {
		return err
	}
	if len(times) != 2 {
		return fmt.Errorf("got %d elements, expected 2", len(times))
	}
	cmd.val = time.Unix(times[0], times[1]*1000)
	return nil
}

//------------------------------------------------------------------------------

type BoolCmd struct {
	baseCmd

	val bool
}

var _ Cmder = (*BoolCmd)(nil)

func NewBoolCmd(ctx context.Context, args ...interface{}) *BoolCmd {
	return &BoolCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *BoolCmd) Val() bool {
	return cmd.val
}

func (cmd *BoolCmd) Result() (bool, error) {
	return cmd.val, cmd.err
}

func (cmd *BoolCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *BoolCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.Status()
	return err
}

//------------------------------------------------------------------------------

type StringCmd struct {
	baseCmd

	val string
}

var _ Cmder = (*StringCmd)(nil)

func NewStringCmd(ctx context.Context, args ...interface{}) *StringCmd {
	return &StringCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *StringCmd) Val() string {
	return cmd.val
}

func (cmd *StringCmd) Result() (string, error) {
	return cmd.Val(), cmd.err
}

func (cmd *StringCmd) Bytes() ([]byte, error) {
	return util.StringToBytes(cmd.val), cmd.err
}

func (cmd *StringCmd) Bool() (bool, error) {
	if cmd.err != nil {
		return false, cmd.err
	}
	return strconv.ParseBool(cmd.val)
}

func (cmd *StringCmd) Int() (int, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.Atoi(cmd.Val())
}

func (cmd *StringCmd) Int64() (int64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.ParseInt(cmd.Val(), 10, 64)
}

func (cmd *StringCmd) Uint64() (uint64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.ParseUint(cmd.Val(), 10, 64)
}

func (cmd *StringCmd) Float32() (float32, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	f, err := strconv.ParseFloat(cmd.Val(), 32)
	if err != nil {
		return 0, err
	}
	return float32(f), nil
}

func (cmd *StringCmd) Float64() (float64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.ParseFloat(cmd.Val(), 64)
}

func (cmd *StringCmd) Time() (time.Time, error) {
	if cmd.err != nil {
		return time.Time{}, cmd.err
	}
	return time.Parse(time.RFC3339Nano, cmd.Val())
}

func (cmd *StringCmd) Scan(val interface{}) error {
	if cmd.err != nil {
		return cmd.err
	}
	return proto.Scan([]byte(cmd.val), val)
}

func (cmd *StringCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *StringCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.String()
	return err
}

//------------------------------------------------------------------------------

type FloatCmd struct {
	baseCmd

	val float64
}

var _ Cmder = (*FloatCmd)(nil)

func NewFloatCmd(ctx context.Context, args ...interface{}) *FloatCmd {
	return &FloatCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *FloatCmd) Val() float64 {
	return cmd.val
}

func (cmd *FloatCmd) Result() (float64, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *FloatCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *FloatCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.Float64()
	return err
}

//------------------------------------------------------------------------------

type FloatSliceCmd struct {
	baseCmd

	val []float64
}

var _ Cmder = (*FloatSliceCmd)(nil)

func NewFloatSliceCmd(ctx context.Context, args ...interface{}) *FloatSliceCmd {
	return &FloatSliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *FloatSliceCmd) Val() []float64 {
	return cmd.val
}

func (cmd *FloatSliceCmd) Result() ([]float64, error) {
	return cmd.val, cmd.err
}

func (cmd *FloatSliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *FloatSliceCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.SliceFloat64()
	return err
}

//------------------------------------------------------------------------------

type StringSliceCmd struct {
	baseCmd

	val []string
}

var _ Cmder = (*StringSliceCmd)(nil)

func NewStringSliceCmd(ctx context.Context, args ...interface{}) *StringSliceCmd {
	return &StringSliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *StringSliceCmd) Val() []string {
	return cmd.val
}

func (cmd *StringSliceCmd) Result() ([]string, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *StringSliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *StringSliceCmd) ScanSlice(container interface{}) error {
	return proto.ScanSlice(cmd.Val(), container)
}

func (cmd *StringSliceCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.SliceString()
	return err
}

//------------------------------------------------------------------------------

type BoolSliceCmd struct {
	baseCmd

	val []bool
}

var _ Cmder = (*BoolSliceCmd)(nil)

func NewBoolSliceCmd(ctx context.Context, args ...interface{}) *BoolSliceCmd {
	return &BoolSliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *BoolSliceCmd) Val() []bool {
	return cmd.val
}

func (cmd *BoolSliceCmd) Result() ([]bool, error) {
	return cmd.val, cmd.err
}

func (cmd *BoolSliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *BoolSliceCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.SliceStatus()
	return err
}

//------------------------------------------------------------------------------

type StringStringMapCmd struct {
	baseCmd

	val map[string]string
}

var _ Cmder = (*StringStringMapCmd)(nil)

func NewStringStringMapCmd(ctx context.Context, args ...interface{}) *StringStringMapCmd {
	return &StringStringMapCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *StringStringMapCmd) Val() map[string]string {
	return cmd.val
}

func (cmd *StringStringMapCmd) Result() (map[string]string, error) {
	return cmd.val, cmd.err
}

func (cmd *StringStringMapCmd) String() string {
	return cmdString(cmd, cmd.val)
}

// Scan scans the results from the map into a destination struct. The map keys
// are matched in the Redis struct fields by the `redis:"field"` tag.
func (cmd *StringStringMapCmd) Scan(dst interface{}) error {
	if cmd.err != nil {
		return cmd.err
	}

	strct, err := hscan.Struct(dst)
	if err != nil {
		return err
	}

	for k, v := range cmd.val {
		if err := strct.Scan(k, v); err != nil {
			return err
		}
	}

	return nil
}

func (cmd *StringStringMapCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.MapStringString()
	return err
}

//------------------------------------------------------------------------------

type StringIntMapCmd struct {
	baseCmd

	val map[string]int64
}

var _ Cmder = (*StringIntMapCmd)(nil)

func NewStringIntMapCmd(ctx context.Context, args ...interface{}) *StringIntMapCmd {
	return &StringIntMapCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *StringIntMapCmd) Val() map[string]int64 {
	return cmd.val
}

func (cmd *StringIntMapCmd) Result() (map[string]int64, error) {
	return cmd.val, cmd.err
}

func (cmd *StringIntMapCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *StringIntMapCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = pv.MapStringInt64()
	return err
}

//------------------------------------------------------------------------------

type StringStructMapCmd struct {
	baseCmd

	val map[string]struct{}
}

var _ Cmder = (*StringStructMapCmd)(nil)

func NewStringStructMapCmd(ctx context.Context, args ...interface{}) *StringStructMapCmd {
	return &StringStructMapCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *StringStructMapCmd) Val() map[string]struct{} {
	return cmd.val
}

func (cmd *StringStructMapCmd) Result() (map[string]struct{}, error) {
	return cmd.val, cmd.err
}

func (cmd *StringStructMapCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *StringStructMapCmd) readReply(pv *proto.Value) (err error) {
	ss, err := pv.SliceString()
	if err != nil {
		return err
	}

	cmd.val = make(map[string]struct{}, len(ss))
	for _, val := range ss {
		cmd.val[val] = struct{}{}
	}
	return nil
}

//------------------------------------------------------------------------------

type XMessage struct {
	ID     string
	Values map[string]interface{}
}

type XMessageSliceCmd struct {
	baseCmd

	val []XMessage
}

var _ Cmder = (*XMessageSliceCmd)(nil)

func NewXMessageSliceCmd(ctx context.Context, args ...interface{}) *XMessageSliceCmd {
	return &XMessageSliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *XMessageSliceCmd) Val() []XMessage {
	return cmd.val
}

func (cmd *XMessageSliceCmd) Result() ([]XMessage, error) {
	return cmd.val, cmd.err
}

func (cmd *XMessageSliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *XMessageSliceCmd) readReply(pv *proto.Value) (err error) {
	cmd.val, err = readXMessages(pv)
	return err
}

func readXMessages(pv *proto.Value) ([]XMessage, error) {
	n, err := pv.SliceLen()
	if err != nil {
		return nil, err
	}

	if n == 0 {
		return nil, nil
	}

	xMsgs := make([]XMessage, 0, n)
	err = pv.SliceScan(func(item *proto.Value) error {
		xMsg, err := readXMessage(item)
		if err != nil {
			return err
		}
		xMsgs = append(xMsgs, xMsg)
		return nil
	})
	return xMsgs, err
}

func readXMessage(item *proto.Value) (XMessage, error) {
	n, err := item.SliceLen()
	if err != nil && err != Nil {
		return XMessage{}, err
	}
	if n != 2 {
		return XMessage{}, fmt.Errorf("got %d, wanted 2", n)
	}

	id, err := item.Slice[0].String()
	if err != nil && err != Nil {
		return XMessage{}, err
	}

	ss, err := item.Slice[1].SliceString()
	if err != nil && err != Nil {
		return XMessage{}, err
	}
	n = len(ss)

	var value map[string]interface{}
	if n > 0 {
		if n%2 != 0 {
			return XMessage{}, fmt.Errorf("got %d, wanted multiple of 2", n)
		}
		value = make(map[string]interface{}, n/2)
		for i := 0; i < n; i += 2 {
			value[ss[i]] = ss[i+1]
		}
	}

	return XMessage{
		ID:     id,
		Values: value,
	}, nil
}

//------------------------------------------------------------------------------

type XStream struct {
	Stream   string
	Messages []XMessage
}

type XStreamSliceCmd struct {
	baseCmd

	val []XStream
}

var _ Cmder = (*XStreamSliceCmd)(nil)

func NewXStreamSliceCmd(ctx context.Context, args ...interface{}) *XStreamSliceCmd {
	return &XStreamSliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *XStreamSliceCmd) Val() []XStream {
	return cmd.val
}

func (cmd *XStreamSliceCmd) Result() ([]XStream, error) {
	return cmd.val, cmd.err
}

func (cmd *XStreamSliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *XStreamSliceCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}

	if n == 0 {
		return nil
	}

	cmd.val = make([]XStream, 0, n)
	err = pv.SliceScan(func(item *proto.Value) error {
		n, err = item.SliceLen()
		if err != nil {
			return err
		}
		if n != 2 {
			return fmt.Errorf("got %d, wanted 2", n)
		}

		stream, err := item.Slice[0].String()
		if err != nil {
			return err
		}

		xMsgs, err := readXMessages(item.Slice[1])
		if err != nil {
			return err
		}
		cmd.val = append(cmd.val, XStream{
			Stream:   stream,
			Messages: xMsgs,
		})
		return nil
	})

	return err
}

//------------------------------------------------------------------------------

type XPending struct {
	Count     int64
	Lower     string
	Higher    string
	Consumers map[string]int64
}

type XPendingCmd struct {
	baseCmd
	val *XPending
}

var _ Cmder = (*XPendingCmd)(nil)

func NewXPendingCmd(ctx context.Context, args ...interface{}) *XPendingCmd {
	return &XPendingCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *XPendingCmd) Val() *XPending {
	return cmd.val
}

func (cmd *XPendingCmd) Result() (*XPending, error) {
	return cmd.val, cmd.err
}

func (cmd *XPendingCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *XPendingCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}
	if n != 4 {
		return fmt.Errorf("got %d, wanted 4", n)
	}

	count, err := pv.Slice[0].Int64()
	if err != nil {
		return err
	}

	lower, err := pv.Slice[1].String()
	if err != nil {
		return err
	}

	higher, err := pv.Slice[2].String()
	if err != nil {
		return err
	}
	cmd.val = &XPending{
		Count:  count,
		Lower:  lower,
		Higher: higher,
	}

	n, err = pv.Slice[3].SliceLen()
	if err == nil && n > 0 {
		cmd.val.Consumers = make(map[string]int64, n)
		err = pv.Slice[3].SliceScan(func(item *proto.Value) error {
			n, err = item.SliceLen()
			if err != nil {
				return err
			}
			if n != 2 {
				return fmt.Errorf("got %d, wanted 2", n)
			}
			name, err := item.Slice[0].String()
			if err != nil {
				return err
			}
			pending, err := item.Slice[1].Int64()
			if err != nil {
				return err
			}
			cmd.val.Consumers[name] = pending

			return nil
		})
		if err != nil {
			return err
		}
	}

	return nil
}

//------------------------------------------------------------------------------

type XPendingExt struct {
	ID         string
	Consumer   string
	Idle       time.Duration
	RetryCount int64
}

type XPendingExtCmd struct {
	baseCmd
	val []XPendingExt
}

var _ Cmder = (*XPendingExtCmd)(nil)

func NewXPendingExtCmd(ctx context.Context, args ...interface{}) *XPendingExtCmd {
	return &XPendingExtCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *XPendingExtCmd) Val() []XPendingExt {
	return cmd.val
}

func (cmd *XPendingExtCmd) Result() ([]XPendingExt, error) {
	return cmd.val, cmd.err
}

func (cmd *XPendingExtCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *XPendingExtCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}

	cmd.val = make([]XPendingExt, 0, n)
	err = pv.SliceScan(func(item *proto.Value) error {
		n, err = item.SliceLen()
		if err != nil {
			return err
		}
		if n != 4 {
			return fmt.Errorf("got %d, wanted 4", n)
		}

		id, err := item.Slice[0].String()
		if err != nil {
			return err
		}

		consumer, err := item.Slice[1].String()
		if err != nil && err != Nil {
			return err
		}

		idle, err := item.Slice[2].Int64()
		if err != nil && err != Nil {
			return err
		}

		retryCount, err := item.Slice[3].Int64()
		if err != nil && err != Nil {
			return err
		}

		cmd.val = append(cmd.val, XPendingExt{
			ID:         id,
			Consumer:   consumer,
			Idle:       time.Duration(idle) * time.Millisecond,
			RetryCount: retryCount,
		})

		return nil
	})

	return err
}

//------------------------------------------------------------------------------

type XInfoConsumersCmd struct {
	baseCmd
	val []XInfoConsumer
}

type XInfoConsumer struct {
	Name    string
	Pending int64
	Idle    int64
}

var _ Cmder = (*XInfoGroupsCmd)(nil)

func NewXInfoConsumersCmd(ctx context.Context, stream string, group string) *XInfoConsumersCmd {
	return &XInfoConsumersCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: []interface{}{"xinfo", "consumers", stream, group},
		},
	}
}

func (cmd *XInfoConsumersCmd) Val() []XInfoConsumer {
	return cmd.val
}

func (cmd *XInfoConsumersCmd) Result() ([]XInfoConsumer, error) {
	return cmd.val, cmd.err
}

func (cmd *XInfoConsumersCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *XInfoConsumersCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	cmd.val = make([]XInfoConsumer, 0, n)
	err = pv.SliceScan(func(item *proto.Value) error {
		n, err = item.SliceLen()
		if err != nil {
			return err
		}
		if n != 6 {
			return fmt.Errorf("redis: got %d elements in XINFO CONSUMERS reply, wanted 6", n)
		}

		var consumer XInfoConsumer
		for i := 0; i < n; i += 2 {
			key, err := item.Slice[i].String()
			if err != nil {
				return err
			}

			switch key {
			case "name":
				val, err := item.Slice[i+1].String()
				if err != nil {
					return err
				}
				consumer.Name = val
			case "pending":
				val, err := item.Slice[i+1].Int64()
				if err != nil {
					return err
				}
				consumer.Pending = val
			case "idle":
				val, err := item.Slice[i+1].Int64()
				if err != nil {
					return err
				}
				consumer.Idle = val
			default:
				return fmt.Errorf("redis: unexpected content %s in XINFO CONSUMERS reply", key)
			}
		}
		cmd.val = append(cmd.val, consumer)
		return nil
	})

	return err
}

//------------------------------------------------------------------------------

type XInfoGroupsCmd struct {
	baseCmd
	val []XInfoGroup
}

type XInfoGroup struct {
	Name            string
	Consumers       int64
	Pending         int64
	LastDeliveredID string
}

var _ Cmder = (*XInfoGroupsCmd)(nil)

func NewXInfoGroupsCmd(ctx context.Context, stream string) *XInfoGroupsCmd {
	return &XInfoGroupsCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: []interface{}{"xinfo", "groups", stream},
		},
	}
}

func (cmd *XInfoGroupsCmd) Val() []XInfoGroup {
	return cmd.val
}

func (cmd *XInfoGroupsCmd) Result() ([]XInfoGroup, error) {
	return cmd.val, cmd.err
}

func (cmd *XInfoGroupsCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *XInfoGroupsCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	cmd.val = make([]XInfoGroup, 0, n)
	err = pv.SliceScan(func(item *proto.Value) error {
		n, err = item.SliceLen()
		if err != nil {
			return err
		}
		if n != 8 {
			return fmt.Errorf("redis: got %d elements in XINFO GROUPS reply, wanted 8", n)
		}

		var group XInfoGroup
		for i := 0; i < n; i += 2 {
			key, err := item.Slice[i].String()
			if err != nil {
				return err
			}

			switch key {
			case "name":
				val, err := item.Slice[i+1].String()
				if err != nil {
					return err
				}
				group.Name = val
			case "consumers":
				val, err := item.Slice[i+1].Int64()
				if err != nil {
					return err
				}
				group.Consumers = val
			case "pending":
				val, err := item.Slice[i+1].Int64()
				if err != nil {
					return err
				}
				group.Pending = val
			case "last-delivered-id":
				val, err := item.Slice[i+1].String()
				if err != nil {
					return err
				}
				group.LastDeliveredID = val
			default:
				return fmt.Errorf("redis: unexpected content %s in XINFO GROUPS reply", key)
			}
		}
		cmd.val = append(cmd.val, group)
		return nil
	})

	return err
}

//------------------------------------------------------------------------------

type XInfoStreamCmd struct {
	baseCmd
	val *XInfoStream
}

type XInfoStream struct {
	Length          int64
	RadixTreeKeys   int64
	RadixTreeNodes  int64
	Groups          int64
	LastGeneratedID string
	FirstEntry      XMessage
	LastEntry       XMessage
}

var _ Cmder = (*XInfoStreamCmd)(nil)

func NewXInfoStreamCmd(ctx context.Context, stream string) *XInfoStreamCmd {
	return &XInfoStreamCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: []interface{}{"xinfo", "stream", stream},
		},
	}
}

func (cmd *XInfoStreamCmd) Val() *XInfoStream {
	return cmd.val
}

func (cmd *XInfoStreamCmd) Result() (*XInfoStream, error) {
	return cmd.val, cmd.err
}

func (cmd *XInfoStreamCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *XInfoStreamCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	// no full.
	if n != 14 {
		return fmt.Errorf("redis: got %d elements in XINFO STREAM reply, wanted 14", n)
	}

	info := &XInfoStream{}
	for i := 0; i < n; i += 2 {
		var key string
		key, err = pv.Slice[i].String()
		if err != nil {
			return err
		}

		switch key {
		case "length":
			info.Length, err = pv.Slice[i+1].Int64()
		case "radix-tree-keys":
			info.RadixTreeKeys, err = pv.Slice[i+1].Int64()
		case "radix-tree-nodes":
			info.RadixTreeNodes, err = pv.Slice[i+1].Int64()
		case "groups":
			info.Groups, err = pv.Slice[i+1].Int64()
		case "last-generated-id":
			info.LastGeneratedID, err = pv.Slice[i+1].String()
		case "first-entry":
			info.FirstEntry, err = readXMessage(pv.Slice[i+1])
		case "last-entry":
			info.LastEntry, err = readXMessage(pv.Slice[i+1])
		default:
			return fmt.Errorf("redis: unexpected content %s "+
				"in XINFO STREAM reply", key)
		}
		if err != nil {
			return err
		}
	}
	cmd.val = info
	return nil
}

//------------------------------------------------------------------------------

type ZSliceCmd struct {
	baseCmd

	val []Z
}

var _ Cmder = (*ZSliceCmd)(nil)

func NewZSliceCmd(ctx context.Context, args ...interface{}) *ZSliceCmd {
	return &ZSliceCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *ZSliceCmd) Val() []Z {
	return cmd.val
}

func (cmd *ZSliceCmd) Result() ([]Z, error) {
	return cmd.val, cmd.err
}

func (cmd *ZSliceCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *ZSliceCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}
	if n%2 != 0 {
		return fmt.Errorf("got %d, wanted multiple of 2", n)
	}

	cmd.val = make([]Z, 0, n/2)
	for i := 0; i < n; i += 2 {
		member, err := pv.Slice[i].String()
		if err != nil {
			return err
		}

		score, err := pv.Slice[i+1].Float64()
		if err != nil {
			return err
		}

		cmd.val = append(cmd.val, Z{
			Score:  score,
			Member: member,
		})
	}

	return nil
}

//------------------------------------------------------------------------------

type ZWithKeyCmd struct {
	baseCmd

	val *ZWithKey
}

var _ Cmder = (*ZWithKeyCmd)(nil)

func NewZWithKeyCmd(ctx context.Context, args ...interface{}) *ZWithKeyCmd {
	return &ZWithKeyCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *ZWithKeyCmd) Val() *ZWithKey {
	return cmd.val
}

func (cmd *ZWithKeyCmd) Result() (*ZWithKey, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *ZWithKeyCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *ZWithKeyCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if n != 3 {
		return fmt.Errorf("got %d elements, expected 3", n)
	}

	cmd.val = &ZWithKey{}

	cmd.val.Key, err = pv.Slice[0].String()
	if err != nil {
		return err
	}

	cmd.val.Member, err = pv.Slice[1].String()
	if err != nil {
		return err
	}

	cmd.val.Score, err = pv.Slice[2].Float64()
	if err != nil {
		return err
	}

	return nil
}

//------------------------------------------------------------------------------

type ScanCmd struct {
	baseCmd

	page   []string
	cursor uint64

	process cmdable
}

var _ Cmder = (*ScanCmd)(nil)

func NewScanCmd(ctx context.Context, process cmdable, args ...interface{}) *ScanCmd {
	return &ScanCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
		process: process,
	}
}

func (cmd *ScanCmd) Val() (keys []string, cursor uint64) {
	return cmd.page, cmd.cursor
}

func (cmd *ScanCmd) Result() (keys []string, cursor uint64, err error) {
	return cmd.page, cmd.cursor, cmd.err
}

func (cmd *ScanCmd) String() string {
	return cmdString(cmd, cmd.page)
}

func (cmd *ScanCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}
	if n != 2 {
		return fmt.Errorf("redis: got %d elements in scan reply, expected 2", n)
	}

	cursor, err := pv.Slice[0].Int64()
	if err != nil {
		return err
	}
	cmd.cursor = uint64(cursor)
	cmd.page, err = pv.Slice[1].SliceString()

	return err
}

// Iterator creates a new ScanIterator.
func (cmd *ScanCmd) Iterator() *ScanIterator {
	return &ScanIterator{
		cmd: cmd,
	}
}

//------------------------------------------------------------------------------

type ClusterNode struct {
	ID   string
	Addr string
}

type ClusterSlot struct {
	Start int
	End   int
	Nodes []ClusterNode
}

type ClusterSlotsCmd struct {
	baseCmd

	val []ClusterSlot
}

var _ Cmder = (*ClusterSlotsCmd)(nil)

func NewClusterSlotsCmd(ctx context.Context, args ...interface{}) *ClusterSlotsCmd {
	return &ClusterSlotsCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *ClusterSlotsCmd) Val() []ClusterSlot {
	return cmd.val
}

func (cmd *ClusterSlotsCmd) Result() ([]ClusterSlot, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *ClusterSlotsCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *ClusterSlotsCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	cmd.val = make([]ClusterSlot, 0, n)
	err = pv.SliceScan(func(item *proto.Value) error {
		n, err = item.SliceLen()
		if err != nil {
			return err
		}
		if n < 2 {
			return fmt.Errorf("redis: got %d elements in cluster info, expected at least 2", n)
		}

		start, err := item.Slice[0].Int64()
		if err != nil {
			return err
		}
		end, err := item.Slice[1].Int64()
		if err != nil {
			return err
		}

		var nodeVal *proto.Value
		nodes := make([]ClusterNode, n-2)
		for i := 0; i < (n - 2); i++ {
			nodeVal = item.Slice[i+2]
			nx, err := nodeVal.SliceLen()
			if err != nil {
				return err
			}
			if nx != 2 && nx != 3 {
				return fmt.Errorf("got %d elements in cluster info address, expected 2 or 3", n)
			}

			ip, err := nodeVal.Slice[0].String()
			if err != nil {
				return err
			}
			port, err := nodeVal.Slice[1].String()
			if err != nil {
				return err
			}

			nodes[i].Addr = net.JoinHostPort(ip, port)

			// new version.
			if nx == 3 {
				id, err := nodeVal.Slice[2].String()
				if err != nil {
					return err
				}
				nodes[i].ID = id
			}
		}

		cmd.val = append(cmd.val, ClusterSlot{
			Start: int(start),
			End:   int(end),
			Nodes: nodes,
		})
		return nil
	})
	return err
}

//------------------------------------------------------------------------------

// GeoLocation is used with GeoAdd to add geospatial location.
type GeoLocation struct {
	Name                      string
	Longitude, Latitude, Dist float64
	GeoHash                   int64
}

// GeoRadiusQuery is used with GeoRadius to query geospatial index.
type GeoRadiusQuery struct {
	Radius float64
	// Can be m, km, ft, or mi. Default is km.
	Unit        string
	WithCoord   bool
	WithDist    bool
	WithGeoHash bool
	Count       int
	// Can be ASC or DESC. Default is no sort order.
	Sort      string
	Store     string
	StoreDist string
}

type GeoLocationCmd struct {
	baseCmd

	q         *GeoRadiusQuery
	locations []GeoLocation
}

var _ Cmder = (*GeoLocationCmd)(nil)

func NewGeoLocationCmd(ctx context.Context, q *GeoRadiusQuery, args ...interface{}) *GeoLocationCmd {
	return &GeoLocationCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: geoLocationArgs(q, args...),
		},
		q: q,
	}
}

func geoLocationArgs(q *GeoRadiusQuery, args ...interface{}) []interface{} {
	args = append(args, q.Radius)
	if q.Unit != "" {
		args = append(args, q.Unit)
	} else {
		args = append(args, "km")
	}
	if q.WithCoord {
		args = append(args, "withcoord")
	}
	if q.WithDist {
		args = append(args, "withdist")
	}
	if q.WithGeoHash {
		args = append(args, "withhash")
	}
	if q.Count > 0 {
		args = append(args, "count", q.Count)
	}
	if q.Sort != "" {
		args = append(args, q.Sort)
	}
	if q.Store != "" {
		args = append(args, "store")
		args = append(args, q.Store)
	}
	if q.StoreDist != "" {
		args = append(args, "storedist")
		args = append(args, q.StoreDist)
	}
	return args
}

func (cmd *GeoLocationCmd) Val() []GeoLocation {
	return cmd.locations
}

func (cmd *GeoLocationCmd) Result() ([]GeoLocation, error) {
	return cmd.locations, cmd.err
}

func (cmd *GeoLocationCmd) String() string {
	return cmdString(cmd, cmd.locations)
}

func (cmd *GeoLocationCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	cmd.locations = make([]GeoLocation, 0, n)
	err = pv.SliceScan(func(item *proto.Value) error {
		if item.IsSimpleType() {
			name, err := item.String()
			if err != nil {
				return err
			}
			cmd.locations = append(cmd.locations, GeoLocation{
				Name: name,
			})
		} else {
			n, err = item.SliceLen()
			if err != nil {
				return err
			}
			expectNum := 1
			if cmd.q.WithDist {
				expectNum++
			}
			if cmd.q.WithGeoHash {
				expectNum++
			}
			if cmd.q.WithCoord {
				expectNum++
			}

			if n != expectNum {
				return fmt.Errorf("got %d, wanted %d", n, expectNum)
			}

			var loc GeoLocation
			var idx int

			loc.Name, err = item.Slice[idx].String()
			if err != nil {
				return err
			}
			idx++

			if cmd.q.WithDist {
				loc.Dist, err = item.Slice[idx].Float64()
				if err != nil {
					return err
				}
				idx++
			}

			if cmd.q.WithGeoHash {
				loc.GeoHash, err = item.Slice[idx].Int64()
				if err != nil {
					return err
				}
				idx++
			}

			if cmd.q.WithCoord {
				cd, err := item.Slice[idx].SliceFloat64()
				if err != nil {
					return err
				}
				if len(cd) != 2 {
					return fmt.Errorf("got %d coordinates, expected 2", len(cd))
				}

				loc.Longitude = cd[0]
				loc.Latitude = cd[1]
			}
			cmd.locations = append(cmd.locations, loc)
		}
		return nil
	})

	return err
}

//------------------------------------------------------------------------------

type GeoPos struct {
	Longitude, Latitude float64
}

type GeoPosCmd struct {
	baseCmd

	val []*GeoPos
}

var _ Cmder = (*GeoPosCmd)(nil)

func NewGeoPosCmd(ctx context.Context, args ...interface{}) *GeoPosCmd {
	return &GeoPosCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *GeoPosCmd) Val() []*GeoPos {
	return cmd.val
}

func (cmd *GeoPosCmd) Result() ([]*GeoPos, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *GeoPosCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *GeoPosCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}
	if n == 0 {
		return nil
	}

	cmd.val = make([]*GeoPos, 0, n)
	err = pv.SliceScan(func(item *proto.Value) error {
		var sf []float64
		sf, err = item.SliceFloat64()
		switch err {
		case Nil:
			cmd.val = append(cmd.val, nil)
		case nil:
			if len(sf) != 2 {
				return fmt.Errorf("got %d, wanted 2", len(sf))
			}
			cmd.val = append(cmd.val, &GeoPos{
				Longitude: sf[0],
				Latitude:  sf[1],
			})
		default:
			return err
		}
		return nil
	})

	return err
}

//------------------------------------------------------------------------------

type CommandInfo struct {
	Name        string
	Arity       int8
	Flags       []string
	ACLFlags    []string
	FirstKeyPos int8
	LastKeyPos  int8
	StepCount   int8
	ReadOnly    bool
}

type CommandsInfoCmd struct {
	baseCmd

	val map[string]*CommandInfo
}

var _ Cmder = (*CommandsInfoCmd)(nil)

func NewCommandsInfoCmd(ctx context.Context, args ...interface{}) *CommandsInfoCmd {
	return &CommandsInfoCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *CommandsInfoCmd) Val() map[string]*CommandInfo {
	return cmd.val
}

func (cmd *CommandsInfoCmd) Result() (map[string]*CommandInfo, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *CommandsInfoCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *CommandsInfoCmd) readReply(pv *proto.Value) error {
	const numArgRedis5 = 6
	const numArgRedis6 = 7

	n, err := pv.SliceLen()
	if err != nil {
		return err
	}

	cmd.val = make(map[string]*CommandInfo, n)
	err = pv.SliceScan(func(item *proto.Value) error {
		n, err = item.SliceLen()
		if err != nil {
			return err
		}
		if n != numArgRedis5 && n != numArgRedis6 {
			return fmt.Errorf("redis: got %d elements in COMMAND reply, wanted 7", n)
		}

		info := &CommandInfo{}

		info.Name, err = item.Slice[0].String()
		if err != nil {
			return err
		}

		var arity, firstKeyPos, lastKeyPos, stepCount int64

		arity, err = item.Slice[1].Int64()
		if err != nil {
			return err
		}
		info.Arity = int8(arity)

		info.Flags, err = item.Slice[2].SliceString()
		if err != nil {
			return err
		}

		firstKeyPos, err = item.Slice[3].Int64()
		if err != nil {
			return err
		}
		info.FirstKeyPos = int8(firstKeyPos)

		lastKeyPos, err = item.Slice[4].Int64()
		if err != nil {
			return err
		}
		info.LastKeyPos = int8(lastKeyPos)

		stepCount, err = item.Slice[5].Int64()
		if err != nil {
			return err
		}
		info.StepCount = int8(stepCount)

		for _, flag := range info.Flags {
			if flag == "readonly" {
				info.ReadOnly = true
				break
			}
		}

		if n >= numArgRedis6 {
			info.ACLFlags, err = item.Slice[6].SliceString()
			if err != nil {
				return err
			}
		}

		cmd.val[info.Name] = info
		return nil
	})

	return err
}

//------------------------------------------------------------------------------

type cmdsInfoCache struct {
	fn func(ctx context.Context) (map[string]*CommandInfo, error)

	once internal.Once
	cmds map[string]*CommandInfo
}

func newCmdsInfoCache(fn func(ctx context.Context) (map[string]*CommandInfo, error)) *cmdsInfoCache {
	return &cmdsInfoCache{
		fn: fn,
	}
}

func (c *cmdsInfoCache) Get(ctx context.Context) (map[string]*CommandInfo, error) {
	err := c.once.Do(func() error {
		cmds, err := c.fn(ctx)
		if err != nil {
			return err
		}

		// Extensions have cmd names in upper case. Convert them to lower case.
		for k, v := range cmds {
			lower := internal.ToLower(k)
			if lower != k {
				cmds[lower] = v
			}
		}

		c.cmds = cmds
		return nil
	})
	return c.cmds, err
}

//------------------------------------------------------------------------------

type SlowLog struct {
	ID       int64
	Time     time.Time
	Duration time.Duration
	Args     []string
	// These are also optional fields emitted only by Redis 4.0 or greater:
	// https://redis.io/commands/slowlog#output-format
	ClientAddr string
	ClientName string
}

type SlowLogCmd struct {
	baseCmd

	val []SlowLog
}

var _ Cmder = (*SlowLogCmd)(nil)

func NewSlowLogCmd(ctx context.Context, args ...interface{}) *SlowLogCmd {
	return &SlowLogCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *SlowLogCmd) Val() []SlowLog {
	return cmd.val
}

func (cmd *SlowLogCmd) Result() ([]SlowLog, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *SlowLogCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *SlowLogCmd) readReply(pv *proto.Value) error {
	n, err := pv.SliceLen()
	if err != nil {
		return err
	}

	cmd.val = make([]SlowLog, 0, n)
	err = pv.SliceScan(func(item *proto.Value) error {
		n, err = item.SliceLen()
		if err != nil {
			if err == Nil {
				return nil
			}
			return err
		}
		if n < 4 {
			return fmt.Errorf("redis: got %d elements in slowlog get, expected at least 4", n)
		}

		id, err := item.Slice[0].Int64()
		if err != nil && err != Nil {
			return err
		}

		createAt, err := item.Slice[1].Int64()
		if err != nil && err != Nil {
			return err
		}
		createdAtTime := time.Unix(createAt, 0)

		costs, err := item.Slice[2].Int64()
		if err != nil && err != Nil {
			return err
		}
		costsDuration := time.Duration(costs) * time.Microsecond

		cmdString, err := item.Slice[3].SliceString()
		if err != nil {
			return err
		}
		if len(cmdString) < 1 {
			return fmt.Errorf("redis: got %d elements commands reply in slowlog get, expected at least 1", len(cmdString))
		}

		var address, name string
		if n > 4 {
			address, err = item.Slice[4].String()
			if err != nil && err != Nil {
				return err
			}
		}
		if n > 5 {
			name, err = item.Slice[5].String()
			if err != nil && err != Nil {
				return err
			}
		}

		cmd.val = append(cmd.val, SlowLog{
			ID:         id,
			Time:       createdAtTime,
			Duration:   costsDuration,
			Args:       cmdString,
			ClientAddr: address,
			ClientName: name,
		})
		return nil
	})

	return err
}
