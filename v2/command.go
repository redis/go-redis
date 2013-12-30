package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

type Cmder interface {
	args() []string
	parseReply(reader) (interface{}, error)
	setErr(error)
	setVal(interface{})

	writeTimeout() *time.Duration
	readTimeout() *time.Duration

	Err() error
}

func setCmdsErr(cmds []Cmder, e error) {
	for _, cmd := range cmds {
		cmd.setErr(e)
	}
}

//------------------------------------------------------------------------------

type baseCmd struct {
	_args []string

	val interface{}
	err error

	_writeTimeout, _readTimeout *time.Duration
}

func newBaseCmd(args ...string) *baseCmd {
	return &baseCmd{
		_args: args,
	}
}

func (cmd *baseCmd) String() string {
	args := strings.Join(cmd._args, " ")
	if cmd.err != nil {
		return args + ": " + cmd.err.Error()
	} else if cmd.val != nil {
		return args + ": " + fmt.Sprint(cmd.val)
	}
	return args
}

func (cmd *baseCmd) Err() error {
	if cmd.err != nil {
		return cmd.err
	}
	if cmd.val == nil {
		return errValNotSet
	}
	return nil
}

func (cmd *baseCmd) args() []string {
	return cmd._args
}

func (cmd *baseCmd) setErr(err error) {
	if err == nil {
		panic("non-nil value expected")
	}
	cmd.err = err
}

func (cmd *baseCmd) setVal(val interface{}) {
	if val == nil {
		panic("non-nil value expected")
	}
	cmd.val = val
}

func (cmd *baseCmd) parseReply(rd reader) (interface{}, error) {
	return parseReply(rd)
}

func (cmd *baseCmd) readTimeout() *time.Duration {
	return cmd._readTimeout
}

func (cmd *baseCmd) setReadTimeout(d time.Duration) {
	cmd._readTimeout = &d
}

func (cmd *baseCmd) writeTimeout() *time.Duration {
	return cmd._writeTimeout
}

func (cmd *baseCmd) setWriteTimeout(d time.Duration) {
	cmd._writeTimeout = &d
}

//------------------------------------------------------------------------------

type Cmd struct {
	*baseCmd
}

func NewCmd(args ...string) *Cmd {
	return &Cmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *Cmd) Val() interface{} {
	return cmd.val
}

//------------------------------------------------------------------------------

type StatusCmd struct {
	*baseCmd
}

func NewStatusCmd(args ...string) *StatusCmd {
	return &StatusCmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *StatusCmd) Val() string {
	if cmd.val == nil {
		return ""
	}
	return cmd.val.(string)
}

//------------------------------------------------------------------------------

type IntCmd struct {
	*baseCmd
}

func NewIntCmd(args ...string) *IntCmd {
	return &IntCmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *IntCmd) Val() int64 {
	if cmd.val == nil {
		return 0
	}
	return cmd.val.(int64)
}

//------------------------------------------------------------------------------

type DurationCmd struct {
	*baseCmd
	precision time.Duration
}

func NewDurationCmd(precision time.Duration, args ...string) *DurationCmd {
	return &DurationCmd{
		baseCmd:   newBaseCmd(args...),
		precision: precision,
	}
}

func (cmd *DurationCmd) parseReply(rd reader) (interface{}, error) {
	v, err := parseReply(rd)
	if err != nil {
		return 0, err
	}
	vv := time.Duration(v.(int64))
	return vv * cmd.precision, nil
}

func (cmd *DurationCmd) Val() time.Duration {
	if cmd.val == nil {
		return 0
	}
	return cmd.val.(time.Duration)
}

//------------------------------------------------------------------------------

type BoolCmd struct {
	*baseCmd
}

func NewBoolCmd(args ...string) *BoolCmd {
	return &BoolCmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *BoolCmd) parseReply(rd reader) (interface{}, error) {
	v, err := parseReply(rd)
	if err != nil {
		return nil, err
	}
	return v.(int64) == 1, nil
}

func (cmd *BoolCmd) Val() bool {
	if cmd.val == nil {
		return false
	}
	return cmd.val.(bool)
}

//------------------------------------------------------------------------------

type StringCmd struct {
	*baseCmd
}

func NewStringCmd(args ...string) *StringCmd {
	return &StringCmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *StringCmd) Val() string {
	if cmd.val == nil {
		return ""
	}
	return cmd.val.(string)
}

//------------------------------------------------------------------------------

type FloatCmd struct {
	*baseCmd
}

func NewFloatCmd(args ...string) *FloatCmd {
	return &FloatCmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *FloatCmd) parseReply(rd reader) (interface{}, error) {
	v, err := parseReply(rd)
	if err != nil {
		return nil, err
	}
	return strconv.ParseFloat(v.(string), 64)
}

func (cmd *FloatCmd) Val() float64 {
	if cmd.val == nil {
		return 0
	}
	return cmd.val.(float64)
}

//------------------------------------------------------------------------------

type SliceCmd struct {
	*baseCmd
}

func NewSliceCmd(args ...string) *SliceCmd {
	return &SliceCmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *SliceCmd) Val() []interface{} {
	if cmd.val == nil {
		return nil
	}
	return cmd.val.([]interface{})
}

//------------------------------------------------------------------------------

type StringSliceCmd struct {
	*baseCmd
}

func NewStringSliceCmd(args ...string) *StringSliceCmd {
	return &StringSliceCmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *StringSliceCmd) parseReply(rd reader) (interface{}, error) {
	return parseStringSliceReply(rd)
}

func (cmd *StringSliceCmd) Val() []string {
	if cmd.val == nil {
		return nil
	}
	return cmd.val.([]string)
}

//------------------------------------------------------------------------------

type BoolSliceCmd struct {
	*baseCmd
}

func NewBoolSliceCmd(args ...string) *BoolSliceCmd {
	return &BoolSliceCmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *BoolSliceCmd) parseReply(rd reader) (interface{}, error) {
	return parseBoolSliceReply(rd)
}

func (cmd *BoolSliceCmd) Val() []bool {
	if cmd.val == nil {
		return nil
	}
	return cmd.val.([]bool)
}

//------------------------------------------------------------------------------

type StringStringMapCmd struct {
	*baseCmd
}

func NewStringStringMapCmd(args ...string) *StringStringMapCmd {
	return &StringStringMapCmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *StringStringMapCmd) parseReply(rd reader) (interface{}, error) {
	return parseStringStringMapReply(rd)
}

func (cmd *StringStringMapCmd) Val() map[string]string {
	if cmd.val == nil {
		return nil
	}
	return cmd.val.(map[string]string)
}

//------------------------------------------------------------------------------

type StringFloatMapCmd struct {
	*baseCmd
}

func NewStringFloatMapCmd(args ...string) *StringFloatMapCmd {
	return &StringFloatMapCmd{
		baseCmd: newBaseCmd(args...),
	}
}

func (cmd *StringFloatMapCmd) parseReply(rd reader) (interface{}, error) {
	return parseStringFloatMapReply(rd)
}

func (cmd *StringFloatMapCmd) Val() map[string]float64 {
	if cmd.val == nil {
		return nil
	}
	return cmd.val.(map[string]float64)
}
