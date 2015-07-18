package redis

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/bufio.v1"
)

var (
	_ Cmder = (*Cmd)(nil)
	_ Cmder = (*SliceCmd)(nil)
	_ Cmder = (*StatusCmd)(nil)
	_ Cmder = (*IntCmd)(nil)
	_ Cmder = (*DurationCmd)(nil)
	_ Cmder = (*BoolCmd)(nil)
	_ Cmder = (*StringCmd)(nil)
	_ Cmder = (*FloatCmd)(nil)
	_ Cmder = (*StringSliceCmd)(nil)
	_ Cmder = (*BoolSliceCmd)(nil)
	_ Cmder = (*StringStringMapCmd)(nil)
	_ Cmder = (*StringIntMapCmd)(nil)
	_ Cmder = (*ZSliceCmd)(nil)
	_ Cmder = (*ScanCmd)(nil)
	_ Cmder = (*ClusterSlotCmd)(nil)
)

type Cmder interface {
	args() []interface{}
	parseReply(*bufio.Reader) error
	setErr(error)
	reset()

	writeTimeout() *time.Duration
	readTimeout() *time.Duration
	clusterKey() string

	Err() error
	fmt.Stringer
}

func setCmdsErr(cmds []Cmder, e error) {
	for _, cmd := range cmds {
		cmd.setErr(e)
	}
}

func resetCmds(cmds []Cmder) {
	for _, cmd := range cmds {
		cmd.reset()
	}
}

func cmdString(cmd Cmder, val interface{}) string {
	var ss []string
	for _, arg := range cmd.args() {
		ss = append(ss, fmt.Sprint(arg))
	}
	s := strings.Join(ss, " ")
	if err := cmd.Err(); err != nil {
		return s + ": " + err.Error()
	}
	if val != nil {
		switch vv := val.(type) {
		case []byte:
			return s + ": " + string(vv)
		default:
			return s + ": " + fmt.Sprint(val)
		}
	}
	return s

}

//------------------------------------------------------------------------------

type baseCmd struct {
	_args []interface{}

	err error

	_clusterKeyPos int

	_writeTimeout, _readTimeout *time.Duration
}

func (cmd *baseCmd) Err() error {
	if cmd.err != nil {
		return cmd.err
	}
	return nil
}

func (cmd *baseCmd) args() []interface{} {
	return cmd._args
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

func (cmd *baseCmd) clusterKey() string {
	if cmd._clusterKeyPos > 0 && cmd._clusterKeyPos < len(cmd._args) {
		return fmt.Sprint(cmd._args[cmd._clusterKeyPos])
	}
	return ""
}

func (cmd *baseCmd) setWriteTimeout(d time.Duration) {
	cmd._writeTimeout = &d
}

func (cmd *baseCmd) setErr(e error) {
	cmd.err = e
}

//------------------------------------------------------------------------------

type Cmd struct {
	baseCmd

	val interface{}
}

func NewCmd(args ...interface{}) *Cmd {
	return &Cmd{baseCmd: baseCmd{_args: args}}
}

func (cmd *Cmd) reset() {
	cmd.val = nil
	cmd.err = nil
}

func (cmd *Cmd) Val() interface{} {
	return cmd.val
}

func (cmd *Cmd) Result() (interface{}, error) {
	return cmd.val, cmd.err
}

func (cmd *Cmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *Cmd) parseReply(rd *bufio.Reader) error {
	cmd.val, cmd.err = parseReply(rd, parseSlice)
	// Convert to string to preserve old behaviour.
	// TODO: remove in v4
	if v, ok := cmd.val.([]byte); ok {
		cmd.val = string(v)
	}
	return cmd.err
}

//------------------------------------------------------------------------------

type SliceCmd struct {
	baseCmd

	val []interface{}
}

func NewSliceCmd(args ...interface{}) *SliceCmd {
	return &SliceCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *SliceCmd) reset() {
	cmd.val = nil
	cmd.err = nil
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

func (cmd *SliceCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, parseSlice)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = v.([]interface{})
	return nil
}

//------------------------------------------------------------------------------

type StatusCmd struct {
	baseCmd

	val string
}

func NewStatusCmd(args ...interface{}) *StatusCmd {
	return &StatusCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func newKeylessStatusCmd(args ...interface{}) *StatusCmd {
	return &StatusCmd{baseCmd: baseCmd{_args: args}}
}

func (cmd *StatusCmd) reset() {
	cmd.val = ""
	cmd.err = nil
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

func (cmd *StatusCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, nil)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = string(v.([]byte))
	return nil
}

//------------------------------------------------------------------------------

type IntCmd struct {
	baseCmd

	val int64
}

func NewIntCmd(args ...interface{}) *IntCmd {
	return &IntCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *IntCmd) reset() {
	cmd.val = 0
	cmd.err = nil
}

func (cmd *IntCmd) Val() int64 {
	return cmd.val
}

func (cmd *IntCmd) Result() (int64, error) {
	return cmd.val, cmd.err
}

func (cmd *IntCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *IntCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, nil)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = v.(int64)
	return nil
}

//------------------------------------------------------------------------------

type DurationCmd struct {
	baseCmd

	val       time.Duration
	precision time.Duration
}

func NewDurationCmd(precision time.Duration, args ...interface{}) *DurationCmd {
	return &DurationCmd{
		precision: precision,
		baseCmd:   baseCmd{_args: args, _clusterKeyPos: 1},
	}
}

func (cmd *DurationCmd) reset() {
	cmd.val = 0
	cmd.err = nil
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

func (cmd *DurationCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, nil)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = time.Duration(v.(int64)) * cmd.precision
	return nil
}

//------------------------------------------------------------------------------

type BoolCmd struct {
	baseCmd

	val bool
}

func NewBoolCmd(args ...interface{}) *BoolCmd {
	return &BoolCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *BoolCmd) reset() {
	cmd.val = false
	cmd.err = nil
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

var ok = []byte("OK")

func (cmd *BoolCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, nil)
	// `SET key value NX` returns nil when key already exists.
	if err == Nil {
		cmd.val = false
		return nil
	}
	if err != nil {
		cmd.err = err
		return err
	}
	switch vv := v.(type) {
	case int64:
		cmd.val = vv == 1
		return nil
	case []byte:
		cmd.val = bytes.Equal(vv, ok)
		return nil
	default:
		return fmt.Errorf("got %T, wanted int64 or string")
	}
}

//------------------------------------------------------------------------------

type StringCmd struct {
	baseCmd

	val []byte
}

func NewStringCmd(args ...interface{}) *StringCmd {
	return &StringCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *StringCmd) reset() {
	cmd.val = nil
	cmd.err = nil
}

func (cmd *StringCmd) Val() string {
	return bytesToString(cmd.val)
}

func (cmd *StringCmd) Result() (string, error) {
	return cmd.Val(), cmd.err
}

func (cmd *StringCmd) Bytes() ([]byte, error) {
	return cmd.val, cmd.err
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

func (cmd *StringCmd) Float64() (float64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.ParseFloat(cmd.Val(), 64)
}

func (cmd *StringCmd) Scan(val interface{}) error {
	if cmd.err != nil {
		return cmd.err
	}
	return scan(cmd.val, val)
}

func (cmd *StringCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *StringCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, nil)
	if err != nil {
		cmd.err = err
		return err
	}
	b := v.([]byte)
	cmd.val = make([]byte, len(b))
	copy(cmd.val, b)
	return nil
}

//------------------------------------------------------------------------------

type FloatCmd struct {
	baseCmd

	val float64
}

func NewFloatCmd(args ...interface{}) *FloatCmd {
	return &FloatCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *FloatCmd) reset() {
	cmd.val = 0
	cmd.err = nil
}

func (cmd *FloatCmd) Val() float64 {
	return cmd.val
}

func (cmd *FloatCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *FloatCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, nil)
	if err != nil {
		cmd.err = err
		return err
	}
	b := v.([]byte)
	cmd.val, cmd.err = strconv.ParseFloat(bytesToString(b), 64)
	return cmd.err
}

//------------------------------------------------------------------------------

type StringSliceCmd struct {
	baseCmd

	val []string
}

func NewStringSliceCmd(args ...interface{}) *StringSliceCmd {
	return &StringSliceCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *StringSliceCmd) reset() {
	cmd.val = nil
	cmd.err = nil
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

func (cmd *StringSliceCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, parseStringSlice)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = v.([]string)
	return nil
}

//------------------------------------------------------------------------------

type BoolSliceCmd struct {
	baseCmd

	val []bool
}

func NewBoolSliceCmd(args ...interface{}) *BoolSliceCmd {
	return &BoolSliceCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *BoolSliceCmd) reset() {
	cmd.val = nil
	cmd.err = nil
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

func (cmd *BoolSliceCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, parseBoolSlice)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = v.([]bool)
	return nil
}

//------------------------------------------------------------------------------

type StringStringMapCmd struct {
	baseCmd

	val map[string]string
}

func NewStringStringMapCmd(args ...interface{}) *StringStringMapCmd {
	return &StringStringMapCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *StringStringMapCmd) reset() {
	cmd.val = nil
	cmd.err = nil
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

func (cmd *StringStringMapCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, parseStringStringMap)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = v.(map[string]string)
	return nil
}

//------------------------------------------------------------------------------

type StringIntMapCmd struct {
	baseCmd

	val map[string]int64
}

func NewStringIntMapCmd(args ...interface{}) *StringIntMapCmd {
	return &StringIntMapCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
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

func (cmd *StringIntMapCmd) reset() {
	cmd.val = nil
	cmd.err = nil
}

func (cmd *StringIntMapCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, parseStringIntMap)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = v.(map[string]int64)
	return nil
}

//------------------------------------------------------------------------------

type ZSliceCmd struct {
	baseCmd

	val []Z
}

func NewZSliceCmd(args ...interface{}) *ZSliceCmd {
	return &ZSliceCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *ZSliceCmd) reset() {
	cmd.val = nil
	cmd.err = nil
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

func (cmd *ZSliceCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, parseZSlice)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = v.([]Z)
	return nil
}

//------------------------------------------------------------------------------

type ScanCmd struct {
	baseCmd

	cursor int64
	keys   []string
}

func NewScanCmd(args ...interface{}) *ScanCmd {
	return &ScanCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *ScanCmd) reset() {
	cmd.cursor = 0
	cmd.keys = nil
	cmd.err = nil
}

func (cmd *ScanCmd) Val() (int64, []string) {
	return cmd.cursor, cmd.keys
}

func (cmd *ScanCmd) Result() (int64, []string, error) {
	return cmd.cursor, cmd.keys, cmd.err
}

func (cmd *ScanCmd) String() string {
	return cmdString(cmd, cmd.keys)
}

func (cmd *ScanCmd) parseReply(rd *bufio.Reader) error {
	vi, err := parseReply(rd, parseSlice)
	if err != nil {
		cmd.err = err
		return cmd.err
	}
	v := vi.([]interface{})

	cmd.cursor, cmd.err = strconv.ParseInt(v[0].(string), 10, 64)
	if cmd.err != nil {
		return cmd.err
	}

	keys := v[1].([]interface{})
	for _, keyi := range keys {
		cmd.keys = append(cmd.keys, keyi.(string))
	}

	return nil
}

//------------------------------------------------------------------------------

type ClusterSlotInfo struct {
	Start, End int
	Addrs      []string
}

type ClusterSlotCmd struct {
	baseCmd

	val []ClusterSlotInfo
}

func NewClusterSlotCmd(args ...interface{}) *ClusterSlotCmd {
	return &ClusterSlotCmd{baseCmd: baseCmd{_args: args, _clusterKeyPos: 1}}
}

func (cmd *ClusterSlotCmd) Val() []ClusterSlotInfo {
	return cmd.val
}

func (cmd *ClusterSlotCmd) Result() ([]ClusterSlotInfo, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *ClusterSlotCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *ClusterSlotCmd) reset() {
	cmd.val = nil
	cmd.err = nil
}

func (cmd *ClusterSlotCmd) parseReply(rd *bufio.Reader) error {
	v, err := parseReply(rd, parseClusterSlotInfoSlice)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = v.([]ClusterSlotInfo)
	return nil
}
