package redis

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"time"

	"gopkg.in/redis.v4/internal/pool"
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
	_ Cmder = (*ClusterSlotsCmd)(nil)
)

type Cmder interface {
	args() []interface{}
	readReply(*pool.Conn) error
	setErr(error)
	reset()

	readTimeout() *time.Duration
	clusterKey() string

	Err() error
	IsWriter() bool
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

func writeCmd(cn *pool.Conn, cmds ...Cmder) error {
	cn.Buf = cn.Buf[:0]
	for _, cmd := range cmds {
		var err error
		cn.Buf, err = appendArgs(cn.Buf, cmd.args())
		if err != nil {
			return err
		}
	}

	_, err := cn.Write(cn.Buf)
	return err
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

	_readTimeout *time.Duration

	_isWriter bool
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

func (cmd *baseCmd) clusterKey() string {
	if cmd._clusterKeyPos > 0 && cmd._clusterKeyPos < len(cmd._args) {
		return fmt.Sprint(cmd._args[cmd._clusterKeyPos])
	}
	return ""
}

func (cmd *baseCmd) setErr(e error) {
	cmd.err = e
}

func (cmd *baseCmd) IsWriter() bool {
	return cmd._isWriter
}

func NewBaseCmd(args []interface{}) baseCmd {
	cmd := baseCmd{}
	if len(args) > 0 {
		if isWriter, ok := args[len(args)-1].(bool); ok {
			cmd._isWriter = isWriter
			if len(args) > 1 {
				cmd._args = args[0 : len(args)-1]
			}
		} else {
			cmd._args = args
		}
	}
	return cmd
}

//------------------------------------------------------------------------------

type Cmd struct {
	baseCmd

	val interface{}
}

func NewCmd(args ...interface{}) *Cmd {
	return &Cmd{baseCmd: NewBaseCmd(args)}
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

func (cmd *Cmd) readReply(cn *pool.Conn) error {
	val, err := readReply(cn, sliceParser)
	if err != nil {
		cmd.err = err
		return cmd.err
	}
	if b, ok := val.([]byte); ok {
		// Bytes must be copied, because underlying memory is reused.
		cmd.val = string(b)
	} else {
		cmd.val = val
	}
	return nil
}

//------------------------------------------------------------------------------

type SliceCmd struct {
	baseCmd

	val []interface{}
}

func NewSliceCmd(args ...interface{}) *SliceCmd {
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &SliceCmd{baseCmd: cmd}
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

func (cmd *SliceCmd) readReply(cn *pool.Conn) error {
	v, err := readArrayReply(cn, sliceParser)
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
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &StatusCmd{baseCmd: cmd}
}

func newKeylessStatusCmd(args ...interface{}) *StatusCmd {
	return &StatusCmd{baseCmd: NewBaseCmd(args)}
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

func (cmd *StatusCmd) readReply(cn *pool.Conn) error {
	cmd.val, cmd.err = readStringReply(cn)
	return cmd.err
}

//------------------------------------------------------------------------------

type IntCmd struct {
	baseCmd

	val int64
}

func NewIntCmd(args ...interface{}) *IntCmd {
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &IntCmd{baseCmd: cmd}
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

func (cmd *IntCmd) readReply(cn *pool.Conn) error {
	cmd.val, cmd.err = readIntReply(cn)
	return cmd.err
}

//------------------------------------------------------------------------------

type DurationCmd struct {
	baseCmd

	val       time.Duration
	precision time.Duration
}

func NewDurationCmd(precision time.Duration, args ...interface{}) *DurationCmd {
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &DurationCmd{
		precision: precision,
		baseCmd:   cmd,
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

func (cmd *DurationCmd) readReply(cn *pool.Conn) error {
	n, err := readIntReply(cn)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = time.Duration(n) * cmd.precision
	return nil
}

//------------------------------------------------------------------------------

type BoolCmd struct {
	baseCmd

	val bool
}

func NewBoolCmd(args ...interface{}) *BoolCmd {
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &BoolCmd{baseCmd: cmd}
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

func (cmd *BoolCmd) readReply(cn *pool.Conn) error {
	v, err := readReply(cn, nil)
	// `SET key value NX` returns nil when key already exists. But
	// `SETNX key value` returns bool (0/1). So convert nil to bool.
	// TODO: is this okay?
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
		return fmt.Errorf("got %T, wanted int64 or string", v)
	}
}

//------------------------------------------------------------------------------

type StringCmd struct {
	baseCmd

	val []byte
}

func NewStringCmd(args ...interface{}) *StringCmd {
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &StringCmd{baseCmd: cmd}
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

func (cmd *StringCmd) readReply(cn *pool.Conn) error {
	b, err := readBytesReply(cn)
	if err != nil {
		cmd.err = err
		return err
	}

	new := make([]byte, len(b))
	copy(new, b)
	cmd.val = new

	return nil
}

//------------------------------------------------------------------------------

type FloatCmd struct {
	baseCmd

	val float64
}

func NewFloatCmd(args ...interface{}) *FloatCmd {
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &FloatCmd{baseCmd: cmd}
}

func (cmd *FloatCmd) reset() {
	cmd.val = 0
	cmd.err = nil
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

func (cmd *FloatCmd) readReply(cn *pool.Conn) error {
	cmd.val, cmd.err = readFloatReply(cn)
	return cmd.err
}

//------------------------------------------------------------------------------

type StringSliceCmd struct {
	baseCmd

	val []string
}

func NewStringSliceCmd(args ...interface{}) *StringSliceCmd {
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &StringSliceCmd{baseCmd: cmd}
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

func (cmd *StringSliceCmd) readReply(cn *pool.Conn) error {
	v, err := readArrayReply(cn, stringSliceParser)
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
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &BoolSliceCmd{baseCmd: cmd}
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

func (cmd *BoolSliceCmd) readReply(cn *pool.Conn) error {
	v, err := readArrayReply(cn, boolSliceParser)
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
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &StringStringMapCmd{baseCmd: cmd}
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

func (cmd *StringStringMapCmd) readReply(cn *pool.Conn) error {
	v, err := readArrayReply(cn, stringStringMapParser)
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
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &StringIntMapCmd{baseCmd: cmd}
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

func (cmd *StringIntMapCmd) readReply(cn *pool.Conn) error {
	v, err := readArrayReply(cn, stringIntMapParser)
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
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &ZSliceCmd{baseCmd: cmd}
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

func (cmd *ZSliceCmd) readReply(cn *pool.Conn) error {
	v, err := readArrayReply(cn, zSliceParser)
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

	page   []string
	cursor uint64
}

func NewScanCmd(args ...interface{}) *ScanCmd {
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &ScanCmd{
		baseCmd: cmd,
	}
}

func (cmd *ScanCmd) reset() {
	cmd.cursor = 0
	cmd.page = nil
	cmd.err = nil
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

func (cmd *ScanCmd) readReply(cn *pool.Conn) error {
	page, cursor, err := readScanReply(cn)
	if err != nil {
		cmd.err = err
		return cmd.err
	}
	cmd.page = page
	cmd.cursor = cursor
	return nil
}

//------------------------------------------------------------------------------

type ClusterNode struct {
	Id   string
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

func NewClusterSlotsCmd(args ...interface{}) *ClusterSlotsCmd {
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &ClusterSlotsCmd{baseCmd: cmd}
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

func (cmd *ClusterSlotsCmd) reset() {
	cmd.val = nil
	cmd.err = nil
}

func (cmd *ClusterSlotsCmd) readReply(cn *pool.Conn) error {
	v, err := readArrayReply(cn, clusterSlotsParser)
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.val = v.([]ClusterSlot)
	return nil
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
	Sort string
}

type GeoLocationCmd struct {
	baseCmd

	q         *GeoRadiusQuery
	locations []GeoLocation
}

func NewGeoLocationCmd(q *GeoRadiusQuery, args ...interface{}) *GeoLocationCmd {
	args = append(args, q.Radius)
	if q.Unit != "" {
		args = append(args, q.Unit)
	} else {
		args = append(args, "km")
	}
	if q.WithCoord {
		args = append(args, "WITHCOORD")
	}
	if q.WithDist {
		args = append(args, "WITHDIST")
	}
	if q.WithGeoHash {
		args = append(args, "WITHHASH")
	}
	if q.Count > 0 {
		args = append(args, "COUNT", q.Count)
	}
	if q.Sort != "" {
		args = append(args, q.Sort)
	}
	args = append(args, isReadOnly)
	cmd := NewBaseCmd(args)
	cmd._clusterKeyPos = 1
	return &GeoLocationCmd{
		baseCmd: cmd,
		q:       q,
	}
}

func (cmd *GeoLocationCmd) reset() {
	cmd.locations = nil
	cmd.err = nil
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

func (cmd *GeoLocationCmd) readReply(cn *pool.Conn) error {
	reply, err := readArrayReply(cn, newGeoLocationSliceParser(cmd.q))
	if err != nil {
		cmd.err = err
		return err
	}
	cmd.locations = reply.([]GeoLocation)
	return nil
}
