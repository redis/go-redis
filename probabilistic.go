package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9/internal/proto"
)

type probabilisticCmdable interface {
	BFADD(ctx context.Context, key string, element interface{}) *BoolCmd
	BFCARD(ctx context.Context, key string) *IntCmd
	BFEXISTS(ctx context.Context, key string, element interface{}) *BoolCmd
	BFINFO(ctx context.Context, key string) *BFINFOCmd
	BFINFOARG(ctx context.Context, key, option string) *BFINFOCmd
	BFINFOCAPACITY(ctx context.Context, key string) *BFINFOCmd
	BFINFOSIZE(ctx context.Context, key string) *BFINFOCmd
	BFINFOFILTERS(ctx context.Context, key string) *BFINFOCmd
	BFINFOITEMS(ctx context.Context, key string) *BFINFOCmd
	BFINFOEXPANSION(ctx context.Context, key string) *BFINFOCmd
	BFINSERT(ctx context.Context, key string, options *BFINSERTOptions, elements ...interface{}) *BoolSliceCmd
	BFMADD(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd
	BFMEXISTS(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd
	BFRESERVE(ctx context.Context, key string, errorRate float64, capacity int64) *StatusCmd
	BFRESERVEEXPANSION(ctx context.Context, key string, errorRate float64, capacity, expansion int64) *StatusCmd
	BFRESERVENONSCALING(ctx context.Context, key string, errorRate float64, capacity int64) *StatusCmd
	BFRESERVEARGS(ctx context.Context, key string, options *BFRESERVEOptions) *StatusCmd
	BFSCANDUMP(ctx context.Context, key string, iterator int64) *ScanDumpCmd
	BFLOADCHUNK(ctx context.Context, key string, iterator int64, data interface{}) *StatusCmd

	CFADD(ctx context.Context, key string, element interface{}) *BoolCmd
	CFADDNX(ctx context.Context, key string, element interface{}) *BoolCmd
	CFCOUNT(ctx context.Context, key string, element interface{}) *IntCmd
	CFDEL(ctx context.Context, key string, element interface{}) *BoolCmd
	CFEXISTS(ctx context.Context, key string, element interface{}) *BoolCmd
	CFINFO(ctx context.Context, key string) *CFINFOCmd
	CFINSERT(ctx context.Context, key string, options *CFINSERTOptions, elements ...interface{}) *BoolSliceCmd
	CFINSERTNX(ctx context.Context, key string, options *CFINSERTOptions, elements ...interface{}) *IntSliceCmd
	CFMEXISTS(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd
	CFRESERVE(ctx context.Context, key string, capacity int64) *StatusCmd
	CFRESERVEARGS(ctx context.Context, key string, options *CFRESERVEOptions) *StatusCmd
	CFRESERVEEXPANSION(ctx context.Context, key string, capacity int64, expansion int64) *StatusCmd
	CFRESERVEBUCKETSIZE(ctx context.Context, key string, capacity int64, bucketsize int64) *StatusCmd
	CFRESERVEMAXITERATIONS(ctx context.Context, key string, capacity int64, maxiterations int64) *StatusCmd
	CFSCANDUMP(ctx context.Context, key string, iterator int64) *ScanDumpCmd
	CFLOADCHUNK(ctx context.Context, key string, iterator int64, data interface{}) *StatusCmd

	CMSINCRBY(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd
	CMSINFO(ctx context.Context, key string) *CMSINFOCmd
	CMSINITBYDIM(ctx context.Context, key string, width, height int64) *StatusCmd
	CMSINITBYPROB(ctx context.Context, key string, errorRate, probability float64) *StatusCmd
	CMSMERGE(ctx context.Context, destKey string, sourceKeys ...string) *StatusCmd
	CMSMERGEWITHWEIGHT(ctx context.Context, destKey string, sourceKeys map[string]int64) *StatusCmd
	CMSQUERY(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd

	TOPKADD(ctx context.Context, key string, elements ...interface{}) *StringSliceCmd
	TOPKCOUNT(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd
	TOPKINCRBY(ctx context.Context, key string, elements ...interface{}) *StringSliceCmd
	TOPKINFO(ctx context.Context, key string) *TOPKINFOCmd
	TOPKLIST(ctx context.Context, key string) *StringSliceCmd
	TOPKLISTWITHCOUNT(ctx context.Context, key string) *MapStringIntCmd
	TOPKQUERY(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd
	TOPKRESERVE(ctx context.Context, key string, k int64) *StatusCmd
	TOPKRESERVEWITHOPTIONS(ctx context.Context, key string, k int64, width, depth int64, decay float64) *StatusCmd

	TDIGESTADD(ctx context.Context, key string, elements ...float64) *StatusCmd
	TDIGESTBYRANK(ctx context.Context, key string, rank ...uint64) *FloatSliceCmd
	TDIGESTBYREVRANK(ctx context.Context, key string, rank ...uint64) *FloatSliceCmd
	TDIGESTCDF(ctx context.Context, key string, elements ...float64) *FloatSliceCmd
	TDIGESTCREATE(ctx context.Context, key string) *StatusCmd
	TDIGESTCREATEWITHCOMPRESSION(ctx context.Context, key string, compression int64) *StatusCmd
	TDIGESTINFO(ctx context.Context, key string) *TDIGESTINFOCmd
	TDIGESTMAX(ctx context.Context, key string) *FloatCmd
	TDIGESTMIN(ctx context.Context, key string) *FloatCmd
	TDIGESTMERGE(ctx context.Context, destKey string, options *TDIGESTMERGEOptions, sourceKeys ...string) *StatusCmd
	TDIGESTQUANTILE(ctx context.Context, key string, elements ...float64) *FloatSliceCmd
	TDIGESTRANK(ctx context.Context, key string, values ...float64) *IntSliceCmd
	TDIGESTRESET(ctx context.Context, key string) *StatusCmd
	TDIGESTREVRANK(ctx context.Context, key string, values ...float64) *IntSliceCmd
	TDIGESTTRIMMEDMEAN(ctx context.Context, key string, lowCutQuantile, highCutQuantile float64) *FloatCmd
}

type BFINSERTOptions struct {
	Capacity   int64
	Error      float64
	Expansion  int64
	NonScaling bool
	NoCreate   bool
}

type BFRESERVEOptions struct {
	Capacity   int64
	Error      float64
	Expansion  int64
	NonScaling bool
}

type CFRESERVEOptions struct {
	Capacity      int64
	BucketSize    int64
	MaxIterations int64
	Expansion     int64
}

type CFINSERTOptions struct {
	Capacity int64
	NoCreate bool
}

// -------------------------------------------
// Bloom filter commands
//-------------------------------------------

// BFRESERVE creates an empty Bloom filter with a single sub-filter
// for the initial specified capacity and with an upper bound error_rate.
// For more information - https://redis.io/commands/bf.reserve/
func (c cmdable) BFRESERVE(ctx context.Context, key string, errorRate float64, capacity int64) *StatusCmd {
	args := []interface{}{"bf.reserve", key, errorRate, capacity}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// BFRESERVEEXPANSION creates an empty Bloom filter with a single sub-filter
// for the initial specified capacity and with an upper bound error_rate.
// This function also allows for specifying an expansion rate for the filter.
// For more information - https://redis.io/commands/bf.reserve/
func (c cmdable) BFRESERVEEXPANSION(ctx context.Context, key string, errorRate float64, capacity, expansion int64) *StatusCmd {
	args := []interface{}{"bf.reserve", key, errorRate, capacity, "expansion", expansion}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// BFRESERVENONSCALING creates an empty Bloom filter with a single sub-filter
// for the initial specified capacity and with an upper bound error_rate.
// This function also allows for specifying that the filter should not scale.
// For more information - https://redis.io/commands/bf.reserve/
func (c cmdable) BFRESERVENONSCALING(ctx context.Context, key string, errorRate float64, capacity int64) *StatusCmd {
	args := []interface{}{"bf.reserve", key, errorRate, capacity, "nonscaling"}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// BFRESERVEARGS creates an empty Bloom filter with a single sub-filter
// for the initial specified capacity and with an upper bound error_rate.
// This function also allows for specifying additional options such as expansion rate and non-scaling behavior.
// For more information - https://redis.io/commands/bf.reserve/
func (c cmdable) BFRESERVEARGS(ctx context.Context, key string, options *BFRESERVEOptions) *StatusCmd {
	args := []interface{}{"bf.reserve", key}
	if options != nil {
		if options.Error != 0 {
			args = append(args, options.Error)
		}
		if options.Capacity != 0 {
			args = append(args, options.Capacity)
		}
		if options.Expansion != 0 {
			args = append(args, "expansion", options.Expansion)
		}
		if options.NonScaling {
			args = append(args, "nonscaling")
		}
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// BFADD adds an item to a Bloom filter.
// For more information - https://redis.io/commands/bf.add/
func (c cmdable) BFADD(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"bf.add", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// BFCARD returns the cardinality of a Bloom filter -
// number of items that were added to a Bloom filter and detected as unique
// (items that caused at least one bit to be set in at least one sub-filter).
// For more information - https://redis.io/commands/bf.card/
func (c cmdable) BFCARD(ctx context.Context, key string) *IntCmd {
	args := []interface{}{"bf.card", key}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// BFEXISTS determines whether a given item was added to a Bloom filter.
// For more information - https://redis.io/commands/bf.exists/
func (c cmdable) BFEXISTS(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"bf.exists", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// BFLOADCHUNK restores a Bloom filter previously saved using BF.SCANDUMP.
// For more information - https://redis.io/commands/bf.loadchunk/
func (c cmdable) BFLOADCHUNK(ctx context.Context, key string, iterator int64, data interface{}) *StatusCmd {
	args := []interface{}{"bf.loadchunk", key, iterator, data}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// Begins an incremental save of the Bloom filter.
// This command is useful for large Bloom filters that cannot fit into the DUMP and RESTORE model.
// For more information - https://redis.io/commands/bf.scandump/
func (c cmdable) BFSCANDUMP(ctx context.Context, key string, iterator int64) *ScanDumpCmd {
	args := []interface{}{"bf.scandump", key, iterator}
	cmd := newScanDumpCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type ScanDump struct {
	Iter int64
	Data string
}

type ScanDumpCmd struct {
	baseCmd

	val ScanDump
}

func newScanDumpCmd(ctx context.Context, args ...interface{}) *ScanDumpCmd {
	return &ScanDumpCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *ScanDumpCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *ScanDumpCmd) SetVal(val ScanDump) {
	cmd.val = val
}

func (cmd *ScanDumpCmd) Result() (ScanDump, error) {
	return cmd.val, cmd.err
}

func (cmd *ScanDumpCmd) Val() ScanDump {
	return cmd.val
}

func (cmd *ScanDumpCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}
	cmd.val = ScanDump{}
	for i := 0; i < n; i++ {
		iter, err := rd.ReadInt()
		if err != nil {
			return err
		}
		data, err := rd.ReadString()
		if err != nil {
			return err
		}
		cmd.val.Data = data
		cmd.val.Iter = iter

	}

	return nil
}

// Returns information about a Bloom filter.
// For more information - https://redis.io/commands/bf.info/
func (c cmdable) BFINFO(ctx context.Context, key string) *BFINFOCmd {
	args := []interface{}{"bf.info", key}
	cmd := NewBFINFOCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type BFINFO struct {
	Capacity      int64
	Size          int64
	Filters       int64
	ItemsInserted int64
	ExpansionRate int64
}

type BFINFOCmd struct {
	baseCmd

	val BFINFO
}

func NewBFINFOCmd(ctx context.Context, args ...interface{}) *BFINFOCmd {
	return &BFINFOCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *BFINFOCmd) SetVal(val BFINFO) {
	cmd.val = val
}
func (cmd *BFINFOCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *BFINFOCmd) Val() BFINFO {
	return cmd.val
}

func (cmd *BFINFOCmd) Result() (BFINFO, error) {
	return cmd.val, cmd.err
}

func (cmd *BFINFOCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}

	var key string
	var result BFINFO
	for f := 0; f < n; f++ {
		key, err = rd.ReadString()
		if err != nil {
			return err
		}

		switch key {
		case "Capacity":
			result.Capacity, err = rd.ReadInt()
		case "Size":
			result.Size, err = rd.ReadInt()
		case "Number of filters":
			result.Filters, err = rd.ReadInt()
		case "Number of items inserted":
			result.ItemsInserted, err = rd.ReadInt()
		case "Expansion rate":
			result.ExpansionRate, err = rd.ReadInt()
		default:
			return fmt.Errorf("redis: bloom.info unexpected key %s", key)
		}

		if err != nil {
			return err
		}
	}

	cmd.val = result
	return nil
}

// BFINFOCAPACITY returns information about the capacity of a Bloom filter.
// For more information - https://redis.io/commands/bf.info/
func (c cmdable) BFINFOCAPACITY(ctx context.Context, key string) *BFINFOCmd {
	return c.BFINFOARG(ctx, key, "capacity")
}

// BFINFOSIZE returns information about the size of a Bloom filter.
// For more information - https://redis.io/commands/bf.info/
func (c cmdable) BFINFOSIZE(ctx context.Context, key string) *BFINFOCmd {
	return c.BFINFOARG(ctx, key, "size")
}

// BFINFOFILTERS returns information about the filters of a Bloom filter.
// For more information - https://redis.io/commands/bf.info/
func (c cmdable) BFINFOFILTERS(ctx context.Context, key string) *BFINFOCmd {
	return c.BFINFOARG(ctx, key, "filters")
}

// BFINFOITEMS returns information about the items of a Bloom filter.
// For more information - https://redis.io/commands/bf.info/
func (c cmdable) BFINFOITEMS(ctx context.Context, key string) *BFINFOCmd {
	return c.BFINFOARG(ctx, key, "items")
}

// BFINFOEXPANSION returns information about the expansion rate of a Bloom filter.
// For more information - https://redis.io/commands/bf.info/
func (c cmdable) BFINFOEXPANSION(ctx context.Context, key string) *BFINFOCmd {
	return c.BFINFOARG(ctx, key, "expansion")
}

// BFINFOARG returns information about a specific option of a Bloom filter.
// For more information - https://redis.io/commands/bf.info/
func (c cmdable) BFINFOARG(ctx context.Context, key, option string) *BFINFOCmd {
	args := []interface{}{"bf.info", key, option}
	cmd := NewBFINFOCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// BFINSERT inserts elements into a Bloom filter.
// This function also allows for specifying additional options such as:
// capacity, error rate, expansion rate, and non-scaling behavior.
// For more information - https://redis.io/commands/bf.insert/
func (c cmdable) BFINSERT(ctx context.Context, key string, options *BFINSERTOptions, elements ...interface{}) *BoolSliceCmd {
	args := []interface{}{"bf.insert", key}
	if options != nil {
		if options.Capacity != 0 {
			args = append(args, "capacity", options.Capacity)
		}
		if options.Error != 0 {
			args = append(args, "error", options.Error)
		}
		if options.Expansion != 0 {
			args = append(args, "expansion", options.Expansion)
		}
		if options.NoCreate {
			args = append(args, "nocreate")
		}
		if options.NonScaling {
			args = append(args, "nonscaling")
		}
	}
	args = append(args, "items")
	args = append(args, elements...)

	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// BFMADD adds multiple elements to a Bloom filter.
// Returns an array of booleans indicating whether each element was added to the filter or not.
// For more information - https://redis.io/commands/bf.madd/
func (c cmdable) BFMADD(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd {
	args := []interface{}{"bf.madd", key}
	args = append(args, elements...)
	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// BFMEXISTS check if multiple elements exist in a Bloom filter.
// Returns an array of booleans indicating whether each element exists in the filter or not.
// For more information - https://redis.io/commands/bf.mexists/
func (c cmdable) BFMEXISTS(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd {
	args := []interface{}{"bf.mexists", key}
	args = append(args, elements...)

	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// -------------------------------------------
// Cuckoo filter commands
//-------------------------------------------

// CFRESERVE creates an empty Cuckoo filter with the specified capacity.
// For more information - https://redis.io/commands/cf.reserve/
func (c cmdable) CFRESERVE(ctx context.Context, key string, capacity int64) *StatusCmd {
	args := []interface{}{"cf.reserve", key, capacity}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFRESERVEEXPANSION creates an empty Cuckoo filter with the specified capacity and expansion rate.
// For more information - https://redis.io/commands/cf.reserve/
func (c cmdable) CFRESERVEEXPANSION(ctx context.Context, key string, capacity int64, expansion int64) *StatusCmd {
	args := []interface{}{"cf.reserve", key, capacity, "expansion", expansion}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFRESERVEBUCKETSIZE creates an empty Cuckoo filter with the specified capacity and bucket size.
// For more information - https://redis.io/commands/cf.reserve/
func (c cmdable) CFRESERVEBUCKETSIZE(ctx context.Context, key string, capacity int64, bucketsize int64) *StatusCmd {
	args := []interface{}{"cf.reserve", key, capacity, "bucketsize", bucketsize}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFRESERVEMAXITERATIONS creates an empty Cuckoo filter with the specified capacity and maximum number of iterations.
// For more information - https://redis.io/commands/cf.reserve/
func (c cmdable) CFRESERVEMAXITERATIONS(ctx context.Context, key string, capacity int64, maxiterations int64) *StatusCmd {
	args := []interface{}{"cf.reserve", key, capacity, "maxiterations", maxiterations}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFRESERVEARGS creates an empty Cuckoo filter with the specified options.
// This function allows for specifying additional options such as bucket size and maximum number of iterations.
// For more information - https://redis.io/commands/cf.reserve/
func (c cmdable) CFRESERVEARGS(ctx context.Context, key string, options *CFRESERVEOptions) *StatusCmd {
	args := []interface{}{"cf.reserve", key, options.Capacity}
	if options.BucketSize != 0 {
		args = append(args, "bucketsize", options.BucketSize)
	}
	if options.MaxIterations != 0 {
		args = append(args, "maxiterations", options.MaxIterations)
	}
	if options.Expansion != 0 {
		args = append(args, "expansion", options.Expansion)
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFADD adds an element to a Cuckoo filter.
// Returns true if the element was added to the filter or false if it already exists in the filter.
// For more information - https://redis.io/commands/cf.add/
func (c cmdable) CFADD(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"cf.add", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFADDNX adds an element to a Cuckoo filter only if it does not already exist in the filter.
// Returns true if the element was added to the filter or false if it already exists in the filter.
// For more information - https://redis.io/commands/cf.addnx/
func (c cmdable) CFADDNX(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"cf.addnx", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFCOUNT returns an estimate of the number of times an element may be in a Cuckoo Filter.
// For more information - https://redis.io/commands/cf.count/
func (c cmdable) CFCOUNT(ctx context.Context, key string, element interface{}) *IntCmd {
	args := []interface{}{"cf.count", key, element}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFDEL deletes an item once from the cuckoo filter.
// For more information - https://redis.io/commands/cf.del/
func (c cmdable) CFDEL(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"cf.del", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFEXISTS determines whether an item may exist in the Cuckoo Filter or not.
// For more information - https://redis.io/commands/cf.exists/
func (c cmdable) CFEXISTS(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"cf.exists", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFLOADCHUNK restores a filter previously saved using SCANDUMP.
// For more information - https://redis.io/commands/cf.loadchunk/
func (c cmdable) CFLOADCHUNK(ctx context.Context, key string, iterator int64, data interface{}) *StatusCmd {
	args := []interface{}{"cf.loadchunk", key, iterator, data}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFSCANDUMP begins an incremental save of the cuckoo filter.
// For more information - https://redis.io/commands/cf.scandump/
func (c cmdable) CFSCANDUMP(ctx context.Context, key string, iterator int64) *ScanDumpCmd {
	args := []interface{}{"cf.scandump", key, iterator}
	cmd := newScanDumpCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type CFINFO struct {
	Size             int64
	NumBuckets       int64
	NumFilters       int64
	NumItemsInserted int64
	NumItemsDeleted  int64
	BucketSize       int64
	ExpansionRate    int64
	MaxIteration     int64
}

type CFINFOCmd struct {
	baseCmd

	val CFINFO
}

func NewCFINFOCmd(ctx context.Context, args ...interface{}) *CFINFOCmd {
	return &CFINFOCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *CFINFOCmd) SetVal(val CFINFO) {
	cmd.val = val
}

func (cmd *CFINFOCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *CFINFOCmd) Val() CFINFO {
	return cmd.val
}

func (cmd *CFINFOCmd) Result() (CFINFO, error) {
	return cmd.val, cmd.err
}

func (cmd *CFINFOCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}

	var key string
	var result CFINFO
	for f := 0; f < n; f++ {
		key, err = rd.ReadString()
		if err != nil {
			return err
		}

		switch key {
		case "Size":
			result.Size, err = rd.ReadInt()
		case "Number of buckets":
			result.NumBuckets, err = rd.ReadInt()
		case "Number of filters":
			result.NumFilters, err = rd.ReadInt()
		case "Number of items inserted":
			result.NumItemsInserted, err = rd.ReadInt()
		case "Number of items deleted":
			result.NumItemsDeleted, err = rd.ReadInt()
		case "Bucket size":
			result.BucketSize, err = rd.ReadInt()
		case "Expansion rate":
			result.ExpansionRate, err = rd.ReadInt()
		case "Max iterations":
			result.MaxIteration, err = rd.ReadInt()

		default:
			return fmt.Errorf("redis: cf.info unexpected key %s", key)
		}

		if err != nil {
			return err
		}
	}

	cmd.val = result
	return nil
}

// CFINFO returns information about a Cuckoo filter.
// For more information - https://redis.io/commands/cf.info/
func (c cmdable) CFINFO(ctx context.Context, key string) *CFINFOCmd {
	args := []interface{}{"cf.info", key}
	cmd := NewCFINFOCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFINSERT inserts elements into a Cuckoo filter.
// This function also allows for specifying additional options such as capacity, error rate, expansion rate, and non-scaling behavior.
// Returns an array of booleans indicating whether each element was added to the filter or not.
// For more information - https://redis.io/commands/cf.insert/
func (c cmdable) CFINSERT(ctx context.Context, key string, options *CFINSERTOptions, elements ...interface{}) *BoolSliceCmd {
	args := []interface{}{"cf.insert", key}
	args = c.getCfInsertArgs(args, options, elements...)

	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CFINSERTNX inserts elements into a Cuckoo filter only if they do not already exist in the filter.
// This function also allows for specifying additional options such as:
// capacity, error rate, expansion rate, and non-scaling behavior.
// Returns an array of integers indicating whether each element was added to the filter or not.
// For more information - https://redis.io/commands/cf.insertnx/
func (c cmdable) CFINSERTNX(ctx context.Context, key string, options *CFINSERTOptions, elements ...interface{}) *IntSliceCmd {
	args := []interface{}{"cf.insertnx", key}
	args = c.getCfInsertArgs(args, options, elements...)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) getCfInsertArgs(args []interface{}, options *CFINSERTOptions, elements ...interface{}) []interface{} {
	if options != nil {
		if options.Capacity != 0 {
			args = append(args, "capacity", options.Capacity)
		}
		if options.NoCreate {
			args = append(args, "nocreate")
		}
	}
	args = append(args, "items")
	args = append(args, elements...)

	return args
}

// CFMEXISTS check if multiple elements exist in a Cuckoo filter.
// Returns an array of booleans indicating whether each element exists in the filter or not.
// For more information - https://redis.io/commands/cf.mexists/
func (c cmdable) CFMEXISTS(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd {
	args := []interface{}{"cf.mexists", key}
	args = append(args, elements...)
	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// -------------------------------------------
// CMS commands
//-------------------------------------------

// CMSINCRBY increments the count of one or more items in a Count-Min Sketch filter.
// Returns an array of integers representing the updated count of each item.
// For more information - https://redis.io/commands/cms.incrby/
func (c cmdable) CMSINCRBY(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "cms.incrby"
	args[1] = key
	args = appendArgs(args, elements)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type CMSINFO struct {
	Width int64
	Depth int64
	Count int64
}

type CMSINFOCmd struct {
	baseCmd

	val CMSINFO
}

func NewCMSINFOCmd(ctx context.Context, args ...interface{}) *CMSINFOCmd {
	return &CMSINFOCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *CMSINFOCmd) SetVal(val CMSINFO) {
	cmd.val = val
}

func (cmd *CMSINFOCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *CMSINFOCmd) Val() CMSINFO {
	return cmd.val
}

func (cmd *CMSINFOCmd) Result() (CMSINFO, error) {
	return cmd.val, cmd.err
}

func (cmd *CMSINFOCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}

	var key string
	var result CMSINFO
	for f := 0; f < n; f++ {
		key, err = rd.ReadString()
		if err != nil {
			return err
		}

		switch key {
		case "width":
			result.Width, err = rd.ReadInt()
		case "depth":
			result.Depth, err = rd.ReadInt()
		case "count":
			result.Count, err = rd.ReadInt()
		default:
			return fmt.Errorf("redis: cms.info unexpected key %s", key)
		}

		if err != nil {
			return err
		}
	}

	cmd.val = result
	return nil
}

// CMSINFO returns information about a Count-Min Sketch filter.
// For more information - https://redis.io/commands/cms.info/
func (c cmdable) CMSINFO(ctx context.Context, key string) *CMSINFOCmd {
	args := []interface{}{"cms.info", key}
	cmd := NewCMSINFOCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CMSINITBYDIM creates an empty Count-Min Sketch filter with the specified dimensions.
// For more information - https://redis.io/commands/cms.initbydim/
func (c cmdable) CMSINITBYDIM(ctx context.Context, key string, width, depth int64) *StatusCmd {
	args := []interface{}{"cms.initbydim", key, width, depth}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CMSINITBYPROB creates an empty Count-Min Sketch filter with the specified error rate and probability.
// For more information - https://redis.io/commands/cms.initbyprob/
func (c cmdable) CMSINITBYPROB(ctx context.Context, key string, errorRate, probability float64) *StatusCmd {
	args := []interface{}{"cms.initbyprob", key, errorRate, probability}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CMSMERGE merges multiple Count-Min Sketch filters into a single filter.
// The destination filter must not exist and will be created with the dimensions of the first source filter.
// The number of items in each source filter must be equal.
// Returns OK on success or an error if the filters could not be merged.
// For more information - https://redis.io/commands/cms.merge/
func (c cmdable) CMSMERGE(ctx context.Context, destKey string, sourceKeys ...string) *StatusCmd {
	args := []interface{}{"cms.merge", destKey, len(sourceKeys)}
	for _, s := range sourceKeys {
		args = append(args, s)
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CMSMERGEWITHWEIGHT merges multiple Count-Min Sketch filters into a single filter with weights for each source filter.
// The destination filter must not exist and will be created with the dimensions of the first source filter.
// The number of items in each source filter must be equal.
// Returns OK on success or an error if the filters could not be merged.
// For more information - https://redis.io/commands/cms.merge/
func (c cmdable) CMSMERGEWITHWEIGHT(ctx context.Context, destKey string, sourceKeys map[string]int64) *StatusCmd {
	args := make([]interface{}, 0, 4+(len(sourceKeys)*2+1))
	args = append(args, "cms.merge", destKey, len(sourceKeys))

	if len(sourceKeys) > 0 {
		sk := make([]interface{}, len(sourceKeys))
		sw := make([]interface{}, len(sourceKeys))

		i := 0
		for k, w := range sourceKeys {
			sk[i] = k
			sw[i] = w
			i++
		}

		args = append(args, sk...)
		args = append(args, "weights")
		args = append(args, sw...)
	}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// CMSQUERY returns count for item(s).
// For more information - https://redis.io/commands/cms.query/
func (c cmdable) CMSQUERY(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd {
	args := []interface{}{"cms.query", key}
	args = append(args, elements...)
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// -------------------------------------------
// TopK commands
//--------------------------------------------

// TOPKADD adds one or more elements to a Top-K filter.
// Returns an array of strings representing the items that were removed from the filter, if any.
// For more information - https://redis.io/commands/topk.add/
func (c cmdable) TOPKADD(ctx context.Context, key string, elements ...interface{}) *StringSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "topk.add"
	args[1] = key
	args = appendArgs(args, elements)

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TOPKRESERVE creates an empty Top-K filter with the specified number of top items to keep.
// For more information - https://redis.io/commands/topk.reserve/
func (c cmdable) TOPKRESERVE(ctx context.Context, key string, k int64) *StatusCmd {
	args := []interface{}{"topk.reserve", key, k}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TOPKRESERVEWITHOPTIONS creates an empty Top-K filter with the specified number of top items to keep and additional options.
// This function allows for specifying additional options such as width, depth and decay.
// For more information - https://redis.io/commands/topk.reserve/
func (c cmdable) TOPKRESERVEWITHOPTIONS(ctx context.Context, key string, k int64, width, depth int64, decay float64) *StatusCmd {
	args := []interface{}{"topk.reserve", key, k, width, depth, decay}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type TOPKINFO struct {
	K     int64
	Width int64
	Depth int64
	Decay float64
}

type TOPKINFOCmd struct {
	baseCmd

	val TOPKINFO
}

func NewTOPKINFOCmd(ctx context.Context, args ...interface{}) *TOPKINFOCmd {
	return &TOPKINFOCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *TOPKINFOCmd) SetVal(val TOPKINFO) {
	cmd.val = val
}

func (cmd *TOPKINFOCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *TOPKINFOCmd) Val() TOPKINFO {
	return cmd.val
}

func (cmd *TOPKINFOCmd) Result() (TOPKINFO, error) {
	return cmd.val, cmd.err
}

func (cmd *TOPKINFOCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}

	var key string
	var result TOPKINFO
	for f := 0; f < n; f++ {
		key, err = rd.ReadString()
		if err != nil {
			return err
		}

		switch key {
		case "k":
			result.K, err = rd.ReadInt()
		case "width":
			result.Width, err = rd.ReadInt()
		case "depth":
			result.Depth, err = rd.ReadInt()
		case "decay":
			result.Decay, err = rd.ReadFloat()
		default:
			return fmt.Errorf("redis: topk.info unexpected key %s", key)
		}

		if err != nil {
			return err
		}
	}

	cmd.val = result
	return nil
}

// TOPKINFO returns information about a Top-K filter.
// For more information - https://redis.io/commands/topk.info/
func (c cmdable) TOPKINFO(ctx context.Context, key string) *TOPKINFOCmd {
	args := []interface{}{"topk.info", key}

	cmd := NewTOPKINFOCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TOPKQUERY check if multiple elements exist in a Top-K filter.
// Returns an array of booleans indicating whether each element exists in the filter or not.
// For more information - https://redis.io/commands/topk.query/
func (c cmdable) TOPKQUERY(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "topk.query"
	args[1] = key
	args = appendArgs(args, elements)

	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TOPKCOUNT returns an estimate of the number of times an item may be in a Top-K filter.
// For more information - https://redis.io/commands/topk.count/
func (c cmdable) TOPKCOUNT(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "topk.count"
	args[1] = key
	args = appendArgs(args, elements)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TOPKINCRBY increases the count of one or more items in a Top-K filter.
// For more information - https://redis.io/commands/topk.incrby/
func (c cmdable) TOPKINCRBY(ctx context.Context, key string, elements ...interface{}) *StringSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "topk.incrby"
	args[1] = key
	args = appendArgs(args, elements)

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TOPKLIST returns all items in Top-K list.
// For more information - https://redis.io/commands/topk.list/
func (c cmdable) TOPKLIST(ctx context.Context, key string) *StringSliceCmd {
	args := []interface{}{"topk.list", key}

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TOPKLISTWITHCOUNT returns all items in Top-K list with their respective count.
// For more information - https://redis.io/commands/topk.list/
func (c cmdable) TOPKLISTWITHCOUNT(ctx context.Context, key string) *MapStringIntCmd {
	args := []interface{}{"topk.list", key, "withcount"}

	cmd := NewMapStringIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// -------------------------------------------
// t-digest commands
// --------------------------------------------

// TDIGESTADD adds one or more elements to a t-Digest data structure.
// Returns OK on success or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.add/
func (c cmdable) TDIGESTADD(ctx context.Context, key string, elements ...float64) *StatusCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "tdigest.add"
	args[1] = key

	// Convert floatSlice to []interface{}
	interfaceSlice := make([]interface{}, len(elements))
	for i, v := range elements {
		interfaceSlice[i] = v
	}

	args = append(args, interfaceSlice...)

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTBYRANK returns an array of values from a t-Digest data structure based on their rank.
// The rank of an element is its position in the sorted list of all elements in the t-Digest.
// Returns an array of floats representing the values at the specified ranks or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.byrank/
func (c cmdable) TDIGESTBYRANK(ctx context.Context, key string, rank ...uint64) *FloatSliceCmd {
	args := make([]interface{}, 2, 2+len(rank))
	args[0] = "tdigest.byrank"
	args[1] = key

	// Convert uint slice to []interface{}
	interfaceSlice := make([]interface{}, len(rank))
	for i, v := range rank {
		interfaceSlice[i] = v
	}

	args = append(args, interfaceSlice...)

	cmd := NewFloatSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTBYREVRANK returns an array of values from a t-Digest data structure based on their reverse rank.
// The reverse rank of an element is its position in the sorted list of all elements in the t-Digest when sorted in descending order.
// Returns an array of floats representing the values at the specified ranks or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.byrevrank/
func (c cmdable) TDIGESTBYREVRANK(ctx context.Context, key string, rank ...uint64) *FloatSliceCmd {
	args := make([]interface{}, 2, 2+len(rank))
	args[0] = "tdigest.byrevrank"
	args[1] = key

	// Convert uint slice to []interface{}
	interfaceSlice := make([]interface{}, len(rank))
	for i, v := range rank {
		interfaceSlice[i] = v
	}

	args = append(args, interfaceSlice...)

	cmd := NewFloatSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTCDF returns an array of cumulative distribution function (CDF) values for one or more elements in a t-Digest data structure.
// The CDF value for an element is the fraction of all elements in the t-Digest that are less than or equal to it.
// Returns an array of floats representing the CDF values for each element or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.cdf/
func (c cmdable) TDIGESTCDF(ctx context.Context, key string, elements ...float64) *FloatSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "tdigest.cdf"
	args[1] = key

	// Convert floatSlice to []interface{}
	interfaceSlice := make([]interface{}, len(elements))
	for i, v := range elements {
		interfaceSlice[i] = v
	}

	args = append(args, interfaceSlice...)

	cmd := NewFloatSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTCREATE creates an empty t-Digest data structure with default parameters.
// Returns OK on success or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.create/
func (c cmdable) TDIGESTCREATE(ctx context.Context, key string) *StatusCmd {
	args := []interface{}{"tdigest.create", key}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTCREATEWITHCOMPRESSION creates an empty t-Digest data structure with a specified compression parameter.
// The compression parameter controls the accuracy and memory usage of the t-Digest.
// Returns OK on success or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.create/
func (c cmdable) TDIGESTCREATEWITHCOMPRESSION(ctx context.Context, key string, compression int64) *StatusCmd {
	args := []interface{}{"tdigest.create", key, "compression", compression}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type TDIGESTINFO struct {
	Compression       int64
	Capacity          int64
	MergedNodes       int64
	UnmergedNodes     int64
	MergedWeight      int64
	UnmergedWeight    int64
	Observations      int64
	TotalCompressions int64
	MemoryUsage       int64
}

type TDIGESTINFOCmd struct {
	baseCmd

	val TDIGESTINFO
}

func NewTDIGESTINFOCmd(ctx context.Context, args ...interface{}) *TDIGESTINFOCmd {
	return &TDIGESTINFOCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *TDIGESTINFOCmd) SetVal(val TDIGESTINFO) {
	cmd.val = val
}

func (cmd *TDIGESTINFOCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *TDIGESTINFOCmd) Val() TDIGESTINFO {
	return cmd.val
}

func (cmd *TDIGESTINFOCmd) Result() (TDIGESTINFO, error) {
	return cmd.val, cmd.err
}

func (cmd *TDIGESTINFOCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}

	var key string
	var result TDIGESTINFO
	for f := 0; f < n; f++ {
		key, err = rd.ReadString()
		if err != nil {
			return err
		}

		switch key {
		case "Compression":
			result.Compression, err = rd.ReadInt()
		case "Capacity":
			result.Capacity, err = rd.ReadInt()
		case "Merged nodes":
			result.MergedNodes, err = rd.ReadInt()
		case "Unmerged nodes":
			result.UnmergedNodes, err = rd.ReadInt()
		case "Merged weight":
			result.MergedWeight, err = rd.ReadInt()
		case "Unmerged weight":
			result.UnmergedWeight, err = rd.ReadInt()
		case "Observations":
			result.Observations, err = rd.ReadInt()
		case "Total compressions":
			result.TotalCompressions, err = rd.ReadInt()
		case "Memory usage":
			result.MemoryUsage, err = rd.ReadInt()
		default:
			return fmt.Errorf("redis: tdigest.info unexpected key %s", key)
		}

		if err != nil {
			return err
		}
	}

	cmd.val = result
	return nil
}

// TDIGESTINFO returns information about a t-Digest data structure.
// For more information - https://redis.io/commands/tdigest.info/
func (c cmdable) TDIGESTINFO(ctx context.Context, key string) *TDIGESTINFOCmd {
	args := []interface{}{"tdigest.info", key}

	cmd := NewTDIGESTINFOCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTMAX returns the maximum value from a t-Digest data structure.
// For more information - https://redis.io/commands/tdigest.max/
func (c cmdable) TDIGESTMAX(ctx context.Context, key string) *FloatCmd {
	args := []interface{}{"tdigest.max", key}

	cmd := NewFloatCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type TDIGESTMERGEOptions struct {
	Compression int64
	Override    bool
}

// TDIGESTMERGE merges multiple t-Digest data structures into a single t-Digest.
// This function also allows for specifying additional options such as compression and override behavior.
// Returns OK on success or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.merge/
func (c cmdable) TDIGESTMERGE(ctx context.Context, destKey string, options *TDIGESTMERGEOptions, sourceKeys ...string) *StatusCmd {
	args := []interface{}{"tdigest.merge", destKey, len(sourceKeys)}

	for _, sourceKey := range sourceKeys {
		args = append(args, sourceKey)
	}

	if options != nil {
		if options.Compression != 0 {
			args = append(args, "COMPRESSION", options.Compression)
		}
		if options.Override {
			args = append(args, "OVERRIDE")
		}
	}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTMIN returns the minimum value from a t-Digest data structure.
// For more information - https://redis.io/commands/tdigest.min/
func (c cmdable) TDIGESTMIN(ctx context.Context, key string) *FloatCmd {
	args := []interface{}{"tdigest.min", key}

	cmd := NewFloatCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTQUANTILE returns an array of quantile values for one or more elements in a t-Digest data structure.
// The quantile value for an element is the fraction of all elements in the t-Digest that are less than or equal to it.
// Returns an array of floats representing the quantile values for each element or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.quantile/
func (c cmdable) TDIGESTQUANTILE(ctx context.Context, key string, elements ...float64) *FloatSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "tdigest.quantile"
	args[1] = key

	// Convert floatSlice to []interface{}
	interfaceSlice := make([]interface{}, len(elements))
	for i, v := range elements {
		interfaceSlice[i] = v
	}

	args = append(args, interfaceSlice...)

	cmd := NewFloatSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTRANK returns an array of rank values for one or more elements in a t-Digest data structure.
// The rank of an element is its position in the sorted list of all elements in the t-Digest.
// Returns an array of integers representing the rank values for each element or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.rank/
func (c cmdable) TDIGESTRANK(ctx context.Context, key string, values ...float64) *IntSliceCmd {
	args := make([]interface{}, 2, 2+len(values))
	args[0] = "tdigest.rank"
	args[1] = key

	// Convert floatSlice to []interface{}
	interfaceSlice := make([]interface{}, len(values))
	for i, v := range values {
		interfaceSlice[i] = v
	}

	args = append(args, interfaceSlice...)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTRESET resets a t-Digest data structure to its initial state.
// Returns OK on success or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.reset/
func (c cmdable) TDIGESTRESET(ctx context.Context, key string) *StatusCmd {
	args := []interface{}{"tdigest.reset", key}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTREVRANK returns an array of reverse rank values for one or more elements in a t-Digest data structure.
// The reverse rank of an element is its position in the sorted list of all elements in the t-Digest when sorted in descending order.
// Returns an array of integers representing the reverse rank values for each element or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.revrank/
func (c cmdable) TDIGESTREVRANK(ctx context.Context, key string, values ...float64) *IntSliceCmd {
	args := make([]interface{}, 2, 2+len(values))
	args[0] = "tdigest.revrank"
	args[1] = key

	// Convert floatSlice to []interface{}
	interfaceSlice := make([]interface{}, len(values))
	for i, v := range values {
		interfaceSlice[i] = v
	}

	args = append(args, interfaceSlice...)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// TDIGESTTRIMMEDMEAN returns the trimmed mean value from a t-Digest data structure.
// The trimmed mean is calculated by removing a specified fraction of the highest and lowest values from the t-Digest and then calculating the mean of the remaining values.
// Returns a float representing the trimmed mean value or an error if the operation could not be completed.
// For more information - https://redis.io/commands/tdigest.trimmed_mean/
func (c cmdable) TDIGESTTRIMMEDMEAN(ctx context.Context, key string, lowCutQuantile, highCutQuantile float64) *FloatCmd {
	args := []interface{}{"tdigest.trimmed_mean", key, lowCutQuantile, highCutQuantile}

	cmd := NewFloatCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}
