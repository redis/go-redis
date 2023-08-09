package redis

import (
	"context"
	"fmt"

	"github.com/redis/go-redis/v9/internal/proto"
)

type probabilisticCmdable interface {
	BFAdd(ctx context.Context, key string, element interface{}) *BoolCmd
	BFCard(ctx context.Context, key string) *IntCmd
	BFExists(ctx context.Context, key string, element interface{}) *BoolCmd
	BFInfo(ctx context.Context, key string) *BFInfoCmd
	BFInfoCapacity(ctx context.Context, key string) *BFInfoCmd
	BFInfoSize(ctx context.Context, key string) *BFInfoCmd
	BFInfoFilters(ctx context.Context, key string) *BFInfoCmd
	BFInfoItems(ctx context.Context, key string) *BFInfoCmd
	BFInfoExpansion(ctx context.Context, key string) *BFInfoCmd
	BFInsert(ctx context.Context, key string, options *BFInsertOptions, elements ...interface{}) *BoolSliceCmd
	BFMAdd(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd
	BFMExists(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd
	BFReserve(ctx context.Context, key string, errorRate float64, capacity int64) *StatusCmd
	BFReserveExpansion(ctx context.Context, key string, errorRate float64, capacity, expansion int64) *StatusCmd
	BFReserveNonScaling(ctx context.Context, key string, errorRate float64, capacity int64) *StatusCmd
	BFReserveArgs(ctx context.Context, key string, options *BFReserveOptions) *StatusCmd
	BFScanDump(ctx context.Context, key string, iterator int64) *ScanDumpCmd
	BFLoadChunk(ctx context.Context, key string, iterator int64, data interface{}) *StatusCmd

	CFAdd(ctx context.Context, key string, element interface{}) *BoolCmd
	CFAddNX(ctx context.Context, key string, element interface{}) *BoolCmd
	CFCount(ctx context.Context, key string, element interface{}) *IntCmd
	CFDel(ctx context.Context, key string, element interface{}) *BoolCmd
	CFExists(ctx context.Context, key string, element interface{}) *BoolCmd
	CFInfo(ctx context.Context, key string) *CFInfoCmd
	CFInsert(ctx context.Context, key string, options *CFInsertOptions, elements ...interface{}) *BoolSliceCmd
	CFInsertNx(ctx context.Context, key string, options *CFInsertOptions, elements ...interface{}) *IntSliceCmd
	CFMExists(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd
	CFReserve(ctx context.Context, key string, capacity int64) *StatusCmd
	CFReserveArgs(ctx context.Context, key string, options *CFReserveOptions) *StatusCmd
	CFScanDump(ctx context.Context, key string, iterator int64) *ScanDumpCmd
	CFLoadChunk(ctx context.Context, key string, iterator int64, data interface{}) *StatusCmd

	CMSIncrBy(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd
	CMSInfo(ctx context.Context, key string) *CMSInfoCmd
	CMSInitByDim(ctx context.Context, key string, width, height int64) *StatusCmd
	CMSInitByProb(ctx context.Context, key string, errorRate, probability float64) *StatusCmd
	CMSMerge(ctx context.Context, destKey string, sourceKeys ...string) *StatusCmd
	CMSMergeWithWeight(ctx context.Context, destKey string, sourceKeys map[string]int64) *StatusCmd
	CMSQuery(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd

	TopKAdd(ctx context.Context, key string, elements ...interface{}) *StringSliceCmd
	TopKCount(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd
	TopKIncrBy(ctx context.Context, key string, elements ...interface{}) *StringSliceCmd
	TopKInfo(ctx context.Context, key string) *TopKInfoCmd
	TopKList(ctx context.Context, key string) *StringSliceCmd
	TopKListWithCount(ctx context.Context, key string) *MapStringIntCmd
	TopKQuery(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd
	TopKReserve(ctx context.Context, key string, k int64) *StatusCmd
	TopKReserveWithOptions(ctx context.Context, key string, k int64, width, depth int64, decay float64) *StatusCmd

	TDigestAdd(ctx context.Context, key string, elements ...float64) *StatusCmd
	TDigestByRank(ctx context.Context, key string, rank ...uint64) *FloatSliceCmd
	TDigestByRevRank(ctx context.Context, key string, rank ...uint64) *FloatSliceCmd
	TDigestCDF(ctx context.Context, key string, elements ...float64) *FloatSliceCmd
	TDigestCreate(ctx context.Context, key string) *StatusCmd
	TDigestCreateWithCompression(ctx context.Context, key string, compression int64) *StatusCmd
	TDigestInfo(ctx context.Context, key string) *TDigestInfoCmd
	TDigestMax(ctx context.Context, key string) *FloatCmd
	TDigestMin(ctx context.Context, key string) *FloatCmd
	TDigestQuantile(ctx context.Context, key string, elements ...float64) *FloatSliceCmd
	TDigestRank(ctx context.Context, key string, values ...float64) *IntSliceCmd
	TDigestReset(ctx context.Context, key string) *StatusCmd
	TDigestRevRank(ctx context.Context, key string, values ...float64) *IntSliceCmd
	TDigestTrimmedMean(ctx context.Context, key string, lowCutQuantile, highCutQuantile float64) *FloatCmd
}

type BFInsertOptions struct {
	Capacity   int64
	Error      float64
	Expansion  int64
	NonScaling bool
	NoCreate   bool
}

type BFReserveOptions struct {
	Capacity   int64
	Error      float64
	Expansion  int64
	NonScaling bool
}

type CFReserveOptions struct {
	Capacity      int64
	BucketSize    int64
	MaxIterations int64
	Expansion     int64
}

type CFInsertOptions struct {
	Capacity int64
	NoCreate bool
}

// -------------------------------------------
// Bloom filter commands
//-------------------------------------------

func (c cmdable) BFReserve(ctx context.Context, key string, errorRate float64, capacity int64) *StatusCmd {
	args := []interface{}{"bf.reserve", key, errorRate, capacity}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFReserveExpansion(ctx context.Context, key string, errorRate float64, capacity, expansion int64) *StatusCmd {
	args := []interface{}{"bf.reserve", key, errorRate, capacity, "expansion", expansion}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFReserveNonScaling(ctx context.Context, key string, errorRate float64, capacity int64) *StatusCmd {
	args := []interface{}{"bf.reserve", key, errorRate, capacity, "nonscaling"}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFReserveArgs(ctx context.Context, key string, options *BFReserveOptions) *StatusCmd {
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

func (c cmdable) BFAdd(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"bf.add", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFCard(ctx context.Context, key string) *IntCmd {
	args := []interface{}{"bf.card", key}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFExists(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"bf.exists", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFLoadChunk(ctx context.Context, key string, iterator int64, data interface{}) *StatusCmd {
	args := []interface{}{"bf.loadchunk", key, iterator, data}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFScanDump(ctx context.Context, key string, iterator int64) *ScanDumpCmd {
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

func (c cmdable) BFInfo(ctx context.Context, key string) *BFInfoCmd {
	args := []interface{}{"bf.info", key}
	cmd := NewBFInfoCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type BFInfo struct {
	Capacity      int64
	Size          int64
	Filters       int64
	ItemsInserted int64
	ExpansionRate int64
}

type BFInfoCmd struct {
	baseCmd

	val BFInfo
}

func NewBFInfoCmd(ctx context.Context, args ...interface{}) *BFInfoCmd {
	return &BFInfoCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *BFInfoCmd) SetVal(val BFInfo) {
	cmd.val = val
}
func (cmd *BFInfoCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *BFInfoCmd) Val() BFInfo {
	return cmd.val
}

func (cmd *BFInfoCmd) Result() (BFInfo, error) {
	return cmd.val, cmd.err
}

func (cmd *BFInfoCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}

	var key string
	var result BFInfo
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

func (c cmdable) BFInfoCapacity(ctx context.Context, key string) *BFInfoCmd {
	return c.bFInfoArg(ctx, key, "capacity")
}

func (c cmdable) BFInfoSize(ctx context.Context, key string) *BFInfoCmd {
	return c.bFInfoArg(ctx, key, "size")
}

func (c cmdable) BFInfoFilters(ctx context.Context, key string) *BFInfoCmd {
	return c.bFInfoArg(ctx, key, "filters")
}

func (c cmdable) BFInfoItems(ctx context.Context, key string) *BFInfoCmd {
	return c.bFInfoArg(ctx, key, "items")
}

func (c cmdable) BFInfoExpansion(ctx context.Context, key string) *BFInfoCmd {
	return c.bFInfoArg(ctx, key, "expansion")
}

func (c cmdable) bFInfoArg(ctx context.Context, key, option string) *BFInfoCmd {
	args := []interface{}{"bf.info", key, option}
	cmd := NewBFInfoCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFInsert(ctx context.Context, key string, options *BFInsertOptions, elements ...interface{}) *BoolSliceCmd {
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

func (c cmdable) BFMAdd(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd {
	args := []interface{}{"bf.madd", key}
	args = append(args, elements...)
	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFMExists(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd {
	args := []interface{}{"bf.mexists", key}
	args = append(args, elements...)

	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// -------------------------------------------
// Cuckoo filter commands
//-------------------------------------------

func (c cmdable) CFReserve(ctx context.Context, key string, capacity int64) *StatusCmd {
	args := []interface{}{"cf.reserve", key, capacity}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFReserveExpansion(ctx context.Context, key string, capacity int64, expansion int64) *StatusCmd {
	args := []interface{}{"cf.reserve", key, capacity, "expansion", expansion}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFReserveBucketsize(ctx context.Context, key string, capacity int64, bucketsize int64) *StatusCmd {
	args := []interface{}{"cf.reserve", key, capacity, "bucketsize", bucketsize}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFReserveMaxiterations(ctx context.Context, key string, capacity int64, maxiterations int64) *StatusCmd {
	args := []interface{}{"cf.reserve", key, capacity, "maxiterations", maxiterations}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFReserveArgs(ctx context.Context, key string, options *CFReserveOptions) *StatusCmd {
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

func (c cmdable) CFAdd(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"cf.add", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFAddNX(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"cf.addnx", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFCount(ctx context.Context, key string, element interface{}) *IntCmd {
	args := []interface{}{"cf.count", key, element}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFDel(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"cf.del", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFExists(ctx context.Context, key string, element interface{}) *BoolCmd {
	args := []interface{}{"cf.exists", key, element}
	cmd := NewBoolCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFLoadChunk(ctx context.Context, key string, iterator int64, data interface{}) *StatusCmd {
	args := []interface{}{"cf.loadchunk", key, iterator, data}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFScanDump(ctx context.Context, key string, iterator int64) *ScanDumpCmd {
	args := []interface{}{"cf.scandump", key, iterator}
	cmd := newScanDumpCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type CFInfo struct {
	Size             int64
	NumBuckets       int64
	NumFilters       int64
	NumItemsInserted int64
	NumItemsDeleted  int64
	BucketSize       int64
	ExpansionRate    int64
	MaxIteration     int64
}

type CFInfoCmd struct {
	baseCmd

	val CFInfo
}

func NewCFInfoCmd(ctx context.Context, args ...interface{}) *CFInfoCmd {
	return &CFInfoCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *CFInfoCmd) SetVal(val CFInfo) {
	cmd.val = val
}

func (cmd *CFInfoCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *CFInfoCmd) Val() CFInfo {
	return cmd.val
}

func (cmd *CFInfoCmd) Result() (CFInfo, error) {
	return cmd.val, cmd.err
}

func (cmd *CFInfoCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}

	var key string
	var result CFInfo
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

func (c cmdable) CFInfo(ctx context.Context, key string) *CFInfoCmd {
	args := []interface{}{"cf.info", key}
	cmd := NewCFInfoCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFInsert(ctx context.Context, key string, options *CFInsertOptions, elements ...interface{}) *BoolSliceCmd {
	args := []interface{}{"cf.insert", key}
	args = c.getCfInsertArgs(args, options, elements...)

	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFInsertNx(ctx context.Context, key string, options *CFInsertOptions, elements ...interface{}) *IntSliceCmd {
	args := []interface{}{"cf.insertnx", key}
	args = c.getCfInsertArgs(args, options, elements...)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) getCfInsertArgs(args []interface{}, options *CFInsertOptions, elements ...interface{}) []interface{} {
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

func (c cmdable) CFMExists(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd {
	args := []interface{}{"cf.mexists", key}
	args = append(args, elements...)
	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// -------------------------------------------
// CMS commands
//-------------------------------------------

func (c cmdable) CMSIncrBy(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "cms.incrby"
	args[1] = key
	args = appendArgs(args, elements)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type CMSInfo struct {
	Width int64
	Depth int64
	Count int64
}

type CMSInfoCmd struct {
	baseCmd

	val CMSInfo
}

func NewCMSInfoCmd(ctx context.Context, args ...interface{}) *CMSInfoCmd {
	return &CMSInfoCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *CMSInfoCmd) SetVal(val CMSInfo) {
	cmd.val = val
}

func (cmd *CMSInfoCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *CMSInfoCmd) Val() CMSInfo {
	return cmd.val
}

func (cmd *CMSInfoCmd) Result() (CMSInfo, error) {
	return cmd.val, cmd.err
}

func (cmd *CMSInfoCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}

	var key string
	var result CMSInfo
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

func (c cmdable) CMSInfo(ctx context.Context, key string) *CMSInfoCmd {
	args := []interface{}{"cms.info", key}
	cmd := NewCMSInfoCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CMSInitByDim(ctx context.Context, key string, width, depth int64) *StatusCmd {
	args := []interface{}{"cms.initbydim", key, width, depth}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CMSInitByProb(ctx context.Context, key string, errorRate, probability float64) *StatusCmd {
	args := []interface{}{"cms.initbyprob", key, errorRate, probability}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CMSMerge(ctx context.Context, destKey string, sourceKeys ...string) *StatusCmd {
	args := []interface{}{"cms.merge", destKey, len(sourceKeys)}
	for _, s := range sourceKeys {
		args = append(args, s)
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CMSMergeWithWeight(ctx context.Context, destKey string, sourceKeys map[string]int64) *StatusCmd {
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

func (c cmdable) CMSQuery(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd {
	args := []interface{}{"cms.query", key}
	args = append(args, elements...)
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// -------------------------------------------
// TopK commands
//--------------------------------------------

func (c cmdable) TopKAdd(ctx context.Context, key string, elements ...interface{}) *StringSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "topk.add"
	args[1] = key
	args = appendArgs(args, elements)

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TopKReserve(ctx context.Context, key string, k int64) *StatusCmd {
	args := []interface{}{"topk.reserve", key, k}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TopKReserveWithOptions(ctx context.Context, key string, k int64, width, depth int64, decay float64) *StatusCmd {
	args := []interface{}{"topk.reserve", key, k, width, depth, decay}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type TopKInfo struct {
	K     int64
	Width int64
	Depth int64
	Decay float64
}

type TopKInfoCmd struct {
	baseCmd

	val TopKInfo
}

func NewTopKInfoCmd(ctx context.Context, args ...interface{}) *TopKInfoCmd {
	return &TopKInfoCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *TopKInfoCmd) SetVal(val TopKInfo) {
	cmd.val = val
}

func (cmd *TopKInfoCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *TopKInfoCmd) Val() TopKInfo {
	return cmd.val
}

func (cmd *TopKInfoCmd) Result() (TopKInfo, error) {
	return cmd.val, cmd.err
}

func (cmd *TopKInfoCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}

	var key string
	var result TopKInfo
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

func (c cmdable) TopKInfo(ctx context.Context, key string) *TopKInfoCmd {
	args := []interface{}{"topk.info", key}

	cmd := NewTopKInfoCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TopKQuery(ctx context.Context, key string, elements ...interface{}) *BoolSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "topk.query"
	args[1] = key
	args = appendArgs(args, elements)

	cmd := NewBoolSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TopKCount(ctx context.Context, key string, elements ...interface{}) *IntSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "topk.count"
	args[1] = key
	args = appendArgs(args, elements)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TopKIncrBy(ctx context.Context, key string, elements ...interface{}) *StringSliceCmd {
	args := make([]interface{}, 2, 2+len(elements))
	args[0] = "topk.incrby"
	args[1] = key
	args = appendArgs(args, elements)

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TopKList(ctx context.Context, key string) *StringSliceCmd {
	args := []interface{}{"topk.list", key}

	cmd := NewStringSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TopKListWithCount(ctx context.Context, key string) *MapStringIntCmd {
	args := []interface{}{"topk.list", key, "withcount"}

	cmd := NewMapStringIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// -------------------------------------------
// t-digest commands
// --------------------------------------------

func (c cmdable) TDigestAdd(ctx context.Context, key string, elements ...float64) *StatusCmd {
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

func (c cmdable) TDigestByRank(ctx context.Context, key string, rank ...uint64) *FloatSliceCmd {
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

func (c cmdable) TDigestByRevRank(ctx context.Context, key string, rank ...uint64) *FloatSliceCmd {
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

func (c cmdable) TDigestCDF(ctx context.Context, key string, elements ...float64) *FloatSliceCmd {
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

func (c cmdable) TDigestCreate(ctx context.Context, key string) *StatusCmd {
	args := []interface{}{"tdigest.create", key}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TDigestCreateWithCompression(ctx context.Context, key string, compression int64) *StatusCmd {
	args := []interface{}{"tdigest.create", key, "compression", compression}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type TDigestInfo struct {
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

type TDigestInfoCmd struct {
	baseCmd

	val TDigestInfo
}

func NewTDigestInfoCmd(ctx context.Context, args ...interface{}) *TDigestInfoCmd {
	return &TDigestInfoCmd{
		baseCmd: baseCmd{
			ctx:  ctx,
			args: args,
		},
	}
}

func (cmd *TDigestInfoCmd) SetVal(val TDigestInfo) {
	cmd.val = val
}

func (cmd *TDigestInfoCmd) String() string {
	return cmdString(cmd, cmd.val)
}

func (cmd *TDigestInfoCmd) Val() TDigestInfo {
	return cmd.val
}

func (cmd *TDigestInfoCmd) Result() (TDigestInfo, error) {
	return cmd.val, cmd.err
}

func (cmd *TDigestInfoCmd) readReply(rd *proto.Reader) (err error) {
	n, err := rd.ReadMapLen()
	if err != nil {
		return err
	}

	var key string
	var result TDigestInfo
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

func (c cmdable) TDigestInfo(ctx context.Context, key string) *TDigestInfoCmd {
	args := []interface{}{"tdigest.info", key}

	cmd := NewTDigestInfoCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TDigestMax(ctx context.Context, key string) *FloatCmd {
	args := []interface{}{"tdigest.max", key}

	cmd := NewFloatCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type TDigestMergeOptions struct {
	Compression int64
	Override    bool
}

func (c cmdable) TDigestMerge(ctx context.Context, destKey string, options *TDigestMergeOptions, sourceKeys ...string) *StatusCmd {
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

func (c cmdable) TDigestMin(ctx context.Context, key string) *FloatCmd {
	args := []interface{}{"tdigest.min", key}

	cmd := NewFloatCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TDigestQuantile(ctx context.Context, key string, elements ...float64) *FloatSliceCmd {
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
func (c cmdable) TDigestRank(ctx context.Context, key string, values ...float64) *IntSliceCmd {
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

func (c cmdable) TDigestReset(ctx context.Context, key string) *StatusCmd {
	args := []interface{}{"tdigest.reset", key}

	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) TDigestRevRank(ctx context.Context, key string, values ...float64) *IntSliceCmd {
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

func (c cmdable) TDigestTrimmedMean(ctx context.Context, key string, lowCutQuantile, highCutQuantile float64) *FloatCmd {
	args := []interface{}{"tdigest.trimmed_mean", key, lowCutQuantile, highCutQuantile}

	cmd := NewFloatCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}
