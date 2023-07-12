package redis

import (
	"context"
)

type ProbabilisticCmdble interface {
	BFAdd(ctx context.Context, key, item interface{}) *IntCmd
	BFCard(ctx context.Context, key string) *IntCmd
	BFExists(ctx context.Context, key, item interface{}) *IntCmd
	BFInfo(ctx context.Context, key string) *MapStringIntCmd
	BFInfoArg(ctx context.Context, key string, option BFInfo) *IntCmd
	BFInsert(ctx context.Context, key string, options *BFReserveOptions, items ...interface{}) *IntSliceCmd
	BFMAdd(ctx context.Context, key string, items ...interface{}) *IntSliceCmd
	BFMExists(ctx context.Context, key string, items ...interface{}) *IntSliceCmd
	BFReserve(ctx context.Context, key string, errorRate float64, capacity int64) *StatusCmd
	BFReserveExpansion(ctx context.Context, key string, errorRate float64, capacity, expansion int64) *StatusCmd
	BFReserveNonScaling(ctx context.Context, key string, errorRate float64, capacity int64) *StatusCmd
	BFReserveArgs(ctx context.Context, key string, options *BFReserveOptions) *StatusCmd
	//TODO Loadchunk and scandump missing

	CFAdd(ctx context.Context, key, item interface{}) *IntCmd
	CFAddNX(ctx context.Context, key, item interface{}) *IntCmd
	CFCount(ctx context.Context, key, item interface{}) *IntCmd
	CFDel(ctx context.Context, key string) *IntCmd
	CFExists(ctx context.Context, key, item interface{}) *IntCmd
	CFInfo(ctx context.Context, key string) *MapStringStringCmd
	CFReserve(ctx context.Context, key string, capacity int64) *StatusCmd
	CFInsert(ctx context.Context, key string, options *CFInsertOptions, items ...interface{}) *IntSliceCmd
	CFInsertNx(ctx context.Context, key string, options *CFInsertOptions, items ...interface{}) *IntSliceCmd
	CFMExists(ctx context.Context, key string, items ...interface{}) *IntSliceCmd
	//TODO Loadchunk and scandump missing

	CMSIncrBy(ctx context.Context, key string, items ...interface{}) *IntSliceCmd
	CMSInitByDim(ctx context.Context, key string, width, height int64) *StatusCmd
	CMSInitByProb(ctx context.Context, key string, error_rate, probability float64) *StatusCmd
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

type BFInfo int

const (
	BFCAPACITY BFInfo = iota
	BFSIZE
	BFFILTERS
	BFITEMS
	BFEXPANSION
)

func (b BFInfo) String() string {
	switch b {
	case BFCAPACITY:
		return "capacity"
	case BFSIZE:
		return "size"
	case BFFILTERS:
		return "filters"
	case BFITEMS:
		return "items"
	case BFEXPANSION:
		return "expansion"
	}
	return ""
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

func (c cmdable) BFAdd(ctx context.Context, key, item interface{}) *IntCmd {
	args := []interface{}{"bf.add", key, item}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFCard(ctx context.Context, key string) *IntCmd {
	args := []interface{}{"bf.card", key}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFExists(ctx context.Context, key, item string) *IntCmd {
	args := []interface{}{"bf.exists", key, item}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFInfo(ctx context.Context, key string) *MapStringIntCmd {
	args := []interface{}{"bf.info", key}
	cmd := NewMapStringIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFInfoArg(ctx context.Context, key string, option BFInfo) *IntCmd {
	args := []interface{}{"bf.info", key, option.String()}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFInsert(ctx context.Context, key string, options *BFReserveOptions, items ...string) *IntSliceCmd {
	args := []interface{}{"bf.insert", key}
	if options != nil {
		if options.Error != 0 {
			args = append(args, "error", options.Error)
		}
		if options.Capacity != 0 {
			args = append(args, "capacity", options.Capacity)
		}
		if options.Expansion != 0 {
			args = append(args, "expansion", options.Expansion)
		}
		if options.NonScaling {
			args = append(args, "nonscaling")
		}
	}
	args = append(args, "items")
	for _, s := range items {
		args = append(args, s)
	}

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFMAdd(ctx context.Context, key string, items ...string) *IntSliceCmd {
	args := []interface{}{"bf.madd", key}
	for _, s := range items {
		args = append(args, s)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) BFMExists(ctx context.Context, key string, items ...string) *IntSliceCmd {
	args := []interface{}{"bf.mexists", key}
	for _, s := range items {
		args = append(args, s)
	}
	cmd := NewIntSliceCmd(ctx, args...)
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

func (c cmdable) CFAdd(ctx context.Context, key, item string) *IntCmd {
	args := []interface{}{"cf.add", key, item}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFAddNX(ctx context.Context, key, item string) *IntCmd {
	args := []interface{}{"cf.addnx", key, item}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFCount(ctx context.Context, key, item string) *IntCmd {
	args := []interface{}{"cf.count", key, item}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFDel(ctx context.Context, key string, item string) *IntCmd {
	args := []interface{}{"cf.del", key, item}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFExists(ctx context.Context, key, item string) *IntCmd {
	args := []interface{}{"cf.exists", key, item}
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFInfo(ctx context.Context, key string) *MapStringIntCmd {
	args := []interface{}{"cf.info", key}
	cmd := NewMapStringIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFInsert(ctx context.Context, key string, options *CFInsertOptions, items ...string) *IntSliceCmd {
	args := []interface{}{"cf.insert", key}
	args = c.getCfInsertArgs(args, options, items...)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CFInsertNx(ctx context.Context, key string, options *CFInsertOptions, items ...string) *IntSliceCmd {
	args := []interface{}{"cf.insertnx", key}
	args = c.getCfInsertArgs(args, options, items...)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) getCfInsertArgs(args []interface{}, options *CFInsertOptions, items ...string) []interface{} {
	if options != nil {
		if options.Capacity != 0 {
			args = append(args, "capacity", options.Capacity)
		}
		if options.NoCreate {
			args = append(args, "nocreate")
		}
	}
	args = append(args, "items")
	for _, s := range items {
		args = append(args, s)
	}
	return args
}

func (c cmdable) CFMExists(ctx context.Context, key string, items ...string) *IntSliceCmd {
	args := []interface{}{"cf.mexists", key}
	for _, s := range items {
		args = append(args, s)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// -------------------------------------------
// CMS commands
//-------------------------------------------

func (c cmdable) CMSIncrBy(ctx context.Context, key string, items ...interface{}) *IntSliceCmd {
	args := make([]interface{}, 2, 2+len(items))
	args[0] = "cms.incrby"
	args[1] = key
	args = appendArgs(args, items)

	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CMSInitByDim(ctx context.Context, key string, width, depth int64) *StatusCmd {
	args := []interface{}{"cms.initbydim", key, width, depth}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CMSInitByProb(ctx context.Context, key string, error_rate, probability float64) *StatusCmd {
	args := []interface{}{"cms.initbyprob", key, error_rate, probability}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) CMSInfo(ctx context.Context, key string) *MapStringIntCmd {
	args := []interface{}{"cms.info", key}
	cmd := NewMapStringIntCmd(ctx, args...)
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

func (c cmdable) CMSMergeWithWeight(ctx context.Context, destKey string, sourceKeys map[string]int) *StatusCmd {
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

func (c cmdable) CMSQuery(ctx context.Context, key string, items ...interface{}) *IntSliceCmd {
	args := []interface{}{"cms.query", key}
	for _, s := range items {
		args = append(args, s)
	}
	cmd := NewIntSliceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}
