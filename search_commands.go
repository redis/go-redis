package redis

import (
	"context"
	"strings"
)

type SearchCmdable interface {
	FT_List(ctx context.Context) *StringSliceCmd
	FTAggregate(ctx context.Context, index string, query string) *MapStringInterfaceCmd
	FTAggregateWithArgs(ctx context.Context, index string, query string, options *FTAggregateOptions) *MapStringInterfaceCmd
	FTAliasAdd(ctx context.Context, index string, alias string) *StatusCmd
	FTAliasDel(ctx context.Context, alias string) *StatusCmd
	FTAliasUpdate(ctx context.Context, index string, alias string) *StatusCmd
	FTAlter(ctx context.Context, index string, skipInitalScan bool, definition []interface{}) *StatusCmd
	FTConfigGet(ctx context.Context, option string) *MapStringInterfaceCmd
	FTConfigSet(ctx context.Context, option string, value interface{}) *StatusCmd
	FTCreate(ctx context.Context, index string, options *FTCreateOptions, schema ...*SearchSchema) *StatusCmd
	FTCursorDel(ctx context.Context, index string, cursorId int) *StatusCmd
	FTCursorRead(ctx context.Context, index string, cursorId int, count int) *MapStringInterfaceCmd
	FTDictAdd(ctx context.Context, dict string, term []interface{}) *IntCmd
	FTDictDel(ctx context.Context, dict string, term []interface{}) *IntCmd
	FTDictDump(ctx context.Context, dict string) *StringSliceCmd
	FTDropIndex(ctx context.Context, index string) *StatusCmd
	FTDropIndexWithArgs(ctx context.Context, index string, options *FTDropIndexOptions) *StatusCmd
	FTExplain(ctx context.Context, index string, query string) *StringCmd
	FTExplainWithArgs(ctx context.Context, index string, query string, options *FTExplainOptions) *StringCmd
	FTInfo(ctx context.Context, index string) *MapStringInterfaceCmd
	FTProfileSearch(ctx context.Context, index string, limited bool, query SearchQuery) *MapStringInterfaceCmd
	FTProfileAggregate(ctx context.Context, index string, limited bool, query AggregateQuery) *MapStringInterfaceCmd
	FTSpellCheck(ctx context.Context, index string, query string) *MapStringInterfaceCmd
	FTSpellCheckWithArgs(ctx context.Context, index string, query string, options *FTSpellCheckOptions) *MapStringInterfaceCmd
	FTSearch(ctx context.Context, index string, query string) *Cmd
	FTSearchWithArgs(ctx context.Context, index string, query string, options *FTSearchOptions) *Cmd
	FTSynDump(ctx context.Context, index string) *MapStringSliceInterfaceCmd
	FTSynUpdate(ctx context.Context, index string, synGroupId interface{}, terms []interface{}) *StatusCmd
	FTSynUpdateWithArgs(ctx context.Context, index string, synGroupId interface{}, options *FTSynUpdateOptions, terms []interface{}) *StatusCmd
	FTTagVals(ctx context.Context, index string, field string) *StringSliceCmd
}

type FTCreateOptions struct {
	OnHash          bool
	OnJSON          bool
	Prefix          []interface{}
	Filter          string
	DefaultLanguage string
	LanguageField   string
	Score           float64
	ScoreField      string
	PayloadField    string
	MaxTextFields   int
	NoOffsets       bool
	Temporary       int
	NoHL            bool
	NoFields        bool
	NoFreqs         bool
	StopWords       []interface{}
	SkipInitalScan  bool
}

type SearchSchema struct {
	FieldName         string
	As                string
	FieldType         string
	Sortable          bool
	UNF               bool
	NoStem            bool
	NoIndex           bool
	PhoneticMatcher   string
	Weight            float64
	Seperator         string
	CaseSensitive     bool
	WithSuffixtrie    bool
	VectorArgs        *FTVectorArgs
	GeoShapeFieldType string
}

type FTVectorArgs struct {
	FlatOptions *FTFlatOptions
	HNSWOptions *FTHNSWOptions
}

type FTFlatOptions struct {
	Type            string
	Dim             int
	DistanceMetric  string
	InitialCapacity int
	BlockSize       int
}

type FTHNSWOptions struct {
	Type                   string
	Dim                    int
	DistanceMetric         string
	InitialCapacity        int
	MaxEdgesPerNode        int
	MaxAllowedEdgesPerNode int
	EFRunTime              int
	Epsilon                float64
}

type FTDropIndexOptions struct {
	DeleteDocs bool
}

type SpellCheckTerms struct {
	Include    bool
	Exclude    bool
	Dictionary string
}

type FTSpellCheckOptions struct {
	Distance int
	Terms    SpellCheckTerms
	Dialect  int
}

type FTExplainOptions struct {
	Dialect string
}

type FTSynUpdateOptions struct {
	SkipInitialScan bool
}

type SearchAggregator int

const (
	SearchInvalid = SearchAggregator(iota)
	SearchAvg
	SearchSum
	SearchMin
	SearchMax
	SearchCount
	SearchCountDistinct
	SearchCountDistinctish
	SearchStdDev
	SearchQuantile
	SearchToList
	SearchFirstValue
	SearchRandomSample
)

func (a SearchAggregator) String() string {
	switch a {
	case SearchInvalid:
		return ""
	case SearchAvg:
		return "AVG"
	case SearchSum:
		return "SUM"
	case SearchMin:
		return "MIN"
	case SearchMax:
		return "MAX"
	case SearchCount:
		return "COUNT"
	case SearchCountDistinct:
		return "COUNT_DISTINCT"
	case SearchCountDistinctish:
		return "COUNT_DISTINCTISH"
	case SearchStdDev:
		return "STDDEV"
	case SearchQuantile:
		return "QUANTILE"
	case SearchToList:
		return "TOLIST"
	case SearchFirstValue:
		return "FIRST_VALUE"
	case SearchRandomSample:
		return "RANDOM_SAMPLE"
	default:
		return ""
	}
}

// Each AggregateReducer have different args.
// Please follow https://redis.io/docs/interact/search-and-query/search/aggregations/#supported-groupby-reducers for more information.
type FTAggregateReducer struct {
	Reducer SearchAggregator
	Args    []interface{}
	As      string
}

type FTAggregateGroupBy struct {
	Fields []interface{}
	Reduce []FTAggregateReducer
}

type FTAggregateSortBy struct {
	FieldName string
	Asc       bool
	Desc      bool
}

type FTAggregateApply struct {
	Field string
	As    string
}

type FTAggregateLoad struct {
	Field string
	As    string
}

type FTAggregateWithCursor struct {
	Count   int
	MaxIdle int
}

type FTAggregateOptions struct {
	Verbatim          bool
	LoadAll           bool
	Load              []FTAggregateLoad
	Timeout           int
	GroupBy           []FTAggregateGroupBy
	SortBy            []FTAggregateSortBy
	SortByMax         int
	Apply             []FTAggregateApply
	LimitOffset       int
	Limit             int
	Filter            string
	WithCursor        bool
	WithCursorOptions *FTAggregateWithCursor
	Params            map[string]interface{}
	DialectVersion    int
}

type FTSearchFilter struct {
	FieldName interface{}
	Min       interface{}
	Max       interface{}
}

type FTSearchGeoFilter struct {
	FieldName string
	Longitude float64
	Latitude  float64
	Radius    float64
	Unit      string
}

type FTSearchReturn struct {
	FieldName string
	As        string
}

type FTSearchSortBy struct {
	FieldName string
	Asc       bool
	Desc      bool
}

type FTSearchOptions struct {
	NoContent       bool
	Verbatim        bool
	NoStopWrods     bool
	WithScores      bool
	WithPayloads    bool
	WithSortKeys    bool
	Filters         []FTSearchFilter
	GeoFilter       []FTSearchGeoFilter
	InKeys          []interface{}
	InFields        []interface{}
	Return          []FTSearchReturn
	Slop            int
	Timeout         int
	InOrder         bool
	Language        string
	Expander        string
	Scorer          string
	ExplainScore    bool
	Payload         string
	SortBy          []FTSearchSortBy
	SortByWithCount bool
	LimitOffset     int
	Limit           int
	Params          map[string]interface{}
	DialectVersion  int
}

// FT_List - Lists all the existing indexes in the database.
// For more information, please refer to the Redis documentation: [FT._LIST](https://redis.io/commands/ft._list/)
func (c cmdable) FT_List(ctx context.Context) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "FT._LIST")
	_ = c(ctx, cmd)
	return cmd
}

// FTAggregate - Performs a search query on an index and applies a series of aggregate transformations to the result.
// The 'index' parameter specifies the index to search, and the 'query' parameter specifies the search query.
// For more information, please refer to the Redis documentation: [FT.AGGREGATE](https://redis.io/commands/ft.aggregate/)
func (c cmdable) FTAggregate(ctx context.Context, index string, query string) *MapStringInterfaceCmd {
	args := []interface{}{"FT.AGGREGATE", index, query}
	cmd := NewMapStringInterfaceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type AggregateQuery []interface{}

func FTAggregateQuery(query string, options *FTAggregateOptions) AggregateQuery {
	queryArgs := []interface{}{query}
	if options != nil {
		if options.Verbatim {
			queryArgs = append(queryArgs, "VERBATIM")
		}
		if options.LoadAll && options.Load != nil {
			panic("FT.AGGREGATE: LOADALL and LOAD are mutually exclusive")
		}
		if options.LoadAll {
			queryArgs = append(queryArgs, "LOAD", "*")
		}
		if options.Load != nil {
			queryArgs = append(queryArgs, "LOAD", len(options.Load))
			for _, load := range options.Load {
				queryArgs = append(queryArgs, load.Field)
				if load.As != "" {
					queryArgs = append(queryArgs, "AS", load.As)
				}
			}
		}
		if options.Timeout > 0 {
			queryArgs = append(queryArgs, "TIMEOUT", options.Timeout)
		}
		if options.GroupBy != nil {
			for _, groupBy := range options.GroupBy {
				queryArgs = append(queryArgs, "GROUPBY", len(groupBy.Fields))
				queryArgs = append(queryArgs, groupBy.Fields...)

				for _, reducer := range groupBy.Reduce {
					queryArgs = append(queryArgs, "REDUCE")
					queryArgs = append(queryArgs, reducer.Reducer.String())
					if reducer.Args != nil {
						queryArgs = append(queryArgs, len(reducer.Args))
						queryArgs = append(queryArgs, reducer.Args...)
					} else {
						queryArgs = append(queryArgs, 0)
					}
					if reducer.As != "" {
						queryArgs = append(queryArgs, "AS", reducer.As)
					}
				}
			}
		}
		if options.SortBy != nil {
			queryArgs = append(queryArgs, "SORTBY")
			sortByOptions := []interface{}{}
			for _, sortBy := range options.SortBy {
				sortByOptions = append(sortByOptions, sortBy.FieldName)
				if sortBy.Asc && sortBy.Desc {
					panic("FT.AGGREGATE: ASC and DESC are mutually exclusive")
				}
				if sortBy.Asc {
					sortByOptions = append(sortByOptions, "ASC")
				}
				if sortBy.Desc {
					sortByOptions = append(sortByOptions, "DESC")
				}
			}
			queryArgs = append(queryArgs, len(sortByOptions))
			queryArgs = append(queryArgs, sortByOptions...)
		}
		if options.SortByMax > 0 {
			queryArgs = append(queryArgs, "MAX", options.SortByMax)
		}
		for _, apply := range options.Apply {
			queryArgs = append(queryArgs, "APPLY", apply.Field)
			if apply.As != "" {
				queryArgs = append(queryArgs, "AS", apply.As)
			}
		}
		if options.LimitOffset > 0 {
			queryArgs = append(queryArgs, "LIMIT", options.LimitOffset)
		}
		if options.Limit > 0 {
			queryArgs = append(queryArgs, options.Limit)
		}
		if options.Filter != "" {
			queryArgs = append(queryArgs, "FILTER", options.Filter)
		}
		if options.WithCursor {
			queryArgs = append(queryArgs, "WITHCURSOR")
			if options.WithCursorOptions != nil {
				if options.WithCursorOptions.Count > 0 {
					queryArgs = append(queryArgs, "COUNT", options.WithCursorOptions.Count)
				}
				if options.WithCursorOptions.MaxIdle > 0 {
					queryArgs = append(queryArgs, "MAXIDLE", options.WithCursorOptions.MaxIdle)
				}
			}
		}
		if options.Params != nil {
			queryArgs = append(queryArgs, "PARAMS", len(options.Params)*2)
			for key, value := range options.Params {
				queryArgs = append(queryArgs, key, value)
			}
		}
		if options.DialectVersion > 0 {
			queryArgs = append(queryArgs, "DIALECT", options.DialectVersion)
		}
	}
	return queryArgs
}

// FTAggregateWithArgs - Performs a search query on an index and applies a series of aggregate transformations to the result.
// The 'index' parameter specifies the index to search, and the 'query' parameter specifies the search query.
// This function also allows for specifying additional options such as: Verbatim, LoadAll, Load, Timeout, GroupBy, SortBy, SortByMax, Apply, LimitOffset, Limit, Filter, WithCursor, Params, and DialectVersion.
// For more information, please refer to the Redis documentation: [FT.AGGREGATE](https://redis.io/commands/ft.aggregate/)
func (c cmdable) FTAggregateWithArgs(ctx context.Context, index string, query string, options *FTAggregateOptions) *MapStringInterfaceCmd {
	args := []interface{}{"FT.AGGREGATE", index, query}
	if options != nil {
		if options.Verbatim {
			args = append(args, "VERBATIM")
		}
		if options.LoadAll && options.Load != nil {
			panic("FT.AGGREGATE: LOADALL and LOAD are mutually exclusive")
		}
		if options.LoadAll {
			args = append(args, "LOAD", "*")
		}
		if options.Load != nil {
			args = append(args, "LOAD", len(options.Load))
			for _, load := range options.Load {
				args = append(args, load.Field)
				if load.As != "" {
					args = append(args, "AS", load.As)
				}
			}
		}
		if options.Timeout > 0 {
			args = append(args, "TIMEOUT", options.Timeout)
		}
		if options.GroupBy != nil {
			for _, groupBy := range options.GroupBy {
				args = append(args, "GROUPBY", len(groupBy.Fields))
				args = append(args, groupBy.Fields...)

				for _, reducer := range groupBy.Reduce {
					args = append(args, "REDUCE")
					args = append(args, reducer.Reducer.String())
					if reducer.Args != nil {
						args = append(args, len(reducer.Args))
						args = append(args, reducer.Args...)
					} else {
						args = append(args, 0)
					}
					if reducer.As != "" {
						args = append(args, "AS", reducer.As)
					}
				}
			}
		}
		if options.SortBy != nil {
			args = append(args, "SORTBY")
			sortByOptions := []interface{}{}
			for _, sortBy := range options.SortBy {
				sortByOptions = append(sortByOptions, sortBy.FieldName)
				if sortBy.Asc && sortBy.Desc {
					panic("FT.AGGREGATE: ASC and DESC are mutually exclusive")
				}
				if sortBy.Asc {
					sortByOptions = append(sortByOptions, "ASC")
				}
				if sortBy.Desc {
					sortByOptions = append(sortByOptions, "DESC")
				}
			}
			args = append(args, len(sortByOptions))
			args = append(args, sortByOptions...)
		}
		if options.SortByMax > 0 {
			args = append(args, "MAX", options.SortByMax)
		}
		for _, apply := range options.Apply {
			args = append(args, "APPLY", apply.Field)
			if apply.As != "" {
				args = append(args, "AS", apply.As)
			}
		}
		if options.LimitOffset > 0 {
			args = append(args, "LIMIT", options.LimitOffset)
		}
		if options.Limit > 0 {
			args = append(args, options.Limit)
		}
		if options.Filter != "" {
			args = append(args, "FILTER", options.Filter)
		}
		if options.WithCursor {
			args = append(args, "WITHCURSOR")
			if options.WithCursorOptions != nil {
				if options.WithCursorOptions.Count > 0 {
					args = append(args, "COUNT", options.WithCursorOptions.Count)
				}
				if options.WithCursorOptions.MaxIdle > 0 {
					args = append(args, "MAXIDLE", options.WithCursorOptions.MaxIdle)
				}
			}
		}
		if options.Params != nil {
			args = append(args, "PARAMS", len(options.Params)*2)
			for key, value := range options.Params {
				args = append(args, key, value)
			}
		}
		if options.DialectVersion > 0 {
			args = append(args, "DIALECT", options.DialectVersion)
		}
	}

	cmd := NewMapStringInterfaceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTAliasAdd - Adds an alias to an index.
// The 'index' parameter specifies the index to which the alias is added, and the 'alias' parameter specifies the alias.
// For more information, please refer to the Redis documentation: [FT.ALIASADD](https://redis.io/commands/ft.aliasadd/)
func (c cmdable) FTAliasAdd(ctx context.Context, index string, alias string) *StatusCmd {
	args := []interface{}{"FT.ALIASADD", alias, index}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTAliasDel - Removes an alias from an index.
// The 'alias' parameter specifies the alias to be removed.
// For more information, please refer to the Redis documentation: [FT.ALIASDEL](https://redis.io/commands/ft.aliasdel/)
func (c cmdable) FTAliasDel(ctx context.Context, alias string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.ALIASDEL", alias)
	_ = c(ctx, cmd)
	return cmd
}

// FTAliasUpdate - Updates an alias to an index.
// The 'index' parameter specifies the index to which the alias is updated, and the 'alias' parameter specifies the alias.
// If the alias already exists for a different index, it updates the alias to point to the specified index instead.
// For more information, please refer to the Redis documentation: [FT.ALIASUPDATE](https://redis.io/commands/ft.aliasupdate/)
func (c cmdable) FTAliasUpdate(ctx context.Context, index string, alias string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.ALIASUPDATE", alias, index)
	_ = c(ctx, cmd)
	return cmd
}

// FTAlter - Alters the definition of an existing index.
// The 'index' parameter specifies the index to alter, and the 'skipInitalScan' parameter specifies whether to skip the initial scan.
// The 'definition' parameter specifies the new definition for the index.
// For more information, please refer to the Redis documentation: [FT.ALTER](https://redis.io/commands/ft.alter/)
func (c cmdable) FTAlter(ctx context.Context, index string, skipInitalScan bool, definition []interface{}) *StatusCmd {
	args := []interface{}{"FT.ALTER", index}
	if skipInitalScan {
		args = append(args, "SKIPINITIALSCAN")
	}
	args = append(args, "SCHEMA", "ADD")
	args = append(args, definition...)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTConfigGet - Retrieves the value of a RediSearch configuration parameter.
// The 'option' parameter specifies the configuration parameter to retrieve.
// For more information, please refer to the Redis documentation: [FT.CONFIG GET](https://redis.io/commands/ft.config-get/)
func (c cmdable) FTConfigGet(ctx context.Context, option string) *MapStringInterfaceCmd {
	cmd := NewMapStringInterfaceCmd(ctx, "FT.CONFIG", "GET", option)
	_ = c(ctx, cmd)
	return cmd
}

// FTConfigSet - Sets the value of a RediSearch configuration parameter.
// The 'option' parameter specifies the configuration parameter to set, and the 'value' parameter specifies the new value.
// For more information, please refer to the Redis documentation: [FT.CONFIG SET](https://redis.io/commands/ft.config-set/)
func (c cmdable) FTConfigSet(ctx context.Context, option string, value interface{}) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.CONFIG", "SET", option, value)
	_ = c(ctx, cmd)
	return cmd
}

// FTCreate - Creates a new index with the given options and schema.
// The 'index' parameter specifies the name of the index to create.
// The 'options' parameter specifies various options for the index, such as:
// whether to index hashes or JSONs, prefixes, filters, default language, score, score field, payload field, etc.
// The 'schema' parameter specifies the schema for the index, which includes the field name, field type, etc.
// For more information, please refer to the Redis documentation: [FT.CREATE](https://redis.io/commands/ft.create/)
func (c cmdable) FTCreate(ctx context.Context, index string, options *FTCreateOptions, schema ...*SearchSchema) *StatusCmd {
	args := []interface{}{"FT.CREATE", index}
	if options != nil {
		if options.OnHash && !options.OnJSON {
			args = append(args, "ON", "HASH")
		}
		if options.OnJSON && !options.OnHash {
			args = append(args, "ON", "JSON")
		}
		if options.OnHash && options.OnJSON {
			panic("FT.CREATE: ON HASH and ON JSON are mutually exclusive")
		}
		if options.Prefix != nil {
			args = append(args, "PREFIX", len(options.Prefix))
			args = append(args, options.Prefix...)
		}
		if options.Filter != "" {
			args = append(args, "FILTER", options.Filter)
		}
		if options.DefaultLanguage != "" {
			args = append(args, "LANGUAGE", options.DefaultLanguage)
		}
		if options.LanguageField != "" {
			args = append(args, "LANGUAGE_FIELD", options.LanguageField)
		}
		if options.Score > 0 {
			args = append(args, "SCORE", options.Score)
		}
		if options.ScoreField != "" {
			args = append(args, "SCORE_FIELD", options.ScoreField)
		}
		if options.PayloadField != "" {
			args = append(args, "PAYLOAD_FIELD", options.PayloadField)
		}
		if options.MaxTextFields > 0 {
			args = append(args, "MAXTEXTFIELDS", options.MaxTextFields)
		}
		if options.NoOffsets {
			args = append(args, "NOOFFSETS")
		}
		if options.Temporary > 0 {
			args = append(args, "TEMPORARY", options.Temporary)
		}
		if options.NoHL {
			args = append(args, "NOHL")
		}
		if options.NoFields {
			args = append(args, "NOFIELDS")
		}
		if options.NoFreqs {
			args = append(args, "NOFREQS")
		}
		if options.StopWords != nil {
			args = append(args, "STOPWORDS", len(options.StopWords))
			args = append(args, options.StopWords...)
		}
		if options.SkipInitalScan {
			args = append(args, "SKIPINITIALSCAN")
		}
	}
	if schema == nil {
		panic("FT.CREATE: SCHEMA is required")
	}
	args = append(args, "SCHEMA")
	for _, schema := range schema {
		if schema.FieldName == "" || schema.FieldType == "" {
			panic("FT.CREATE: SCHEMA FieldName and FieldType are required")
		}
		args = append(args, schema.FieldName)
		if schema.As != "" {
			args = append(args, "AS", schema.As)
		}
		args = append(args, schema.FieldType)
		if schema.VectorArgs != nil {
			if strings.ToUpper(schema.FieldType) != "VECTOR" {
				panic("FT.CREATE: SCHEMA FieldType VECTOR is required for VectorArgs")
			}
			if schema.VectorArgs.FlatOptions != nil && schema.VectorArgs.HNSWOptions != nil {
				panic("FT.CREATE: SCHEMA VectorArgs FlatOptions and HNSWOptions are mutually exclusive")
			}
			if schema.VectorArgs.FlatOptions != nil {
				args = append(args, "FLAT")
				if schema.VectorArgs.FlatOptions.Type == "" || schema.VectorArgs.FlatOptions.Dim == 0 || schema.VectorArgs.FlatOptions.DistanceMetric == "" {
					panic("FT.CREATE: Type, Dim and DistanceMetric are required for VECTOR FLAT")
				}
				flatArgs := []interface{}{
					"TYPE", schema.VectorArgs.FlatOptions.Type,
					"DIM", schema.VectorArgs.FlatOptions.Dim,
					"DISTANCE_METRIC", schema.VectorArgs.FlatOptions.DistanceMetric,
				}
				if schema.VectorArgs.FlatOptions.InitialCapacity > 0 {
					flatArgs = append(flatArgs, "INITIAL_CAP", schema.VectorArgs.FlatOptions.InitialCapacity)
				}
				if schema.VectorArgs.FlatOptions.BlockSize > 0 {
					flatArgs = append(flatArgs, "BLOCK_SIZE", schema.VectorArgs.FlatOptions.BlockSize)
				}
				args = append(args, len(flatArgs))
				args = append(args, flatArgs...)
			}
			if schema.VectorArgs.HNSWOptions != nil {
				args = append(args, "HNSW")
				if schema.VectorArgs.HNSWOptions.Type == "" || schema.VectorArgs.HNSWOptions.Dim == 0 || schema.VectorArgs.HNSWOptions.DistanceMetric == "" {
					panic("FT.CREATE: Type, Dim and DistanceMetric are required for VECTOR HNSW")
				}
				hnswArgs := []interface{}{
					"TYPE", schema.VectorArgs.HNSWOptions.Type,
					"DIM", schema.VectorArgs.HNSWOptions.Dim,
					"DISTANCE_METRIC", schema.VectorArgs.HNSWOptions.DistanceMetric,
				}
				if schema.VectorArgs.HNSWOptions.InitialCapacity > 0 {
					hnswArgs = append(hnswArgs, "INITIAL_CAP", schema.VectorArgs.HNSWOptions.InitialCapacity)
				}
				if schema.VectorArgs.HNSWOptions.MaxEdgesPerNode > 0 {
					hnswArgs = append(hnswArgs, "M", schema.VectorArgs.HNSWOptions.MaxEdgesPerNode)
				}
				if schema.VectorArgs.HNSWOptions.MaxAllowedEdgesPerNode > 0 {
					hnswArgs = append(hnswArgs, "EF_CONSTRUCTION", schema.VectorArgs.HNSWOptions.MaxAllowedEdgesPerNode)
				}
				if schema.VectorArgs.HNSWOptions.EFRunTime > 0 {
					hnswArgs = append(hnswArgs, "EF_RUNTIME", schema.VectorArgs.HNSWOptions.EFRunTime)
				}
				if schema.VectorArgs.HNSWOptions.Epsilon > 0 {
					hnswArgs = append(hnswArgs, "EPSILON", schema.VectorArgs.HNSWOptions.Epsilon)
				}
				args = append(args, len(hnswArgs))
				args = append(args, hnswArgs...)
			}
		}
		if schema.GeoShapeFieldType != "" {
			if strings.ToUpper(schema.FieldType) != "GEOSHAPE" {
				panic("FT.CREATE: SCHEMA FieldType GEOSHAPE is required for GeoShapeFieldType")
			}
			args = append(args, schema.GeoShapeFieldType)
		}
		if schema.NoStem {
			args = append(args, "NOSTEM")
		}
		if schema.Sortable {
			args = append(args, "SORTABLE")
		}
		if schema.UNF {
			args = append(args, "UNF")
		}
		if schema.NoIndex {
			args = append(args, "NOINDEX")
		}
		if schema.PhoneticMatcher != "" {
			args = append(args, "PHONETIC", schema.PhoneticMatcher)
		}
		if schema.Weight > 0 {
			args = append(args, "WEIGHT", schema.Weight)
		}
		if schema.Seperator != "" {
			args = append(args, "SEPERATOR", schema.Seperator)
		}
		if schema.CaseSensitive {
			args = append(args, "CASESENSITIVE")
		}
		if schema.WithSuffixtrie {
			args = append(args, "WITHSUFFIXTRIE")
		}
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTCursorDel - Deletes a cursor from an existing index.
// The 'index' parameter specifies the index from which to delete the cursor, and the 'cursorId' parameter specifies the ID of the cursor to delete.
// For more information, please refer to the Redis documentation: [FT.CURSOR DEL](https://redis.io/commands/ft.cursor-del/)
func (c cmdable) FTCursorDel(ctx context.Context, index string, cursorId int) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.CURSOR", "DEL", index, cursorId)
	_ = c(ctx, cmd)
	return cmd
}

// FTCursorRead - Reads the next results from an existing cursor.
// The 'index' parameter specifies the index from which to read the cursor, the 'cursorId' parameter specifies the ID of the cursor to read, and the 'count' parameter specifies the number of results to read.
// For more information, please refer to the Redis documentation: [FT.CURSOR READ](https://redis.io/commands/ft.cursor-read/)
func (c cmdable) FTCursorRead(ctx context.Context, index string, cursorId int, count int) *MapStringInterfaceCmd {
	args := []interface{}{"FT.CURSOR", "READ", index, cursorId}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	cmd := NewMapStringInterfaceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTDictAdd - Adds terms to a dictionary.
// The 'dict' parameter specifies the dictionary to which to add the terms, and the 'term' parameter specifies the terms to add.
// For more information, please refer to the Redis documentation: [FT.DICTADD](https://redis.io/commands/ft.dictadd/)
func (c cmdable) FTDictAdd(ctx context.Context, dict string, term []interface{}) *IntCmd {
	args := []interface{}{"FT.DICTADD", dict}
	args = append(args, term...)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTDictDel - Deletes terms from a dictionary.
// The 'dict' parameter specifies the dictionary from which to delete the terms, and the 'term' parameter specifies the terms to delete.
// For more information, please refer to the Redis documentation: [FT.DICTDEL](https://redis.io/commands/ft.dictdel/)
func (c cmdable) FTDictDel(ctx context.Context, dict string, term []interface{}) *IntCmd {
	args := []interface{}{"FT.DICTDEL", dict}
	args = append(args, term...)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTDictDump - Returns all terms in the specified dictionary.
// The 'dict' parameter specifies the dictionary from which to return the terms.
// For more information, please refer to the Redis documentation: [FT.DICTDUMP](https://redis.io/commands/ft.dictdump/)
func (c cmdable) FTDictDump(ctx context.Context, dict string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "FT.DICTDUMP", dict)
	_ = c(ctx, cmd)
	return cmd
}

// FTDropIndex - Deletes an index.
// The 'index' parameter specifies the index to delete.
// For more information, please refer to the Redis documentation: [FT.DROPINDEX](https://redis.io/commands/ft.dropindex/)
func (c cmdable) FTDropIndex(ctx context.Context, index string) *StatusCmd {
	args := []interface{}{"FT.DROPINDEX", index}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTDropIndexWithArgs - Deletes an index with options.
// The 'index' parameter specifies the index to delete, and the 'options' parameter specifies the DeleteDocs option for docs deletion.
// For more information, please refer to the Redis documentation: [FT.DROPINDEX](https://redis.io/commands/ft.dropindex/)
func (c cmdable) FTDropIndexWithArgs(ctx context.Context, index string, options *FTDropIndexOptions) *StatusCmd {
	args := []interface{}{"FT.DROPINDEX", index}
	if options != nil {
		if options.DeleteDocs {
			args = append(args, "DD")
		}
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTExplain - Returns the execution plan for a complex query.
// The 'index' parameter specifies the index to query, and the 'query' parameter specifies the query string.
// For more information, please refer to the Redis documentation: [FT.EXPLAIN](https://redis.io/commands/ft.explain/)
func (c cmdable) FTExplain(ctx context.Context, index string, query string) *StringCmd {
	cmd := NewStringCmd(ctx, "FT.EXPLAIN", index, query)
	_ = c(ctx, cmd)
	return cmd
}

// FTExplainWithArgs - Returns the execution plan for a complex query with options.
// The 'index' parameter specifies the index to query, the 'query' parameter specifies the query string, and the 'options' parameter specifies the Dialect for the query.
// For more information, please refer to the Redis documentation: [FT.EXPLAIN](https://redis.io/commands/ft.explain/)
func (c cmdable) FTExplainWithArgs(ctx context.Context, index string, query string, options *FTExplainOptions) *StringCmd {
	args := []interface{}{"FT.EXPLAIN", index, query}
	if options.Dialect != "" {
		args = append(args, "DIALECT", options.Dialect)
	}
	cmd := NewStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTInfo - Retrieves information about an index.
// The 'index' parameter specifies the index to retrieve information about.
// For more information, please refer to the Redis documentation: [FT.INFO](https://redis.io/commands/ft.info/)
func (c cmdable) FTInfo(ctx context.Context, index string) *MapStringInterfaceCmd {
	cmd := NewMapStringInterfaceCmd(ctx, "FT.INFO", index)
	_ = c(ctx, cmd)
	return cmd
}

// FTProfileSearch - Executes a search query and returns a profile of how the query was processed.
// The 'index' parameter specifies the index to search, the 'limited' parameter specifies whether to limit the results, and the 'query' parameter specifies the search query.
// For more information, please refer to the Redis documentation: [FT.PROFILE SEARCH](https://redis.io/commands/ft.profile/)
func (c cmdable) FTProfileSearch(ctx context.Context, index string, limited bool, query SearchQuery) *MapStringInterfaceCmd {
	args := []interface{}{"FT.PROFILE", index, "SEARCH"}
	if limited {
		args = append(args, "LIMITED")
	}
	args = append(args, "QUERY")
	args = append(args, query...)

	cmd := NewMapStringInterfaceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTProfileAggregate - Executes an aggregate query and returns a profile of how the query was processed.
// The 'index' parameter specifies the index to search, the 'limited' parameter specifies whether to limit the results, and the 'query' parameter specifies the aggregate query.
// For more information, please refer to the Redis documentation: [FT.PROFILE AGGREGATE](https://redis.io/commands/ft.aggregate/)
func (c cmdable) FTProfileAggregate(ctx context.Context, index string, limited bool, query AggregateQuery) *MapStringInterfaceCmd {
	args := []interface{}{"FT.PROFILE", index, "AGGREGATE"}
	if limited {
		args = append(args, "LIMITED")
	}
	args = append(args, "QUERY")
	args = append(args, query...)

	cmd := NewMapStringInterfaceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTSpellCheck - Checks a query string for spelling errors.
// For more details about spellcheck query please follow:
// https://redis.io/docs/interact/search-and-query/advanced-concepts/spellcheck/
// For more information, please refer to the Redis documentation: [FT.SPELLCHECK](https://redis.io/commands/ft.spellcheck/)
func (c cmdable) FTSpellCheck(ctx context.Context, index string, query string) *MapStringInterfaceCmd {
	cmd := NewMapStringInterfaceCmd(ctx, "FT.SPELLCHECK", index, query)
	_ = c(ctx, cmd)
	return cmd
}

// FTSpellCheckWithArgs - Checks a query string for spelling errors with additional options.
// For more details about spellcheck query please follow:
// https://redis.io/docs/interact/search-and-query/advanced-concepts/spellcheck/
// For more information, please refer to the Redis documentation: [FT.SPELLCHECK](https://redis.io/commands/ft.spellcheck/)
func (c cmdable) FTSpellCheckWithArgs(ctx context.Context, index string, query string, options *FTSpellCheckOptions) *MapStringInterfaceCmd {
	args := []interface{}{"FT.SPELLCHECK", index, query}
	if options != nil {
		if options.Distance > 4 {
			panic("FT.SPELLCHECK: DISTANCE must be between 0 and 4")
		}
		if options.Distance > 0 {
			args = append(args, "DISTANCE", options.Distance)
		}
		if options.Terms.Include && options.Terms.Exclude {
			panic("FT.SPELLCHECK: INCLUDE and EXCLUDE are mutually exclusive")
		}
		if options.Terms.Include {
			args = append(args, "TERMS", "INCLUDE")
		}
		if options.Terms.Exclude {
			args = append(args, "TERMS", "EXCLUDE")
		}
		if options.Terms.Dictionary != "" {
			args = append(args, options.Terms.Dictionary)
		}
		if options.Dialect > 0 {
			args = append(args, "DIALECT", options.Dialect)
		}
	}
	cmd := NewMapStringInterfaceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTSearch - Executes a search query on an index.
// The 'index' parameter specifies the index to search, and the 'query' parameter specifies the search query.
// For more information, please refer to the Redis documentation: [FT.SEARCH](https://redis.io/commands/ft.search/)
func (c cmdable) FTSearch(ctx context.Context, index string, query string) *Cmd {
	args := []interface{}{"FT.SEARCH", index, query}
	cmd := NewCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

type SearchQuery []interface{}

func FTSearchQuery(query string, options *FTSearchOptions) SearchQuery {
	queryArgs := []interface{}{query}
	if options != nil {
		if options.NoContent {
			queryArgs = append(queryArgs, "NOCONTENT")
		}
		if options.Verbatim {
			queryArgs = append(queryArgs, "VERBATIM")
		}
		if options.NoStopWrods {
			queryArgs = append(queryArgs, "NOSTOPWORDS")
		}
		if options.WithScores {
			queryArgs = append(queryArgs, "WITHSCORES")
		}
		if options.WithPayloads {
			queryArgs = append(queryArgs, "WITHPAYLOADS")
		}
		if options.WithSortKeys {
			queryArgs = append(queryArgs, "WITHSORTKEYS")
		}
		if options.Filters != nil {
			for _, filter := range options.Filters {
				queryArgs = append(queryArgs, "FILTER", filter.FieldName, filter.Min, filter.Max)
			}
		}
		if options.GeoFilter != nil {
			for _, geoFilter := range options.GeoFilter {
				queryArgs = append(queryArgs, "GEOFILTER", geoFilter.FieldName, geoFilter.Longitude, geoFilter.Latitude, geoFilter.Radius, geoFilter.Unit)
			}
		}
		if options.InKeys != nil {
			queryArgs = append(queryArgs, "INKEYS", len(options.InKeys))
			queryArgs = append(queryArgs, options.InKeys...)
		}
		if options.InFields != nil {
			queryArgs = append(queryArgs, "INFIELDS", len(options.InFields))
			queryArgs = append(queryArgs, options.InFields...)
		}
		if options.Return != nil {
			queryArgs = append(queryArgs, "RETURN")
			queryArgsReturn := []interface{}{}
			for _, ret := range options.Return {
				queryArgsReturn = append(queryArgsReturn, ret.FieldName)
				if ret.As != "" {
					queryArgsReturn = append(queryArgsReturn, "AS", ret.As)
				}
			}
			queryArgs = append(queryArgs, len(queryArgsReturn))
			queryArgs = append(queryArgs, queryArgsReturn...)
		}
		if options.Slop > 0 {
			queryArgs = append(queryArgs, "SLOP", options.Slop)
		}
		if options.Timeout > 0 {
			queryArgs = append(queryArgs, "TIMEOUT", options.Timeout)
		}
		if options.InOrder {
			queryArgs = append(queryArgs, "INORDER")
		}
		if options.Language != "" {
			queryArgs = append(queryArgs, "LANGUAGE", options.Language)
		}
		if options.Expander != "" {
			queryArgs = append(queryArgs, "EXPANDER", options.Expander)
		}
		if options.Scorer != "" {
			queryArgs = append(queryArgs, "SCORER", options.Scorer)
		}
		if options.ExplainScore {
			queryArgs = append(queryArgs, "EXPLAINSCORE")
		}
		if options.Payload != "" {
			queryArgs = append(queryArgs, "PAYLOAD", options.Payload)
		}
		if options.SortBy != nil {
			queryArgs = append(queryArgs, "SORTBY")
			for _, sortBy := range options.SortBy {
				queryArgs = append(queryArgs, sortBy.FieldName)
				if sortBy.Asc && sortBy.Desc {
					panic("FT.SEARCH: ASC and DESC are mutually exclusive")
				}
				if sortBy.Asc {
					queryArgs = append(queryArgs, "ASC")
				}
				if sortBy.Desc {
					queryArgs = append(queryArgs, "DESC")
				}
			}
			if options.SortByWithCount {
				queryArgs = append(queryArgs, "WITHCOUT")
			}
		}
		if options.LimitOffset >= 0 && options.Limit > 0 {
			queryArgs = append(queryArgs, "LIMIT", options.LimitOffset, options.Limit)
		}
		if options.Params != nil {
			queryArgs = append(queryArgs, "PARAMS", len(options.Params)*2)
			for key, value := range options.Params {
				queryArgs = append(queryArgs, key, value)
			}
		}
		if options.DialectVersion > 0 {
			queryArgs = append(queryArgs, "DIALECT", options.DialectVersion)
		}
	}
	return queryArgs
}

// FTSearchWithArgs - Executes a search query on an index with additional options.
// The 'index' parameter specifies the index to search, the 'query' parameter specifies the search query,
// and the 'options' parameter specifies additional options for the search.
// For more information, please refer to the Redis documentation: [FT.SEARCH](https://redis.io/commands/ft.search/)
func (c cmdable) FTSearchWithArgs(ctx context.Context, index string, query string, options *FTSearchOptions) *Cmd {
	args := []interface{}{"FT.SEARCH", index, query}
	if options != nil {
		if options.NoContent {
			args = append(args, "NOCONTENT")
		}
		if options.Verbatim {
			args = append(args, "VERBATIM")
		}
		if options.NoStopWrods {
			args = append(args, "NOSTOPWORDS")
		}
		if options.WithScores {
			args = append(args, "WITHSCORES")
		}
		if options.WithPayloads {
			args = append(args, "WITHPAYLOADS")
		}
		if options.WithSortKeys {
			args = append(args, "WITHSORTKEYS")
		}
		if options.Filters != nil {
			for _, filter := range options.Filters {
				args = append(args, "FILTER", filter.FieldName, filter.Min, filter.Max)
			}
		}
		if options.GeoFilter != nil {
			for _, geoFilter := range options.GeoFilter {
				args = append(args, "GEOFILTER", geoFilter.FieldName, geoFilter.Longitude, geoFilter.Latitude, geoFilter.Radius, geoFilter.Unit)
			}
		}
		if options.InKeys != nil {
			args = append(args, "INKEYS", len(options.InKeys))
			args = append(args, options.InKeys...)
		}
		if options.InFields != nil {
			args = append(args, "INFIELDS", len(options.InFields))
			args = append(args, options.InFields...)
		}
		if options.Return != nil {
			args = append(args, "RETURN")
			argsReturn := []interface{}{}
			for _, ret := range options.Return {
				argsReturn = append(argsReturn, ret.FieldName)
				if ret.As != "" {
					argsReturn = append(argsReturn, "AS", ret.As)
				}
			}
			args = append(args, len(argsReturn))
			args = append(args, argsReturn...)
		}
		if options.Slop > 0 {
			args = append(args, "SLOP", options.Slop)
		}
		if options.Timeout > 0 {
			args = append(args, "TIMEOUT", options.Timeout)
		}
		if options.InOrder {
			args = append(args, "INORDER")
		}
		if options.Language != "" {
			args = append(args, "LANGUAGE", options.Language)
		}
		if options.Expander != "" {
			args = append(args, "EXPANDER", options.Expander)
		}
		if options.Scorer != "" {
			args = append(args, "SCORER", options.Scorer)
		}
		if options.ExplainScore {
			args = append(args, "EXPLAINSCORE")
		}
		if options.Payload != "" {
			args = append(args, "PAYLOAD", options.Payload)
		}
		if options.SortBy != nil {
			args = append(args, "SORTBY")
			for _, sortBy := range options.SortBy {
				args = append(args, sortBy.FieldName)
				if sortBy.Asc && sortBy.Desc {
					panic("FT.SEARCH: ASC and DESC are mutually exclusive")
				}
				if sortBy.Asc {
					args = append(args, "ASC")
				}
				if sortBy.Desc {
					args = append(args, "DESC")
				}
			}
			if options.SortByWithCount {
				args = append(args, "WITHCOUT")
			}
		}
		if options.LimitOffset >= 0 && options.Limit > 0 {
			args = append(args, "LIMIT", options.LimitOffset, options.Limit)
		}
		if options.Params != nil {
			args = append(args, "PARAMS", len(options.Params)*2)
			for key, value := range options.Params {
				args = append(args, key, value)
			}
		}
		if options.DialectVersion > 0 {
			args = append(args, "DIALECT", options.DialectVersion)
		}
	}
	cmd := NewCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTSynDump - Dumps the synonyms data structure.
// The 'index' parameter specifies the index to dump.
// For more information, please refer to the Redis documentation: [FT.SYNDUMP](https://redis.io/commands/ft.syndump/)
func (c cmdable) FTSynDump(ctx context.Context, index string) *MapStringSliceInterfaceCmd {
	cmd := NewMapStringSliceInterfaceCmd(ctx, "FT.SYNDUMP", index)
	_ = c(ctx, cmd)
	return cmd
}

// FTSynUpdate - Creates or updates a synonym group with additional terms.
// The 'index' parameter specifies the index to update, the 'synGroupId' parameter specifies the synonym group id, and the 'terms' parameter specifies the additional terms.
// For more information, please refer to the Redis documentation: [FT.SYNUPDATE](https://redis.io/commands/ft.synupdate/)
func (c cmdable) FTSynUpdate(ctx context.Context, index string, synGroupId interface{}, terms []interface{}) *StatusCmd {
	args := []interface{}{"FT.SYNUPDATE", index, synGroupId}
	args = append(args, terms...)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTSynUpdateWithArgs - Creates or updates a synonym group with additional terms and options.
// The 'index' parameter specifies the index to update, the 'synGroupId' parameter specifies the synonym group id, the 'options' parameter specifies additional options for the update, and the 'terms' parameter specifies the additional terms.
// For more information, please refer to the Redis documentation: [FT.SYNUPDATE](https://redis.io/commands/ft.synupdate/)
func (c cmdable) FTSynUpdateWithArgs(ctx context.Context, index string, synGroupId interface{}, options *FTSynUpdateOptions, terms []interface{}) *StatusCmd {
	args := []interface{}{"FT.SYNUPDATE", index, synGroupId}
	if options.SkipInitialScan {
		args = append(args, "SKIPINITIALSCAN")
	}
	args = append(args, terms...)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

// FTTagVals - Returns all distinct values indexed in a tag field.
// The 'index' parameter specifies the index to check, and the 'field' parameter specifies the tag field to retrieve values from.
// For more information, please refer to the Redis documentation: [FT.TAGVALS](https://redis.io/commands/ft.tagvals/)
func (c cmdable) FTTagVals(ctx context.Context, index string, field string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "FT.TAGVALS", index, field)
	_ = c(ctx, cmd)
	return cmd
}
