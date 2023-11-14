package redis

import (
	"context"
)

type SearchCmdable interface {
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
	Identifier      string
	Attribute       string
	AttributeType   string
	Sortable        bool
	UNF             bool
	NoStem          bool
	NoIndex         bool
	PhoneticMatcher string
	Weight          float64
	Seperator       string
	CaseSensitive   bool
	WithSuffix      bool
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

func (c cmdable) FT_List(ctx context.Context) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "FT._LIST")
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTAggregate(ctx context.Context, index string, query string) *MapStringInterfaceCmd {
	args := []interface{}{"FT.AGGREGATE", index, query}
	cmd := NewMapStringInterfaceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

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
		if options.Apply != nil {
			args = append(args, "APPLY", len(options.Apply))
			for _, apply := range options.Apply {
				args = append(args, apply.Field)
				if apply.As != "" {
					args = append(args, "AS", apply.As)
				}
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

func (c cmdable) FTAliasAdd(ctx context.Context, index string, alias string) *StatusCmd {
	args := []interface{}{"FT.ALIASADD", alias, index}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTAliasDel(ctx context.Context, alias string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.ALIASDEL", alias)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTAliasUpdate(ctx context.Context, index string, alias string) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.ALIASUPDATE", alias, index)
	_ = c(ctx, cmd)
	return cmd
}

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

func (c cmdable) FTConfigGet(ctx context.Context, option string) *MapStringInterfaceCmd {
	cmd := NewMapStringInterfaceCmd(ctx, "FT.CONFIG", "GET", option)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTConfigSet(ctx context.Context, option string, value interface{}) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.CONFIG", "SET", option, value)
	_ = c(ctx, cmd)
	return cmd
}

// TODO Fix schema for loop
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
		if schema.Identifier == "" || schema.AttributeType == "" {
			panic("FT.CREATE: SCHEMA IDENTIFIER and ATTRIBUTE_TYPE are required")
		}
		args = append(args, schema.Identifier)
		if schema.Attribute != "" {
			args = append(args, "AS", schema.Attribute)
		}
		args = append(args, schema.AttributeType)
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
		if schema.WithSuffix {
			args = append(args, "WITHSUFFIX")
		}
	}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTCursorDel(ctx context.Context, index string, cursorId int) *StatusCmd {
	cmd := NewStatusCmd(ctx, "FT.CURSOR", "DEL", index, cursorId)
	_ = c(ctx, cmd)
	return cmd
}

// TODO TEST IT
func (c cmdable) FTCursorRead(ctx context.Context, index string, cursorId int, count int) *MapStringInterfaceCmd {
	args := []interface{}{"FT.CURSOR", "READ", index, cursorId}
	if count > 0 {
		args = append(args, "COUNT", count)
	}
	cmd := NewMapStringInterfaceCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTDictAdd(ctx context.Context, dict string, term []interface{}) *IntCmd {
	args := []interface{}{"FT.DICTADD", dict}
	args = append(args, term...)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTDictDel(ctx context.Context, dict string, term []interface{}) *IntCmd {
	args := []interface{}{"FT.DICTDEL", dict}
	args = append(args, term...)
	cmd := NewIntCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTDictDump(ctx context.Context, dict string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "FT.DICTDUMP", dict)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTDropIndex(ctx context.Context, index string) *StatusCmd {
	args := []interface{}{"FT.DROPINDEX", index}
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

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

func (c cmdable) FTExplain(ctx context.Context, index string, query string) *StringCmd {
	cmd := NewStringCmd(ctx, "FT.EXPLAIN", index, query)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTExplainWithArgs(ctx context.Context, index string, query string, options *FTExplainOptions) *StringCmd {
	args := []interface{}{"FT.EXPLAIN", index, query}
	if options.Dialect != "" {
		args = append(args, "DIALECT", options.Dialect)
	}
	cmd := NewStringCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTInfo(ctx context.Context, index string) *MapStringInterfaceCmd {
	cmd := NewMapStringInterfaceCmd(ctx, "FT.INFO", index)
	_ = c(ctx, cmd)
	return cmd
}

// TODO - When ft search is ready
// func (c cmdable) FTProfileSearch(ctx context.Context, index string, limited bool, query string) *StringCmd {
// 	args := []interface{}{"FT.PROFILE", index, "SEARCH"}
// 	if limited {
// 		args = append(args, "LIMITED")
// 	}
// 	args = append(args, "QUERY", query)

// 	cmd := NewStringCmd(ctx, args...)
// 	_ = c(ctx, cmd)
// 	return cmd
// }

// func (c cmdable) FTProfileAggregate(ctx context.Context, index string, limited bool, query string) *FTProfileAggregateCmd {
// 	args := []interface{}{"FT.PROFILE", index, "AGGREGATE"}
// 	if limited {
// 		args = append(args, "LIMITED")
// 	}
// 	args = append(args, "QUERY", query)

// 	cmd := newFTProfileAggregateCmd(ctx, args...)
// 	_ = c(ctx, cmd)
// 	return cmd
// }

// type FTProfileAggregateResult struct {
// 	aggregateResult MapStringInterfaceCmd
// 	profileResult   KeyValueSliceCmd
// }

// type FTProfileAggregateCmd struct {
// 	baseCmd

// 	val FTProfileAggregateResult
// }

// func newFTProfileAggregateCmd(ctx context.Context, args ...interface{}) *FTProfileAggregateCmd {
// 	return &FTProfileAggregateCmd{
// 		baseCmd: baseCmd{
// 			ctx:  ctx,
// 			args: args,
// 		},
// 	}
// }

// func (cmd *FTProfileAggregateCmd) String() string {
// 	return cmdString(cmd, cmd.val)
// }

// func (cmd *FTProfileAggregateCmd) SetVal(val FTProfileAggregateResult) {
// 	cmd.val = val
// }

// func (cmd *FTProfileAggregateCmd) Result() (FTProfileAggregateResult, error) {
// 	return cmd.val, cmd.err
// }

// func (cmd *FTProfileAggregateCmd) Val() FTProfileAggregateResult {
// 	return cmd.val
// }

// func (cmd *FTProfileAggregateCmd) readReply(rd *proto.Reader) (err error) {
// 	_, err = rd.ReadArrayLen()
// 	if err != nil {
// 		return err
// 	}
// 	_, err = rd.ReadArrayLen()
// 	if err != nil {
// 		return err
// 	}
// 	cmd.val = FTProfileAggregateResult{}
// 	status, err := rd.ReadInt()
// 	if err != nil {
// 		return err
// 	}
// 	cmd.val.aggregateResult.Status = status
// 	nn, err := rd.ReadArrayLen()
// 	if err != nil {
// 		return err
// 	}
// 	cmd.val.aggregateResult.Fields = make(map[string]string, nn/2)
// 	for i := 0; i < nn; i++ {
// 		key, err := rd.ReadString()
// 		if err != nil {
// 			return err
// 		}

// 		value, err := rd.ReadString()
// 		if err != nil {
// 			return err
// 		}

// 		cmd.val.aggregateResult.Fields[key] = value
// 	}
// 	cmd.val.profileResult = *NewKeyValueSliceCmd(cmd.ctx, cmd.args...)
// 	return nil

// }

// For more details about spellcheck query please follow:
// https://redis.io/docs/interact/search-and-query/advanced-concepts/spellcheck/
func (c cmdable) FTSpellCheck(ctx context.Context, index string, query string) *MapStringInterfaceCmd {
	cmd := NewMapStringInterfaceCmd(ctx, "FT.SPELLCHECK", index, query)
	_ = c(ctx, cmd)
	return cmd
}

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

func (c cmdable) FTSearch(ctx context.Context, index string, query string) *Cmd {
	args := []interface{}{"FT.SEARCH", index, query}
	cmd := NewCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

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
			args = append(args, "RETURN", len(options.Return))
			for _, ret := range options.Return {
				args = append(args, ret.FieldName)
				if ret.As != "" {
					args = append(args, "AS", ret.As)
				}
			}
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

func (c cmdable) FTSynDump(ctx context.Context, index string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "FT.SYNDUMP", index)
	_ = c(ctx, cmd)
	return cmd
}

func (c cmdable) FTSynUpdate(ctx context.Context, index string, synGroupId interface{}, terms []interface{}) *StatusCmd {
	args := []interface{}{"FT.SYNUPDATE", index, synGroupId}
	args = append(args, terms...)
	cmd := NewStatusCmd(ctx, args...)
	_ = c(ctx, cmd)
	return cmd
}

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

func (c cmdable) FTTagVals(ctx context.Context, index string, field string) *StringSliceCmd {
	cmd := NewStringSliceCmd(ctx, "FT.TAGVALS", index, field)
	_ = c(ctx, cmd)
	return cmd
}
