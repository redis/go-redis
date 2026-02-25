package redis

// Result types for pipeline operations
// These types wrap (value, error) tuples for channel-based pipeline results

// StringResult wraps a string value and error for pipeline operations
type StringResult struct {
	Val string
	Err error
}

// IntResult wraps an int64 value and error for pipeline operations
type IntResult struct {
	Val int64
	Err error
}

// BoolResult wraps a bool value and error for pipeline operations
type BoolResult struct {
	Val bool
	Err error
}

// FloatResult wraps a float64 value and error for pipeline operations
type FloatResult struct {
	Val float64
	Err error
}

// StatusResult wraps a status string and error for pipeline operations
type StatusResult struct {
	Val string
	Err error
}

// SliceResult wraps a []interface{} value and error for pipeline operations
type SliceResult struct {
	Val []interface{}
	Err error
}

// StringSliceResult wraps a []string value and error for pipeline operations
type StringSliceResult struct {
	Val []string
	Err error
}

// IntSliceResult wraps a []int64 value and error for pipeline operations
type IntSliceResult struct {
	Val []int64
	Err error
}

// FloatSliceResult wraps a []float64 value and error for pipeline operations
type FloatSliceResult struct {
	Val []float64
	Err error
}

// BoolSliceResult wraps a []bool value and error for pipeline operations
type BoolSliceResult struct {
	Val []bool
	Err error
}

// MapStringStringResult wraps a map[string]string value and error for pipeline operations
type MapStringStringResult struct {
	Val map[string]string
	Err error
}

// MapStringIntResult wraps a map[string]int64 value and error for pipeline operations
type MapStringIntResult struct {
	Val map[string]int64
	Err error
}

// MapStringInterfaceResult wraps a map[string]interface{} value and error for pipeline operations
type MapStringInterfaceResult struct {
	Val map[string]interface{}
	Err error
}

// DurationResult wraps a time.Duration value and error for pipeline operations
type DurationResult struct {
	Val int64 // Duration in the appropriate unit (seconds, milliseconds, etc.)
	Err error
}

// TimeResult wraps a time.Time value and error for pipeline operations  
type TimeResult struct {
	Val int64 // Unix timestamp
	Err error
}

// ScanResult wraps scan cursor and keys for pipeline operations
type ScanResult struct {
	Cursor uint64
	Keys   []string
	Err    error
}

// ZResult wraps a Z (sorted set member) and error for pipeline operations
type ZResult struct {
	Val Z
	Err error
}

// ZSliceResult wraps a []Z (sorted set members) and error for pipeline operations
type ZSliceResult struct {
	Val []Z
	Err error
}

// ZWithKeyResult wraps a ZWithKey and error for pipeline operations
type ZWithKeyResult struct {
	Val ZWithKey
	Err error
}

// GeoLocationResult wraps a GeoLocation and error for pipeline operations
type GeoLocationResult struct {
	Val *GeoLocation
	Err error
}

// GeoLocationSliceResult wraps []GeoLocation and error for pipeline operations
type GeoLocationSliceResult struct {
	Val []GeoLocation
	Err error
}

// GeoPosResult wraps a GeoPos and error for pipeline operations
type GeoPosResult struct {
	Val *GeoPos
	Err error
}

// CommandInfoResult wraps a CommandInfo and error for pipeline operations
type CommandInfoResult struct {
	Val *CommandInfo
	Err error
}

// CommandInfoSliceResult wraps []CommandInfo and error for pipeline operations
type CommandInfoSliceResult struct {
	Val []*CommandInfo
	Err error
}

// ClusterSlotResult wraps a ClusterSlot and error for pipeline operations
type ClusterSlotResult struct {
	Val ClusterSlot
	Err error
}

// ClusterSlotSliceResult wraps []ClusterSlot and error for pipeline operations
type ClusterSlotSliceResult struct {
	Val []ClusterSlot
	Err error
}

// XMessageResult wraps an XMessage and error for pipeline operations
type XMessageResult struct {
	Val XMessage
	Err error
}

// XMessageSliceResult wraps []XMessage and error for pipeline operations
type XMessageSliceResult struct {
	Val []XMessage
	Err error
}

// XStreamResult wraps an XStream and error for pipeline operations
type XStreamResult struct {
	Val XStream
	Err error
}

// XStreamSliceResult wraps []XStream and error for pipeline operations
type XStreamSliceResult struct {
	Val []XStream
	Err error
}

// XPendingResult wraps an XPending and error for pipeline operations
type XPendingResult struct {
	Val *XPending
	Err error
}

// XPendingExtResult wraps an XPendingExt and error for pipeline operations
type XPendingExtResult struct {
	Val XPendingExt
	Err error
}

// XPendingExtSliceResult wraps []XPendingExt and error for pipeline operations
type XPendingExtSliceResult struct {
	Val []XPendingExt
	Err error
}

// XInfoGroupResult wraps an XInfoGroup and error for pipeline operations
type XInfoGroupResult struct {
	Val XInfoGroup
	Err error
}

// XInfoGroupSliceResult wraps []XInfoGroup and error for pipeline operations
type XInfoGroupSliceResult struct {
	Val []XInfoGroup
	Err error
}

// XInfoStreamResult wraps an XInfoStream and error for pipeline operations
type XInfoStreamResult struct {
	Val *XInfoStream
	Err error
}

// XInfoConsumerResult wraps an XInfoConsumer and error for pipeline operations
type XInfoConsumerResult struct {
	Val XInfoConsumer
	Err error
}

// XInfoConsumerSliceResult wraps []XInfoConsumer and error for pipeline operations
type XInfoConsumerSliceResult struct {
	Val []XInfoConsumer
	Err error
}

// XAutoClaimResult wraps XAutoClaim result (messages and start cursor) for pipeline operations
type XAutoClaimResult struct {
	Messages []XMessage
	Start    string
	Err      error
}

// XAutoClaimJustIDResult wraps XAutoClaimJustID result (IDs and start cursor) for pipeline operations
type XAutoClaimJustIDResult struct {
	IDs   []string
	Start string
	Err   error
}

// FunctionStatsResult wraps FunctionStats and error for pipeline operations
type FunctionStatsResult struct {
	Val FunctionStats
	Err error
}

// FunctionListQueryResult wraps FunctionListQuery and error for pipeline operations
type FunctionListQueryResult struct {
	Val []FunctionListQuery
	Err error
}

// LibraryInfoResult wraps LibraryInfo and error for pipeline operations
type LibraryInfoResult struct {
	Val LibraryInfo
	Err error
}

// KeyValueResult wraps a KeyValue and error for pipeline operations
type KeyValueResult struct {
	Val KeyValue
	Err error
}

// KeyValueSliceResult wraps []KeyValue and error for pipeline operations
type KeyValueSliceResult struct {
	Val []KeyValue
	Err error
}

// KeyValuesResult wraps a key and values slice for pipeline operations
type KeyValuesResult struct {
	Key    string
	Values []string
	Err    error
}

// KeyFlagsResult wraps a KeyFlags and error for pipeline operations
type KeyFlagsResult struct {
	Val KeyFlags
	Err error
}

// RankResult wraps a RankScore and error for pipeline operations
type RankResult struct {
	Val RankScore
	Err error
}

// DigestResult wraps a uint64 digest and error for pipeline operations
type DigestResult struct {
	Val uint64
	Err error
}

