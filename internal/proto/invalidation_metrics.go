package proto

import "sync/atomic"

// InvalidationBytesRead tracks total bytes consumed from "invalidate" push
// frames across all connections. Incremented in the invalidate notification
// handler by summing the serialized key-name lengths. Use Load() to read the
// counter; it is package-level and never reset by the library itself.
var InvalidationBytesRead atomic.Int64
