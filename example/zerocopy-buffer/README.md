# Zero-Copy Buffer Operations

`GetToBuffer` and `SetFromBuffer` let you read and write Redis values directly
from/to pre-allocated byte buffers, eliminating data allocations entirely. This
is useful when working with large values (KB to GB) where reducing GC pressure
and memory copies matters.

## API

```go
// Write: sends buf contents directly to the socket, no string conversion.
client.SetFromBuffer(ctx, key, buf)

// Read: reads response payload directly into buf, no intermediate allocation.
cmd := client.GetToBuffer(ctx, key, buf)
n   := cmd.Val()       // number of bytes read
data := cmd.Bytes()    // buf[:n]
```

## How It Works

### Write Path (`SetFromBuffer`)

```
 User buffer           proto.Writer             net.Conn (socket)
┌──────────┐      ┌───────────────────┐      ┌──────────────┐
│  buf      │      │ 1. Write RESP     │      │              │
│  ([]byte) │──────│    header only    │──────│  $<len>\r\n  │  ← buffered write
│           │      │    ($<len>\r\n)   │      │              │
│           │      │                   │      │              │
│           │──────│ 2. Skip buffering │──────│  <raw data>  │  ← direct socket write
│           │      │    write buf      │      │  \r\n        │
└──────────┘      │    directly to    │      └──────────────┘
                   │    net.Conn       │
                   └───────────────────┘
```

1. `proto.Writer.zeroCopyBytes()` writes only the RESP bulk string header
   (`$<len>\r\n`) into the buffered writer, and stores a reference to the
   user's buffer.
2. `pool.Conn.WithWriter()` detects pending zero-copy data, flushes the
   buffered header, then calls `net.Conn.Write(buf)` directly — the data goes
   from the user's buffer to the kernel socket buffer with no intermediate
   copies.

### Read Path (`GetToBuffer`)

```
 net.Conn (socket)     bufio.Reader            User buffer
┌──────────────┐    ┌───────────────────┐    ┌──────────┐
│  $<len>\r\n  │────│ 1. ReadLine()     │    │          │
│              │    │    parse RESP     │    │          │
│              │    │    header         │    │          │
│              │    │                   │    │          │
│  <raw data>  │────│ 2. io.ReadFull()  │────│  buf     │
│              │    │    drain internal │    │  ([]byte)│
│              │    │    buffer, then   │    │          │
│              │    │    read remaining │    │          │
│  \r\n        │────│ 3. discard CRLF  │    └──────────┘
└──────────────┘    └───────────────────┘
```

1. `proto.Reader.ReadStringInto(buf)` calls `ReadLine()` to parse the RESP
   header (`$<len>\r\n`) through the standard buffered reader. This is
   important — it means push notifications, errors, and nil responses are
   handled correctly through the normal code path.
2. `io.ReadFull(bufio.Reader, buf[:n])` reads the bulk data directly into the
   user's buffer. `bufio.Reader.Read()` first drains any data already in its
   internal buffer, then for remaining bytes larger than its buffer size, reads
   directly from the underlying socket — achieving near zero-copy for the
   payload.
3. The trailing `\r\n` is consumed and discarded.

## Benchmarks

1 MB GET — `go test -bench=BenchmarkZeroCopyGet_1MB -benchmem -run=NONE .`:

| Method | ns/op | B/op | allocs/op |
|--------|------:|-----:|----------:|
| Regular Get | ~139,000 | 1,056,960 | 6 |
| **GetToBuffer** | **~109,000** | **202** | **6** |

The allocation count is the same (cmd struct, args slice, pool ops), but
**GetToBuffer allocates a constant 202 bytes regardless of payload size** —
the 1 MB data allocation is completely eliminated.

## Limitations

- **Buffer sizing**: the caller must provide a buffer large enough for the
  value. If the value exceeds the buffer, an error is returned.
- **No automatic retry**: both `GetToBuffer` and `SetFromBuffer` set
  `NoRetry = true`. A retry after a partial read/write would corrupt the
  buffer.
- **No expiration on SetFromBuffer**: use `Expire()` separately if needed.