# Zero-Copy Buffer Operations

`GetToBuffer` and `SetFromBuffer` let you read and write Redis values directly
from/to pre-allocated byte buffers, eliminating data allocations entirely. This
is useful when working with large values (KB to GB) where reducing GC pressure
and memory copies matters.

## API

```go
// Write: sends buf contents to Redis, no string conversion.
client.SetFromBuffer(ctx, key, buf)

// Read: reads response payload directly into buf, no intermediate allocation.
cmd := client.GetToBuffer(ctx, key, buf)
n   := cmd.Val()       // number of bytes read
data := cmd.Bytes()    // buf[:n]
```

## How It Works

Both paths rely on a key property of Go's `bufio` package: when the data being
read or written is larger than the internal buffer (typically 4–32 KB),
`bufio.Reader.Read` and `bufio.Writer.Write` bypass the internal buffer and
transfer data directly between the user's buffer and the underlying socket.

No special marker types, no raw socket access, no deferred writes — just the
standard buffered I/O path with the optimization that `bufio` already provides.

### Write Path (`SetFromBuffer`)

```
 User buffer          bufio.Writer             net.Conn (socket)
┌──────────┐      ┌───────────────────┐      ┌──────────────┐
│  buf      │      │ 1. Write header   │      │              │
│  ([]byte) │      │    *3\r\n$3\r\n.. │      │  RESP header │  ← small, buffered
│           │      │                   │      │              │
│           │──────│ 2. Write(buf)     │──────│  <raw data>  │  ← large, direct
│           │      │    bypasses       │      │  \r\n        │
└──────────┘      │    internal buf   │      └──────────────┘
                   └───────────────────┘
```

1. The RESP command header (`*3\r\n$3\r\nSET\r\n$<keylen>\r\n<key>\r\n$<len>\r\n`)
   is written into `bufio.Writer`'s internal buffer — it's small.
2. `bufio.Writer.Write(buf)` sees that `buf` is larger than its remaining
   buffer space, flushes the header, then writes `buf` directly to the socket.
   The data goes from the user's buffer to the kernel with no intermediate copy.

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
   header (`$<len>\r\n`) through the standard buffered reader. This means push
   notifications, errors, and nil responses are all handled correctly through
   the normal code path.
2. `io.ReadFull(bufio.Reader, buf[:n])` reads the bulk data directly into the
   user's buffer. `bufio.Reader.Read()` first drains any data already in its
   internal buffer, then for the remaining bytes reads directly from the
   underlying socket into `buf`.
3. The trailing `\r\n` is consumed and discarded.

## Benchmarks

1 MB payloads — `go test -bench=BenchmarkZeroCopy -benchmem -run=NONE .`:

### GET

| Method | ns/op | B/op | allocs/op |
|--------|------:|-----:|----------:|
| Regular Get | ~140,000 | 1,056,960 | 6 |
| **GetToBuffer** | **~109,000** | **202** | **6** |

### SET

| Method | ns/op | B/op | allocs/op |
|--------|------:|-----:|----------:|
| Regular Set | ~100,000 | 274 | 8 |
| **SetFromBuffer** | **~100,000** | **226** | **7** |

### Round-trip (SET + GET)

| Method | ns/op | B/op | allocs/op |
|--------|------:|-----:|----------:|
| Regular Set+Get | ~221,000 | 1,057,236 | 14 |
| **SetFromBuffer+GetToBuffer** | **~200,000** | **428** | **13** |

The allocation count is similar, but **the bytes allocated are vastly
different**. `GetToBuffer` allocates a constant 202 bytes regardless of payload
size — the ~1 MB data allocation is completely eliminated.

## Limitations

- **Buffer sizing**: the caller must provide a buffer large enough for the
  value. If the value exceeds the buffer, an error is returned.
- **No automatic retry for GetToBuffer**: `GetToBuffer` sets `NoRetry = true`
  because a retry after a partial read would corrupt the buffer.
- **No expiration on SetFromBuffer**: use `Expire()` separately if needed.