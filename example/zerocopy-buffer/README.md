# Zero-copy buffer operations

`GetToBuffer` and `SetFromBuffer` read and write Redis values directly
from/to pre-allocated byte buffers, eliminating the per-call payload
allocation that `Get` / `Set` would do. Useful when working with values
in the KiB–MiB range where GC pressure matters.

## API

```go
// Write: sends buf contents to Redis, no string conversion.
client.SetFromBuffer(ctx, key, buf)

// Read: reads response payload directly into buf, no intermediate allocation.
cmd := client.GetToBuffer(ctx, key, buf)
n   := cmd.Val()    // number of bytes read
data := cmd.Bytes() // buf[:n]
```

Both methods are available on `*Client`, `*ClusterClient`, `*Ring`, `*Conn`
and `Pipeliner`.

## How it works

Both paths rely on a property of Go's `bufio` package: when the data being
read or written is larger than the internal buffer (32 KiB by default),
`bufio.Reader.Read` and `bufio.Writer.Write` bypass the internal buffer and
transfer data directly between the user's buffer and the underlying socket.

No special marker types, no raw socket access, no deferred writes — just
the standard buffered I/O path with the optimization that `bufio` already
provides.

### Write path (`SetFromBuffer`)

```
 User buffer          bufio.Writer             net.Conn (socket)
┌──────────┐      ┌───────────────────┐      ┌──────────────┐
│  buf     │      │ 1. Write header   │      │              │
│ ([]byte) │      │    *3\r\n$3\r\n.. │      │  RESP header │  ← small, buffered
│          │      │                   │      │              │
│          │──────│ 2. Write(buf)     │──────│  <raw data>  │  ← large, direct
│          │      │    bypasses       │      │  \r\n        │
└──────────┘      │    internal buf   │      └──────────────┘
                  └───────────────────┘
```

1. The RESP command header
   (`*3\r\n$3\r\nSET\r\n$<keylen>\r\n<key>\r\n$<len>\r\n`) is written into
   `bufio.Writer`'s internal buffer — it's small.
2. `bufio.Writer.Write(buf)` sees that `buf` is larger than its remaining
   buffer space, flushes the header, then writes `buf` directly to the
   socket. The data goes from the user's buffer to the kernel with no
   intermediate copy.

### Read path (`GetToBuffer`)

```
 net.Conn (socket)    bufio.Reader            User buffer
┌──────────────┐    ┌───────────────────┐    ┌──────────┐
│  $<len>\r\n  │────│ 1. ReadLine()     │    │          │
│              │    │    parse RESP     │    │          │
│              │    │    header         │    │          │
│              │    │                   │    │          │
│  <raw data>  │────│ 2. io.ReadFull()  │────│  buf     │
│              │    │    drain internal │    │ ([]byte) │
│              │    │    buffer, then   │    │          │
│              │    │    read remaining │    │          │
│  \r\n        │────│ 3. discard CRLF   │    └──────────┘
└──────────────┘    └───────────────────┘
```

1. `proto.Reader.ReadStringInto(buf)` calls `ReadLine()` to parse the RESP
   header (`$<len>\r\n`) through the standard buffered reader. This means
   push notifications, errors and nil responses are all handled correctly
   through the normal code path.
2. `io.ReadFull(bufio.Reader, buf[:n])` reads the bulk data into the user's
   buffer. `bufio.Reader.Read()` first drains any data already in its
   internal buffer, then for the remaining bytes reads directly from the
   underlying socket into `buf`.
3. The trailing `\r\n` is consumed and discarded.

## Benchmarks

Headline numbers for 1 MiB payloads against the test stack
(`make docker.start` + `localhost:6379`, Apple M4 Max, 5 samples, medians):

| Method            | ns/op       | B/op        | allocs/op |
| ----------------- | ----------: | ----------: | --------: |
| `Get`             |   5 007 885 |   1 058 497 |        10 |
| **`GetToBuffer`** | **4 978 895** | **21 227**  |     **7** |
| Raw `net.Conn`    |   4 976 219 |      21 133 |         1 |

| Method                | ns/op     | B/op    | allocs/op |
| --------------------- | --------: | ------: | --------: |
| `Set([]byte)`         | 3 349 685 |   1 260 |        10 |
| **`SetFromBuffer`**   | 3 426 387 |   1 228 |         9 |

`GetToBuffer` matches a hand-written raw-socket implementation on both
latency (within 3 µs at 1 MiB) and allocation footprint (within 100 B). The
vanilla `Get` path allocates a full payload-sized slice every call;
`GetToBuffer` removes that completely. 

## Limitations

- **Buffer sizing**: the caller must provide a buffer large enough for the
  value. If the value exceeds the buffer, `cmd.Err()` returns
  `"buffer too small"` and no extra bytes are consumed from the reader.
- **No automatic retry for `GetToBuffer`**: `ZeroCopyStringCmd.NoRetry()`
  is `true` because a retry after a partial read would corrupt the buffer.
  This also disables retry for any pipeline that contains a `GetToBuffer`.
- **No expiration on `SetFromBuffer`**: use `Expire()` separately if
  needed.
- **Shared buffers across commands**: a `*ZeroCopyStringCmd` holds a
  reference to the buffer passed in. Reusing that buffer for a later
  `GetToBuffer` will overwrite earlier results — Example 5 in `main.go`
  demonstrates this hazard.

## Run

```sh
docker run --rm -p 6379:6379 redis        # or: make docker.start (from repo root)
go run .
```
