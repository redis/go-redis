# Connection Check Closure Optimization

## Problem

The `connCheck` function in `internal/pool/conn_check.go` was allocating a closure on every call:

```go
func connCheck(conn net.Conn) error {
    var sysErr error
    
    if err := rawConn.Read(func(fd uintptr) bool {  // ← Closure allocation!
        var buf [1]byte
        n, _, err := syscall.Recvfrom(int(fd), buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
        
        switch {
        case n == 0 && err == nil:
            sysErr = io.EOF  // ← Captures sysErr
        case n > 0:
            sysErr = errUnexpectedRead
        ...
        }
        return true
    }); err != nil {
        return err
    }
    
    return sysErr
}
```

**Why it allocates:**
- The closure captures the `sysErr` variable
- Closures that capture variables must be heap-allocated
- This happens on **every** `connCheck` call

## Solution

Use a **global pre-allocated checker** with a method instead of a closure:

```go
type connChecker struct {
    mu     sync.Mutex
    sysErr error
    buf    [1]byte
}

var globalChecker = &connChecker{}

// Method - no closure allocation!
func (c *connChecker) checkFn(fd uintptr) bool {
    n, _, err := syscall.Recvfrom(int(fd), c.buf[:], syscall.MSG_PEEK|syscall.MSG_DONTWAIT)
    
    switch {
    case n == 0 && err == nil:
        c.sysErr = io.EOF
    case n > 0:
        c.sysErr = errUnexpectedRead
    case err == syscall.EAGAIN || err == syscall.EWOULDBLOCK:
        c.sysErr = nil
    default:
        c.sysErr = err
    }
    return true
}

func connCheck(conn net.Conn) error {
    // ... setup code ...
    
    // Use global checker with mutex
    globalChecker.mu.Lock()
    globalChecker.sysErr = nil
    
    // Method reference - no closure!
    err = rawConn.Read(globalChecker.checkFn)
    result := globalChecker.sysErr
    globalChecker.mu.Unlock()
    
    if err != nil {
        return err
    }
    return result
}
```

## Why This Works

**Method vs Closure:**
- **Closure**: `func() { ... }` - captures variables, allocates on heap
- **Method**: `obj.method` - just a function pointer, no allocation

**Thread Safety:**
- Use `sync.Mutex` to protect the global checker
- Only one goroutine can use it at a time
- Mutex overhead is much less than allocation overhead

## Alternative Approaches Considered

### 1. sync.Pool (Initial Attempt)

```go
var connCheckerPool = sync.Pool{
    New: func() interface{} {
        return &connChecker{}
    },
}

func connCheck(conn net.Conn) error {
    checker := connCheckerPool.Get().(*connChecker)
    defer connCheckerPool.Put(checker)
    // ...
}
```

**Problem**: 
- `sync.Pool.Get()` and `Put()` have overhead
- Type assertion `(*connChecker)` adds cost
- **Result**: 33% slower (220746 ns → 295233 ns)

### 2. Global Checker with Mutex (Final Solution)

```go
var globalChecker = &connChecker{}

func connCheck(conn net.Conn) error {
    globalChecker.mu.Lock()
    defer globalChecker.mu.Unlock()
    // ...
}
```

**Benefits**:
- No pool overhead
- No type assertion
- Mutex is very fast for uncontended locks
- **Result**: 2.8% faster than before (220746 ns → 214673 ns)

## Benchmark Results

### Before (with closure allocation):
```
BenchmarkClientCommands/Get-16    16054    220746 ns/op    56 B/op    4 allocs/op
```

### After (with global checker):
```
BenchmarkClientCommands/Get-16    15930    214673 ns/op    40 B/op    3 allocs/op
```

### Improvements:
- **2.8% faster** (220746 → 214673 ns)
- **29% less memory** (56 B → 40 B)
- **1 fewer allocation** (4 → 3)

## Why Mutex is Fast Enough

**Mutex overhead:**
- Uncontended lock/unlock: ~10-20 ns
- Our operation: ~214,000 ns
- Mutex overhead: **< 0.01%**

**Allocation overhead:**
- Heap allocation: ~50-100 ns
- GC pressure: additional cost
- Mutex is **5-10x faster** than allocation

## Thread Safety Analysis

**Is the mutex necessary?**

Yes, because:
1. Multiple goroutines can call `connCheck` concurrently
2. They would race on `globalChecker.sysErr`
3. The mutex ensures only one goroutine uses the checker at a time

**Could we use per-connection checkers?**

No, because:
1. Connections are pooled and reused
2. Would need to allocate checker per connection
3. Defeats the purpose of avoiding allocations

**Could we use thread-local storage?**

No, because:
1. Go doesn't have true thread-local storage
2. Would need to use `sync.Map` or similar
3. More complex and slower than mutex

## Key Insights

### 1. Method References Don't Allocate

```go
// Allocates closure
rawConn.Read(func(fd uintptr) bool { ... })

// No allocation - just a method reference
rawConn.Read(obj.method)
```

### 2. Global State + Mutex < Allocation

For short-lived operations:
- **Mutex**: ~10-20 ns
- **Allocation**: ~50-100 ns + GC pressure
- **Winner**: Mutex

### 3. sync.Pool Has Overhead

`sync.Pool` is great for:
- Large objects
- Frequent allocation/deallocation
- High contention

But for tiny objects with low contention:
- Pool overhead > allocation savings
- Global + mutex is faster

## Real-World Impact

**For 1M operations/second:**

### Before:
- 4M allocations/sec
- 56 MB/sec allocated

### After:
- 3M allocations/sec (**25% reduction**)
- 40 MB/sec allocated (**29% reduction**)

### Annual Savings:
- **31.5 billion fewer allocations** per year
- **504 TB less memory** allocated per year

## Conclusion

By replacing a closure with a method reference and using a global checker with mutex, we:

✅ **Eliminated 1 allocation** per operation  
✅ **Reduced memory by 29%** (56 B → 40 B)  
✅ **Improved speed by 2.8%** (220746 ns → 214673 ns)  

**Key Lesson**: Sometimes the simplest solution (global + mutex) beats the "clever" solution (sync.Pool) when the overhead of the clever solution exceeds the problem it's solving.

## Code Changes

**File**: `internal/pool/conn_check.go`

**Lines changed**: ~30 lines

**Complexity**: Low - simple struct with mutex

**Risk**: Low - mutex ensures thread safety

**Benefit**: High - 1 fewer allocation, faster, less memory

This optimization demonstrates that **understanding the cost of abstractions** (closures, pools, type assertions) is crucial for performance optimization.

