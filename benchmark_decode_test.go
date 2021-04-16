package redis

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8/internal/proto"
	"net"
	"testing"
	"time"
)

var ctx = context.TODO()

type ConnStub struct {
	buff []byte
	idx  int
}

func (c *ConnStub) Read(b []byte) (n int, err error) {
	n = copy(b, c.buff[c.idx:])
	if len(c.buff) > c.idx+n {
		c.idx += n
	} else {
		c.idx = 0
	}
	return n, nil
}
func (c *ConnStub) Write(b []byte) (n int, err error)  { return len(b), nil }
func (c *ConnStub) Close() error                       { return nil }
func (c *ConnStub) LocalAddr() net.Addr                { return nil }
func (c *ConnStub) RemoteAddr() net.Addr               { return nil }
func (c *ConnStub) SetDeadline(_ time.Time) error      { return nil }
func (c *ConnStub) SetReadDeadline(_ time.Time) error  { return nil }
func (c *ConnStub) SetWriteDeadline(_ time.Time) error { return nil }

type ClientStub struct {
	Cmdable
	name string
	buff []byte
	conn []*ConnStub
}

func (stub *ClientStub) SetResponse(b []byte) {
	stub.buff = make([]byte, len(b))
	copy(stub.buff, b)

	for _, c := range stub.conn {
		c.buff = make([]byte, len(b))
		copy(c.buff, b)
	}
}

func (stub *ClientStub) AppendConn(c *ConnStub) {
	if len(stub.buff) > 0 {
		c.buff = make([]byte, len(stub.buff))
		copy(c.buff, stub.buff)
	}
	stub.conn = append(stub.conn, c)
}

func (stub *ClientStub) Name() string {
	return stub.name
}

func NewDecodeClient() *ClientStub {
	stub := &ClientStub{
		name: "Client",
	}
	stub.Cmdable = NewClient(&Options{
		PoolSize: 128,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			conn := &ConnStub{}
			stub.AppendConn(conn)
			return conn, nil
		},
	})
	return stub
}

func NewDecodeClusterClient() *ClientStub {
	stub := &ClientStub{
		name: "Cluster",
	}
	client := NewClusterClient(&ClusterOptions{
		PoolSize: 128,
		Addrs:    []string{"127.0.0.1:6379"},
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			c := &ConnStub{}
			stub.AppendConn(c)
			return c, nil
		},
		ClusterSlots: func(_ context.Context) ([]ClusterSlot, error) {
			return []ClusterSlot{
				{
					Start: 0,
					End:   16383,
					Nodes: []ClusterNode{{Addr: "127.0.0.1:6379"}},
				},
			}, nil
		},
	})
	// init command.
	tmpClient := NewClient(&Options{Addr: ":6379"})
	cmdCache, err := tmpClient.Command(ctx).Result()
	_ = tmpClient.Close()
	client.cmdsInfoCache = newCmdsInfoCache(func(_ context.Context) (map[string]*CommandInfo, error) {
		return cmdCache, err
	})

	stub.Cmdable = client
	return stub
}

func BenchmarkDecode(b *testing.B) {
	stubs := []*ClientStub{
		NewDecodeClient(),
		NewDecodeClusterClient(),
	}

	for _, stub := range stubs {
		b.Run(fmt.Sprintf("RespError-%s", stub.Name()), func(b *testing.B) {
			respError(b, stub)
		})
		b.Run(fmt.Sprintf("RespStatus-%s", stub.Name()), func(b *testing.B) {
			respStatus(b, stub)
		})
		b.Run(fmt.Sprintf("RespInt-%s", stub.Name()), func(b *testing.B) {
			respInt(b, stub)
		})
		b.Run(fmt.Sprintf("RespString-%s", stub.Name()), func(b *testing.B) {
			respString(b, stub)
		})
		b.Run(fmt.Sprintf("RespArray-%s", stub.Name()), func(b *testing.B) {
			respArray(b, stub)
		})
		b.Run(fmt.Sprintf("RespPipeline-%s", stub.Name()), func(b *testing.B) {
			respPipeline(b, stub)
		})
		b.Run(fmt.Sprintf("RespTxPipeline-%s", stub.Name()), func(b *testing.B) {
			respTxPipeline(b, stub)
		})

		// goroutine
		b.Run(fmt.Sprintf("DynamicGoroutine-%s-pool=5", stub.Name()), func(b *testing.B) {
			dynamicGoroutine(b, stub, 5)
		})
		b.Run(fmt.Sprintf("DynamicGoroutine-%s-pool=20", stub.Name()), func(b *testing.B) {
			dynamicGoroutine(b, stub, 20)
		})
		b.Run(fmt.Sprintf("DynamicGoroutine-%s-pool=50", stub.Name()), func(b *testing.B) {
			dynamicGoroutine(b, stub, 50)
		})
		b.Run(fmt.Sprintf("DynamicGoroutine-%s-pool=100", stub.Name()), func(b *testing.B) {
			dynamicGoroutine(b, stub, 100)
		})

		b.Run(fmt.Sprintf("StaticGoroutine-%s-pool=5", stub.Name()), func(b *testing.B) {
			staticGoroutine(b, stub, 5)
		})
		b.Run(fmt.Sprintf("StaticGoroutine-%s-pool=20", stub.Name()), func(b *testing.B) {
			staticGoroutine(b, stub, 20)
		})
		b.Run(fmt.Sprintf("StaticGoroutine-%s-pool=50", stub.Name()), func(b *testing.B) {
			staticGoroutine(b, stub, 50)
		})
		b.Run(fmt.Sprintf("StaticGoroutine-%s-pool=100", stub.Name()), func(b *testing.B) {
			staticGoroutine(b, stub, 100)
		})
	}
}

func respError(b *testing.B, stub *ClientStub) {
	stub.SetResponse([]byte("-ERR test error\r\n"))
	respErr := proto.RedisError("ERR test error")
	var err error

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = stub.Get(ctx, "key").Err(); err != respErr {
			b.Fatalf("response error, got %q, want %q", err, respErr)
		}
	}
}

func respStatus(b *testing.B, stub *ClientStub) {
	stub.SetResponse([]byte("+OK\r\n"))
	var val string

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = stub.Set(ctx, "key", "value", 0).Val(); val != "OK" {
			b.Fatalf("response error, got %q, want OK", val)
		}
	}
}

func respInt(b *testing.B, stub *ClientStub) {
	stub.SetResponse([]byte(":10\r\n"))
	var val int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = stub.Incr(ctx, "key").Val(); val != 10 {
			b.Fatalf("response error, got %q, want 10", val)
		}
	}
}

func respString(b *testing.B, stub *ClientStub) {
	stub.SetResponse([]byte("$5\r\nhello\r\n"))
	var val string

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = stub.Get(ctx, "key").Val(); val != "hello" {
			b.Fatalf("response error, got %q, want hello", val)
		}
	}
}

func respArray(b *testing.B, stub *ClientStub) {
	stub.SetResponse([]byte("*3\r\n$5\r\nhello\r\n:10\r\n+OK\r\n"))
	var val []interface{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = stub.MGet(ctx, "key").Val(); len(val) != 3 {
			b.Fatalf("response error, got len(%d), want len(3)", len(val))
		}
	}
}

func respPipeline(b *testing.B, stub *ClientStub) {
	stub.SetResponse([]byte("+OK\r\n$5\r\nhello\r\n:1\r\n"))
	var pipe Pipeliner

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipe = stub.Pipeline()
		set := pipe.Set(ctx, "key", "value", 0)
		get := pipe.Get(ctx, "key")
		del := pipe.Del(ctx, "key")
		_, err := pipe.Exec(ctx)
		if err != nil {
			b.Fatalf("response error, got %q, want nil", err)
		}
		if set.Val() != "OK" || get.Val() != "hello" || del.Val() != 1 {
			b.Fatal("response error")
		}
	}
}

func respTxPipeline(b *testing.B, stub *ClientStub) {
	stub.SetResponse([]byte("+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n+OK\r\n$5\r\nhello\r\n:1\r\n"))

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var set *StatusCmd
		var get *StringCmd
		var del *IntCmd
		_, err := stub.TxPipelined(ctx, func(pipe Pipeliner) error {
			set = pipe.Set(ctx, "key", "value", 0)
			get = pipe.Get(ctx, "key")
			del = pipe.Del(ctx, "key")
			return nil
		})
		if err != nil {
			b.Fatalf("response error, got %q, want nil", err)
		}
		if set.Val() != "OK" || get.Val() != "hello" || del.Val() != 1 {
			b.Fatal("response error")
		}
	}
}

func dynamicGoroutine(b *testing.B, stub *ClientStub, concurrency int) {
	stub.SetResponse([]byte("$5\r\nhello\r\n"))
	c := make(chan struct{}, concurrency)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c <- struct{}{}
		go func() {
			if val := stub.Get(ctx, "key").Val(); val != "hello" {
				panic(fmt.Sprintf("response error, got %q, want hello", val))
			}
			<-c
		}()
	}
	// Here no longer wait for all goroutines to complete, it will not affect the test results.
	close(c)
}

func staticGoroutine(b *testing.B, stub *ClientStub, concurrency int) {
	stub.SetResponse([]byte("$5\r\nhello\r\n"))
	c := make(chan struct{}, concurrency)

	b.ResetTimer()

	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				_, ok := <-c
				if !ok {
					return
				}
				if val := stub.Get(ctx, "key").Val(); val != "hello" {
					panic(fmt.Sprintf("response error, got %q, want hello", val))
				}
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		c <- struct{}{}
	}
	close(c)
}
