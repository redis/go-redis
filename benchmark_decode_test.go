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

type BenchConn struct {
	buff []byte
}

func (c *BenchConn) Read(b []byte) (n int, err error)   { return copy(b, c.buff), nil }
func (c *BenchConn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (c *BenchConn) Close() error                       { return nil }
func (c *BenchConn) LocalAddr() net.Addr                { return nil }
func (c *BenchConn) RemoteAddr() net.Addr               { return nil }
func (c *BenchConn) SetDeadline(_ time.Time) error      { return nil }
func (c *BenchConn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *BenchConn) SetWriteDeadline(_ time.Time) error { return nil }

func NewDecodeClient() *RespDecode {
	conn := &BenchConn{}
	client := NewClient(&Options{
		PoolSize: 128,
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return conn, nil
		},
	})
	return &RespDecode{
		name:   "Client",
		conn:   conn,
		client: client,
	}
}

func NewDecodeClusterClient() *RespDecode {
	conn := &BenchConn{}
	client := NewClusterClient(&ClusterOptions{
		PoolSize: 128,
		Addrs:    []string{"127.0.0.1:6379"},
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return conn, nil
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

	return &RespDecode{
		name:   "Cluster",
		conn:   conn,
		client: client,
	}
}

type RespDecode struct {
	name   string
	conn   *BenchConn
	client Cmdable
}

func BenchmarkDecode(b *testing.B) {
	cs := []*RespDecode{
		NewDecodeClient(),
		NewDecodeClusterClient(),
	}

	for _, c := range cs {
		b.Run(fmt.Sprintf("RespError-%s", c.name), func(b *testing.B) {
			respError(b, c.conn, c.client)
		})
		b.Run(fmt.Sprintf("RespStatus-%s", c.name), func(b *testing.B) {
			respStatus(b, c.conn, c.client)
		})
		b.Run(fmt.Sprintf("RespInt-%s", c.name), func(b *testing.B) {
			respInt(b, c.conn, c.client)
		})
		b.Run(fmt.Sprintf("RespString-%s", c.name), func(b *testing.B) {
			respString(b, c.conn, c.client)
		})
		b.Run(fmt.Sprintf("RespArray-%s", c.name), func(b *testing.B) {
			respArray(b, c.conn, c.client)
		})
		b.Run(fmt.Sprintf("RespPipeline-%s", c.name), func(b *testing.B) {
			respPipeline(b, c.conn, c.client)
		})
		b.Run(fmt.Sprintf("RespTxPipeline-%s", c.name), func(b *testing.B) {
			respTxPipeline(b, c.conn, c.client)
		})

		// goroutine
		b.Run(fmt.Sprintf("DynamicGoroutine-%s-pool=5", c.name), func(b *testing.B) {
			dynamicGoroutine(b, c.conn, c.client, 5)
		})
		b.Run(fmt.Sprintf("DynamicGoroutine-%s-pool=20", c.name), func(b *testing.B) {
			dynamicGoroutine(b, c.conn, c.client, 20)
		})
		b.Run(fmt.Sprintf("DynamicGoroutine-%s-pool=50", c.name), func(b *testing.B) {
			dynamicGoroutine(b, c.conn, c.client, 50)
		})
		b.Run(fmt.Sprintf("DynamicGoroutine-%s-pool=100", c.name), func(b *testing.B) {
			dynamicGoroutine(b, c.conn, c.client, 100)
		})

		b.Run(fmt.Sprintf("StaticGoroutine-%s-pool=5", c.name), func(b *testing.B) {
			staticGoroutine(b, c.conn, c.client, 5)
		})
		b.Run(fmt.Sprintf("StaticGoroutine-%s-pool=20", c.name), func(b *testing.B) {
			staticGoroutine(b, c.conn, c.client, 20)
		})
		b.Run(fmt.Sprintf("StaticGoroutine-%s-pool=50", c.name), func(b *testing.B) {
			staticGoroutine(b, c.conn, c.client, 50)
		})
		b.Run(fmt.Sprintf("StaticGoroutine-%s-pool=100", c.name), func(b *testing.B) {
			staticGoroutine(b, c.conn, c.client, 100)
		})
	}
}

func respError(b *testing.B, conn *BenchConn, client Cmdable) {
	conn.buff = []byte("-ERR test error\r\n")
	respErr := proto.RedisError("ERR test error")
	var err error

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err = client.Get(ctx, "key").Err(); err != respErr {
			b.Fatalf("response error, got %q, want %q", err, respErr)
		}
	}
}

func respStatus(b *testing.B, conn *BenchConn, client Cmdable) {
	conn.buff = []byte("+OK\r\n")
	var val string

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = client.Set(ctx, "key", "value", 0).Val(); val != "OK" {
			b.Fatalf("response error, got %q, want OK", val)
		}
	}
}

func respInt(b *testing.B, conn *BenchConn, client Cmdable) {
	conn.buff = []byte(":10\r\n")
	var val int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = client.Incr(ctx, "key").Val(); val != 10 {
			b.Fatalf("response error, got %q, want 10", val)
		}
	}
}

func respString(b *testing.B, conn *BenchConn, client Cmdable) {
	conn.buff = []byte("$5\r\nhello\r\n")
	var val string

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = client.Get(ctx, "key").Val(); val != "hello" {
			b.Fatalf("response error, got %q, want hello", val)
		}
	}
}

func respArray(b *testing.B, conn *BenchConn, client Cmdable) {
	conn.buff = []byte("*3\r\n$5\r\nhello\r\n:10\r\n+OK\r\n")
	var val []interface{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = client.MGet(ctx, "key").Val(); len(val) != 3 {
			b.Fatalf("response error, got len(%d), want len(3)", len(val))
		}
	}
}

func respPipeline(b *testing.B, conn *BenchConn, client Cmdable) {
	conn.buff = []byte("+OK\r\n$5\r\nhello\r\n:1\r\n")
	var pipe Pipeliner

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipe = client.Pipeline()
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

func respTxPipeline(b *testing.B, conn *BenchConn, client Cmdable) {
	conn.buff = []byte("+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n+OK\r\n$5\r\nhello\r\n:1\r\n")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var set *StatusCmd
		var get *StringCmd
		var del *IntCmd
		_, err := client.TxPipelined(ctx, func(pipe Pipeliner) error {
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

func dynamicGoroutine(b *testing.B, conn *BenchConn, client Cmdable, concurrency int) {
	conn.buff = []byte("$5\r\nhello\r\n")
	c := make(chan struct{}, concurrency)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c <- struct{}{}
		go func() {
			if val := client.Get(ctx, "key").Val(); val != "hello" {
				panic(fmt.Sprintf("response error, got %q, want hello", val))
			}
			<-c
		}()
	}
	close(c)
}

func staticGoroutine(b *testing.B, conn *BenchConn, client Cmdable, concurrency int) {
	conn.buff = []byte("$5\r\nhello\r\n")
	c := make(chan struct{}, concurrency)

	b.ResetTimer()

	for i := 0; i < concurrency; i++ {
		go func() {
			for {
				_, ok := <-c
				if !ok {
					return
				}
				if val := client.Get(ctx, "key").Val(); val != "hello" {
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
