package redis_test

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v8"
	"github.com/go-redis/redis/v8/internal/proto"
	"net"
	"strings"
	"testing"
	"time"
)

type Conn struct {
	buff []byte
}

func (c *Conn) Read(b []byte) (n int, err error)   { return copy(b, c.buff), nil }
func (c *Conn) Write(b []byte) (n int, err error)  { return len(b), nil }
func (c *Conn) Close() error                       { return nil }
func (c *Conn) LocalAddr() net.Addr                { return nil }
func (c *Conn) RemoteAddr() net.Addr               { return nil }
func (c *Conn) SetDeadline(_ time.Time) error      { return nil }
func (c *Conn) SetReadDeadline(_ time.Time) error  { return nil }
func (c *Conn) SetWriteDeadline(_ time.Time) error { return nil }

func NewDecodeClient() *RespDecode {
	conn := &Conn{}
	client := redis.NewClient(&redis.Options{
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
	conn := &Conn{}
	client := redis.NewClusterClient(&redis.ClusterOptions{
		PoolSize: 128,
		Addrs:    []string{"127.0.0.1:6379"},
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return conn, nil
		},
		ClusterSlots: func(_ context.Context) ([]redis.ClusterSlot, error) {
			return []redis.ClusterSlot{
				{
					Start: 0,
					End:   16383,
					Nodes: []redis.ClusterNode{{Addr: "127.0.0.1:6379"}},
				},
			}, nil
		},
	})
	// init command.
	cmd := []string{
		// set
		"*7\r\n+set\r\n:-3\r\n*2\r\n+write\r\n+denyoom\r\n:1\r\n:1\r\n:1\r\n" +
			"*3\r\n+@write\r\n+@string\r\n+@slow\r\n",

		// get
		"*7\r\n+get\r\n:2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:1\r\n:1\r\n" +
			"*3\r\n+@read\r\n+@string\r\n+@fast\r\n",

		// del
		"*7\r\n+del\r\n:-2\r\n*1\r\n+write\r\n:1\r\n:-1\r\n:1\r\n" +
			"*3\r\n+@keyspace\r\n+@write\r\n+@slow\r\n",

		// incr
		"*7\r\n+incr\r\n:2\r\n*3\r\n+write\r\n+denyoom\r\n+fast\r\n:1\r\n:1\r\n:1\r\n" +
			"*3\r\n+@write\r\n+@string\r\n+@fast\r\n",

		// mget
		"*7\r\n+mget\r\n:-2\r\n*2\r\n+readonly\r\n+fast\r\n:1\r\n:-1\r\n:1\r\n" +
			"*3\r\n+@read\r\n+@string\r\n+@fast\r\n",

		// multi
		"*7\r\n+multi\r\n:1\r\n*4\r\n+noscript\r\n+loading\r\n+stale\r\n+fast\r\n:0\r\n:0\r\n:0\r\n" +
			"*2\r\n+@fast\r\n+@transaction\r\n",

		// exec
		"*7\r\n+exec\r\n:1\r\n*5\r\n+noscript\r\n+loading\r\n+stale\r\n+skip_monitor\r\n+skip_slowlog\r\n" +
			":0\r\n:0\r\n:0\r\n*2\r\n+@slow\r\n+@transaction\r\n",

		// ping
		"*7\r\n+ping\r\n:-1\r\n*2\r\n+stale\r\n+fast\r\n:0\r\n:0\r\n:0\r\n*2\r\n+@fast\r\n+@connection\r\n",
	}
	conn.buff = []byte(fmt.Sprintf("*%d\r\n%s", len(cmd), strings.Join(cmd, "")))
	client.Ping(ctx)
	conn.buff = conn.buff[:0]

	return &RespDecode{
		name:   "Cluster",
		conn:   conn,
		client: client,
	}
}

type RespDecode struct {
	name   string
	conn   *Conn
	client redis.Cmdable
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

func respError(b *testing.B, conn *Conn, client redis.Cmdable) {
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

func respStatus(b *testing.B, conn *Conn, client redis.Cmdable) {
	conn.buff = []byte("+OK\r\n")
	var val string

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = client.Set(ctx, "key", "value", 0).Val(); val != "OK" {
			b.Fatalf("response error, got %q, want OK", val)
		}
	}
}

func respInt(b *testing.B, conn *Conn, client redis.Cmdable) {
	conn.buff = []byte(":10\r\n")
	var val int64

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = client.Incr(ctx, "key").Val(); val != 10 {
			b.Fatalf("response error, got %q, want 10", val)
		}
	}
}

func respString(b *testing.B, conn *Conn, client redis.Cmdable) {
	conn.buff = []byte("$5\r\nhello\r\n")
	var val string

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = client.Get(ctx, "key").Val(); val != "hello" {
			b.Fatalf("response error, got %q, want hello", val)
		}
	}
}

func respArray(b *testing.B, conn *Conn, client redis.Cmdable) {
	conn.buff = []byte("*3\r\n$5\r\nhello\r\n:10\r\n+OK\r\n")
	var val []interface{}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if val = client.MGet(ctx, "key").Val(); len(val) != 3 {
			b.Fatalf("response error, got len(%d), want len(3)", len(val))
		}
	}
}

func respPipeline(b *testing.B, conn *Conn, client redis.Cmdable) {
	conn.buff = []byte("+OK\r\n$5\r\nhello\r\n:1\r\n")
	var pipe redis.Pipeliner

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

func respTxPipeline(b *testing.B, conn *Conn, client redis.Cmdable) {
	conn.buff = []byte("+OK\r\n+QUEUED\r\n+QUEUED\r\n+QUEUED\r\n*3\r\n+OK\r\n$5\r\nhello\r\n:1\r\n")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var set *redis.StatusCmd
		var get *redis.StringCmd
		var del *redis.IntCmd
		_, err := client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
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

func dynamicGoroutine(b *testing.B, conn *Conn, client redis.Cmdable, concurrency int) {
	conn.buff = []byte("$5\r\nhello\r\n")
	c := make(chan struct{}, concurrency)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c <- struct{}{}
		go func() {
			if val := client.Get(ctx, "key").Val(); val != "hello" {
				b.Fatalf("response error, got %q, want hello", val)
			}
			<-c
		}()
	}
	close(c)
}

func staticGoroutine(b *testing.B, conn *Conn, client redis.Cmdable, concurrency int) {
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
					b.Fatalf("response error, got %q, want hello", val)
				}
			}
		}()
	}
	for i := 0; i < b.N; i++ {
		c <- struct{}{}
	}
	close(c)
}