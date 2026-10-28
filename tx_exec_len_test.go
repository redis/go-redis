package redis_test

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/proto"
)

// txServer is a minimal RESP2 server with Redis' MULTI semantics: WATCH and
// MULTI inside MULTI are rejected without being queued, so EXEC replies with
// fewer results than the client wrote commands. extra adds results to every
// EXEC reply to model a server that sends more than it queued.
func txServer(t *testing.T, extra int) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	_, port, _ := net.SplitHostPort(ln.Addr().String())

	serve := func(c net.Conn) {
		defer c.Close()
		rd := proto.NewReader(c)
		wr := bufio.NewWriter(c)
		inMulti, queued := false, 0
		for {
			v, err := rd.ReadReply()
			if err != nil {
				return
			}
			args, _ := v.([]interface{})
			if len(args) == 0 {
				return
			}
			name := strings.ToLower(fmt.Sprint(args[0]))
			switch {
			case name == "hello":
				wr.WriteString("-ERR unknown command 'hello'\r\n")
			case name == "cluster":
				fmt.Fprintf(wr, "*1\r\n*3\r\n:0\r\n:16383\r\n*2\r\n$9\r\n127.0.0.1\r\n:%s\r\n", port)
			case name == "command":
				wr.WriteString("*0\r\n")
			case inMulti && (name == "watch" || name == "multi"):
				fmt.Fprintf(wr, "-ERR %s inside MULTI is not allowed\r\n", strings.ToUpper(name))
			case name == "multi":
				inMulti, queued = true, 0
				wr.WriteString("+OK\r\n")
			case name == "exec":
				inMulti = false
				fmt.Fprintf(wr, "*%d\r\n", queued+extra)
				for i := 0; i < queued; i++ {
					wr.WriteString("$2\r\ntx\r\n")
				}
				for i := 0; i < extra; i++ {
					wr.WriteString("$5\r\nstale\r\n")
				}
			case inMulti:
				queued++
				wr.WriteString("+QUEUED\r\n")
			case name == "get":
				wr.WriteString("$4\r\nreal\r\n")
			default:
				wr.WriteString("+OK\r\n")
			}
			if err := wr.Flush(); err != nil {
				return
			}
		}
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go serve(c)
		}
	}()
	return ln.Addr().String()
}

// EXEC's result count must match the queued commands. Otherwise results are
// assigned to the wrong commands, or left on a pooled connection and read as
// the reply to a later command.
func TestTxPipelineExecReplyCount(t *testing.T) {
	ctx := context.Background()

	clients := map[string]func(addr string) redis.UniversalClient{
		"Client": func(addr string) redis.UniversalClient {
			return redis.NewClient(&redis.Options{
				Addr: addr, Protocol: 2, DisableIdentity: true,
				PoolSize: 1, ReadTimeout: time.Second,
			})
		},
		"ClusterClient": func(addr string) redis.UniversalClient {
			return redis.NewClusterClient(&redis.ClusterOptions{
				Addrs: []string{addr}, Protocol: 2, DisableIdentity: true,
				PoolSize: 1, ReadTimeout: time.Second, MaxRedirects: -1,
			})
		},
	}

	for name, newClient := range clients {
		t.Run(name+"/WATCH inside MULTI", func(t *testing.T) {
			rdb := newClient(txServer(t, 0))
			defer rdb.Close()

			var get *redis.StringCmd
			_, err := rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "k", "v", 0)
				pipe.Do(ctx, "WATCH", "k")
				get = pipe.Get(ctx, "k")
				return nil
			})
			if err == nil || !strings.Contains(err.Error(), "EXEC returned") {
				t.Fatalf("TxPipelined err = %v, want EXEC reply count error", err)
			}
			if get.Err() == nil {
				t.Fatalf("GET in transaction got %q, want an error", get.Val())
			}
		})

		t.Run(name+"/extra EXEC results", func(t *testing.T) {
			rdb := newClient(txServer(t, 1))
			defer rdb.Close()

			_, err := rdb.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
				pipe.Set(ctx, "k", "v", 0)
				pipe.Get(ctx, "k")
				return nil
			})
			if err == nil {
				t.Fatal("TxPipelined accepted an EXEC reply with an extra result")
			}
			got, err := rdb.Get(ctx, "k").Result()
			if err != nil || got != "real" {
				t.Fatalf("GET after transaction = %q, %v; want \"real\"", got, err)
			}
		})
	}
}
