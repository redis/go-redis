package redis_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// TestTxCleanupPoolState uses a protocol peer to model complete Redis error
// replies which leave WATCH or MULTI active. No transport failure is involved.
func TestTxCleanupPoolState(t *testing.T) {
	for _, tc := range []struct {
		name        string
		cleanup     string
		execReply   string
		transaction bool
		watch       bool
		discard     bool
	}{
		{name: "refused UNWATCH", cleanup: "-NOPERM cleanup refused\r\n", watch: true, discard: true},
		{name: "queued UNWATCH", cleanup: "+QUEUED\r\n", watch: true, discard: true},
		{name: "unknown UNWATCH status", cleanup: "+UNKNOWN\r\n", watch: true, discard: true},
		{name: "refused EXEC with WATCH", transaction: true, watch: true, execReply: "-NOPERM exec refused\r\n", discard: true},
		{name: "refused EXEC without WATCH", transaction: true, execReply: "-NOPERM exec refused\r\n", discard: true},
		{name: "successful UNWATCH", cleanup: "+OK\r\n", watch: true},
		{name: "committed EXEC", transaction: true, watch: true, execReply: "*1\r\n+OK\r\n"},
		{name: "aborted EXEC", transaction: true, watch: true, execReply: "*-1\r\n"},
		{name: "EXECABORT", transaction: true, watch: true, execReply: "-EXECABORT transaction discarded\r\n"},
		{name: "command error in EXEC", transaction: true, watch: true, execReply: "*1\r\n-WRONGTYPE command failed\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			var mu sync.Mutex
			counts := make(map[string]int)
			connections := 0
			go func() {
				for {
					conn, err := ln.Accept()
					if err != nil {
						return
					}
					mu.Lock()
					connections++
					id := connections
					mu.Unlock()
					go func() {
						defer conn.Close()
						rd := bufio.NewReader(conn)
						watched, multi := false, false
						for {
							args, err := readRESPCommand(rd)
							if err != nil {
								return
							}
							name := strings.ToLower(args[0])
							mu.Lock()
							counts[name]++
							mu.Unlock()
							var reply string
							switch name {
							case "hello":
								reply = "%1\r\n+proto\r\n:3\r\n"
							case "watch":
								watched = true
								reply = "+OK\r\n"
							case "multi":
								multi = true
								reply = "+OK\r\n"
							case "exec":
								reply = tc.execReply
								if !strings.HasPrefix(reply, "-NOPERM") {
									watched, multi = false, false
								}
							case "unwatch":
								reply = tc.cleanup
								if reply == "" {
									reply = "+OK\r\n"
								}
								if multi {
									reply = "+QUEUED\r\n"
								}
								if reply == "+OK\r\n" {
									watched = false
								}
							case "ping":
								reply = fmt.Sprintf("+conn=%d watched=%t multi=%t\r\n", id, watched, multi)
							default:
								if multi {
									reply = "+QUEUED\r\n"
								} else {
									reply = "+OK\r\n"
								}
							}
							if _, err := io.WriteString(conn, reply); err != nil {
								return
							}
						}
					}()
				}
			}()
			client := redis.NewClient(&redis.Options{
				Addr: ln.Addr().String(), Protocol: 3, DisableIdentity: true,
				MaintNotificationsConfig: &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
				PoolSize:                 1, PipelinePoolSize: -1, ReadTimeout: time.Second,
				MaxRetries: -1,
			})
			defer client.Close()
			ctx := context.Background()
			stop := errors.New("validation stopped before EXEC")
			callbackCalls := 0
			var keys []string
			if tc.watch {
				keys = []string{"source"}
			}
			err = client.Watch(ctx, func(tx *redis.Tx) error {
				callbackCalls++
				if !tc.transaction {
					return stop
				}
				_, err := tx.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
					pipe.Set(ctx, "source", "value", 0)
					return nil
				})
				return err
			}, keys...)
			var wantErr string
			switch {
			case !tc.transaction:
				if err != stop {
					t.Fatalf("Watch error = %v, want original callback error", err)
				}
			case strings.HasPrefix(tc.execReply, "-NOPERM"):
				wantErr = "NOPERM exec refused"
			case strings.HasPrefix(tc.execReply, "-EXECABORT"):
				wantErr = "EXECABORT transaction discarded"
			case strings.Contains(tc.execReply, "WRONGTYPE"):
				wantErr = "WRONGTYPE command failed"
			case tc.execReply == "*-1\r\n":
				wantErr = redis.TxFailedErr.Error()
			}
			if tc.transaction && ((wantErr == "" && err != nil) || (wantErr != "" && (err == nil || err.Error() != wantErr))) {
				t.Fatalf("Watch error = %v, want %q", err, wantErr)
			}
			got, err := client.Ping(ctx).Result()
			if err != nil {
				t.Fatal(err)
			}
			wantID := 1
			if tc.discard {
				wantID = 2
			}
			want := fmt.Sprintf("conn=%d watched=false multi=false", wantID)
			if got != want {
				t.Errorf("next lease = %q, want %q; uncleared transaction state must not be pooled", got, want)
			}
			mu.Lock()
			defer mu.Unlock()
			if callbackCalls != 1 {
				t.Errorf("callback calls = %d, want 1", callbackCalls)
			}
			wantTx := 0
			if tc.transaction {
				wantTx = 1
			}
			for _, name := range []string{"multi", "set", "exec"} {
				if counts[name] != wantTx {
					t.Errorf("%s count = %d, want %d (no replay)", name, counts[name], wantTx)
				}
			}
			if counts["unwatch"] > 1 || counts["discard"] != 0 {
				t.Errorf("cleanup replay: UNWATCH=%d DISCARD=%d", counts["unwatch"], counts["discard"])
			}
		})
	}
}
