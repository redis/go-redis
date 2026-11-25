package redis_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
	"github.com/redis/go-redis/v9/push"
)

// peekPushProcessor drains a pending push frame like the built-in processor:
// it only reads a frame after peeking the reply type, so an ordinary reply is
// never mistaken for a notification.
type peekPushProcessor struct{ push.NotificationProcessor }

func (peekPushProcessor) ProcessPendingNotifications(_ context.Context, _ push.NotificationHandlerContext, rd *proto.Reader) error {
	typ, err := rd.PeekReplyType()
	if err != nil {
		return err
	}
	if typ != proto.RespPush {
		return nil
	}
	_, err = rd.ReadReply()
	return err
}

func readScriptedCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return nil, fmt.Errorf("expected array header, got %q", line)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		hdr = strings.TrimRight(hdr, "\r\n")
		length, err := strconv.Atoi(strings.TrimPrefix(hdr, "$"))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, length+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:length]))
	}
	return args, nil
}

// TestPushDrainOversizedReplyLengthKeepsDoError reproduces the desync a
// rejected bulk length used to leave behind: a pending RESP3 push frame
// carries an unreadable bulk element, the pre-reply drain consumes the frame's
// header and fails, and the client logs the drain error and reads the command
// reply on the same connection. Without the Reader marking the rejected header
// fatal, the remaining frame bytes were decoded as that reply and PING returned
// a bogus success; with the guard the command fails with the protocol error.
func TestPushDrainOversizedReplyLengthKeepsDoError(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	oversized := "$" + strconv.Itoa(math.MaxInt-1) + "\r\n"
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				rd := bufio.NewReader(c)
				for {
					args, err := readScriptedCommand(rd)
					if err != nil {
						return
					}
					switch strings.ToLower(args[0]) {
					case "hello":
						// RESP2-style HELLO reply: the client parses both forms.
						_, _ = io.WriteString(c, "*4\r\n$6\r\nserver\r\n$5\r\nredis\r\n$5\r\nproto\r\n:3\r\n")
					case "ping":
						// A push frame that cannot be drained mid-frame, then
						// the reply the client would parse next.
						_, _ = io.WriteString(c, ">2\r\n+message\r\n"+oversized+"+PONG\r\n")
					default:
						_, _ = io.WriteString(c, "+OK\r\n")
					}
				}
			}()
		}
	}()

	client := redis.NewClient(&redis.Options{
		Addr:                      ln.Addr().String(),
		Protocol:                  3,
		DisableIdentity:           true,
		PoolSize:                  1,
		MaxRetries:                -1,
		ReadTimeout:               time.Second,
		WriteTimeout:              time.Second,
		PushNotificationProcessor: peekPushProcessor{push.NewProcessor()},
		MaintNotificationsConfig:  &maintnotifications.Config{Mode: maintnotifications.ModeDisabled},
	})
	defer client.Close()

	got, err := client.Ping(context.Background()).Result()
	if err == nil {
		t.Fatalf("PING succeeded with %q; the desynced stream was decoded as the reply", got)
	}
	if !strings.Contains(err.Error(), "bulk length") {
		t.Fatalf("PING error = %v; want the rejected bulk length error", err)
	}
}
