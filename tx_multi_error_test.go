package redis_test

import (
	"bufio"
	"context"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// TestTxPipelineMultiErrorDiscardsConn checks that a connection whose MULTI was
// rejected is not reused. Redis still answers every command written after the
// rejected MULTI (they run outside a transaction) and EXEC, so those replies are
// owed on the wire. An ACL user without @transaction gets -NOPERM for MULTI; when
// one of the commands blocks (BLMOVE), its reply arrives after the transaction
// has returned and must not be read as the next caller's reply.
func TestTxPipelineMultiErrorDiscardsConn(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveNoMultiACL(conn)
		}
	}()

	client := redis.NewClient(&redis.Options{
		Addr:             ln.Addr().String(),
		Protocol:         2,
		DisableIdentity:  true,
		PoolSize:         1,
		PipelinePoolSize: -1,
		MaxRetries:       -1,
	})
	defer client.Close()
	ctx := context.Background()

	_, err = client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
		pipe.BLMove(ctx, "q", "q2", "LEFT", "LEFT", time.Second)
		return nil
	})
	if err == nil || !strings.HasPrefix(err.Error(), "NOPERM") {
		t.Fatalf("TxPipelined err = %v, want NOPERM", err)
	}

	val, err := client.Get(ctx, "b").Result()
	if err != nil || val != "BBB" {
		t.Fatalf("GET b = %q, %v; want \"BBB\" (read a reply owed to the rejected transaction)", val, err)
	}
}

// serveNoMultiACL answers like Redis for a user that may not run MULTI.
func serveNoMultiACL(conn net.Conn) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	for {
		args, err := readRESPCommand(rd)
		if err != nil {
			return
		}
		var reply string
		switch strings.ToUpper(args[0]) {
		case "MULTI":
			reply = "-NOPERM User rw has no permissions to run the 'multi' command\r\n"
		case "EXEC":
			reply = "-ERR EXEC without MULTI\r\n"
		case "BLMOVE":
			time.Sleep(100 * time.Millisecond)
			reply = "$3\r\njob\r\n"
		case "GET":
			reply = "$3\r\nBBB\r\n"
		case "PING":
			reply = "+PONG\r\n"
		default:
			reply = "-ERR unknown command '" + args[0] + "'\r\n"
		}
		if _, err := io.WriteString(conn, reply); err != nil {
			return
		}
	}
}

func readRESPCommand(rd *bufio.Reader) ([]string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(line[1:]))
	if err != nil {
		return nil, err
	}
	args := make([]string, n)
	for i := range args {
		line, err := rd.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(line[1:]))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:size])
	}
	return args, nil
}
