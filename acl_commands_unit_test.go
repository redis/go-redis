package redis

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestACLGetUser_Args(t *testing.T) {
	var captured Cmder
	c := captureCmdable(&captured)

	c.ACLGetUser(context.Background(), "alice")
	if want := []any{"acl", "getuser", "alice"}; !reflect.DeepEqual(captured.Args(), want) {
		t.Errorf("args = %#v, want %#v", captured.Args(), want)
	}
}

func TestACLUserCmd_readReply(t *testing.T) {
	want := &ACLUser{
		Flags:       []string{"on", "sanitize-payload"},
		Passwords:   []string{"5e88"},
		ACLSelector: ACLSelector{Commands: "-@all +get", Keys: "~app:*", Channels: "&news"},
		Selectors:   []ACLSelector{{Commands: "-@all +set", Keys: "%W~tmp:*"}},
	}

	// The replies of Redis 8.10 to ACL GETUSER, with an unknown field added.
	replies := map[string]string{
		"RESP2": "*14\r\n" +
			"$5\r\nflags\r\n*2\r\n$2\r\non\r\n$16\r\nsanitize-payload\r\n" +
			"$9\r\npasswords\r\n*1\r\n$4\r\n5e88\r\n" +
			"$8\r\ncommands\r\n$10\r\n-@all +get\r\n" +
			"$4\r\nkeys\r\n$6\r\n~app:*\r\n" +
			"$8\r\nchannels\r\n$5\r\n&news\r\n" +
			"$9\r\nselectors\r\n*1\r\n*6\r\n" +
			"$8\r\ncommands\r\n$10\r\n-@all +set\r\n" +
			"$4\r\nkeys\r\n$8\r\n%W~tmp:*\r\n" +
			"$8\r\nchannels\r\n$0\r\n\r\n" +
			"$6\r\nfuture\r\n*1\r\n:1\r\n",
		"RESP3": "%7\r\n" +
			"$5\r\nflags\r\n~2\r\n$2\r\non\r\n$16\r\nsanitize-payload\r\n" +
			"$9\r\npasswords\r\n*1\r\n$4\r\n5e88\r\n" +
			"$8\r\ncommands\r\n$10\r\n-@all +get\r\n" +
			"$4\r\nkeys\r\n$6\r\n~app:*\r\n" +
			"$8\r\nchannels\r\n$5\r\n&news\r\n" +
			"$9\r\nselectors\r\n*1\r\n%3\r\n" +
			"$8\r\ncommands\r\n$10\r\n-@all +set\r\n" +
			"$4\r\nkeys\r\n$8\r\n%W~tmp:*\r\n" +
			"$8\r\nchannels\r\n$0\r\n\r\n" +
			"$6\r\nfuture\r\n*1\r\n:1\r\n",
	}

	for name, reply := range replies {
		t.Run(name, func(t *testing.T) {
			cmd := NewACLUserCmd(context.Background(), "acl", "getuser", "alice")
			if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cmd.Val(), want) {
				t.Errorf("val = %+v, want %+v", cmd.Val(), want)
			}

			clone := cmd.Clone().(*ACLUserCmd)
			clone.Val().Flags[0] = "off"
			clone.Val().Selectors[0].Keys = "~*"
			if !reflect.DeepEqual(cmd.Val(), want) {
				t.Errorf("val changed with its clone: %+v", cmd.Val())
			}
		})
	}

	for name, reply := range map[string]string{"RESP2": "*-1\r\n", "RESP3": "_\r\n"} {
		t.Run("missing user "+name, func(t *testing.T) {
			cmd := NewACLUserCmd(context.Background(), "acl", "getuser", "nobody")
			if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != Nil {
				t.Errorf("err = %v, want redis.Nil", err)
			}
		})
	}
}
