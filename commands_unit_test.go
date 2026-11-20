package redis

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestRole_Args(t *testing.T) {
	var captured Cmder
	c := captureCmdable(&captured)

	c.Role(context.Background())
	if want := []any{"role"}; !reflect.DeepEqual(captured.Args(), want) {
		t.Errorf("args = %#v, want %#v", captured.Args(), want)
	}
}

func TestRoleCmd_readReply(t *testing.T) {
	// ROLE replies the same in RESP2 and RESP3. The first reply is the one of
	// Redis 8.10; the others follow the reply schema of the command.
	tests := []struct {
		name  string
		reply string
		want  *Role
	}{
		{
			name:  "master",
			reply: "*3\r\n$6\r\nmaster\r\n:0\r\n*0\r\n",
			want:  &Role{Role: "master", Replicas: []RoleReplica{}},
		},
		{
			name: "master with replicas",
			reply: "*3\r\n$6\r\nmaster\r\n:3129659\r\n*2\r\n" +
				"*3\r\n$9\r\n127.0.0.1\r\n$4\r\n9001\r\n$7\r\n3129242\r\n" +
				"*3\r\n$9\r\n127.0.0.1\r\n$4\r\n9002\r\n$7\r\n3129543\r\n",
			want: &Role{
				Role:       "master",
				ReplOffset: 3129659,
				Replicas: []RoleReplica{
					{IP: "127.0.0.1", Port: 9001, Offset: 3129242},
					{IP: "127.0.0.1", Port: 9002, Offset: 3129543},
				},
			},
		},
		{
			name:  "replica",
			reply: "*5\r\n$5\r\nslave\r\n$9\r\n127.0.0.1\r\n:9000\r\n$9\r\nconnected\r\n:3167038\r\n",
			want: &Role{
				Role:       "slave",
				MasterHost: "127.0.0.1",
				MasterPort: 9000,
				State:      "connected",
				ReplOffset: 3167038,
			},
		},
		{
			name:  "sentinel",
			reply: "*2\r\n$8\r\nsentinel\r\n*2\r\n$13\r\nresque-master\r\n$12\r\nstats-master\r\n",
			want:  &Role{Role: "sentinel", Masters: []string{"resque-master", "stats-master"}},
		},
		{
			name:  "unknown role",
			reply: "*3\r\n$5\r\nproxy\r\n:1\r\n*1\r\n$1\r\na\r\n",
			want:  &Role{Role: "proxy"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewRoleCmd(context.Background(), "role")
			if err := cmd.readReply(proto.NewReader(strings.NewReader(tt.reply))); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cmd.Val(), tt.want) {
				t.Errorf("val = %+v, want %+v", cmd.Val(), tt.want)
			}

			clone := cmd.Clone().(*RoleCmd)
			clone.Val().Role = "changed"
			for i := range clone.Val().Replicas {
				clone.Val().Replicas[i].Port = 0
			}
			for i := range clone.Val().Masters {
				clone.Val().Masters[i] = ""
			}
			if !reflect.DeepEqual(cmd.Val(), tt.want) {
				t.Errorf("val changed with its clone: %+v", cmd.Val())
			}
		})
	}

	t.Run("short reply", func(t *testing.T) {
		cmd := NewRoleCmd(context.Background(), "role")
		if err := cmd.readReply(proto.NewReader(strings.NewReader("*2\r\n$6\r\nmaster\r\n:0\r\n"))); err == nil {
			t.Errorf("reply accepted: %+v", cmd.Val())
		}
	})
}
