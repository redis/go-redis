package redis

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestModuleList_Args(t *testing.T) {
	var captured Cmder
	c := captureCmdable(&captured)

	c.ModuleList(context.Background())
	if want := []any{"module", "list"}; !reflect.DeepEqual(captured.Args(), want) {
		t.Errorf("args = %#v, want %#v", captured.Args(), want)
	}
}

func TestModuleListCmd_readReply(t *testing.T) {
	// The first two modules of the reply of Redis 8.10 to MODULE LIST. The
	// third module follows the reply schema: no server at hand had a module
	// loaded with arguments.
	const (
		vectorset  = "$4\r\nname\r\n$9\r\nvectorset\r\n$3\r\nver\r\n:1\r\n$4\r\npath\r\n$0\r\n\r\n$4\r\nargs\r\n*0\r\n"
		timeseries = "$4\r\nname\r\n$10\r\ntimeseries\r\n$3\r\nver\r\n:81001\r\n" +
			"$4\r\npath\r\n$48\r\n/usr/local/lib/redis/modules//redistimeseries.so\r\n$4\r\nargs\r\n*0\r\n"
		withArgs = "$4\r\nname\r\n$2\r\nbf\r\n$3\r\nver\r\n:81001\r\n$4\r\npath\r\n$6\r\n/bf.so\r\n" +
			"$4\r\nargs\r\n*2\r\n$10\r\nERROR_RATE\r\n$4\r\n0.01\r\n"
	)
	want := []ModuleInfo{
		{Name: "vectorset", Version: 1, Args: []string{}},
		{
			Name:    "timeseries",
			Version: 81001,
			Path:    "/usr/local/lib/redis/modules//redistimeseries.so",
			Args:    []string{},
		},
		{Name: "bf", Version: 81001, Path: "/bf.so", Args: []string{"ERROR_RATE", "0.01"}},
	}

	tests := []struct {
		name  string
		reply string
		want  []ModuleInfo
	}{
		{
			name:  "RESP2",
			reply: "*3\r\n*8\r\n" + vectorset + "*8\r\n" + timeseries + "*8\r\n" + withArgs,
			want:  want,
		},
		{
			name:  "RESP3",
			reply: "*3\r\n%4\r\n" + vectorset + "%4\r\n" + timeseries + "%4\r\n" + withArgs,
			want:  want,
		},
		{
			name:  "no modules",
			reply: "*0\r\n",
			want:  []ModuleInfo{},
		},
		{
			name:  "name and version only",
			reply: "*1\r\n*4\r\n$4\r\nname\r\n$2\r\nbf\r\n$3\r\nver\r\n:20205\r\n",
			want:  []ModuleInfo{{Name: "bf", Version: 20205}},
		},
		{
			name:  "unknown field",
			reply: "*1\r\n%3\r\n$4\r\nname\r\n$2\r\nbf\r\n$3\r\nnew\r\n*1\r\n:1\r\n$3\r\nver\r\n:2\r\n",
			want:  []ModuleInfo{{Name: "bf", Version: 2}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cmd := NewModuleListCmd(context.Background(), "module", "list")
			if err := cmd.readReply(proto.NewReader(strings.NewReader(tt.reply))); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(cmd.Val(), tt.want) {
				t.Errorf("val = %+v, want %+v", cmd.Val(), tt.want)
			}

			clone := cmd.Clone().(*ModuleListCmd)
			for i := range clone.Val() {
				clone.Val()[i].Name = "changed"
				for j := range clone.Val()[i].Args {
					clone.Val()[i].Args[j] = "changed"
				}
			}
			if !reflect.DeepEqual(cmd.Val(), tt.want) {
				t.Errorf("val changed with its clone: %+v", cmd.Val())
			}
		})
	}

	t.Run("odd RESP2 module", func(t *testing.T) {
		cmd := NewModuleListCmd(context.Background(), "module", "list")
		if err := cmd.readReply(proto.NewReader(strings.NewReader("*1\r\n*1\r\n$4\r\nname\r\n"))); err == nil {
			t.Errorf("reply accepted: %+v", cmd.Val())
		}
	})
}
