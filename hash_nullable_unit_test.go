package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"reflect"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

func TestHMGetStringsCommand(t *testing.T) {
	ctx := context.Background()
	var seen Cmder
	c := cmdable(func(_ context.Context, cmd Cmder) error { seen = cmd; return nil })
	cmd := c.HMGetStrings(ctx, "key{slot}", "a", "", "a")
	if cmd != seen || !reflect.DeepEqual(cmd.Args(), []interface{}{"hmget", "key{slot}", "a", "", "a"}) || cmdFirstKeyPosWithInfo(cmd, nil) != 1 {
		t.Fatal("command shape or routing lost", cmd.Args())
	}
	want := NullableStringSlice{Values: []string{"", "x"}, Valid: []bool{true, false}}
	cmd.SetVal(want)
	v, err := ExtractCommandValue(cmd)
	if err != nil || !reflect.DeepEqual(v, want) {
		t.Fatal("hook extraction", v, err)
	}
	clone := cmd.Clone().(*NullableStringSliceCmd)
	clone.val.Values[0], clone.val.Valid[0] = "changed", false
	if cmd.val.Values[0] != "" || !cmd.val.Valid[0] || clone.expected != cmd.expected {
		t.Fatal("clone shares slots or loses parse state")
	}
	factory := cmd.Clone()
	if _, ok := factory.(*NullableStringSliceCmd); !ok {
		t.Fatal("typed clone lost type")
	}
	if err := (&ClusterClient{}).setCommandValue(factory, want); err != nil {
		t.Fatal(err)
	}
	if got, err := ExtractCommandValue(factory); err != nil || !reflect.DeepEqual(got, want) {
		t.Fatal("routing result", got, err)
	}
}

func TestHMGetStringsOwnedDecode(t *testing.T) {
	large := bytes.Repeat([]byte{'a', 0, '\r', '\n'}, 4096)
	wire := []byte(fmt.Sprintf("*5\r\n_\r\n$0\r\n\r\n$16\r\n9007199254740993\r\n$%d\r\n%s\r\n$5\r\nhello\r\n+next\r\n", len(large), large))
	// The exact integer has sixteen decimal bytes and must never pass a float.
	for _, size := range []int{32 << 10, 1 << 20} {
		input := append([]byte(nil), wire...)
		rd := proto.NewReaderSize(bytes.NewReader(input), size)
		cmd := NewNullableStringSliceCmd(context.Background(), "hmget", "k", "missing", "empty", "id", "binary", "hello")
		if err := cmd.readReply(rd); err != nil {
			t.Fatal(err)
		}
		retained := cmd.Val()
		want := NullableStringSlice{Values: []string{"", "", "9007199254740993", string(large), "hello"}, Valid: []bool{false, true, true, true, true}}
		if !reflect.DeepEqual(retained, want) {
			t.Fatal("bytes or NULL mask changed")
		}
		if next, err := rd.ReadString(); err != nil || next != "next" {
			t.Fatal("reply misalignment", next, err)
		}
		for i := range input {
			input[i] = 'x'
		}
		rd.Reset(bytes.NewBufferString("*5\r\n$1\r\nz\r\n_\r\n_\r\n_\r\n_\r\n"))
		if err := cmd.readReply(rd); err != nil {
			t.Fatal(err)
		}
		cmd.val.Values[0] = "mutated"
		var wg sync.WaitGroup
		for range 4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if !reflect.DeepEqual(retained, want) {
					t.Error("retained result changed after next decode")
				}
			}()
		}
		wg.Wait()
	}
}

func TestHMGetStringsDecodeErrors(t *testing.T) {
	for _, wire := range []string{"*1\r\n$4\r\na", "*2\r\n$1\r\na\r\n", "*1\r\n-ERR nested\r\n+unread\r\n", "*1\r\n#t\r\n", "*2\r\n_\r\n_\r\n", "_\r\n", "-WRONGTYPE fixture\r\n"} {
		cmd := NewNullableStringSliceCmd(context.Background(), "hmget", "k", "field")
		err := cmd.readReply(proto.NewReader(bytes.NewBufferString(wire)))
		if err == nil || cmd.val.Values != nil || cmd.val.Valid != nil {
			t.Fatalf("partial result/error lost for %q: %v", wire, err)
		}
		if wire == "-WRONGTYPE fixture\r\n" {
			if !isRedisError(err) {
				t.Fatal("whole command error lost its type")
			}
			continue
		}
		if wire == "_\r\n" {
			if !errors.Is(err, Nil) {
				t.Fatal(err)
			}
			continue
		}
		if !isBadConn(err, false, "") {
			t.Fatal("malformed array permits socket reuse", err)
		}
		if errors.Is(err, io.EOF) {
			continue
		}
	}
}
