package redis

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/proto"
)

// BACKUP STATUS answers with a map in RESP3 and a flat array of name/value
// pairs in RESP2. The times are Unix seconds, 0 when they haven't happened.
func TestBackupStatusCmdReadReply(t *testing.T) {
	body := "$5\r\nstate\r\n$6\r\nsealed\r\n" +
		"$5\r\nerror\r\n$0\r\n\r\n" +
		"$10\r\nstart_time\r\n:1794431235\r\n" +
		"$8\r\nend_time\r\n:1794431237\r\n"

	for _, reply := range []string{"%4\r\n" + body, "*8\r\n" + body} {
		cmd := NewBackupStatusCmd(context.Background(), "backup", "status")
		if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
			t.Fatalf("readReply(%q): %v", reply, err)
		}
		want := BackupStatus{State: "sealed", StartTime: time.Unix(1794431235, 0), EndTime: time.Unix(1794431237, 0)}
		if got := cmd.Val(); got == nil || *got != want {
			t.Errorf("readReply(%q) = %+v; want %+v", reply, got, want)
		}
	}
}

func TestBackupStatusCmdReadReplyIdleAndUnknownFields(t *testing.T) {
	reply := "%5\r\n" +
		"$5\r\nstate\r\n$4\r\nidle\r\n" +
		"$5\r\nerror\r\n$0\r\n\r\n" +
		"$10\r\nstart_time\r\n:0\r\n" +
		"$8\r\nend_time\r\n:0\r\n" +
		"$5\r\nextra\r\n*2\r\n:1\r\n:2\r\n"
	cmd := NewBackupStatusCmd(context.Background(), "backup", "status")
	if err := cmd.readReply(proto.NewReader(strings.NewReader(reply))); err != nil {
		t.Fatalf("readReply: %v", err)
	}
	got := cmd.Val()
	if got.State != "idle" || got.Error != "" || !got.StartTime.IsZero() || !got.EndTime.IsZero() {
		t.Errorf("idle status = %+v; want state idle and zero times", got)
	}
}

func TestBackupStatusCmdCloneCopiesValue(t *testing.T) {
	cmd := NewBackupStatusCmd(context.Background(), "backup", "status")
	cmd.SetVal(&BackupStatus{State: "incrementing"})
	clone := cmd.Clone().(*BackupStatusCmd)
	clone.Val().State = "sealed"
	if cmd.Val().State != "incrementing" {
		t.Errorf("changing the clone changed the original: %+v", cmd.Val())
	}
	if v, err := ExtractCommandValue(cmd); err != nil || v.(*BackupStatus).State != "incrementing" {
		t.Errorf("ExtractCommandValue = %v, %v", v, err)
	}
}

// backupCaptureHook records each command and answers it without a server.
type backupCaptureHook struct{ cmds []Cmder }

func (h *backupCaptureHook) DialHook(next DialHook) DialHook { return next }

func (h *backupCaptureHook) ProcessHook(ProcessHook) ProcessHook {
	return func(_ context.Context, cmd Cmder) error {
		h.cmds = append(h.cmds, cmd)
		return nil
	}
}

func (h *backupCaptureHook) ProcessPipelineHook(next ProcessPipelineHook) ProcessPipelineHook {
	return next
}

func TestBackupCommandArgs(t *testing.T) {
	ctx := context.Background()
	client := NewClient(&Options{Addr: "127.0.0.1:1"})
	defer client.Close()
	hook := &backupCaptureHook{}
	client.AddHook(hook)

	client.BackupStart(ctx)
	client.BackupStatus(ctx)
	client.BackupList(ctx)
	client.BackupSeal(ctx)
	client.BackupAbort(ctx)
	client.BackupCleanup(ctx)

	want := []string{"start", "status", "list", "seal", "abort", "cleanup"}
	if len(hook.cmds) != len(want) {
		t.Fatalf("sent %d commands; want %d", len(hook.cmds), len(want))
	}
	for i, sub := range want {
		if args := hook.cmds[i].Args(); len(args) != 2 || args[0] != "backup" || args[1] != sub {
			t.Errorf("BACKUP %s args = %v", strings.ToUpper(sub), args)
		}
	}
	if _, ok := hook.cmds[1].(*BackupStatusCmd); !ok {
		t.Errorf("BackupStatus sent a %T; want *BackupStatusCmd", hook.cmds[1])
	}
	if _, ok := keylessCommands["backup"]; !ok {
		t.Error(`keylessCommands["backup"] missing: a raw BACKUP on a ClusterClient would hash the subcommand as a key`)
	}
}
