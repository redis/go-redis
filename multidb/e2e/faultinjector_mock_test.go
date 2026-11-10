package e2e

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// fakeDocker records docker invocations and fails those whose joined args
// contain any of failOn.
type fakeDocker struct {
	calls  []string
	failOn []string
}

func (f *fakeDocker) run(_ context.Context, args ...string) error {
	call := strings.Join(args, " ")
	f.calls = append(f.calls, call)
	for _, s := range f.failOn {
		if strings.Contains(call, s) {
			return errors.New("boom")
		}
	}
	return nil
}

// TestDockerIptablesMechanismCommands pins one iptables command per
// invocation (iptables rejects several -P in one call), the Sever rollback,
// and that Restore attempts every chain.
func TestDockerIptablesMechanismCommands(t *testing.T) {
	ctx := context.Background()
	const c = "proxy-1"

	t.Run("sever sets each chain separately", func(t *testing.T) {
		f := &fakeDocker{}
		if err := (DockerIptablesMechanism{run: f.run}).Sever(ctx, c); err != nil {
			t.Fatal(err)
		}
		want := []string{
			"exec proxy-1 iptables -P INPUT DROP",
			"exec proxy-1 iptables -P FORWARD DROP",
		}
		if !reflect.DeepEqual(f.calls, want) {
			t.Fatalf("calls = %q, want %q", f.calls, want)
		}
	})

	t.Run("sever rolls back on partial failure", func(t *testing.T) {
		f := &fakeDocker{failOn: []string{"FORWARD DROP"}}
		if err := (DockerIptablesMechanism{run: f.run}).Sever(ctx, c); err == nil {
			t.Fatal("Sever succeeded, want error")
		}
		want := []string{
			"exec proxy-1 iptables -P INPUT DROP",
			"exec proxy-1 iptables -P FORWARD DROP",
			"exec proxy-1 iptables -P INPUT ACCEPT",
		}
		if !reflect.DeepEqual(f.calls, want) {
			t.Fatalf("calls = %q, want %q", f.calls, want)
		}
	})

	t.Run("restore attempts every chain", func(t *testing.T) {
		f := &fakeDocker{failOn: []string{"INPUT ACCEPT", "FORWARD ACCEPT"}}
		err := (DockerIptablesMechanism{run: f.run}).Restore(ctx, c, "127.0.0.1:1")
		if err == nil || strings.Count(err.Error(), "boom") != 2 {
			t.Fatalf("err = %v, want both chain errors", err)
		}
		want := []string{
			"exec proxy-1 iptables -P INPUT ACCEPT",
			"exec proxy-1 iptables -P FORWARD ACCEPT",
		}
		if !reflect.DeepEqual(f.calls, want) {
			t.Fatalf("calls = %q, want %q", f.calls, want)
		}
	})
}
