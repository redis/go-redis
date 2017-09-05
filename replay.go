package redis

import (
	"fmt"
	"reflect"
)

type replayAction struct {
	cmd Cmder
}

type Replay struct {
	actions []*replayAction
}

func NewReplay() *Replay {
	return &Replay{}
}

func (r *Replay) Add(cmd Cmder) *Replay {
	action := &replayAction{
		cmd: cmd,
	}
	r.actions = append(r.actions, action)
	return r
}

func (r *Replay) WrapClient(c *Client) {
	c.WrapProcess(func(oldProcess func(cmd Cmder) error) func(cmd Cmder) error {
		return r.process
	})
}

func (r *Replay) process(cmd Cmder) error {
	for _, a := range r.actions {
		if argsEqual(cmd.args(), a.cmd.args()) {
			if err := setCmd(cmd, a.cmd); err != nil {
				return err
			}
			return cmd.Err()
		}
	}

	cmd.SetErr(fmt.Errorf("unexpected cmd: %s", cmd))
	return cmd.Err()
}

func argsEqual(a []interface{}, b []interface{}) bool {
	if len(a) != len(b) {
		return false
	}
	for i, v := range a {
		if v != b[i] {
			return false
		}
	}
	return true
}

func setCmd(dst, src interface{}) error {
	dstv := reflect.ValueOf(dst).Elem()
	srcv := reflect.ValueOf(src).Elem()
	if dstv.Type() != srcv.Type() {
		return fmt.Errorf("dst and src commands have different types: %T and %T", dst, src)
	}
	dstv.Set(srcv)
	return nil
}
