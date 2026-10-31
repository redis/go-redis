package hscan

import (
	"errors"
	"strings"
	"testing"
)

var (
	errBadState = errors.New("bad state")
	errBadLevel = errors.New("bad level")
)

// codeError is a typed error so errors.As can be checked.
type codeError struct{ code int }

func (e *codeError) Error() string { return "code error" }

type stateField string

func (s *stateField) ScanRedis(v string) error {
	if v != "ok" {
		return errBadState
	}
	*s = stateField(v)
	return nil
}

type levelField string

func (l *levelField) ScanRedis(v string) error {
	if v != "ok" {
		return errBadLevel
	}
	*l = levelField(v)
	return nil
}

type codeField int

func (c *codeField) ScanRedis(v string) error {
	return &codeError{code: 42}
}

type errorsData struct {
	State stateField `redis:"state"`
	Level levelField `redis:"level"`
	Code  codeField  `redis:"code"`
	Later string     `redis:"later"`
	Bool  bool       `redis:"bool"`
}

// One failing field: the error is returned as is, so errors.Is, errors.As and
// the error text are the same as before Scan kept going after a failure.
func TestScanSingleFieldErrorIsReturnedAsIs(t *testing.T) {
	var d errorsData
	err := Scan(&d, i{"state", "later"}, i{"broken", "filled"})
	if !errors.Is(err, errBadState) {
		t.Fatalf("errors.Is(err, errBadState) = false, err = %v", err)
	}
	if err != errBadState {
		t.Fatalf("err = %q, want the scanner's error unchanged", err)
	}
	if d.Later != "filled" {
		t.Fatalf("Later = %q, want fields after the failed one to be scanned", d.Later)
	}
}

func TestScanErrorsAsMatchesTypedScannerError(t *testing.T) {
	var d errorsData
	err := Scan(&d, i{"code", "later"}, i{"x", "filled"})
	var ce *codeError
	if !errors.As(err, &ce) || ce.code != 42 {
		t.Fatalf("errors.As(err, *codeError) failed, err = %v", err)
	}
}

// Several failing fields: every error stays reachable through errors.Is and
// errors.As, and the fields that decode are still set.
func TestScanJoinsSeveralFieldErrors(t *testing.T) {
	var d errorsData
	err := Scan(&d, i{"state", "level", "code", "later"}, i{"broken", "broken", "x", "filled"})
	if err == nil {
		t.Fatal("err = nil, want an error")
	}
	for _, want := range []error{errBadState, errBadLevel} {
		if !errors.Is(err, want) {
			t.Errorf("errors.Is(err, %v) = false, err = %v", want, err)
		}
	}
	var ce *codeError
	if !errors.As(err, &ce) {
		t.Errorf("errors.As(err, *codeError) = false, err = %v", err)
	}
	// The first failure comes first, as it was the only error returned before.
	if !strings.HasPrefix(err.Error(), errBadState.Error()) {
		t.Errorf("err = %q, want it to start with the first field error", err)
	}
	if d.Later != "filled" {
		t.Errorf("Later = %q, want fields after the failed ones to be scanned", d.Later)
	}
}

// Built-in decoders keep their existing message for a single failure.
func TestScanBuiltinDecodeErrorTextUnchanged(t *testing.T) {
	var d errorsData
	err := Scan(&d, i{"bool"}, i{"-1"})
	want := "cannot scan redis.result -1 into struct field errorsData.Bool of type bool"
	if err == nil || !strings.HasPrefix(err.Error(), want) {
		t.Fatalf("err = %v, want prefix %q", err, want)
	}
}
