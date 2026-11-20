package proto_test

import (
	"errors"
	"io"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

var errCachedReadTimeout = errors.New("i/o timeout")

// stepReader returns one scripted read per call, so a test can put bytes and an
// error in the same fill — the shape a socket read deadline has when a reply and
// the timeout arrive together.
type stepReader struct {
	chunks [][]byte
	errs   []error
	i      int
}

func (s *stepReader) Read(p []byte) (int, error) {
	if s.i >= len(s.chunks) {
		return 0, io.EOF
	}
	n := copy(p, s.chunks[s.i])
	err := s.errs[s.i]
	s.i++
	return n, err
}

// A transport can hand back a complete reply, the next frame and a timeout in the
// same fill. The reply parses and the timeout is cached while the frame is still
// buffered; DiscardCachedReadError must clear that cached error without dropping
// the frame, so the next command does not fail with the previous command's timeout.
func TestReader_DiscardCachedReadErrorKeepsBufferedFrames(t *testing.T) {
	src := &stepReader{
		chunks: [][]byte{[]byte("+OK\r\n+PUSH\r\n"), []byte("+NEXT\r\n")},
		errs:   []error{errCachedReadTimeout, nil},
	}
	r := proto.NewReader(src)

	if got, err := r.ReadReply(); err != nil || got != "OK" {
		t.Fatalf("first reply = %v, %v; want OK, nil", got, err)
	}
	if r.Buffered() == 0 {
		t.Fatalf("expected the push frame to stay buffered")
	}

	r.DiscardCachedReadError()

	if got, err := r.ReadReply(); err != nil || got != "PUSH" {
		t.Fatalf("buffered frame after discard = %v, %v; want PUSH, nil (the cached timeout must not surface)", got, err)
	}
	if got, err := r.ReadReply(); err != nil || got != "NEXT" {
		t.Fatalf("next reply after the buffered frame = %v, %v; want NEXT, nil", got, err)
	}
}

// With nothing buffered there are no bytes to preserve, and the cached error is
// dropped by a plain Reset.
func TestReader_DiscardCachedReadErrorClearsCachedErrorWithoutBuffer(t *testing.T) {
	src := &stepReader{
		chunks: [][]byte{[]byte("+OK\r\n"), []byte("+NEXT\r\n")},
		errs:   []error{errCachedReadTimeout, nil},
	}
	r := proto.NewReader(src)

	if got, err := r.ReadReply(); err != nil || got != "OK" {
		t.Fatalf("first reply = %v, %v; want OK, nil", got, err)
	}
	if r.Buffered() != 0 {
		t.Fatalf("expected no buffered bytes, got %d", r.Buffered())
	}

	r.DiscardCachedReadError()

	if got, err := r.ReadReply(); err != nil || got != "NEXT" {
		t.Fatalf("reply after discard = %v, %v; want NEXT, nil", got, err)
	}
}
