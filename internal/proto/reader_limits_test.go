package proto_test

import (
	"bytes"
	"io"
	"math"
	"runtime"
	"strconv"
	"strings"
	"testing"

	"github.com/redis/go-redis/v9/internal/proto"
)

// bulkHeader is the reply header line without the trailing CRLF, the form
// readLine hands to replyLen.
func bulkHeader(n string) string { return "$" + n }

// largeBulkLengths are payload lengths at and above the 512MB default of the
// server's configurable proto-max-bulk-len (the bitmap tests in this repository
// set it to 2gb). They are legitimate, so the reader must not reject them.
func largeBulkLengths() []string {
	lengths := []string{
		"536870912",  // 512MB, the default proto-max-bulk-len
		"536870913",  // one byte over the default
		"1073741824", // 1GB
	}
	if strconv.IntSize == 64 {
		lengths = append(lengths, "2147483648") // 2GB, as configured in the bitmap tests
	}
	return lengths
}

// TestReaderAcceptsLargeBulkLengths pins the large-reply policy: a bulk length
// above the 512MB protocol default is not malformed, because proto-max-bulk-len
// is configurable. DiscardNext is used so no payload is allocated: the header
// is accepted and the read then ends at EOF, while a rejection would surface the
// "invalid reply" error here.
func TestReaderAcceptsLargeBulkLengths(t *testing.T) {
	for _, n := range largeBulkLengths() {
		t.Run(n, func(t *testing.T) {
			r := proto.NewReader(strings.NewReader(bulkHeader(n) + "\r\n"))
			err := r.DiscardNext()
			if err == nil {
				t.Fatalf("DiscardNext(%q) = nil; want EOF for the missing payload", n)
			}
			if strings.Contains(err.Error(), "invalid reply") {
				t.Fatalf("DiscardNext(%q) = %v; legitimate large bulk length rejected", n, err)
			}
		})
	}
}

// TestReaderReadsLargeBulkPayloadAcrossGrowthSteps exercises the incremental
// allocation path: payloads above the first reservation are read while the
// buffer grows, for both the parsed (ReadReply) and raw (ReadRawReply) paths,
// and the stream stays aligned for the reply that follows.
func TestReaderReadsLargeBulkPayloadAcrossGrowthSteps(t *testing.T) {
	const size = 3 << 20 // larger than the first 1MiB reservation
	payload := bytes.Repeat([]byte{'x'}, size)
	frame := append([]byte(bulkHeader(strconv.Itoa(size))+"\r\n"), payload...)
	frame = append(frame, '\r', '\n')
	frame = append(frame, "+next\r\n"...)

	for _, tc := range []struct {
		name string
		read func(*proto.Reader) (string, error)
	}{
		{"ReadReply", func(r *proto.Reader) (string, error) {
			v, err := r.ReadReply()
			if err != nil {
				return "", err
			}
			return v.(string), nil
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := proto.NewReader(bytes.NewReader(frame))
			got, err := tc.read(r)
			if err != nil {
				t.Fatalf("%s: %v", tc.name, err)
			}
			if got != string(payload) {
				t.Fatalf("%s: payload mismatch: got %d bytes, want %d", tc.name, len(got), size)
			}
			if next, err := r.ReadString(); err != nil || next != "next" {
				t.Fatalf("follow-up ReadString = %q, %v; want \"next\", nil", next, err)
			}
		})
	}

	t.Run("ReadRawReply", func(t *testing.T) {
		r := proto.NewReader(bytes.NewReader(frame))
		raw, err := r.ReadRawReply()
		if err != nil {
			t.Fatalf("ReadRawReply: %v", err)
		}
		want := size + len(bulkHeader(strconv.Itoa(size))) + 4
		if len(raw) != want {
			t.Fatalf("ReadRawReply returned %d bytes, want %d", len(raw), want)
		}
		if next, err := r.ReadString(); err != nil || next != "next" {
			t.Fatalf("follow-up ReadString = %q, %v; want \"next\", nil", next, err)
		}
	})
}

// TestReaderStreamsLargeBulkLengthWithoutAllocating covers the raw reply path
// (DoRawWriteTo): ReadRawReplyWriteTo streams the payload instead of reserving
// the advertised length, so a large but non-overflowing length must not be
// rejected even when the peer sends no payload.
func TestReaderStreamsLargeBulkLengthWithoutAllocating(t *testing.T) {
	for _, n := range largeBulkLengths() {
		t.Run(n, func(t *testing.T) {
			r := proto.NewReader(strings.NewReader(bulkHeader(n) + "\r\n"))
			err := func() error {
				_, err := r.ReadRawReplyWriteTo(io.Discard)
				return err
			}()
			if err == nil {
				t.Fatalf("ReadRawReplyWriteTo(%q) = nil; want EOF for the missing payload", n)
			}
			if strings.Contains(err.Error(), "invalid reply") {
				t.Fatalf("ReadRawReplyWriteTo(%q) = %v; large streaming reply rejected", n, err)
			}
		})
	}
}

// TestReadReplyRejectsUnreadableLengths keeps the panic-regression coverage for
// headers that must still be refused: bulk lengths whose n+2 (payload plus
// trailing CRLF) overflows int used to wrap negative and panic in make, and
// aggregate counts near MaxInt used to allocate the advertised element count up
// front. A corrupt or malicious server must produce an error, never a panic.
func TestReadReplyRejectsUnreadableLengths(t *testing.T) {
	cases := []struct {
		name string
		data string
	}{
		{"bulk length of MaxInt", bulkHeader(strconv.Itoa(math.MaxInt)) + "\r\n"},
		{"bulk length just below MaxInt", bulkHeader(strconv.Itoa(math.MaxInt-1)) + "\r\n"},
		{"bulk length above MaxInt", bulkHeader("9223372036854775808") + "\r\n"},
		// Within the int range but far larger than any allocation: guarded by
		// reading the payload as it arrives, so this must error at EOF without
		// reserving the advertised size or panicking make.
		{"bulk length beyond allocatable size", bulkHeader("4611686018427387904") + "\r\n"},
		{"array count of MaxInt", "*" + strconv.Itoa(math.MaxInt) + "\r\n"},
		{"set count of MaxInt", "~" + strconv.Itoa(math.MaxInt) + "\r\n"},
		{"push count of MaxInt", ">" + strconv.Itoa(math.MaxInt) + "\r\n"},
		{"map count of MaxInt", "%" + strconv.Itoa(math.MaxInt) + "\r\n"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := proto.NewReader(bytes.NewReader([]byte(tc.data)))
			if _, err := r.ReadReply(); err == nil {
				t.Fatalf("ReadReply(%q) = nil error, want an error", tc.data)
			}
		})
	}
}

// patternReader streams the byte 'x' without allocating, so a large payload can
// be read by a test without holding a second copy of it.
type patternReader struct{}

func (patternReader) Read(p []byte) (int, error) {
	for i := range p {
		p[i] = 'x'
	}
	return len(p), nil
}

// patternBulkFrame returns a reader for one bulk reply of size bytes filled with
// 'x', followed by the +next status reply that proves the stream stays aligned.
func patternBulkFrame(size int) io.Reader {
	return io.MultiReader(
		strings.NewReader(bulkHeader(strconv.Itoa(size))+"\r\n"),
		io.LimitReader(patternReader{}, int64(size)),
		strings.NewReader("\r\n+next\r\n"),
	)
}

// TestReaderReadsLargeRawBulkReplyInOneBuffer pins the raw-reply allocation
// behavior above maxBulkPrealloc: ReadRawReply reads the payload into the reply
// buffer as it arrives instead of staging the whole payload in a second
// full-size slice and appending it. A 64 MiB reply used to allocate about
// 149 MiB (staging payload + reply buffer); the growth copies of the single
// buffer now stay under 1.5x the payload.
func TestReaderReadsLargeRawBulkReplyInOneBuffer(t *testing.T) {
	if testing.Short() {
		t.Skip("reads a 64 MiB reply")
	}
	const size = 64 << 20
	header := bulkHeader(strconv.Itoa(size)) + "\r\n"
	r := proto.NewReader(patternBulkFrame(size))

	var before, after runtime.MemStats
	runtime.GC()
	runtime.ReadMemStats(&before)
	raw, err := r.ReadRawReply()
	runtime.ReadMemStats(&after)
	if err != nil {
		t.Fatalf("ReadRawReply: %v", err)
	}

	wantLen := len(header) + size + 2
	if len(raw) != wantLen {
		t.Fatalf("ReadRawReply returned %d bytes, want %d", len(raw), wantLen)
	}
	if got := string(raw[:len(header)]); got != header {
		t.Fatalf("reply header = %q, want %q", got, header)
	}
	if got := string(raw[len(raw)-2:]); got != "\r\n" {
		t.Fatalf("reply trailer = %q, want %q", got, "\r\n")
	}
	for i, c := range raw[len(header) : len(header)+size] {
		if c != 'x' {
			t.Fatalf("payload byte %d = %q, want 'x'", i, c)
		}
	}

	// The payload is appended to the reply buffer, so the total allocated for
	// the read is the final buffer plus the geometric growth copies (1 + 4 + 16
	// MiB for this size), never a second complete payload.
	if grown := after.TotalAlloc - before.TotalAlloc; grown > size+size/2 {
		t.Fatalf("ReadRawReply allocated %d bytes for a %d byte payload, want at most %d (payload copied twice?)",
			grown, size, size+size/2)
	}

	if next, err := r.ReadString(); err != nil || next != "next" {
		t.Fatalf("follow-up ReadString = %q, %v; want \"next\", nil", next, err)
	}
}

// TestReader_RejectedLengthIsTerminalUntilReset covers the stream-integrity
// half of the guard: the rejected header has already been consumed, so the rest
// of the frame cannot be located. Any later read would decode the frame body as
// the next reply — which is how a pipelined/pending-push read could return a
// bogus success. The error therefore has to be fatal for the Reader, and only
// Reset may clear it.
func TestReader_RejectedLengthIsTerminalUntilReset(t *testing.T) {
	for _, tc := range []struct {
		name   string
		header string
	}{
		{"bulk length of MaxInt", bulkHeader(strconv.Itoa(math.MaxInt)) + "\r\n"},
		{"bulk length just below MaxInt", bulkHeader(strconv.Itoa(math.MaxInt-1)) + "\r\n"},
		{"negative bulk length below nil", "$-2\r\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			// The bytes after the header stand for the unread part of the
			// rejected frame. They must never be parsed as the next reply.
			body := "+body\r\n"
			r := proto.NewReader(strings.NewReader(tc.header + body))
			fatalErr := func() error { _, err := r.ReadReply(); return err }()
			if fatalErr == nil {
				t.Fatalf("ReadReply on %q = nil error, want an error", tc.header)
			}
			if got, want := r.Buffered(), len(body); got != want {
				t.Fatalf("buffered after rejected header = %d, want %d (frame body consumed)", got, want)
			}

			for _, check := range []struct {
				name string
				read func() error
			}{
				{"ReadReply", func() error { _, err := r.ReadReply(); return err }},
				{"ReadString", func() error { _, err := r.ReadString(); return err }},
				{"ReadStringInto", func() error { _, err := r.ReadStringInto(make([]byte, 64)); return err }},
				{"ReadInt", func() error { _, err := r.ReadInt(); return err }},
				{"ReadArrayLen", func() error { _, err := r.ReadArrayLen(); return err }},
				{"ReadMapLen", func() error { _, err := r.ReadMapLen(); return err }},
				{"PeekReplyType", func() error { _, err := r.PeekReplyType(); return err }},
				{"Peek", func() error { _, err := r.Peek(1); return err }},
				{"PeekPushNotificationName", func() error { _, err := r.PeekPushNotificationName(); return err }},
				{"DiscardNext", r.DiscardNext},
				{"Discard", func() error { return r.Discard([]byte("+item")) }},
				{"ReadRawReply", func() error { _, err := r.ReadRawReply(); return err }},
				{"ReadRawReplyWriteTo", func() error { _, err := r.ReadRawReplyWriteTo(io.Discard); return err }},
			} {
				if err := check.read(); err != fatalErr {
					t.Errorf("%s after rejected header = %v; want the fatal error %v", check.name, err, fatalErr)
				}
			}
			if r.Buffered() != len(body) {
				t.Errorf("buffered after fatal reads = %d, want %d; a read consumed frame bytes", r.Buffered(), len(body))
			}
			if r.HasBufferedReply() {
				t.Error("HasBufferedReply after rejected header = true, want false")
			}

			r.Reset(strings.NewReader("+reset\r\n+next\r\n"))
			for _, want := range []string{"reset", "next"} {
				if got, err := r.ReadReply(); err != nil || got != want {
					t.Fatalf("ReadReply after Reset = %v, %v; want %q", got, err, want)
				}
			}
		})
	}
}

// TestReader_CommandReplyAfterFailedPushDrain mirrors what a client does when a
// pending RESP3 push notification cannot be drained: it reports the drain error
// and then reads the command reply on the same Reader. With an oversized element
// inside the push, the drain consumed the push header and failed mid-frame; the
// command reply read must return that same error instead of treating the push's
// remaining bytes as the reply.
func TestReader_CommandReplyAfterFailedPushDrain(t *testing.T) {
	push := ">2\r\n+message\r\n" + bulkHeader(strconv.Itoa(math.MaxInt)) + "\r\n"
	r := proto.NewReader(strings.NewReader(push + "+PONG\r\n"))
	_, drainErr := r.ReadReply()
	if drainErr == nil {
		t.Fatal("push drain: ReadReply = nil error, want the oversized bulk error")
	}
	reply, err := r.ReadString()
	if err != drainErr {
		t.Fatalf("command reply read = %q, %v; want the drain error %v (frame body decoded as a reply)", reply, err, drainErr)
	}
}
