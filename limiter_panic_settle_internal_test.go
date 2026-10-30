package redis

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9/internal/pool"
)

// settleLimiter records every Allow call and every ReportResult argument, so a
// test can check the pairing invariant of Options.Limiter without a server.
type settleLimiter struct {
	mu      sync.Mutex
	allows  int
	reports []error
	deny    error
}

func (l *settleLimiter) Allow() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.allows++
	return l.deny
}

func (l *settleLimiter) ReportResult(err error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.reports = append(l.reports, err)
}

func (l *settleLimiter) counts() (int, []error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	reports := make([]error, len(l.reports))
	copy(reports, l.reports)
	return l.allows, reports
}

// oneConnPool hands out a single preset conn and never dials, so the test drives
// the init phase of _getConn. It records Remove to keep the failure paths honest.
type oneConnPool struct {
	pool.Pooler
	conn    *pool.Conn
	removes int
	mu      sync.Mutex
}

func (p *oneConnPool) Get(context.Context) (*pool.Conn, error) { return p.conn, nil }

func (p *oneConnPool) Remove(context.Context, *pool.Conn, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.removes++
}

func (p *oneConnPool) removed() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.removes
}

// errorPool fails the lease before the conn is inited.
type errorPool struct {
	pool.Pooler
	err error
}

func (p *errorPool) Get(context.Context) (*pool.Conn, error) { return nil, p.err }

func newSettleTestClient(t *testing.T, p pool.Pooler, lim Limiter, onConnect func(context.Context, *Conn) error) *baseClient {
	t.Helper()
	// A RESP2 handshake with no credentials, no SELECT and no CLIENT SETINFO
	// reaches the OnConnect hook without reading a reply from the conn, so the
	// only thing that can fail here is the hook itself.
	opt := &Options{
		Protocol:         2,
		DisableIndentity: true,
		DisableIdentity:  true,
		OnConnect:        onConnect,
		Limiter:          lim,
	}
	return &baseClient{opt: opt, connPool: p}
}

// TestGetConnLimitedSettlesAdmissionOnPanic pins the invariant that every
// Limiter.Allow is paired with exactly one ReportResult: a panic inside _getConn
// (an OnConnect or credentials hook blowing up, which the hook contract forbids
// but the other admission paths already survive) must settle the admission with
// errInitPanic instead of leaving the permit unpaired for a caller that recovers.
func TestGetConnLimitedSettlesAdmissionOnPanic(t *testing.T) {
	ctx := context.Background()
	lim := &settleLimiter{}
	c := newSettleTestClient(t, &oneConnPool{conn: pool.NewConn(&mockNetConn{})}, lim,
		func(context.Context, *Conn) error { panic("test: OnConnect panic") })

	var recovered any
	func() {
		defer func() { recovered = recover() }()
		cn, limited, err := c.getConnLimited(ctx)
		t.Errorf("getConnLimited must not return: cn=%v limited=%v err=%v", cn, limited, err)
	}()
	if recovered == nil {
		t.Fatal("the OnConnect panic must reach the caller")
	}

	allows, reports := lim.counts()
	if allows != 1 {
		t.Fatalf("Limiter.Allow called %d times, want 1", allows)
	}
	if len(reports) != 1 {
		t.Fatalf("Limiter.ReportResult called %d times, want 1 (the admission was left unpaired by the panic)", len(reports))
	}
	if !errors.Is(reports[0], errInitPanic) {
		t.Fatalf("reported %v, want errInitPanic", reports[0])
	}
}

// TestGetConnLimitedAdmissionResults covers the paths that must stay as they
// were: a dial failure is reported once, an admission denial is never reported,
// and a client without a Limiter pays nothing.
func TestGetConnLimitedAdmissionResults(t *testing.T) {
	ctx := context.Background()
	dialErr := errors.New("dial failed")

	t.Run("dial failure is reported", func(t *testing.T) {
		lim := &settleLimiter{}
		c := newSettleTestClient(t, &errorPool{err: dialErr}, lim, nil)

		cn, limited, err := c.getConnLimited(ctx)
		if cn != nil || limited {
			t.Fatalf("got cn=%v limited=%v, want nil false", cn, limited)
		}
		if !errors.Is(err, dialErr) {
			t.Fatalf("err = %v, want %v", err, dialErr)
		}
		allows, reports := lim.counts()
		if allows != 1 || len(reports) != 1 || !errors.Is(reports[0], dialErr) {
			t.Fatalf("allows=%d reports=%v, want 1 and [dial failed]", allows, reports)
		}
	})

	t.Run("admission denial is not reported", func(t *testing.T) {
		denied := errors.New("breaker open")
		lim := &settleLimiter{deny: denied}
		p := &oneConnPool{conn: pool.NewConn(&mockNetConn{})}
		c := newSettleTestClient(t, p, lim, nil)

		cn, limited, err := c.getConnLimited(ctx)
		if cn != nil || !limited {
			t.Fatalf("got cn=%v limited=%v, want nil true", cn, limited)
		}
		if !errors.Is(err, denied) {
			t.Fatalf("err = %v, want %v", err, denied)
		}
		allows, reports := lim.counts()
		if allows != 1 || len(reports) != 0 {
			t.Fatalf("allows=%d reports=%v, want 1 and none", allows, reports)
		}
		if got := p.removed(); got != 0 {
			t.Fatalf("removes = %d, want 0 (no conn was leased)", got)
		}
	})

	t.Run("no limiter pays nothing", func(t *testing.T) {
		c := newSettleTestClient(t, &errorPool{err: dialErr}, nil, nil)

		if _, _, err := c.getConnLimited(ctx); !errors.Is(err, dialErr) {
			t.Fatalf("err = %v, want %v", err, dialErr)
		}
	})
}
