// Package flowtoken is a request flow controller, it controls the traffic of request per second
// and adjusts the flow threshold automatically based on the call success rate.
package flowtoken

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// Config for FlowToken.
type Config struct {
	// The initial size of window, means the available number of token in flowtoken at beginning.
	// Default is 1000.
	InitCwnd int64
	// The min size of window. When requests on-going failed, the window will continue to shrink until MinCwnd.
	// This value should not smaller than 1, as we need few requests to detect whether the service has recovered.
	// Default is 10.
	MinCwnd int64
	// The max size of window.
	// Default is 0, means no limit.
	MaxCwnd int64
}

// FlowToken represents a token dispenser.
type FlowToken struct {
	servId   string // server identity, like ip:port
	initCwnd int64  // the initial size of window
	minCwnd  int64  // the min size of window
	maxCwnd  int64  // the max size of window

	totalCount   int64 // the total count of request currently
	succCount    int64 // the count of success request currently
	failCount    int64 // the count of fail request currently
	exhaustCount int64 // the count of token exhausted currently

	tokenNum       int64 // the available number of token can be issued currently
	cwnd           int64 // the size of window currently
	ssthresh       int64 // the workload of server currently
	lastReinitTime int64 // the timestamp of last reinit

	l *sync.Mutex
}

// Token represents an available token.
type Token struct {
	TokenNum int64
	ft       *FlowToken
}

// Snapshot is a copy of flowtoken state.
type Snapshot struct {
	ServId string

	TokenNum       int64
	Cwnd           int64
	Ssthresh       int64
	LastReinitTime int64

	TotalCount   int64
	SuccCount    int64
	FailCount    int64
	ExhaustCount int64
}

// NewFlowToken create a new flowtoken.
func NewFlowToken(servId string, conf *Config) *FlowToken {
	ft := &FlowToken{
		servId: servId,
		l:      &sync.Mutex{},
	}

	if conf.InitCwnd <= 0 {
		ft.initCwnd = 1000
	} else {
		ft.initCwnd = conf.InitCwnd
	}

	if conf.MinCwnd <= 0 {
		ft.minCwnd = 10
	} else {
		ft.minCwnd = conf.MinCwnd
	}

	if conf.MaxCwnd > 0 {
		ft.maxCwnd = conf.MaxCwnd
	}

	return ft
}

// GetToken return an available token from flowtoken if there have available token,
// otherwise return TokenExhaustedError, which indicate that have reached the traffic threshold.
func (ft *FlowToken) GetToken() (*Token, error) {
	ft.l.Lock()
	defer ft.l.Unlock()

	currSec := time.Now().Unix()
	if currSec > ft.lastReinitTime {
		ft.reInit(currSec)
	}

	ft.totalCount++
	if ft.tokenNum <= 0 {
		ft.exhaustCount++
		snapshot := &Snapshot{
			ServId:         ft.servId,
			TokenNum:       ft.tokenNum,
			Cwnd:           ft.cwnd,
			Ssthresh:       ft.ssthresh,
			LastReinitTime: ft.lastReinitTime,
			TotalCount:     ft.totalCount,
			SuccCount:      ft.succCount,
			FailCount:      ft.failCount,
			ExhaustCount:   ft.exhaustCount,
		}

		return nil, &TokenExhaustedError{
			Snapshot: snapshot,
			Msg:      ft.servId + " token exhausted",
		}
	}

	ft.tokenNum--
	token := &Token{
		TokenNum: ft.tokenNum,
		ft:       ft,
	}

	return token, nil
}

// GetSnapshot return current state of flowtoken.
func (ft *FlowToken) GetSnapshot() *Snapshot {
	ft.l.Lock()
	defer ft.l.Unlock()

	return &Snapshot{
		ServId:         ft.servId,
		TokenNum:       ft.tokenNum,
		Cwnd:           ft.cwnd,
		Ssthresh:       ft.ssthresh,
		LastReinitTime: ft.lastReinitTime,
		TotalCount:     ft.totalCount,
		SuccCount:      ft.succCount,
		FailCount:      ft.failCount,
		ExhaustCount:   ft.exhaustCount,
	}
}

// reInit reinitialize the flowtoken, and adjusts the flow threshold based on success rate of call.
func (ft *FlowToken) reInit(currSec int64) {
	succ := ft.succCount
	fail := ft.failCount
	exhaust := ft.exhaustCount

	if ft.lastReinitTime <= 0 {
		// first init
		ft.cwnd = ft.initCwnd
	} else if ft.allFail(succ, fail) {
		// all fail, we reduce the window size by half
		ft.cwnd >>= 1
		if ft.cwnd < ft.minCwnd {
			ft.cwnd = ft.minCwnd
		}
	} else if ft.allSucc(succ, fail) {
		// all success, we increase the window size linearly.
		if (succ >= ft.ssthresh || exhaust > 0) && ft.ssthresh < math.MaxInt32 {
			// the count of success exceeds the ssthresh, it indicates the server can withstand more workload,
			// so we increase ssthresh.
			if succ < ft.initCwnd && ft.ssthresh < ft.initCwnd {
				ft.ssthresh += ft.initCwnd
			} else {
				ft.ssthresh += succ
			}
		}

		ft.cwnd = ft.ssthresh
	} else if ft.partFail(succ, fail) {
		// partial fail, we reduce the window size.
		if fail >= succ {
			// the count of fail is more than succ, we reduce the window size by half.
			ft.cwnd >>= 1
		} else {
			ft.cwnd -= fail
			if ft.ssthresh > succ {
				// it indicates the server may overload, we reset ssthresh to the count of success.
				ft.ssthresh = succ
			}
		}

		if ft.cwnd < ft.minCwnd {
			ft.cwnd = ft.minCwnd
		}
	} else if succ > 0 {
		if ft.cwnd <= ft.minCwnd {
			ft.cwnd = ft.minCwnd + succ
		}
	}

	if ft.maxCwnd > 0 && ft.cwnd > ft.maxCwnd {
		ft.tokenNum = ft.maxCwnd
	} else {
		ft.tokenNum = ft.cwnd
	}

	//	ft.totalCount = 0
	ft.succCount = 0
	ft.failCount = 0
	ft.exhaustCount = 0
	ft.lastReinitTime = currSec

	return
}

// allFail estimate whether all requests fail.
// If the success rate is less than 0.02, we think that is all fail.
func (ft *FlowToken) allFail(succ, fail int64) bool {
	total := succ + fail
	if total < 200 {
		// if the total requests is small, the success rate based on percentage may be inaccurate,
		// so we just compare with minCwnd.
		if succ == 0 && fail > 0 {
			return true
		}
		return fail >= ft.minCwnd && succ < ft.minCwnd
	} else {
		failRate := float64(fail) / float64(total)
		if failRate >= 0.98 {
			return true
		} else {
			return false
		}
	}
}

// allSucc estimate whether all requests success.
// If the success rate is more than 0.98, we think that is all success.
func (ft *FlowToken) allSucc(succ, fail int64) bool {
	total := succ + fail
	if total < 200 {
		return fail < ft.minCwnd && succ >= ft.minCwnd
	} else {
		failRate := float64(fail) / float64(total)
		if failRate <= 0.02 {
			return true
		} else {
			return false
		}
	}
}

// partFail estimate whether requests are partial fail.
// The requests should have been checked by allFail and allSucc.
func (ft *FlowToken) partFail(succ, fail int64) bool {
	total := succ + fail
	if total < 200 {
		return fail >= ft.minCwnd
	} else {
		failRate := float64(fail) / float64(total)
		if failRate <= 0.02 {
			return false
		} else {
			return true
		}
	}
}

// reportSucc report request status to flowtoken.
func (ft *FlowToken) reportSucc() {
	ft.l.Lock()
	defer ft.l.Unlock()

	ft.succCount++
	ft.tokenNum++
}

// reportFail report request status to flowtoken.
func (ft *FlowToken) reportFail() {
	ft.l.Lock()
	defer ft.l.Unlock()

	ft.failCount++
}

// Succ report request status to flowtoken.
func (t *Token) Succ() {
	t.ft.reportSucc()
}

// Fail report request status to flowtoken.
func (t *Token) Fail() {
	t.ft.reportFail()
}

func (s *Snapshot) String() string {
	return fmt.Sprintf("ServId:%s, TokenNum:%d, Cwnd:%d, Ssthresh:%d, LastReinitTime:%d, TotalCount:%d, SuccCount:%d, FailCount:%d, ExhaustCount:%d",
		s.ServId, s.TokenNum, s.Cwnd, s.Ssthresh, s.LastReinitTime, s.TotalCount, s.SuccCount, s.FailCount, s.ExhaustCount)
}

// TokenExhaustedError represents there is no available token.
type TokenExhaustedError struct {
	Msg      string
	Snapshot *Snapshot
}

// Error return the string format of TokenExhaustedError.
func (e *TokenExhaustedError) Error() string {
	return fmt.Sprintf("flowtoken shutdown, %s, snapshot:[%s]", e.Msg, e.Snapshot)
}

// IsTokenExhaustedError judge whether the error is TokenExhaustedError
func IsTokenExhaustedError(e error) (*TokenExhaustedError, bool) {
	if e == nil {
		return nil, false
	}

	te, ok := e.(*TokenExhaustedError)
	return te, ok
}
