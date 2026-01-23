package e2e

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	logs2 "github.com/redis/go-redis/v9/internal/maintnotifications/logs"
)

// logs is a slice of strings that provides additional functionality
// for filtering and analysis
type logs []string

func (l logs) Contains(searchString string) bool {
	for _, log := range l {
		if log == searchString {
			return true
		}
	}
	return false
}

func (l logs) GetCount() int {
	return len(l)
}

func (l logs) GetCountThatContain(searchString string) int {
	count := 0
	for _, log := range l {
		if strings.Contains(log, searchString) {
			count++
		}
	}
	return count
}

func (l logs) GetLogsFiltered(filter func(string) bool) []string {
	filteredLogs := make([]string, 0, len(l))
	for _, log := range l {
		if filter(log) {
			filteredLogs = append(filteredLogs, log)
		}
	}
	return filteredLogs
}

func (l logs) GetTimedOutLogs() logs {
	return l.GetLogsFiltered(isTimeout)
}

func (l logs) GetLogsPerConn(connID uint64) logs {
	return l.GetLogsFiltered(func(log string) bool {
		return strings.Contains(log, fmt.Sprintf("conn[%d]", connID))
	})
}

func (l logs) GetAnalysis() *LogAnalisis {
	return NewLogAnalysis(l)
}

// TestLogCollector is a simple logger that captures logs for analysis
// It is thread safe and can be used to capture logs from multiple clients
// It uses type logs to provide additional functionality like filtering
// and analysis
type TestLogCollector struct {
	l               logs
	doPrint         bool
	matchFuncs      []*MatchFunc
	matchFuncsMutex sync.Mutex
	mu              sync.Mutex
}

func (tlc *TestLogCollector) DontPrint() {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	tlc.doPrint = false
}

func (tlc *TestLogCollector) DoPrint() {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	tlc.l = make([]string, 0)
	tlc.doPrint = true
}

// MatchFunc is a slice of functions that check the logs for a specific condition
// use in WaitForLogMatchFunc
type MatchFunc struct {
	completed   atomic.Bool
	F           func(lstring string) bool
	matches     []string
	matchesMu   sync.Mutex // protects matches slice
	found       chan struct{} // channel to notify when match is found, will be closed
	done        func()
}

func (tlc *TestLogCollector) Printf(_ context.Context, format string, v ...interface{}) {
	tlc.mu.Lock()
	lstr := fmt.Sprintf(format, v...)

	// Check if there are match functions to process
	// Use matchFuncsMutex to safely read matchFuncs
	tlc.matchFuncsMutex.Lock()
	hasMatchFuncs := len(tlc.matchFuncs) > 0
	// Create a copy of matchFuncs to avoid holding the lock while processing
	matchFuncsCopy := make([]*MatchFunc, len(tlc.matchFuncs))
	copy(matchFuncsCopy, tlc.matchFuncs)
	tlc.matchFuncsMutex.Unlock()

	if hasMatchFuncs {
		go func(lstr string) {
			for _, matchFunc := range matchFuncsCopy {
				if matchFunc.F(lstr) {
					matchFunc.matchesMu.Lock()
					matchFunc.matches = append(matchFunc.matches, lstr)
					matchFunc.matchesMu.Unlock()
					matchFunc.done()
					return
				}
			}
		}(lstr)
	}
	if tlc.doPrint {
		fmt.Println(lstr)
	}
	tlc.l = append(tlc.l, fmt.Sprintf(format, v...))
	tlc.mu.Unlock()
}

func (tlc *TestLogCollector) WaitForLogContaining(searchString string, timeout time.Duration) bool {
	timeoutCh := time.After(timeout)
	ticker := time.NewTicker(100 * time.Millisecond)
	for {
		select {
		case <-timeoutCh:
			return false
		case <-ticker.C:
			if tlc.Contains(searchString) {
				return true
			}
		}
	}
}

func (tlc *TestLogCollector) MatchOrWaitForLogMatchFunc(mf func(string) bool, timeout time.Duration) (string, bool) {
	if logs := tlc.GetLogsFiltered(mf); len(logs) > 0 {
		return logs[0], true
	}
	return tlc.WaitForLogMatchFunc(mf, timeout)
}

func (tlc *TestLogCollector) WaitForLogMatchFunc(mf func(string) bool, timeout time.Duration) (string, bool) {
	matchFunc := &MatchFunc{
		completed: atomic.Bool{},
		F:         mf,
		found:     make(chan struct{}),
		matches:   make([]string, 0),
	}
	matchFunc.done = func() {
		if !matchFunc.completed.CompareAndSwap(false, true) {
			return
		}
		close(matchFunc.found)
		tlc.matchFuncsMutex.Lock()
		defer tlc.matchFuncsMutex.Unlock()
		for i, mf := range tlc.matchFuncs {
			if mf == matchFunc {
				tlc.matchFuncs = append(tlc.matchFuncs[:i], tlc.matchFuncs[i+1:]...)
				return
			}
		}
	}

	tlc.matchFuncsMutex.Lock()
	tlc.matchFuncs = append(tlc.matchFuncs, matchFunc)
	tlc.matchFuncsMutex.Unlock()

	select {
	case <-matchFunc.found:
		matchFunc.matchesMu.Lock()
		defer matchFunc.matchesMu.Unlock()
		if len(matchFunc.matches) > 0 {
			return matchFunc.matches[0], true
		}
		return "", false
	case <-time.After(timeout):
		return "", false
	}
}

func (tlc *TestLogCollector) GetLogs() logs {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	return tlc.l
}

func (tlc *TestLogCollector) DumpLogs() {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	fmt.Println("Dumping logs:")
	fmt.Println("===================================================")
	for _, log := range tlc.l {
		fmt.Println(log)
	}
}

func (tlc *TestLogCollector) ClearLogs() {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	tlc.l = make([]string, 0)
}

func (tlc *TestLogCollector) Contains(searchString string) bool {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	return tlc.l.Contains(searchString)
}

func (tlc *TestLogCollector) MatchContainsAll(searchStrings []string) []string {
	// match a log that contains all
	return tlc.GetLogsFiltered(func(log string) bool {
		for _, searchString := range searchStrings {
			if !strings.Contains(log, searchString) {
				return false
			}
		}
		return true
	})
}

func (tlc *TestLogCollector) GetLogCount() int {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	return tlc.l.GetCount()
}

func (tlc *TestLogCollector) GetLogCountThatContain(searchString string) int {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	return tlc.l.GetCountThatContain(searchString)
}

func (tlc *TestLogCollector) GetLogsFiltered(filter func(string) bool) logs {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	return tlc.l.GetLogsFiltered(filter)
}

func (tlc *TestLogCollector) GetTimedOutLogs() []string {
	return tlc.GetLogsFiltered(isTimeout)
}

func (tlc *TestLogCollector) GetLogsPerConn(connID uint64) logs {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	return tlc.l.GetLogsPerConn(connID)
}

func (tlc *TestLogCollector) GetAnalysisForConn(connID uint64) *LogAnalisis {
	return NewLogAnalysis(tlc.GetLogsPerConn(connID))
}

func NewTestLogCollector() *TestLogCollector {
	return &TestLogCollector{
		l: make([]string, 0),
	}
}

func (tlc *TestLogCollector) GetAnalysis() *LogAnalisis {
	return NewLogAnalysis(tlc.GetLogs())
}

func (tlc *TestLogCollector) Clear() {
	tlc.mu.Lock()
	defer tlc.mu.Unlock()
	tlc.matchFuncs = make([]*MatchFunc, 0)
	tlc.l = make([]string, 0)
}

// LogAnalisis provides analysis of logs captured by TestLogCollector
type LogAnalisis struct {
	logs                    []string
	TimeoutErrorsCount      int64
	RelaxedTimeoutCount     int64
	RelaxedPostHandoffCount int64
	UnrelaxedTimeoutCount   int64
	UnrelaxedAfterMoving    int64
	ConnectionCount         int64
	connLogs                map[uint64][]string
	connIds                 map[uint64]bool

	TotalNotifications int64
	MovingCount        int64
	MigratingCount     int64
	MigratedCount      int64
	SMigratingCount    int64
	SMigratedCount     int64
	FailingOverCount   int64
	FailedOverCount    int64
	UnexpectedCount    int64

	TotalHandoffCount             int64
	FailedHandoffCount            int64
	SucceededHandoffCount         int64
	TotalHandoffRetries           int64
	TotalHandoffToCurrentEndpoint int64

	// Cluster state reload tracking
	ClusterStateReloadCount int64
}

func NewLogAnalysis(logs []string) *LogAnalisis {
	la := &LogAnalisis{
		logs:     logs,
		connLogs: make(map[uint64][]string),
		connIds:  make(map[uint64]bool),
	}
	la.Analyze()
	return la
}

func (la *LogAnalisis) Analyze() {
	hasMoving := false
	for _, log := range la.logs {
		if isTimeout(log) {
			la.TimeoutErrorsCount++
		}
		if strings.Contains(log, "MOVING") {
			hasMoving = true
		}
		if strings.Contains(log, logs2.RelaxedTimeoutDueToNotificationMessage) {
			la.RelaxedTimeoutCount++
		}
		if strings.Contains(log, logs2.ApplyingRelaxedTimeoutDueToPostHandoffMessage) {
			la.RelaxedTimeoutCount++
			la.RelaxedPostHandoffCount++
		}
		if strings.Contains(log, logs2.UnrelaxedTimeoutMessage) {
			la.UnrelaxedTimeoutCount++
		}
		if strings.Contains(log, logs2.UnrelaxedTimeoutAfterDeadlineMessage) {
			if hasMoving {
				la.UnrelaxedAfterMoving++
			} else {
				fmt.Printf("Unrelaxed after deadline but no MOVING: %s\n", log)
			}
		}

		if strings.Contains(log, logs2.ProcessingNotificationMessage) {
			la.TotalNotifications++

			switch {
			case notificationType(log, "MOVING"):
				la.MovingCount++
			case notificationType(log, "SMIGRATING"):
				la.SMigratingCount++
			case notificationType(log, "SMIGRATED"):
				la.SMigratedCount++
			case notificationType(log, "MIGRATING"):
				la.MigratingCount++
			case notificationType(log, "MIGRATED"):
				la.MigratedCount++
			case notificationType(log, "FAILING_OVER"):
				la.FailingOverCount++
			case notificationType(log, "FAILED_OVER"):
				la.FailedOverCount++
			default:
				fmt.Printf("[ERROR] Unexpected notification: %s\n", log)
				la.UnexpectedCount++
			}
		}

		// Track cluster state reloads (triggered by SMIGRATED notifications)
		if strings.Contains(log, logs2.SlotMigratedMessage) {
			la.ClusterStateReloadCount++
		}

		if strings.Contains(log, "conn[") {
			connID := extractConnID(log)
			if _, ok := la.connIds[connID]; !ok {
				la.connIds[connID] = true
				la.ConnectionCount++
			}
			la.connLogs[connID] = append(la.connLogs[connID], log)
		}

		if strings.Contains(log, logs2.SchedulingHandoffToCurrentEndpointMessage) {
			la.TotalHandoffToCurrentEndpoint++
		}

		if strings.Contains(log, logs2.HandoffSuccessMessage) {
			la.SucceededHandoffCount++
		}
		if strings.Contains(log, logs2.HandoffFailedMessage) {
			la.FailedHandoffCount++
		}
		if strings.Contains(log, logs2.HandoffStartedMessage) {
			la.TotalHandoffCount++
		}
		if strings.Contains(log, logs2.HandoffRetryAttemptMessage) {
			la.TotalHandoffRetries++
		}
	}
}

func (la *LogAnalisis) Print(t *testing.T) {
	t.Logf("Log Analysis results for %d logs and %d connections:", len(la.logs), len(la.connIds))
	t.Logf("Connection Count: %d", la.ConnectionCount)
	t.Logf("-------------")
	t.Logf("-Timeout Analysis-")
	t.Logf("-------------")
	t.Logf("Timeout Errors: %d", la.TimeoutErrorsCount)
	t.Logf("Relaxed Timeout Count: %d", la.RelaxedTimeoutCount)
	t.Logf(" - Relaxed Timeout After Post-Handoff: %d", la.RelaxedPostHandoffCount)
	t.Logf("Unrelaxed Timeout Count: %d", la.UnrelaxedTimeoutCount)
	t.Logf(" - Unrelaxed Timeout After Moving: %d", la.UnrelaxedAfterMoving)
	t.Logf("-------------")
	t.Logf("-Handoff Analysis-")
	t.Logf("-------------")
	t.Logf("Total Handoffs: %d", la.TotalHandoffCount)
	t.Logf(" - Succeeded: %d", la.SucceededHandoffCount)
	t.Logf(" - Failed: %d", la.FailedHandoffCount)
	t.Logf(" - Retries: %d", la.TotalHandoffRetries)
	t.Logf(" - Handoffs to current endpoint: %d", la.TotalHandoffToCurrentEndpoint)
	t.Logf("-------------")
	t.Logf("-Notification Analysis-")
	t.Logf("-------------")
	t.Logf("Total Notifications: %d", la.TotalNotifications)
	t.Logf(" - MOVING: %d", la.MovingCount)
	t.Logf(" - MIGRATING: %d", la.MigratingCount)
	t.Logf(" - MIGRATED: %d", la.MigratedCount)
	t.Logf(" - FAILING_OVER: %d", la.FailingOverCount)
	t.Logf(" - FAILED_OVER: %d", la.FailedOverCount)
	t.Logf(" - Unexpected: %d", la.UnexpectedCount)
	t.Logf("-------------")
	t.Logf("-Cluster-Specific Notification Analysis-")
	t.Logf("-------------")
	t.Logf(" - SMIGRATING: %d", la.SMigratingCount)
	t.Logf(" - SMIGRATED: %d", la.SMigratedCount)
	t.Logf(" - Cluster state reloads: %d", la.ClusterStateReloadCount)
	t.Logf("-------------")
	t.Logf("Log Analysis completed successfully")
}

func extractConnID(log string) uint64 {
	logParts := strings.Split(log, "conn[")
	if len(logParts) < 2 {
		return 0
	}
	connIDStr := strings.Split(logParts[1], "]")[0]
	connID, err := strconv.ParseUint(connIDStr, 10, 64)
	if err != nil {
		return 0
	}
	return connID
}

func notificationType(log string, nt string) bool {
	return strings.Contains(log, nt)
}
func connID(log string, connID uint64) bool {
	return strings.Contains(log, fmt.Sprintf("conn[%d]", connID))
}
func seqID(log string, seqID int64) bool {
	return strings.Contains(log, fmt.Sprintf("seqID[%d]", seqID))
}
