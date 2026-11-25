package redis

import (
	"bufio"
	"context"
	"crypto/sha1"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9/internal/otel"
	"github.com/redis/go-redis/v9/internal/pool"
	"github.com/redis/go-redis/v9/internal/proto"
	"github.com/redis/go-redis/v9/maintnotifications"
)

// partialRetryServer is a scripted RESP server for pipeline retry tests.
// The first connection answers the first pipeline command and then drops
// mid-pipeline; later connections answer everything. It records every
// command name it receives so tests can assert what a retry resent.
type partialRetryServer struct {
	ln       net.Listener
	conns    atomic.Int32
	received atomic.Int32 // total commands seen, all connections
	incrs    atomic.Int32 // INCR commands seen, all connections
}

func newPartialRetryServer(t *testing.T) *partialRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &partialRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

// readPartialRetryCommand reads one RESP array command and returns its
// lower-cased name, or "" when the stream is done.
func readPartialRetryCommand(scanner *bufio.Scanner) string {
	if !scanner.Scan() {
		return ""
	}
	header := scanner.Text()
	if !strings.HasPrefix(header, "*") {
		return ""
	}
	n, err := strconv.Atoi(strings.TrimPrefix(header, "*"))
	if err != nil || n <= 0 {
		return ""
	}
	name := ""
	for i := 0; i < n; i++ {
		if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "$") || !scanner.Scan() {
			return ""
		}
		if i == 0 {
			name = strings.ToLower(scanner.Text())
		}
	}
	return name
}

func (s *partialRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	answered := false
	for {
		name := readPartialRetryCommand(scanner)
		if name == "" {
			return
		}
		s.received.Add(1)
		if name == "incr" {
			s.incrs.Add(1)
		}
		// Connection setup commands (HELLO and friends) are not part of
		// the pipeline: always answer them and never drop on them.
		isPipelineCmd := name == "incr" || name == "set" || name == "get"
		if first && answered && isPipelineCmd {
			// Drop the connection mid-pipeline: this command and the
			// ones after it get no reply on this connection.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "incr":
			reply = ":1\r\n"
		case "get":
			reply = "$1\r\n1\r\n"
		case "ping":
			reply = "+PONG\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
		if isPipelineCmd {
			answered = true
		}
	}
}

// retryTestHelper is the subset of *testing.T / *testing.B the scripted
// retry servers need, so benchmarks can reuse the same harness.
type retryTestHelper interface {
	Helper()
	Cleanup(func())
	Fatalf(format string, args ...any)
}

func newPartialRetryClient(t retryTestHelper, addr string) *Client {
	t.Helper()
	client := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestPipelineRetryDoesNotReplayAnsweredCommands: when a connection drops
// after the first pipeline reply was read, the retry must resend only the
// commands that were not answered. Before the fix the whole batch was
// resent, so the server executed the first INCR twice.
// connStateRetryServer is a scripted RESP server for connection-state
// pipeline retry tests. It tracks per-connection state (selected DB,
// MULTI) so tests can assert a retry did not silently drop it. The first
// connection answers the first pipeline command and then drops
// mid-pipeline; later connections answer everything.
// queuedCmd is one command accepted into the open MULTI frame of a
// connStateRetryServer connection, so EXEC can answer one result
// element per queued command (a queued SELECT the frame rejects
// answers its error as its element, exactly like a real server).
type queuedCmd struct {
	name string
	args []string
}

type connStateRetryServer struct {
	ln               net.Listener
	conns            atomic.Int32
	setDB            atomic.Int32 // DB in effect when SET last ran, -1 = never
	sets             atomic.Int32
	setWhileWatched  atomic.Int32
	multiCount       atomic.Int32
	execWithMulti    atomic.Int32
	execWithoutMulti atomic.Int32
	execs            atomic.Int32 // EXECs received, including rejected ones
	execAborted      atomic.Int32 // EXECs that aborted because a watched key was modified
	incrs            atomic.Int32
	exists           atomic.Int32
	selects          atomic.Int32
	swapdbs          atomic.Int32
	discards         atomic.Int32
	resets           atomic.Int32
	mgets            atomic.Int32
	// dbPerm is the server-global SWAPDB permutation: dbPerm[d] is
	// the physical database logical database d currently maps to.
	// SWAPDB exchanges two entries for every connection, so tests
	// with mixed pairs (SWAPDB 0 1 then SWAPDB 0 2) need the full
	// permutation — a single swapped flag cannot tell them apart.
	swapMu  sync.Mutex
	dbPerm  [16]int
	askings atomic.Int32
	// requireAskingBeforeMulti, when set, makes this server refuse
	// (MOVED) a write queued into a MULTI frame that was not opened
	// directly after ASKING, modelling a cluster node importing the
	// slot: ASKING authorises only the command immediately after it,
	// so a transaction opened without it cannot write there.
	requireAskingBeforeMulti bool
	// dropAfter is how many pipeline commands the first connection
	// answers before dropping (1 = the historical behaviour).
	dropAfter int
	// secondMaxDB is the highest DB index the replacement (second and
	// later) connections accept; the first connection always accepts
	// 0-15. Tests that need a reissued SELECT to fail on the retry
	// connection set it below 15.
	secondMaxDB int
	// tryagainConn, when nonzero, is the connection whose first MGET
	// is answered TRYAGAIN (slot migration); later MGETs succeed.
	tryagainConn int32
	// failWatch, when set, makes every WATCH answer NOPERM: the
	// watch is rejected and establishes nothing.
	failWatch atomic.Bool
	// failDiscard, when set, makes every DISCARD answer NOPERM: the
	// transaction stays open, exactly like a real server refusing
	// DISCARD for an ACL user (see the script server for the same
	// rule).
	failDiscard atomic.Bool
	// failExec, when set, makes every EXEC answer NOPERM: the
	// transaction stays open, exactly like a real server refusing
	// EXEC for an ACL user (see failDiscard for the same rule).
	failExec atomic.Bool
	// msetTryagainOnce, when set, makes the next MSET answer TRYAGAIN
	// once; later MSETs succeed. Unlike msetTryagain it lets a retry
	// complete, so a test can assert what the retry re-ran.
	msetTryagainOnce atomic.Bool
	// msetNoreplicasOnce, when set, makes the next MSET answer
	// NOREPLICAS without executing: the keys are not recorded,
	// exactly like a real server refusing the write while replicas
	// lag. Unlike a TRYAGAIN it is a retryable error on the
	// boundary command itself, so the retry suffix starts at it.
	msetNoreplicasOnce atomic.Bool
	// trackValues, when set, makes SET/MSET record values and GET
	// return the recorded value for a key instead of the fixed "v":
	// tests that assert a retry preserved the batch's ordering need
	// to read the final value back. Values are keyed by the
	// effective database they were written in: SWAPDB moves values
	// with the databases, so a GET after a swap reads the database
	// the swap moved in (see dbValueKey).
	trackValues bool
	valuesMu    sync.Mutex
	values      map[string]string
	watches     atomic.Int32
	// failMulti, when set, makes every MULTI answer NOPERM (an ACL
	// denying MULTI): the transaction never opens and the commands
	// after it run standalone.
	failMulti atomic.Bool
	waits     atomic.Int32
	// failWait, when set, makes every WAIT answer NOPERM: the
	// barrier is rejected and measures nothing.
	failWait atomic.Bool
	// failSwapdbOnce, when set, makes the next SWAPDB answer
	// NOREPLICAS without swapping; later SWAPDBs succeed.
	failSwapdbOnce atomic.Bool
	// failSwapdbTryagainOnce, when set, makes the next SWAPDB answer
	// TRYAGAIN without swapping; later SWAPDBs succeed. Unlike
	// failSwapdbOnce it models a migration rejection, so the
	// boundary is the SWAPDB itself.
	failSwapdbTryagainOnce atomic.Bool
	// setTryagainOnce, when set, makes the next SET answer TRYAGAIN
	// without executing: the key is not recorded and the database
	// is not changed, so a retry that runs it lands in the
	// database the swap state leaves it in.
	setTryagainOnce atomic.Bool
	// getTryagainOnce, when set, makes the next GET answer TRYAGAIN
	// without executing: the key is not read, exactly like a real
	// server refusing the read while a slot migrates. Unlike
	// mgetTryagainCount it fires once, so a retry completes.
	getTryagainOnce atomic.Bool
	// setReadonlyOnce, when set, makes the next SET answer READONLY
	// without executing: the key is not recorded, exactly like a
	// read-only replica refusing the write during promotion.
	setReadonlyOnce atomic.Bool
	// setsDB0 and setsDB1 count SETs that actually executed in DB 0
	// and DB 1 (a TRYAGAIN SET counts in neither): tests that assert
	// a retry preserved the batch's database ordering read them
	// instead of the last-DB snapshot. setsDB2 is the same counter
	// for DB 2, for mixed SWAPDB pairs that move writes there.
	setsDB0 atomic.Int32
	setsDB1 atomic.Int32
	setsDB2 atomic.Int32
	// failIncrReadonlyOnce, when set, makes the next INCR answer
	// READONLY without executing: it counts in neither incrs nor
	// any watch dirtiness, so the counter records executions only.
	failIncrReadonlyOnce atomic.Bool
	// failExecAbortOnce, when set, makes the next EXEC answer
	// EXECABORT without executing the queued body: the frame was
	// aborted by the server, so its body never ran.
	failExecAbortOnce atomic.Bool
	// failExecNoreplicasOnce, when set, makes the next EXEC answer
	// NOREPLICAS without closing the transaction: the frame stays
	// open, exactly like a real server refusing EXEC while MULTI
	// is still live, so a retry that reuses the connection sends
	// MULTI into the open transaction.
	failExecNoreplicasOnce atomic.Bool
	// msetTryagain, when set, makes every MSET answer TRYAGAIN.
	msetTryagain atomic.Bool
	// dropExecFirst, when set, makes the first connection close
	// without replying to EXEC (the transaction never commits there).
	dropExecFirst bool
	// evalTryagainOnce, when set, makes the next EVAL answer TRYAGAIN
	// without executing: the script never ran, so a retry that runs
	// it increments the counter once.
	evalTryagainOnce atomic.Bool
	// evals counts EVALs that actually executed (a TRYAGAIN EVAL
	// counts in neither evals nor any other counter).
	evals atomic.Int32
	// mgetTryagainCount, when positive, makes that many MGETs answer
	// TRYAGAIN without executing; later MGETs succeed. Unlike
	// tryagainConn it counts across connections, so a test can fail
	// the MGET on every attempt until the count runs out.
	mgetTryagainCount atomic.Int32
	// setNoreplicasOnce, when set, makes the next SET answer
	// NOREPLICAS without executing: the key is not recorded, exactly
	// like a real server refusing the write while replicas lag.
	setNoreplicasOnce atomic.Bool
	// pingLoadingOnce, when set, makes the next PING answer LOADING
	// without executing: a refused PING cannot invalidate the data
	// results after it, exactly like a real server refusing
	// commands while loading.
	pingLoadingOnce atomic.Bool
	// dropAfterExecReplyOnce, when set, makes the connection close
	// right after writing the EXEC reply: the queued commands ran,
	// but the commands after EXEC get no reply on this connection.
	dropAfterExecReplyOnce atomic.Bool
	// cachingCmds counts CLIENT CACHING YES commands received, and
	// optinGets counts GETs that arrived while a one-shot CLIENT
	// CACHING YES flag was armed on their connection: the next
	// command after the flag runs opt-in, and the flag is then
	// consumed.
	cachingCmds atomic.Int32
	optinGets   atomic.Int32
}

func newConnStateRetryServer(t retryTestHelper) *connStateRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &connStateRetryServer{ln: ln, dropAfter: 1, secondMaxDB: 15}
	s.setDB.Store(-1)
	for i := range s.dbPerm {
		s.dbPerm[i] = i
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

// dbValueKey keys a tracked value by the effective database it was
// written in: SWAPDB moves values with the databases, so a GET after
// a swap must read the database the swap moved in, not the key's last
// write in any database.
func dbValueKey(db int, key string) string {
	return strconv.Itoa(db) + "\x00" + key
}

// effectiveDB maps a connection's selected database through the
// server-global SWAPDB permutation.
func (s *connStateRetryServer) effectiveDB(db int) int {
	s.swapMu.Lock()
	defer s.swapMu.Unlock()
	if db < 0 || db >= len(s.dbPerm) {
		return db
	}
	return s.dbPerm[db]
}

// applySwapdb exchanges two databases in the server-global
// permutation, exactly like a real server's SWAPDB.
func (s *connStateRetryServer) applySwapdb(a, b int) {
	s.swapMu.Lock()
	defer s.swapMu.Unlock()
	if a < 0 || a >= len(s.dbPerm) || b < 0 || b >= len(s.dbPerm) {
		return
	}
	s.dbPerm[a], s.dbPerm[b] = s.dbPerm[b], s.dbPerm[a]
}

// readConnStateRetryCommand reads one RESP array command and returns its
// lower-cased name and arguments, or "" when the stream is done.
func readConnStateRetryCommand(scanner *bufio.Scanner) (string, []string) {
	if !scanner.Scan() {
		return "", nil
	}
	header := scanner.Text()
	if !strings.HasPrefix(header, "*") {
		return "", nil
	}
	n, err := strconv.Atoi(strings.TrimPrefix(header, "*"))
	if err != nil || n <= 0 {
		return "", nil
	}
	var args []string
	for i := 0; i < n; i++ {
		if !scanner.Scan() || !strings.HasPrefix(scanner.Text(), "$") || !scanner.Scan() {
			return "", nil
		}
		args = append(args, scanner.Text())
	}
	if len(args) == 0 {
		return "", nil
	}
	return strings.ToLower(args[0]), args[1:]
}

func (s *connStateRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	first := n == 1
	scanner := bufio.NewScanner(conn)
	db := 0
	inMulti := false
	watched := false
	// watchedKeys tracks the keys this connection watches, and dirty
	// whether one of them was modified since the WATCH: like a real
	// server, an EXEC then aborts (nil array) instead of committing.
	watchedKeys := map[string]bool{}
	dirty := false
	answered := 0
	mgetsThisConn := 0
	// cachingArmed tracks a one-shot CLIENT CACHING YES flag on this
	// connection: the next command after it runs opt-in, and the flag
	// is then consumed (see the modelling below).
	cachingArmed := false
	// limited tracks whether this connection authenticated as the
	// limited user: WAIT is refused NOPERM for it, exactly like a
	// real server's ACL, so a barrier that runs after the AUTH to
	// that user fails where one that ran before it succeeds.
	limited := false
	// queued holds the commands accepted into the open MULTI frame,
	// in order, so EXEC can answer one result element per queued
	// command (see the exec case below).
	var queued []queuedCmd
	// askingTx records whether the ASKING flag was in effect when the
	// open MULTI frame started: ASKING authorises only the command
	// immediately after it, so only a MULTI directly preceded by
	// ASKING runs under it. lastWasAsking tracks the previous command
	// for that decision.
	askingTx := false
	lastWasAsking := false
	// effDB is the database a SET lands in: SWAPDB is server-global,
	// so a swap exchanges two databases for every connection — the
	// effective database is the permutation's image of the
	// connection's selected database.
	effDB := func() int {
		return s.effectiveDB(db)
	}
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		// CLIENT CACHING YES arms a one-shot opt-in flag for the
		// next command on this connection: that command runs
		// opt-in, and the flag is then consumed. A later CACHING
		// YES re-arms it.
		if name == "client" && len(args) >= 2 && strings.EqualFold(args[0], "caching") && strings.EqualFold(args[1], "yes") {
			s.cachingCmds.Add(1)
			cachingArmed = true
		} else if cachingArmed {
			if name == "get" {
				s.optinGets.Add(1)
			}
			cachingArmed = false
		}
		// Connection setup commands (HELLO and friends) are not part of
		// the pipeline: always answer them and never drop on them.
		isPipelineCmd := name == "select" || name == "set" || name == "get" || name == "exists" ||
			name == "multi" || name == "exec" || name == "watch" || name == "unwatch" ||
			name == "incr" || name == "discard" || name == "reset" || name == "swapdb" || name == "mget"
		if first && answered >= s.dropAfter && isPipelineCmd {
			// Drop the connection mid-pipeline: this command and the
			// ones after it get no reply on this connection.
			return
		}
		// Record commands accepted into the open transaction, so
		// EXEC can answer one result element per queued command
		// below. MULTI, EXEC, DISCARD, RESET and WATCH are never
		// queued: a real server rejects WATCH inside MULTI and a
		// nested MULTI outright. UNWATCH is queued, like any other
		// command: processCommand excludes only MULTI, EXEC,
		// DISCARD, WATCH and RESET from queueing, so an UNWATCH
		// inside MULTI answers +QUEUED, holds an EXEC result element
		// and clears the watch only when EXEC runs it.
		if inMulti {
			switch name {
			case "multi", "exec", "discard", "reset", "watch":
			default:
				queued = append(queued, queuedCmd{name: name, args: args})
			}
		}
		// ASKING authorises only the command immediately after it:
		// remember whether the previous command was ASKING, so the
		// MULTI case can decide whether the frame it opens runs
		// under it.
		prevWasAsking := lastWasAsking
		lastWasAsking = name == "asking"
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "auth":
			// AUTH to the limited user restricts this connection
			// from here on; any other user keeps full access.
			// The reply is +OK either way, like a real server.
			if len(args) >= 1 && args[0] == "limited" {
				limited = true
			}
		case "select":
			s.selects.Add(1)
			if inMulti {
				// A queued SELECT runs only at EXEC time; DISCARD
				// drops it without effect, so it must not change
				// this connection's database yet.
				reply = "+QUEUED\r\n"
				break
			}
			if len(args) > 0 {
				if n, err := strconv.Atoi(args[0]); err == nil {
					maxDB := 15
					if !first {
						maxDB = s.secondMaxDB
					}
					if n < 0 || n > maxDB {
						reply = "-ERR DB index is out of range\r\n"
					} else {
						db = n
					}
				}
			}
		case "set":
			s.sets.Add(1)
			if s.setReadonlyOnce.CompareAndSwap(true, false) {
				// A READONLY SET never executed: it records no
				// value and changes no database, exactly like a
				// real read-only replica refusing the write.
				reply = "-READONLY You can't write against a read only replica\r\n"
				break
			}
			if s.setTryagainOnce.CompareAndSwap(true, false) {
				// A TRYAGAIN SET never executed: it records no
				// value, changes no database and counts in
				// neither per-DB counter.
				reply = "-TRYAGAIN Multiple keys request during slot migration\r\n"
				break
			}
			if s.setNoreplicasOnce.CompareAndSwap(true, false) {
				// A NOREPLICAS SET never executed: it records no
				// value and counts in no per-DB counter, exactly
				// like a real server refusing the write while
				// replicas lag.
				reply = "-NOREPLICAS Not enough replicas to apply the write\r\n"
				break
			}
			s.setDB.Store(int32(effDB()))
			switch effDB() {
			case 0:
				s.setsDB0.Add(1)
			case 1:
				s.setsDB1.Add(1)
			case 2:
				s.setsDB2.Add(1)
			}
			if s.trackValues && !inMulti && len(args) >= 2 {
				// Only a SET that actually ran records its value: a
				// queued one runs only at EXEC time.
				s.valuesMu.Lock()
				if s.values == nil {
					s.values = map[string]string{}
				}
				s.values[dbValueKey(effDB(), args[0])] = args[1]
				s.valuesMu.Unlock()
			}
			if watched {
				s.setWhileWatched.Add(1)
			}
			if len(args) > 0 && watchedKeys[args[0]] {
				dirty = true
			}
			if inMulti {
				if s.requireAskingBeforeMulti && !askingTx {
					// This transaction was opened without
					// ASKING: on a cluster node importing the
					// slot, the queued write is refused with
					// MOVED instead of being queued, so it
					// holds no result element in the EXEC's
					// array either.
					reply = "-MOVED 0 127.0.0.1:0\r\n"
					queued = queued[:len(queued)-1]
				} else {
					reply = "+QUEUED\r\n"
				}
			}
		case "incr":
			if s.failIncrReadonlyOnce.CompareAndSwap(true, false) {
				// A READONLY INCR never executed: it counts as no
				// INCR and dirties no watch, exactly like a real
				// read-only replica refusing the write.
				reply = "-READONLY You can't write against a read only replica\r\n"
				break
			}
			s.incrs.Add(1)
			if len(args) > 0 && watchedKeys[args[0]] {
				dirty = true
			}
			reply = ":1\r\n"
		case "watch":
			s.watches.Add(1)
			if s.failWatch.Load() {
				reply = "-NOPERM this user has no permissions to run the 'watch' command\r\n"
				break
			}
			watched = true
			for _, key := range args {
				watchedKeys[key] = true
			}
		case "unwatch":
			if inMulti {
				// Queued, like any other command inside MULTI: the
				// watch stays live (and dirty) until EXEC runs the
				// queued UNWATCH, so nothing is cleared here.
				reply = "+QUEUED\r\n"
			} else {
				watched = false
				clear(watchedKeys)
				dirty = false
			}
		case "exists":
			s.exists.Add(1)
			if inMulti {
				reply = "+QUEUED\r\n"
			} else {
				reply = ":1\r\n"
			}
		case "get":
			if s.getTryagainOnce.CompareAndSwap(true, false) {
				// A TRYAGAIN GET never executed: it reads no
				// value, exactly like a real server refusing
				// the read while a slot migrates.
				reply = "-TRYAGAIN Multiple keys request during slot migration\r\n"
				break
			}
			reply = "$1\r\nv\r\n"
			if s.trackValues && len(args) >= 1 {
				s.valuesMu.Lock()
				v, ok := s.values[dbValueKey(effDB(), args[0])]
				s.valuesMu.Unlock()
				if ok {
					reply = "$" + strconv.Itoa(len(v)) + "\r\n" + v + "\r\n"
				}
			}
		case "mget":
			s.mgets.Add(1)
			mgetsThisConn++
			tryagainMget := false
			if s.mgetTryagainCount.Add(-1) >= 0 {
				// A TRYAGAIN MGET never executed: the count is
				// spent across connections, so a test can fail
				// the MGET on every attempt until it runs out.
				tryagainMget = true
			} else {
				s.mgetTryagainCount.Add(1)
				tryagainMget = s.tryagainConn == n && mgetsThisConn == 1
			}
			if tryagainMget {
				reply = "-TRYAGAIN Multiple keys request during slot migration\r\n"
			} else {
				reply = "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
			}
		case "mset":
			if s.msetNoreplicasOnce.CompareAndSwap(true, false) {
				// A NOREPLICAS MSET never executed: it records
				// no value, exactly like a real server refusing
				// the write while replicas lag.
				reply = "-NOREPLICAS Not enough replicas to apply the write\r\n"
			} else if s.msetTryagain.Load() || s.msetTryagainOnce.CompareAndSwap(true, false) {
				reply = "-TRYAGAIN Multiple keys request during slot migration\r\n"
			} else if s.trackValues && !inMulti {
				s.valuesMu.Lock()
				if s.values == nil {
					s.values = map[string]string{}
				}
				for i := 0; i+1 < len(args); i += 2 {
					s.values[dbValueKey(effDB(), args[i])] = args[i+1]
				}
				s.valuesMu.Unlock()
			}
		case "eval":
			if s.evalTryagainOnce.CompareAndSwap(true, false) {
				// A TRYAGAIN EVAL never executed: the script
				// never ran, so a retry that runs it increments
				// the counter once.
				reply = "-TRYAGAIN Multiple keys request during slot migration\r\n"
				break
			}
			s.evals.Add(1)
			reply = ":1\r\n"
		case "wait":
			s.waits.Add(1)
			if s.failWait.Load() || limited {
				reply = "-NOPERM this user has no permissions to run the 'wait' command\r\n"
			} else {
				reply = ":1\r\n"
			}
		case "asking":
			s.askings.Add(1)
		case "himport":
			if inMulti {
				// A queued HIMPORT PREPARE runs only at EXEC
				// time, like any other queued command.
				reply = "+QUEUED\r\n"
			}
		case "multi":
			s.multiCount.Add(1)
			if s.failMulti.Load() {
				// An ACL denying MULTI: the transaction never
				// opens and the commands after it run standalone,
				// exactly like a real server.
				reply = "-NOPERM this user has no permissions to run the 'multi' command\r\n"
				break
			}
			if inMulti {
				// A nested MULTI is rejected, but it does not
				// close the outer transaction: the frame and its
				// queued body stay open, exactly like a real
				// server, and the frame still runs under the
				// ASKING that opened it.
				reply = "-ERR MULTI calls can not be nested\r\n"
			} else {
				askingTx = prevWasAsking
				inMulti = true
			}
		case "discard":
			s.discards.Add(1)
			if s.failDiscard.Load() {
				// A rejected DISCARD leaves the transaction open:
				// the frame and its queued body stay, exactly
				// like a real server refusing DISCARD for an ACL
				// user, so a later EXEC still runs the body.
				reply = "-NOPERM this user has no permissions to run the 'discard' command\r\n"
				break
			}
			inMulti = false
			queued = queued[:0]
		case "reset":
			// RESET discards the transaction like DISCARD, and
			// clears the watches too: the queued commands never
			// ran and the connection leaves MULTI. It also
			// restores the default database, like a real server.
			s.resets.Add(1)
			db = 0
			inMulti = false
			queued = queued[:0]
			clear(watchedKeys)
			watched = false
			dirty = false
			reply = "+RESET\r\n"
		case "swapdb":
			s.swapdbs.Add(1)
			if s.failSwapdbTryagainOnce.CompareAndSwap(true, false) {
				// The first SWAPDB is rejected TRYAGAIN: the swap
				// never applied, so the retry must run it again —
				// and the answered writes after it already ran in
				// the pre-swap database, so they must not be
				// re-sent once the swap takes effect.
				reply = "-TRYAGAIN Multiple keys request during slot migration\r\n"
				break
			}
			if s.failSwapdbOnce.CompareAndSwap(true, false) {
				// The first SWAPDB fails NOREPLICAS: the swap
				// never applied, so the retry must run it again
				// (and reissue any barrier that follows it).
				reply = "-NOREPLICAS Not enough replicas to apply the swap\r\n"
				break
			}
			if inMulti {
				// A queued SWAPDB runs only at EXEC time: it
				// answers +QUEUED here and swaps (or fails) as
				// its EXEC result element, exactly like a real
				// server.
				reply = "+QUEUED\r\n"
				break
			}
			if len(args) >= 2 {
				if a, err := strconv.Atoi(args[0]); err == nil {
					if b, err := strconv.Atoi(args[1]); err == nil {
						maxDB := 15
						if !first {
							maxDB = s.secondMaxDB
						}
						if a < 0 || a > maxDB || b < 0 || b > maxDB {
							reply = "-ERR DB index is out of range\r\n"
						} else {
							s.applySwapdb(a, b)
						}
					}
				}
			}
		case "exec":
			s.execs.Add(1)
			if s.failExec.Load() {
				// A rejected EXEC leaves the transaction open:
				// the queued commands stay queued, exactly like
				// a real server refusing EXEC for an ACL user, so
				// a later DISCARD still drops them.
				reply = "-NOPERM this user has no permissions to run the 'exec' command\r\n"
				break
			}
			if s.failExecNoreplicasOnce.CompareAndSwap(true, false) {
				// The first EXEC is refused NOREPLICAS: the
				// transaction stays open, exactly like a real
				// server refusing EXEC while MULTI is still live,
				// so the frame and its queued body survive for a
				// later retry — on a fresh connection.
				reply = "-NOREPLICAS Not enough replicas to apply the transaction\r\n"
				break
			}
			if s.failExecAbortOnce.CompareAndSwap(true, false) {
				// The server aborted the transaction: the queued
				// body never executed, so the frame is closed
				// without running it, exactly like a real server
				// answering EXECABORT for a failed queued command.
				inMulti = false
				queued = queued[:0]
				reply = "-EXECABORT Transaction discarded because of previous errors\r\n"
				break
			}
			if s.dropExecFirst && first {
				// Drop before replying: the queued commands never
				// execute on this connection.
				return
			}
			if inMulti {
				inMulti = false
				clear(watchedKeys)
				watched = false
				if dirty {
					dirty = false
					s.execAborted.Add(1)
					reply = "*-1\r\n"
				} else {
					s.execWithMulti.Add(1)
					// One result element per queued command, in
					// order, exactly like a real server: a queued
					// SELECT the frame rejects (DB index out of
					// range on this connection) answers its error
					// as its element, while the EXEC itself carries
					// no error.
					var b strings.Builder
					b.WriteString("*" + strconv.Itoa(len(queued)) + "\r\n")
					for _, q := range queued {
						if q.name == "select" {
							dbIdx := -1
							if len(q.args) > 0 {
								dbIdx, _ = strconv.Atoi(q.args[0])
							}
							maxDB := 15
							if !first {
								maxDB = s.secondMaxDB
							}
							if dbIdx < 0 || dbIdx > maxDB {
								b.WriteString("-ERR DB index is out of range\r\n")
							} else {
								b.WriteString("+OK\r\n")
							}
						} else if q.name == "swapdb" {
							// A queued SWAPDB swaps only at EXEC
							// time: an out-of-range index answers
							// its error as the element, while a
							// valid one swaps and answers +OK.
							idxA, idxB := -1, -1
							if len(q.args) >= 2 {
								idxA, _ = strconv.Atoi(q.args[0])
								idxB, _ = strconv.Atoi(q.args[1])
							}
							maxDB := 15
							if !first {
								maxDB = s.secondMaxDB
							}
							if idxA < 0 || idxA > maxDB || idxB < 0 || idxB > maxDB {
								b.WriteString("-ERR DB index is out of range\r\n")
							} else {
								s.applySwapdb(idxA, idxB)
								b.WriteString("+OK\r\n")
							}
						} else {
							b.WriteString("+OK\r\n")
						}
					}
					reply = b.String()
				}
				queued = queued[:0]
			} else {
				s.execWithoutMulti.Add(1)
				reply = "-ERR EXEC without MULTI\r\n"
			}
		case "ping":
			if s.pingLoadingOnce.CompareAndSwap(true, false) {
				// A LOADING PING never executed: it reads
				// nothing, exactly like a real server refusing
				// commands while loading.
				reply = "-LOADING Redis is loading the dataset in memory\r\n"
				break
			}
			reply = "+PONG\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
		if name == "exec" && s.dropAfterExecReplyOnce.CompareAndSwap(true, false) {
			// The EXEC reply was written: the queued commands
			// ran, but the connection drops before the commands
			// after it are answered.
			return
		}
		if isPipelineCmd {
			answered++
		}
	}
}

// TestPipelineRetryKeepsSelectDB: a pipeline whose first command SELECTs a
// database must not retry its remaining commands on a fresh connection
// that is still on the default database. Regression test for the review
// finding on #4351 (SELECT dropped by the suffix retry, SET lands in DB 0).
func TestPipelineRetryKeepsSelectDB(t *testing.T) {
	server := newConnStateRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.setDB.Load(); got != 2 {
		t.Fatalf("SET ran on DB %d, want 2 (the answered SELECT must not be dropped by the retry)", got)
	}
}

// TestPipelineRetryDoesNotRunSuffixUnderAnsweredTailSelect: a SELECT
// answered after the retry boundary must not leak its database into the
// retry suffix: PING; SET out v; SELECT 1 reads [PONG, NOREPLICAS, OK],
// so the retry is [SET, SELECT 1] — and the SET must run in DB 0, the
// database it was issued for, not in DB 1 on the reused connection the
// first attempt left selected. Regression test for the Codex P1 finding
// on #4351.
func TestPipelineRetryDoesNotRunSuffixUnderAnsweredTailSelect(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.setNoreplicasOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	ping := pipe.Ping(ctx)
	set := pipe.Set(ctx, "out", "v", 0)
	sel := pipe.Select(ctx, 1)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must run the SET in DB 0 and re-run the SELECT after it)", err)
	}
	if err := ping.Err(); err != nil {
		t.Fatalf("PING: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got := server.setsDB0.Load(); got != 1 {
		t.Fatalf("server executed %d SETs in DB 0, want 1 (the retried SET must run in the database it was issued for)", got)
	}
	if got := server.setsDB1.Load(); got != 0 {
		t.Fatalf("server executed %d SETs in DB 1, want 0 (the answered SELECT after the boundary must not leak into the retry suffix)", got)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the retry must start on a fresh connection, not the one the first attempt left in DB 1)", got)
	}
}

// TestPipelineRetryKeepsMultiTransaction: a MULTI issued in a regular
// pipeline must not be dropped by the suffix retry, or the retried EXEC
// runs on a fresh connection with no transaction open.
func TestPipelineRetryKeepsMultiTransaction(t *testing.T) {
	server := newConnStateRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "k", "v", 0)
	exec := pipe.Do(ctx, "exec")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got := server.execWithoutMulti.Load(); got != 0 {
		t.Fatalf("EXEC ran %d time(s) on a connection with no MULTI open", got)
	}
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("EXEC ran %d time(s) inside MULTI, want 1", got)
	}
}

// TestPipelineRetryRejectedMultiDoesNotRetryTxBody: a MULTI the server
// rejected (an ACL denying MULTI) opens no transaction: the INCR after
// it ran standalone, so the retry must not re-run it as transaction
// body. Before the fix pipelineTxBodyNeedsRetryMap opened a frame on
// the rejected MULTI and the tail filter retained the already-executed
// INCR, so the counter incremented twice. Regression test for the Codex
// P1 finding on #4351 (review 6019219493).
func TestPipelineRetryRejectedMultiDoesNotRetryTxBody(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.tryagainConn = 1
	server.failMulti.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	ping := pipe.Ping(ctx)
	mget := pipe.MGet(ctx, "a", "b")
	multi := pipe.Do(ctx, "multi")
	incr := pipe.Incr(ctx, "counter")
	execCmd := pipe.Do(ctx, "exec")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("Exec: %v, want NOPERM (the rejected MULTI is the batch's first error)", err)
	}
	if err := ping.Err(); err != nil {
		t.Fatalf("PING: %v", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if err := multi.Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("MULTI: %v, want NOPERM", err)
	}
	if got, err := incr.Result(); err != nil || got != int64(1) {
		t.Fatalf("INCR = %v, %v; want 1, nil", got, err)
	}
	if err := execCmd.Err(); err == nil || !strings.Contains(err.Error(), "EXEC without MULTI") {
		t.Fatalf("EXEC: %v, want ERR EXEC without MULTI", err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the INCR after a rejected MULTI ran standalone and must not run again)", got)
	}
}

// TestPipelineRetryRejectedMultiDoesNotForceWholeBatch: the same rejected
// opener, with the retry boundary after the frame — MULTI (NOPERM),
// INCR (1), EXEC (ERR EXEC without MULTI), then GET, whose reply is
// lost when the connection drops. The prefix scan must not treat the
// rejected MULTI (or its rejected EXEC) as connection state that forces
// the whole batch: the retry runs only the GET, and the answered INCR
// is not executed twice. A TRYAGAIN boundary would not reach this scan
// — the batch error would be the MULTI's NOPERM, which is not
// retryable — so the boundary here is the transport failure.
// Regression test for the Codex P1 finding on #4351 (review 6019219493).
func TestPipelineRetryRejectedMultiDoesNotForceWholeBatch(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 3
	server.failMulti.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	incr := pipe.Incr(ctx, "counter")
	execCmd := pipe.Do(ctx, "exec")
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("Exec: %v, want NOPERM (the rejected MULTI is the batch's first error)", err)
	}
	if err := multi.Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("MULTI: %v, want NOPERM", err)
	}
	if got, err := incr.Result(); err != nil || got != int64(1) {
		t.Fatalf("INCR = %v, %v; want 1, nil", got, err)
	}
	if err := execCmd.Err(); err == nil || !strings.Contains(err.Error(), "EXEC without MULTI") {
		t.Fatalf("EXEC: %v, want ERR EXEC without MULTI", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %v, %v; want v, nil", got, err)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the first connection must drop before GET's reply)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not run again on a whole-batch retry)", got)
	}
	if got := server.multiCount.Load(); got != 1 {
		t.Fatalf("server received %d MULTI commands, want 1 (the rejected MULTI must not force the batch back to it)", got)
	}
	if got := server.execWithoutMulti.Load(); got != 1 {
		t.Fatalf("server received %d EXEC-without-MULTI commands, want 1", got)
	}
}

// TestPipelineRetryAbortedTxBodyDoesNotSeedRetry: a transaction the server
// aborted never executed its body, so a retryable refusal on a queued
// write (READONLY on a read-only replica) is the frame's outcome, not a
// boundary the retry may rewind to: retrying just the INCR standalone
// would apply a write from a definitively aborted transaction, and the
// EXEC would keep its EXECABORT. The scan skips the aborted body, falls
// through to the whole batch, and the transaction re-runs as a unit on
// the promoted node — so the INCR executes exactly once. Regression test
// for the Codex P1 finding on #4351.
func TestPipelineRetryAbortedTxBodyDoesNotSeedRetry(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.failIncrReadonlyOnce.Store(true)
	server.failExecAbortOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	incr := pipe.Incr(ctx, "counter")
	execCmd := pipe.Do(ctx, "exec")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the aborted transaction must re-run as a unit on the promoted node)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != int64(1) {
		t.Fatalf("INCR = %v, %v; want 1, nil", got, err)
	}
	if err := execCmd.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server executed %d INCR commands, want 1 (the READONLY INCR never executed; the retried transaction runs it once)", got)
	}
	if got := server.execs.Load(); got != 2 {
		t.Fatalf("server received %d EXEC commands, want 2 (the aborted EXEC must not settle the batch; the retried EXEC runs the transaction)", got)
	}
	if got := server.multiCount.Load(); got != 2 {
		t.Fatalf("server received %d MULTI commands, want 2 (the transaction re-runs as a unit)", got)
	}
}

// TestPipelineRetryAbortedTxBodyIsNotRetriedStandalone: a SET refused
// READONLY ahead of a MULTI/EXEC frame whose INCR the replica also
// refuses READONLY (so EXEC answers EXECABORT) must not promote the
// aborted INCR into a standalone retry: the retry is only the SET —
// the INCR never executed, and re-running it alone would apply a write
// from a definitively aborted transaction while the original EXEC still
// reports its abort. Regression test for the Codex P1 finding on #4351.
func TestPipelineRetryAbortedTxBodyIsNotRetriedStandalone(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.setReadonlyOnce.Store(true)
	server.failIncrReadonlyOnce.Store(true)
	server.failExecAbortOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "a", "v", 0)
	multi := pipe.Do(ctx, "multi")
	incr := pipe.Incr(ctx, "counter")
	execCmd := pipe.Do(ctx, "exec")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "READONLY") {
		t.Fatalf("Exec: %v, want READONLY (the first failed command wins; the aborted transaction's INCR keeps its refusal)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := incr.Err(); err == nil || !strings.Contains(err.Error(), "READONLY") {
		t.Fatalf("INCR: %v, want READONLY (the queued write was refused, not executed)", err)
	}
	if err := execCmd.Err(); err == nil || !strings.Contains(err.Error(), "EXECABORT") {
		t.Fatalf("EXEC: %v, want EXECABORT (the frame's outcome is preserved)", err)
	}
	if got := server.incrs.Load(); got != 0 {
		t.Fatalf("server executed %d INCR commands, want 0 (the aborted transaction's body must not run standalone on retry)", got)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the refused SET plus its retry)", got)
	}
	if got := server.setsDB0.Load(); got != 1 {
		t.Fatalf("server executed %d SETs in DB 0, want 1 (the retried SET runs once)", got)
	}
	if got := server.execs.Load(); got != 1 {
		t.Fatalf("server received %d EXEC commands, want 1 (the aborted EXEC must not run again)", got)
	}
}

// TestPipelineRetryKeepsWatch: a WATCH issued in a regular pipeline must
// not be dropped by the suffix retry, or the retried write runs unwatched
// and loses its optimistic-concurrency guard.
func TestPipelineRetryKeepsWatch(t *testing.T) {
	server := newConnStateRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.setWhileWatched.Load(); got != 1 {
		t.Fatalf("SET ran %d time(s) while watched, want 1 (the answered WATCH must not be dropped by the retry)", got)
	}
}

// TestPipelineRetryDirtyWatchAbortsExec: a WATCH followed by an answered
// write that dirtied it must keep the whole batch on retry. Reissuing
// the WATCH alone on a fresh connection would give it a clean watch, so
// the retried EXEC would commit a transaction the original ordering
// would have aborted (the client's own INCR invalidates the watch).
// Regression test for the review finding on #4351.
func TestPipelineRetryDirtyWatchAbortsExec(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	incr := pipe.Incr(ctx, "k")
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "other", "v", 0)
	exec := pipe.Do(ctx, "exec")
	// The aborted EXEC surfaces as redis.Nil, exactly as the same
	// batch would without any disconnect: the transaction aborted
	// because the client's own INCR invalidated the watch.
	if _, err := pipe.Exec(ctx); !errors.Is(err, Nil) {
		t.Fatalf("Exec: %v, want redis.Nil (the retried EXEC must abort, like the same batch without a disconnect)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := exec.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EXEC: %v, want redis.Nil (the aborted EXEC reply)", err)
	}
	// The retried transaction must abort: the answered INCR dirtied the
	// watch, so a clean reissued WATCH would silently commit the SET.
	if got := server.execAborted.Load(); got != 1 {
		t.Fatalf("EXEC aborted %d time(s), want 1 (the retried transaction must abort, not silently commit)", got)
	}
	if got := server.execWithMulti.Load(); got != 0 {
		t.Fatalf("EXEC committed %d time(s), want 0 (a reissued clean WATCH must not let the transaction commit)", got)
	}
	// Whole-batch retry re-runs the answered INCR (the pre-existing
	// whole-batch cost): it ran once on the first connection and once
	// on the retry, which is what dirties the retried watch.
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the answered INCR must be re-run with the whole batch)", got)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil (the retried INCR overwrites the first attempt's reply)", got, err)
	}
}

// syntheticPrefixRetryServer is a scripted RESP server for the synthetic
// prefix retry test. The first connection answers INCR and SELECT and
// then drops before SET's reply; the next connection answers the
// synthetic SELECT and rejects the retried SET with READONLY (failover
// in progress); later connections answer everything, so a correct retry
// eventually applies the SET.
type syntheticPrefixRetryServer struct {
	ln           net.Listener
	conns        atomic.Int32
	setsReceived atomic.Int32
	readonlySets atomic.Int32
	setsApplied  atomic.Int32
	setDB        atomic.Int32 // DB in effect when SET last ran, -1 = never
}

func newSyntheticPrefixRetryServer(t *testing.T) *syntheticPrefixRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &syntheticPrefixRetryServer{ln: ln}
	s.setDB.Store(-1)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *syntheticPrefixRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	db := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			if len(args) > 0 {
				if n, err := strconv.Atoi(args[0]); err == nil {
					db = n
				}
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "incr":
			_, _ = conn.Write([]byte(":1\r\n"))
		case "set":
			s.setsReceived.Add(1)
			s.setDB.Store(int32(db))
			if first {
				// Drop before replying: the SET must be retried
				// with a synthetic SELECT ahead of it.
				return
			}
			if s.readonlySets.Load() == 0 {
				s.readonlySets.Add(1)
				_, _ = conn.Write([]byte("-READONLY You can't write against a read only replica.\r\n"))
				continue
			}
			s.setsApplied.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryClassifiesOriginalErrorAfterSyntheticPrefix: when a
// retry opens with synthetic commands that succeed, a retryable error on
// a later original command must still be classified for retry. Here the
// retried SET answers READONLY during failover behind a synthetic SELECT
// that succeeded: without the fix the attempt reports success, the outer
// loop exits with retries left, and Exec returns the READONLY too late
// to retry it. Regression test for the review finding on #4351.
func TestPipelineRetryClassifiesOriginalErrorAfterSyntheticPrefix(t *testing.T) {
	server := newSyntheticPrefixRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	incr := pipe.Incr(ctx, "counter")
	sel := pipe.Select(ctx, 2)
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the READONLY reply on the retried SET must be retried, not reported as success)", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (it must be retried after the failover, not left with the READONLY error)", err)
	}
	if got := server.setDB.Load(); got != 2 {
		t.Fatalf("SET ran on DB %d, want 2 (the retried SET must run on the re-established database)", got)
	}
	if got := server.readonlySets.Load(); got != 1 {
		t.Fatalf("server returned READONLY %d time(s), want 1", got)
	}
	if got := server.setsApplied.Load(); got != 1 {
		t.Fatalf("server applied %d SET commands, want 1 (the SET rejected READONLY must be applied by a later retry)", got)
	}
	if got := server.setsReceived.Load(); got != 3 {
		t.Fatalf("server received %d SET commands, want 3 (dropped once, READONLY once, applied once)", got)
	}
}

// syntheticPrefixTrimServer is a scripted RESP server for the
// synthetic-prefix trim test. The first connection answers SELECT and
// then drops before INCR's reply; the second answers the synthetic
// SELECT and the INCR, then rejects the SET with READONLY (failover in
// progress); later connections answer everything. It counts INCRs the
// server actually answered, so a retry that re-runs an answered INCR
// shows up as a second increment.
type syntheticPrefixTrimServer struct {
	ln            net.Listener
	conns         atomic.Int32
	incrsAnswered atomic.Int32
	setsReceived  atomic.Int32
	readonlySets  atomic.Int32
	setsApplied   atomic.Int32
	setDB         atomic.Int32 // DB in effect when SET last ran, -1 = never
}

func newSyntheticPrefixTrimServer(t *testing.T) *syntheticPrefixTrimServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &syntheticPrefixTrimServer{ln: ln}
	s.setDB.Store(-1)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *syntheticPrefixTrimServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	db := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			if len(args) > 0 {
				if v, err := strconv.Atoi(args[0]); err == nil {
					db = v
				}
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "incr":
			if n == 1 {
				// Drop before replying: the INCR must be
				// retried with a synthetic SELECT ahead of it.
				return
			}
			s.incrsAnswered.Add(1)
			_, _ = conn.Write([]byte(":1\r\n"))
		case "set":
			s.setsReceived.Add(1)
			s.setDB.Store(int32(db))
			if n == 2 && s.readonlySets.Load() == 0 {
				s.readonlySets.Add(1)
				_, _ = conn.Write([]byte("-READONLY You can't write against a read only replica.\r\n"))
				continue
			}
			s.setsApplied.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryTrimsAnsweredWritesAfterSyntheticPrefixError: when a
// retry opens with a synthetic SELECT that succeeds, a retryable error
// on a later original command must narrow the next retry to that
// command, not return the whole batch. Here the retried SET answers
// READONLY behind a synthetic SELECT: the whole-batch return keeps the
// already-answered INCR in pending, so the next healthy attempt
// increments the counter a second time and Exec still reports success.
// Regression test for the review finding on #4351.
func TestPipelineRetryTrimsAnsweredWritesAfterSyntheticPrefixError(t *testing.T) {
	server := newSyntheticPrefixTrimServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	incr := pipe.Incr(ctx, "counter")
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the READONLY reply on the retried SET must be retried, not reported as success)", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (it must be retried after the failover, not left with the READONLY error)", err)
	}
	if got := server.setDB.Load(); got != 2 {
		t.Fatalf("SET ran on DB %d, want 2 (the retried SET must run on the re-established database)", got)
	}
	if got := server.readonlySets.Load(); got != 1 {
		t.Fatalf("server returned READONLY %d time(s), want 1", got)
	}
	if got := server.incrsAnswered.Load(); got != 1 {
		t.Fatalf("server answered %d INCR commands, want 1 (the answered INCR must not be re-run by the retry after the READONLY SET)", got)
	}
	if got := server.setsApplied.Load(); got != 1 {
		t.Fatalf("server applied %d SET commands, want 1 (the SET rejected READONLY must be applied by a later retry)", got)
	}
}

// failedRestoreRetryServer is a scripted RESP server for the failed
// state-restoration test. The first connection answers SELECT and then
// drops before either SET's reply; the second connection rejects the
// synthetic SELECT (the DB does not exist there) and then answers the
// first SET in DB 0 before dropping before the second SET's reply; a
// third connection would accept everything. It records which DB each
// SET ran in, so a retry that keeps the DB-0 SET and only re-runs the
// second SET shows up as a success with the first SET missing from
// DB 2.
type failedRestoreRetryServer struct {
	ln           net.Listener
	conns        atomic.Int32
	setsA        atomic.Int32
	setsB        atomic.Int32
	setsAApplied atomic.Int32 // SET a commands answered +OK
	setsBApplied atomic.Int32 // SET b commands answered +OK
	setADB       atomic.Int32 // DB in effect when SET a last ran, -1 = never
	selectErrs   atomic.Int32
}

func newFailedRestoreRetryServer(t *testing.T) *failedRestoreRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &failedRestoreRetryServer{ln: ln}
	s.setADB.Store(-1)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *failedRestoreRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	db := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			if n == 2 {
				// The replacement server has no DB 2: the
				// required re-establishment fails, so the
				// SETs that follow run in DB 0.
				s.selectErrs.Add(1)
				_, _ = conn.Write([]byte("-ERR DB index is out of range\r\n"))
				continue
			}
			if len(args) > 0 {
				if v, err := strconv.Atoi(args[0]); err == nil {
					db = v
				}
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "set":
			if len(args) > 0 && args[0] == "a" {
				s.setsA.Add(1)
				s.setADB.Store(int32(db))
			} else {
				s.setsB.Add(1)
			}
			if n == 1 || (n == 2 && len(args) > 0 && args[0] == "b") {
				// Drop before replying: the SET must be retried.
				return
			}
			if len(args) > 0 && args[0] == "a" {
				s.setsAApplied.Add(1)
			} else {
				s.setsBApplied.Add(1)
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryKeepsFailedStateRestoration: when a retry's required
// synthetic SELECT fails, the writes that followed it ran under the
// wrong session state and must not be retained as the batch's outcome.
// Here the second connection rejects SELECT 2, SET a answers OK in
// DB 0, and the connection drops before SET b's reply: without the fix
// the transport return bypasses required-synthetic validation, the
// narrowing drops SET a and reissues a cleared SELECT, and a third
// connection runs only SET b in DB 2 while Exec reports success even
// though SET a never landed in DB 2. Regression test for the review
// finding on #4351.
func TestPipelineRetryKeepsFailedStateRestoration(t *testing.T) {
	server := newFailedRestoreRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	setA := pipe.Set(ctx, "a", "v", 0)
	setB := pipe.Set(ctx, "b", "v", 0)
	if _, err := pipe.Exec(ctx); err == nil {
		t.Fatalf("Exec: nil, want an error (the failed SELECT 2 restoration must not be erased by a retry that only re-runs SET b)")
	}
	// The original SELECT was answered OK on the first connection; the
	// failure belongs to the synthetic reissue, so it surfaces as the
	// Exec error, not on the original command.
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered OK on the first connection; the failed reissue is reported on Exec)", err)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (no third attempt may run after the restoration failure)", got)
	}
	if got := server.selectErrs.Load(); got != 1 {
		t.Fatalf("server rejected SELECT %d time(s), want 1", got)
	}
	if got := server.setsA.Load(); got != 2 {
		t.Fatalf("server received %d SET a commands, want 2 (dropped once on the first connection, answered once in the wrong DB)", got)
	}
	if got := server.setsAApplied.Load(); got != 1 {
		t.Fatalf("server applied %d SET a commands, want 1 (only the DB-0 run was answered; it must not be re-run in DB 2 as a silent success)", got)
	}
	if got := server.setADB.Load(); got != 0 {
		t.Fatalf("SET a ran on DB %d, want 0 (it only ever ran under the failed SELECT, never in DB 2)", got)
	}
	if got := server.setsB.Load(); got != 1 {
		t.Fatalf("server received %d SET b commands, want 1 (it must not be re-run on a third connection as if the batch had succeeded)", got)
	}
	if got := server.setsBApplied.Load(); got != 0 {
		t.Fatalf("server applied %d SET b commands, want 0 (its reply was never read)", got)
	}
	if err := setA.Err(); err != nil {
		t.Fatalf("SET a: %v, want nil (it was answered OK in DB 0; the failure is reported on Exec, not by rewriting its reply)", err)
	}
	if err := setB.Err(); err == nil {
		t.Fatalf("SET b: nil, want an error (its reply was never read)")
	}
}

// failedBoundaryRestoreRetryServer is a scripted RESP server for the
// failed-boundary state-restoration test. Like failedRestoreRetryServer
// it drops the first connection before SET a's reply, but the second
// connection rejects the synthetic SELECT with a retryable -LOADING error
// and then answers both SETs in DB 0, while a third connection accepts
// everything: the failed synthetic is the retry boundary itself, so a
// guard that only walks the commands before the boundary never sees it.
type failedBoundaryRestoreRetryServer struct {
	ln           net.Listener
	conns        atomic.Int32
	setsA        atomic.Int32
	setsB        atomic.Int32
	setsAApplied atomic.Int32 // SET a commands answered +OK
	setsBApplied atomic.Int32 // SET b commands answered +OK
	setADB       atomic.Int32 // DB in effect when SET a last ran, -1 = never
	setBDB       atomic.Int32 // DB in effect when SET b last ran, -1 = never
	selectErrs   atomic.Int32
}

func newFailedBoundaryRestoreRetryServer(t *testing.T) *failedBoundaryRestoreRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &failedBoundaryRestoreRetryServer{ln: ln}
	s.setADB.Store(-1)
	s.setBDB.Store(-1)
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *failedBoundaryRestoreRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	db := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			if n == 2 {
				// The replacement server is still loading: the
				// required re-establishment fails, so the SETs
				// that follow run in DB 0.
				s.selectErrs.Add(1)
				_, _ = conn.Write([]byte("-LOADING Server is loading the dataset in memory\r\n"))
				continue
			}
			if len(args) > 0 {
				if v, err := strconv.Atoi(args[0]); err == nil {
					db = v
				}
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "set":
			if len(args) > 0 && args[0] == "a" {
				s.setsA.Add(1)
				s.setADB.Store(int32(db))
			} else {
				s.setsB.Add(1)
				s.setBDB.Store(int32(db))
			}
			if n == 1 {
				// Drop before replying: the SET must be retried.
				return
			}
			if len(args) > 0 && args[0] == "a" {
				s.setsAApplied.Add(1)
			} else {
				s.setsBApplied.Add(1)
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
			if n == 2 && len(args) > 0 && args[0] == "b" {
				// The batch is fully answered: close, so the
				// next attempt dials a fresh connection instead
				// of reusing this one — a healthy pooled
				// connection would answer the reissued SELECT
				// with LOADING again until retries exhaust, and
				// the test could never reach a third connection.
				return
			}
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryKeepsFailedBoundaryStateRestoration: when the failed
// required synthetic is the retry boundary itself, the failed-restoration
// guard must still see it. Here the second connection rejects the
// synthetic SELECT 2 with LOADING and then answers both SETs in DB 0:
// without the fix the guard walks only the commands before the boundary,
// the answered SETs are dropped as settled, and Exec reports success even
// though both writes landed in DB 0; with it the whole batch is retried
// on a third connection and both SETs land in DB 2. Regression test for
// the Cursor Bugbot finding on #4351.
func TestPipelineRetryKeepsFailedBoundaryStateRestoration(t *testing.T) {
	server := newFailedBoundaryRestoreRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	setA := pipe.Set(ctx, "a", "v", 0)
	setB := pipe.Set(ctx, "b", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the whole batch must be retried on a third connection, where the restoration succeeds)", err)
	}
	// The original SELECT was answered OK on the first connection; the
	// failure belongs to the synthetic reissue, so it surfaces nowhere
	// on the original commands once the retry re-establishes it.
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered OK on the first connection)", err)
	}
	if err := setA.Err(); err != nil {
		t.Fatalf("SET a: %v, want nil", err)
	}
	if err := setB.Err(); err != nil {
		t.Fatalf("SET b: %v, want nil", err)
	}
	if got := server.conns.Load(); got != 3 {
		t.Fatalf("server saw %d connections, want 3 (the failed restoration must force a third attempt)", got)
	}
	if got := server.selectErrs.Load(); got != 1 {
		t.Fatalf("server rejected SELECT %d time(s), want 1", got)
	}
	if got := server.setsA.Load(); got != 3 {
		t.Fatalf("server received %d SET a commands, want 3 (dropped once on the first connection, answered once in the wrong DB, re-run in DB 2)", got)
	}
	if got := server.setsB.Load(); got != 2 {
		t.Fatalf("server received %d SET b commands, want 2 (answered once in the wrong DB, re-run in DB 2)", got)
	}
	if got := server.setADB.Load(); got != 2 {
		t.Fatalf("SET a ran on DB %d, want 2 (its last run must be under the restored SELECT)", got)
	}
	if got := server.setBDB.Load(); got != 2 {
		t.Fatalf("SET b ran on DB %d, want 2 (its last run must be under the restored SELECT)", got)
	}
}

func TestPipelineRetryDoesNotReplayAnsweredCommands(t *testing.T) {
	server := newPartialRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	incr := pipe.Incr(ctx, "counter")
	set := pipe.Set(ctx, "other", "v", 0)
	get := pipe.Get(ctx, "counter")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "1" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "1")
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be replayed)", got)
	}
}

// waitRetryServer is a scripted RESP server for WAIT pipeline retry tests.
// The first connection answers SET and then drops before WAIT's reply;
// later connections answer everything. It counts WAITs that arrive on a
// connection which never ran a SET, i.e. a barrier retried without the
// write whose replication it is supposed to confirm.
type waitRetryServer struct {
	ln             net.Listener
	conns          atomic.Int32
	sets           atomic.Int32
	waits          atomic.Int32
	waitWithoutSet atomic.Int32
}

func newWaitRetryServer(t *testing.T) *waitRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &waitRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *waitRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	seenSet := false
	answered := false
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
			continue
		case "set":
			s.sets.Add(1)
			seenSet = true
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "wait":
			s.waits.Add(1)
			if !seenSet {
				s.waitWithoutSet.Add(1)
			}
			if first && answered {
				// Drop the connection before replying: the WAIT
				// must be retried together with its SET.
				return
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
		answered = true
	}
}

// TestPipelineRetryKeepsWaitWithWrite: WAIT reads the replication offset of
// the connection that runs it (the server's per-connection woff), so a WAIT
// retried alone on a fresh connection would report success at once while
// the answered SET is still unreplicated. Regression test for the review
// finding on #4351 (SET + WAIT trimmed to WAIT-only on retry).
func TestPipelineRetryKeepsWaitWithWrite(t *testing.T) {
	server := newWaitRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	// Pipeliner has no Wait helper; issue the barrier as a raw command.
	wait := pipe.Do(ctx, "wait", 1, 1000)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := wait.Result(); err != nil || got != int64(1) {
		t.Fatalf("WAIT = %v, %v; want 1, nil", got, err)
	}
	if got := server.waitWithoutSet.Load(); got != 0 {
		t.Fatalf("WAIT ran %d time(s) on a connection that never ran the SET it follows", got)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the answered SET must be re-run with its WAIT)", got)
	}
}

// TestPipelineRetryKeepsBarrierErrorWhenNoRetryRuns: with retries
// disabled, MSET answers TRYAGAIN and WAIT answers NOPERM. The retry
// builder still runs after that last attempt, and it must not clear the
// barrier's error while merely constructing a retry that will never be
// dispatched: WAIT keeps its NOPERM instead of showing nil for a
// rejected command, while Exec still returns the MSET error (the
// batch's first error). Regression test for the Codex P2 finding on
// #4351 (review 6019219493).
func TestPipelineRetryKeepsBarrierErrorWhenNoRetryRuns(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.msetTryagain.Store(true)
	server.failWait.Store(true)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1, // truly disabled: 0 would be normalised to 3
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	pipe := client.Pipeline()
	mset := pipe.Do(ctx, "mset", "a", 1, "b", 2)
	wait := pipe.Do(ctx, "wait", 1, 1000)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "TRYAGAIN") {
		t.Fatalf("Exec: %v, want TRYAGAIN (the MSET error is the batch's first error)", err)
	}
	if err := mset.Err(); err == nil || !strings.Contains(err.Error(), "TRYAGAIN") {
		t.Fatalf("MSET: %v, want TRYAGAIN", err)
	}
	if err := wait.Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("WAIT: %v, want NOPERM (the rejected barrier keeps its error when no retry runs)", err)
	}
}

// himportSetRecord records one HIMPORT SET received by the scripted server:
// the connection number and the fields of the last HIMPORT PREPARE for that
// fieldset on the same connection.
type himportSetRecord struct {
	conn   int32
	fields []string
}

// himportRetryServer is a scripted RESP server for HIMPORT pipeline retry
// tests. The first connection answers the user-issued HIMPORT PREPARE and
// then drops before the SET's reply; later connections answer everything.
type himportRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	mu    sync.Mutex
	sets  []himportSetRecord
	incrs atomic.Int32
	// dropNextAfterSet, when set, answers the first connection's HIMPORT
	// SET and then drops before the next command's reply, so the command
	// after the SET is the retry boundary instead of the SET itself.
	dropNextAfterSet bool
	// dropNextAfterDiscard, when set, answers the first connection's
	// HIMPORT DISCARD and then drops before the next command's reply,
	// so the command after the DISCARD is the retry boundary instead
	// of the DISCARD itself.
	dropNextAfterDiscard bool
	// dropNextAfterPrepare, when set, answers the first connection's
	// HIMPORT PREPARE and then drops before the next command's reply,
	// so the command after the PREPARE is the retry boundary instead
	// of the PREPARE itself.
	dropNextAfterPrepare bool
	discards             atomic.Int32 // HIMPORT DISCARD commands seen, all connections
}

func newHImportRetryServer(t *testing.T) *himportRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &himportRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *himportRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	prepared := map[string][]string{}
	// cleared holds fieldsets whose prepared entry a RESET removed
	// from this connection's server-side session: the client still
	// believes they are prepared, so a SET for one answers "no such
	// fieldset" until a PREPARE restores it. A fieldset never
	// prepared on this connection is not in here and keeps the
	// server's lenient +OK, as before.
	cleared := map[string]bool{}
	// inMulti tracks a MULTI/EXEC transaction on this connection, and
	// pending the PREPAREs queued inside it: like a real server, a
	// queued PREPARE takes effect only when EXEC runs it.
	inMulti := false
	var pending map[string][]string
	queued := 0
	// seenSet records whether this connection already answered an
	// HIMPORT SET: with dropNextAfterSet, the next command after it on
	// the first connection gets no reply.
	seenSet := false
	// seenDiscard records whether this connection already answered an
	// HIMPORT DISCARD: with dropNextAfterDiscard, the next command
	// after it on the first connection gets no reply.
	seenDiscard := false
	// seenPrepare records whether this connection already answered an
	// HIMPORT PREPARE: with dropNextAfterPrepare, the next command
	// after it on the first connection gets no reply.
	seenPrepare := false
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		if n == 1 && s.dropNextAfterSet && seenSet {
			// Drop before replying: the command after the answered
			// SET gets no reply, so it is the retry boundary.
			return
		}
		if n == 1 && s.dropNextAfterDiscard && seenDiscard {
			// Drop before replying: the command after the answered
			// DISCARD gets no reply, so it is the retry boundary.
			return
		}
		if n == 1 && s.dropNextAfterPrepare && seenPrepare {
			// Drop before replying: the command after the answered
			// PREPARE gets no reply, so it is the retry boundary.
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "incr":
			s.incrs.Add(1)
			_, _ = conn.Write([]byte(":1\r\n"))
		case "multi":
			inMulti = true
			pending = map[string][]string{}
			queued = 0
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "exec":
			if !inMulti {
				_, _ = conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}
			inMulti = false
			for fs, fields := range pending {
				prepared[fs] = append([]string(nil), fields...)
				delete(cleared, fs)
			}
			pending = nil
			var b strings.Builder
			fmt.Fprintf(&b, "*%d\r\n", queued)
			for i := 0; i < queued; i++ {
				b.WriteString("+OK\r\n")
			}
			queued = 0
			_, _ = conn.Write([]byte(b.String()))
		case "reset":
			// RESET clears the server-side session, including every
			// prepared fieldset, while the client's prepared flags
			// still claim they are there: a later SET for one
			// answers "no such fieldset" until a PREPARE restores
			// it (see the set case below).
			for name := range prepared {
				cleared[name] = true
			}
			prepared = map[string][]string{}
			_, _ = conn.Write([]byte("+RESET\r\n"))
		case "discard":
			inMulti = false
			pending = nil
			queued = 0
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "himport":
			// readConnStateRetryCommand strips the command name, so
			// args[0] is the subcommand (prepare/set), args[1] the
			// fieldset for PREPARE and the key for SET.
			if len(args) < 1 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments\r\n"))
				continue
			}
			switch strings.ToLower(args[0]) {
			case "prepare":
				seenPrepare = true
				if len(args) >= 2 {
					fields := append([]string(nil), args[2:]...)
					if inMulti {
						pending[args[1]] = fields
						queued++
						_, _ = conn.Write([]byte("+QUEUED\r\n"))
					} else {
						prepared[args[1]] = fields
						delete(cleared, args[1])
						_, _ = conn.Write([]byte("+OK\r\n"))
					}
				} else {
					_, _ = conn.Write([]byte("+OK\r\n"))
				}
			case "set":
				var fields []string
				if len(args) >= 3 {
					fields = append([]string(nil), prepared[args[2]]...)
				}
				s.mu.Lock()
				s.sets = append(s.sets, himportSetRecord{conn: n, fields: fields})
				s.mu.Unlock()
				if n == 1 && !s.dropNextAfterSet {
					// Drop before replying: the SET must be retried
					// together with its PREPARE.
					return
				}
				seenSet = true
				if len(args) >= 3 && cleared[args[2]] {
					// The server-side session lost this fieldset (a
					// RESET cleared it) while the client still
					// believes it is prepared: the SET answers "no
					// such fieldset" until a PREPARE restores it.
					_, _ = conn.Write([]byte("-ERR no such fieldset\r\n"))
				} else {
					_, _ = conn.Write([]byte("+OK\r\n"))
				}
			case "discard", "discardall":
				// A real server answers an integer count, which the
				// IntCmd reply decoder expects; +OK would fail to
				// parse as an integer and the DISCARD would carry
				// an error instead of an acknowledgement.
				s.discards.Add(1)
				seenDiscard = true
				_, _ = conn.Write([]byte(":1\r\n"))
			default:
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryKeepsHImportPrepare: a pipeline HIMPORT PREPARE followed
// by HIMPORT SET must not be trimmed to the SET on retry. The client-side
// registry still holds the old fields (himportAfterBatch never ran on the
// dropped connection), so a trimmed retry would inject a PREPARE with the
// stale fields and write under the wrong field names. Regression test for
// the review finding on #4351.
func TestPipelineRetryKeepsHImportPrepare(t *testing.T) {
	server := newHImportRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	// Seed the client-side registry as if fs had been registered with
	// field "old" by an earlier HImportPrepare.
	client.himport.register("fs", []string{"old"})

	pipe := client.Pipeline()
	prep := pipe.HImportPrepare(ctx, "fs", "new")
	set := pipe.HImportSet(ctx, "k", "fs", "v")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := prep.Err(); err != nil {
		t.Fatalf("PREPARE: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	var retry []himportSetRecord
	for _, rec := range server.sets {
		if rec.conn == 2 {
			retry = append(retry, rec)
		}
	}
	if len(retry) != 1 {
		t.Fatalf("server received %d SET commands on the retry connection, want 1", len(retry))
	}
	if got := retry[0].fields; len(got) != 1 || got[0] != "new" {
		t.Fatalf("SET on the retry connection ran under PREPARE fields %v, want [new] (the answered PREPARE must not be trimmed)", got)
	}
}

// TestPipelineRetryHImportSetIsOrdinaryCommand: an answered HIMPORT SET is
// an ordinary write, not connection state: only HIMPORT PREPARE
// establishes state on the connection. A SET answered before the
// boundary must neither force the whole batch on retry (the prefix
// path) nor be kept and re-sent from the answered tail (the tail path):
// re-running it would apply the write twice while Exec reports success.
// Regression test for the review finding on #4351 (HIMPORT SET treated
// as session state).
func TestPipelineRetryHImportSetIsOrdinaryCommand(t *testing.T) {
	server := newHImportRetryServer(t)
	// Answer the HIMPORT SET on the first connection, then drop before
	// the INCR after it: the SET is answered, the INCR is the boundary.
	server.dropNextAfterSet = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	incr := pipe.Incr(ctx, "k")
	set := pipe.HImportSet(ctx, "k", "fs", "v")
	incr2 := pipe.Incr(ctx, "k2")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the answered HIMPORT SET must not force the whole batch)", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("HIMPORT SET: %v", err)
	}
	if got, err := incr2.Result(); err != nil || got != 1 {
		t.Fatalf("INCR k2 = %d, %v; want 1, nil", got, err)
	}
	// Each write ran once: the answered INCR and the answered HIMPORT
	// SET were not re-run with the retry.
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the answered INCR must not be re-run)", got)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if got := len(server.sets); got != 1 {
		t.Fatalf("server received %d HIMPORT SET commands, want 1 (the answered SET must not be re-sent)", got)
	}
}

// TestPipelineRetryQueuedHImportPrepareDoesNotRegister: a PREPARE queued
// inside a MULTI frame executes only when the frame's EXEC commits it, so
// a +QUEUED reply is not an execution: when the frame is DISCARD dropped
// after the PREPARE queued, the fieldset was never prepared server-side
// and the client's registry must not record the new fields. Without the
// guard, a later HImportSet injects a PREPARE with the wrong fields and
// writes values under the wrong field names. Regression test for the
// Codex P2 finding on #4351.
func TestPipelineRetryQueuedHImportPrepareDoesNotRegister(t *testing.T) {
	server := newHImportRetryServer(t)
	// Answer the PREPARE's +QUEUED on the first connection, then drop
	// before the DISCARD after it: the DISCARD is the boundary.
	server.dropNextAfterPrepare = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	// Seed the client-side registry as if fs had been registered with
	// field "old" by an earlier HImportPrepare.
	client.himport.register("fs", []string{"old"})

	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	prep := pipe.HImportPrepare(ctx, "fs", "new")
	discard := pipe.Do(ctx, "discard")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the DISCARD drops the queued PREPARE; the retry runs it on a fresh connection)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := prep.Err(); err != nil {
		t.Fatalf("PREPARE: %v", err)
	}
	if err := discard.Err(); err != nil {
		t.Fatalf("DISCARD: %v", err)
	}

	// A later SET for the same fieldset must inject a PREPARE with the
	// old fields: the queued PREPARE registered nothing, so the
	// registry still holds "old".
	pipe2 := client.Pipeline()
	set := pipe2.HImportSet(ctx, "k2", "fs", "v2")
	if _, err := pipe2.Exec(ctx); err != nil {
		t.Fatalf("second Exec: %v, want nil", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("HIMPORT SET: %v", err)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	if got := len(server.sets); got != 1 {
		t.Fatalf("server received %d HIMPORT SET commands, want 1", got)
	}
	if got := server.sets[0].fields; len(got) != 1 || got[0] != "old" {
		t.Fatalf("SET ran under PREPARE fields %v, want [old] (the queued PREPARE in the discarded frame must not register the new fields)", got)
	}
}

// staleErrRetryServer is a scripted RESP server for stale-error pipeline
// retry tests. The first connection answers the first batch (SET with a
// LOADING error, GET with a value) and then stops reading forever, so the
// retry's read stalls until the read timeout; later connections answer
// everything.
type staleErrRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	sets  atomic.Int32
	done  chan struct{}
}

func newStaleErrRetryServer(t *testing.T) *staleErrRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &staleErrRetryServer{ln: ln, done: make(chan struct{})}
	t.Cleanup(func() { close(s.done) })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

// readStaleErrRESPCommand reads one RESP array command from rd and returns its
// lower-cased name. Unlike the bufio.Scanner helpers above it reads bulk
// strings by their declared length, so a multi-megabyte argument does not
// depend on scanner token buffering.
func readStaleErrRESPCommand(rd *bufio.Reader) (string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return "", err
	}
	line = strings.TrimRight(line, "\r\n")
	if !strings.HasPrefix(line, "*") {
		return "", io.EOF
	}
	n, err := strconv.Atoi(strings.TrimPrefix(line, "*"))
	if err != nil || n <= 0 {
		return "", io.EOF
	}
	var args []string
	for i := 0; i < n; i++ {
		hline, err := rd.ReadString('\n')
		if err != nil {
			return "", err
		}
		hline = strings.TrimRight(hline, "\r\n")
		if !strings.HasPrefix(hline, "$") {
			return "", io.EOF
		}
		size, err := strconv.Atoi(strings.TrimPrefix(hline, "$"))
		if err != nil || size < 0 {
			return "", io.EOF
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(rd, buf); err != nil {
			return "", err
		}
		args = append(args, string(buf[:size]))
	}
	if len(args) == 0 {
		return "", io.EOF
	}
	return strings.ToLower(args[0]), nil
}

func (s *staleErrRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	answered := 0
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
			continue
		case "set":
			s.sets.Add(1)
			if first {
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "get":
			_, _ = conn.Write([]byte("$1\r\nv\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
		if first {
			answered++
			if answered >= 2 {
				// Stop reading: the retry's read must stall until
				// the read timeout instead of being answered by
				// this connection.
				<-s.done
				return
			}
		}
	}
}

// TestPipelineRetryDoesNotReportStaleError: when the first attempt reads a
// retryable LOADING error for SET and a successful GET, and the next
// attempt fails during its read, the retry must re-execute SET — not drop
// it as though the earlier LOADING had been this attempt's answer and
// report the stale error after the server has recovered. (LOADING, not
// READONLY, seeds the stale error: a READONLY batch error makes the pool
// drop the connection outright, see #790, so the read failure this test
// needs would never happen.) The stalled retry read is made deterministic
// with a small payload and a short ReadTimeout: the retry's write lands in
// the kernel buffers of a connection that stopped reading, and its read
// then stalls until the timeout — instead of a 32 MiB payload racing a
// 100ms WriteTimeout, whose outcome depends on runner speed (CI 8.4.x /
// 8.6.x). Regression test for the review finding on #4351.
func TestPipelineRetryDoesNotReportStaleError(t *testing.T) {
	server := newStaleErrRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())
	client.opt.ReadTimeout = 100 * time.Millisecond

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "x", 0)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v (a LOADING reply from the first attempt must not survive a retry that never re-executed it)", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the SET must be re-executed once the server has recovered)", got)
	}
}

// firstErrRetryServer is a scripted RESP server for first-failure
// aggregation tests. The first connection answers HGET with a WRONGTYPE
// error and then drops before INCR's reply; later connections answer INCR
// with a Redis error (retryErr=true) or a value.
type firstErrRetryServer struct {
	ln       net.Listener
	conns    atomic.Int32
	hgets    atomic.Int32
	incrs    atomic.Int32
	retryErr bool
}

func newFirstErrRetryServer(t *testing.T, retryErr bool) *firstErrRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &firstErrRetryServer{ln: ln, retryErr: retryErr}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *firstErrRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "hget":
			s.hgets.Add(1)
			if n == 1 {
				_, _ = conn.Write([]byte("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"))
			} else {
				_, _ = conn.Write([]byte("$1\r\nv\r\n"))
			}
		case "incr":
			s.incrs.Add(1)
			if n == 1 {
				// Drop before replying: INCR must be retried on its own.
				return
			}
			if s.retryErr {
				_, _ = conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
			} else {
				_, _ = conn.Write([]byte(":1\r\n"))
			}
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryReportsFirstErr: when the first command of a pipeline is
// answered with a Redis error and a later command is retried on its own and
// fails with a different Redis error, Exec must report the first failed
// command's error, per its documented contract — not the retried tail's
// error. Regression test for the review finding on #4351.
func TestPipelineRetryReportsFirstErr(t *testing.T) {
	server := newFirstErrRetryServer(t, true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	hget := pipe.HGet(ctx, "str", "f")
	incr := pipe.Incr(ctx, "text")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("Exec: %v, want the first failed command's WRONGTYPE error, not the retried INCR's error", err)
	}
	if err := hget.Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("HGET: %v, want WRONGTYPE", err)
	}
	if err := incr.Err(); err == nil || !strings.Contains(err.Error(), "ERR") {
		t.Fatalf("INCR: %v, want the retry's ERR", err)
	}
	if got := server.hgets.Load(); got != 1 {
		t.Fatalf("server received %d HGET commands, want 1 (the answered HGET must not be re-executed)", got)
	}
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the unanswered INCR retried once)", got)
	}
}

// TestPipelineRetryKeepsAnsweredResultsOnNonRetryableExit: when one attempt
// answers the first command and the connection then drops before the next
// reply, a non-retryable failure must not stamp the terminal error onto the
// command that already succeeded. The batch holds a NoRetry command (as a
// RawWriteToCmd would), so the early-exit branch runs instead of a retry:
// stamping the whole pending set would overwrite the answered SET's success
// with the transport error, so callers would observe failure for a command
// that already applied. Regression test for the review finding on #4351.
func TestPipelineRetryKeepsAnsweredResultsOnNonRetryableExit(t *testing.T) {
	server := newPartialRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "text")
	incr.setNoRetry(true) // stands in for a RawWriteToCmd in the batch
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the transport error for the unanswered INCR")
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (the answered SET must keep its success, not inherit the terminal error)", err)
	}
	if got, err := set.Result(); err != nil || got != "OK" {
		t.Fatalf("SET = %q, %v; want OK, nil", got, err)
	}
	if err := incr.Err(); err == nil {
		t.Fatalf("INCR: nil, want the transport error for the unanswered command")
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (no retry on a non-retryable exit)", got)
	}
}

// firstErrMetricRecorder captures the error passed to
// RecordPipelineOperationDuration.
type firstErrMetricRecorder struct {
	fdOtelRecorder
	calls   atomic.Int64
	lastErr atomic.Value
}

func (r *firstErrMetricRecorder) RecordPipelineOperationDuration(_ context.Context, _ time.Duration, _ string, _ int, _ int, err error, _ *pool.Conn, _ int) {
	r.calls.Add(1)
	if err != nil {
		r.lastErr.Store(err)
	}
}

// TestPipelineRetryRecordsFirstErrInMetrics: when a pipeline's first command
// keeps a Redis error and the retried tail then succeeds, Exec still
// reports the first error — but the pipeline duration metric must also
// record it as a failure, not as a success. Regression test for the
// review finding on #4351.
func TestPipelineRetryRecordsFirstErrInMetrics(t *testing.T) {
	server := newFirstErrRetryServer(t, false)
	client := newPartialRetryClient(t, server.ln.Addr().String())
	rec := &firstErrMetricRecorder{}
	otel.SetGlobalRecorder(rec)
	defer otel.SetGlobalRecorder(nil)

	ctx := context.Background()
	pipe := client.Pipeline()
	hget := pipe.HGet(ctx, "str", "f")
	incr := pipe.Incr(ctx, "text")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("Exec: %v, want WRONGTYPE (the first failed command)", err)
	}
	if err := hget.Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("HGET: %v, want WRONGTYPE", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %v, %v; want 1, nil", got, err)
	}
	if n := rec.calls.Load(); n != 1 {
		t.Fatalf("%d pipeline metrics for one operation, want 1", n)
	}
	e, _ := rec.lastErr.Load().(error)
	if e == nil || !strings.Contains(e.Error(), "WRONGTYPE") {
		t.Fatalf("metric error %v, want WRONGTYPE (a retained prefix error must not record as success)", e)
	}
}

// loadingRetryServer is a scripted RESP server for retryable-reply pipeline
// retry tests. The first connection answers SET with a LOADING error and then
// drops before INCR's reply; later connections answer everything.
type loadingRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	sets  atomic.Int32
}

func newLoadingRetryServer(t *testing.T) *loadingRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &loadingRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *loadingRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			s.sets.Add(1)
			if first {
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "incr":
			if first {
				// Drop before replying: the INCR must be retried
				// together with the SET the LOADING reply never executed.
				return
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryReexecutesLoadingCommand: when one command of a pipeline is
// answered with a retryable LOADING error and a later command then fails at
// the transport layer, the retry must re-execute the LOADING command — it
// never applied — not drop it as though the LOADING reply had been a
// definitive answer and keep reporting the stale error after the server has
// recovered. Regression test for the review finding on #4351.
func TestPipelineRetryReexecutesLoadingCommand(t *testing.T) {
	server := newLoadingRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the LOADING reply never executed the SET, so the retry must re-execute it)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %v, %v; want 1, nil", got, err)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the SET answered LOADING must be re-executed once the server has recovered)", got)
	}
}

// scriptRetryServer is a scripted RESP server for SCRIPT LOAD pipeline retry
// tests. Each connection holds its own (initially empty) script cache, like a
// fresh server instance after a restart: SCRIPT LOAD records the digest,
// EVALSHA answers only when the digest was loaded on that same connection.
// The first connection answers the LOAD and then drops before EVALSHA's reply;
// later connections answer everything.
//
// dropBefore names the command before whose reply the first connection
// drops ("evalsha" by default); denyLoad answers SCRIPT LOAD with NOPERM,
// modelling an ACL user without that permission; dropRetryConn closes a
// retry connection once its last command arrived, without replying to any
// of it, so the retry fails while re-reading its first command.
// queuedRuntimeErrScript is the Lua source used by
// TestPipelineRetryQueuedEvalRuntimeErrIsPreloaded: with a truthy ARGV[1]
// it returns 42, otherwise it fails with a Lua runtime error. The runtime
// error means the script ran (and cached) before failing, unlike a
// pre-execution rejection.
const queuedRuntimeErrScript = "if ARGV[1] then return 42 end; error('x')"

// queuedRuntimeErrScriptSHA is the SHA1 of queuedRuntimeErrScript, the
// digest EVALSHA carries for it.
var queuedRuntimeErrScriptSHA = func() string {
	sum := sha1.Sum([]byte(queuedRuntimeErrScript))
	return hex.EncodeToString(sum[:])
}()

// runtimeIntErrScript is the Lua source used by
// TestPipelineRetryEvalRuntimeErrorStillCachesScript: it pcall()s INCR on
// KEYS[1], so with a non-integer key the script fails at runtime with
// "value is not an integer or out of range" — after it ran and cached.
// The same text is what argument validation of a bad numkeys reports
// before execution, so the two cases differ only in the numkeys argument.
const runtimeIntErrScript = "return redis.pcall('INCR', KEYS[1])"

// runtimeMsetArityErrScript is the Lua source used by
// TestPipelineRetryEvalMsetArityRuntimeErrorIsPreloaded: it pcall()s MSET
// with KEYS[1], one value, and ARGV unpacked, so with a single ARGV value
// the call has an odd argument count and fails at runtime with "wrong
// number of arguments for 'mset' command" — after the script ran and
// cached. The same prefix is what a refusal of EVAL itself reports
// before execution, so the two cases differ only in EVAL's own arity.
const runtimeMsetArityErrScript = "return redis.pcall('MSET', KEYS[1], 'v', unpack(ARGV))"

// emptyScriptSHA is the SHA1 of the empty Lua source, the digest EVALSHA
// carries for an EVAL "".
var emptyScriptSHA = func() string {
	sum := sha1.Sum([]byte(""))
	return hex.EncodeToString(sum[:])
}()

// retryRedisIntValid reports whether s is an integer in the form Redis
// accepts on the wire: an optional leading '-' (but not '+'), digits
// only, no leading zero unless the value is exactly "0" (or "-0"). It
// mirrors the count guard in routing_metadata.go and the client-side
// evalNumKeysValid; the script server models a real server, so it must
// reject the forms strconv.Atoi accepts but Redis does not ("01", "+1").
// A negative count parses fine — it is an integer, just not a valid
// count — so "-1" is valid here: it must reach the negative-count
// branch below and be refused "wrong number of keys", not answered
// "value is not an integer".
func retryRedisIntValid(s string) bool {
	if strings.HasPrefix(s, "-") {
		s = s[1:]
	}
	if len(s) == 0 || s[0] < '0' || s[0] > '9' || len(s) > 1 && s[0] == '0' {
		return false
	}
	_, err := strconv.Atoi(s)
	return err == nil
}

type scriptRetryServer struct {
	ln             net.Listener
	conns          atomic.Int32
	loads          atomic.Int32
	flushes        atomic.Int32
	evals          atomic.Int32
	evalshas       atomic.Int32
	noscripts      atomic.Int32
	dropBefore     string
	denyLoad       bool
	denyFlushRetry bool // retry connections refuse SCRIPT FLUSH with NOPERM
	dropRetryConn  bool
	// tryagainMget, when set, makes the first connection answer its
	// first MGET with TRYAGAIN (slot migration); later MGETs succeed.
	tryagainMget bool
	// loadUser, when set, is the only user allowed to SCRIPT LOAD;
	// other users get NOPERM. Tests that need a LOAD to run under a
	// specific AUTH use it.
	loadUser string
	// failReset, when set, makes every RESET answer NOPERM: the
	// transaction stays open, exactly like a real server refusing
	// RESET for an ACL user.
	failReset atomic.Bool
	// attrEvalRejectionOnce, when set, decorates the next rejected
	// EVAL's error reply with a RESP3 attribute block, modelling an
	// endpoint that wraps raw replies in attributes: a classifier
	// that reads val[0] without skipping the block sees '|' instead
	// of the error.
	attrEvalRejectionOnce atomic.Bool
	// failDiscard, when set, makes every DISCARD answer NOPERM: the
	// transaction stays open, exactly like a real server refusing
	// DISCARD for an ACL user.
	failDiscard atomic.Bool
	// persistCache makes the script cache survive across connections
	// (a still-running server), instead of a fresh cache per
	// connection (a restarted server).
	persistCache bool
	cacheMu      sync.Mutex
	sharedLoaded map[string]bool
}

func (s *scriptRetryServer) cacheHas(sha string) bool {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	return s.sharedLoaded[sha]
}

func (s *scriptRetryServer) cachePut(sha string) {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	if s.sharedLoaded == nil {
		s.sharedLoaded = map[string]bool{}
	}
	s.sharedLoaded[sha] = true
}

func (s *scriptRetryServer) cacheClear() {
	s.cacheMu.Lock()
	defer s.cacheMu.Unlock()
	s.sharedLoaded = map[string]bool{}
}

func newScriptRetryServer(t *testing.T) *scriptRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &scriptRetryServer{ln: ln, dropBefore: "evalsha"}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *scriptRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	loaded := map[string]bool{}
	user := "default"
	// inMulti tracks a MULTI/EXEC transaction on this connection, and
	// queued records, per queued command, the error EXEC reports for it
	// ("" when it ran): a negative key count for EVAL is a
	// pre-execution rejection, while a Lua runtime error means the
	// script ran and cached before failing. The rejection is not in
	// the EVAL's own +QUEUED reply, it is an element of EXEC's
	// result array.
	inMulti := false
	var queued []string
	// watched tracks the keys this connection watches, and dirty
	// whether one of them was modified since the WATCH: like a real
	// server, an EXEC then aborts (a nil array) instead of committing,
	// so the queued commands never executed and their results never
	// exist.
	watched := map[string]bool{}
	dirty := false
	mgets := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		if first && s.dropBefore != "" && name == s.dropBefore {
			// Drop before replying: the command must be retried.
			return
		}
		if !first && s.dropRetryConn {
			// The connection handshake still needs its HELLO reply;
			// only the pipeline batch goes unanswered.
			if name == "hello" {
				_, _ = conn.Write([]byte("%0\r\n"))
				continue
			}
			// Read the retry batch, then close without replying to
			// any of it, so the retry fails while re-reading its
			// first command.
			if name == "script" && len(args) >= 2 && strings.EqualFold(args[0], "load") {
				s.loads.Add(1)
			}
			if name == "evalsha" {
				return
			}
			continue
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "auth":
			if len(args) >= 1 {
				user = args[0]
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "script":
			if len(args) >= 2 && strings.EqualFold(args[0], "load") {
				s.loads.Add(1)
				if s.denyLoad {
					_, _ = conn.Write([]byte("-NOPERM this user has no permissions to run the 'script|load' command\r\n"))
					continue
				}
				if s.loadUser != "" && user != s.loadUser {
					_, _ = conn.Write([]byte("-NOPERM this user has no permissions to run the 'script|load' command\r\n"))
					continue
				}
				sum := sha1.Sum([]byte(args[1]))
				digest := hex.EncodeToString(sum[:])
				if s.persistCache {
					s.cachePut(digest)
				} else {
					loaded[digest] = true
				}
				_, _ = fmt.Fprintf(conn, "$%d\r\n%s\r\n", len(digest), digest)
			} else if len(args) >= 1 && strings.EqualFold(args[0], "flush") {
				s.flushes.Add(1)
				if inMulti {
					// A FLUSH inside MULTI is queued: it runs only
					// when EXEC runs it, so a bogus mode is refused
					// in the EXEC result array, not in its own
					// reply — exactly like a queued EVAL's
					// rejection. A FLUSH with no mode, or ASYNC /
					// SYNC, holds a nil element; any other mode
					// holds the syntax error EXEC reports for it.
					elemErr := ""
					if len(args) >= 2 && !strings.EqualFold(args[1], "async") && !strings.EqualFold(args[1], "sync") {
						elemErr = "ERR syntax error"
					}
					queued = append(queued, elemErr)
					_, _ = conn.Write([]byte("+QUEUED\r\n"))
					continue
				}
				if len(args) >= 2 && !strings.EqualFold(args[1], "async") && !strings.EqualFold(args[1], "sync") {
					// A FLUSH takes only an optional ASYNC / SYNC
					// mode: any other argument is a syntax error,
					// like a real server — including a replayed
					// FLUSH the retry reissues.
					_, _ = conn.Write([]byte("-ERR syntax error\r\n"))
					continue
				}
				if !first && s.denyFlushRetry {
					// The replayed FLUSH is refused: the cache
					// keeps the script the reissued LOAD just
					// repopulated, so the retried EVALSHA runs
					// it — the failure must be visible, not
					// ignored as a best-effort preload.
					_, _ = conn.Write([]byte("-NOPERM this user has no permissions to run the 'script|flush' command\r\n"))
					continue
				}
				if s.persistCache {
					s.cacheClear()
				} else {
					loaded = map[string]bool{}
				}
				_, _ = conn.Write([]byte("+OK\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "multi":
			if inMulti {
				// A nested MULTI is rejected, but it does not
				// close the outer transaction: the frame and its
				// queued body stay open, exactly like a real
				// server, and it contributes no EXEC result element.
				_, _ = conn.Write([]byte("-ERR MULTI calls can not be nested\r\n"))
				continue
			}
			inMulti = true
			queued = nil
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "mget":
			mgets++
			if first && s.tryagainMget && mgets == 1 {
				_, _ = conn.Write([]byte("-TRYAGAIN Multiple keys request during slot migration\r\n"))
				continue
			}
			_, _ = conn.Write([]byte("*2\r\n$1\r\na\r\n$1\r\nb\r\n"))
		case "watch":
			if inMulti {
				// A WATCH inside MULTI is rejected without being
				// queued, exactly like a real server, so it holds
				// no EXEC result element (see
				// pipelineQueuedExecErrMap).
				_, _ = conn.Write([]byte("-ERR WATCH inside MULTI is not allowed\r\n"))
				continue
			}
			for _, key := range args {
				watched[key] = true
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "unwatch":
			if inMulti {
				// Redis queues UNWATCH inside MULTI (unlike WATCH,
				// which it rejects without queuing): it answers
				// +QUEUED, holds an EXEC result element and clears
				// the watch only when EXEC runs it (see
				// pipelineQueuedExecErrMap).
				queued = append(queued, "")
				_, _ = conn.Write([]byte("+QUEUED\r\n"))
				continue
			}
			watched = map[string]bool{}
			dirty = false
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "incr":
			if len(args) >= 1 && watched[args[0]] {
				dirty = true
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		case "discard":
			if s.failDiscard.Load() {
				// A rejected DISCARD leaves the transaction open:
				// the frame and its queued body stay, exactly
				// like a real server refusing DISCARD for an ACL
				// user (see pipelineTxBodyNeedsRetryMap).
				_, _ = conn.Write([]byte("-NOPERM this user has no permissions to run the 'discard' command\r\n"))
				continue
			}
			inMulti = false
			queued = nil
			watched = map[string]bool{}
			dirty = false
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "reset":
			if s.failReset.Load() {
				// A rejected RESET leaves the transaction open:
				// the frame and its queued body stay, exactly
				// like a real server refusing RESET for an ACL
				// user (see pipelineDiscardedTxCmds).
				_, _ = conn.Write([]byte("-NOPERM this user has no permissions to run the 'reset' command\r\n"))
				continue
			}
			// A successful RESET discards the transaction like
			// DISCARD and restores the default database.
			inMulti = false
			queued = nil
			watched = map[string]bool{}
			dirty = false
			_, _ = conn.Write([]byte("+RESET\r\n"))
		case "exec":
			if !inMulti {
				_, _ = conn.Write([]byte("-ERR EXEC without MULTI\r\n"))
				continue
			}
			if dirty {
				// A watched key changed after the WATCH: the
				// transaction aborts, so the queued commands
				// never executed — a nil array, not an array
				// of their results.
				inMulti = false
				queued = nil
				watched = map[string]bool{}
				dirty = false
				_, _ = conn.Write([]byte("*-1\r\n"))
				continue
			}
			inMulti = false
			var b strings.Builder
			fmt.Fprintf(&b, "*%d\r\n", len(queued))
			for _, elemErr := range queued {
				if elemErr != "" {
					b.WriteString("-" + elemErr + "\r\n")
				} else {
					b.WriteString(":1\r\n")
				}
			}
			queued = nil
			_, _ = conn.Write([]byte(b.String()))
		case "eval", "eval_ro":
			if inMulti {
				// A queued EVAL is accepted with +QUEUED; a
				// negative key count is refused only when EXEC
				// runs it, so the rejection is recorded for the
				// EXEC result array instead of replied here. A
				// Lua runtime error is recorded the same way, but
				// it means the script ran and cached before
				// failing — it is not a pre-execution rejection.
				s.evals.Add(1)
				elemErr := ""
				if len(args) >= 2 {
					if n, err := strconv.Atoi(args[1]); err == nil && n < 0 {
						elemErr = "ERR wrong number of keys for 'eval' command"
					}
				}
				if len(args) >= 1 && args[0] == "crossslot" {
					elemErr = "ERR wrong number of keys for 'eval' command"
				}
				if len(args) >= 1 && args[0] == queuedRuntimeErrScript {
					elemErr = "ERR Error running script (call to f_0123456789abcdef): @user_script:1: user_script:1: attempt to call error"
				}
				queued = append(queued, elemErr)
				_, _ = conn.Write([]byte("+QUEUED\r\n"))
				continue
			}
			s.evals.Add(1)
			// A malformed EVAL is refused before the script is
			// cached, like a negative key count: a missing numkeys
			// is an arity error, and a non-integer one fails
			// argument validation — including the wire forms
			// strconv.Atoi accepts but Redis rejects ("01", "+1"):
			// both are pre-execution rejections, so the script
			// cached nothing.
			if len(args) < 2 {
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'eval' command\r\n"))
				continue
			}
			if !retryRedisIntValid(args[1]) {
				if s.attrEvalRejectionOnce.CompareAndSwap(true, false) {
					// A RESP3 attribute block in front of the
					// error: a raw client keeps it in the reply
					// bytes, so the classifier must skip it to
					// see the rejection behind it.
					_, _ = conn.Write([]byte("|1\r\n+key\r\n+val\r\n-ERR value is not an integer or out of range\r\n"))
					continue
				}
				_, _ = conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
				continue
			}
			// A rejected EVAL never runs, so it populates nothing: a
			// negative key count is refused before the script is
			// cached. Only a successful EVAL counts as a cache entry.
			// A CROSSSLOT rejection is the same kind of pre-execution
			// refusal: the script never ran, so it cached nothing.
			rejected := false
			if len(args) >= 2 {
				if n, err := strconv.Atoi(args[1]); err == nil && n < 0 {
					rejected = true
				}
			}
			if rejected {
				_, _ = conn.Write([]byte("-ERR wrong number of keys for 'eval' command\r\n"))
				continue
			}
			if len(args) >= 1 && args[0] == "crossslot" {
				_, _ = conn.Write([]byte("-CROSSSLOT Keys in request don't hash to the same slot\r\n"))
				continue
			}
			if len(args) >= 1 {
				sum := sha1.Sum([]byte(args[0]))
				digest := hex.EncodeToString(sum[:])
				if s.persistCache {
					s.cachePut(digest)
				} else {
					loaded[digest] = true
				}
			}
			// A script that ran but returned nil, or failed at runtime,
			// still populated the cache before producing its reply.
			switch {
			case len(args) >= 1 && args[0] == "if ARGV[1] then return 42 end":
				_, _ = conn.Write([]byte("$-1\r\n"))
			case len(args) >= 1 && args[0] == "":
				// An empty script runs and returns nil, like a
				// real server: it still populated the cache above.
				_, _ = conn.Write([]byte("$-1\r\n"))
			case len(args) >= 1 && args[0] == runtimeIntErrScript:
				// The pcall fails at runtime on a non-integer key:
				// the script ran and cached before producing this
				// error, so it is not a pre-execution rejection.
				_, _ = conn.Write([]byte("-ERR value is not an integer or out of range\r\n"))
			case len(args) >= 1 && args[0] == runtimeMsetArityErrScript:
				// The pcall fails at runtime on MSET's arity: the
				// script ran and cached before producing this
				// error, so it is not a pre-execution rejection
				// of EVAL itself.
				_, _ = conn.Write([]byte("-ERR wrong number of arguments for 'mset' command\r\n"))
			case len(args) >= 1 && args[0] == "return redis.call('BADCMD')":
				_, _ = conn.Write([]byte("-ERR Error running script (call to f_0123456789abcdef): @user_script:1: Unknown Redis command called from Lua script\r\n"))
			default:
				_, _ = conn.Write([]byte(":1\r\n"))
			}
		case "evalsha":
			s.evalshas.Add(1)
			cached := false
			if len(args) >= 1 {
				if s.persistCache {
					cached = s.cacheHas(args[0])
				} else {
					cached = loaded[args[0]]
				}
			}
			if cached {
				if len(args) >= 1 && args[0] == queuedRuntimeErrScriptSHA {
					// The runtime-error script returns 42 when
					// ARGV[1] is truthy.
					_, _ = conn.Write([]byte(":42\r\n"))
				} else if len(args) >= 1 && args[0] == emptyScriptSHA {
					// An empty script runs and returns nil.
					_, _ = conn.Write([]byte("$-1\r\n"))
				} else {
					_, _ = conn.Write([]byte(":1\r\n"))
				}
			} else {
				s.noscripts.Add(1)
				_, _ = conn.Write([]byte("-NOSCRIPT No matching script. Please use EVAL.\r\n"))
			}
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryReissuesScriptLoad: a pipeline SCRIPT LOAD followed by
// EVALSHA must not be trimmed to the EVALSHA on retry. The LOAD populates
// the server's volatile script cache, which a restarted server does not
// have, so the retried EVALSHA would fail NOSCRIPT without it. Regression
// test for the review finding on #4351.
func TestPipelineRetryReissuesScriptLoad(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	load := pipe.ScriptLoad(ctx, src)
	eval := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must reissue the SCRIPT LOAD before the EVALSHA)", err)
	}
	if got := load.Val(); got != sha {
		t.Fatalf("SCRIPT LOAD = %q, want %q", got, sha)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil", got, err)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must be reissued on the retry connection)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryReissuesScriptFlush: a pipeline SCRIPT LOAD, SCRIPT FLUSH,
// EVALSHA must not be rebuilt as LOAD, EVALSHA on retry. The flush
// invalidated the script the LOAD populated, so a retry that drops it
// executes a script the flush had removed — the previous whole-batch retry
// repeated the flush and returned NOSCRIPT instead. Regression test for the
// review finding on #4351.
func TestPipelineRetryReissuesScriptFlush(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	pipe.ScriptLoad(ctx, src)
	pipe.ScriptFlush(ctx)
	eval := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the retry must replay the FLUSH after the LOAD, so the flushed script stays gone)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT", err)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must be reissued on the retry connection)", got)
	}
	if got := server.flushes.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT FLUSH commands, want 2 (the answered FLUSH must be reissued after the LOAD)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryBackoffCancelKeepsAnsweredResults: cancelling the context
// while the retry backoff sleeps must not overwrite commands a prior attempt
// already answered. The first attempt answers SELECT (a connection-local
// state command, so the retry is whole-batch) and then drops before SET's
// reply; the cancel lands in the backoff before the second attempt. The
// SELECT keeps its +OK, while the unanswered SET and INCR take the cancel
// error. Regression test for the review finding on #4351.
func TestPipelineRetryBackoffCancelKeepsAnsweredResults(t *testing.T) {
	server := newConnStateRetryServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: 5 * time.Second,
		MaxRetryBackoff: 5 * time.Second,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 1)
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "counter")
	errCh := make(chan error, 1)
	go func() {
		_, err := pipe.Exec(ctx)
		errCh <- err
	}()
	// The first attempt fails in milliseconds; the backoff lasts 5s, so
	// cancelling now lands inside the backoff sleep.
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Exec err=%v, want context.Canceled", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Exec did not return after the context was cancelled")
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered before the drop and must keep its result)", err)
	}
	if err := set.Err(); err == nil {
		t.Fatal("SET: nil, want the attempt's transport error (it was never answered)")
	}
	// INCR keeps the read path's transport error: it was never answered
	// either, and setCmdsErr only stamps commands with a nil error.
	if err := incr.Err(); err == nil {
		t.Fatal("INCR: nil, want the attempt's transport error (it was never answered)")
	}
}

// TestPipelineRetryReportsTransportErrOverFirstRedisErr: when the first
// command of a pipeline is answered with a Redis error and a later command
// fails with a transport error on the final attempt, Exec must report the
// transport error, not the first command's Redis error. The transport error
// means the batch never completed and the connection may hold an unread
// reply: hiding it behind the earlier Redis error lets callers that branch
// on the error kind — notably initConn's identity setup, which deliberately
// tolerates Redis errors — accept a desynchronized connection. Regression
// test for the review finding on #4351.
func TestPipelineRetryReportsTransportErrOverFirstRedisErr(t *testing.T) {
	server := newFirstErrRetryServer(t, true)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1, // a single attempt: the transport error is final
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	pipe := client.Pipeline()
	hget := pipe.HGet(ctx, "str", "f")
	incr := pipe.Incr(ctx, "text")
	_, err := pipe.Exec(ctx)
	if err == nil || isRedisError(err) {
		t.Fatalf("Exec: %v, want the transport error for the unanswered INCR, not the first command's WRONGTYPE", err)
	}
	if err := hget.Err(); err == nil || !strings.Contains(err.Error(), "WRONGTYPE") {
		t.Fatalf("HGET: %v, want WRONGTYPE (its own reply is preserved)", err)
	}
	if err := incr.Err(); err == nil || isRedisError(err) {
		t.Fatalf("INCR: %v, want the transport error (it was never answered)", err)
	}
}

// dialFailRetryServer is a scripted RESP server for retry-acquisition tests.
// The first connection answers SELECT and then drops mid-pipeline; the
// listener is closed as soon as that connection drops, so every later dial
// fails with connection refused.
type dialFailRetryServer struct {
	ln       net.Listener
	conns    atomic.Int32
	closeLn  sync.Once
	dials    atomic.Int32 // connections accepted after the first
	answered atomic.Int32 // SELECT replies sent
}

func newDialFailRetryServer(t *testing.T) *dialFailRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &dialFailRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			if n > 1 {
				s.dials.Add(1)
			}
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *dialFailRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			s.answered.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			if first {
				// Drop mid-pipeline and stop listening: the retry's
				// dial must fail, not reach a fresh server.
				s.closeLn.Do(func() { _ = s.ln.Close() })
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryDialFailureKeepsAnsweredResults: after a whole-batch retry
// is narrowed (the first attempt answered SELECT, then dropped before SET),
// the retry's dial failure must not stamp the dial error onto SELECT. The
// pre-attempt error clear used to wipe the tail transport errors that
// pipelineUnansweredSuffix uses as the answered/unanswered boundary, so a
// dial failure after the clear stamped the whole batch — including the
// command a prior attempt had already answered. The backoff-cancel test only
// covers a cancel during the sleep that runs before that clear, not a
// failure after it. Regression test for the review finding on #4351.
func TestPipelineRetryDialFailureKeepsAnsweredResults(t *testing.T) {
	server := newDialFailRetryServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 1)
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the dial error (the listener is closed after the first drop)")
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered before the drop and must keep its result)", err)
	}
	if err := set.Err(); err == nil {
		t.Fatal("SET: nil, want the attempt's transport error (it was never answered)")
	}
	if err := incr.Err(); err == nil {
		t.Fatal("INCR: nil, want the attempt's transport error (it was never answered)")
	}
}

// identityTransportServer is a scripted RESP server for initConn identity
// setup tests. It answers HELLO, rejects the first CLIENT SETINFO with a
// Redis error (as a server older than 7.2 would), and withholds the second
// SETINFO reply until it reads the next command — only then does it send
// the late +OK. A client that accepts the connection anyway reads that
// late reply as its next command's reply.
type identityTransportServer struct {
	ln    net.Listener
	conns atomic.Int32
}

func newIdentityTransportServer(t *testing.T) *identityTransportServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &identityTransportServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.conns.Add(1)
			go s.serve(conn)
		}
	}()
	return s
}

func (s *identityTransportServer) serve(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	setinfo := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "client":
			// readConnStateRetryCommand strips the command name, so
			// args[0] is "setinfo" and args[1] is LIB-NAME / LIB-VER.
			if len(args) >= 2 && strings.EqualFold(args[0], "setinfo") {
				setinfo++
				if setinfo == 1 {
					_, _ = conn.Write([]byte("-ERR unknown command 'client', with args beginning with: 'setinfo'\r\n"))
				}
				// The second SETINFO reply is withheld: the read
				// times out on the client, and the late reply is
				// only sent after the next command arrives.
				continue
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "ping":
			// Send the withheld SETINFO reply first: an accepted
			// connection consumes it as PING's reply.
			if setinfo >= 2 {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
			_, _ = conn.Write([]byte("+PONG\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestInitConnRejectsIdentityPipelineTransportFailure: with MaxRetries -1 and
// identity setup enabled, a CLIENT SETINFO Redis error on the first setup
// command must not hide a transport timeout on the second: initConn
// deliberately ignores Redis errors, so if Exec reported only the Redis
// error the connection would be accepted with a pending SETINFO reply in
// its stream, and the next user command would consume that reply instead
// of its own. Regression test for the review finding on #4351.
func TestInitConnRejectsIdentityPipelineTransportFailure(t *testing.T) {
	server := newIdentityTransportServer(t)
	client := NewClient(&Options{
		Addr:         server.ln.Addr().String(),
		Protocol:     2,
		PoolSize:     1,
		MaxRetries:   -1, // a single attempt: the timeout is final
		DialTimeout:  5 * time.Second,
		ReadTimeout:  200 * time.Millisecond,
		WriteTimeout: 5 * time.Second,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	val, err := client.Ping(ctx).Result()
	if err == nil {
		t.Fatalf("Ping = %q, nil; want a transport error (the identity pipeline's SETINFO reply timed out, so init must reject the connection)", val)
	}
	if isRedisError(err) {
		t.Fatalf("Ping err = %v, want a transport error, not a Redis error", err)
	}
}

// dialFailLoadingServer is a scripted RESP server for rejected-command
// retry tests. The first connection answers SET with a retryable LOADING
// error and then drops before INCR's reply, and closes the listener, so
// every later dial fails instead of reaching a fresh server.
type dialFailLoadingServer struct {
	ln      net.Listener
	conns   atomic.Int32
	closeLn sync.Once
}

func newDialFailLoadingServer(t *testing.T) *dialFailLoadingServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &dialFailLoadingServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *dialFailLoadingServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			if first {
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "incr":
			if first {
				// Drop before replying and stop listening: the
				// retry's dial must fail, not reach a fresh server.
				s.closeLn.Do(func() { _ = s.ln.Close() })
				return
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryDialFailureKeepsRejectedCommandError: when SET is answered
// with a retryable LOADING error (the server rejected it without executing
// it) and INCR's reply is then lost, a later dial failure must not make SET
// look successful: the SET was never executed, so its error must survive
// until a retry can dispatch it again. Regression test for the review
// finding on #4351.
func TestPipelineRetryDialFailureKeepsRejectedCommandError(t *testing.T) {
	server := newDialFailLoadingServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the dial error (the listener is closed after the first drop)")
	}
	if err := set.Err(); err == nil {
		t.Fatal("SET: nil, want the LOADING rejection (it was never executed, so it must not look successful)")
	}
	if err := incr.Err(); err == nil {
		t.Fatal("INCR: nil, want the attempt's transport error (it was never answered)")
	}
}

// relistenLoadingServer is a scripted RESP server for the cleared-reply
// retry test. The first connection answers SET with LOADING and drops
// before INCR's reply; the listener then closes for reopenDelay and
// re-opens on the same address. reopenDelay is chosen so that one retry
// attempt's whole dial sequence is exhausted while the listener is down
// (the pool retries a failed dial up to 5 times, 100ms apart, inside a
// single getConn) and a later attempt's dial sequence reaches the fresh
// server that answers everything.
type relistenLoadingServer struct {
	ln          net.Listener
	addr        string
	conns       atomic.Int32
	sets        atomic.Int32
	closeLn     sync.Once
	reopenOnce  sync.Once
	reopenDelay time.Duration
	reopenErr   atomic.Value // error, if re-listening failed
}

func newRelistenLoadingServer(t *testing.T, reopenDelay time.Duration) *relistenLoadingServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &relistenLoadingServer{ln: ln, addr: ln.Addr().String(), reopenDelay: reopenDelay}
	t.Cleanup(func() { _ = ln.Close() })
	go s.acceptLoop(ln)
	return s
}

func (s *relistenLoadingServer) acceptLoop(ln net.Listener) {
	for {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		n := s.conns.Add(1)
		go s.serve(conn, n == 1)
	}
}

func (s *relistenLoadingServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			s.sets.Add(1)
			if first {
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
			} else {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "incr":
			if first {
				// Drop before replying, close the listener, and
				// re-open it after reopenDelay: the next dial
				// fails, the one after it reaches a fresh server.
				s.closeLn.Do(func() { _ = s.ln.Close() })
				s.reopenOnce.Do(func() {
					go func() {
						time.Sleep(s.reopenDelay)
						ln, err := net.Listen("tcp", s.addr)
						if err != nil {
							s.reopenErr.Store(err)
							return
						}
						s.ln = ln
						go s.acceptLoop(ln)
					}()
				})
				return
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryClearedLoadingCommandIsNotDropped: when SET is answered
// with a retryable LOADING error and INCR's reply is then lost, a later
// dial failure must not drop SET from the retry: the LOADING reply never
// executed it, so a succeeding retry must re-execute it — Exec must not
// report success for a write that never applied. Regression test for the
// review finding on #4351.
func TestPipelineRetryClearedLoadingCommandIsNotDropped(t *testing.T) {
	// Timeline (client backoff fixed at 50ms): attempt 0 answers SET
	// with LOADING and loses INCR's reply. Attempt 1 dispatches ~50ms
	// later; one dispatch runs TWO dial sequences back to back — the
	// pipeline pool's TryGet dials (5 dials, 100ms apart, ~400ms) and,
	// on failure, spills to the main pool which dials again (~400ms) —
	// so the retry loop only sees a dial failure at ~850ms. Attempt 2
	// dispatches ~900ms in; its dial sequence reaches the reopened
	// listener (~1250ms, the ~1300ms dial) and dispatches the pending
	// commands. PoolSize 2 keeps the pool's dial breaker (which trips
	// when dialErrorsNum reaches PoolSize) from short-circuiting
	// attempt 2's dial sequence after attempt 1's sequences were
	// exhausted.
	server := newRelistenLoadingServer(t, 1250*time.Millisecond)
	client := NewClient(&Options{
		Addr:     server.addr,
		Protocol: 2,
		// A custom Dialer makes this a non-fixed-endpoint client
		// (fixedDialEndpoint is opt.Dialer == nil): only then does the
		// retry loop keep going after a dial failure. With the default
		// dialer, an exhausted dial sequence sets the dialExhausted
		// marker and the loop exits at once, so no later attempt could
		// ever succeed — the dropped write would surface only as a
		// dial error, never as a silent success.
		Dialer: func(ctx context.Context, network, addr string) (net.Conn, error) {
			return (&net.Dialer{Timeout: 5 * time.Second}).DialContext(ctx, network, addr)
		},
		PoolSize:        2,
		MaxRetries:      3,
		MinRetryBackoff: 50 * time.Millisecond,
		MaxRetryBackoff: 50 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry after the failed dial must succeed)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (it must have been re-executed once the server recovered)", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %v, %v; want 1, nil", got, err)
	}
	if err, ok := server.reopenErr.Load().(error); ok && err != nil {
		t.Fatalf("re-listening failed: %v", err)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the SET answered LOADING must be re-executed after the failed dial, not dropped as though it had been answered)", got)
	}
}

// TestScriptCmdRawArgs: the SCRIPT LOAD / FLUSH predicates must match the
// subcommand however the caller spelled it. Writer.WriteArg and
// baseCmd.stringArg both accept string, *string and []byte, so a raw command
// built with Do(ctx, "SCRIPT", []byte("FLUSH")) is a real SCRIPT FLUSH on
// the wire — the predicates must not only recognise the string form.
// Regression test for the review finding on #4351.
func TestScriptCmdRawArgs(t *testing.T) {
	ctx := context.Background()
	flush := "FLUSH"
	load := "LOAD"
	cases := []struct {
		name            string
		args            []interface{}
		isLoad, isFlush bool
	}{
		{"string LOAD", []interface{}{"SCRIPT", "LOAD", "return 1"}, true, false},
		{"string FLUSH", []interface{}{"SCRIPT", "FLUSH"}, false, true},
		{"bytes LOAD", []interface{}{"SCRIPT", []byte("LOAD"), "return 1"}, true, false},
		{"bytes FLUSH", []interface{}{"SCRIPT", []byte("FLUSH")}, false, true},
		{"stringptr LOAD", []interface{}{"SCRIPT", &load, "return 1"}, true, false},
		{"stringptr FLUSH", []interface{}{"SCRIPT", &flush}, false, true},
		{"lowercase bytes flush", []interface{}{"SCRIPT", []byte("flush")}, false, true},
		{"string EXISTS", []interface{}{"SCRIPT", "EXISTS", "abc"}, false, false},
		{"bytes EXISTS", []interface{}{"SCRIPT", []byte("EXISTS"), "abc"}, false, false},
		{"not script", []interface{}{"EVALSHA", "abc"}, false, false},
		{"no subcommand", []interface{}{"SCRIPT"}, false, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewCmd(ctx, tc.args...)
			if got := isScriptLoadCmd(cmd); got != tc.isLoad {
				t.Fatalf("isScriptLoadCmd = %v, want %v", got, tc.isLoad)
			}
			if got := isScriptFlushCmd(cmd); got != tc.isFlush {
				t.Fatalf("isScriptFlushCmd = %v, want %v", got, tc.isFlush)
			}
		})
	}
}

// TestPipelineRetryReissuesScriptFlushRawArgs: the same LOAD / FLUSH /
// EVALSHA batch as TestPipelineRetryReissuesScriptFlush, but the FLUSH is a
// raw Do command carrying the subcommand as []byte. The retry must still
// replay the FLUSH after the LOAD: replaying the LOAD without it executes a
// script the batch had already removed. Regression test for the review
// finding on #4351.
func TestPipelineRetryReissuesScriptFlushRawArgs(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	pipe.ScriptLoad(ctx, src)
	pipe.Do(ctx, "SCRIPT", []byte("FLUSH"))
	eval := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the retry must replay the raw []byte FLUSH after the LOAD, so the flushed script stays gone)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT", err)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must be reissued on the retry connection)", got)
	}
	if got := server.flushes.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT FLUSH commands, want 2 (the answered []byte FLUSH must be reissued after the LOAD)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetrySkipsRejectedTopLevelRawScriptFlush: a top-level raw
// SCRIPT FLUSH the server rejects never emptied the cache, so the retry
// must not replay it as a required prerequisite. Here the first
// connection answers the LOAD, refuses the bogus FLUSH with a syntax
// error, and drops before EVALSHA's reply: without the fix the rebuild
// reissues the FLUSH with the same bogus argument, its repeated syntax
// error fails the batch even though the retried EVALSHA succeeds; with
// it the FLUSH is skipped, the LOAD repopulates the cache, and the
// EVALSHA runs. Regression test for the Codex P2 finding on #4351.
func TestPipelineRetrySkipsRejectedTopLevelRawScriptFlush(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	pipe.ScriptLoad(ctx, src)
	// A raw FLUSH: its error frame stays in Val() with a nil Err(), so
	// the batch can still succeed once the retry stops replaying it —
	// a typed command would keep the syntax error as the batch's first
	// error through finalErr no matter what the retry does.
	flush := NewRawCmd(ctx, "SCRIPT", "FLUSH", "bogus")
	if err := pipe.BatchProcess(ctx, flush); err != nil {
		t.Fatalf("BatchProcess: %v", err)
	}
	eval := pipe.EvalSha(ctx, sha, nil)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the rejected FLUSH must not be replayed, and the retried EVALSHA succeeds)", err)
	}
	// The original FLUSH keeps its syntax error in Val(): the server
	// refused it, so it emptied nothing.
	if err := flush.Err(); err != nil {
		t.Fatalf("FLUSH Err(): %v, want nil (a raw command's error frame stays in Val() by contract)", err)
	}
	if got := string(flush.Val()); !strings.Contains(got, "syntax error") {
		t.Fatalf("FLUSH Val() = %q, want it to keep the syntax error frame", got)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil", got, err)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must be reissued on the retry connection)", got)
	}
	if got := server.flushes.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT FLUSH commands, want 1 (the rejected FLUSH must not be replayed on the retry connection)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0 (the retried EVALSHA must find the reloaded script)", got)
	}
}

// TestPipelineRetryStaleReusedCommandIsStampedOnDialFailure: a Cmder reused
// from an earlier pipeline keeps its old io.EOF, and that stale error must
// not act as the answered/unanswered boundary when the next pipeline never
// acquires a connection. A fresh SET queued before the reused command was
// never dispatched, so it must be stamped with the dial error instead of
// keeping Err() == nil while Exec reports the failure. Regression test for
// the review finding on #4351.
func TestPipelineRetryStaleReusedCommandIsStampedOnDialFailure(t *testing.T) {
	// First pipeline: stamp io.EOF onto the shared command. A single
	// attempt (MaxRetries -1 normalises to 0) answers the warmup SET and
	// drops before the shared SET's reply, so no retry heals it.
	server := newPartialRetryServer(t)
	first := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = first.Close() })

	ctx := context.Background()
	pipe := first.Pipeline()
	pipe.Set(ctx, "warmup", "v", 0)
	shared := pipe.Set(ctx, "shared", "v", 0)
	if _, err := pipe.Exec(ctx); err == nil {
		t.Fatalf("first Exec: nil, want the drop's transport error")
	}
	if err := shared.Err(); !errors.Is(err, io.EOF) {
		t.Fatalf("shared SET after first Exec: %v, want io.EOF (the drop must leave the transport error on it)", err)
	}

	// Second pipeline: the same shared command, behind a fresh SET, against
	// an address nothing listens on — every dial is refused, so no command
	// is ever dispatched.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	dead := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = dead.Close() })

	pipe2 := dead.Pipeline()
	fresh := pipe2.Set(ctx, "fresh", "v", 0)
	_ = pipe2.BatchProcess(ctx, shared)
	_, err = pipe2.Exec(ctx)
	if err == nil {
		t.Fatalf("second Exec: nil, want the dial error (nothing listens on %s)", addr)
	}
	if err := fresh.Err(); err == nil {
		t.Fatal("fresh SET: nil, want the dial error (it was never dispatched, so it must not look successful)")
	}
	if err := shared.Err(); err == nil {
		t.Fatal("shared SET: nil, want an error (stale EOF or the dial error)")
	}
}

// staleTransportRetryServer is a scripted RESP server for the stale
// transport-error pipeline retry test. The first connection reads the whole
// batch and then closes without replying, so the first attempt leaves
// io.EOF on every command. Later connections answer HELLO and then stop
// reading forever, so the retry's write blocks until the caller's context
// deadline.
type staleTransportRetryServer struct {
	ln       net.Listener
	conns    atomic.Int32
	sets     atomic.Int32
	wantSets int32
	done     chan struct{}
}

func newStaleTransportRetryServer(t *testing.T, wantSets int) *staleTransportRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &staleTransportRetryServer{ln: ln, wantSets: int32(wantSets), done: make(chan struct{})}
	t.Cleanup(func() { close(s.done) })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *staleTransportRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	if first {
		for {
			name, err := readStaleErrRESPCommand(rd)
			if err != nil {
				return
			}
			switch name {
			case "hello":
				_, _ = conn.Write([]byte("%0\r\n"))
			case "set":
				if s.sets.Add(1) >= s.wantSets {
					// Whole batch read: close without replying, so
					// the first attempt's read fails with io.EOF.
					return
				}
			default:
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		}
	}
	// Later connections: answer the setup HELLO, then never read again.
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		if name == "hello" {
			_, _ = conn.Write([]byte("%0\r\n"))
			<-s.done
			return
		}
	}
}

// TestPipelineRetryReportsLatestTransportErrAfterRetryWriteFails: when the
// first attempt leaves io.EOF on the pending commands and the retry's write
// then hits the context deadline, Exec must report the latest transport
// error — errors.Is(err, context.DeadlineExceeded) — not the stale EOF, and
// the pipeline duration metric must describe the deadline failure, not the
// earlier EOF. Regression test for the review finding on #4351.
func TestPipelineRetryReportsLatestTransportErrAfterRetryWriteFails(t *testing.T) {
	const nsets = 128
	server := newStaleTransportRetryServer(t, nsets)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		// The retry write must fail with the caller's context deadline, not
		// the socket WriteTimeout: c.context only forwards the caller's ctx
		// when ContextTimeoutEnabled is set.
		ContextTimeoutEnabled: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	value := strings.Repeat("x", 256*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pipe := client.Pipeline()
	var sets []*StatusCmd
	for i := 0; i < nsets; i++ {
		sets = append(sets, pipe.Set(ctx, fmt.Sprintf("k%d", i), value, 0))
	}
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the retry write's context deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exec: %v, want context.DeadlineExceeded (the retry write hit the deadline; the stale io.EOF from the first attempt must not mask it)", err)
	}
	if errors.Is(err, io.EOF) {
		t.Fatalf("Exec: %v, want the latest transport error, not the stale io.EOF", err)
	}
	if err := sets[0].Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SET[0]: %v, want context.DeadlineExceeded", err)
	}
	if got := server.sets.Load(); got != int32(nsets) {
		t.Fatalf("server received %d SET commands on the first connection, want %d (the whole batch must have been read before the drop)", got, nsets)
	}
}

// TestPipelineRetryReissuesEvalAsScriptLoad: a pipeline EVAL followed by
// EVALSHA must not be trimmed to the EVALSHA on retry. EVAL populates the
// server's volatile script cache like SCRIPT LOAD, so a retry on a
// restarted server must reissue the answered EVAL's source as a SCRIPT LOAD
// ahead of the EVALSHA — without executing the answered script again.
// Regression test for the review finding on #4351.
func TestPipelineRetryReissuesEvalAsScriptLoad(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must reissue the answered EVAL's source as SCRIPT LOAD before the EVALSHA)", err)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVAL = %v, %v; want 1, nil", got, err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (the answered EVAL's source must be reissued once as a LOAD on the retry connection)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryRejectedEvalIsNotPreloaded: an EVAL the server rejected
// (negative key count) never ran, so it cached nothing. The retry must not
// turn that rejection into a successful SCRIPT LOAD: the retried EVALSHA
// would then execute writes where the original pipeline and the previous
// whole-batch retry returned NOSCRIPT. Regression test for the review
// finding on #4351.
func TestPipelineRetryRejectedEvalIsNotPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Do(ctx, "eval", src, -1)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "wrong number of keys") {
		t.Fatalf("Exec: %v, want the rejected EVAL's error (the first failed command wins; the retry must not turn the rejection into a successful SCRIPT LOAD)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "wrong number of keys") {
		t.Fatalf("EVAL: %v, want the wrong-number-of-keys rejection", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to reissue)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryEvalLeadingZeroNumkeysIsNotPreloaded: an EVAL whose
// numkeys is "01" is refused before execution — Redis's integer parser
// rejects the leading zero, even though strconv.Atoi accepts it — so
// the script never cached and the retried EVALSHA must fail NOSCRIPT,
// not execute the write script a synthesised SCRIPT LOAD would load.
// Regression test for the Codex P1 finding on #4351.
func TestPipelineRetryEvalLeadingZeroNumkeysIsNotPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Do(ctx, "eval", src, "01", "k")
	evalsha := pipe.EvalSha(ctx, sha, []string{"k"})
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "value is not an integer") {
		t.Fatalf("Exec: %v, want the rejected EVAL's error (the first failed command wins; the retry must not turn the rejection into a successful SCRIPT LOAD)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "value is not an integer") {
		t.Fatalf("EVAL: %v, want the value-is-not-an-integer rejection", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to reissue)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// pre-execution cluster refusal — the script never ran, so it cached
// nothing. The retry must not turn it into a successful SCRIPT LOAD: the
// retried EVALSHA would then execute writes the original batch refused.
// Regression test for the Cursor Bugbot finding on #4351.
func TestPipelineRetryCrossslotEvalIsNotPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "crossslot"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Do(ctx, "eval", src, 0)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "CROSSSLOT") {
		t.Fatalf("Exec: %v, want the CROSSSLOT rejection (the first failed command wins; the retry must not turn the rejection into a successful SCRIPT LOAD)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "CROSSSLOT") {
		t.Fatalf("EVAL: %v, want the CROSSSLOT rejection", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to reissue)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetrySkipsScriptPreloadWithoutEvalSha: when no command in the
// retry suffix needs the script cache, no SCRIPT LOAD may be prepended. An
// ACL user allowed EVAL and GET but not SCRIPT LOAD would otherwise fail a
// batch whose own commands all succeed, on the synthetic LOAD's NOPERM.
// Regression test for the review finding on #4351.
func TestPipelineRetrySkipsScriptPreloadWithoutEvalSha(t *testing.T) {
	server := newScriptRetryServer(t)
	server.denyLoad = true
	server.dropBefore = "get"
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (no command in the retry suffix needs the script cache, so no SCRIPT LOAD may be prepended)", err)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVAL = %v, %v; want 1, nil", got, err)
	}
	if err := get.Err(); err != nil {
		t.Fatalf("GET: %v, want nil", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (the retry suffix has no EVALSHA)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
}

// TestPipelineRetryReissuedScriptLoadKeepsOriginalResult: a SCRIPT LOAD the
// first attempt answered must keep its digest when the retry that reissues
// it fails while reading. The reissued LOAD is a fresh command; stamping
// the retry's transport error onto the original would replace its digest
// with a failure for work that already succeeded. Regression test for the
// review finding on #4351.
func TestPipelineRetryReissuedScriptLoadKeepsOriginalResult(t *testing.T) {
	server := newScriptRetryServer(t)
	server.dropRetryConn = true
	client := newMaxRetriesClient(t, server.ln.Addr().String(), 1)

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	load := pipe.ScriptLoad(ctx, src)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the retry's read failure")
	}
	if got := load.Val(); got != sha {
		t.Fatalf("SCRIPT LOAD = %q, want %q (the digest was answered on the first attempt; the retry's read failure must not overwrite it)", got, sha)
	}
	if err := load.Err(); err != nil {
		t.Fatalf("SCRIPT LOAD: %v, want nil", err)
	}
	if err := evalsha.Err(); err == nil {
		t.Fatalf("EVALSHA: nil, want the retry's read failure (it was never answered)")
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must be reissued on the retry connection)", got)
	}
}

// readStampRetryServer is a scripted RESP server for retry-read-stamping
// tests. The first connection answers SET and GET, then drops before WAIT's
// reply, so the retry re-runs the whole batch; the retry connection reads
// the batch and closes without replying, so the retry fails while
// re-reading its first command.
type readStampRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	sets  atomic.Int32
	gets  atomic.Int32
	waits atomic.Int32
}

func newReadStampRetryServer(t *testing.T) *readStampRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &readStampRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *readStampRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			s.sets.Add(1)
			if first {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "get":
			s.gets.Add(1)
			if first {
				_, _ = conn.Write([]byte("$1\r\nv\r\n"))
			}
		case "wait":
			s.waits.Add(1)
			// First connection: drop before replying, so the retry
			// re-runs the whole batch. Retry connection: close once
			// the batch arrived, without replying to any of it.
			return
		default:
			if first {
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		}
	}
}

// TestPipelineRetryReadFailureKeepsAnsweredResults: when a whole-batch retry
// (a WAIT follows the answered writes) dies while re-reading its first
// command, the read failure must not stamp over commands a prior attempt
// already answered. The GET keeps its value; only the re-dispatched SET
// and the never-answered WAIT take the transport error. Regression test
// for the review finding on #4351.
func TestPipelineRetryReadFailureKeepsAnsweredResults(t *testing.T) {
	server := newReadStampRetryServer(t)
	client := newMaxRetriesClient(t, server.ln.Addr().String(), 1)

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	get := pipe.Get(ctx, "k")
	wait := pipe.Do(ctx, "wait", 1, 1000)
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the retry's read failure")
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil (the value was answered on the first attempt and the retry's read failure must not overwrite it)", got, err, "v")
	}
	if err := set.Err(); err == nil {
		t.Fatalf("SET: nil, want the retry's read failure (it was re-dispatched and never answered on the retry)")
	}
	if err := wait.Err(); err == nil {
		t.Fatalf("WAIT: nil, want the retry's read failure (it was never answered)")
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the answered SET must be re-run with its WAIT)", got)
	}
	if got := server.waits.Load(); got != 2 {
		t.Fatalf("server received %d WAIT commands, want 2", got)
	}
}

// newMaxRetriesClient is newPartialRetryClient with an explicit retry budget:
// a budget of 1 makes the second attempt the last, so an exit-path test can
// pin what a failed retry leaves behind.
func newMaxRetriesClient(t *testing.T, addr string, maxRetries int) *Client {
	t.Helper()
	client := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      maxRetries,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// seedStaleEOFIncr runs one pipeline that leaves io.EOF on a shared INCR
// and returns it: a warmup SET is answered, then the connection drops
// before the INCR's reply. The returned command carries the stale EOF a
// later pipeline must not mistake for its own progress.
func seedStaleEOFIncr(t *testing.T) *IntCmd {
	t.Helper()
	server := newPartialRetryServer(t)
	seeder := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = seeder.Close() })

	ctx := context.Background()
	pipe := seeder.Pipeline()
	pipe.Set(ctx, "warmup", "v", 0)
	shared := pipe.Incr(ctx, "counter")
	if _, err := pipe.Exec(ctx); err == nil {
		t.Fatalf("seeding Exec: nil, want the drop's transport error")
	}
	if err := shared.Err(); !errors.Is(err, io.EOF) {
		t.Fatalf("shared INCR after seeding: %v, want io.EOF", err)
	}
	return shared
}

// TestPipelineRetryReusedCommandFreshFailureIsRetried: a reused INCR that
// carries a stale io.EOF must still be retried when THIS attempt records a
// fresh EOF on it. The batch is a fresh SET, the reused INCR, and a fresh
// GET; the first connection answers the SET and then drops. The stale
// marker must be retired once the attempt dispatches the command, so the
// retry starts at the INCR instead of skipping it and leaving it with EOF
// after a healthy reconnection. Regression test for the review finding on
// #4351.
func TestPipelineRetryReusedCommandFreshFailureIsRetried(t *testing.T) {
	shared := seedStaleEOFIncr(t)

	server := newPartialRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	_ = pipe.BatchProcess(ctx, shared)
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must start at the reused INCR, whose fresh EOF is this attempt's progress)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil", err)
	}
	if got, err := shared.Result(); err != nil || got != int64(1) {
		t.Fatalf("reused INCR = %v, %v; want 1, nil (it must be retried, not left with the stale EOF)", got, err)
	}
	if got, err := get.Result(); err != nil || got != "1" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "1")
	}
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the reused INCR must be dispatched once per attempt)", got)
	}
}

// freshLoadingRetryServer is a scripted RESP server for the reused-command
// LOADING test. The first connection answers INCR with LOADING and then
// drops before SET's reply; later connections answer everything.
type freshLoadingRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	incrs atomic.Int32
	sets  atomic.Int32
}

func newFreshLoadingRetryServer(t *testing.T) *freshLoadingRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &freshLoadingRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *freshLoadingRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "incr":
			s.incrs.Add(1)
			if first {
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
			} else {
				_, _ = conn.Write([]byte(":1\r\n"))
			}
		case "set":
			s.sets.Add(1)
			if first {
				// Drop before replying: the SET gets no reply on
				// this connection.
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryReusedCommandFreshLoadingIsRetried: a reused INCR that
// carries a stale io.EOF must still be retried when THIS attempt records a
// fresh LOADING reply on it. The batch is the reused INCR and a fresh SET;
// the first connection answers the INCR with LOADING and then drops before
// the SET's reply. The stale marker must be retired once the attempt
// dispatches the command, so the retry starts at the INCR's retryable
// LOADING instead of treating the command as an answered prefix and
// dropping it. Regression test for the review finding on #4351.
func TestPipelineRetryReusedCommandFreshLoadingIsRetried(t *testing.T) {
	shared := seedStaleEOFIncr(t)

	server := newFreshLoadingRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	_ = pipe.BatchProcess(ctx, shared)
	set := pipe.Set(ctx, "k", "v", 0)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must start at the reused INCR's fresh LOADING, not drop it as an answered prefix)", err)
	}
	if got, err := shared.Result(); err != nil || got != int64(1) {
		t.Fatalf("reused INCR = %v, %v; want 1, nil (its fresh LOADING must be retried, not kept)", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil", err)
	}
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the reused INCR must be dispatched once per attempt)", got)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the SET must be retried with the INCR)", got)
	}
}

// answeredWriteFailRetryServer is a scripted RESP server for the
// answered-results-on-write-failure test. The first connection answers
// SELECT and then drops before the next pipeline command's reply; later
// connections answer HELLO and then stop reading forever, so the retry's
// write blocks until the caller's context deadline.
type answeredWriteFailRetryServer struct {
	ln      net.Listener
	conns   atomic.Int32
	selects atomic.Int32
	sets    atomic.Int32
	done    chan struct{}
}

func newAnsweredWriteFailRetryServer(t *testing.T) *answeredWriteFailRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &answeredWriteFailRetryServer{ln: ln, done: make(chan struct{})}
	t.Cleanup(func() { close(s.done) })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *answeredWriteFailRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	if !first {
		// Later connections: answer the setup HELLO, then never
		// read again, so the retry's write blocks until the
		// caller's context deadline.
		for {
			name, err := readStaleErrRESPCommand(rd)
			if err != nil {
				return
			}
			if name == "hello" {
				_, _ = conn.Write([]byte("%0\r\n"))
				<-s.done
				return
			}
		}
	}
	answered := false
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "select":
			s.selects.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
			answered = true
		case "set":
			s.sets.Add(1)
			if answered {
				// Drop before replying: the rest of the batch
				// gets no reply on this connection.
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			if answered {
				// Drop before replying: the rest of the batch
				// gets no reply on this connection.
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryWriteFailureKeepsAnsweredResults: when the first attempt
// answers SELECT (a connection-local state command, so the retry is
// whole-batch) and then drops, and the retry's write fails on the
// context deadline, the answered SELECT must keep its +OK. A batch-level
// transport stamp must not overwrite results a prior attempt already
// read. Regression test for the review finding on #4351.
func TestPipelineRetryWriteFailureKeepsAnsweredResults(t *testing.T) {
	server := newAnsweredWriteFailRetryServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		// The retry write must fail with the caller's context deadline, not
		// the socket WriteTimeout: c.context only forwards the caller's ctx
		// when ContextTimeoutEnabled is set.
		ContextTimeoutEnabled: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 1)
	set := pipe.Set(ctx, "k", strings.Repeat("x", 32*1024*1024), 0)
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the retry write's context deadline error")
	}
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Exec: %v, want context.DeadlineExceeded", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered by the first attempt and the retry never re-executed it)", err)
	}
	if err := set.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("SET: %v, want context.DeadlineExceeded (it was never answered)", err)
	}
	if err := incr.Err(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("INCR: %v, want context.DeadlineExceeded (it was never answered)", err)
	}
	if got := server.selects.Load(); got != 1 {
		t.Fatalf("server received %d SELECT commands, want 1 (the retry's write must have blocked before re-executing it)", got)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (the retry's write must have blocked before re-executing it)", got)
	}
}

// loadingWriteFailRetryServer is a scripted RESP server for the
// write-failure fallback test. The first connection answers SET with
// LOADING and GET with a value, then closes; later connections answer
// HELLO and then stop reading forever, so a retry write that reaches a
// fresh connection blocks until the caller's context deadline.
type loadingWriteFailRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	sets  atomic.Int32
	done  chan struct{}
}

func newLoadingWriteFailRetryServer(t *testing.T) *loadingWriteFailRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &loadingWriteFailRetryServer{ln: ln, done: make(chan struct{})}
	t.Cleanup(func() { close(s.done) })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *loadingWriteFailRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	if !first {
		// Later connections: answer the setup HELLO, then never
		// read again, so a retry write that reaches a fresh
		// connection blocks until the caller's context deadline.
		for {
			name, err := readStaleErrRESPCommand(rd)
			if err != nil {
				return
			}
			if name == "hello" {
				_, _ = conn.Write([]byte("%0\r\n"))
				<-s.done
				return
			}
		}
	}
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			s.sets.Add(1)
			_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
		case "get":
			_, _ = conn.Write([]byte("$1\r\nv\r\n"))
			// The batch is fully answered: close, so the retry
			// cannot silently re-run it on this connection.
			return
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryWriteFailureKeepsAnsweredResultsOnFallback: the same
// answered-results guarantee as TestPipelineRetryWriteFailureKeepsAnsweredResults,
// but on the exit path where no command carries a transport error. The
// first attempt answers SET with LOADING and GET with a value, so the
// retry's write fails with nothing stamped; the exit stamping must still
// leave the answered GET alone instead of falling back to stamping the
// whole batch. Regression test for the review finding on #4351.
func TestPipelineRetryWriteFailureKeepsAnsweredResultsOnFallback(t *testing.T) {
	server := newLoadingWriteFailRetryServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		// The retry write must fail with the caller's context deadline, not
		// the socket WriteTimeout: c.context only forwards the caller's ctx
		// when ContextTimeoutEnabled is set.
		ContextTimeoutEnabled: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", strings.Repeat("x", 32*1024*1024), 0)
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "LOADING") {
		t.Fatalf("Exec: %v, want the retained LOADING (the retry write failed, but the answered GET must keep its value and the SET its LOADING reply)", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil (it was answered by the first attempt)", got, err, "v")
	}
	if err := set.Err(); err == nil || !strings.Contains(err.Error(), "LOADING") {
		t.Fatalf("SET: %v, want LOADING (its first-attempt reply must survive the failed retry write)", err)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (the retry's write must have failed before re-executing it)", got)
	}
}

// writeFailRetryServer is a scripted RESP server for write-failure retry
// tests. The first connection answers the HELLO handshake and then closes
// immediately, so the pipeline batch's write fails mid-batch; later
// connections answer everything. It counts the commands it actually
// answers, so tests can assert what a retry resent.
type writeFailRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	incrs atomic.Int32
	sets  atomic.Int32
}

func newWriteFailRetryServer(t *testing.T) *writeFailRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &writeFailRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *writeFailRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	if first {
		// Answer the handshake, then close before the batch is
		// written: the client's write must fail, not its read.
		name, err := readStaleErrRESPCommand(rd)
		if err != nil || name != "hello" {
			return
		}
		_, _ = conn.Write([]byte("%0\r\n"))
		return
	}
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "incr":
			s.incrs.Add(1)
			_, _ = conn.Write([]byte(":1\r\n"))
		case "set":
			s.sets.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "get":
			_, _ = conn.Write([]byte("$1\r\n1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryNilEvalIsPreloaded: an EVAL that ran but returned nil
// (redis.Nil) still populated the script cache, so the retry must preload
// its source before the retried EVALSHA. The previous guard only preloaded
// an EVAL with a nil error, so the retried EVALSHA returned NOSCRIPT
// instead of executing. Regression test for the review finding on #4351.
func TestPipelineRetryNilEvalIsPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "if ARGV[1] then return 42 end"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	evalsha := pipe.EvalSha(ctx, sha, nil, "x")
	_, err := pipe.Exec(ctx)
	// The EVAL's own nil reply is the first command error, so Exec
	// reports redis.Nil; what must hold is that the retried EVALSHA
	// executed instead of returning NOSCRIPT.
	if err == nil || !errors.Is(err, Nil) {
		t.Fatalf("Exec: %v, want redis.Nil (the EVAL's own nil reply is the first command error)", err)
	}
	if err := eval.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EVAL: %v, want redis.Nil (the script ran and returned nil)", err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil (the answered EVAL ran and cached the script, so the retry must preload it)", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (the nil-returning EVAL populated the cache)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryRuntimeErrorEvalIsPreloaded: an EVAL that failed at
// runtime (a Lua error) still populated the script cache before failing,
// so the retry must preload its source before the retried EVALSHA. The
// previous guard only preloaded an EVAL with a nil error, so the retried
// EVALSHA returned NOSCRIPT instead of executing. Regression test for the
// review finding on #4351.
func TestPipelineRetryRuntimeErrorEvalIsPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return redis.call('BADCMD')"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "Error running script") {
		t.Fatalf("Exec: %v, want the EVAL's Lua runtime error (it is the first command error)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "Error running script") {
		t.Fatalf("EVAL: %v, want the Lua runtime error", err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil (the EVAL ran and cached the script before failing, so the retry must preload it)", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (a runtime-failing EVAL populated the cache)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryEvalRuntimeErrorStillCachesScript: a script that fails
// at runtime with "value is not an integer or out of range" ran and cached
// before failing — the error comes from redis.pcall('INCR') on a
// non-integer key, not from argument validation — so the retry must preload
// its source before the retried EVALSHA. Classifying the shared error
// prefix by text alone treats it as a pre-execution rejection and the
// EVALSHA fails NOSCRIPT on the restarted node. Regression test for the
// Codex P2 finding on #4351.
func TestPipelineRetryEvalRuntimeErrorStillCachesScript(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	sum := sha1.Sum([]byte(runtimeIntErrScript))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, runtimeIntErrScript, []string{"bad"})
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "value is not an integer") {
		t.Fatalf("Exec: %v, want the EVAL's runtime integer error (it is the first command error)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "value is not an integer") {
		t.Fatalf("EVAL: %v, want the runtime integer error", err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil (the EVAL ran and cached the script before failing, so the retry must preload it)", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (a runtime-failing EVAL populated the cache)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryEvalBadNumKeysIsNotPreloaded: the mirror of
// TestPipelineRetryEvalRuntimeErrorStillCachesScript. The same "value is
// not an integer" text from argument validation of a non-integer numkeys
// is a pre-execution rejection — the script never ran, so it cached
// nothing — and the retry must not preload it: the retried EVALSHA
// returns NOSCRIPT. The two cases differ only in the numkeys argument.
func TestPipelineRetryEvalBadNumKeysIsNotPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Do(ctx, "eval", src, "bad", 0)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "value is not an integer") {
		t.Fatalf("Exec: %v, want the rejected EVAL's error (the first failed command wins; the retry must not turn the rejection into a successful SCRIPT LOAD)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "value is not an integer") {
		t.Fatalf("EVAL: %v, want the non-integer numkeys rejection", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to reissue)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryResp3AttributesDoNotHideEvalRejection: a raw EVAL the
// server rejected before execution must not count as cached just because
// a RESP3 attribute block decorates its error reply: ReadRawReply keeps
// the block in the raw reply bytes, so a classifier that reads val[0]
// sees '|' instead of the error and preloads a script that never ran —
// the retried EVALSHA then executes a write the original batch refused.
// A raw command's error frame stays in Val() by contract, so the
// rejection is visible only through the raw-bytes decode. Regression
// test for the Codex P2 finding on #4351.
func TestPipelineRetryResp3AttributesDoNotHideEvalRejection(t *testing.T) {
	server := newScriptRetryServer(t)
	server.attrEvalRejectionOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := NewRawCmd(ctx, "eval", src, "bad", 0)
	if err := pipe.BatchProcess(ctx, eval); err != nil {
		t.Fatalf("BatchProcess: %v", err)
	}
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the retried EVALSHA must not execute a script the rejected raw EVAL never cached)", err)
	}
	if got := string(eval.Val()); !strings.Contains(got, "|1\r\n") || !strings.Contains(got, "value is not an integer") {
		t.Fatalf("raw EVAL Val() = %q, want it to keep the attribute-decorated rejection", got)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("raw EVAL Err(): %v, want nil (a raw command's error frame stays in Val() by contract)", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to reissue)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryEmptyEvalSourceIsPreloaded: an empty Lua source is still
// a script the server ran and cached, so a retried EVALSHA must find it
// after the retry preloads it — judging presence by the source's contents
// instead of its representation drops the preload and the EVALSHA fails
// NOSCRIPT instead of returning the script's nil. Regression test for the
// Codex P2 finding on #4351.
func TestPipelineRetryEmptyEvalSourceIsPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, "", nil)
	evalsha := pipe.EvalSha(ctx, emptyScriptSHA, nil)
	_, err := pipe.Exec(ctx)
	// The EVAL's own nil reply is the first command error, so Exec
	// reports redis.Nil; what must hold is that the retried EVALSHA
	// executed instead of returning NOSCRIPT.
	if err == nil || !errors.Is(err, Nil) {
		t.Fatalf("Exec: %v, want redis.Nil (the empty EVAL's own nil reply is the first command error)", err)
	}
	if err := eval.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EVAL: %v, want redis.Nil (the empty script ran and returned nil)", err)
	}
	if err := evalsha.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EVALSHA: %v, want redis.Nil (the empty script is cached, so the retry must preload it)", err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (the empty EVAL populated the cache)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryPreloadFailureDoesNotFailBatch: when the server is still
// running, the script an answered EVAL cached is still there, so a
// synthetic SCRIPT LOAD that fails (an ACL user without SCRIPT LOAD
// permission) must not fail a batch whose own commands all succeed. The
// retry still attempts the preload; only its failure is ignored when the
// suffix it was built for succeeds. Regression test for the review finding
// on #4351.
func TestPipelineRetryPreloadFailureDoesNotFailBatch(t *testing.T) {
	server := newScriptRetryServer(t)
	server.denyLoad = true
	server.persistCache = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the script is still cached on the live server, so the failed synthetic SCRIPT LOAD must not fail a batch whose own commands succeeded)", err)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVAL = %v, %v; want 1, nil", got, err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil (it reads the cache the live server still holds)", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (the retry still attempts the preload; only its failure is ignored)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryConnStateReissueDoesNotReplayAnsweredWrites: a pipeline
// whose answered prefix established connection-local state must not replay
// its answered writes on retry. SELECT is re-established on the retry
// connection by reissuing a fresh copy, and only the unanswered suffix
// runs. Regression test for the review finding on #4351.
func TestPipelineRetryConnStateReissueDoesNotReplayAnsweredWrites(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	incr := pipe.Incr(ctx, "counter")
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be replayed when the retry only re-establishes the SELECT)", got)
	}
	if got := server.selects.Load(); got != 2 {
		t.Fatalf("server received %d SELECT commands, want 2 (the answered SELECT must be reissued on the retry connection)", got)
	}
	if got := server.setDB.Load(); got != 2 {
		t.Fatalf("SET ran on DB %d, want 2 (the retried SET must run on the re-established database)", got)
	}
}

// TestPipelineRetryWriteFailureRetiresStaleReusedCommand: a reused INCR
// that entered with a stale io.EOF must still be retried when THIS
// attempt's write fails on it. The write failure stamps a fresh transport
// error via setCmdsTransportErr; the attempt must retire the stale marker
// the way the read path already does, so the retry starts at the INCR
// instead of trimming it out of the batch and leaving it with the fresh
// error after a healthy reconnection. Regression test for the review
// finding on #4351.
func TestPipelineRetryWriteFailureRetiresStaleReusedCommand(t *testing.T) {
	shared := seedStaleEOFIncr(t)

	server := newWriteFailRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	value := strings.Repeat("x", 256*1024)
	ctx := context.Background()
	pipe := client.Pipeline()
	_ = pipe.BatchProcess(ctx, shared)
	var sets []*StatusCmd
	for i := 0; i < 128; i++ {
		sets = append(sets, pipe.Set(ctx, fmt.Sprintf("k%d", i), value, 0))
	}
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must resend the reused INCR whose stale EOF the failed write replaced)", err)
	}
	if got, err := shared.Result(); err != nil || got != int64(1) {
		t.Fatalf("reused INCR = %v, %v; want 1, nil (it must be retried, not trimmed out of the batch)", got, err)
	}
	for i, set := range sets {
		if err := set.Err(); err != nil {
			t.Fatalf("SET[%d]: %v, want nil", i, err)
		}
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server answered %d INCR commands, want 1 (the reused INCR must be dispatched on the retry connection)", got)
	}
	if got := server.sets.Load(); got != 128 {
		t.Fatalf("server answered %d SET commands, want 128", got)
	}
}

// TestPipelineRetryPreloadFailureSurfacesSuffixError: the mirror of
// TestPipelineRetryPreloadFailureDoesNotFailBatch. When the server did
// restart, the script is gone, so the retried EVALSHA fails NOSCRIPT —
// and that suffix error, not the synthetic LOAD's NOPERM, is what Exec
// must report. A failed preload is ignored only when the suffix it was
// built for succeeds. Regression test for the review finding on #4351.
func TestPipelineRetryPreloadFailureSurfacesSuffixError(t *testing.T) {
	server := newScriptRetryServer(t)
	server.denyLoad = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, nil)
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the restarted server lost the script, so the retried EVALSHA fails; the LOAD's NOPERM must not mask it)", err)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVAL = %v, %v; want 1, nil", got, err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT", err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (the retry still attempts the preload)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryFailedConnStateReissueFailsBatch: a reissued
// connection-state command is required, not best-effort. When the
// reissued SELECT fails (a database index the server rejects), the suffix
// must not silently run on the wrong database and report success: Exec
// fails with the re-establishment error. Regression test for the review
// finding on #4351.
func TestPipelineRetryFailedConnStateReissueFailsBatch(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 99)
	incr := pipe.Incr(ctx, "counter")
	set := pipe.Set(ctx, "k", "v", 0)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "DB index is out of range") {
		t.Fatalf("Exec: %v, want the DB-index error (the reissued SELECT failed, so the batch must fail loudly instead of running the SET on the wrong database)", err)
	}
	if err := sel.Err(); err == nil || !strings.Contains(err.Error(), "DB index is out of range") {
		t.Fatalf("SELECT: %v, want the DB-index error", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil (it was answered by the first attempt and must not be replayed)", got, err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (it ran on the default database; the batch failure is reported through Exec, not the SET)", err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be replayed)", got)
	}
	if got := server.selects.Load(); got != 2 {
		t.Fatalf("server received %d SELECT commands, want 2 (the answered SELECT must be reissued on the retry connection)", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (the reissued SELECT failed, so the retried SET could only run on the default database — and Exec fails loudly for it)", got)
	}
}

// TestPipelineRetryReportsLaterConnStateReissueFailure: the mirror of
// TestPipelineRetryFailedConnStateReissueFailsBatch where the FIRST
// reissued SELECT succeeds. The read loop reports only the first
// command's error, so when the replacement server rejects a LATER
// reissued SELECT, the batch must still fail: Exec must not report
// success while the suffix ran on the wrong database. Regression test
// for the review finding on #4351.
func TestPipelineRetryReportsLaterConnStateReissueFailure(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	server.secondMaxDB = 0
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel0 := pipe.Select(ctx, 0)
	sel2 := pipe.Select(ctx, 2)
	set := pipe.Set(ctx, "k", "v", 0)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "DB index is out of range") {
		t.Fatalf("Exec: %v, want the DB-index error (the reissued SELECT 2 failed on the replacement server, so the batch must fail loudly instead of reporting success with the SET on the wrong database)", err)
	}
	if err := sel0.Err(); err != nil {
		t.Fatalf("SELECT 0: %v, want nil (it was answered by the first attempt)", err)
	}
	if err := sel2.Err(); err != nil {
		t.Fatalf("SELECT 2: %v, want nil (it was answered by the first attempt; the failure belongs to the reissued copy, not the original)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (it ran on the default database; the batch failure is reported through Exec, not the SET)", err)
	}
	if got := server.selects.Load(); got != 4 {
		t.Fatalf("server received %d SELECT commands, want 4 (two answered, two reissued)", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (the reissued SELECT 2 failed, so the retried SET could only run on the default database — and Exec fails loudly for it)", got)
	}
}

// helloRetryServer is a scripted RESP server for the HELLO reissue test.
// The first connection answers the pipeline HELLO (a RESP3 map, as a
// real server would) and then drops before the GET; later connections
// answer everything.
type helloRetryServer struct {
	ln     net.Listener
	conns  atomic.Int32
	hellos atomic.Int32
	gets   atomic.Int32
}

func newHelloRetryServer(t *testing.T) *helloRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &helloRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *helloRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			s.hellos.Add(1)
			_, _ = conn.Write([]byte("%0\r\n"))
		case "get":
			s.gets.Add(1)
			if first {
				// Drop before replying: the GET must be retried, and
				// the retry must reissue the HELLO ahead of it.
				return
			}
			_, _ = conn.Write([]byte("$1\r\nv\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryReissuesHelloWithMapReply: a pipeline HELLO answered
// on the first attempt must be reissued on the retry with the same reply
// decoder. HELLO returns a map (RESP3) or an array (RESP2), which a
// fresh StringCmd cannot read, so reissuing it as one fails every retry
// attempt and the GET reply is never read. Regression test for the
// review finding on #4351.
func TestPipelineRetryReissuesHelloWithMapReply(t *testing.T) {
	server := newHelloRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	hello := pipe.Hello(ctx, 2, "", "", "")
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must reissue the HELLO with its map decoder, then read the GET)", err)
	}
	if err := hello.Err(); err != nil {
		t.Fatalf("HELLO: %v, want nil", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want \"v\", nil (the retried GET must be read after the reissued HELLO)", got, err)
	}
	if got := server.hellos.Load(); got < 2 {
		t.Fatalf("server received %d HELLO commands, want at least 2 (the answered HELLO must be reissued on the retry connection)", got)
	}
	if got := server.gets.Load(); got != 2 {
		t.Fatalf("server received %d GET commands, want 2 (one dropped, one answered)", got)
	}
}

// TestPipelineRetryPreservesSessionAndScriptPrerequisiteOrder: when the
// answered prefix mixes session commands and script prerequisites, the
// retry must replay them in the original prefix order. Grouping all
// session reissues before all script preloads runs the SCRIPT LOAD as
// the wrong user (AUTH runner) when only the loader may load scripts,
// so the LOAD gets NOPERM and the retried EVALSHA fails NOSCRIPT.
// Regression test for the review finding on #4351.
func TestPipelineRetryPreservesSessionAndScriptPrerequisiteOrder(t *testing.T) {
	server := newScriptRetryServer(t)
	server.dropBefore = "evalsha"
	server.loadUser = "loader"
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	authLoader := pipe.Do(ctx, "auth", "loader", "pass")
	load := pipe.ScriptLoad(ctx, src)
	authRunner := pipe.Do(ctx, "auth", "runner", "pass")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must replay AUTH loader, SCRIPT LOAD, AUTH runner in order, so the LOAD runs as loader and the EVALSHA finds the script)", err)
	}
	if err := authLoader.Err(); err != nil {
		t.Fatalf("AUTH loader: %v, want nil", err)
	}
	if got, err := load.Result(); err != nil || got != sha {
		t.Fatalf("SCRIPT LOAD = %q, %v; want %q, nil", got, err, sha)
	}
	if err := authRunner.Err(); err != nil {
		t.Fatalf("AUTH runner: %v, want nil", err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil", got, err)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (one per attempt)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0 (the LOAD must run as loader, before the runner AUTH)", got)
	}
}

// oneShotRetryServer is a scripted RESP server for one-shot
// connection-state commands (CLIENT CACHING, ASKING). The first
// connection answers the one-shot command and its follower, then drops
// before the next command's reply; later connections answer everything.
// It records the command sequence per connection so tests can assert
// which command a reissued one-shot flag ended up attached to.
type oneShotRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	incrs atomic.Int32
	mu    sync.Mutex
	seqs  map[int32][]string
}

func newOneShotRetryServer(t *testing.T) *oneShotRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &oneShotRetryServer{ln: ln, seqs: make(map[int32][]string)}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *oneShotRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	answered := 0
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		s.mu.Lock()
		s.seqs[n] = append(s.seqs[n], name)
		s.mu.Unlock()
		isPipelineCmd := name == "client" || name == "asking" || name == "get" || name == "incr"
		if n == 1 && answered >= 2 && isPipelineCmd {
			// Drop the connection mid-pipeline: this command and the
			// ones after it get no reply on this connection.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "get":
			reply = "$1\r\n1\r\n"
		case "incr":
			s.incrs.Add(1)
			reply = ":1\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
		if isPipelineCmd {
			answered++
		}
	}
}

func (s *oneShotRetryServer) sequence(n int32) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seqs[n]...)
}

// TestPipelineRetryDoesNotReissueOneShotClientCaching: CLIENT CACHING
// applies only to the next command, so reissuing an answered one ahead
// of a retry suffix attaches the flag to a later command instead of its
// original follower. Here the first connection answers CLIENT CACHING
// YES and GET k, then drops before INCR's reply: the flag was consumed
// by the answered GET, so the retry is just the unanswered INCR — the
// flag is not re-sent and the answered GET is not re-run. Regression
// test for the review finding on #4351.
func TestPipelineRetryDoesNotReissueOneShotClientCaching(t *testing.T) {
	server := newOneShotRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	caching := pipe.Do(ctx, "client", "caching", "yes")
	get := pipe.Get(ctx, "k")
	incr := pipe.Incr(ctx, "counter")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := caching.Err(); err != nil {
		t.Fatalf("CLIENT CACHING: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "1" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "1")
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1", got)
	}
	seq := server.sequence(2)
	// The retry must not re-send the consumed flag, and must run the
	// unanswered INCR once.
	for _, name := range seq {
		if name == "client" {
			t.Fatalf("second connection ran %v, want no CLIENT CACHING (the answered GET already consumed it)", seq)
		}
	}
	found := false
	for _, name := range seq {
		if name == "incr" {
			found = true
		}
	}
	if !found {
		t.Fatalf("second connection ran %v, want the unanswered INCR", seq)
	}
}

// TestPipelineRetryDoesNotReissueOneShotAsking: ASKING lasts for one
// command only, so the same reissue fault applies: an answered ASKING
// reissued ahead of a retry suffix would steer the first suffix
// command instead of the GET it was issued for. Here the flag was
// consumed by the answered GET, so the retry is just the unanswered
// INCR — the flag is not re-sent and the answered GET is not re-run.
// Same shape as the CLIENT CACHING test above. Regression test for
// the review finding on #4351.
func TestPipelineRetryDoesNotReissueOneShotAsking(t *testing.T) {
	server := newOneShotRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	asking := pipe.Do(ctx, "asking")
	get := pipe.Get(ctx, "k")
	incr := pipe.Incr(ctx, "counter")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := asking.Err(); err != nil {
		t.Fatalf("ASKING: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "1" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "1")
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1", got)
	}
	seq := server.sequence(2)
	// The retry must not re-send the consumed flag, and must run the
	// unanswered INCR once.
	for _, name := range seq {
		if name == "asking" {
			t.Fatalf("second connection ran %v, want no ASKING (the answered GET already consumed it)", seq)
		}
	}
	found := false
	for _, name := range seq {
		if name == "incr" {
			found = true
		}
	}
	if !found {
		t.Fatalf("second connection ran %v, want the unanswered INCR", seq)
	}
}

// TestPipelineRetryAskingKeptWhenRewindingToTx: an ASKING whose follower
// is a MULTI whose frame is still open was consumed only in the sense
// that the MULTI ran — the flag stays in effect for the whole
// transaction, so when the retry rewinds to the MULTI it must reissue
// the ASKING immediately before it. If the retry drops the flag, the
// replacement connection refuses the transaction's write with MOVED.
// Regression test for the review finding on #4351 (ASKING lost when the
// boundary rewinds to MULTI).
func TestPipelineRetryAskingKeptWhenRewindingToTx(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.requireAskingBeforeMulti = true
	// Answer ASKING and MULTI, then drop before SET's reply: the
	// boundary rewinds to the open MULTI, and the ASKING must be
	// reissued ahead of it.
	server.dropAfter = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	asking := pipe.Do(ctx, "asking")
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "k", "v", 0)
	exec := pipe.Do(ctx, "exec")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must reissue ASKING ahead of the rewound MULTI, or the SET is refused MOVED)", err)
	}
	if err := asking.Err(); err != nil {
		t.Fatalf("ASKING: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	// The ASKING ran once on each connection: the original and the
	// reissued one ahead of the rewound MULTI.
	if got := server.askings.Load(); got != 2 {
		t.Fatalf("server received %d ASKING commands, want 2 (the flag must be reissued ahead of the rewound MULTI)", got)
	}
	// The transaction committed exactly once, on the retry.
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("EXEC committed %d time(s), want 1", got)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (the SET runs once, on the retry; the first connection dropped before reading it)", got)
	}
}

// oneShotPrefixRetryServer is a scripted RESP server for a one-shot
// flag answered in the prefix: the first connection answers INCR and
// ASKING, then drops before SET's reply; later connections answer
// everything. It records the command sequence per connection so tests
// can assert the retry reissued the flag immediately before its
// follower instead of replaying the whole batch.
type oneShotPrefixRetryServer struct {
	ln      net.Listener
	conns   atomic.Int32
	incrs   atomic.Int32
	askings atomic.Int32
	mu      sync.Mutex
	seqs    map[int32][]string
}

func newOneShotPrefixRetryServer(t *testing.T) *oneShotPrefixRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &oneShotPrefixRetryServer{ln: ln, seqs: make(map[int32][]string)}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *oneShotPrefixRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	answered := 0
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		s.mu.Lock()
		s.seqs[n] = append(s.seqs[n], name)
		s.mu.Unlock()
		isPipelineCmd := name == "asking" || name == "incr" || name == "set"
		if n == 1 && answered >= 2 && isPipelineCmd {
			// Drop the connection mid-pipeline: this command and the
			// ones after it get no reply on this connection.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "incr":
			s.incrs.Add(1)
			reply = ":1\r\n"
		case "asking":
			s.askings.Add(1)
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
		if isPipelineCmd {
			answered++
		}
	}
}

func (s *oneShotPrefixRetryServer) sequence(n int32) []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.seqs[n]...)
}

// TestPipelineRetryOneShotPrefixDoesNotRerunAnsweredWrite: an answered
// ASKING in the prefix must not force the whole batch: its follower is
// the boundary command, so the flag is reissued immediately before it
// on the retry and the answered INCR before it is not re-run. Without
// the fix the retry replays the whole batch and the INCR runs twice
// while Exec reports success. Regression test for the Cursor Medium
// review finding on #4351.
func TestPipelineRetryOneShotPrefixDoesNotRerunAnsweredWrite(t *testing.T) {
	server := newOneShotPrefixRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	incr := pipe.Incr(ctx, "counter")
	asking := pipe.Do(ctx, "asking")
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must reissue ASKING before SET, not replay the answered INCR)", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := asking.Err(); err != nil {
		t.Fatalf("ASKING: %v, want nil", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil", err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR before the one-shot flag must not be re-run)", got)
	}
	if got := server.askings.Load(); got != 2 {
		t.Fatalf("server received %d ASKING commands, want 2 (one per attempt: the flag is reissued before its follower)", got)
	}
	seq := server.sequence(2)
	// The retry connection must run ASKING immediately before SET,
	// and no INCR: the whole batch was not replayed.
	idx := -1
	for i, name := range seq {
		if name == "asking" {
			idx = i
		}
		if name == "incr" {
			t.Fatalf("second connection ran %v, want no INCR (the answered INCR must not be re-run)", seq)
		}
	}
	if idx < 0 || idx+1 >= len(seq) || seq[idx+1] != "set" {
		t.Fatalf("second connection ran %v, want ASKING immediately before SET", seq)
	}
}

// optionalPreloadRetryServer is a scripted RESP server for the optional
// preload retry test. The first connection answers EVAL and drops before
// INCR's reply; the second answers the retry's SCRIPT LOAD with LOADING,
// answers INCR, and drops before EVALSHA's reply; later connections
// answer everything. It counts INCRs so a retry that re-runs the
// answered INCR shows up as a double increment.
type optionalPreloadRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	incrs atomic.Int32
	loads atomic.Int32
}

func newOptionalPreloadRetryServer(t *testing.T) *optionalPreloadRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &optionalPreloadRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *optionalPreloadRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "eval":
			_, _ = conn.Write([]byte(":1\r\n"))
		case "incr":
			if n == 1 {
				// Drop before replying: the INCR must be retried
				// with a script preload ahead of it.
				return
			}
			s.incrs.Add(1)
			_, _ = conn.Write([]byte(":1\r\n"))
		case "script":
			s.loads.Add(1)
			if n == 2 {
				// The dataset is still loading on the retry: a
				// retryable error on the best-effort preload.
				_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
				continue
			}
			// SCRIPT LOAD answers the script SHA.
			sha := ""
			if len(args) > 1 {
				sum := sha1.Sum([]byte(args[1]))
				sha = hex.EncodeToString(sum[:])
			}
			_, _ = conn.Write([]byte("$" + strconv.Itoa(len(sha)) + "\r\n" + sha + "\r\n"))
		case "evalsha":
			if n == 2 {
				// Drop before replying: the EVALSHA must be retried
				// without re-running the answered INCR.
				return
			}
			_, _ = conn.Write([]byte(":1\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryIgnoresOptionalPreloadErrorForSuffix: when a retry's
// best-effort SCRIPT LOAD fails with a retryable error, that failure
// must not start the retry suffix: the LOAD is not an original command,
// and starting there re-runs the answered originals behind it. Here the
// second attempt's LOAD answers LOADING, INCR answers, and the
// connection drops before EVALSHA's reply: without the fix the next
// retry resends [LOAD, INCR, EVALSHA] and the counter increments twice
// while Exec reports success. Regression test for the review finding on
// #4351.
func TestPipelineRetryIgnoresOptionalPreloadErrorForSuffix(t *testing.T) {
	server := newOptionalPreloadRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	src := "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, []string{"k"})
	incr := pipe.Incr(ctx, "counter")
	evalsha := pipe.EvalSha(ctx, sha, []string{"k"})
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := evalsha.Err(); err != nil {
		t.Fatalf("EVALSHA: %v", err)
	}
	if got := server.conns.Load(); got != 3 {
		t.Fatalf("server saw %d connections, want 3", got)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (one per retry attempt)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be re-run when only the optional preload failed)", got)
	}
}

// lateRestoreRetryServer is a scripted RESP server for the failed
// restoration connection-retirement test. The first connection answers
// SELECT and drops before SET a's reply; the second rejects the
// synthetic SELECT, answers SET a, and answers SET b only after a delay
// past the client's read timeout, so the late reply is still
// outstanding when the attempt returns the restoration error. A third
// connection would accept everything.
type lateRestoreRetryServer struct {
	ln         net.Listener
	conns      atomic.Int32
	setsA      atomic.Int32
	selectErrs atomic.Int32
}

func newLateRestoreRetryServer(t *testing.T) *lateRestoreRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &lateRestoreRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *lateRestoreRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "ping":
			_, _ = conn.Write([]byte("+PONG\r\n"))
		case "select":
			if n == 2 {
				// The replacement server has no DB 2: the
				// required re-establishment fails, so the SETs
				// that follow run in DB 0.
				s.selectErrs.Add(1)
				_, _ = conn.Write([]byte("-ERR DB index is out of range\r\n"))
				continue
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "set":
			if len(args) > 0 && args[0] == "a" {
				s.setsA.Add(1)
			}
			if n == 1 {
				// Drop before replying: the SET must be retried.
				return
			}
			if n == 2 && len(args) > 0 && args[0] == "b" {
				// Answer late, past the client's read timeout:
				// the reply is still outstanding when the
				// attempt reports the restoration failure.
				time.Sleep(500 * time.Millisecond)
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

func newLateRestoreRetryClient(t *testing.T, addr string) *Client {
	t.Helper()
	client := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     100 * time.Millisecond,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestPipelineRetryRetiresConnAfterFailedRequiredReissue: when a retry's
// required synthetic SELECT fails and the read loop then dies partway
// with a transport error, the attempt reports the restoration error —
// but the connection may still hold unread replies, so it must be
// removed, not returned to the pool for the next pipeline to reuse.
// Here the second connection's SET b reply arrives after the client's
// read timeout: without the fix the connection is Put back with the
// Redis error and the next pipeline reuses it, consuming the late +OK
// as its PING reply instead of PONG. Regression test for the review
// finding on #4351.
func TestPipelineRetryRetiresConnAfterFailedRequiredReissue(t *testing.T) {
	server := newLateRestoreRetryServer(t)
	client := newLateRestoreRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	setA := pipe.Set(ctx, "a", "v", 0)
	setB := pipe.Set(ctx, "b", "v", 0)
	if _, err := pipe.Exec(ctx); err == nil {
		t.Fatalf("Exec: nil, want an error (the failed SELECT 2 restoration must fail the batch)")
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (it was answered OK on the first connection)", err)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2", got)
	}
	if got := server.selectErrs.Load(); got != 1 {
		t.Fatalf("server rejected SELECT %d time(s), want 1", got)
	}
	if got := server.setsA.Load(); got != 2 {
		t.Fatalf("server received %d SET a commands, want 2", got)
	}
	if err := setA.Err(); err != nil {
		t.Fatalf("SET a: %v, want nil (it was answered OK in DB 0)", err)
	}
	if err := setB.Err(); err == nil {
		t.Fatalf("SET b: nil, want an error (its reply was never read)")
	}
	// The second connection may still hold SET b's late reply when it
	// is released with the restoration error: it must be removed, not
	// returned to the pipeline pool for the next pipeline to reuse. The
	// next pipeline runs before the late reply arrives (so the pool
	// health check cannot see it yet) and reads past it: without the
	// fix it consumes the late +OK as the PING reply instead of PONG.
	client.opt.MaxRetries = 0
	client.opt.ReadTimeout = 2 * time.Second
	pipe2 := client.Pipeline()
	pong := pipe2.Ping(ctx)
	if _, err := pipe2.Exec(ctx); err != nil {
		t.Fatalf("second pipeline Exec: %v, want nil (it must run on a fresh connection)", err)
	}
	if got := pong.Val(); got != "PONG" {
		t.Fatalf("PING = %q, want %q (the late reply of the failed attempt must not be consumed as this pipeline's reply)", got, "PONG")
	}
	if got := server.conns.Load(); got != 3 {
		t.Fatalf("server saw %d connections after the next pipeline, want 3 (the connection released with replies outstanding must not be reused)", got)
	}
}

// TestPipelineRetryRawRejectedEvalIsNotPreloaded: a raw EVAL the server
// rejected (negative key count) never ran, so it cached nothing — the same
// hole as TestPipelineRetryRejectedEvalIsNotPreloaded, but through a RawCmd.
// RawCmd.readReply preserves the error frame verbatim in Val() and returns
// nil, so the retry machinery must classify the raw response before
// concluding the EVAL populated the cache: without that the retry
// synthesizes a successful SCRIPT LOAD and the retried EVALSHA executes a
// write script the original pipeline refused with NOSCRIPT. Regression test
// for the Codex review finding on #4351.
func TestPipelineRetryRawRejectedEvalIsNotPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := NewRawCmd(ctx, "eval", src, -1)
	if err := pipe.BatchProcess(ctx, eval); err != nil {
		t.Fatalf("BatchProcess: %v", err)
	}
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the retried EVALSHA must not execute a script the rejected raw EVAL never cached)", err)
	}
	if got := string(eval.Val()); !strings.Contains(got, "wrong number of keys") {
		t.Fatalf("raw EVAL Val() = %q, want it to keep the wrong-number-of-keys error frame", got)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("raw EVAL Err(): %v, want nil (a raw command's error frame stays in Val() by contract)", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to reissue)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryRawRequiredReissueErrorSurfaces: a raw session command
// reissued as a required synthetic must surface its failure. The first
// connection answers a raw SELECT 2 and drops before SET; the replacement
// server has no DB 2, so the cloned synthetic SELECT is rejected — but its
// RawCmd.readReply captures the error frame in Val() while leaving
// rawErr() nil, so both required-synthetic error checks accept the failed
// restoration, SET executes in DB 0, and Exec reports success. Regression
// test for the Codex review finding on #4351.
func TestPipelineRetryRawRequiredReissueErrorSurfaces(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.secondMaxDB = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := NewRawCmd(ctx, "SELECT", 2)
	set := NewStringCmd(ctx, "set", "k", "v")
	if err := pipe.BatchProcess(ctx, sel, set); err != nil {
		t.Fatalf("BatchProcess: %v", err)
	}
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "DB index is out of range") {
		t.Fatalf("Exec: %v, want the failed SELECT 2 restoration error (a rejected required reissue must fail the batch, not report success)", err)
	}
	if got := string(sel.Val()); !strings.Contains(got, "+OK") {
		t.Fatalf("raw SELECT Val() = %q, want the +OK it was answered with on the first connection", got)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the restoration error is not retryable)", got)
	}
	if got := server.selects.Load(); got != 2 {
		t.Fatalf("server received %d SELECT commands, want 2 (the answered SELECT must be reissued on the retry connection)", got)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (it ran after the failed restoration — the fix surfaces the error, it cannot undo the write)", got)
	}
}

// tryagainRetryServer is a scripted RESP server for the answered-tail
// retry test. The first connection answers SELECT and drops before MGET;
// the second answers SELECT, rejects MGET with TRYAGAIN (slot migration)
// and answers INCR; later connections answer everything. It records how
// many INCR and MGET commands were answered, so tests can assert a retry
// did not re-run commands the previous attempt already answered.
type tryagainRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	incrs atomic.Int32
	mgets atomic.Int32
}

func newTryagainRetryServer(t *testing.T) *tryagainRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &tryagainRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *tryagainRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	answered := 0
	mgetsThisConn := 0
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		isPipelineCmd := name == "select" || name == "mget" || name == "incr"
		if n == 1 && answered >= 1 && isPipelineCmd {
			// Drop the connection mid-pipeline: this command and the
			// ones after it get no reply on this connection.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "ping":
			reply = "+PONG\r\n"
		case "mget":
			s.mgets.Add(1)
			mgetsThisConn++
			if n == 2 && mgetsThisConn == 1 {
				// Slot migration on the first batch this
				// connection serves: the key moved mid-pipeline.
				// The connection stays healthy, so the retry
				// reuses it and the next MGET succeeds — the
				// TRYAGAIN must fire exactly once.
				reply = "-TRYAGAIN Multiple keys request during slot migration\r\n"
			} else {
				reply = "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
			}
		case "incr":
			s.incrs.Add(1)
			reply = ":1\r\n"
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
		if isPipelineCmd {
			answered++
		}
	}
}

// TestPipelineRetryOmitsAnsweredTailAfterRedisRejection: when a retryable
// Redis error on one command triggers another retry, commands after it
// that a previous attempt already answered must not be re-run: their
// replies were read in the same attempt. Here the second attempt receives
// [OK, TRYAGAIN, 1] for [SELECT, MGET, INCR] during slot migration; the
// retry after the TRYAGAIN must resend only the MGET (plus its SELECT
// prerequisite), not the INCR that already returned 1 — a healthy next
// attempt would otherwise increment the counter to 2 while Exec reports
// success. Regression test for the Codex review finding on #4351.
func TestPipelineRetryOmitsAnsweredTailAfterRedisRejection(t *testing.T) {
	server := newTryagainRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 0)
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must resend only the rejected MGET)", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the dropped first connection is replaced once; the healthy second one is reused for the retry)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be re-run after the TRYAGAIN)", got)
	}
}

// queuedRetryServer is a scripted RESP server for the answered-tail
// MULTI/EXEC retry tests. The first connection answers dropAfter pipeline
// commands and then drops mid-pipeline; the second answers MGET with
// TRYAGAIN once (slot migration), MULTI with OK, a command inside MULTI
// with +QUEUED, and — when dropExecConn names it — closes on EXEC without
// replying, so the transaction never commits on that connection; later
// connections answer everything, and EXEC returns one reply per queued
// command. It records how many MGET, INCR and EXEC commands it received,
// so tests can assert a retry did not drop a queued command or re-run one
// whose EXEC already committed.
type queuedRetryServer struct {
	ln           net.Listener
	conns        atomic.Int32
	mgets        atomic.Int32
	incrs        atomic.Int32
	sets         atomic.Int32
	execs        atomic.Int32
	dropAfter    int
	dropExecConn int32
}

func newQueuedRetryServer(t *testing.T) *queuedRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &queuedRetryServer{ln: ln, dropAfter: 1}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *queuedRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	inMulti := false
	queued := 0
	answered := 0
	mgetsThisConn := 0
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		isPipelineCmd := name == "select" || name == "mget" || name == "multi" || name == "incr" || name == "exec" || name == "set"
		if n == 1 && answered >= s.dropAfter && isPipelineCmd {
			// Drop mid-pipeline: this command and the ones after it
			// get no reply on this connection.
			return
		}
		if n == s.dropExecConn && name == "exec" {
			// Disconnect before EXEC runs: the queued commands never
			// execute on this connection.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "ping":
			reply = "+PONG\r\n"
		case "set":
			s.sets.Add(1)
			if inMulti {
				queued++
				reply = "+QUEUED\r\n"
			}
		case "mget":
			s.mgets.Add(1)
			mgetsThisConn++
			if n == 2 && mgetsThisConn == 1 {
				// Slot migration on the first batch this
				// connection serves: the TRYAGAIN must fire
				// exactly once.
				reply = "-TRYAGAIN Multiple keys request during slot migration\r\n"
			} else {
				reply = "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
			}
		case "multi":
			inMulti = true
		case "incr":
			s.incrs.Add(1)
			if inMulti {
				queued++
				reply = "+QUEUED\r\n"
			} else {
				reply = ":1\r\n"
			}
		case "exec":
			s.execs.Add(1)
			if inMulti {
				inMulti = false
				var b strings.Builder
				fmt.Fprintf(&b, "*%d\r\n", queued)
				for i := 0; i < queued; i++ {
					b.WriteString(":1\r\n")
				}
				reply = b.String()
				queued = 0
			} else {
				reply = "-ERR EXEC without MULTI\r\n"
			}
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
		if isPipelineCmd {
			answered++
		}
	}
}

// TestPipelineRetryKeepsQueuedCommandsAfterRedisRejection: a +QUEUED reply
// is not a definitive answer — the command runs only when its transaction's
// EXEC runs. Here the second attempt receives [TRYAGAIN, OK, QUEUED] for
// [MGET, MULTI, INCR] and drops before EXEC runs; the retry after the
// TRYAGAIN must resend the INCR with its MULTI/EXEC framing, or it would
// commit an empty transaction and report success while the counter never
// increments. Regression test for the Codex review finding on #4351.
func TestPipelineRetryKeepsQueuedCommandsAfterRedisRejection(t *testing.T) {
	server := newQueuedRetryServer(t)
	server.dropAfter = 0
	server.dropExecConn = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.Do(ctx, "mget", "{m}a", "{m}b")
	multi := pipe.Do(ctx, "multi")
	incr := pipe.Do(ctx, "incr", "counter")
	exec := pipe.Do(ctx, "exec")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-queue the QUEUED INCR and commit it)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if got, err := mget.Result(); err != nil {
		t.Fatalf("MGET: %v", err)
	} else if vals, ok := got.([]interface{}); !ok || len(vals) != 2 || vals[0] != "a" || vals[1] != "b" {
		t.Fatalf("MGET = %v, want [a b]", got)
	}
	if got, err := incr.Result(); err != nil || got != "QUEUED" {
		t.Fatalf("INCR = %v, %v; want QUEUED, nil (the retried INCR is queued again on the new connection)", got, err)
	}
	if got, err := exec.Result(); err != nil {
		t.Fatalf("EXEC: %v", err)
	} else if vals, ok := got.([]interface{}); !ok || len(vals) != 1 {
		t.Fatalf("EXEC = %v, want one result (the committed INCR); an empty array means the retry committed an empty transaction", got)
	}
	if got := server.conns.Load(); got != 3 {
		t.Fatalf("server saw %d connections, want 3 (the first connection drops before replying, the second drops before EXEC)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the QUEUED INCR must be re-queued on the retry)", got)
	}
	if got := server.execs.Load(); got != 1 {
		t.Fatalf("server received %d EXEC commands, want 1 (only the retry's EXEC runs; the second connection drops before its EXEC)", got)
	}
}

// TestPipelineRetryDropsQueuedCommandsAfterExecCommitted: the counterpart
// of the test above — when the transaction's EXEC already answered
// successfully on an attempt, the queued commands ran, so the retry after
// the TRYAGAIN must not re-queue them. Here the second connection commits
// the transaction (EXEC returns the INCR's result) and the retry runs an
// empty MULTI/EXEC on the same connection; re-sending the INCR would
// increment the counter twice.
func TestPipelineRetryDropsQueuedCommandsAfterExecCommitted(t *testing.T) {
	server := newQueuedRetryServer(t)
	server.dropAfter = 1
	server.dropExecConn = 0
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Do(ctx, "select", 0)
	mget := pipe.Do(ctx, "mget", "{m}a", "{m}b")
	multi := pipe.Do(ctx, "multi")
	incr := pipe.Do(ctx, "incr", "counter")
	exec := pipe.Do(ctx, "exec")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry runs an empty transaction after the committed one)", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != "QUEUED" {
		t.Fatalf("INCR = %v, %v; want QUEUED, nil (it ran under the committed EXEC on the second connection)", got, err)
	}
	if got, err := mget.Result(); err != nil {
		t.Fatalf("MGET: %v", err)
	} else if vals, ok := got.([]interface{}); !ok || len(vals) != 2 {
		t.Fatalf("MGET = %v, want [a b]", got)
	}
	if got, err := exec.Result(); err != nil {
		t.Fatalf("EXEC: %v", err)
	} else if vals, ok := got.([]interface{}); !ok || len(vals) != 0 {
		t.Fatalf("EXEC = %v, want an empty array (the retry's transaction holds nothing)", got)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the first drops after SELECT; the second stays healthy)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the INCR already ran under the committed EXEC)", got)
	}
	if got := server.execs.Load(); got != 2 {
		t.Fatalf("server received %d EXEC commands, want 2 (one committing, one empty on retry)", got)
	}
}

// tailFlushRetryServer is a scripted RESP server for the answered-tail
// script-prerequisite test. Each connection holds its own (initially
// empty) script cache: SCRIPT LOAD records the digest, SCRIPT FLUSH
// empties it, EVALSHA answers only when the digest was loaded on that
// same connection. The first connection answers the LOAD and then drops
// before MGET's reply; the second answers MGET with TRYAGAIN once,
// acknowledges the FLUSH, and drops before EVALSHA's reply; later
// connections answer everything.
type tailFlushRetryServer struct {
	ln        net.Listener
	conns     atomic.Int32
	loads     atomic.Int32
	flushes   atomic.Int32
	evalshas  atomic.Int32
	noscripts atomic.Int32
	mgets     atomic.Int32
}

func newTailFlushRetryServer(t *testing.T) *tailFlushRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &tailFlushRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *tailFlushRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	loaded := map[string]bool{}
	mgetsThisConn := 0
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		if n == 1 && name == "mget" {
			// Drop before replying: the LOAD was answered, the rest
			// of the batch must be retried.
			return
		}
		if n == 2 && name == "evalsha" {
			// Drop before replying: the FLUSH was acknowledged, the
			// EVALSHA must be retried.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "ping":
			reply = "+PONG\r\n"
		case "mget":
			s.mgets.Add(1)
			mgetsThisConn++
			if n == 2 && mgetsThisConn == 1 {
				reply = "-TRYAGAIN Multiple keys request during slot migration\r\n"
			} else {
				reply = "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
			}
		case "script":
			if len(args) >= 2 && strings.EqualFold(args[0], "load") {
				s.loads.Add(1)
				sum := sha1.Sum([]byte(args[1]))
				digest := hex.EncodeToString(sum[:])
				loaded[digest] = true
				reply = fmt.Sprintf("$%d\r\n%s\r\n", len(digest), digest)
			} else if len(args) >= 1 && strings.EqualFold(args[0], "flush") {
				s.flushes.Add(1)
				loaded = map[string]bool{}
				reply = "+OK\r\n"
			}
		case "evalsha":
			s.evalshas.Add(1)
			if len(args) >= 1 && loaded[args[0]] {
				reply = ":1\r\n"
			} else {
				s.noscripts.Add(1)
				reply = "-NOSCRIPT No matching script. Please use EVAL.\r\n"
			}
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

// TestPipelineRetryRebuildsScriptPrereqsFromAnsweredTail: the script
// prerequisites a retry rebuilds must come from the whole answered batch,
// not just the prefix before the retry boundary. Here the second attempt
// receives TRYAGAIN for MGET, acknowledges SCRIPT FLUSH, and drops before
// EVALSHA; the FLUSH sits after the boundary, so the tail filter drops it
// as answered — but the retry must still replay it after the LOAD it
// rebuilds, or the retried EVALSHA would execute a script the batch had
// already flushed. Regression test for the Codex review finding on #4351.
func TestPipelineRetryRebuildsScriptPrereqsFromAnsweredTail(t *testing.T) {
	server := newTailFlushRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	load := pipe.Do(ctx, "script", "load", src)
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	flush := pipe.Do(ctx, "script", "flush")
	evalsha := pipe.EvalSha(ctx, sha, []string{"counter"})
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the FLUSH answered after the TRYAGAIN removed the script, so the retried EVALSHA must not execute it)", err)
	}
	if err := load.Err(); err != nil {
		t.Fatalf("SCRIPT LOAD: %v", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if err := flush.Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT", err)
	}
	if got := server.conns.Load(); got != 3 {
		t.Fatalf("server saw %d connections, want 3 (the first drops before MGET, the second before EVALSHA)", got)
	}
	if got := server.loads.Load(); got != 3 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 3 (one per attempt: the original plus two rebuilt preloads)", got)
	}
	if got := server.flushes.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT FLUSH commands, want 2 (the answered FLUSH plus its rebuilt replay)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1 (only the retried EVALSHA, after the replayed FLUSH)", got)
	}
}

// scriptOrderRetryServer is a scripted RESP server for the answered-tail
// script-order test. Each connection holds its own (initially empty)
// script cache: EVAL runs the script and caches it, SCRIPT FLUSH empties
// it, EVALSHA answers only when the digest was loaded on that same
// connection. The first connection rejects the EVAL with READONLY,
// acknowledges the FLUSH, and drops before EVALSHA's reply; later
// connections answer everything.
type scriptOrderRetryServer struct {
	ln          net.Listener
	conns       atomic.Int32
	evals       atomic.Int32
	evalRuns    atomic.Int32
	evalshaRuns atomic.Int32
	noscripts   atomic.Int32
	flushes     atomic.Int32
}

func newScriptOrderRetryServer(t *testing.T) *scriptOrderRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &scriptOrderRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *scriptOrderRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	loaded := map[string]bool{}
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		if n == 1 && name == "eval" {
			// READONLY: the script never ran, so it is not cached.
			_, _ = conn.Write([]byte("-READONLY You can't write against a read only replica.\r\n"))
			continue
		}
		if n == 1 && name == "evalsha" {
			// Drop before replying: the FLUSH was acknowledged, the
			// EVALSHA must be retried.
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "ping":
			reply = "+PONG\r\n"
		case "eval":
			s.evals.Add(1)
			if len(args) >= 1 {
				sum := sha1.Sum([]byte(args[0]))
				loaded[hex.EncodeToString(sum[:])] = true
			}
			s.evalRuns.Add(1)
			reply = ":1\r\n"
		case "script":
			if len(args) >= 1 && strings.EqualFold(args[0], "flush") {
				s.flushes.Add(1)
				loaded = map[string]bool{}
				reply = "+OK\r\n"
			}
		case "evalsha":
			if len(args) >= 1 && loaded[args[0]] {
				s.evalshaRuns.Add(1)
				reply = ":1\r\n"
			} else {
				s.noscripts.Add(1)
				reply = "-NOSCRIPT No matching script. Please use EVAL.\r\n"
			}
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

// TestPipelineRetryKeepsAnsweredTailScriptOrder: the script effects of the
// answered tail must stay at their original positions relative to the
// retained commands, not move ahead of them all into a prefix. Here the
// first attempt rejects the EVAL with READONLY, acknowledges SCRIPT
// FLUSH, and drops before EVALSHA's reply: without the fix the retry
// replays the FLUSH ahead of the EVAL, so the EVAL repopulates the cache
// and the EVALSHA executes the write a second time while Exec reports
// success; in the original order the EVAL runs, the FLUSH empties the
// cache, and the EVALSHA answers NOSCRIPT. Regression test for the review
// finding on #4351.
func TestPipelineRetryKeepsAnsweredTailScriptOrder(t *testing.T) {
	server := newScriptOrderRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return redis.call('incr', 'counter')"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, src, []string{})
	flush := pipe.Do(ctx, "script", "flush")
	evalsha := pipe.EvalSha(ctx, sha, []string{})
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the FLUSH answered after the READONLY removed the script, so the retried EVALSHA must not execute it)", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v, want nil", err)
	}
	if err := flush.Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT", err)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2", got)
	}
	if got := server.evalRuns.Load(); got != 1 {
		t.Fatalf("server executed EVAL %d time(s), want 1 (only the retried EVAL, after the READONLY rejection)", got)
	}
	if got := server.evalshaRuns.Load(); got != 0 {
		t.Fatalf("server executed EVALSHA %d time(s), want 0 (the retried EVALSHA must answer NOSCRIPT, not run the script)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1 (only the retried EVALSHA, after the FLUSH)", got)
	}
}

// oneShotTailRetryServer is a scripted RESP server for the one-shot
// command in the retry tail test. The first connection answers MGET with
// TRYAGAIN, acknowledges ASKING, answers GET, and drops before SET's
// reply; later connections answer everything. It counts each command so
// a retry that re-sends a consumed one-shot flag shows up.
type oneShotTailRetryServer struct {
	ln      net.Listener
	conns   atomic.Int32
	mgets   atomic.Int32
	askings atomic.Int32
	gets    atomic.Int32
	sets    atomic.Int32
}

func newOneShotTailRetryServer(t *testing.T) *oneShotTailRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &oneShotTailRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *oneShotTailRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "mget":
			s.mgets.Add(1)
			if n == 1 {
				_, _ = conn.Write([]byte("-TRYAGAIN Multiple keys request during slot migration\r\n"))
			} else {
				_, _ = conn.Write([]byte("*2\r\n$1\r\na\r\n$1\r\nb\r\n"))
			}
		case "asking":
			s.askings.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		case "get":
			s.gets.Add(1)
			_, _ = conn.Write([]byte("$1\r\n1\r\n"))
		case "set":
			s.sets.Add(1)
			if n == 1 {
				// Drop before replying: the SET must be retried,
				// without the ASKING that the answered GET consumed.
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// oneShotTrailingRetryServer is a scripted RESP server for an
// unanswered one-shot flag at the end of the batch. The first
// connection answers MGET with TRYAGAIN, then drops before ASKING's
// reply, so the flag was never consumed by any follower; later
// connections answer everything. It counts each command so a retry
// that drops the unconsumed flag shows up.
type oneShotTrailingRetryServer struct {
	ln      net.Listener
	conns   atomic.Int32
	mgets   atomic.Int32
	askings atomic.Int32
}

func newOneShotTrailingRetryServer(t *testing.T) *oneShotTrailingRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &oneShotTrailingRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *oneShotTrailingRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "mget":
			s.mgets.Add(1)
			if n == 1 {
				_, _ = conn.Write([]byte("-TRYAGAIN Multiple keys request during slot migration\r\n"))
			} else {
				_, _ = conn.Write([]byte("*2\r\n$1\r\na\r\n$1\r\nb\r\n"))
			}
		case "asking":
			s.askings.Add(1)
			if n == 1 {
				// Drop before replying: the ASKING was never
				// answered, so no follower consumed it.
				return
			}
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryDropsConsumedOneShotAfterAnsweredTail: a one-shot
// connection-state command in the retry tail must go when the follower
// that consumed it is dropped as answered. Here the first attempt
// answers ASKING and GET and drops before SET's reply: without the fix
// the retry resends [MGET, ASKING, SET], so the ASKING lands on the SET
// instead of the GET it was issued for, and the SET could run under an
// authorization its own ordering would not have given it. Regression
// test for the review finding on #4351.
func TestPipelineRetryDropsConsumedOneShotAfterAnsweredTail(t *testing.T) {
	server := newOneShotTailRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	asking := pipe.Do(ctx, "asking")
	get := pipe.Get(ctx, "a")
	set := pipe.Set(ctx, "b", "v", 0)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if err := asking.Err(); err != nil {
		t.Fatalf("ASKING: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "1" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "1")
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
	if got := server.askings.Load(); got != 1 {
		t.Fatalf("server received %d ASKING commands, want 1 (the ASKING the answered GET consumed must not be re-sent ahead of the SET)", got)
	}
	if got := server.gets.Load(); got != 1 {
		t.Fatalf("server received %d GET commands, want 1 (the answered GET must not run again)", got)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the unanswered SET must be retried once)", got)
	}
}

// staleRawRetryServer is a scripted RESP server for the retained raw
// reply test. It answers a raw GET with LOADING, and never answers GET
// "block", so a command holding the pool's only connection blocks until
// the test ends.
type staleRawRetryServer struct {
	ln     net.Listener
	conns  atomic.Int32
	gets   atomic.Int32
	blocks atomic.Int32
	sets   atomic.Int32
	done   chan struct{}
}

func newStaleRawRetryServer(t *testing.T) *staleRawRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &staleRawRetryServer{ln: ln, done: make(chan struct{})}
	t.Cleanup(func() {
		close(s.done)
		_ = ln.Close()
	})
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *staleRawRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, args := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "get":
			s.gets.Add(1)
			if len(args) > 0 && args[0] == "block" {
				s.blocks.Add(1)
				<-s.done
				return
			}
			_, _ = conn.Write([]byte("-LOADING Redis is loading the dataset in memory\r\n"))
		case "set":
			s.sets.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// seedStaleLoadingRaw runs a raw GET once against the server, so the
// returned RawCmd keeps the -LOADING error frame in its reply bytes
// while rawErr() stays nil — the retained reply a later operation must
// not mistake for its own progress.
func seedStaleLoadingRaw(t *testing.T, addr string) *RawCmd {
	t.Helper()
	seeder := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = seeder.Close() })

	ctx := context.Background()
	raw := NewRawCmd(ctx, "get", "k")
	pipe := seeder.Pipeline()
	_ = pipe.BatchProcess(ctx, raw)
	_, _ = pipe.Exec(ctx)
	if len(raw.Val()) == 0 {
		t.Fatalf("seeding RawCmd: no reply bytes retained, want the -LOADING frame")
	}
	return raw
}

// TestPipelineRetryStaleRawReplyIsNotProgress: a retained raw reply is
// not this invocation's progress. Here a reused RawCmd still holds its
// previous -LOADING frame while a fresh SET follows it, and every
// connection acquisition hits a pool timeout: without the fix the retry
// narrows at the retained LOADING as though it were this attempt's
// boundary, drops the SET that was never dispatched, and the final
// stamping leaves the SET with a nil error while Exec reports the pool
// timeout. Regression test for the review finding on #4351.
func TestPipelineRetryStaleRawReplyIsNotProgress(t *testing.T) {
	server := newStaleRawRetryServer(t)
	raw := seedStaleLoadingRaw(t, server.ln.Addr().String())

	client := NewClient(&Options{
		Addr:             server.ln.Addr().String(),
		Protocol:         2,
		PoolSize:         1,
		PoolTimeout:      200 * time.Millisecond,
		PipelinePoolSize: -1, // pipelines run on the main pool, so the blocked GET below holds their only connection too
		MaxRetries:       3,
		MinRetryBackoff:  time.Millisecond,
		MaxRetryBackoff:  2 * time.Millisecond,
		DialTimeout:      5 * time.Second,
		ReadTimeout:      30 * time.Second,
		WriteTimeout:     5 * time.Second,
		DisableIdentity:  true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	// Hold the pool's only connection with a command that never
	// answers, so every acquisition for the pipeline times out.
	blockCtx, blockCancel := context.WithCancel(ctx)
	defer blockCancel()
	go func() { _ = client.Get(blockCtx, "block").Err() }()
	deadline := time.Now().Add(5 * time.Second)
	for server.blocks.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.blocks.Load() == 0 {
		t.Fatalf("blocking GET never reached the server")
	}

	pipe := client.Pipeline()
	_ = pipe.BatchProcess(ctx, raw)
	set := pipe.Set(ctx, "k", "v", 0)
	_, err := pipe.Exec(ctx)
	if err == nil {
		t.Fatalf("Exec: nil, want the pool timeout")
	}
	if err := set.Err(); err == nil {
		t.Fatalf("SET: nil, want the pool timeout (it was never dispatched, so it must carry the failure, not report success)")
	}
	if got := server.sets.Load(); got != 0 {
		t.Fatalf("server received %d SET commands, want 0 (no connection was ever acquired)", got)
	}
}

// committedTxRetryServer is a scripted RESP server for the committed
// transaction prefix test. The first MGET answers TRYAGAIN and later
// ones answer the values; every other command answers as a healthy
// server would, and MULTI/EXEC track a transaction so a re-run of the
// committed INCR shows up in the counters.
type committedTxRetryServer struct {
	ln      net.Listener
	conns   atomic.Int32
	selects atomic.Int32
	mults   atomic.Int32
	incrs   atomic.Int32
	execs   atomic.Int32
	mgets   atomic.Int32
}

func newCommittedTxRetryServer(t *testing.T) *committedTxRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &committedTxRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n)
		}
	}()
	return s
}

func (s *committedTxRetryServer) serve(conn net.Conn, n int32) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		reply := "+OK\r\n"
		switch name {
		case "hello":
			reply = "%0\r\n"
		case "select":
			s.selects.Add(1)
		case "multi":
			s.mults.Add(1)
		case "incr":
			s.incrs.Add(1)
			reply = "+QUEUED\r\n"
		case "exec":
			s.execs.Add(1)
			reply = "*1\r\n:1\r\n"
		case "mget":
			s.mgets.Add(1)
			if s.mgets.Load() == 1 {
				reply = "-TRYAGAIN Multiple keys request during slot migration\r\n"
			} else {
				reply = "*2\r\n$1\r\na\r\n$1\r\nb\r\n"
			}
		}
		if _, err := conn.Write([]byte(reply)); err != nil {
			return
		}
	}
}

// TestPipelineRetryDoesNotRerunCommittedTransaction: an answered MULTI
// or EXEC in the prefix must not force a whole-batch retry when the
// transaction already committed. Here the first attempt commits the
// transaction and then the MGET answers TRYAGAIN: without the fix the
// prefix guard returns the whole batch, so the retry re-runs the queued
// INCR and it applies twice while Exec reports success. Regression test
// for the review finding on #4351.
func TestPipelineRetryDoesNotRerunCommittedTransaction(t *testing.T) {
	server := newCommittedTxRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Do(ctx, "select", 0)
	multi := pipe.Do(ctx, "multi")
	incr := pipe.Do(ctx, "incr", "counter")
	exec := pipe.Do(ctx, "exec")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must run only the MGET, not the committed transaction)", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != "QUEUED" {
		t.Fatalf("INCR = %v, %v; want QUEUED, nil", got, err)
	}
	if got, err := exec.Result(); err != nil {
		t.Fatalf("EXEC: %v", err)
	} else if vals, ok := got.([]interface{}); !ok || len(vals) != 1 {
		t.Fatalf("EXEC = %v, want one result (the committed INCR)", got)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the committed INCR must not run again)", got)
	}
	if got := server.execs.Load(); got != 1 {
		t.Fatalf("server received %d EXEC commands, want 1 (the committed transaction must not run again)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
}

// TestPipelineRetryDiscardsQueuedSelectOnDiscard: a SELECT queued inside a
// MULTI/EXEC transaction that DISCARD drops never executed, so its session
// effect never happened. The retry must not rebuild it as a prerequisite:
// reissuing SELECT 2 ahead of the SET would write the key to DB 2 instead
// of the original DB 0 while reporting success. Regression test for the
// Codex P1 review finding on #4351.
func TestPipelineRetryDiscardsQueuedSelectOnDiscard(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 3
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	sel := pipe.Do(ctx, "select", 2)
	discard := pipe.Do(ctx, "discard")
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must run only the SET, on the original DB 0)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if err := discard.Err(); err != nil {
		t.Fatalf("DISCARD: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.selects.Load(); got != 1 {
		t.Fatalf("server received %d SELECT commands, want 1 (the discarded queued SELECT must not be reissued as a session prerequisite)", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (DISCARD prevented the queued SELECT 2 from executing, so the key belongs in the original database)", got)
	}
}

// TestPipelineRetryQueuedEvalRejectionIsNotPreloaded: an EVAL queued inside
// a MULTI/EXEC transaction holds +QUEUED with a nil error, but its negative
// key count rejection lives in EXEC's result array. The retry must inspect
// that EXEC element before treating the EVAL as a cache prerequisite: a
// SCRIPT LOAD synthesised from the rejected script would let the retried
// EVALSHA execute writes where the original ordering returns NOSCRIPT.
// Regression test for the Codex P1 review finding on #4351.
func TestPipelineRetryQueuedEvalRejectionIsNotPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	eval := pipe.Do(ctx, "eval", src, -1)
	exec := pipe.Do(ctx, "exec")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the queued EVAL was rejected inside EXEC, so the retried EVALSHA must not execute the script)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v, want nil (the queued EVAL itself holds +QUEUED; its rejection is in EXEC's result array)", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a queued EVAL rejected inside EXEC establishes no cache entry to reissue)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryAbortedTxEvalIsNotPreloaded: a WATCH-aborted EXEC
// returns a nil array, so the queued EVAL has no result array for the
// EXEC-element lookup to read — but the EVAL still cached nothing, so the
// rebuild must not synthesise a SCRIPT LOAD for it. When the connection
// drops before the EVALSHA runs, the retried EVALSHA must fail NOSCRIPT
// (the original EVAL never ran, so its script was never cached); if the
// rebuild preloaded it, the EVALSHA would execute a script the original
// pipeline should have refused. Regression test for the review finding
// on #4351 (aborted-transaction EVAL preloaded).
func TestPipelineRetryAbortedTxEvalIsNotPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	incr := pipe.Incr(ctx, "k")
	multi := pipe.Do(ctx, "multi")
	eval := pipe.Eval(ctx, src, nil)
	exec := pipe.Do(ctx, "exec")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	// The aborted EXEC is the batch's first error: it reports the WATCH
	// abort, and the EVALSHA reports NOSCRIPT on its own command.
	if !errors.Is(err, Nil) {
		t.Fatalf("Exec: %v, want redis.Nil (the WATCH-aborted EXEC is the batch's first error)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v, want nil (the queued EVAL itself holds +QUEUED; the abort is in EXEC's nil reply)", err)
	}
	if err := exec.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EXEC: %v, want redis.Nil (the WATCH key changed, so the EXEC aborted)", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the aborted transaction cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (the aborted EVAL cached nothing to preload)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the queued EVAL must not run again)", got)
	}
	if got := server.evalshas.Load(); got != 1 {
		t.Fatalf("server received %d EVALSHA commands, want 1", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryNestedMultiKeepsOuterBody: a nested MULTI is rejected
// but does not close the outer transaction, so the outer frame's body
// stays pending: when the connection drops before EXEC runs, the retry
// must re-queue every body command, including the one queued before the
// nested MULTI. If the rebuild resets the frame at the nested MULTI, it
// drops that first SET as answered and the retry commits a transaction
// missing it. The nested MULTI's own error still surfaces: the batch
// contains a genuinely erroneous command, so Exec reports it, and only
// the missing write distinguishes the fixed rebuild. Regression test for
// the review finding on #4351 (nested MULTI resets the frame).
func TestPipelineRetryNestedMultiKeepsOuterBody(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.tryagainConn = 1
	// Answer MGET, MULTI, SET a, the nested MULTI and SET b, then drop
	// before EXEC's reply: the MGET's TRYAGAIN is the boundary, and the
	// EXEC never runs on the first connection.
	server.dropAfter = 5
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	multi := pipe.Do(ctx, "multi")
	setA := pipe.Set(ctx, "{m}a", "v", 0)
	nested := pipe.Do(ctx, "multi")
	setB := pipe.Set(ctx, "{m}b", "v", 0)
	exec := pipe.Do(ctx, "exec")
	_, err := pipe.Exec(ctx)
	// The nested MULTI is a genuine error in the batch, so it surfaces
	// as the batch's first error once the MGET succeeds on retry.
	if err == nil || !strings.Contains(err.Error(), "MULTI calls can not be nested") {
		t.Fatalf("Exec: %v, want the nested MULTI error (the retry must still report the erroneous command)", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := setA.Err(); err != nil {
		t.Fatalf("SET a: %v", err)
	}
	if err := nested.Err(); err == nil || !strings.Contains(err.Error(), "MULTI calls can not be nested") {
		t.Fatalf("nested MULTI: %v, want the nesting error", err)
	}
	if err := setB.Err(); err != nil {
		t.Fatalf("SET b: %v", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	// Both SETs were received twice: once queued on the first
	// connection, once re-queued and committed on the retry. A rebuild
	// that resets the frame at the nested MULTI drops SET a and
	// receives only 3.
	if got := server.sets.Load(); got != 4 {
		t.Fatalf("server received %d SET commands, want 4 (both body commands must be re-queued on retry)", got)
	}
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("EXEC committed %d time(s), want 1 (the retry must commit the full outer frame)", got)
	}
}

// TestPipelineRetryKeepsQueuedBoolCmdAfterRedisRejection: a SET NX queued
// inside a MULTI/EXEC transaction is a BoolCmd, and BoolCmd.readReply
// accepts +QUEUED as (false, nil) — the QUEUED detector cannot see it.
// When MGET answers TRYAGAIN and the connection drops before EXEC runs,
// the tail filter must still keep the queued SET NX with its transaction
// frame, or the retry commits an empty transaction and reports success
// while the write never ran. Regression test for the Codex P1 review
// finding on #4351.
func TestPipelineRetryKeepsQueuedBoolCmdAfterRedisRejection(t *testing.T) {
	server := newQueuedRetryServer(t)
	server.dropAfter = 0
	server.dropExecConn = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.Do(ctx, "mget", "{m}a", "{m}b")
	multi := pipe.Do(ctx, "multi")
	setnx := pipe.SetNX(ctx, "k", "v", 0)
	exec := pipe.Do(ctx, "exec")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-queue the QUEUED SET NX and commit it)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if got, err := mget.Result(); err != nil {
		t.Fatalf("MGET: %v", err)
	} else if vals, ok := got.([]interface{}); !ok || len(vals) != 2 || vals[0] != "a" || vals[1] != "b" {
		t.Fatalf("MGET = %v, want [a b]", got)
	}
	if err := setnx.Err(); err != nil {
		t.Fatalf("SET NX: %v", err)
	}
	if got, err := exec.Result(); err != nil {
		t.Fatalf("EXEC: %v", err)
	} else if vals, ok := got.([]interface{}); !ok || len(vals) != 1 {
		t.Fatalf("EXEC = %v, want one result (the committed SET NX); an empty array means the retry committed an empty transaction", got)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the QUEUED SET NX must be re-queued on the retry)", got)
	}
	if got := server.execs.Load(); got != 1 {
		t.Fatalf("server received %d EXEC commands, want 1 (only the retry's EXEC runs; the second connection drops before its EXEC)", got)
	}
}

// TestPipelineRetryDoesNotRerunAnsweredSwapdb: SWAPDB is server-global, not
// per-connection state. An answered SWAPDB already swapped the databases,
// so a whole-batch retry that runs it again swaps them back and later
// writes land in the original databases while Exec still reports success.
// The retry must drop the answered SWAPDB instead of forcing the whole
// batch. Regression test for the Cursor Bugbot finding on #4351.
func TestPipelineRetryDoesNotRerunAnsweredSwapdb(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	swapdb := pipe.Do(ctx, "swapdb", 0, 1)
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must run only the SET, against the already-swapped databases)", err)
	}
	if err := swapdb.Err(); err != nil {
		t.Fatalf("SWAPDB: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.swapdbs.Load(); got != 1 {
		t.Fatalf("server received %d SWAPDB commands, want 1 (the answered SWAPDB must not run again and swap the databases back)", got)
	}
	if got := server.setDB.Load(); got != 1 {
		t.Fatalf("SET ran on DB %d, want 1 (after one SWAPDB 0 1, a SET on DB 0 lands in DB 1)", got)
	}
}

// TestPipelineRetryDoesNotRerunAnsweredSwapdbAfterBoundary: the same
// server-global rule after a retry boundary — an answered SWAPDB past a
// TRYAGAIN MGET is dropped by the tail filter's answered set, so it must
// not be kept as connection-local state to re-establish. The retry runs
// the MGET and the SET against the already-swapped databases.
func TestPipelineRetryDoesNotRerunAnsweredSwapdbAfterBoundary(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 0
	server.tryagainConn = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	swapdb := pipe.Do(ctx, "swapdb", 0, 1)
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must resend only the rejected MGET and the SET)", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if err := swapdb.Err(); err != nil {
		t.Fatalf("SWAPDB: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.swapdbs.Load(); got != 1 {
		t.Fatalf("server received %d SWAPDB commands, want 1 (the answered SWAPDB after the TRYAGAIN boundary must not run again)", got)
	}
	if got := server.setDB.Load(); got != 1 {
		t.Fatalf("SET ran on DB %d, want 1 (after one SWAPDB 0 1, a SET on DB 0 lands in DB 1)", got)
	}
}

// TestPipelineRetryWholeBatchDoesNotRerunAnsweredSwapdb: a live WATCH
// followed by an answered SWAPDB forces the whole batch on retry (the
// swap dirties the watch), but the whole-batch retry must not re-send
// the answered SWAPDB bare: that would swap the databases back and undo
// the confirmed swap, so the SET after it would land in the pre-swap
// database while Exec still reports success. The retry undoes the swap
// ahead of the batch and re-applies it at its own position, so the SET
// still lands in the post-swap database. Regression test for the Cursor
// Bugbot finding on #4351.
func TestPipelineRetryWholeBatchDoesNotRerunAnsweredSwapdb(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.trackValues = true
	server.setTryagainOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	swapdb := pipe.Do(ctx, "swapdb", 0, 1)
	set := pipe.Set(ctx, "x", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must run the SET in the post-swap database)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if err := swapdb.Err(); err != nil {
		t.Fatalf("SWAPDB: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.swapdbs.Load(); got != 3 {
		t.Fatalf("server received %d SWAPDB commands, want 3 (the answered SWAPDB plus its swap-back clone ahead of the whole-batch retry)", got)
	}
	if got := server.setsDB1.Load(); got != 1 {
		t.Fatalf("server executed %d SETs in DB 1, want 1 (after SWAPDB 0 1, a SET on DB 0 lands in DB 1)", got)
	}
	if got := server.setsDB0.Load(); got != 0 {
		t.Fatalf("server executed %d SETs in DB 0, want 0 (the whole-batch retry must not undo the confirmed swap)", got)
	}
}

// server rejected with NOREPLICAS is a pending server-global write, so
// a WAIT answered after it must be reissued with the retried SWAPDB:
// the retained acknowledgement count measured nothing about the swap.
// Before the fix the barrier's pending-write count excluded SWAPDB as
// connection state, so the retry ran only the SWAPDB and Exec returned
// nil with the stale count. Regression test for the Codex P1 finding
// on #4351 (review 6019219493).
func TestPipelineRetrySwapdbCountsAsPendingWriteForBarrier(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.failSwapdbOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	swapdb := pipe.Do(ctx, "swapdb", 0, 1)
	wait := pipe.Do(ctx, "wait", 1, 1000)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil", err)
	}
	if err := swapdb.Err(); err != nil {
		t.Fatalf("SWAPDB: %v", err)
	}
	if got, err := wait.Result(); err != nil || got != int64(1) {
		t.Fatalf("WAIT = %v, %v; want 1, nil", got, err)
	}
	if got := server.swapdbs.Load(); got != 2 {
		t.Fatalf("server received %d SWAPDB commands, want 2 (the NOREPLICAS SWAPDB must run again)", got)
	}
	if got := server.waits.Load(); got != 2 {
		t.Fatalf("server received %d WAIT commands, want 2 (the barrier must be reissued after the pending SWAPDB)", got)
	}
}

// TestPipelineRetryAnsweredSwapdbDoesNotMisorderPendingWrite: a pending
// write issued before an answered SWAPDB must run in the database it was
// issued for. The answered SWAPDB already swapped the databases on the
// first attempt, so the retry first reissues a fresh clone of the kept
// SWAPDB to swap them back, runs the pending write, and the kept original
// SWAPDB then swaps them back at its own position. Regression test for
// the Cursor Bugbot finding on #4351.
func TestPipelineRetryAnsweredSwapdbDoesNotMisorderPendingWrite(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	server.trackValues = true
	server.setTryagainOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	swapdb := pipe.Do(ctx, "swapdb", 0, 1)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must run the SET in the original database, ahead of the kept SWAPDB)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := swapdb.Err(); err != nil {
		t.Fatalf("SWAPDB: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %v, %v; want v, nil", got, err)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the TRYAGAIN SET plus its retry)", got)
	}
	if got := server.setsDB0.Load(); got != 1 {
		t.Fatalf("server executed %d SET commands in DB 0, want 1 (the pending write must land in the database it was issued for)", got)
	}
	if got := server.setsDB1.Load(); got != 0 {
		t.Fatalf("server executed %d SET commands in DB 1, want 0 (the swap-back clone must undo the answered swap before the pending write)", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (the pending write runs before the kept SWAPDB swaps back)", got)
	}
	if got := server.swapdbs.Load(); got != 3 {
		t.Fatalf("server received %d SWAPDB commands, want 3 (the answered SWAPDB, its swap-back clone, and the kept original)", got)
	}
}

// TestPipelineRetryPendingSwapdbDoesNotResendAnsweredWrite: an answered
// write after a pending SWAPDB already ran in the pre-swap database, so
// the retry must run only the SWAPDB itself: re-sending the write once
// the swap takes effect would land it in the wrong database while Exec
// still reports success. Regression test for the Cursor Bugbot finding
// on #4351.
func TestPipelineRetryPendingSwapdbDoesNotResendAnsweredWrite(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.failSwapdbTryagainOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	swapdb := pipe.Do(ctx, "swapdb", 0, 1)
	set := pipe.Set(ctx, "k", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must run only the pending SWAPDB, not the answered SET)", err)
	}
	if err := swapdb.Err(); err != nil {
		t.Fatalf("SWAPDB: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.swapdbs.Load(); got != 2 {
		t.Fatalf("server received %d SWAPDB commands, want 2 (the TRYAGAIN SWAPDB plus its retry)", got)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (the answered SET must not be re-sent after the swap)", got)
	}
	if got := server.setsDB0.Load(); got != 1 {
		t.Fatalf("server executed %d SET commands in DB 0, want 1 (the SET ran in the pre-swap database)", got)
	}
	if got := server.setsDB1.Load(); got != 0 {
		t.Fatalf("server executed %d SET commands in DB 1, want 0 (a re-sent SET would land in the swapped database)", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (the SET ran before the swap took effect)", got)
	}
}

// TestPipelineRetryMixedSwapdbPairsUndoInReverseOrder: undoing answered
// SWAPDBs ahead of a pending write must undo each swap individually,
// in reverse original order — not by parity, and not by cloning only
// the last one. Here SET is refused TRYAGAIN once and two answered
// SWAPDBs touch different pairs (0 1, then 0 2): they do not cancel
// out, and cloning only the last pair does not restore the pre-swap
// databases, so without the fix the retried SET lands in DB 2 while
// Exec reports success; with it the retry undoes both swaps in
// reverse, runs the SET in DB 0, and the kept originals swap back.
// Regression test for the Codex P1 and Cursor Medium findings on
// #4351 (review comments 4680803399 and 4680855831).
func TestPipelineRetryMixedSwapdbPairsUndoInReverseOrder(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 3
	server.trackValues = true
	server.setTryagainOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	swapdb1 := pipe.Do(ctx, "swapdb", 0, 1)
	swapdb2 := pipe.Do(ctx, "swapdb", 0, 2)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must undo both swaps, run the SET in DB 0, and swap back)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := swapdb1.Err(); err != nil {
		t.Fatalf("SWAPDB 0 1: %v", err)
	}
	if err := swapdb2.Err(); err != nil {
		t.Fatalf("SWAPDB 0 2: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %v, %v; want v, nil", got, err)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the TRYAGAIN SET plus its retry)", got)
	}
	if got := server.setsDB0.Load(); got != 1 {
		t.Fatalf("server executed %d SET commands in DB 0, want 1 (the pending write must land in the database it was issued for)", got)
	}
	if got := server.setsDB1.Load(); got != 0 {
		t.Fatalf("server executed %d SET commands in DB 1, want 0 (a forward-order undo would leave the write in DB 1)", got)
	}
	if got := server.setsDB2.Load(); got != 0 {
		t.Fatalf("server executed %d SET commands in DB 2, want 0 (no undo at all would leave the write in DB 2)", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (the pending write runs after both swaps are undone)", got)
	}
	if got := server.swapdbs.Load(); got != 6 {
		t.Fatalf("server received %d SWAPDB commands, want 6 (the two answered SWAPDBs, one undo clone each, and the two kept originals)", got)
	}
}

// TestPipelineRetryChainedSwapdbPairsUndoInReverseOrder: the same
// per-swap reverse-order undo for a chained pair (SWAPDB 0 1, then
// SWAPDB 1 2, the Codex example): the composition moves DB 0's data
// to DB 1, so no undo at all would land the pending write there,
// and undoing in forward order would land it in DB 2. Regression
// test for the Codex P1 finding on #4351 (review comment 4680803399).
func TestPipelineRetryChainedSwapdbPairsUndoInReverseOrder(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 3
	server.trackValues = true
	server.setTryagainOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	swapdb1 := pipe.Do(ctx, "swapdb", 0, 1)
	swapdb2 := pipe.Do(ctx, "swapdb", 1, 2)
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must undo both swaps, run the SET in DB 0, and swap back)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := swapdb1.Err(); err != nil {
		t.Fatalf("SWAPDB 0 1: %v", err)
	}
	if err := swapdb2.Err(); err != nil {
		t.Fatalf("SWAPDB 1 2: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %v, %v; want v, nil", got, err)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the TRYAGAIN SET plus its retry)", got)
	}
	if got := server.setsDB0.Load(); got != 1 {
		t.Fatalf("server executed %d SET commands in DB 0, want 1 (the pending write must land in the database it was issued for)", got)
	}
	if got := server.setsDB1.Load(); got != 0 {
		t.Fatalf("server executed %d SET commands in DB 1, want 0 (no undo at all would leave the write in DB 1)", got)
	}
	if got := server.setsDB2.Load(); got != 0 {
		t.Fatalf("server executed %d SET commands in DB 2, want 0 (a forward-order undo would leave the write in DB 2)", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (the pending write runs after both swaps are undone)", got)
	}
	if got := server.swapdbs.Load(); got != 6 {
		t.Fatalf("server received %d SWAPDB commands, want 6 (the two answered SWAPDBs, one undo clone each, and the two kept originals)", got)
	}
}

// TestPipelineRetryRetiresConnAfterUnsettledExec: an EXEC the server
// refuses with NOREPLICAS leaves MULTI open, so the connection must
// not return to the pool: NOREPLICAS is not a bad-conn error, so
// without the fix the retry reuses the same connection and sends
// MULTI into the still-open transaction, failing with a nested-MULTI
// error; with it the connection is retired and the retry opens the
// transaction on a fresh connection. Regression test for the Codex P2
// finding on #4351 (review comment 4680803417).
func TestPipelineRetryRetiresConnAfterUnsettledExec(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.failExecNoreplicasOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	incr := pipe.Incr(ctx, "counter")
	exec := pipe.Do(ctx, "exec")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must open the transaction on a fresh connection, not nest MULTI into the still-open one)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != int64(1) {
		t.Fatalf("INCR = %v, %v; want 1, nil", got, err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the connection with the still-open MULTI must be retired, not reused for the retry)", got)
	}
	if got := server.multiCount.Load(); got != 2 {
		t.Fatalf("server received %d MULTI commands, want 2 (one per attempt)", got)
	}
	if got := server.execs.Load(); got != 2 {
		t.Fatalf("server received %d EXEC commands, want 2 (the refused EXEC plus the retried one)", got)
	}
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("EXEC ran %d time(s) inside MULTI, want 1 (only the retried EXEC commits)", got)
	}
}

// TestPipelineRetryRefreshesReadAfterRetriedSwapdb: a read answered
// after a pending SWAPDB holds a pre-swap value, so the retry must
// re-run it after the swap instead of keeping it. Here SWAPDB 0 1
// answers NOREPLICAS once and the following GET answers the old
// value from DB 0: without the fix the retry runs only the SWAPDB
// and the GET keeps the old value while Exec reports success; with
// it the retry re-runs the GET after the swap, so the read is
// refreshed to the value in DB 1. Regression test for the Codex P2
// finding on #4351 (review comment 4680803438).
func TestPipelineRetryRefreshesReadAfterRetriedSwapdb(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.trackValues = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	// Pre-seed each database with its own value: DB 0 holds "old",
	// DB 1 holds "new", so the GET's answer proves which database
	// it read from.
	seed := client.Pipeline()
	seed.Select(ctx, 1)
	seed.Set(ctx, "k", "new", 0)
	seed.Select(ctx, 0)
	seed.Set(ctx, "k", "old", 0)
	if _, err := seed.Exec(ctx); err != nil {
		t.Fatalf("seed Exec: %v", err)
	}
	// The pipeline's SWAPDB is refused NOREPLICAS once; the GET
	// after it must be re-run on the retry, not keep its pre-swap
	// value.
	server.failSwapdbOnce.Store(true)
	pipe := client.Pipeline()
	swapdb := pipe.Do(ctx, "swapdb", 0, 1)
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-run the refused SWAPDB and refresh the GET after it)", err)
	}
	if err := swapdb.Err(); err != nil {
		t.Fatalf("SWAPDB: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "new" {
		t.Fatalf("GET = %q, %v; want %q, nil (the GET after the retried SWAPDB must be re-run, not keep its pre-swap value)", got, err, "new")
	}
	if got := server.swapdbs.Load(); got != 2 {
		t.Fatalf("server received %d SWAPDB commands, want 2 (the NOREPLICAS SWAPDB plus its retry)", got)
	}
}

// TestPipelineRetryDirtyWatchFollowerAfterBoundaryAbortsExec: the WATCH
// safety check must look past the retry boundary too. Here the second
// attempt receives TRYAGAIN for MGET, answers INCR on the watched key,
// and its EXEC aborts because the INCR invalidated the watch. A suffix
// retry that replayed the frame's WATCH / MULTI / EXEC around the dropped
// body would commit an empty transaction and overwrite the abort with
// success, so the settled-aborted frame is dropped with its body instead
// and the retry re-runs only the retryable MGET: the EXEC keeps its
// original redis.Nil and never commits. (An unsettled EXEC still forces
// the whole batch, as before: the first attempt's dropped EXEC is what
// drives the whole-batch retry to the second attempt.)
// Regression test for the Cursor Bugbot finding on #4351, updated for the
// Codex round-27 finding that a settled abort's outcome must be preserved
// rather than replayed.
func TestPipelineRetryDirtyWatchFollowerAfterBoundaryAbortsExec(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 99
	server.dropExecFirst = true
	server.tryagainConn = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	incr := pipe.Incr(ctx, "k")
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "other", "v", 0)
	exec := pipe.Do(ctx, "exec")
	// The aborted EXEC surfaces as redis.Nil, exactly as the same
	// batch would without any disconnect: the transaction aborted
	// because the client's own INCR invalidated the watch.
	if _, err := pipe.Exec(ctx); !errors.Is(err, Nil) {
		t.Fatalf("Exec: %v, want redis.Nil (the retried EXEC must abort, like the same batch without a disconnect)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := exec.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EXEC: %v, want redis.Nil (the aborted EXEC reply)", err)
	}
	// The retried transaction must not commit: the settled-aborted
	// frame is dropped rather than replayed, so its EXEC runs once
	// (on the second attempt) and never again.
	if got := server.execAborted.Load(); got != 1 {
		t.Fatalf("EXEC aborted %d time(s), want 1 (the second attempt's EXEC aborted, and the settled-aborted frame is not replayed)", got)
	}
	if got := server.execWithMulti.Load(); got != 0 {
		t.Fatalf("EXEC committed %d time(s), want 0 (a reissued clean WATCH must not let the transaction commit)", got)
	}
	// The answered INCR runs once per attempt before the abort (the
	// first attempt, and the whole-batch retry after the first
	// attempt's dropped EXEC); the retry after the settled abort
	// re-runs only the retryable MGET, so it runs no third time.
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (one per attempt before the settled abort)", got)
	}
}

// answeredWaitRetryServer is a scripted RESP server for the answered-WAIT
// pipeline retry test. It answers SET, answers the first MGET with TRYAGAIN
// (slot migration) and later MGETs with values, and answers WAIT. No
// connection is dropped: the TRYAGAIN error alone drives the retry, so the
// test can tell whether the retry re-ran the answered SET and WAIT.
type answeredWaitRetryServer struct {
	ln               net.Listener
	conns            atomic.Int32
	sets             atomic.Int32
	waits            atomic.Int32
	mgets            atomic.Int32
	tryagain         atomic.Bool  // the first MGET gets TRYAGAIN
	tryagainSet      atomic.Bool  // the first SET gets TRYAGAIN
	waitAfterSet     atomic.Int32 // WAITs that arrived after a SET succeeded on the same connection
	waitTimeoutRetry atomic.Bool  // WAITs after the first answer 0 (timeout), not 1
}

func newAnsweredWaitRetryServer(t *testing.T) *answeredWaitRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &answeredWaitRetryServer{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			s.conns.Add(1)
			go s.serve(conn)
		}
	}()
	return s
}

func (s *answeredWaitRetryServer) serve(conn net.Conn) {
	defer conn.Close()
	scanner := bufio.NewScanner(conn)
	seenSet := false
	for {
		name, _ := readConnStateRetryCommand(scanner)
		if name == "" {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "set":
			s.sets.Add(1)
			if s.tryagainSet.CompareAndSwap(true, false) {
				_, _ = conn.Write([]byte("-TRYAGAIN Multiple keys request during slot migration\r\n"))
			} else {
				seenSet = true
				_, _ = conn.Write([]byte("+OK\r\n"))
			}
		case "mget":
			s.mgets.Add(1)
			if s.tryagain.CompareAndSwap(false, true) {
				_, _ = conn.Write([]byte("-TRYAGAIN Multiple keys request during slot migration\r\n"))
			} else {
				_, _ = conn.Write([]byte("*2\r\n$1\r\na\r\n$1\r\nb\r\n"))
			}
		case "wait", "waitaof":
			n := s.waits.Add(1)
			if seenSet {
				s.waitAfterSet.Add(1)
			}
			if s.waitTimeoutRetry.Load() && n > 1 {
				_, _ = conn.Write([]byte(":0\r\n"))
			} else {
				_, _ = conn.Write([]byte(":1\r\n"))
			}
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryTxHImportPrepareNotLost: an HIMPORT PREPARE queued inside
// a MULTI/EXEC transaction that already committed must not be treated as
// completed session state the retry can drop. The registry still holds the
// old fields (himportAfterBatch never ran on the dropped connection), so a
// trimmed retry would inject a PREPARE with the stale fields and the SET
// would write under the wrong field names. Regression test for the Codex P1
// review finding on #4351.
func TestPipelineRetryTxHImportPrepareNotLost(t *testing.T) {
	server := newHImportRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	// Seed the client-side registry as if fs had been registered with
	// field "old" by an earlier HImportPrepare.
	client.himport.register("fs", []string{"old"})

	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	prep := pipe.HImportPrepare(ctx, "fs", "new")
	exec := pipe.Do(ctx, "exec")
	set := pipe.HImportSet(ctx, "k", "fs", "v")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := prep.Err(); err != nil {
		t.Fatalf("PREPARE: %v", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}

	server.mu.Lock()
	defer server.mu.Unlock()
	var retry []himportSetRecord
	for _, rec := range server.sets {
		if rec.conn == 2 {
			retry = append(retry, rec)
		}
	}
	if len(retry) != 1 {
		t.Fatalf("server received %d SET commands on the retry connection, want 1", len(retry))
	}
	if got := retry[0].fields; len(got) != 1 || got[0] != "new" {
		t.Fatalf("SET on the retry connection ran under PREPARE fields %v, want [new] (the committed transaction's PREPARE must be re-run, not replaced by the registry's stale fields)", got)
	}
}

// TestPipelineRetryOpenMultiDoesNotRerunAnsweredPrefix: a MULTI still open
// at the retry boundary frames only its own body. An answered write before
// it (INCR) must not be re-run: the retry narrows to the MULTI, its
// unanswered body and its EXEC, while the prefix's session state is
// re-established ahead of it. Regression test for the Cursor High review
// finding on #4351.
func TestPipelineRetryOpenMultiDoesNotRerunAnsweredPrefix(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	incr := pipe.Incr(ctx, "k")
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "a", "1", 0)
	exec := pipe.Do(ctx, "exec")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-run only the open transaction, not the answered INCR before it)", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil (the answered INCR must not run twice)", got, err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := exec.Result(); err != nil {
		t.Fatalf("EXEC: %v", err)
	} else if vals, ok := got.([]interface{}); !ok || len(vals) != 1 {
		t.Fatalf("EXEC = %v, want one result (the committed SET)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR before the open MULTI must not be re-run)", got)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1", got)
	}
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("EXEC committed %d time(s), want 1", got)
	}
}

// TestPipelineRetryTrailingOpenMultiKeepsQueuedBody: a MULTI frame still
// open at the end of the batch never ran its body — no EXEC follows it,
// so the queued commands never executed and must be re-queued on retry.
// Here the first connection answers MULTI and SET a's +QUEUED, then
// drops: the retry must send MULTI, SET a, SET b and a later EXEC must
// commit all of them (3 SETs on the server). Without the fix the tail
// filter drops SET a as answered — its reply is +QUEUED with a nil
// error — the retry sends only MULTI and SET b, and the later EXEC
// commits a transaction missing SET a. Regression test for the Codex P2
// review finding on #4351.
func TestPipelineRetryTrailingOpenMultiKeepsQueuedBody(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 2
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	setA := pipe.Set(ctx, "a", "v", 0)
	setB := pipe.Set(ctx, "b", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-queue the body of the still-open frame)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v, want nil", err)
	}
	if err := setA.Err(); err != nil {
		t.Fatalf("SET a: %v, want nil", err)
	}
	if err := setB.Err(); err != nil {
		t.Fatalf("SET b: %v, want nil", err)
	}
	if got := server.sets.Load(); got != 3 {
		t.Fatalf("server received %d SET commands, want 3 (the answered SET a from the still-open frame must be re-queued, plus its retry and SET b)", got)
	}
	// A later EXEC on the same connection must commit the frame the
	// retry left open, with SET a inside it.
	pipe2 := client.Pipeline()
	pipe2.Do(ctx, "exec")
	if _, err := pipe2.Exec(ctx); err != nil {
		t.Fatalf("later EXEC: %v, want nil", err)
	}
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("server EXEC'd %d transaction(s), want 1 (the retried frame must actually execute)", got)
	}
}

// TestPipelineRetryQueuedEvalRuntimeErrIsPreloaded: a Lua runtime error
// inside EXEC's result array means the queued EVAL ran (and cached its
// script) before failing — unlike a pre-execution rejection, it must still
// be preloaded with a SCRIPT LOAD, or the retried EVALSHA returns NOSCRIPT
// instead of the script's result. Regression test for the Codex P2 review
// finding on #4351. The reverse case — a queued EVAL rejected before
// execution (negative key count) preloads nothing — is already covered by
// TestPipelineRetryQueuedEvalRejectionIsNotPreloaded.
func TestPipelineRetryQueuedEvalRuntimeErrIsPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	sum := sha1.Sum([]byte(queuedRuntimeErrScript))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	eval := pipe.Do(ctx, "eval", queuedRuntimeErrScript, 0)
	exec := pipe.Do(ctx, "exec")
	evalsha := pipe.EvalSha(ctx, sha, nil, "arg")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retried EVALSHA must execute the preloaded script and return 42)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v, want nil (the queued EVAL itself holds +QUEUED; its runtime error is in EXEC's result array)", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(42) {
		t.Fatalf("EVALSHA = %v, %v; want 42, nil (the runtime error means the script ran and cached, so the retry must preload it)", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (the queued EVAL's runtime error must not be treated as a pre-execution rejection)", got)
	}
	if got := server.evalshas.Load(); got != 1 {
		t.Fatalf("server received %d EVALSHA commands, want 1", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryRawExecRejectionIsNotPreloaded: a queued EVAL rejected
// inside a raw EXEC's result array never ran, so it cached nothing — the
// same hole as TestPipelineRetryQueuedEvalRejectionIsNotPreloaded, but
// the EXEC is a RawCmd queued through BatchProcess: its reply is the
// verbatim RESP array, so the lookup must decode the array's error
// element instead of treating the EXEC as an ordinary answered command.
// Without that the retry synthesises a SCRIPT LOAD and the retried
// EVALSHA executes a write script the original pipeline refused with
// NOSCRIPT. Regression test for the Codex P1 review finding on #4351.
func TestPipelineRetryRawExecRejectionIsNotPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	eval := pipe.Do(ctx, "eval", src, -1)
	exec := NewRawCmd(ctx, "exec")
	if err := pipe.BatchProcess(ctx, exec); err != nil {
		t.Fatalf("BatchProcess: %v", err)
	}
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the retried EVALSHA must not execute a script the raw EXEC's rejection shows never ran)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v, want nil", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v, want nil (the queued EVAL itself holds +QUEUED; its rejection is in the raw EXEC's result array)", err)
	}
	if got := string(exec.Val()); !strings.Contains(got, "wrong number of keys") {
		t.Fatalf("raw EXEC Val() = %q, want it to keep the wrong-number-of-keys error element", got)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("raw EXEC Err(): %v, want nil (a raw command's error frame stays in Val() by contract)", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to reissue)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.evalshas.Load(); got != 1 {
		t.Fatalf("server received %d EVALSHA commands, want 1", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryMalformedEvalIsNotPreloaded: an EVAL refused for a
// malformed request — a missing numkeys, or a non-integer one — never
// runs, so it cached nothing and the retry must not synthesise a SCRIPT
// LOAD for it: the retried EVALSHA would then execute a write script
// the original pipeline refused with NOSCRIPT. Both subcases are the
// same pre-execution rejection class as the negative key count, with
// the argument-validation error wording Redis returns for them.
// Regression test for the Codex P2 review finding on #4351.
func TestPipelineRetryMalformedEvalIsNotPreloaded(t *testing.T) {
	cases := []struct {
		name string
		do   func(ctx context.Context, pipe Pipeliner) Cmder
		want string
	}{
		{
			name: "missing numkeys",
			do: func(ctx context.Context, pipe Pipeliner) Cmder {
				return pipe.Do(ctx, "eval", "return 1")
			},
			want: "wrong number of arguments",
		},
		{
			name: "non-integer numkeys",
			do: func(ctx context.Context, pipe Pipeliner) Cmder {
				return pipe.Do(ctx, "eval", "return 1", "one")
			},
			want: "value is not an integer or out of range",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			server := newScriptRetryServer(t)
			client := newPartialRetryClient(t, server.ln.Addr().String())

			const src = "return 1"
			sum := sha1.Sum([]byte(src))
			sha := hex.EncodeToString(sum[:])

			ctx := context.Background()
			pipe := client.Pipeline()
			eval := tc.do(ctx, pipe)
			evalsha := pipe.EvalSha(ctx, sha, nil)
			_, err := pipe.Exec(ctx)
			// The EVAL is the first command and it failed, so it
			// wins Exec's error; the proof of the fix is on the
			// EVALSHA below (NOSCRIPT, no SCRIPT LOAD), not here.
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Exec: %v, want the %q rejection (the malformed EVAL is the first failed command)", err, tc.want)
			}
			if err := eval.Err(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("EVAL: %v, want the %q rejection", err, tc.want)
			}
			if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
				t.Fatalf("EVALSHA: %v, want NOSCRIPT (the malformed EVAL cached nothing)", err)
			}
			if got := server.loads.Load(); got != 0 {
				t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a malformed EVAL establishes no cache entry to reissue)", got)
			}
			if got := server.evals.Load(); got != 1 {
				t.Fatalf("server received %d EVAL commands, want 1 (the malformed script must not be executed again)", got)
			}
			if got := server.evalshas.Load(); got != 1 {
				t.Fatalf("server received %d EVALSHA commands, want 1", got)
			}
			if got := server.noscripts.Load(); got != 1 {
				t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
			}
		})
	}
}

// TestPipelineRetryAnsweredWaitDoesNotForceWholeBatch: an answered WAIT past
// a retryable error must not force a whole-batch retry. The WAIT barrier
// already ran on the original connection, so re-running the batch would
// re-execute the answered write before it. Regression test for the Cursor
// Medium review finding on #4351.
func TestPipelineRetryAnsweredWaitDoesNotForceWholeBatch(t *testing.T) {
	server := newAnsweredWaitRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	wait := pipe.Do(ctx, "wait", 1, 100)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must resend only the rejected MGET)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got, err := wait.Result(); err != nil || got != int64(1) {
		t.Fatalf("WAIT = %v, %v; want 1, nil", got, err)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (the answered SET must not be re-run with the retried MGET)", got)
	}
	if got := server.waits.Load(); got != 1 {
		t.Fatalf("server received %d WAIT commands, want 1 (the answered WAIT must not be re-run)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET plus its retry)", got)
	}
}

// TestPipelineRetrySuffixNoTxIsLinear: a large transaction-free batch that
// drops near its start must not pay a rescan per command. Before the fix
// every remaining command rescanned back to the enclosing MULTI (which does
// not exist), so 20,000 SETs cost ~2e8 command-name comparisons; the
// precomputed frame table answers the same question in one pass.
func TestPipelineRetrySuffixNoTxIsLinear(t *testing.T) {
	const n = 20000
	ctx := context.Background()
	cmds := make([]Cmder, 0, n)
	for i := 0; i < n; i++ {
		cmds = append(cmds, NewStringCmd(ctx, "set", fmt.Sprintf("k%d", i), "v"))
	}
	// Simulate a drop just after the first command: the first SET was
	// answered, everything from the second on carries the transport
	// failure the drop stamped on it.
	for _, cmd := range cmds[1:] {
		cmd.SetErr(io.ErrUnexpectedEOF)
	}
	start := time.Now()
	suffix := pipelineRetrySuffix(ctx, cmds, io.ErrUnexpectedEOF, nil)
	elapsed := time.Since(start)
	if len(suffix) != n-1 {
		t.Fatalf("suffix has %d commands, want %d (every unanswered SET, once)", len(suffix), n-1)
	}
	// Machine-dependent by nature: the precomputed scan runs in
	// milliseconds on this workload, while the per-command rescan it
	// replaced took seconds. The bound only guards against the
	// quadratic behaviour coming back, with ample headroom.
	if elapsed > time.Second {
		t.Fatalf("pipelineRetrySuffix took %v for %d transaction-free commands, want < 1s (the per-command rescan is quadratic)", elapsed, n)
	}
}

// BenchmarkPipelineRetrySuffixNoTx measures pipelineRetrySuffix on a
// 20,000-SET transaction-free batch that drops near its start.
func BenchmarkPipelineRetrySuffixNoTx(b *testing.B) {
	const n = 20000
	ctx := context.Background()
	cmds := make([]Cmder, 0, n)
	for i := 0; i < n; i++ {
		cmds = append(cmds, NewStringCmd(ctx, "set", fmt.Sprintf("k%d", i), "v"))
	}
	for _, cmd := range cmds[1:] {
		cmd.SetErr(io.ErrUnexpectedEOF)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pipelineRetrySuffix(ctx, cmds, io.ErrUnexpectedEOF, nil)
	}
}

// TestPipelineRetrySuffixNoTxEvalIsLinear: a large transaction-free batch
// of EVALs that drops near its start must not pay a backward transaction
// scan per EVAL. Before the fix the rejected-queued-EVAL lookup walked
// back to the start of the batch for every EVAL — 20,000 of them, none
// in a transaction — so the retry cost ~2e8 command-name comparisons
// before it even started. The precomputed EXEC-error table answers the
// same question in one pass, and is only built when the suffix needs
// scripts at all. Regression test for the Codex P2 review finding on
// #4351.
func TestPipelineRetrySuffixNoTxEvalIsLinear(t *testing.T) {
	const n = 20000
	ctx := context.Background()
	cmds := make([]Cmder, 0, n)
	for i := 0; i < n; i++ {
		cmds = append(cmds, NewStringCmd(ctx, "eval", "return 1", 0))
	}
	// Simulate a drop just after the first command: the first EVAL was
	// answered, everything from the second on carries the transport
	// failure the drop stamped on it.
	for _, cmd := range cmds[1:] {
		cmd.SetErr(io.ErrUnexpectedEOF)
	}
	start := time.Now()
	suffix := pipelineRetrySuffix(ctx, cmds, io.ErrUnexpectedEOF, nil)
	elapsed := time.Since(start)
	if len(suffix) != n-1 {
		t.Fatalf("suffix has %d commands, want %d (every unanswered EVAL, once)", len(suffix), n-1)
	}
	// Machine-dependent by nature: the precomputed scan runs in
	// milliseconds on this workload, while the per-command backward
	// scan it replaced took seconds. The bound only guards against
	// the quadratic behaviour coming back, with ample headroom.
	if elapsed > time.Second {
		t.Fatalf("pipelineRetrySuffix took %v for %d transaction-free EVAL commands, want < 1s (the per-command backward scan is quadratic)", elapsed, n)
	}
}

// BenchmarkPipelineRetrySuffixNoTxEval measures pipelineRetrySuffix on a
// 20,000-EVAL transaction-free batch that drops near its start, with a
// trailing EVALSHA so the rejected-queued-EVAL lookup actually runs.
func BenchmarkPipelineRetrySuffixNoTxEval(b *testing.B) {
	const n = 20000
	ctx := context.Background()
	cmds := make([]Cmder, 0, n+1)
	for i := 0; i < n; i++ {
		cmds = append(cmds, NewStringCmd(ctx, "eval", "return 1", 0))
	}
	cmds = append(cmds, NewStringCmd(ctx, "evalsha", "deadbeef", 0))
	for _, cmd := range cmds[1:] {
		cmd.SetErr(io.ErrUnexpectedEOF)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = pipelineRetrySuffix(ctx, cmds, io.ErrUnexpectedEOF, nil)
	}
}

// TestPipelineRetryUnwatchClearsWatchSafetyScan: a successful UNWATCH
// ends the WATCH safety interval: the watch it cleared can no longer be
// dirtied, so an answered write after it must not force the whole batch.
// Here the first attempt answers WATCH, UNWATCH and INCR, then MGET
// answers TRYAGAIN: without the fix the retry replays the whole batch
// and the answered INCR runs twice while Exec reports success.
// Regression test for the Codex P1 review finding on #4351.
func TestPipelineRetryUnwatchClearsWatchSafetyScan(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 99
	server.tryagainConn = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	unwatch := pipe.Do(ctx, "unwatch")
	incr := pipe.Incr(ctx, "counter")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must send only the rejected MGET, after reissuing the cleared watch)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if err := unwatch.Err(); err != nil {
		t.Fatalf("UNWATCH: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR after the successful UNWATCH must not be re-run)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
}

// TestPipelineRetryResetClearsWatchSafetyScan: a successful RESET clears
// every live watch, like UNWATCH — Redis RESET includes UNWATCH. Here
// WATCH, RESET, INCR and a TRYAGAIN MGET run: without the fix the safety
// scan treats the INCR as a dirtying follower of the still-live watch
// and returns the whole batch, so the answered INCR runs a second time
// while Exec reports success; with it the RESET ends the safety interval
// and the retry resends only the rejected MGET. Regression test for the
// Codex P1 / Cursor High review findings on #4351.
func TestPipelineRetryResetClearsWatchSafetyScan(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 99
	server.tryagainConn = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	reset := pipe.Do(ctx, "reset")
	incr := pipe.Incr(ctx, "counter")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must send only the rejected MGET)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if err := reset.Err(); err != nil {
		t.Fatalf("RESET: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR after the successful RESET must not be re-run)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
}

// TestPipelineRetryFailedWatchIsNotLive: a WATCH the server rejected
// establishes no watch, so a write after it cannot dirty one. Here the
// first MGET answers TRYAGAIN, the WATCH answers NOPERM and the INCR
// answers: without the fix the safety scan still counts the failed
// WATCH as live, treats the answered INCR as a dirtying follower and
// returns the whole batch, so the INCR runs a second time; with it the
// retry resends only the rejected MGET and the failed WATCH. Regression
// test for the Cursor Medium review finding on #4351.
func TestPipelineRetryFailedWatchIsNotLive(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 99
	server.tryagainConn = 1
	server.failWatch.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	watch := pipe.Do(ctx, "watch", "k")
	incr := pipe.Incr(ctx, "counter")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("Exec: %v, want NOPERM (the rejected WATCH keeps its error)", err)
	}
	if err := watch.Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("WATCH: %v, want NOPERM", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR after the failed WATCH must not be re-run)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
}

// TestPipelineRetryKeepsUnansweredTrailingOneShot: a one-shot flag at the
// end of the batch whose reply was never read was not consumed by any
// follower, so the retry must keep it instead of dropping it as if its
// missing follower had consumed it. Here the first attempt answers MGET
// with TRYAGAIN and drops before ASKING's reply: without the fix the
// retry sends only MGET, the ASKING keeps its transport failure, and
// Exec reports it although another attempt could have completed it.
// Regression test for the Codex P2 review finding on #4351.
func TestPipelineRetryKeepsUnansweredTrailingOneShot(t *testing.T) {
	server := newOneShotTrailingRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	asking := pipe.Do(ctx, "asking")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must complete the unanswered trailing ASKING instead of reporting its transport failure)", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if err := asking.Err(); err != nil {
		t.Fatalf("ASKING: %v", err)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
	if got := server.askings.Load(); got != 2 {
		t.Fatalf("server received %d ASKING commands, want 2 (the unanswered trailing ASKING must be re-sent, not dropped as if consumed)", got)
	}
}

// TestPipelineRetryRejectedQueuedSelectIsNotReissued: a queued SELECT
// the transaction's EXEC rejected never changed the database, so the
// retry must not rebuild it as a session prerequisite. Here SELECT 99
// queues, EXEC answers its error as its result element, and the
// connection drops before GET's reply: without the fix the retry
// reissues SELECT 99 on the replacement connection, the required
// reissue fails, and the batch burns a whole-batch re-run instead of
// simply completing GET in the unchanged database. Regression test for
// the Codex P2 review finding on #4351.
func TestPipelineRetryRejectedQueuedSelectIsNotReissued(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 3
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	sel := pipe.Select(ctx, 99)
	exec := pipe.Do(ctx, "exec")
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must complete GET without reissuing the SELECT the EXEC rejected)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.selects.Load(); got != 1 {
		t.Fatalf("server received %d SELECT commands, want 1 (the SELECT the EXEC rejected must not be reissued as a session prerequisite)", got)
	}
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("server EXEC'd %d transaction(s), want 1 (the rejected SELECT must not burn a whole-batch re-run of the transaction)", got)
	}
}

// TestPipelineRetrySuffixQueuedTxIsLinear: a large queued transaction
// that loses the connection before EXEC replies must not pay a forward
// EXEC scan per queued command. Before the fix the tail filter scanned
// forward to EXEC separately for every queued SET — 20,000 of them, so
// the retry cost ~2e8 command-name comparisons before it even started.
// The precomputed frame/EXEC table answers the same question in one
// pass. Regression test for the Codex P2 review finding on #4351.
func TestPipelineRetrySuffixQueuedTxIsLinear(t *testing.T) {
	const n = 20000
	ctx := context.Background()
	cmds := make([]Cmder, 0, n+3)
	mget := NewStringCmd(ctx, "mget", "{m}a", "{m}b")
	mget.SetErr(proto.RedisError("TRYAGAIN Multiple keys request during slot migration"))
	cmds = append(cmds, mget)
	cmds = append(cmds, NewStringCmd(ctx, "multi"))
	for i := 0; i < n; i++ {
		set := NewStringCmd(ctx, "set", fmt.Sprintf("k%d", i), "v")
		// The server answered +QUEUED for every body command; the
		// test server records it as the StringCmd value.
		set.val = "QUEUED"
		cmds = append(cmds, set)
	}
	exec := NewStringCmd(ctx, "exec")
	// The connection dropped before EXEC's reply was read.
	exec.SetErr(io.ErrUnexpectedEOF)
	cmds = append(cmds, exec)
	start := time.Now()
	suffix := pipelineRetrySuffix(ctx, cmds, proto.RedisError("TRYAGAIN Multiple keys request during slot migration"), nil)
	elapsed := time.Since(start)
	if len(suffix) != n+3 {
		t.Fatalf("suffix has %d commands, want %d (the whole batch: the rejected MGET, the open frame and its EXEC, once)", len(suffix), n+3)
	}
	// Machine-dependent by nature: the precomputed scan runs in
	// milliseconds on this workload, while the per-command forward
	// scan it replaced took seconds. The bound only guards against
	// the quadratic behaviour coming back, with ample headroom.
	if elapsed > time.Second {
		t.Fatalf("pipelineRetrySuffix took %v for %d queued transaction commands, want < 1s (the per-command forward EXEC scan is quadratic)", elapsed, n)
	}
}

// TestPipelineRetryCommittedWatchTxDoesNotReplay: a WATCH/MULTI/EXEC
// transaction that already committed must not be replayed on retry.
// The queued writes never dirtied the watch (they run only at EXEC),
// and the successful EXEC cleared it, so a later retryable failure
// must retry only the suffix. Here the first attempt commits the
// transaction, then MGET answers TRYAGAIN: without the fix the WATCH
// safety scan treats MULTI and the queued SET as dirtying followers,
// returns the whole batch, and the transaction writes run twice while
// Exec reports success. Regression test for the Cursor High review
// finding on #4351.
func TestPipelineRetryCommittedWatchTxDoesNotReplay(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 99
	server.tryagainConn = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "other", "v", 0)
	exec := pipe.Do(ctx, "exec")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must send only the rejected MGET, not replay the committed transaction)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (the committed transaction's write must not run again)", got)
	}
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("server EXEC'd %d transaction(s), want 1 (the committed transaction must not be committed twice)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
}

// TestPipelineRetryAbortedExecIsNotRetriedAsOpenTx: an EXEC that
// answered redis.Nil — the WATCH abort — settles its transaction: the
// queued commands never ran and the caller already saw the abort, so
// a later retryable failure must not narrow back to the MULTI and
// re-run the body, where it could commit. Here the first attempt's own
// INCR dirties the watch, EXEC aborts, then MGET answers TRYAGAIN:
// without the fix the aborted frame counts as still open (and the
// WATCH scan replays the whole batch), so the answered INCR runs again
// and the transaction is re-run instead of staying aborted.
// Regression test for the Cursor High review finding on #4351.
func TestPipelineRetryAbortedExecIsNotRetriedAsOpenTx(t *testing.T) {
	server := newConnStateRetryServer(t)
	// Answer WATCH, INCR, MULTI, SET and EXEC, then drop before
	// MGET's reply: the later failure is the transport error, so the
	// retry is triggered (a TRYAGAIN on MGET alone would not be: the
	// EXEC's redis.Nil is the batch's first error and is not
	// retryable, so the pipeline would exit without retrying).
	server.dropAfter = 5
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	incr := pipe.Incr(ctx, "k")
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "other", "v", 0)
	exec := pipe.Do(ctx, "exec")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	// The aborted EXEC surfaces as redis.Nil, exactly as the same
	// batch would without any disconnect: the transaction aborted
	// because the client's own INCR invalidated the watch.
	if _, err := pipe.Exec(ctx); !errors.Is(err, Nil) {
		t.Fatalf("Exec: %v, want redis.Nil (the aborted EXEC reply must stand: the transaction aborted and must not be re-run)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := exec.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EXEC: %v, want redis.Nil (the aborted EXEC reply)", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	// The aborted transaction must stay aborted: it never commits on
	// the retry, and its body is never re-run.
	if got := server.execAborted.Load(); got != 1 {
		t.Fatalf("EXEC aborted %d time(s), want 1 (the aborted transaction must not be re-run)", got)
	}
	if got := server.execWithMulti.Load(); got != 0 {
		t.Fatalf("EXEC committed %d time(s), want 0 (the aborted transaction must not commit on retry)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be re-run)", got)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (the aborted transaction's body must not be re-queued)", got)
	}
}

// TestPipelineRetryClearedWatchIsNotReissued: a WATCH the first attempt
// already cleared — here by the first EXEC, which committed — is dead
// state, not a prerequisite: reissuing it would resurrect a watch the
// retry's own writes then dirty, aborting a transaction the original
// ordering would have committed. When the connection drops after the
// first EXEC and before the INCR's reply, the retry must run the INCR
// and the second transaction without the stale WATCH, so the second
// EXEC commits. Regression test for the review finding on #4351 (stale
// WATCH reissued past its clear).
func TestPipelineRetryClearedWatchIsNotReissued(t *testing.T) {
	server := newConnStateRetryServer(t)
	// Answer WATCH, MULTI, SET first and EXEC, then drop before INCR's
	// reply: the first transaction committed and cleared the watch, so
	// the retry must not bring it back.
	server.dropAfter = 4
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	multi := pipe.Do(ctx, "multi")
	setFirst := pipe.Set(ctx, "first", "v", 0)
	execFirst := pipe.Do(ctx, "exec")
	incr := pipe.Incr(ctx, "k")
	multi2 := pipe.Do(ctx, "multi")
	setOther := pipe.Set(ctx, "other", "v", 0)
	exec2 := pipe.Do(ctx, "exec")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the second transaction must commit: the cleared WATCH must not be reissued to abort it)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := setFirst.Err(); err != nil {
		t.Fatalf("SET first: %v", err)
	}
	if err := execFirst.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := multi2.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := setOther.Err(); err != nil {
		t.Fatalf("SET other: %v", err)
	}
	if err := exec2.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	// The first EXEC committed on the first connection and the second
	// on the retry; nothing aborted.
	if got := server.execWithMulti.Load(); got != 2 {
		t.Fatalf("EXEC committed %d time(s), want 2 (both transactions must commit)", got)
	}
	if got := server.execAborted.Load(); got != 0 {
		t.Fatalf("EXEC aborted %d time(s), want 0 (the resurrected WATCH must not abort the second transaction)", got)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (each SET runs once)", got)
	}
}

// TestPipelineRetryRawNullExecAbortsTx: a WATCH-aborted EXEC that is a raw
// command answers `*-1` with a nil error — RawCmd.readReply preserves the
// null frame, so the abort check must decode the bytes, not just the error.
// Here WATCH k; INCR k dirties the watch, MULTI queues a SELECT 2, the raw
// EXEC aborts, and the connection drops before SET's reply: without the fix
// the rebuild reissues the queued SELECT as a session prerequisite and SET
// lands in DB 2 while Exec reports success; with it the retry runs only SET
// in DB 0. Regression test for the Codex P1 review finding on #4351.
func TestPipelineRetryRawNullExecAbortsTx(t *testing.T) {
	server := newConnStateRetryServer(t)
	// Answer WATCH, INCR, MULTI, the queued SELECT and the raw EXEC,
	// then drop before SET's reply: the EXEC's null reply was read, so
	// the abort is known before the retry.
	server.dropAfter = 5
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	incr := pipe.Incr(ctx, "k")
	multi := pipe.Do(ctx, "multi")
	sel := pipe.Select(ctx, 2)
	exec := NewRawCmd(ctx, "exec")
	if err := pipe.BatchProcess(ctx, exec); err != nil {
		t.Fatalf("BatchProcess: %v", err)
	}
	set := pipe.Set(ctx, "out", "v", 0)
	_, err := pipe.Exec(ctx)
	// The aborted EXEC is a raw command, so its nil reply carries no
	// error: Exec reports success either way, and the proof of the fix
	// is where SET landed, not the error.
	if err != nil {
		t.Fatalf("Exec: %v, want nil", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (the queued SELECT itself holds +QUEUED; the abort is in EXEC's null reply)", err)
	}
	if got := string(exec.Val()); got != "*-1\r\n" {
		t.Fatalf("raw EXEC Val() = %q, want %q (the WATCH-aborted null reply)", got, "*-1\r\n")
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("raw EXEC Err(): %v, want nil (a raw command's reply stays in Val() by contract)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.selects.Load(); got != 1 {
		t.Fatalf("server received %d SELECT commands, want 1 (the queued SELECT must not be reissued as a prerequisite)", got)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET landed in DB %d, want 0 (the aborted transaction selected nothing)", got)
	}
}

// TestPipelineRetryAbortedTxFrameIsNotReplayed: a settled WATCH abort must
// not be replayed as an empty transaction. Here MGET answers TRYAGAIN and
// the EXEC answers the null array (the client's own INCR dirtied the
// watch), so the retry boundary is the MGET and the aborted frame sits in
// the tail: without the fix the connection-state keep replays WATCH /
// MULTI / EXEC around the dropped SET, the empty transaction commits and
// overwrites the EXEC's redis.Nil with success although SET never ran.
// With the fix the whole frame is dropped and the abort stands.
// Regression test for the Codex P1 review finding on #4351.
func TestPipelineRetryAbortedTxFrameIsNotReplayed(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 99
	server.tryagainConn = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	watch := pipe.Do(ctx, "watch", "k")
	incr := pipe.Incr(ctx, "k")
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "k", "v", 0)
	exec := pipe.Do(ctx, "exec")
	// The MGET's TRYAGAIN is the batch's first error, so the attempt
	// fails and retries even though the EXEC already answered the
	// WATCH abort; the aborted frame must not be replayed as an empty
	// transaction around the dropped SET.
	if _, err := pipe.Exec(ctx); !errors.Is(err, Nil) {
		t.Fatalf("Exec: %v, want redis.Nil (the aborted EXEC reply must stand: the transaction aborted and SET never ran)", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v, want nil (the queued SET itself holds +QUEUED; the abort is in EXEC's nil reply)", err)
	}
	if err := exec.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EXEC: %v, want redis.Nil (the aborted EXEC reply)", err)
	}
	// The aborted transaction must stay aborted: it never commits on
	// the retry, and its body is never re-run.
	if got := server.execAborted.Load(); got != 1 {
		t.Fatalf("EXEC aborted %d time(s), want 1 (the aborted transaction must not be re-run)", got)
	}
	if got := server.execWithMulti.Load(); got != 0 {
		t.Fatalf("EXEC committed %d time(s), want 0 (the retry must not commit an empty transaction over the abort)", got)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (the aborted transaction's body must not be re-queued)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR must not be re-run)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
}

// TestPipelineRetryHImportDiscardBookkeepingSurvivesDrop: an acknowledged
// HIMPORT DISCARD must apply its client-side registry bookkeeping even
// when the connection drops right after its reply. Only PREPARE counts as
// HIMPORT connection state, so the retry trims the answered DISCARD; if the
// failed read returns before himportAfterBatch runs, the registry still
// holds the fieldset and a later HImportSet injects a PREPARE from it,
// resurrecting the discarded fieldset instead of rejecting its use.
// Regression test for the Codex P2 review finding on #4351.
func TestPipelineRetryHImportDiscardBookkeepingSurvivesDrop(t *testing.T) {
	server := newHImportRetryServer(t)
	// Answer the HIMPORT DISCARD on the first connection, then drop
	// before the GET's reply: the DISCARD is acknowledged, so its
	// registry bookkeeping must be applied even though the retry
	// trims it as answered.
	server.dropNextAfterDiscard = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	// Seed the client-side registry as if fs had been registered with
	// field "old" by an earlier HImportPrepare.
	client.himport.register("fs", []string{"old"})

	pipe := client.Pipeline()
	discard := pipe.HImportDiscard(ctx, "fs")
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry runs only the GET)", err)
	}
	if err := discard.Err(); err != nil {
		t.Fatalf("DISCARD: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "OK" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "OK")
	}
	if got := server.discards.Load(); got != 1 {
		t.Fatalf("server received %d HIMPORT DISCARD commands, want 1 (the answered DISCARD must not be re-sent)", got)
	}
	// The acknowledged DISCARD removed the fieldset from the registry:
	// a later HImportSet must not inject a PREPARE for it.
	if _, ok := client.himport.lookup("fs"); ok {
		t.Fatal("fieldset fs is still registered after its acknowledged DISCARD (the retry trimmed the DISCARD before its bookkeeping ran)")
	}
	pipe2 := client.Pipeline()
	set := pipe2.HImportSet(ctx, "k", "fs", "v")
	if _, err := pipe2.Exec(ctx); err != nil {
		t.Fatalf("second Exec: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("HImportSet: %v", err)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	var after []himportSetRecord
	for _, rec := range server.sets {
		if rec.conn != 1 {
			after = append(after, rec)
		}
	}
	if len(after) != 1 {
		t.Fatalf("server received %d HIMPORT SET commands after the first connection, want 1", len(after))
	}
	if got := after[0].fields; len(got) != 0 {
		t.Fatalf("SET after the DISCARD ran under PREPARE fields %v, want none (the discarded fieldset must not be resurrected)", got)
	}
}

// TestPipelineRetryNestedMultiExecElemIsOrdinal: a rejected nested MULTI
// contributes no EXEC result element, so the EXEC result for a queued
// command must be looked up by its ordinal among the actually queued
// commands, not by its distance from MULTI. Here MGET answers TRYAGAIN,
// MULTI opens the frame, the nested MULTI is rejected, EVAL with a
// negative key count is queued, EXEC answers the rejection as its only
// element, and the connection drops before EVALSHA's reply: without the
// fix the EVAL's rejection is read past the end of the result array and
// becomes nil, so the rebuild synthesises a SCRIPT LOAD and the retried
// EVALSHA executes a script the original ordering would refuse with
// NOSCRIPT. Regression test for the Codex P2 review finding on #4351.
func TestPipelineRetryNestedMultiExecElemIsOrdinal(t *testing.T) {
	server := newScriptRetryServer(t)
	server.tryagainMget = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	multi := pipe.Do(ctx, "multi")
	nested := pipe.Do(ctx, "multi")
	eval := pipe.Do(ctx, "eval", src, -1)
	exec := pipe.Do(ctx, "exec")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	// The nested MULTI is a genuine error in the batch, so it surfaces
	// as the batch's first error once the MGET succeeds on retry; the
	// proof of the fix is on the EVALSHA (NOSCRIPT, no SCRIPT LOAD),
	// not here.
	if err == nil || !strings.Contains(err.Error(), "MULTI calls can not be nested") {
		t.Fatalf("Exec: %v, want the nested MULTI error", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := nested.Err(); err == nil || !strings.Contains(err.Error(), "MULTI calls can not be nested") {
		t.Fatalf("nested MULTI: %v, want the nesting error", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v, want nil (the queued EVAL itself holds +QUEUED; its rejection is in the EXEC result array)", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to preload)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.evalshas.Load(); got != 1 {
		t.Fatalf("server received %d EVALSHA commands, want 1", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// writeTimeoutRetryServer is a scripted RESP server for write-failure
// pipeline retry tests. The first connection answers HELLO and then stops
// reading forever, so a large batch's write blocks until the socket write
// timeout; later connections answer everything.
type writeTimeoutRetryServer struct {
	ln    net.Listener
	conns atomic.Int32
	incrs atomic.Int32
	sets  atomic.Int32
	done  chan struct{}
}

func newWriteTimeoutRetryServer(t *testing.T) *writeTimeoutRetryServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &writeTimeoutRetryServer{ln: ln, done: make(chan struct{})}
	t.Cleanup(func() { close(s.done) })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			n := s.conns.Add(1)
			go s.serve(conn, n == 1)
		}
	}()
	return s
}

func (s *writeTimeoutRetryServer) serve(conn net.Conn, first bool) {
	defer conn.Close()
	rd := bufio.NewReaderSize(conn, 64*1024)
	if first {
		// Answer the setup HELLO, then never read again: the batch's
		// write blocks until the socket write timeout.
		for {
			name, err := readStaleErrRESPCommand(rd)
			if err != nil {
				return
			}
			if name == "hello" {
				_, _ = conn.Write([]byte("%0\r\n"))
				<-s.done
				return
			}
		}
	}
	for {
		name, err := readStaleErrRESPCommand(rd)
		if err != nil {
			return
		}
		switch name {
		case "hello":
			_, _ = conn.Write([]byte("%0\r\n"))
		case "incr":
			s.incrs.Add(1)
			_, _ = conn.Write([]byte(":1\r\n"))
		case "set":
			s.sets.Add(1)
			_, _ = conn.Write([]byte("+OK\r\n"))
		default:
			_, _ = conn.Write([]byte("+OK\r\n"))
		}
	}
}

// TestPipelineRetryStaleRedisErrIsReplacedOnWriteFailure: an INCR reused
// through BatchProcess after its key was fixed still carries the WRONGTYPE
// of its previous invocation. When the new batch's write fails before
// sending anything, that stale error must not survive as the command's
// outcome: the write-failure path stamps the fresh transport error over
// it, so the retry re-executes the increment on a healthy connection
// instead of dropping it as answered and reporting the old WRONGTYPE.
// Regression test for the Codex P2 review finding on #4351.
func TestPipelineRetryStaleRedisErrIsReplacedOnWriteFailure(t *testing.T) {
	const nsets = 128
	server := newWriteTimeoutRetryServer(t)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		// The write must fail on the socket timeout, well before the
		// caller's context deadline, so the retry still has a live
		// context to run on a healthy connection.
		WriteTimeout:    time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	value := strings.Repeat("x", 256*1024)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// The reused INCR still carries the WRONGTYPE of an earlier
	// invocation, made against a key that has since been fixed.
	incr := NewIntCmd(ctx, "incr", "k")
	incr.SetErr(proto.RedisError("WRONGTYPE Operation against a key holding the wrong kind of value"))

	pipe := client.Pipeline()
	if err := pipe.BatchProcess(ctx, incr); err != nil {
		t.Fatalf("BatchProcess: %v", err)
	}
	var sets []*StatusCmd
	for i := 0; i < nsets; i++ {
		sets = append(sets, pipe.Set(ctx, fmt.Sprintf("k%d", i), value, 0))
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry re-executes the INCR on a healthy connection)", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil (the stale WRONGTYPE must not survive the write failure)", got, err)
	}
	for i, set := range sets {
		if err := set.Err(); err != nil {
			t.Fatalf("SET[%d]: %v", i, err)
		}
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the increment must run on the retry)", got)
	}
	if got := server.sets.Load(); got != int32(nsets) {
		t.Fatalf("server received %d SET commands, want %d (the whole batch must run on the retry)", got, nsets)
	}
}

// TestPipelineRetryNestedMultiCommittedTxIsNotReplayed: a settled outer
// EXEC must record the outer MULTI as completed even when the frame holds
// a rejected nested MULTI. Here MULTI, INCR, a rejected nested MULTI,
// INCR and EXEC all answer, then the connection drops before MGET's
// reply: without the fix the completed-transaction scan records only the
// nested MULTI, the outer MULTI still counts as unreissuable state and
// the retry re-runs the whole batch, re-running the committed writes.
// With the fix the retry resends only the MGET. Regression test for the
// Cursor Bugbot finding on #4351.
func TestPipelineRetryNestedMultiCommittedTxIsNotReplayed(t *testing.T) {
	server := newConnStateRetryServer(t)
	// Answer MULTI, INCR, the nested MULTI, INCR and EXEC, then drop
	// before MGET's reply: the outer transaction already committed.
	server.dropAfter = 5
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	incr := pipe.Incr(ctx, "k")
	nested := pipe.Do(ctx, "multi")
	incr2 := pipe.Incr(ctx, "k2")
	exec := pipe.Do(ctx, "exec")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	_, err := pipe.Exec(ctx)
	// The nested MULTI is a genuine error in the batch, so it surfaces
	// as the batch's first error once the MGET succeeds on retry.
	if err == nil || !strings.Contains(err.Error(), "MULTI calls can not be nested") {
		t.Fatalf("Exec: %v, want the nested MULTI error (the retry must still report the erroneous command)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := nested.Err(); err == nil || !strings.Contains(err.Error(), "MULTI calls can not be nested") {
		t.Fatalf("nested MULTI: %v, want the nesting error", err)
	}
	if got, err := incr2.Result(); err != nil || got != 1 {
		t.Fatalf("INCR2 = %d, %v; want 1, nil", got, err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got := server.multiCount.Load(); got != 2 {
		t.Fatalf("server received %d MULTI commands, want 2 (the outer MULTI and the rejected nested one, once each on the first attempt; the retry must send neither again)", got)
	}
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the committed INCRs must not be re-run)", got)
	}
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("EXEC committed %d time(s), want 1 (the retry must not commit the transaction again)", got)
	}
	if got := server.mgets.Load(); got != 1 {
		t.Fatalf("server received %d MGET commands, want 1 (the dropped MGET is never answered, only its retry runs)", got)
	}
}

// TestPipelineRetryResetDiscardsTransaction: a successful RESET discards
// the MULTI/EXEC transaction, so the frame is closed and its queued body
// is abandoned — the retry must not rewind to MULTI and re-run it, and
// must not rebuild a queued SELECT as a session prerequisite. Here
// MULTI queues a SELECT 2, RESET discards the frame, a standalone INCR
// runs, and MGET answers TRYAGAIN: without the fix the retry rewinds to
// MULTI and re-runs the whole batch; with it the retry resends only the
// MGET (plus the harmless RESET reissue). Regression test for the Codex
// P1 review finding on #4351.
func TestPipelineRetryResetDiscardsTransaction(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 99
	server.tryagainConn = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	sel := pipe.Select(ctx, 2)
	reset := pipe.Do(ctx, "reset")
	incr := pipe.Incr(ctx, "counter")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must resend only the rejected MGET)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v, want nil (the queued SELECT holds +QUEUED)", err)
	}
	if err := reset.Err(); err != nil {
		t.Fatalf("RESET: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got := server.multiCount.Load(); got != 1 {
		t.Fatalf("server received %d MULTI commands, want 1 (the RESET-discarded frame must not be re-run)", got)
	}
	if got := server.selects.Load(); got != 1 {
		t.Fatalf("server received %d SELECT commands, want 1 (the queued SELECT must not be rebuilt as a prerequisite)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the standalone INCR must not be re-run)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET plus its retry)", got)
	}
	if got := server.execWithMulti.Load(); got != 0 {
		t.Fatalf("EXEC committed %d time(s), want 0 (no transaction ever ran)", got)
	}
}

// TestPipelineRetryResetRestoresDatabaseAfterDiscardedTx: a RESET that
// discards a transaction restores the default database, so a SET after
// it must land in DB 0 — not in the DB the answered SELECT before the
// transaction chose. Here SELECT 2, MULTI, a queued SELECT 1, RESET and
// SET run, and the first connection drops before SET's reply: without
// the fix the rebuild drops the RESET with the discarded frame and
// resends only SELECT 2 and SET, so the SET lands in DB 2 while Exec
// reports success; with it the RESET is reissued as a prerequisite and
// the SET lands in DB 0. Regression test for the Codex P1 review
// finding on #4351.
func TestPipelineRetryResetRestoresDatabaseAfterDiscardedTx(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 4
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	multi := pipe.Do(ctx, "multi")
	queuedSel := pipe.Select(ctx, 1)
	reset := pipe.Do(ctx, "reset")
	set := pipe.Set(ctx, "out", "v", 0)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must resend SELECT 2, RESET and SET)", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT 2: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := queuedSel.Err(); err != nil {
		t.Fatalf("queued SELECT 1: %v, want nil (it holds +QUEUED)", err)
	}
	if err := reset.Err(); err != nil {
		t.Fatalf("RESET: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.setDB.Load(); got != 0 {
		t.Fatalf("SET ran on DB %d, want 0 (the RESET that discarded the transaction restores the default database)", got)
	}
	if got := server.selects.Load(); got != 3 {
		t.Fatalf("server received %d SELECT commands, want 3 (the first attempt's SELECT 2 and queued SELECT 1, plus the reissue of SELECT 2; the queued SELECT 1 must not be rebuilt)", got)
	}
	if got := server.resets.Load(); got != 2 {
		t.Fatalf("server received %d RESET commands, want 2 (the answered RESET must be reissued as a prerequisite)", got)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (only the retried SET runs)", got)
	}
	if got := server.multiCount.Load(); got != 1 {
		t.Fatalf("server received %d MULTI commands, want 1 (the RESET-discarded frame must not be re-run)", got)
	}
}

// TestPipelineRetryAnsweredWaitReissuedAfterPendingWrite: an answered
// WAIT/WAITAOF dropped by the tail filter must be reissued when a write
// before it is still pending. Here SET answers TRYAGAIN and WAIT answers
// 1: without the fix the retry resends only the SET and Exec returns the
// retained WAIT result even though the write executed after that barrier
// and may still be unreplicated; with it the retry reissues the WAIT
// after the SET on the retry connection. Regression test for the Codex
// P1 review finding on #4351.
func TestPipelineRetryAnsweredWaitReissuedAfterPendingWrite(t *testing.T) {
	server := newAnsweredWaitRetryServer(t)
	server.tryagainSet.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	wait := pipe.Do(ctx, "wait", 1, 100)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-run the SET with its WAIT)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := wait.Result(); err != nil || got != int64(1) {
		t.Fatalf("WAIT = %v, %v; want 1, nil", got, err)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the rejected SET plus its retry)", got)
	}
	if got := server.waits.Load(); got != 2 {
		t.Fatalf("server received %d WAIT commands, want 2 (the answered WAIT must be reissued after the retried SET)", got)
	}
	if got := server.waitAfterSet.Load(); got != 1 {
		t.Fatalf("WAIT ran after a SET %d time(s), want 1 (the reissued barrier must follow its write on the retry connection)", got)
	}
}

// TestPipelineRetryReissuedWaitResultReplacesOriginal: the reissued
// barrier's acknowledgement count must land on the original WAIT, not
// stay on the clone. Here SET answers TRYAGAIN and WAIT answers 1, and
// the retry re-runs the SET with a fresh WAIT that times out (0): without
// the fix Exec returns nil while the original WAIT still reads 1, so the
// caller accepts the retried write as replicated without a replica
// acknowledgement; with it the original reads 0. Regression test for
// the Codex P1 review finding on #4351.
func TestPipelineRetryReissuedWaitResultReplacesOriginal(t *testing.T) {
	server := newAnsweredWaitRetryServer(t)
	server.tryagainSet.Store(true)
	server.waitTimeoutRetry.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	wait := pipe.Do(ctx, "wait", 1, 100)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-run the SET with its WAIT)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := wait.Result(); err != nil || got != int64(0) {
		t.Fatalf("WAIT = %v, %v; want 0, nil (the reissued barrier timed out, so the original must read 0, not its first attempt's 1)", got, err)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the rejected SET plus its retry)", got)
	}
	if got := server.waits.Load(); got != 2 {
		t.Fatalf("server received %d WAIT commands, want 2 (the answered WAIT must be reissued after the retried SET)", got)
	}
	if got := server.waitAfterSet.Load(); got != 1 {
		t.Fatalf("WAIT ran after a SET %d time(s), want 1 (the reissued barrier must follow its write on the retry connection)", got)
	}
}

// TestPipelineRetryReplayedScriptFlushFailureIsVisible: a replayed SCRIPT
// FLUSH is not an optional preload — it preserves the cache semantics the
// batch established. Here SCRIPT LOAD, SCRIPT FLUSH and EVALSHA run, the
// first connection drops before EVALSHA's reply, and the retry connection
// refuses the replayed FLUSH with NOPERM: without the fix the failure is
// ignored as a best-effort preload, the LOAD repopulates the cache and
// EVALSHA executes the flushed script while Exec reports success; with
// it the FLUSH failure propagates and Exec reports NOPERM. Regression
// test for the Codex P1 review finding on #4351.
func TestPipelineRetryReplayedScriptFlushFailureIsVisible(t *testing.T) {
	server := newScriptRetryServer(t)
	server.denyFlushRetry = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	load := pipe.ScriptLoad(ctx, src)
	flush := pipe.ScriptFlush(ctx)
	pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("Exec: %v, want NOPERM (the replayed FLUSH failure must propagate, not be ignored as a best-effort preload)", err)
	}
	if got := load.Val(); got != sha {
		t.Fatalf("SCRIPT LOAD = %q, want %q", got, sha)
	}
	if err := flush.Err(); err != nil {
		t.Fatalf("SCRIPT FLUSH: %v, want nil (the original FLUSH still holds its +OK)", err)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must be reissued on the retry connection)", got)
	}
	if got := server.flushes.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT FLUSH commands, want 2 (the answered FLUSH must be replayed, and its failure reported)", got)
	}
	if got := server.evalshas.Load(); got != 1 {
		t.Fatalf("server received %d EVALSHA commands, want 1 (only the retry's EVALSHA runs; the first connection drops before its reply)", got)
	}
}

// TestPipelineRetryStaleRawCmdIsStampedOnDialFailure: a reused RawCmd
// retains its previous -LOADING frame in Val() with a nil rawErr(), and
// that retained frame must not hide the next invocation's failure. Here
// a raw SET first answers -LOADING, then the same command is reused
// behind a fresh SET against an address nothing listens on: without the
// fix the terminal fallback excludes the stale command from stamping and
// it keeps Err() == nil while Exec reports the dial failure; with it the
// dial error is recorded on the raw command. Regression test for the
// Codex P2 review finding on #4351.
func TestPipelineRetryStaleRawCmdIsStampedOnDialFailure(t *testing.T) {
	// First pipeline: stamp -LOADING onto the shared raw command. A
	// single attempt (MaxRetries -1 normalises to 0) answers it, so no
	// retry heals it. The raw command keeps the error frame in Val()
	// with a nil error by contract, so this Exec reports success.
	server := newLoadingRetryServer(t)
	first := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = first.Close() })

	ctx := context.Background()
	pipe := first.Pipeline()
	shared := NewRawCmd(ctx, "set", "shared", "v")
	if err := pipe.BatchProcess(ctx, shared); err != nil {
		t.Fatalf("BatchProcess: %v", err)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("first Exec: %v, want nil (the raw command's error frame stays in Val() by contract)", err)
	}
	if got := string(shared.Val()); !strings.Contains(got, "LOADING") {
		t.Fatalf("shared raw SET Val() = %q, want the retained -LOADING frame", got)
	}
	if err := shared.Err(); err != nil {
		t.Fatalf("shared raw SET Err() = %v, want nil (a raw command's error frame stays in Val() by contract)", err)
	}

	// Second pipeline: the same shared command, behind a fresh SET, against
	// an address nothing listens on — every dial is refused, so no command
	// is ever dispatched.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	addr := ln.Addr().String()
	_ = ln.Close()
	dead := NewClient(&Options{
		Addr:            addr,
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      3,
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = dead.Close() })

	pipe2 := dead.Pipeline()
	fresh := pipe2.Set(ctx, "fresh", "v", 0)
	_ = pipe2.BatchProcess(ctx, shared)
	_, err = pipe2.Exec(ctx)
	if err == nil {
		t.Fatalf("second Exec: nil, want the dial error (nothing listens on %s)", addr)
	}
	if err := fresh.Err(); err == nil {
		t.Fatal("fresh SET: nil, want the dial error (it was never dispatched, so it must not look successful)")
	}
	if err := shared.Err(); err == nil {
		t.Fatal("shared raw SET: nil, want the dial error (it was never dispatched in this invocation, so the retained -LOADING frame must not stand as its result)")
	}
}

// TestPipelineRetrySuffixManyWatchesIsLinear: a large batch of individually
// queued WATCH commands followed by one lost reply must not pay a rescan
// per WATCH. Before the fix every WATCH walked the remaining batch in the
// safety check and the rebuild rescanned it again per WATCH through the
// cleared-before lookup, so 20,000 WATCH commands cost ~4e8 follower
// checks; the precomputed clear-event table and the single-pass safety
// check answer the same questions in one pass. Regression test for the
// Codex P2 review finding on #4351.
func TestPipelineRetrySuffixManyWatchesIsLinear(t *testing.T) {
	const n = 20000
	ctx := context.Background()
	cmds := make([]Cmder, 0, n+1)
	for i := 0; i < n; i++ {
		cmds = append(cmds, NewStringCmd(ctx, "watch", "k"))
	}
	cmds = append(cmds, NewStringCmd(ctx, "get", "k"))
	// Simulate a drop just after the last WATCH: every WATCH was
	// answered, the GET carries the transport failure the drop stamped
	// on it.
	cmds[n].SetErr(io.ErrUnexpectedEOF)
	start := time.Now()
	suffix := pipelineRetrySuffix(ctx, cmds, io.ErrUnexpectedEOF, nil)
	elapsed := time.Since(start)
	// Every WATCH is reissued as a fresh clone ahead of the GET: the
	// watches are still live at the boundary and nothing dirtied them.
	if len(suffix) != n+1 {
		t.Fatalf("suffix has %d commands, want %d (every reissued WATCH plus the unanswered GET, once each)", len(suffix), n+1)
	}
	// Machine-dependent by nature: the precomputed scan runs in
	// milliseconds on this workload, while the per-command rescans it
	// replaced took seconds. The bound only guards against the
	// quadratic behaviour coming back, with ample headroom.
	if elapsed > time.Second {
		t.Fatalf("pipelineRetrySuffix took %v for %d WATCH commands, want < 1s (the per-WATCH rescans are quadratic)", elapsed, n)
	}
}

// TestPipelineRetryFailedWatchIsNotReissued: a WATCH the server rejected
// established nothing, so the rebuild must not reissue it as a required
// synthetic — the retry would otherwise resurrect a watch the original
// ordering never had, and a later EXEC could abort a transaction the
// original would have committed. Regression test for the Cursor Medium
// review finding on #4351.
func TestPipelineRetryFailedWatchIsNotReissued(t *testing.T) {
	ctx := context.Background()
	watch := NewStringCmd(ctx, "watch", "k")
	watch.SetErr(proto.RedisError("NOPERM this user has no permissions to run the 'watch' command"))
	incr := NewStringCmd(ctx, "incr", "counter")
	get := NewStringCmd(ctx, "get", "k")
	get.SetErr(io.ErrUnexpectedEOF)
	cmds := []Cmder{watch, incr, get}
	suffix := pipelineRetrySuffix(ctx, cmds, io.ErrUnexpectedEOF, nil)
	if len(suffix) != 1 || suffix[0] != get {
		t.Fatalf("suffix has %d commands, want 1 (only the unanswered GET; the rejected WATCH must not be reissued)", len(suffix))
	}
}

// TestPipelineRetryDiscardedTxHImportPrepareDoesNotForceWholeBatch: an
// HIMPORT PREPARE inside a transaction DISCARD dropped never executed,
// so it registered no fieldset and holds no connection state the retry
// must re-establish. Here MULTI, HIMPORT PREPARE and DISCARD run, then
// an INCR and a TRYAGAIN MGET: without the fix the prefix scan treats
// the PREPARE as unreissuable state and returns the whole batch, so the
// answered INCR runs a second time while Exec reports success; with it
// the retry resends only the rejected MGET. Regression test for the
// Cursor High review finding on #4351.
func TestPipelineRetryDiscardedTxHImportPrepareDoesNotForceWholeBatch(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 99
	server.tryagainConn = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	prepare := pipe.Do(ctx, "himport", "prepare", "fs")
	discard := pipe.Do(ctx, "discard")
	incr := pipe.Incr(ctx, "counter")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must send only the rejected MGET)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := prepare.Err(); err != nil {
		t.Fatalf("HIMPORT PREPARE: %v", err)
	}
	if err := discard.Err(); err != nil {
		t.Fatalf("DISCARD: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR after the discarded frame must not be re-run)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
}

// TestPipelineRetryReadAfterWatchIsSafeFollower: an answered read after a
// WATCH cannot dirty it — a GET modifies no key — so it is a safe
// follower like a session reissue. Here an INCR runs before the WATCH, a
// GET runs after it, and a TRYAGAIN MGET follows: without the fix the
// safety scan treats the GET as a dirtying follower and returns the
// whole batch, so the answered INCR before the watch runs a second time
// while Exec reports success; with it the retry resends only the
// rejected MGET. Regression test for the Cursor High review finding on
// #4351.
func TestPipelineRetryReadAfterWatchIsSafeFollower(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 99
	server.tryagainConn = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	incr := pipe.Incr(ctx, "counter")
	watch := pipe.Do(ctx, "watch", "k")
	get := pipe.Get(ctx, "k")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must send only the rejected MGET)", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR before the watch must not be re-run)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
}

// TestPipelineRetryUnwatchAfterBoundaryClearsWatchSafetyScan: a successful
// UNWATCH the first attempt answered past the boundary still cleared the
// watch before the followers after it ran, even though the tail filter
// keeps it for re-run (an UNWATCH is connection state). Here WATCH, a
// TRYAGAIN MGET, UNWATCH and an INCR run: without the fix the safety scan
// cannot see the UNWATCH clear event, treats the answered INCR as a
// dirtying follower of the still-live watch and returns the whole batch,
// so the INCR runs a second time while Exec reports success; with it the
// retry resends only the rejected MGET (with the reissued WATCH and the
// kept UNWATCH). Regression test for the Codex P1 review finding on #4351.
func TestPipelineRetryUnwatchAfterBoundaryClearsWatchSafetyScan(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 99
	server.tryagainConn = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	unwatch := pipe.Do(ctx, "unwatch")
	incr := pipe.Incr(ctx, "counter")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must send only the rejected MGET)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if err := unwatch.Err(); err != nil {
		t.Fatalf("UNWATCH: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if got := server.watches.Load(); got != 2 {
		t.Fatalf("server received %d WATCH commands, want 2 (the original plus its reissue ahead of the retried MGET)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the answered INCR after the successful UNWATCH must not be re-run)", got)
	}
	if got := server.mgets.Load(); got != 2 {
		t.Fatalf("server received %d MGET commands, want 2 (the rejected MGET must be retried once)", got)
	}
}

// TestPipelineRetryRejectedResetKeepsDiscardedTx: a RESET the server
// rejected leaves the MULTI frame open, so a DISCARD after it still
// discards the frame's queued commands. Here MULTI, EVAL, a NOPERM RESET
// and DISCARD run, then an EVALSHA the first connection drops before
// answering: without the fix the frame tracking is cleared by the
// rejected RESET, the DISCARD marks nothing discarded, and the rebuild
// synthesises a SCRIPT LOAD for the EVAL that never ran — the retried
// EVALSHA then executes a script the original ordering would have
// refused with NOSCRIPT; with it the EVAL is recognised as discarded
// and the EVALSHA fails NOSCRIPT as it should. Regression test for the
// Codex P2 review finding on #4351.
func TestPipelineRetryRejectedResetKeepsDiscardedTx(t *testing.T) {
	server := newScriptRetryServer(t)
	server.failReset.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	eval := pipe.Do(ctx, "eval", src, 0)
	reset := pipe.Do(ctx, "reset")
	discard := pipe.Do(ctx, "discard")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("Exec: %v, want NOPERM (the rejected RESET is reissued as a required prerequisite and fails again)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v, want nil (the queued EVAL itself holds +QUEUED; it never ran because DISCARD dropped it)", err)
	}
	if err := reset.Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("RESET: %v, want NOPERM", err)
	}
	if err := discard.Err(); err != nil {
		t.Fatalf("DISCARD: %v", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the discarded EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a discarded EVAL establishes no cache entry to preload)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the discarded script must not be executed again)", got)
	}
	if got := server.evalshas.Load(); got != 1 {
		t.Fatalf("server received %d EVALSHA commands, want 1", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryWatchInMultiDoesNotShiftExecOrdinals: a WATCH inside
// MULTI is rejected without being queued, so it contributes no EXEC
// result element. Here a TRYAGAIN MGET, MULTI, WATCH, a queued EVAL with
// a negative key count, EXEC and EVALSHA run: without the fix the ordinal
// map counts the WATCH as a queued body, so the EVAL's EXEC rejection
// lands on the WATCH and the EVAL reads a nil element as though it had
// run — the rebuild synthesises a SCRIPT LOAD and the retried EVALSHA
// executes the rejected script; with it the EVAL is recognised as
// rejected and the EVALSHA fails NOSCRIPT. Regression test for the Codex
// P2 review finding on #4351.
func TestPipelineRetryWatchInMultiDoesNotShiftExecOrdinals(t *testing.T) {
	server := newScriptRetryServer(t)
	server.tryagainMget = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.MGet(ctx, "{m}a", "{m}b")
	multi := pipe.Do(ctx, "multi")
	watch := pipe.Do(ctx, "watch", "k")
	eval := pipe.Do(ctx, "eval", src, -1)
	exec := pipe.Do(ctx, "exec")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "WATCH inside MULTI") {
		t.Fatalf("Exec: %v, want the WATCH rejection", err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := watch.Err(); err == nil || !strings.Contains(err.Error(), "WATCH inside MULTI is not allowed") {
		t.Fatalf("WATCH: %v, want the inside-MULTI rejection", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v, want nil (the queued EVAL itself holds +QUEUED; its rejection is in the EXEC result array)", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to preload)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.evalshas.Load(); got != 1 {
		t.Fatalf("server received %d EVALSHA commands, want 1", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetrySuffixManyAnsweredWaitsIsLinear: many answered WAITs
// after a single pending read must not pay a rescan per barrier. Before
// the fix every barrier scanned its whole preceding batch looking for a
// pending write that does not exist, so 20,000 barriers cost ~2e8
// iterations; the precomputed pending-write prefix answers the same
// question in one pass.
func TestPipelineRetrySuffixManyAnsweredWaitsIsLinear(t *testing.T) {
	const n = 20000
	ctx := context.Background()
	cmds := make([]Cmder, 0, n+1)
	mget := NewStringCmd(ctx, "mget", "{m}a", "{m}b")
	mget.SetErr(proto.RedisError("TRYAGAIN Multiple keys request during slot migration"))
	cmds = append(cmds, mget)
	for i := 0; i < n; i++ {
		cmds = append(cmds, NewStringCmd(ctx, "wait", 1, 100))
	}
	start := time.Now()
	suffix := pipelineRetrySuffix(ctx, cmds, proto.RedisError("TRYAGAIN Multiple keys request during slot migration"), nil)
	elapsed := time.Since(start)
	if len(suffix) != 1 || suffix[0] != mget {
		t.Fatalf("suffix has %d commands, want 1 (only the unanswered MGET; the answered WAITs need no barrier re-run without a pending write)", len(suffix))
	}
	// Machine-dependent by nature: the precomputed scan runs in
	// milliseconds on this workload, while the per-barrier rescans it
	// replaced took seconds. The bound only guards against the
	// quadratic behaviour coming back, with ample headroom.
	if elapsed > time.Second {
		t.Fatalf("pipelineRetrySuffix took %v for %d answered WAITs, want < 1s (the per-barrier prefix rescans are quadratic)", elapsed, n)
	}
}

// TestPipelineRetryQueuedUnwatchDoesNotClearWatchSafetyScan: an UNWATCH
// queued inside MULTI has not cleared the watch yet — Redis queues it and
// runs it only at EXEC — so its +QUEUED acknowledgement is not a
// WATCH-clear event. Here WATCH k, an INCR that dirties it, MULTI, a
// queued UNWATCH, a queued SET and EXEC run, and the connection drops
// after the SET's +QUEUED reply: without the fix the safety scan treats
// the queued UNWATCH as a clear, drops the answered INCR and retries
// WATCH; MULTI; UNWATCH; SET; EXEC, whose clean replacement watch lets
// EXEC commit the SET; with it the whole batch is kept, the retried INCR
// dirties the retried watch and EXEC aborts as the original ordering
// would. Regression test for the Codex P1 review finding on #4351.
func TestPipelineRetryQueuedUnwatchDoesNotClearWatchSafetyScan(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 5
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	watch := pipe.Do(ctx, "watch", "k")
	incr := pipe.Incr(ctx, "k")
	multi := pipe.Do(ctx, "multi")
	unwatch := pipe.Do(ctx, "unwatch")
	set := pipe.Set(ctx, "out", "v", 0)
	exec := pipe.Do(ctx, "exec")
	// The aborted EXEC surfaces as redis.Nil, exactly as the same
	// batch would without any disconnect: the transaction aborted
	// because the client's own INCR invalidated the watch.
	if _, err := pipe.Exec(ctx); !errors.Is(err, Nil) {
		t.Fatalf("Exec: %v, want redis.Nil (the retried EXEC must abort, like the same batch without a disconnect)", err)
	}
	if err := watch.Err(); err != nil {
		t.Fatalf("WATCH: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := unwatch.Err(); err != nil {
		t.Fatalf("UNWATCH: %v, want nil (the queued UNWATCH holds +QUEUED)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := exec.Err(); !errors.Is(err, Nil) {
		t.Fatalf("EXEC: %v, want redis.Nil (the aborted EXEC reply)", err)
	}
	// The retried transaction must abort: the answered INCR dirtied the
	// watch, so a clean reissued WATCH would silently commit the SET.
	if got := server.execAborted.Load(); got != 1 {
		t.Fatalf("EXEC aborted %d time(s), want 1 (the retried transaction must abort, not silently commit)", got)
	}
	if got := server.execWithMulti.Load(); got != 0 {
		t.Fatalf("EXEC committed %d time(s), want 0 (a queued UNWATCH must not clear the dirty watch)", got)
	}
	// Whole-batch retry re-runs the answered INCR (the pre-existing
	// whole-batch cost): it ran once on the first connection and once
	// on the retry, which is what dirties the retried watch.
	if got := server.incrs.Load(); got != 2 {
		t.Fatalf("server received %d INCR commands, want 2 (the answered INCR must be re-run to dirty the retried watch)", got)
	}
}

// TestPipelineRetryUnwatchInMultiHoldsExecElement: an UNWATCH inside MULTI
// is queued, so it holds the first element of EXEC's result array — unlike
// a WATCH, which the server rejects without queuing. Here MULTI, UNWATCH,
// a queued EVAL with a negative key count, EXEC and EVALSHA run, and the
// connection drops before EVALSHA's reply: without the fix the ordinal
// map skips the UNWATCH, so the EVAL's EXEC rejection lands on the
// UNWATCH's element and the EVAL reads a nil element as though it had
// run — the rebuild synthesises a SCRIPT LOAD and the retried EVALSHA
// executes the rejected script; with it the EVAL is recognised as
// rejected and the EVALSHA fails NOSCRIPT. Regression test for the Codex
// P2 review finding on #4351.
func TestPipelineRetryUnwatchInMultiHoldsExecElement(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	unwatch := pipe.Do(ctx, "unwatch")
	eval := pipe.Do(ctx, "eval", src, -1)
	exec := pipe.Do(ctx, "exec")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("Exec: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := unwatch.Err(); err != nil {
		t.Fatalf("UNWATCH: %v, want nil (the queued UNWATCH holds +QUEUED)", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v, want nil (the queued EVAL itself holds +QUEUED; its rejection is in the EXEC result array)", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to preload)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.evalshas.Load(); got != 1 {
		t.Fatalf("server received %d EVALSHA commands, want 1", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryHImportSetNoSuchFieldsetIsRetriedAfterDrop: an HIMPORT
// SET answered "no such fieldset" never wrote: its fieldset's server-side
// session was cleared by a RESET, and the transparent PREPARE/SET recovery
// runs only after a fully read attempt. Here the fieldset is prepared on
// the first connection, RESET clears the server-side session, and a
// pipeline HImportSet; Ping runs: the SET answers "no such fieldset" and
// the connection drops before the PING's reply, so the partial-read path
// returns before the recovery runs. Without the fix the retry sends only
// the PING and Exec keeps the fieldset error while the write never
// happens; with it the SET stays pending, the retry re-prepares and
// re-issues it, and Exec succeeds. Regression test for the Codex P2
// review finding on #4351.
func TestPipelineRetryHImportSetNoSuchFieldsetIsRetriedAfterDrop(t *testing.T) {
	server := newHImportRetryServer(t)
	// Answer the HIMPORT SET on the first connection, then drop before
	// the PING's reply: the SET is answered, the PING is the boundary.
	server.dropNextAfterSet = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	// Prepare the fieldset and then clear the server-side session
	// with RESET, both in one setup pipeline on the first connection:
	// the client's prepared flag still claims the fieldset is there,
	// so the pipeline below sends the SET without a PREPARE and the
	// server answers "no such fieldset".
	setup := client.Pipeline()
	prep := setup.HImportPrepare(ctx, "fs", "f1")
	reset := setup.Do(ctx, "reset")
	if _, err := setup.Exec(ctx); err != nil {
		t.Fatalf("setup Exec: %v", err)
	}
	if err := prep.Err(); err != nil {
		t.Fatalf("HImportPrepare: %v", err)
	}
	if err := reset.Err(); err != nil {
		t.Fatalf("RESET: %v", err)
	}

	pipe := client.Pipeline()
	set := pipe.HImportSet(ctx, "k", "fs", "v")
	ping := pipe.Ping(ctx)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-prepare and re-issue the SET)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("HIMPORT SET: %v", err)
	}
	if err := ping.Err(); err != nil {
		t.Fatalf("PING: %v", err)
	}
	server.mu.Lock()
	defer server.mu.Unlock()
	if got := len(server.sets); got != 2 {
		t.Fatalf("server received %d HIMPORT SET commands, want 2 (the answered SET must be re-issued by the retry)", got)
	}
	if got := server.sets[0]; got.conn != 1 || len(got.fields) != 0 {
		t.Fatalf("first SET ran on connection %d under PREPARE fields %v, want connection 1 with none (the server-side session was cleared)", got.conn, got.fields)
	}
	if got := server.sets[1]; got.conn != 2 || len(got.fields) != 1 || got.fields[0] != "f1" {
		t.Fatalf("second SET ran on connection %d under PREPARE fields %v, want connection 2 with [f1] (the retry must re-prepare before re-issuing it)", got.conn, got.fields)
	}
}

// TestPipelineRetryFailedDiscardKeepsQueuedBody: a DISCARD that fails with
// a transport error leaves its transaction open, so the queued commands
// never executed and still need a retry. Here MULTI, SET, EXISTS and
// DISCARD run, and the connection drops on the DISCARD: without the fix
// the body-needs-retry table closes the frame on the failed DISCARD, the
// tail filter drops the queued SET and EXISTS as answered (+QUEUED carries
// no error) and the retry commits a transaction missing them — Exec
// reports success while the SET never ran; with it the retry re-runs the
// whole open frame and the SET executes. Regression test for the Cursor
// Bugbot Medium review finding on #4351.
func TestPipelineRetryFailedDiscardKeepsQueuedBody(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 3
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "k", "v", 0)
	exists := pipe.Do(ctx, "exists", "k")
	discard := pipe.Do(ctx, "discard")
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-run the open frame and the GET after it)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := exists.Err(); err != nil {
		t.Fatalf("EXISTS: %v", err)
	}
	if err := discard.Err(); err != nil {
		t.Fatalf("DISCARD: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the queued SET must be re-queued by the retry)", got)
	}
	if got := server.exists.Load(); got != 2 {
		t.Fatalf("server received %d EXISTS commands, want 2 (the queued EXISTS must be re-queued by the retry)", got)
	}
	if got := server.discards.Load(); got != 1 {
		t.Fatalf("server received %d DISCARD commands, want 1 (the dropped DISCARD runs once, on the retry)", got)
	}
}

// TestPipelineRetryFailedDiscardKeepsExecOrdinals: a DISCARD that fails
// with a Redis error leaves its transaction open, so a later EXEC's result
// array still belongs to the frame's queued commands. Here MULTI, a queued
// EVAL with a negative key count, a NOPERM DISCARD and EXEC run, and the
// connection drops before EVALSHA's reply: without the fix the ordinal
// map closes the frame on the failed DISCARD, the EXEC maps no elements
// and the EVAL reads no rejection — the rebuild synthesises a SCRIPT LOAD
// and the retried EVALSHA executes the rejected script; with it the EVAL
// is recognised as rejected and the EVALSHA fails NOSCRIPT. Regression
// test for the Cursor Bugbot Medium review finding on #4351.
func TestPipelineRetryFailedDiscardKeepsExecOrdinals(t *testing.T) {
	server := newScriptRetryServer(t)
	server.failDiscard.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	eval := pipe.Do(ctx, "eval", src, -1)
	discard := pipe.Do(ctx, "discard")
	exec := pipe.Do(ctx, "exec")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	// The DISCARD's NOPERM is the batch's first error, so Exec reports
	// it — exactly as the same batch would without any disconnect. The
	// regression signal is the EVALSHA below: it must fail NOSCRIPT
	// (the rejected EVAL cached nothing), not execute on a synthesised
	// SCRIPT LOAD.
	if err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("Exec: %v, want NOPERM (the rejected DISCARD is the batch's first error)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := eval.Err(); err != nil {
		t.Fatalf("EVAL: %v, want nil (the queued EVAL itself holds +QUEUED; its rejection is in the EXEC result array)", err)
	}
	if err := discard.Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("DISCARD: %v, want NOPERM (the rejected DISCARD leaves the transaction open)", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if err := evalsha.Err(); err == nil || !strings.Contains(err.Error(), "NOSCRIPT") {
		t.Fatalf("EVALSHA: %v, want NOSCRIPT (the rejected EVAL cached nothing)", err)
	}
	if got := server.loads.Load(); got != 0 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 0 (a rejected EVAL establishes no cache entry to preload)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the rejected script must not be executed again)", got)
	}
	if got := server.evalshas.Load(); got != 1 {
		t.Fatalf("server received %d EVALSHA commands, want 1", got)
	}
	if got := server.noscripts.Load(); got != 1 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 1", got)
	}
}

// TestPipelineRetryFailedDiscardHidesSettledExec: a DISCARD that fails
// with a Redis error leaves its transaction open, so a later EXEC still
// settles it — the frame is completed, not open. Here MULTI, a queued
// SET, a NOPERM DISCARD and EXEC run, and the connection drops before
// GET's reply: without the fix the completed-frame scan drops the frame
// on the failed DISCARD and pipelineMultiOpen returns on it without
// looking at the settled EXEC, so the retry rewinds to MULTI and runs an
// empty transaction that overwrites the original EXEC results; with it
// the retry runs only the GET. Regression test for the Cursor Bugbot
// Medium review finding on #4351 (review 6019281722).
func TestPipelineRetryFailedDiscardHidesSettledExec(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.failDiscard.Store(true)
	server.dropAfter = 4
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	set := pipe.Set(ctx, "k", "v", 0)
	discard := pipe.Do(ctx, "discard")
	exec := pipe.Do(ctx, "exec")
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	// The DISCARD's NOPERM is the batch's first error, so Exec reports
	// it — exactly as the same batch would without any disconnect. The
	// regression signal is the EXEC below: its results must survive.
	if err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("Exec: %v, want NOPERM (the rejected DISCARD is the batch's first error)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := discard.Err(); err == nil || !strings.Contains(err.Error(), "NOPERM") {
		t.Fatalf("DISCARD: %v, want NOPERM (the rejected DISCARD leaves the transaction open)", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	vals, _ := exec.Val().([]interface{})
	if len(vals) != 1 {
		t.Fatalf("EXEC returned %d result(s), want 1 (a settled EXEC's results must not be overwritten by an empty retried transaction)", len(vals))
	}
	if got := server.execWithMulti.Load(); got != 1 {
		t.Fatalf("EXEC ran %d time(s) inside MULTI, want 1 (the retried batch must not rewind to the settled frame)", got)
	}
	if got := server.multiCount.Load(); got != 1 {
		t.Fatalf("server received %d MULTI commands, want 1 (the settled frame must not be re-opened)", got)
	}
	if got := server.discards.Load(); got != 1 {
		t.Fatalf("server received %d DISCARD commands, want 1 (the failed DISCARD must not be re-sent)", got)
	}
}

// TestPipelineRetryKeepsAnsweredWriteAfterPendingWrite: trimming answered
// successors is unsafe when a pending predecessor mutates data they
// depend on. Here MSET answers TRYAGAIN and the following SET answers
// OK: without the fix the retry runs only the MSET and leaves the key at
// the MSET's value while Exec reports success; with it the retry re-runs
// the SET after the MSET, in order, so the acknowledged final value
// stands. Regression test for the Codex P1 review finding on #4351
// (review 6019793639).
func TestPipelineRetryKeepsAnsweredWriteAfterPendingWrite(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.msetTryagainOnce.Store(true)
	server.trackValues = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mset := pipe.Do(ctx, "mset", "{m}a", 1, "{m}b", 2)
	set := pipe.Set(ctx, "{m}a", "3", 0)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-run the answered SET after the retried MSET)", err)
	}
	if err := mset.Err(); err != nil {
		t.Fatalf("MSET: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the answered SET must be re-run after the retried MSET, in order)", got)
	}
	// The final value proves the order: the retried MSET must not
	// overwrite the acknowledged SET.
	got, err := client.Get(ctx, "{m}a").Result()
	if err != nil || got != "3" {
		t.Fatalf("GET {m}a = %q, %v; want %q, nil (the retried MSET ran before the re-run SET, so the SET's value is final)", got, err, "3")
	}
}

// TestPipelineRetryRefreshesReadAfterRetriedWrite: a read answered after
// a retried write holds a pre-write value, so the retry must re-run it
// instead of keeping it. Here SET answers READONLY once (a read-only
// replica during promotion) and the following GET answers the old value:
// without the fix the retry runs only the SET and the GET keeps the old
// value while Exec reports success; with it the retry re-runs the GET
// after the SET, so the read is refreshed. Regression test for the Codex
// P1 review finding on #4351 (review 6038562056).
func TestPipelineRetryRefreshesReadAfterRetriedWrite(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.trackValues = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	// Pre-seed the key so the first attempt's GET answers the old value.
	if err := client.Set(ctx, "k", "old", 0).Err(); err != nil {
		t.Fatalf("seed SET: %v", err)
	}
	// The pipeline's SET is refused READONLY once; the GET after it must
	// be re-run on the retry, not keep its pre-write value.
	server.setReadonlyOnce.Store(true)
	pipe := client.Pipeline()
	set := pipe.Set(ctx, "k", "new", 0)
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must re-run the refused SET and refresh the GET after it)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "new" {
		t.Fatalf("GET = %q, %v; want %q, nil (the GET after the retried SET must be re-run, not keep its pre-write value)", got, err, "new")
	}
	if got := server.sets.Load(); got != 3 {
		t.Fatalf("server received %d SET commands, want 3 (seed + refused SET + retried SET)", got)
	}
}

// TestPipelineRetrySkipsRejectedQueuedScriptFlush: a SCRIPT FLUSH queued
// inside MULTI with a bogus mode is refused in the EXEC result array, so
// it never emptied the cache. Here a top-level SCRIPT LOAD, MULTI, a
// queued FLUSH bogus, EXEC and EVALSHA run, and the connection drops
// before EVALSHA's reply: without the fix the rebuild reconstructs the
// rejected FLUSH as a required prerequisite and its repeated syntax error
// fails the batch even though the EVALSHA succeeds; with it the FLUSH is
// skipped and the EVALSHA runs on the reissued LOAD. Regression test for
// the Codex P2 review finding on #4351 (review 6019793639).
func TestPipelineRetrySkipsRejectedQueuedScriptFlush(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	const src = "return 1"
	sum := sha1.Sum([]byte(src))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	load := pipe.ScriptLoad(ctx, src)
	multi := pipe.Do(ctx, "multi")
	flush := pipe.Do(ctx, "script", "flush", "bogus")
	exec := pipe.Do(ctx, "exec")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (a FLUSH the EXEC rejected must not be replayed as a required prerequisite)", err)
	}
	if got := load.Val(); got != sha {
		t.Fatalf("SCRIPT LOAD = %q, want %q", got, sha)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := flush.Err(); err != nil {
		t.Fatalf("FLUSH: %v, want nil (the queued FLUSH itself holds +QUEUED; its rejection is in the EXEC result array)", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if err := evalsha.Err(); err != nil {
		t.Fatalf("EVALSHA: %v, want nil (the script stayed cached: the rejected FLUSH never emptied it)", err)
	}
	if got := server.flushes.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT FLUSH commands, want 1 (a FLUSH the EXEC rejected must not be replayed)", got)
	}
	if got := server.loads.Load(); got != 2 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 2 (the answered LOAD must still be reissued before the EVALSHA)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetrySkipsRejectedRawSessionCommand: a raw session command
// the server rejected established no state, so it must not be rebuilt as
// a required restoration. Here a raw SELECT 99 is rejected as out of
// range and the connection drops before GET's reply: without the fix the
// rebuild clones the SELECT and pipelineRequiredSyntheticErr promotes its
// repeated rejection into the batch error even though GET succeeds; with
// it the SELECT is skipped and the retry runs only the GET. Regression
// test for the Codex P2 review finding on #4351 (review 6019793639).
func TestPipelineRetrySkipsRejectedRawSessionCommand(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := NewRawCmd(ctx, "SELECT", 99)
	if err := pipe.BatchProcess(ctx, sel); err != nil {
		t.Fatalf("BatchProcess: %v", err)
	}
	get := pipe.Get(ctx, "k")
	_, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatalf("Exec: %v, want nil (a rejected SELECT establishes no state to restore)", err)
	}
	if got := string(sel.Val()); !strings.Contains(got, "DB index is out of range") {
		t.Fatalf("raw SELECT Val() = %q, want it to keep the out-of-range error frame", got)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("raw SELECT Err(): %v, want nil (a raw command's error frame stays in Val() by contract)", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.selects.Load(); got != 1 {
		t.Fatalf("server received %d SELECT commands, want 1 (a rejected SELECT must not be reissued on the retry connection)", got)
	}
}

// TestPipelineRetryAnsweredWaitNotKeptAfterPendingWrite: a barrier is
// not a data write, so the ordered-retry pass must not keep an answered
// WAIT in the retry tail. Here INCR answers 1, MSET answers TRYAGAIN
// and WAIT answers 1, all replies read: without the fix the pass keeps
// the WAIT (it carries no readonly flag in the COMMAND metadata), the
// WAIT-in-tail check returns the whole batch, and the acknowledged INCR
// runs twice while Exec reports success; with it the WAIT stays dropped
// as answered, the retry runs only the MSET, and the barrier reissue
// path re-runs the WAIT after it. Regression test for the Cursor
// Bugbot finding and the Codex P1 finding on #4351 (reviews 6020480547
// and 6020502814).
func TestPipelineRetryAnsweredWaitNotKeptAfterPendingWrite(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 10
	server.msetTryagainOnce.Store(true)
	server.trackValues = true
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	incr := pipe.Incr(ctx, "counter")
	mset := pipe.Do(ctx, "mset", "{m}a", 1, "{m}b", 2)
	wait := pipe.Do(ctx, "wait", 1, 1000)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must run only the MSET, with its WAIT reissued after it)", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil", got, err)
	}
	if err := mset.Err(); err != nil {
		t.Fatalf("MSET: %v", err)
	}
	if got, err := wait.Result(); err != nil || got != int64(1) {
		t.Fatalf("WAIT = %v, %v; want 1, nil", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCR commands, want 1 (the acknowledged INCR must not run again)", got)
	}
	if got := server.waits.Load(); got != 2 {
		t.Fatalf("server received %d WAIT commands, want 2 (the answered WAIT must be reissued after the retried MSET)", got)
	}
	// The retried MSET must have run: its values are the only record.
	got, err := client.Get(ctx, "{m}a").Result()
	if err != nil || got != "1" {
		t.Fatalf("GET {m}a = %q, %v; want %q, nil (the retried MSET must have applied)", got, err, "1")
	}
}

// TestPipelineRetryRejectedExecPreservesFrameForDiscard: an EXEC the
// server rejects leaves the transaction open, so a DISCARD after it
// still drops the queued commands. Here SELECT 2, MULTI, a queued
// SELECT 1, a raw EXEC refused with NOPERM, DISCARD and SET run, and
// the connection drops before SET's reply: without the fix every frame
// scan treats the rejected EXEC as a terminator, so the rebuild
// reissues the discarded SELECT 1 as a session prerequisite and the
// retried SET lands in DB 1; with it the frame survives the rejected
// EXEC, the DISCARD marks the queued SELECT discarded, and the retry
// runs only the reissued SELECT 2 and the SET. Regression test for the
// Codex P1 finding on #4351 (review 6020502814).
func TestPipelineRetryRejectedExecPreservesFrameForDiscard(t *testing.T) {
	server := newConnStateRetryServer(t)
	// Answer SELECT 2, MULTI, the queued SELECT, the rejected EXEC
	// and the DISCARD, then drop before SET's reply.
	server.dropAfter = 5
	server.failExec.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	sel := pipe.Select(ctx, 2)
	multi := pipe.Do(ctx, "multi")
	selInTx := pipe.Select(ctx, 1)
	exec := NewRawCmd(ctx, "EXEC")
	if err := pipe.BatchProcess(ctx, exec); err != nil {
		t.Fatalf("BatchProcess: %v", err)
	}
	discard := pipe.Do(ctx, "discard")
	set := pipe.Set(ctx, "out", "v", 0)
	// The rejected EXEC is a raw command, so its NOPERM carries no
	// error: Exec reports success either way, and the proof of the
	// fix is where SET landed, not the error.
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := selInTx.Err(); err != nil {
		t.Fatalf("SELECT in tx: %v, want nil (the queued SELECT itself holds +QUEUED; it never ran)", err)
	}
	if got := string(exec.Val()); !strings.Contains(got, "NOPERM") {
		t.Fatalf("raw EXEC Val() = %q, want it to keep the NOPERM error frame", got)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("raw EXEC Err(): %v, want nil (a raw command's error frame stays in Val() by contract)", err)
	}
	if err := discard.Err(); err != nil {
		t.Fatalf("DISCARD: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if got := server.selects.Load(); got != 3 {
		t.Fatalf("server received %d SELECT commands, want 3 (the answered SELECT 2, the queued SELECT 1, and the reissued SELECT 2 — the discarded SELECT 1 must not be reissued)", got)
	}
	if got := server.sets.Load(); got != 1 {
		t.Fatalf("server received %d SET commands, want 1 (only the retried SET: the first attempt's SET got no reply)", got)
	}
	if got := server.setDB.Load(); got != 2 {
		t.Fatalf("SET landed in DB %d, want 2 (the retried SET must run under the reissued SELECT 2, not the discarded SELECT 1)", got)
	}
	if got := server.multiCount.Load(); got != 1 {
		t.Fatalf("server received %d MULTI commands, want 1 (the discarded frame must not be re-opened)", got)
	}
	if got := server.execs.Load(); got != 1 {
		t.Fatalf("server received %d EXEC commands, want 1 (the rejected EXEC must not be re-run)", got)
	}
	if got := server.execWithMulti.Load(); got != 0 {
		t.Fatalf("EXEC ran %d time(s) inside MULTI, want 0 (the rejected EXEC never executed the transaction)", got)
	}
	if got := server.discards.Load(); got != 1 {
		t.Fatalf("server received %d DISCARD commands, want 1 (the DISCARD that dropped the frame must not be re-sent)", got)
	}
}

// BenchmarkPipelineRetrySuccessBookkeeping measures the retry bookkeeping
// a successful pipeline allocates: a 10,000-command batch that answers
// everything first try. Fresh commands carry no stale errors, and a
// success records nothing for a later attempt, so the stale-error set,
// the answered set and the per-attempt dispatched set must stay off this
// path. Regression benchmark for the Codex P2 review finding on #4351
// (review 6038562056).
func BenchmarkPipelineRetrySuccessBookkeeping(b *testing.B) {
	server := newConnStateRetryServer(b)
	server.dropAfter = 1 << 30
	client := newPartialRetryClient(b, server.ln.Addr().String())

	ctx := context.Background()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		pipe := client.Pipeline()
		for j := 0; j < 10000; j++ {
			pipe.Get(ctx, "k")
		}
		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatalf("Exec: %v", err)
		}
	}
}

// TestPipelineRetryRetrimsAnsweredReissuedBarrier: a direct Client
// retrying twice during slot migration must not replay an answered
// WAIT the first retry reissued. The first attempt answers
// [TRYAGAIN, TRYAGAIN, 1] to EVAL, MGET and WAIT; the first retry
// reissues the WAIT (marked synthetic) so it is re-measured after the
// re-run EVAL. On the second attempt the EVAL succeeds while the MGET
// still answers TRYAGAIN: the answered WAIT must be trimmed like any
// other answered command, so the third attempt retries only the MGET.
// Without the fix the synthetic mark keeps the answered WAIT in the
// tail, the WAIT-in-tail check returns the whole batch, and the third
// attempt runs the EVAL a second time — the counter increments twice
// while Exec still reports success. Regression test for the Codex P1
// finding on #4351 (review 6042465465).
func TestPipelineRetryRetrimsAnsweredReissuedBarrier(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.evalTryagainOnce.Store(true)
	server.mgetTryagainCount.Store(2)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Do(ctx, "eval", "return redis.call('INCR', KEYS[1]) + redis.call('INCR', KEYS[2])", 2, "k1", "k2")
	mget := pipe.MGet(ctx, "k1", "k2")
	wait := pipe.Do(ctx, "wait", 1, 1000)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil", err)
	}
	if got, err := eval.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVAL = %v, %v; want 1, nil", got, err)
	}
	if got, err := mget.Result(); err != nil || len(got) != 2 || got[0] != "a" || got[1] != "b" {
		t.Fatalf("MGET = %v, %v; want [a b], nil", got, err)
	}
	if got, err := wait.Result(); err != nil || got != int64(1) {
		t.Fatalf("WAIT = %v, %v; want 1, nil", got, err)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server executed %d EVAL commands, want 1 (the answered EVAL must not run again on the third attempt)", got)
	}
	if got := server.mgets.Load(); got != 3 {
		t.Fatalf("server received %d MGET commands, want 3 (one per attempt until it succeeds)", got)
	}
	if got := server.waits.Load(); got != 2 {
		t.Fatalf("server received %d WAIT commands, want 2 (the first attempt's and the first retry's — the answered WAIT must not be re-measured again)", got)
	}
}

// TestPipelineRetryBarrierReissuedBeforeLaterSessionChange: an answered
// WAIT the retry reissues must run at its original position, not at the
// end of the retry: MSET; WAIT 1 1000; AUTH limited pw; GET k reads
// [NOREPLICAS, ack, ack, drop], so the retry is the MSET, the AUTH and
// the GET with the WAIT re-measured — and the WAIT ran while the
// default user could run it, so it must run before the AUTH switches
// to the limited user, which cannot run WAIT at all. Appending it at
// the end would run it as limited and fail the batch with NOPERM.
// Regression test for the Codex P2 finding on #4351.
func TestPipelineRetryBarrierReissuedBeforeLaterSessionChange(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 0
	server.msetNoreplicasOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mset := pipe.Do(ctx, "mset", "k", "v")
	wait := pipe.Do(ctx, "wait", 1, 1000)
	auth := pipe.Do(ctx, "auth", "limited", "pw")
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the reissued WAIT must run before the AUTH to the limited user)", err)
	}
	if err := mset.Err(); err != nil {
		t.Fatalf("MSET: %v", err)
	}
	if got, err := wait.Result(); err != nil || got != int64(1) {
		t.Fatalf("WAIT = %v, %v; want 1, nil (the reissued barrier must measure as the default user, before the AUTH)", got, err)
	}
	if err := auth.Err(); err != nil {
		t.Fatalf("AUTH: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.waits.Load(); got != 2 {
		t.Fatalf("server received %d WAIT commands, want 2 (one per attempt: the answered WAIT is re-measured on the retry)", got)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the retry must run on a fresh connection)", got)
	}
}

// TestPipelineRetryEvalMsetArityRuntimeErrorIsPreloaded: a Lua runtime
// error reports the same "wrong number of arguments" prefix EVAL itself
// reports when refused before execution — but the script ran and cached
// before the pcall failed, so the retry must preload its source before
// the retried EVALSHA. Only an EVAL whose own arity is wrong is refused
// before execution. Regression test for the Codex P2 finding on #4351
// (review 6042465465).
func TestPipelineRetryEvalMsetArityRuntimeErrorIsPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	sum := sha1.Sum([]byte(runtimeMsetArityErrScript))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Eval(ctx, runtimeMsetArityErrScript, []string{"k1"}, "x")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "wrong number of arguments for 'mset' command") {
		t.Fatalf("Exec: %v, want the EVAL's MSET arity runtime error (it is the first command error)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "wrong number of arguments for 'mset' command") {
		t.Fatalf("EVAL: %v, want the MSET arity runtime error", err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil (the EVAL ran and cached the script before failing, so the retry must preload it)", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (a runtime-failing EVAL populated the cache)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestPipelineRetryEvalByteNumkeysIsPreloaded: evalNumKeysValid must
// accept the byte-string and pointer forms evalScriptSource already
// does, or an EVAL sent with a []byte numkeys looks invalid and its
// runtime error is classified as a pre-execution rejection: the script
// actually ran and cached, but the retry never preloads it and the
// retried EVALSHA fails NOSCRIPT. Regression test for the Cursor
// Bugbot Medium finding on #4351 (review 6042537704).
func TestPipelineRetryEvalByteNumkeysIsPreloaded(t *testing.T) {
	server := newScriptRetryServer(t)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	sum := sha1.Sum([]byte(runtimeIntErrScript))
	sha := hex.EncodeToString(sum[:])

	ctx := context.Background()
	pipe := client.Pipeline()
	eval := pipe.Do(ctx, "eval", runtimeIntErrScript, []byte("1"), "bad")
	evalsha := pipe.EvalSha(ctx, sha, nil)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "value is not an integer") {
		t.Fatalf("Exec: %v, want the EVAL's runtime integer error (it is the first command error)", err)
	}
	if err := eval.Err(); err == nil || !strings.Contains(err.Error(), "value is not an integer") {
		t.Fatalf("EVAL: %v, want the runtime integer error", err)
	}
	if got, err := evalsha.Result(); err != nil || got != int64(1) {
		t.Fatalf("EVALSHA = %v, %v; want 1, nil (the EVAL ran and cached the script before failing, so the retry must preload it)", got, err)
	}
	if got := server.loads.Load(); got != 1 {
		t.Fatalf("server received %d SCRIPT LOAD commands, want 1 (a runtime-failing EVAL populated the cache)", got)
	}
	if got := server.evals.Load(); got != 1 {
		t.Fatalf("server received %d EVAL commands, want 1 (the answered script must not be executed again)", got)
	}
	if got := server.noscripts.Load(); got != 0 {
		t.Fatalf("server returned NOSCRIPT %d time(s), want 0", got)
	}
}

// TestEvalNumKeysValid: the numkeys argument of an EVAL may arrive as a
// string, an integer, a byte slice or a string pointer — WriteArg sends
// all of them the same way — so the validator must accept every form
// evalScriptSource already treats as a script argument.
func TestEvalNumKeysValid(t *testing.T) {
	ctx := context.Background()
	strNumkeys := "1"
	strBad := "bad"
	emptyStr := ""
	var nilStr *string
	var nilBytes []byte
	cases := []struct {
		name string
		args []any
		want bool
	}{
		{"string", []any{"eval", "return 1", "1", "k1"}, true},
		{"int", []any{"eval", "return 1", 1, "k1"}, true},
		{"int64", []any{"eval", "return 1", int64(1), "k1"}, true},
		{"byte slice", []any{"eval", "return 1", []byte("1"), "k1"}, true},
		{"string pointer", []any{"eval", "return 1", &strNumkeys, "k1"}, true},
		{"zero keys", []any{"eval", "return 1", 0}, true},
		{"missing numkeys", []any{"eval", "return 1"}, false},
		{"non-integer string", []any{"eval", "return 1", "bad", "k1"}, false},
		{"non-integer byte slice", []any{"eval", "return 1", []byte("x"), "k1"}, false},
		{"non-integer string pointer", []any{"eval", "return 1", &strBad, "k1"}, false},
		{"negative count", []any{"eval", "return 1", -1, "k1"}, false},
		{"count past the arguments", []any{"eval", "return 1", 2, "k1"}, false},
		{"nil string pointer", []any{"eval", "return 1", nilStr, "k1"}, false},
		{"nil byte slice", []any{"eval", "return 1", nilBytes, "k1"}, false},
		{"empty string", []any{"eval", "return 1", emptyStr, "k1"}, false},
		{"float", []any{"eval", "return 1", 1.0, "k1"}, false},
		// Redis's integer parser rejects the wire forms strconv.Atoi
		// accepts: a leading zero ("01", "00") or a plus sign ("+1")
		// is not a well-formed count, so an EVAL refused for it
		// cached nothing.
		{"leading zero string", []any{"eval", "return 1", "01", "k1"}, false},
		{"double zero string", []any{"eval", "return 1", "00", "k1"}, false},
		{"plus sign string", []any{"eval", "return 1", "+1", "k1"}, false},
		{"leading zero byte slice", []any{"eval", "return 1", []byte("01"), "k1"}, false},
		{"plus sign byte slice", []any{"eval", "return 1", []byte("+1"), "k1"}, false},
		{"zero string", []any{"eval", "return 1", "0", "k1"}, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cmd := NewCmd(ctx, tc.args...)
			if got := evalNumKeysValid(cmd); got != tc.want {
				t.Fatalf("evalNumKeysValid(%v) = %v, want %v", tc.args, got, tc.want)
			}
		})
	}
}

// TestPipelineRetryRestoresUnconsumedTrailingOneShotFlag: on a dedicated
// connection with CLIENT TRACKING ON OPTIN, a trailing CLIENT CACHING
// YES that answered successfully is still armed when the batch ends —
// it has no follower in the batch to consume it. If the SET before it
// is retried, the retried SET consumes the armed flag on the same
// connection, so the next GET after the retry is no longer opt-in and
// produces no tracking invalidation. The retry must re-send the flag
// at the end, so it is armed again for the command after the retry.
// A flag whose follower was already answered stays dropped: it was
// consumed by that follower (see
// TestPipelineRetryDoesNotReissueOneShotClientCaching). Regression test
// for the Codex P2 finding on #4351 (review 6042465465).
func TestPipelineRetryRestoresUnconsumedTrailingOneShotFlag(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.setNoreplicasOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())
	conn := client.Conn()
	t.Cleanup(func() { _ = conn.Close() })

	ctx := context.Background()
	if err := conn.Do(ctx, "client", "tracking", "on", "optin").Err(); err != nil {
		t.Fatalf("CLIENT TRACKING: %v", err)
	}
	pipe := conn.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	caching := pipe.Do(ctx, "client", "caching", "yes")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := caching.Err(); err != nil {
		t.Fatalf("CLIENT CACHING: %v", err)
	}
	get := conn.Get(ctx, "k")
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.sets.Load(); got != 2 {
		t.Fatalf("server received %d SET commands, want 2 (the NOREPLICAS SET must be retried once)", got)
	}
	if got := server.cachingCmds.Load(); got != 2 {
		t.Fatalf("server received %d CLIENT CACHING commands, want 2 (the answered trailing flag must be re-sent at the end of the retry)", got)
	}
	if got := server.optinGets.Load(); got != 1 {
		t.Fatalf("server saw %d opt-in GETs, want 1 (the GET after the retry must still run opt-in)", got)
	}
}

// TestPipelineRetrySwapdbVetoDoesNotSuppressTailSelect: an answered
// SWAPDB past the boundary must not veto the retirement an answered
// SELECT after it requires: PING; SET out v; SWAPDB 0 1; SELECT 1 reads
// [PONG, NOREPLICAS, OK, OK], so the retry must run on a fresh
// connection — the SWAPDB is server-global, so its swap-back clone stays
// valid on the replacement connection, while the SELECT's session state
// does not. Without the fix the retry reuses the connection still
// selecting DB 1 and the SET lands there. Regression test for the Codex
// P1 finding on #4351 (review 6042465465 follow-up, round 40).
func TestPipelineRetrySwapdbVetoDoesNotSuppressTailSelect(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.setNoreplicasOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	ping := pipe.Ping(ctx)
	set := pipe.Set(ctx, "out", "v", 0)
	swapdb := pipe.Do(ctx, "swapdb", 0, 1)
	sel := pipe.Select(ctx, 1)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must run the SET in DB 0 on a fresh connection)", err)
	}
	if err := ping.Err(); err != nil {
		t.Fatalf("PING: %v", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := swapdb.Err(); err != nil {
		t.Fatalf("SWAPDB: %v", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	if got := server.setsDB0.Load(); got != 1 {
		t.Fatalf("server executed %d SETs in DB 0, want 1 (the retried SET must run in the database it was issued for)", got)
	}
	if got := server.setsDB1.Load(); got != 0 {
		t.Fatalf("server executed %d SETs in DB 1, want 0 (the answered SELECT after the SWAPDB must not leak into the retry suffix)", got)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the retry must start on a fresh connection, not the one the first attempt left in DB 1)", got)
	}
}

// TestPipelineRetryTrailingOneShotFlagDoesNotOptInRetriedRead: a trailing
// answered CLIENT CACHING YES is still armed when the batch ends — it
// has no follower in the batch to consume it — so on a pooled connection
// the retry must start on a fresh one: the first retried command would
// otherwise consume the armed flag and run opt-in although it was never
// issued as such. Here the retried GET answers TRYAGAIN first and the
// flag follows it, so the GET is the first retried command. The flag is
// still re-sent at the end of the retry, so it ends armed for the
// command after the batch. Regression test for the Cursor Bugbot High
// finding on #4351 (round 40).
func TestPipelineRetryTrailingOneShotFlagDoesNotOptInRetriedRead(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.getTryagainOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	get := pipe.Get(ctx, "k")
	caching := pipe.Do(ctx, "client", "caching", "yes")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if err := caching.Err(); err != nil {
		t.Fatalf("CLIENT CACHING: %v", err)
	}
	if got := server.optinGets.Load(); got != 0 {
		t.Fatalf("server saw %d opt-in GETs, want 0 (the retried GET must not consume the armed flag)", got)
	}
	if got := server.cachingCmds.Load(); got != 2 {
		t.Fatalf("server received %d CLIENT CACHING commands, want 2 (the answered trailing flag must be re-sent at the end of the retry)", got)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the retry must start on a fresh connection, not the one still carrying the armed flag)", got)
	}
}

// TestPipelineRetrySessionRetireNeedsScheduledRetry: on a dedicated Conn
// with retries disabled (MaxRetries -1), a fully read SET out v; SELECT
// 1 receiving [NOREPLICAS, OK] must not retire the connection: no retry
// can run, so the retirement only turns the otherwise synchronized Conn
// into a persistent BadConnError for the next command. The follow-up SET
// must succeed and run in DB 1, the database the answered SELECT left
// selected. Regression test for the Codex P2 finding on #4351 (round 40).
func TestPipelineRetrySessionRetireNeedsScheduledRetry(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.setNoreplicasOnce.Store(true)
	client := newMaxRetriesClient(t, server.ln.Addr().String(), -1)
	conn := client.Conn()
	t.Cleanup(func() { _ = conn.Close() })

	ctx := context.Background()
	pipe := conn.Pipeline()
	set := pipe.Set(ctx, "out", "v", 0)
	sel := pipe.Select(ctx, 1)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOREPLICAS") {
		t.Fatalf("Exec: %v, want the NOREPLICAS error (no retry is scheduled, so the batch ends on the first attempt)", err)
	}
	if err := set.Err(); err == nil || !strings.Contains(err.Error(), "NOREPLICAS") {
		t.Fatalf("SET: %v, want the NOREPLICAS error", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	after := conn.Set(ctx, "after", "v", 0)
	if err := after.Err(); err != nil {
		t.Fatalf("follow-up SET: %v, want nil (the connection must not be retired when no retry is scheduled)", err)
	}
	if got := server.setsDB1.Load(); got != 1 {
		t.Fatalf("server executed %d SETs in DB 1, want 1 (the follow-up SET must run in the database the answered SELECT left selected)", got)
	}
}

// TestPipelineRetrySessionRetireSkippedOnFinalAttempt: the same gate on
// the final allowed attempt — a pooled client with one retry left runs
// the first attempt, retires, and retries; when that attempt fails the
// same way, no further dispatch is scheduled, so the connection must
// survive for the next operation. The MGET answers TRYAGAIN on both
// attempts, so the SELECT after it is answered session state past the
// boundary on each. A follow-up pipeline must reuse the second connection
// instead of dialing a third. Regression test for the Codex P2 finding
// on #4351 (round 40).
func TestPipelineRetrySessionRetireSkippedOnFinalAttempt(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.mgetTryagainCount.Store(2)
	client := newMaxRetriesClient(t, server.ln.Addr().String(), 1)

	ctx := context.Background()
	pipe := client.Pipeline()
	mget := pipe.MGet(ctx, "k")
	sel := pipe.Select(ctx, 1)
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "TRYAGAIN") {
		t.Fatalf("Exec: %v, want the TRYAGAIN error (both attempts fail the MGET)", err)
	}
	if err := mget.Err(); err == nil || !strings.Contains(err.Error(), "TRYAGAIN") {
		t.Fatalf("MGET: %v, want the TRYAGAIN error", err)
	}
	if err := sel.Err(); err != nil {
		t.Fatalf("SELECT: %v", err)
	}
	// A follow-up pipeline reuses the pipeline pool, so it must run on
	// the second connection: a single-command PING would dial the main
	// pool instead and prove nothing about the retired connection.
	pipe2 := client.Pipeline()
	pipe2.Ping(ctx)
	if _, err := pipe2.Exec(ctx); err != nil {
		t.Fatalf("follow-up pipeline: %v, want nil", err)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the final attempt must not retire the connection it ran on)", got)
	}
}

// TestPipelineRetryBarrierAfterRestorationPrefix: a reissued barrier
// must run after the restoration prefix, not before it. With SET out v;
// WAIT 1 1000; SWAPDB 0 1 receiving [NOREPLICAS, 1, OK], the rebuild
// creates a swap-back clone whose retryPos is 2; the old insertion scan
// met that clone before the pending SET and produced WAIT; SWAPDB; SET;
// SWAPDB, so WAIT measured the old replication offset and Exec could
// succeed although the retried SET was never covered by a barrier. The
// barrier is ordered against the retained logical tail instead: the
// swap-back clone first, then SET, WAIT, SWAPDB. Regression test for
// the Codex P1 finding on #4351 (round 40).
func TestPipelineRetryBarrierAfterRestorationPrefix(t *testing.T) {
	ctx := context.Background()
	noreplicas := proto.RedisError("NOREPLICAS Not enough replicas to apply the write")
	set := NewStringCmd(ctx, "set", "out", "v")
	set.SetErr(noreplicas)
	wait := NewIntCmd(ctx, "wait", 1, 1000)
	wait.SetVal(1)
	swapdb := NewStringCmd(ctx, "swapdb", 0, 1)
	cmds := []Cmder{set, wait, swapdb}
	suffix := pipelineRetrySuffix(ctx, cmds, noreplicas, nil)
	want := []string{"swapdb", "set", "wait", "swapdb"}
	if len(suffix) != len(want) {
		t.Fatalf("suffix has %d commands, want %d", len(suffix), len(want))
	}
	for i, name := range want {
		if got := suffix[i].Name(); got != name {
			t.Fatalf("suffix[%d] = %q, want %q (full order: %v)", i, got, name, cmdNames(suffix))
		}
	}
	if suffix[0] == swapdb {
		t.Fatalf("suffix[0] is the original SWAPDB, want the swap-back clone (the original must stay in the tail to swap back)")
	}
	if suffix[2] != wait {
		t.Fatalf("suffix[2] is not the original WAIT, want the original (the reissued barrier must re-measure, not keep the first count)")
	}
	if suffix[3] != swapdb {
		t.Fatalf("suffix[3] is not the original SWAPDB, want the original (the kept swap must re-apply at its own position)")
	}
}

func cmdNames(cmds []Cmder) []string {
	names := make([]string, len(cmds))
	for i, cmd := range cmds {
		names[i] = cmd.Name()
	}
	return names
}

// TestPipelineRetryBarrierMergeIsLinear: one pending write followed by
// many answered barriers must not rescan the growing retry per barrier:
// the old insertion pass restarted the search for every barrier, so
// 100,000 barriers cost about 2.4 seconds of quadratic work. The single
// ordered merge pass below runs in milliseconds; the bound only guards
// against the quadratic behaviour coming back, with ample headroom (see
// TestPipelineRetrySuffixNoTxIsLinear for the same shape). Regression
// test for the Codex P2 finding on #4351 (round 40).
func TestPipelineRetryBarrierMergeIsLinear(t *testing.T) {
	const n = 100000
	ctx := context.Background()
	noreplicas := proto.RedisError("NOREPLICAS Not enough replicas to apply the write")
	cmds := make([]Cmder, 0, n+1)
	set := NewStringCmd(ctx, "set", "out", "v")
	set.SetErr(noreplicas)
	cmds = append(cmds, set)
	for i := 0; i < n; i++ {
		wait := NewIntCmd(ctx, "wait", 1, 1000)
		wait.SetVal(1)
		cmds = append(cmds, wait)
	}
	start := time.Now()
	suffix := pipelineRetrySuffix(ctx, cmds, noreplicas, nil)
	elapsed := time.Since(start)
	if len(suffix) != n+1 {
		t.Fatalf("suffix has %d commands, want %d (the pending SET plus every reissued barrier)", len(suffix), n+1)
	}
	if suffix[0] != set {
		t.Fatalf("suffix[0] is not the pending SET, want it first (every barrier follows the write it guards)")
	}
	for i := 1; i <= n; i++ {
		if suffix[i] != cmds[i] {
			t.Fatalf("suffix[%d] is not the original WAIT, want the barriers in original order", i)
		}
	}
	// Machine-dependent by nature: the single merge pass runs in
	// milliseconds on this workload, while the per-barrier rescan it
	// replaced took seconds. The bound only guards against the
	// quadratic behaviour coming back, with ample headroom.
	if elapsed > time.Second {
		t.Fatalf("pipelineRetrySuffix took %v for %d barriers, want < 1s (the per-barrier rescan is quadratic)", elapsed, n)
	}
}

// TestPipelineRetryStickyConnSessionRetireSkipped: on a dedicated Conn
// with retries remaining, a SET answering NOREPLICAS followed by an
// answered CLIENT SETNAME must not retire the connection: there is no
// fresh connection to start on — StickyConnPool records the mark as a
// persistent BadConnError, so the scheduled retry cannot run at all
// and the dedicated Conn stays unusable. The retry re-runs the suffix
// on the same connection instead: changing the client name cannot
// affect the SET's database, so the batch recovers. Regression test
// for the Cursor Bugbot High and Codex P2 findings on #4351 (round 41).
func TestPipelineRetryStickyConnSessionRetireSkipped(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.setNoreplicasOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())
	conn := client.Conn()
	t.Cleanup(func() { _ = conn.Close() })

	ctx := context.Background()
	pipe := conn.Pipeline()
	set := pipe.Set(ctx, "k", "v", 0)
	setname := pipe.Do(ctx, "client", "setname", "n")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retry must run on the same dedicated connection, not retire it)", err)
	}
	if err := set.Err(); err != nil {
		t.Fatalf("SET: %v", err)
	}
	if err := setname.Err(); err != nil {
		t.Fatalf("CLIENT SETNAME: %v", err)
	}
	if got := server.setsDB0.Load(); got != 1 {
		t.Fatalf("server executed %d SETs in DB 0, want 1 (the retried SET must run in the database it was issued for)", got)
	}
	if got := server.conns.Load(); got != 1 {
		t.Fatalf("server saw %d connections, want 1 (the retry must reuse the dedicated connection)", got)
	}
	after := conn.Set(ctx, "after", "v", 0)
	if err := after.Err(); err != nil {
		t.Fatalf("follow-up SET: %v, want nil (the dedicated Conn must stay usable after the retry)", err)
	}
}

// TestPipelineRetryPendingPingDoesNotInvalidateAnsweredWrite: a refused
// PING is not a pending write — PING carries no readonly flag in the
// COMMAND metadata, yet it writes nothing, so it cannot invalidate the
// data results after it. With ECHO ready; PING; INCR counter receiving
// [ready, LOADING, 1], the answered INCR must stay dropped: the retry
// re-runs only the PING, and the counter increments once. Regression
// test for the Codex P1 finding on #4351 (round 41).
func TestPipelineRetryPendingPingDoesNotInvalidateAnsweredWrite(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.pingLoadingOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	echo := pipe.Do(ctx, "echo", "ready")
	ping := pipe.Ping(ctx)
	incr := pipe.Incr(ctx, "counter")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the retried PING must succeed once the server has recovered)", err)
	}
	if err := echo.Err(); err != nil {
		t.Fatalf("ECHO: %v", err)
	}
	if err := ping.Err(); err != nil {
		t.Fatalf("PING: %v", err)
	}
	if got, err := incr.Result(); err != nil || got != 1 {
		t.Fatalf("INCR = %d, %v; want 1, nil (the answered INCR must not be re-run)", got, err)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server executed %d INCRs, want 1 (a refused PING is not a pending write)", got)
	}
}

// TestPipelineRetryQueuedSwapdbExecErrorNeedsNoInverse: a SWAPDB queued
// inside MULTI/EXEC answers +QUEUED with a nil error, but its EXEC
// result element holds the rejection — SWAPDB 99 0 on a server with
// fewer than 100 databases never swapped, so the whole-batch retry
// owes it no inverse. With MULTI; SWAPDB 99 0; EXEC; WAIT 1 1000 and a
// disconnect after EXEC's reply, the retry must re-dispatch the batch
// unchanged: a synthesised standalone SWAPDB inverse would be rejected
// on the healthy retry and promoted into the batch error. Regression
// test for the Codex P2 finding on #4351 (round 41).
func TestPipelineRetryQueuedSwapdbExecErrorNeedsNoInverse(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.dropAfterExecReplyOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	multi := pipe.Do(ctx, "multi")
	swapdb := pipe.Do(ctx, "swapdb", 99, 0)
	exec := pipe.Do(ctx, "exec")
	wait := pipe.Do(ctx, "wait", 1, 1000)
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the swap never ran, so the retry owes it no inverse)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := swapdb.Err(); err != nil {
		t.Fatalf("SWAPDB: %v", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if err := wait.Err(); err != nil {
		t.Fatalf("WAIT: %v", err)
	}
	if got := server.swapdbs.Load(); got != 2 {
		t.Fatalf("server received %d SWAPDB commands, want 2 (the first attempt plus the whole-batch re-run, with no synthesised inverse)", got)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the retry must start on a fresh connection after the disconnect)", got)
	}
}

// TestPipelineRetryQueuedSwapdbExecErrorNeedsNoSuffixInverse: the same
// EXEC-element check in the suffix inverse collector — with MSET k v;
// MULTI; SWAPDB 99 0; EXEC; GET k receiving [TRYAGAIN, OK, QUEUED,
// array, v], the answered SWAPDB past the boundary is kept in the tail
// as a dropped write, but its EXEC element rejected it, so no
// swap-back clone is owed ahead of the retry. Without the check the
// clone's standalone SWAPDB is rejected on the healthy retry and fails
// the batch. Regression test for the Codex P2 finding on #4351 (round 41).
func TestPipelineRetryQueuedSwapdbExecErrorNeedsNoSuffixInverse(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.msetTryagainOnce.Store(true)
	client := newPartialRetryClient(t, server.ln.Addr().String())

	ctx := context.Background()
	pipe := client.Pipeline()
	mset := pipe.MSet(ctx, "k", "v")
	multi := pipe.Do(ctx, "multi")
	swapdb := pipe.Do(ctx, "swapdb", 99, 0)
	exec := pipe.Do(ctx, "exec")
	get := pipe.Get(ctx, "k")
	if _, err := pipe.Exec(ctx); err != nil {
		t.Fatalf("Exec: %v, want nil (the swap never ran, so the retry owes it no inverse)", err)
	}
	if err := mset.Err(); err != nil {
		t.Fatalf("MSET: %v", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := swapdb.Err(); err != nil {
		t.Fatalf("SWAPDB: %v", err)
	}
	if err := exec.Err(); err != nil {
		t.Fatalf("EXEC: %v", err)
	}
	if got, err := get.Result(); err != nil || got != "v" {
		t.Fatalf("GET = %q, %v; want %q, nil", got, err, "v")
	}
	if got := server.swapdbs.Load(); got != 2 {
		t.Fatalf("server received %d SWAPDB commands, want 2 (the first attempt plus the tail re-run, with no synthesised inverse)", got)
	}
	if got := server.conns.Load(); got != 1 {
		t.Fatalf("server saw %d connections, want 1 (nothing in this batch forces a fresh connection)", got)
	}
}

// TestPipelineRetryOpenMultiRetireSkippedOnDedicatedConnWithoutRetry: on a
// dedicated Conn with retries disabled, a MULTI; INCR; EXEC batch whose
// EXEC the server refuses with NOREPLICAS must not retire the
// connection: no retry can run, and StickyConnPool records the mark as
// a persistent BadConnError, so the dedicated Conn stays unusable
// although the transaction is still open and a later DISCARD could
// clear it. Regression test for the Codex P2 finding on #4351 (review
// comment 4694618888).
func TestPipelineRetryOpenMultiRetireSkippedOnDedicatedConnWithoutRetry(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.failExecNoreplicasOnce.Store(true)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1, // truly disabled: 0 would be normalised to 3
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })
	conn := client.Conn()
	t.Cleanup(func() { _ = conn.Close() })

	ctx := context.Background()
	pipe := conn.Pipeline()
	multi := pipe.Do(ctx, "multi")
	incr := pipe.Incr(ctx, "counter")
	exec := pipe.Do(ctx, "exec")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOREPLICAS") {
		t.Fatalf("Exec: %v, want NOREPLICAS (no retry runs with MaxRetries -1)", err)
	}
	if err := multi.Err(); err != nil {
		t.Fatalf("MULTI: %v", err)
	}
	if err := incr.Err(); err != nil {
		t.Fatalf("INCR: %v, want nil (+QUEUED)", err)
	}
	if err := exec.Err(); err == nil || !strings.Contains(err.Error(), "NOREPLICAS") {
		t.Fatalf("EXEC: %v, want NOREPLICAS", err)
	}
	if got := server.execWithMulti.Load(); got != 0 {
		t.Fatalf("EXEC ran %d time(s) inside MULTI, want 0 (the refused EXEC committed nothing)", got)
	}
	if got := server.incrs.Load(); got != 1 {
		t.Fatalf("server received %d INCRs, want 1 (the queued INCR was sent once and never re-run)", got)
	}
	// The dedicated Conn must stay usable: DISCARD clears the
	// still-open frame, and a follow-up write runs on it.
	if err := conn.Do(ctx, "discard").Err(); err != nil {
		t.Fatalf("DISCARD: %v, want nil (the dedicated Conn must stay usable after the refused EXEC)", err)
	}
	after := conn.Set(ctx, "after", "v", 0)
	if err := after.Err(); err != nil {
		t.Fatalf("follow-up SET: %v, want nil (the dedicated Conn must stay usable after the refused EXEC)", err)
	}
	if got := server.conns.Load(); got != 1 {
		t.Fatalf("server saw %d connections, want 1 (the dedicated connection must not be retired)", got)
	}
}

// TestPipelineRetryOpenMultiRetireKeepsPooledConnOutOfPoolWithoutRetry: the
// same refused EXEC with retries disabled on a pooled client must still
// retire the connection: the frame stays open, and returning the
// connection to the pool would hand the next borrower a connection
// inside MULTI, where its commands answer +QUEUED instead of running.
// Regression test for the Codex P2 finding on #4351 (review comment
// 4694618888): the retire is skipped only on a dedicated connection,
// never on a replaceable pooled one.
func TestPipelineRetryOpenMultiRetireKeepsPooledConnOutOfPoolWithoutRetry(t *testing.T) {
	server := newConnStateRetryServer(t)
	server.dropAfter = 1 << 30
	server.failExecNoreplicasOnce.Store(true)
	client := NewClient(&Options{
		Addr:            server.ln.Addr().String(),
		Protocol:        2,
		PoolSize:        1,
		MaxRetries:      -1, // truly disabled: 0 would be normalised to 3
		MinRetryBackoff: time.Millisecond,
		MaxRetryBackoff: 2 * time.Millisecond,
		DialTimeout:     5 * time.Second,
		ReadTimeout:     5 * time.Second,
		WriteTimeout:    5 * time.Second,
		DisableIdentity: true,
		MaintNotificationsConfig: &maintnotifications.Config{
			Mode: maintnotifications.ModeDisabled,
		},
	})
	t.Cleanup(func() { _ = client.Close() })

	ctx := context.Background()
	pipe := client.Pipeline()
	pipe.Do(ctx, "multi")
	pipe.Incr(ctx, "counter")
	pipe.Do(ctx, "exec")
	_, err := pipe.Exec(ctx)
	if err == nil || !strings.Contains(err.Error(), "NOREPLICAS") {
		t.Fatalf("Exec: %v, want NOREPLICAS (no retry runs with MaxRetries -1)", err)
	}
	// The follow-up SET must run on a fresh connection: on the retired
	// one it would answer +QUEUED inside the still-open frame.
	after := client.Set(ctx, "after", "v", 0)
	if got, err := after.Result(); err != nil || got != "OK" {
		t.Fatalf("follow-up SET = %q, %v; want \"OK\", nil (the pooled connection with the open frame must be retired)", got, err)
	}
	if got := server.conns.Load(); got != 2 {
		t.Fatalf("server saw %d connections, want 2 (the connection with the still-open MULTI must be retired, not reused)", got)
	}
}
