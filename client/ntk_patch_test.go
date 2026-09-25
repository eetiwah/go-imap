package client

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/eetiwah/go-imap"
)

// declaredLiteral is the literal length the hostile server declares. It sends
// three bytes of it.
const declaredLiteral = 64 << 20

// captureLog is an imap.Logger that records what the client logs.
type captureLog struct {
	mu    sync.Mutex
	lines []string
}

func (l *captureLog) Printf(format string, v ...interface{}) { l.add(fmt.Sprintf(format, v...)) }
func (l *captureLog) Println(v ...interface{})               { l.add(fmt.Sprintln(v...)) }
func (l *captureLog) add(s string) {
	l.mu.Lock()
	l.lines = append(l.lines, s)
	l.mu.Unlock()
}
func (l *captureLog) text() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Join(l.lines, "")
}

// literalServer greets without capabilities, so the client asks CAPABILITY
// INSIDE its constructor, and answers with a literal declared at
// declaredLiteral bytes of which it sends three, then closes.
func literalServer(t *testing.T, s net.Conn) {
	t.Helper()
	defer s.Close()
	if _, err := io.WriteString(s, "* OK ready\r\n"); err != nil {
		return
	}
	if _, err := bufio.NewReader(s).ReadString('\n'); err != nil {
		return
	}
	_, _ = io.WriteString(s, fmt.Sprintf("* CAPABILITY {%d}\r\nabc", declaredLiteral))
}

// allocatedBy reports how many bytes the heap allocated while f ran.
func allocatedBy(f func()) uint64 {
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	f()
	runtime.ReadMemStats(&after)
	return after.TotalAlloc - before.TotalAlloc
}

// TestNewWithOptionsBoundsALiteralReadInsideTheConstructor is patch (i). The
// declared literal arrives in the CAPABILITY response New reads before it
// returns, which is before any caller could reach the reader. With
// MaxLiteralSize applied up front the declaration is refused before the buffer
// is allocated, and the refusal goes to the ErrorLog the options named. The
// upstream constructor is run the same way as the positive control: it
// allocates the declared length, so the test can see the allocation it
// requires to be absent.
func TestNewWithOptionsBoundsALiteralReadInsideTheConstructor(t *testing.T) {
	t.Run("bounded", func(t *testing.T) {
		c, s := net.Pipe()
		go literalServer(t, s)
		logs := &captureLog{}
		grew := allocatedBy(func() {
			_, _ = NewWithOptions(c, Options{MaxLiteralSize: 1024, ErrorLog: logs})
		})
		if grew >= declaredLiteral {
			t.Fatalf("a %d-byte declaration under a 1024-byte bound allocated %d bytes", declaredLiteral, grew)
		}
		if !strings.Contains(logs.text(), "literal exceeding maximum size") {
			t.Errorf("the refusal did not reach the ErrorLog the options named; it logged %q", logs.text())
		}
	})
	t.Run("upstream-constructor-allocates", func(t *testing.T) {
		c, s := net.Pipe()
		go literalServer(t, s)
		grew := allocatedBy(func() { _, _ = NewWithOptions(c, Options{ErrorLog: &captureLog{}}) })
		if grew < declaredLiteral {
			t.Fatalf("with no bound the declaration allocated %d bytes, fewer than the %d declared, so the bounded case cannot be attributed to the bound", grew, declaredLiteral)
		}
	})
}

// TestNewWithOptionsDeliversAnUpdateSentWithTheGreeting is the Updates half of
// patch (i): an unsolicited status response sent immediately after the
// greeting reaches the channel the options named, because the channel was in
// place before the reader goroutine started.
func TestNewWithOptionsDeliversAnUpdateSentWithTheGreeting(t *testing.T) {
	c, s := net.Pipe()
	defer s.Close()
	go func() {
		_, _ = io.WriteString(s, "* OK [CAPABILITY IMAP4rev1] ready\r\n* OK [ALERT] early notice\r\n")
		_, _ = io.Copy(io.Discard, s)
	}()
	updates := make(chan Update, 1)
	cl, err := NewWithOptions(c, Options{Updates: updates, ErrorLog: &captureLog{}})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	defer cl.Terminate()
	select {
	case u := <-updates:
		su, ok := u.(*StatusUpdate)
		if !ok || su.Status.Info != "early notice" {
			t.Fatalf("got update %#v, want the ALERT status update", u)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the update sent with the greeting never reached the Updates channel")
	}
}

// TestAMalformedUnilateralResponseEndsTheConnectionNotTheProcess is patch
// (ii). Each response below made the reader goroutine index a field that is
// not there, which panicked the process. Now the connection ends: LoggedOut
// closes and a command returns an error.
func TestAMalformedUnilateralResponseEndsTheConnectionNotTheProcess(t *testing.T) {
	for _, resp := range []string{"* EXPUNGE\r\n", "* FETCH\r\n", "* 1 FETCH\r\n"} {
		t.Run(strings.TrimSpace(resp), func(t *testing.T) {
			c, s := net.Pipe()
			defer s.Close()
			go func() {
				_, _ = io.WriteString(s, "* OK [CAPABILITY IMAP4rev1] ready\r\n"+resp)
				_, _ = io.Copy(io.Discard, s)
			}()
			logs := &captureLog{}
			cl, err := NewWithOptions(c, Options{ErrorLog: logs})
			if err != nil {
				t.Fatalf("NewWithOptions: %v", err)
			}
			select {
			case <-cl.LoggedOut():
			case <-time.After(2 * time.Second):
				t.Fatalf("the connection survived %q", resp)
			}
			if err := cl.Noop(); err == nil {
				t.Errorf("a command on the ended connection succeeded")
			}
			if !strings.Contains(logs.text(), "cannot handle server response") {
				t.Errorf("the reader did not report the response; it logged %q", logs.text())
			}
		})
	}
}

// deadlineIgnoringConn accepts a deadline on a closed connection. A command
// that begins while the connection is still open has already set its deadline
// by the time the reader fails; this conn puts a command started after the
// failure at that same point, which is registerHandler.
type deadlineIgnoringConn struct{ net.Conn }

func (deadlineIgnoringConn) SetDeadline(time.Time) error { return nil }

// TestTheHandlerLockIsReleasedWhenAHandlerPanics is patch (iii). The response
// handlers run under handlersLocker, and each malformed response below panics
// inside one of them. The reader's recover ends the connection, and the lock
// must be free afterwards: every command registers a handler under it, so a
// lock left held makes the next command block for good instead of failing.
func TestTheHandlerLockIsReleasedWhenAHandlerPanics(t *testing.T) {
	for _, resp := range []string{"* EXPUNGE\r\n", "* FETCH\r\n", "* 1 FETCH\r\n"} {
		t.Run(strings.TrimSpace(resp), func(t *testing.T) {
			c, s := net.Pipe()
			defer s.Close()
			go func() {
				_, _ = io.WriteString(s, "* OK [CAPABILITY IMAP4rev1] ready\r\n"+resp)
				_, _ = io.Copy(io.Discard, s)
			}()
			logs := &captureLog{}
			cl, err := NewWithOptions(deadlineIgnoringConn{c}, Options{ErrorLog: logs})
			if err != nil {
				t.Fatalf("NewWithOptions: %v", err)
			}
			select {
			case <-cl.LoggedOut():
			case <-time.After(2 * time.Second):
				t.Fatalf("the connection survived %q", resp)
			}
			if !strings.Contains(logs.text(), "cannot handle server response") {
				t.Fatalf("the reader did not recover from %q, so this case tests nothing; it logged %q", resp, logs.text())
			}
			if cl.handlersLocker.TryLock() {
				cl.handlersLocker.Unlock()
			} else {
				t.Errorf("handlersLocker is still held after the reader recovered from %q", resp)
			}

			done := make(chan error, 1)
			go func() {
				_, err := cl.Capability()
				done <- err
			}()
			select {
			case err := <-done:
				if err == nil {
					t.Errorf("CAPABILITY on the ended connection succeeded")
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("CAPABILITY after the recover did not return within 2s")
			}
		})
	}
}

// continuationFlood is how many "+" lines the hostile server sends in one
// command's response. Upstream spent a goroutine on each, blocked for good.
const continuationFlood = 2000

// TestAServerContinuationCostsNoGoroutine is patch (iv). The server answers
// CAPABILITY with continuationFlood "+" lines that no literal asked for.
// Upstream's handler started a goroutine per line to deliver the signal, and
// each one blocked on a channel nobody was reading; the count is taken with the
// command complete and the connection still open, where those goroutines
// would still be parked.
//
// The flood leaves a signal held, and the second half is that it does not
// belong to the next command: an APPEND the server refuses without sending "+"
// must fail without its literal being written. A stale signal would have
// released the literal to a server that never asked for it.
func TestAServerContinuationCostsNoGoroutine(t *testing.T) {
	c, s := net.Pipe()
	defer s.Close()
	appendLine := make(chan string, 1)
	after := make(chan string, 1)
	go func() {
		r := bufio.NewReader(s)
		_, _ = io.WriteString(s, "* OK [CAPABILITY IMAP4rev1] ready\r\n")
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		tag := strings.Fields(line)[0]
		_, _ = io.WriteString(s, strings.Repeat("+ \r\n", continuationFlood)+"* CAPABILITY IMAP4rev1\r\n"+tag+" OK done\r\n")
		line, err = r.ReadString('\n')
		if err != nil {
			return
		}
		appendLine <- line
		_, _ = io.WriteString(s, strings.Fields(line)[0]+" NO refused\r\n")
		line, _ = r.ReadString('\n')
		after <- line
	}()
	cl, err := NewWithOptions(c, Options{ErrorLog: &captureLog{}})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	defer cl.Terminate()

	runtime.GC()
	before := runtime.NumGoroutine()
	if _, err := cl.Capability(); err != nil {
		t.Fatalf("CAPABILITY: %v", err)
	}
	runtime.GC()
	grew := runtime.NumGoroutine() - before
	t.Logf("%d continuation lines: goroutines %d -> %d", continuationFlood, before, before+grew)
	if grew >= continuationFlood/10 {
		t.Errorf("%d continuation lines left %d more goroutines running", continuationFlood, grew)
	}

	setClientState(cl, imap.AuthenticatedState, nil)
	if err := cl.Append("INBOX", nil, time.Time{}, bytes.NewBufferString("literal-bytes")); err == nil {
		t.Errorf("APPEND succeeded against a server that refused it")
	}
	if line := <-appendLine; !strings.Contains(line, "APPEND") {
		t.Fatalf("the server read %q where it expected the APPEND", line)
	}
	_ = cl.Terminate()
	if line := <-after; strings.Contains(line, "literal-bytes") {
		t.Errorf("the literal was written to a server that sent no continuation for it: %q", line)
	}
}
