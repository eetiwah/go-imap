package client

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
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
