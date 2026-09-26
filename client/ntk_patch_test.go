package client

import (
	"bufio"
	"bytes"
	"errors"
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
	for _, resp := range []string{"* EXPUNGE\r\n", "* FETCH\r\n"} {
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
	for _, resp := range []string{"* EXPUNGE\r\n", "* FETCH\r\n"} {
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

// scriptedServer greets over s with caps, then answers each command line with
// the text reply returns for it, after replacing "TAG" with the command's tag.
// It returns when reply returns "" or the client closes.
func scriptedServer(s net.Conn, caps string, reply func(line string) string) {
	defer s.Close()
	r := bufio.NewReader(s)
	if _, err := io.WriteString(s, "* PREAUTH [CAPABILITY "+caps+"] ready\r\n"); err != nil {
		return
	}
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		out := reply(line)
		if out == "" {
			return
		}
		if _, err := io.WriteString(s, strings.ReplaceAll(out, "TAG", strings.Fields(line)[0])); err != nil {
			return
		}
	}
}

// TestATaggedRefusalIsAStatusErrorAndAHandlerFailureIsNot is patch (v). A
// tagged NO or BAD is returned as an *imap.ErrStatusResp whose text is the
// status info, as upstream's errors.New(info) was; a response handler that
// could not parse what the server sent returns an error that is NOT one. Upstream
// returned both as untyped errors, so a caller had no way to tell a server
// refusing the command from a response stream it could no longer trust.
func TestATaggedRefusalIsAStatusErrorAndAHandlerFailureIsNot(t *testing.T) {
	for _, tc := range []struct {
		name   string
		reply  string
		status bool
	}{
		{"NO", "TAG NO no such thing\r\n", true},
		{"BAD", "TAG BAD not understood\r\n", true},
		{"a FETCH the handler cannot parse", "* 1 FETCH (UID 1 BODYSTRUCTURE x)\r\nTAG OK done\r\n", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c, s := net.Pipe()
			go scriptedServer(s, "IMAP4rev1", func(string) string { return tc.reply })
			cl, err := NewWithOptions(c, Options{ErrorLog: &captureLog{}})
			if err != nil {
				t.Fatalf("NewWithOptions: %v", err)
			}
			defer cl.Terminate()
			setClientState(cl, imap.SelectedState, imap.NewMailboxStatus("INBOX", nil))
			ch := make(chan *imap.Message, 4)
			err = cl.UidFetch(new(imap.SeqSet), []imap.FetchItem{imap.FetchUid}, ch)
			if err == nil {
				t.Fatalf("UID FETCH answered %q returned no error", tc.reply)
			}
			var status *imap.ErrStatusResp
			if got := errors.As(err, &status); got != tc.status {
				t.Fatalf("UID FETCH answered %q returned %T %q; errors.As(*imap.ErrStatusResp) = %v, want %v", tc.reply, err, err, got, tc.status)
			}
			if tc.status && err.Error() != strings.TrimSpace(strings.SplitN(tc.reply, " ", 3)[2]) {
				t.Errorf("the refusal's text is %q, want the status info", err.Error())
			}
		})
	}
}

// fetchOnce runs one UID FETCH of UID 1 on a real client whose server answers
// with the FETCH message data attr, and returns what the command returned.
func fetchOnce(t *testing.T, attr string) ([]*imap.Message, error) {
	t.Helper()
	c, s := net.Pipe()
	go scriptedServer(s, "IMAP4rev1", func(string) string { return "* 1 FETCH " + attr + "\r\nTAG OK done\r\n" })
	cl, err := NewWithOptions(c, Options{ErrorLog: &captureLog{}})
	if err != nil {
		t.Fatalf("NewWithOptions: %v", err)
	}
	defer cl.Terminate()
	setClientState(cl, imap.SelectedState, imap.NewMailboxStatus("INBOX", nil))
	set := new(imap.SeqSet)
	set.AddNum(1)
	ch := make(chan *imap.Message, 4)
	err = cl.UidFetch(set, []imap.FetchItem{imap.FetchUid}, ch)
	var msgs []*imap.Message
	for m := range ch {
		msgs = append(msgs, m)
	}
	return msgs, err
}

// TestAFetchValueWithoutItsItemsShapeIsRefusedNotZeroed is patch (vi), the
// solicited half. Upstream's Message.Parse discarded the error of every
// conversion it made -- `m.Size, _ = ParseNumber(f)`, `m.Body[section], _ =
// f.(Literal)` -- so a value without its item's shape arrived as that item's
// zero value, and a body sent as a quoted string, which RFC 3501's nstring
// permits, arrived as no body. Each shape below is from RFC 3501 section 9's
// grammar for the item: the ones the grammar does not permit are refused by
// the command's handler, and the permitted ones arrive as what was sent.
func TestAFetchValueWithoutItsItemsShapeIsRefusedNotZeroed(t *testing.T) {
	for _, attr := range []string{
		"(UID x1)", "(UID 0)", "(UID 5000000000)", "(UID NIL)", "(UID (1))", "(UID {1}\r\n1)",
		"(UID 1 RFC822.SIZE x)", "(UID 1 RFC822.SIZE 5000000000)", "(UID 1 RFC822.SIZE NIL)", "(UID 1 RFC822.SIZE (1))", "(UID 1 RFC822.SIZE {1}\r\n1)",
		"(UID 1 INTERNALDATE x)", "(UID 1 INTERNALDATE \"2020-01-01T00:00:00Z\")", "(UID 1 INTERNALDATE NIL)", "(UID 1 INTERNALDATE (1))", "(UID 1 INTERNALDATE {26}\r\n01-Jan-2020 00:00:00 +0000)",
		"(UID 1 FLAGS (NIL))", "(UID 1 FLAGS ((\\Seen)))",
		"(UID 1 BODY[] (a b))",
		"(UID 1 FLAGS)", "(UID 1 UID 2)",
		"UID 1",
	} {
		t.Run(attr, func(t *testing.T) {
			msgs, err := fetchOnce(t, attr)
			var status *imap.ErrStatusResp
			if err == nil || errors.As(err, &status) {
				t.Fatalf("FETCH %q returned %d messages and error %v; want the handler's refusal", attr, len(msgs), err)
			}
		})
	}
	meta := func(m *imap.Message) string {
		return fmt.Sprintf("%d %d %s %v", m.Uid, m.Size, m.InternalDate.UTC().Format(time.RFC3339), m.Flags)
	}
	for _, tc := range []struct {
		attr   string
		render func(*imap.Message) string
		want   string
	}{
		{"(UID 1 RFC822.SIZE 100 INTERNALDATE \"01-Jan-2020 00:00:00 +0000\" FLAGS (\\Seen))", meta, "1 100 2020-01-01T00:00:00Z [\\Seen]"},
		{"(UID 1 FLAGS ())", func(m *imap.Message) string { return fmt.Sprintf("nil=%v len=%d", m.Flags == nil, len(m.Flags)) }, "nil=false len=0"},
		{"(UID 1 BODY[] {3}\r\nabc)", bodyText, `BODY[]="abc"`},
		{"(UID 1 BODY[] \"abc\")", bodyText, `BODY[]="abc"`},
		{"(UID 1 BODY[] \"\")", bodyText, `BODY[]=""`},
		{"(UID 1 BODY[] NIL)", bodyText, "BODY[]=NIL"},
		{"(UID 1)", bodyText, ""},
		{"(UID 1 BODY[]<0> {3}\r\nabc)", bodyText, `BODY[]<0>="abc"`},
		{"(UID 1 X-GM-MSGID 11)", func(m *imap.Message) string { return fmt.Sprintf("%v", m.Items["X-GM-MSGID"]) }, "11"},
	} {
		t.Run(tc.attr, func(t *testing.T) {
			msgs, err := fetchOnce(t, tc.attr)
			if err != nil || len(msgs) != 1 {
				t.Fatalf("FETCH %q returned %d messages and error %v", tc.attr, len(msgs), err)
			}
			if got := tc.render(msgs[0]); got != tc.want {
				t.Errorf("FETCH %q arrived as %q, want %q", tc.attr, got, tc.want)
			}
		})
	}
}

// bodyText renders every body section m carries, NIL as NIL.
func bodyText(m *imap.Message) string {
	var parts []string
	for section, lit := range m.Body {
		name := string(section.FetchItem())
		if lit == nil {
			parts = append(parts, name+"=NIL")
			continue
		}
		b, _ := io.ReadAll(lit)
		parts = append(parts, fmt.Sprintf("%s=%q", name, b))
	}
	return strings.Join(parts, " ")
}

// TestAnUnsolicitedFetchOrExpungeThatDoesNotParseEndsTheConnection is patch
// (vi), the unsolicited half. Upstream's handler for a unilateral FETCH
// dropped one Message.Parse refused -- `if err := msg.Parse(fields); err != nil
// { break }` -- and delivered an EXPUNGE whose number did not parse as
// sequence number 0. Each is now returned to the reader, which ends the
// connection: an update the client cannot read is an update it has lost, and
// nothing after it can be placed. None of them may end it by a panic.
func TestAnUnsolicitedFetchOrExpungeThatDoesNotParseEndsTheConnection(t *testing.T) {
	for _, resp := range []string{
		"* 1 FETCH (FLAGS NIL)", "* 1 FETCH (UID 0)", "* 1 FETCH (RFC822.SIZE x)", "* 1 FETCH FLAGS", "* 1 FETCH",
		"* 0 FETCH (FLAGS ())", "* EXPUNGE x", "* 0 EXPUNGE",
	} {
		t.Run(resp, func(t *testing.T) {
			c, s := net.Pipe()
			defer s.Close()
			go func() {
				_, _ = io.WriteString(s, "* OK [CAPABILITY IMAP4rev1] ready\r\n"+resp+"\r\n")
				_, _ = io.Copy(io.Discard, s)
			}()
			logs := &captureLog{}
			cl, err := NewWithOptions(c, Options{ErrorLog: logs, Updates: make(chan Update, 4)})
			if err != nil {
				t.Fatalf("NewWithOptions: %v", err)
			}
			select {
			case <-cl.LoggedOut():
			case <-time.After(2 * time.Second):
				t.Fatalf("the connection survived %q", resp)
			}
			if text := logs.text(); !strings.Contains(text, "cannot handle server response") || strings.Contains(text, "runtime error") {
				t.Errorf("%q did not end the connection as a response the client could not handle, without a panic; it logged %q", resp, text)
			}
		})
	}
	t.Run("positive-control-a-well-formed-update-is-delivered", func(t *testing.T) {
		c, s := net.Pipe()
		defer s.Close()
		go func() {
			_, _ = io.WriteString(s, "* OK [CAPABILITY IMAP4rev1] ready\r\n* 1 FETCH (FLAGS (\\Seen))\r\n* 2 EXPUNGE\r\n")
			_, _ = io.Copy(io.Discard, s)
		}()
		updates := make(chan Update, 4)
		cl, err := NewWithOptions(c, Options{ErrorLog: &captureLog{}, Updates: updates})
		if err != nil {
			t.Fatalf("NewWithOptions: %v", err)
		}
		defer cl.Terminate()
		for _, want := range []string{"*client.MessageUpdate", "*client.ExpungeUpdate"} {
			select {
			case u := <-updates:
				if got := fmt.Sprintf("%T", u); got != want {
					t.Errorf("got update %s, want %s", got, want)
				}
			case <-time.After(2 * time.Second):
				t.Fatalf("no %s arrived", want)
			}
		}
	})
}

// lateWriter is the client's end of the connection, holding each write's
// return until the reader has ended: the command's bytes are on the wire at
// once, and execute reaches its select only after the server has completed the
// command, hung up, and the reader has seen both. That is the ordering the
// defect needs, made certain rather than left to the scheduler, which lost it
// in 1 of 300 rounds over loopback TCP.
type lateWriter struct {
	net.Conn
	mu     sync.Mutex
	client *Client
}

func (w *lateWriter) Write(p []byte) (int, error) {
	n, err := w.Conn.Write(p)
	w.mu.Lock()
	cl := w.client
	w.mu.Unlock()
	if cl != nil {
		select {
		case <-cl.LoggedOut():
		case <-time.After(2 * time.Second):
		}
	}
	return n, err
}

// TestACommandThatCompletedIsReturnedCompletedWhenTheConnectionEndsAfter is
// patch (vii). The server completes the command and hangs up at once, so by the
// time execute looks, the command's result is waiting AND the reader has ended.
// Upstream selected between the two at random and returned errClosed for a
// command the server had completed -- measured through the connector's
// FetchMetadata at 10 retrievals discarded in 60. A completed command is
// returned as completed, every time.
//
// The positive control is the same server hanging up WITHOUT completing the
// command: that is errClosed, so the test does not pass over an execute that
// never reports a closed connection.
func TestACommandThatCompletedIsReturnedCompletedWhenTheConnectionEndsAfter(t *testing.T) {
	run := func(complete bool) error {
		c, s := net.Pipe()
		go func() {
			defer s.Close()
			_, _ = io.WriteString(s, "* OK [CAPABILITY IMAP4rev1] ready\r\n")
			line, err := bufio.NewReader(s).ReadString('\n')
			if err != nil {
				return
			}
			if complete {
				tag, _, _ := strings.Cut(line, " ")
				_, _ = io.WriteString(s, tag+" OK NOOP completed\r\n")
			}
		}()
		w := &lateWriter{Conn: c}
		cl, err := NewWithOptions(w, Options{ErrorLog: &captureLog{}})
		if err != nil {
			t.Fatalf("NewWithOptions: %v", err)
		}
		defer cl.Terminate()
		w.mu.Lock()
		w.client = cl
		w.mu.Unlock()
		return cl.Noop()
	}
	const rounds = 200
	failed := 0
	for i := 0; i < rounds; i++ {
		if err := run(true); err != nil {
			failed++
		}
	}
	if failed > 0 {
		t.Errorf("%d of %d commands the server completed before hanging up were returned as %v", failed, rounds, errClosed)
	}
	if err := run(false); err != errClosed {
		t.Errorf("a command the server did not complete before hanging up returned %v, want %v", err, errClosed)
	}
}
