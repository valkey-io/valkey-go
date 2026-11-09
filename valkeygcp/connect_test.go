package valkeygcp

import (
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

const (
	iamExhausted    = "-ERR (ERR_IAM_EXHAUSTED) Memorystore IAM authentication backend quota exceeded.\r\n"
	clusterDisabled = "-ERR This instance has cluster support disabled\r\n"
)

// scriptedValkey is a RESP server on loopback. reply answers one command on connection conn
// (numbered from 1); drop closes the connection after that reply.
type scriptedValkey struct {
	ln    net.Listener
	conns atomic.Int64
	reply func(conn int64, args []string) (resp string, drop bool)
}

func newScriptedValkey(t *testing.T, reply func(conn int64, args []string) (string, bool)) *scriptedValkey {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &scriptedValkey{ln: ln, reply: reply}
	var wg sync.WaitGroup
	t.Cleanup(func() { _ = ln.Close(); wg.Wait() })
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			id := f.conns.Add(1)
			wg.Add(1)
			go func() {
				defer wg.Done()
				f.serve(c, id)
			}()
		}
	}()
	return f
}

func (f *scriptedValkey) addr() string { return f.ln.Addr().String() }

func (f *scriptedValkey) serve(c net.Conn, id int64) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	for {
		args, err := readCommand(r)
		if err != nil || len(args) == 0 {
			return
		}
		resp, drop := f.reply(id, args)
		if _, err := io.WriteString(c, resp); err != nil || drop {
			return
		}
	}
}

// standaloneReply answers like a cluster-disabled Valkey node.
func standaloneReply(_ int64, args []string) (string, bool) {
	switch strings.ToUpper(args[0]) {
	case "HELLO":
		return helloReply, false
	case "CLUSTER":
		return clusterDisabled, false
	case "PING":
		return "+PONG\r\n", false
	case "GET":
		return "$2\r\nok\r\n", false
	}
	return "+OK\r\n", false
}

// When Memorystore rejects the handshake with an IAM backend error (ERR_IAM_EXHAUSTED or
// ERR_IAM_OTHER), NewClient retries with exponential backoff, since valkey-go doesn't
// retry that reply. Here the third connection succeeds.
func TestNewClient_RetriesIAMBackendErrors(t *testing.T) {
	srv := newScriptedValkey(t, func(conn int64, args []string) (string, bool) {
		if strings.EqualFold(args[0], "HELLO") && conn <= 2 {
			return iamExhausted, false
		}
		return standaloneReply(conn, args)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	client, err := NewClient(ctx, initOption(srv.addr()), WithIAMOptions(staticIAM()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer client.Close()
	if n := srv.conns.Load(); n < 3 {
		t.Fatalf("connections = %d, want at least 3 (two throttled handshakes, then success)", n)
	}
	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		t.Fatal(err)
	}
}

// The retries of IAM backend errors stop when NewClient's ctx ends; the error keeps the IAM
// reply and wraps the ctx error.
func TestNewClient_IAMRetriesStopAtDeadline(t *testing.T) {
	srv := newScriptedValkey(t, func(conn int64, args []string) (string, bool) {
		if strings.EqualFold(args[0], "HELLO") {
			return iamExhausted, false
		}
		return standaloneReply(conn, args)
	})
	ctx, cancel := context.WithTimeout(context.Background(), 700*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := NewClient(ctx, initOption(srv.addr()), WithIAMOptions(staticIAM()))
	if err == nil || !strings.Contains(err.Error(), "ERR_IAM_EXHAUSTED") || !strings.Contains(err.Error(), "gave up after") ||
		!errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the IAM error after giving up at the deadline", err)
	}
	if elapsed := time.Since(start); elapsed > 3*time.Second {
		t.Fatalf("took %v, want about the 700ms deadline", elapsed)
	}
	if n := srv.conns.Load(); n < 2 {
		t.Fatalf("connections = %d, want at least 2 (retried)", n)
	}
}

// Handshake errors other than the IAM backend errors, such as WRONGPASS, are returned at
// once, without a retry.
func TestNewClient_OtherHandshakeErrorsNotRetried(t *testing.T) {
	srv := newScriptedValkey(t, func(conn int64, args []string) (string, bool) {
		if strings.EqualFold(args[0], "HELLO") {
			return "-WRONGPASS invalid username-password pair or user is disabled\r\n", false
		}
		return standaloneReply(conn, args)
	})
	_, err := NewClient(context.Background(), initOption(srv.addr()), WithIAMOptions(staticIAM()))
	if err == nil || !strings.Contains(err.Error(), "WRONGPASS") {
		t.Fatalf("got %v, want WRONGPASS", err)
	}
	if n := srv.conns.Load(); n != 1 {
		t.Fatalf("connections = %d, want 1", n)
	}
}

// valkey-go sends CLIENT TRACKING on every new connection unless ClientOption.DisableCache
// is set. When an ACL rule refuses it, the error says to allow it, keeps Memorystore's
// NOPERM reply, which names the rule's user, and still wraps valkey.ErrNoCache. Another
// refused CLIENT command gets no such hint.
func TestNewClient_ClientTrackingHint(t *testing.T) {
	srv := newScriptedValkey(t, func(conn int64, args []string) (string, bool) {
		if len(args) > 1 && strings.EqualFold(args[0], "CLIENT") && (strings.EqualFold(args[1], "TRACKING") || strings.EqualFold(args[1], "NO-TOUCH")) {
			return "-NOPERM User app@p.iam.gserviceaccount.com has no permissions to run the 'client|" + strings.ToLower(args[1]) + "' command\r\n", false
		}
		return standaloneReply(conn, args)
	})
	const hint = "valkeygcp: the ACL rule must allow CLIENT TRACKING (+client|tracking), or set ClientOption.DisableCache: NOPERM User app@p.iam.gserviceaccount.com"
	_, err := NewClient(context.Background(), initOption(srv.addr()), WithIAMOptions(staticIAM()))
	if err == nil || !strings.Contains(err.Error(), hint) || !errors.Is(err, valkey.ErrNoCache) {
		t.Fatalf("got %v, want the hint, then Memorystore's reply, wrapping ErrNoCache", err)
	}
	noTouch := initOption(srv.addr())
	noTouch.DisableCache, noTouch.ClientNoTouch = true, true
	_, err = NewClient(context.Background(), noTouch, WithIAMOptions(staticIAM()))
	if err == nil || strings.Contains(err.Error(), "CLIENT TRACKING") {
		t.Fatalf("got %v, want the CLIENT NO-TOUCH error without the CLIENT TRACKING hint", err)
	}
}

func TestDialGate(t *testing.T) {
	g := &dialGate{backoff: func(n int) time.Duration { return time.Duration(n+1) * time.Second }}
	g.handshake(handshakeIAMError)
	if d := g.delay(); d != 0 {
		t.Fatalf("delay before the first successful handshake = %v, want 0", d)
	}
	g.handshake(handshakeOK)
	g.handshake(handshakeIAMError)
	g.handshake(handshakeIAMError)
	if d := g.delay(); d != 2*time.Second {
		t.Fatalf("delay after 2 IAM errors = %v, want backoff(1) = 2s", d)
	}
	g.handshake(handshakeOther) // WRONGPASS and the like don't change the count
	if d := g.delay(); d != 2*time.Second {
		t.Fatalf("delay after another error = %v, want unchanged", d)
	}
	g.handshake(handshakeOK)
	if d := g.delay(); d != 0 {
		t.Fatalf("delay after a successful handshake = %v, want 0", d)
	}
	// The real schedule stays within the IAM cap.
	g = &dialGate{}
	g.handshake(handshakeOK)
	for i := 0; i < 30; i++ {
		g.handshake(handshakeIAMError)
	}
	if d := g.delay(); d <= 0 || d > iamBackoffCap {
		t.Fatalf("delay = %v, want (0, %v]", d, iamBackoffCap)
	}
}

func TestGatedConnPeek(t *testing.T) {
	cases := []struct {
		chunks []string
		want   int // failures after the reply; -1: success (armed, 0 failures)
	}{
		{[]string{helloReply}, -1},
		{[]string{"-ERR (ERR_IAM_", "EXHAUSTED) quota exceeded.\r\n"}, 1},
		{[]string{"-ERR (ERR_IAM_OTHER) backend error.\r\n+ignored\r\n"}, 1},
		{[]string{"-WRONGPASS invalid\r\n"}, 0},
		// A first line longer than maxReplyPeek is judged by its first maxReplyPeek bytes.
		{[]string{"-ERR (ERR_IAM_OTHER) " + strings.Repeat("x", 40), strings.Repeat("x", 60), strings.Repeat("x", 60), strings.Repeat("x", 60), strings.Repeat("x", 60)}, 1},
		// An empty first line tells nothing about the handshake.
		{[]string{"\n+OK\r\n"}, 0},
	}
	for i, c := range cases {
		g := &dialGate{}
		conn := &gatedConn{Conn: &chunkConn{chunks: c.chunks}, gate: g}
		buf := make([]byte, 64)
		for range c.chunks {
			if _, err := conn.Read(buf); err != nil {
				t.Fatal(err)
			}
		}
		g.mu.Lock()
		failures := g.failures
		g.mu.Unlock()
		if c.want == -1 {
			if !g.armed.Load() || failures != 0 {
				t.Errorf("case %d: armed=%v failures=%d, want a recorded success", i, g.armed.Load(), failures)
			}
		} else if failures != c.want || g.armed.Load() {
			t.Errorf("case %d: armed=%v failures=%d, want failures=%d", i, g.armed.Load(), failures, c.want)
		}
		if !conn.seen {
			t.Errorf("case %d: first reply not complete", i)
		}
	}
}

// After a re-dial's HELLO is rejected with an IAM backend error, the dial gate makes the
// next dial wait for the backoff instead of following the request rate; a successful
// handshake clears the delay.
func TestDialGate_SpacesOutRedials(t *testing.T) {
	var dropFirst atomic.Bool
	dropFirst.Store(true)
	srv := newScriptedValkey(t, func(conn int64, args []string) (string, bool) {
		switch {
		case strings.EqualFold(args[0], "HELLO") && conn == 2:
			return iamExhausted, false
		case strings.EqualFold(args[0], "GET") && conn == 1 && dropFirst.CompareAndSwap(true, false):
			return "$2\r\nok\r\n", true // the reply arrives, then the connection drops
		}
		return standaloneReply(conn, args)
	})
	const wait = 300 * time.Millisecond
	g := &dialGate{backoff: func(int) time.Duration { return wait }}
	opt := valkey.ClientOption{
		InitAddress:       []string{srv.addr()},
		ForceSingleClient: true,
		PipelineMultiplex: -1, // one connection, so the test controls which one a command uses
		DisableCache:      true,
		AuthCredentialsFn: func(valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
			return valkey.AuthCredentials{Password: "tok"}, nil
		},
		DialCtxFn: gatedDialer(nil, g, false),
	}
	client, err := valkey.NewClient(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx := context.Background()
	get := func() error { return client.Do(ctx, client.B().Get().Key("k").Build()).Error() }
	if err := get(); err != nil { // served by connection 1, which then drops
		t.Fatal(err)
	}
	if err := get(); err == nil || !strings.Contains(err.Error(), "ERR_IAM_EXHAUSTED") { // re-dial 2 is throttled
		t.Fatalf("got %v, want ERR_IAM_EXHAUSTED from the throttled re-dial", err)
	}
	start := time.Now()
	if err := get(); err != nil { // re-dial 3 waits for the gate, then works
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < wait {
		t.Fatalf("re-dial after the IAM error took %v, want at least the %v backoff", elapsed, wait)
	}
	if d := g.delay(); d != 0 {
		t.Fatalf("delay after the successful handshake = %v, want 0", d)
	}
}

func TestGatedDialer_StopsWaitingWhenCtxEnds(t *testing.T) {
	g := &dialGate{backoff: func(int) time.Duration { return time.Hour }}
	g.handshake(handshakeOK)
	g.handshake(handshakeIAMError)
	dialed := false
	dial := gatedDialer(func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
		dialed = true
		return nil, errors.New("unreachable")
	}, g, false)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := dial(ctx, "10.0.0.1:6379", &net.Dialer{}, nil)
	if err == nil || !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "IAM backend error") || dialed {
		t.Fatalf("got %v (dialed=%v), want the ctx error before dialing", err, dialed)
	}
}

// Without a dialer of the caller's, the gate dials the way valkey-go does: over TLS when the
// ClientOption has a TLSConfig. With DisableTCPNoDelay, disableNoDelay reaches the TCP
// connection under TLS; Go can't read TCP_NODELAY back, so the test checks that the dial
// still works.
func TestGatedDialer_DefaultDialTLS(t *testing.T) {
	srv := httptest.NewTLSServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	pool := x509.NewCertPool()
	pool.AddCert(srv.Certificate())
	dial := gatedDialer(nil, &dialGate{}, true)
	conn, err := dial(context.Background(), srv.Listener.Addr().String(), &net.Dialer{}, &tls.Config{RootCAs: pool})
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = conn.Close() }()
	gc, ok := conn.(*gatedConn)
	if !ok {
		t.Fatalf("conn is %T, want *gatedConn", conn)
	}
	if tc, ok := gc.Conn.(*tls.Conn); !ok || !tc.ConnectionState().HandshakeComplete {
		t.Fatalf("dialed %T, want a TLS connection with its handshake done", gc.Conn)
	}
}

// installDialGate keeps a caller's deprecated DialFn underneath the gate. A caller's
// DialCtxFn is checked in TestResolve_DialGateForIAMOnly.
func TestInstallDialGate_KeepsCallerDialFn(t *testing.T) {
	calls := 0
	errDial := errors.New("dial")
	opt := valkey.ClientOption{DialFn: func(string, *net.Dialer, *tls.Config) (net.Conn, error) {
		calls++
		return nil, errDial
	}}
	installDialGate(&opt)
	if _, err := opt.DialCtxFn(context.Background(), "a:1", &net.Dialer{}, nil); !errors.Is(err, errDial) || calls != 1 {
		t.Fatalf("got %v, calls %d", err, calls)
	}
}

// Only IAM puts the dial gate in front of new connections, since only IAM handshakes can
// fail with the IAM backend errors it reacts to. The gate keeps the caller's DialCtxFn
// underneath.
func TestResolve_DialGateForIAMOnly(t *testing.T) {
	cases := []struct {
		name  string
		opts  []Option
		gated bool
	}{
		{"IAM", []Option{WithIAMOptions(staticIAM())}, true},
		{"no auth option", nil, false},
	}
	for _, c := range cases {
		dials := 0
		local, remote := net.Pipe()
		option := initOption("10.0.0.2:6379")
		option.DialCtxFn = func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
			dials++
			return local, nil
		}
		opt, err := ResolveClientOption(context.Background(), option, c.opts...)
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		conn, err := opt.DialCtxFn(context.Background(), "10.0.0.2:6379", &net.Dialer{}, nil)
		if err != nil || dials != 1 {
			t.Fatalf("%s: dial = %v, the caller's DialCtxFn ran %d times, want once", c.name, err, dials)
		}
		_, gated := conn.(*gatedConn)
		_ = conn.Close()
		_ = remote.Close()
		if gated != c.gated {
			t.Errorf("%s: gated = %v, want %v", c.name, gated, c.gated)
		}
	}
}

// chunkConn returns one chunk per Read.
type chunkConn struct {
	net.Conn
	chunks []string
}

func (c *chunkConn) Read(p []byte) (int, error) {
	if len(c.chunks) == 0 {
		return 0, io.EOF
	}
	n := copy(p, c.chunks[0])
	c.chunks = c.chunks[1:]
	return n, nil
}
