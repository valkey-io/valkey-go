package valkeygcp

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
)

// dialFunc is the type of valkey.ClientOption.DialCtxFn.
type dialFunc = func(ctx context.Context, dst string, dialer *net.Dialer, cfg *tls.Config) (net.Conn, error)

// dialGate spaces out new connections after Memorystore rejected a connection's HELLO with
// an IAM backend error (ERR_IAM_EXHAUSTED or ERR_IAM_OTHER). valkey-go doesn't retry error
// replies: after a failed handshake the next command dials again at once, so without the
// gate re-dials would follow the application's request rate and add to the load on the IAM
// backend.
//
// The gate learns each handshake's outcome from the first reply on the connection (see
// gatedConn). It delays dials only after a first successful handshake, so a client that
// can't connect at all still fails fast; connect backs off construction with the caller's
// ctx instead.
type dialGate struct {
	armed    atomic.Bool
	backoff  func(attempt int) time.Duration // nil means iamBackoff; tests fix it
	mu       sync.Mutex
	failures int // consecutive handshakes rejected with ERR_IAM_*
}

// handshake outcomes, read from the first reply on a new connection.
const (
	handshakeOK       = iota // HELLO succeeded
	handshakeIAMError        // ERR_IAM_EXHAUSTED or ERR_IAM_OTHER
	handshakeOther           // another error reply, such as WRONGPASS: no change
)

// handshake records the outcome of one new connection's handshake. A success arms the gate
// and clears the failure count; an IAM error adds to it.
func (g *dialGate) handshake(outcome int) {
	switch outcome {
	case handshakeOK:
		g.armed.Store(true)
		g.mu.Lock()
		g.failures = 0
		g.mu.Unlock()
	case handshakeIAMError:
		g.mu.Lock()
		g.failures++
		g.mu.Unlock()
	}
}

// delay is how long the next dial waits: full jitter over the IAM backoff for the number of
// consecutive IAM errors, so concurrent re-dials don't all fire at the same moment.
func (g *dialGate) delay() time.Duration {
	if !g.armed.Load() {
		return 0
	}
	g.mu.Lock()
	n := g.failures
	g.mu.Unlock()
	if n == 0 {
		return 0
	}
	if g.backoff != nil {
		return g.backoff(n - 1)
	}
	return iamBackoff(n - 1)
}

// installDialGate wraps opt's dialer with a new gate. It keeps a caller's DialCtxFn or
// DialFn as the underlying dialer.
func installDialGate(opt *valkey.ClientOption) {
	base := opt.DialCtxFn
	if fn := opt.DialFn; base == nil && fn != nil { // a caller's (deprecated) DialFn keeps working
		base = func(_ context.Context, dst string, d *net.Dialer, cfg *tls.Config) (net.Conn, error) {
			return fn(dst, d, cfg)
		}
	}
	opt.DialCtxFn = gatedDialer(base, &dialGate{}, opt.DisableTCPNoDelay)
}

// gatedDialer makes each dial wait for the gate and report its handshake. A nil base dials
// the way valkey-go does without a custom dialer. valkey-go applies DisableTCPNoDelay only
// to a connection it can see is a *net.TCPConn, which the wrapper hides, so noDelayOff
// applies it here.
func gatedDialer(base dialFunc, g *dialGate, noDelayOff bool) dialFunc {
	if base == nil {
		base = defaultDial
	}
	return func(ctx context.Context, dst string, dialer *net.Dialer, cfg *tls.Config) (net.Conn, error) {
		if d := g.delay(); d > 0 {
			t := time.NewTimer(d)
			select {
			case <-ctx.Done():
				t.Stop()
				return nil, fmt.Errorf("valkeygcp: waiting %v to reconnect after an IAM backend error: %w", d.Round(time.Millisecond), ctx.Err())
			case <-t.C:
			}
		}
		conn, err := base(ctx, dst, dialer, cfg)
		if err != nil {
			return nil, err
		}
		if noDelayOff {
			disableNoDelay(conn)
		}
		return &gatedConn{Conn: conn, gate: g}, nil
	}
}

// defaultDial dials like valkey-go does without a custom dialer.
func defaultDial(ctx context.Context, dst string, dialer *net.Dialer, cfg *tls.Config) (net.Conn, error) {
	if cfg != nil {
		td := tls.Dialer{NetDialer: dialer, Config: cfg}
		return td.DialContext(ctx, "tcp", dst)
	}
	return dialer.DialContext(ctx, "tcp", dst)
}

// disableNoDelay turns TCP_NODELAY off on conn, or on the TCP connection under a TLS one.
func disableNoDelay(conn net.Conn) {
	if c, ok := conn.(*tls.Conn); ok {
		conn = c.NetConn()
	}
	if c, ok := conn.(*net.TCPConn); ok {
		_ = c.SetNoDelay(false)
	}
}

// maxReplyPeek bounds how much of the first reply gatedConn keeps.
const maxReplyPeek = 256

// gatedConn passes the first reply line on a new connection, the answer to HELLO, to the
// gate. Only valkey-go's reader goroutine calls Read, so the fields need no lock.
type gatedConn struct {
	net.Conn
	gate *dialGate
	seen bool
	line []byte
}

// Read reads from the connection, showing the bytes to peek until the first reply line has
// been seen.
func (c *gatedConn) Read(p []byte) (int, error) {
	n, err := c.Conn.Read(p)
	if !c.seen && n > 0 {
		c.peek(p[:n])
	}
	return n, err
}

// peek collects the first reply line. Once the line is complete, or maxReplyPeek bytes
// long, it tells the gate whether the handshake succeeded, failed with an IAM backend
// error, or failed for another reason.
func (c *gatedConn) peek(b []byte) {
	if i := bytes.IndexByte(b, '\n'); i >= 0 {
		b, c.seen = b[:i], true
	}
	if room := maxReplyPeek - len(c.line); room > 0 {
		c.line = append(c.line, b[:min(len(b), room)]...)
	}
	if len(c.line) >= maxReplyPeek {
		c.seen = true
	}
	if !c.seen {
		return
	}
	line := c.line
	c.line = nil
	switch {
	case len(line) == 0:
	case line[0] != '-':
		c.gate.handshake(handshakeOK)
	case bytes.Contains(line, []byte("ERR_IAM_EXHAUSTED")) || bytes.Contains(line, []byte("ERR_IAM_OTHER")):
		c.gate.handshake(handshakeIAMError)
	default:
		c.gate.handshake(handshakeOther)
	}
}
