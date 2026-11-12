package valkeygcp

import (
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"time"
)

// Dial wait settings: after n failed dials in a row to an address, new dials to it fail at
// once for a random time up to min(dialWaitBase·2^(n-1), dialWaitCap). An address whose wait
// ended more than dialWaitForget ago without a new failure is forgotten, so the map holds only
// addresses that failed recently, and the next failure there starts over.
const (
	dialWaitBase   = 50 * time.Millisecond
	dialWaitCap    = time.Second
	dialWaitForget = time.Minute
)

// dialWait is the ClientOption.DialCtxFn that NewClient sets when the caller sets none. After
// a dial to an address fails, new dials to that address fail at once, with the error of the
// failed dial, for a random time that grows with each failure in a row, up to dialWaitCap. A
// successful dial resets it.
//
// While a node is down, valkey-go dials it again for nearly every request routed to it, up to
// thousands of times per second per client. The wait doesn't delay recovery: those requests
// fail either way until a topology refresh routes them elsewhere, RetryDelay spaces their
// retries, and the primary that replaces a failed one has a different address.
type dialWait struct {
	dial  func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error)
	now   func() time.Time
	wait  func(failures int) time.Duration
	addrs map[string]*dialFailures
	mu    sync.Mutex
}

// dialFailures is the state of an address whose last dial failed.
type dialFailures struct {
	err   error     // the error of the last failed dial
	at    time.Time // when the last failure that counted happened
	until time.Time // new dials fail at once until then
	n     int       // failed dials in a row
}

func newDialWait() *dialWait {
	return &dialWait{
		dial:  dialDirect,
		now:   time.Now,
		wait:  func(n int) time.Duration { return fullJitter(dialWaitBase, dialWaitCap, n-1) },
		addrs: make(map[string]*dialFailures),
	}
}

// DialCtxFn has the signature of valkey.ClientOption.DialCtxFn.
func (w *dialWait) DialCtxFn(ctx context.Context, dst string, dialer *net.Dialer, tlsConfig *tls.Config) (net.Conn, error) {
	w.mu.Lock()
	start := w.now()
	if f := w.addrs[dst]; f != nil && f.until.After(start) {
		err := &dialWaitError{err: f.err, addr: dst, left: f.until.Sub(start), failures: f.n}
		w.mu.Unlock()
		if ctxErr := ctx.Err(); ctxErr != nil { // as a dial would, report the caller's context ending
			return nil, ctxErr
		}
		return nil, err
	}
	w.mu.Unlock()

	conn, err := w.dial(ctx, dst, dialer, tlsConfig)

	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case err == nil:
		delete(w.addrs, dst)
	case ctx.Err() == nil: // the caller's context ending says nothing about the node
		now := w.now()
		w.forget(now)
		f := w.addrs[dst]
		if f == nil {
			f = &dialFailures{}
			w.addrs[dst] = f
		}
		// Dials that ran at the same time count as one failure: a dial that started before
		// the last counted failure doesn't count, and doesn't move it either, so a dial that
		// starts after the wait ends counts even when an older, slower dial fails first.
		if !f.at.After(start) {
			f.n++
			f.until = now.Add(w.wait(f.n))
			f.at = now
		}
		f.err = err
	}
	return conn, err
}

// forget drops the addresses whose wait ended more than dialWaitForget ago, the dialed one
// included: a failure there now starts over. w.mu must be held.
func (w *dialWait) forget(now time.Time) {
	for addr, f := range w.addrs {
		if now.Sub(f.until) > dialWaitForget {
			delete(w.addrs, addr)
		}
	}
}

// dialDirect dials the way valkey-go does when ClientOption.DialCtxFn is not set.
func dialDirect(ctx context.Context, dst string, dialer *net.Dialer, tlsConfig *tls.Config) (net.Conn, error) {
	if tlsConfig != nil {
		d := tls.Dialer{NetDialer: dialer, Config: tlsConfig}
		return d.DialContext(ctx, "tcp", dst)
	}
	return dialer.DialContext(ctx, "tcp", dst)
}

// dialWaitError is returned for a dial that dialWait didn't try, because the last dial to
// the address failed a moment ago. It wraps the error of that dial.
type dialWaitError struct {
	err      error
	addr     string
	left     time.Duration
	failures int
}

func (e *dialWaitError) Error() string {
	return fmt.Sprintf("valkeygcp: not dialing %s for another %v after %d failed dials in a row: %v",
		e.addr, e.left.Round(time.Millisecond), e.failures, e.err)
}

func (e *dialWaitError) Unwrap() error { return e.err }
