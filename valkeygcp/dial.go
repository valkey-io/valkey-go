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
// once for a random time up to min(dialWaitBase·2^(n-1), dialWaitCap). An address is
// forgotten once dialWaitForget has passed since its last success or the end of its wait, so
// the map holds only the addresses dialed in about the last minute, and the next failure at a
// forgotten address starts over.
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
// While a node is down, valkey-go dials it again for nearly every request routed to it. When
// those dials fail at once (refused or reset), that is up to thousands of dials per second per
// client, and the wait spaces them out. After a failover in cluster mode, the new primary has
// a different address, so the wait doesn't delay it. A node that comes back at the same
// address, as after a network fault, can be delayed by the wait in progress: up to
// dialWaitCap before its first new dial.
//
// The endpoints the caller gives (InitAddress and Standalone.ReplicaAddress) are dialed as
// usual. Each is a fixed address in front of several nodes, so a failed dial there says little
// about the next one. Also, with PreferInitAddressRefresh, topology refreshes go only through
// InitAddress, and a refresh is what routes requests away from a failed node.
type dialWait struct {
	dial      func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error)
	now       func() time.Time
	wait      func(failures int) time.Duration
	endpoints map[string]bool // dialed as usual; read without mu, since only newDialWait writes it
	addrs     map[string]*dialState
	mu        sync.Mutex
}

// dialState is the state of an address dialed in about the last minute. A success sets at
// and until to its time and n to 0: new dials don't wait, and a dial that started before the
// success doesn't count when it fails later.
type dialState struct {
	err   error     // the error of the last failed dial
	at    time.Time // when the last success, or the last failure that counted, happened
	until time.Time // new dials fail at once until then
	n     int       // failed dials in a row
}

// newDialWait returns a dialWait that dials the endpoints as usual, without a wait.
func newDialWait(endpoints ...string) *dialWait {
	w := &dialWait{
		dial:      dialDirect,
		now:       time.Now,
		wait:      func(n int) time.Duration { return fullJitter(dialWaitBase, dialWaitCap, n-1) },
		endpoints: make(map[string]bool, len(endpoints)),
		addrs:     make(map[string]*dialState),
	}
	for _, addr := range endpoints {
		w.endpoints[addr] = true
	}
	return w
}

// DialCtxFn has the signature of valkey.ClientOption.DialCtxFn.
func (w *dialWait) DialCtxFn(ctx context.Context, dst string, dialer *net.Dialer, tlsConfig *tls.Config) (net.Conn, error) {
	if w.endpoints[dst] {
		return w.dial(ctx, dst, dialer, tlsConfig)
	}
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
		now := w.now()
		w.forget(now)
		// Keep the address with the time of the success, so that a dial that started before
		// it and fails later doesn't count (see below).
		w.addrs[dst] = &dialState{at: now, until: now}
	case ctx.Err() == nil: // the caller's context ending says nothing about the node
		// So the wait may never start for a node that drops packets: its dials end by
		// timeout, and when the caller's deadline is shorter than Dialer.Timeout (5s by
		// default), the caller's context ends first. Those dials don't come in bursts,
		// though: each lasts the caller's whole deadline, and valkey-go runs one dial at a
		// time per connection, with other callers waiting for its result.
		now := w.now()
		w.forget(now)
		f := w.addrs[dst]
		if f == nil {
			f = &dialState{}
			w.addrs[dst] = f
		}
		// Dials that ran at the same time count as one failure: a dial that started before
		// the last counted failure doesn't count, and doesn't move it either, so a dial that
		// starts after the wait ends counts even when an older, slower dial fails first. A
		// dial that started before the last success doesn't count either.
		if !f.at.After(start) {
			f.n++
			f.until = now.Add(w.wait(f.n))
			f.at = now
		}
		f.err = err
	}
	return conn, err
}

// forget drops the addresses whose until, the time of their last success or the end of
// their wait, is more than dialWaitForget ago. A failure at a dropped address, the dialed one
// included, starts over. w.mu must be held.
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
