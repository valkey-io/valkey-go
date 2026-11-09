package valkey

import (
	"context"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go/internal/util"
)

// flight is the shared state of one fn execution. err is written exactly once,
// before ch is closed, so any goroutine that observed the close may read err
// without further synchronization.
type flight struct {
	err   error
	ch    chan struct{}
	wake  chan struct{} // only set by DelayDo; closed to end its delay early
	due   time.Time     // when the delay of DelayDo ends; guarded by call.mu
	woken bool          // guarded by call.mu
}

type call struct {
	ts time.Time
	fl *flight
	nx *flight // the flight that the next Do or DelayDo starts, once next has made it
	cn int
	mu sync.Mutex
}

// Do runs fn, deduping concurrent callers: a caller that arrives while fn is
// already running does not start a second run — it waits for the running one
// and returns its error.
//
// The waiting caller must get the real error. It used to get nil even when fn
// failed, and nil looks like success. sentinelClient.refreshRetry retries
// refresh() until it returns nil, so a refreshRetry that waited on someone
// else's failed refresh stopped retrying while the master was still
// unresolved. clusterClient.pick reported the same failure as ErrNoSlot rather
// than the refresh error behind it.
func (c *call) Do(ctx context.Context, fn func() error) error {
	c.mu.Lock()
	c.cn++
	fl := c.fl
	if fl != nil {
		c.mu.Unlock()
		if ctxCh := ctx.Done(); ctxCh != nil {
			select {
			case <-fl.ch:
			case <-ctxCh:
				return ctx.Err()
			}
		} else {
			<-fl.ch
		}
		return fl.err
	}
	fl = c.start()
	c.mu.Unlock()
	return c.do(fl, fn)
}

// DelayDo sleeps for delay then runs fn, deduping concurrent callers via singleflight.
// Expedite can cut the sleep short.
func (c *call) DelayDo(delay time.Duration, fn func() error) {
	c.mu.Lock()
	if c.fl != nil {
		c.mu.Unlock()
		return
	}
	fl := c.start()
	fl.wake = make(chan struct{})
	fl.due = time.Now().Add(delay)
	c.cn++
	c.mu.Unlock()
	go func(delay time.Duration, fl *flight, fn func() error) {
		if delay > 0 {
			timer := time.NewTimer(delay)
			select {
			case <-timer.C:
			case <-fl.wake:
				timer.Stop()
			}
		}
		c.do(fl, fn)
	}(delay, fl, fn)
}

// Expedite makes a flight started by DelayDo that would still wait longer than within run after a random delay
// of up to within instead, or at once when within is 0. A flight whose delay ends within within is left as it
// is, so repeated calls with the same within do not make a flight run sooner and sooner, and keep the random
// spread of its delay. It does nothing when no flight is in progress, or the flight was started by Do.
func (c *call) Expedite(within time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fl := c.fl
	if fl == nil || fl.wake == nil || fl.woken {
		return
	}
	now := time.Now()
	if !fl.due.After(now.Add(within)) {
		return
	}
	var delay time.Duration
	if within > 0 {
		delay = time.Duration(util.FastRand(int(within)))
	}
	fl.due = now.Add(delay)
	if delay <= 0 {
		endDelay(fl)
		return
	}
	time.AfterFunc(delay, func() {
		c.mu.Lock()
		endDelay(fl)
		c.mu.Unlock()
	})
}

// endDelay ends the delay of a flight started by DelayDo, once. call.mu must be held.
func endDelay(fl *flight) {
	if !fl.woken {
		fl.woken = true
		close(fl.wake)
	}
}

// start makes a flight current and returns it: the one made by next, if any, so that its waiters get it.
// c.mu must be held.
func (c *call) start() *flight {
	fl := c.nx
	if fl == nil {
		fl = &flight{ch: make(chan struct{})}
	}
	c.nx = nil
	c.fl = fl
	return fl
}

// next returns the flight in progress or, if there is none, the flight that the next Do or DelayDo
// will start. Its ch is closed once it has run, so a caller can wait for the next completion without
// polling, and without missing one that happens right after next returns.
func (c *call) next() *flight {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.fl != nil {
		return c.fl
	}
	if c.nx == nil {
		c.nx = &flight{ch: make(chan struct{})}
	}
	return c.nx
}

func (c *call) do(fl *flight, fn func() error) error {
	fl.err = fn()
	c.mu.Lock()
	c.fl = nil
	c.cn = 0
	c.ts = time.Now()
	c.mu.Unlock()
	close(fl.ch)
	return fl.err
}

func (c *call) suppressing() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cn
}
