package valkey

import (
	"context"
	"sync"
	"time"
)

// flight is the shared state of one fn execution. err is written exactly once,
// before ch is closed, so any goroutine that observed the close may read err
// without further synchronization.
type flight struct {
	err   error
	ch    chan struct{}
	wake  chan struct{} // only set by DelayDo; closed by Expedite to end the delay early
	woken bool          // guarded by call.mu
}

type call struct {
	ts time.Time
	fl *flight
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
	fl = &flight{ch: make(chan struct{})}
	c.fl = fl
	c.mu.Unlock()
	return c.do(fl, fn)
}

// DelayDo sleeps for delay then runs fn, deduping concurrent callers via singleflight.
// The sleep can be cut short by Expedite.
func (c *call) DelayDo(delay time.Duration, fn func() error) {
	c.mu.Lock()
	if c.fl != nil {
		c.mu.Unlock()
		return
	}
	fl := &flight{ch: make(chan struct{}), wake: make(chan struct{})}
	c.fl = fl
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

// Expedite makes a pending DelayDo flight that is still sleeping run fn immediately.
// It is a no-op when there is no pending flight or the flight was started by Do.
func (c *call) Expedite() {
	c.mu.Lock()
	if fl := c.fl; fl != nil && fl.wake != nil && !fl.woken {
		fl.woken = true
		close(fl.wake)
	}
	c.mu.Unlock()
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
