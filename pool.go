package valkey

import (
	"context"
	"errors"
	"sync"
	"time"
)

// errAcquireComplete is a special error used to indicate that the Acquire operation has completed successfully
var errAcquireComplete = errors.New("acquire complete")

func newPool(cap int, dead wire, cleanup time.Duration, minSize int, makeFn func(context.Context) wire) *pool {
	if cap <= 0 {
		cap = DefaultPoolSize
	}

	return &pool{
		size:    0,
		minSize: minSize,
		cap:     cap,
		dead:    dead,
		make:    makeFn,
		list:    make([]wire, 0, 4),
		cond:    sync.NewCond(&sync.Mutex{}),
		cleanup: cleanup,
	}
}

type pool struct {
	dead    wire
	cond    *sync.Cond
	timer   *time.Timer
	make    func(ctx context.Context) wire
	list    []wire
	cleanup time.Duration
	size    int
	minSize int
	cap     int
	down    bool
	timerOn bool

	dialing      int
	waiters      int
	waitCount    uint64
	waitDuration time.Duration
	waitCanceled uint64
}

type unreservedWire struct {
	wire
}

func (p *pool) Acquire(ctx context.Context) (v wire) {
	p.cond.L.Lock()
	waited := false

	// Set up ctx handling when waiting for an available connection
	if len(p.list) == 0 && p.size == p.cap && !p.down && ctx.Err() == nil && ctx.Done() != nil {
		poolCtx, cancel := context.WithCancelCause(ctx)
		defer cancel(errAcquireComplete)

		go func() {
			<-poolCtx.Done()
			if context.Cause(poolCtx) != errAcquireComplete { // no need to broadcast if the poolCtx is cancelled explicitly.
				p.cond.Broadcast()
			}
		}()
	}

retry:
	if len(p.list) == 0 && p.size == p.cap && !p.down && ctx.Err() == nil {
		if !waited {
			p.waitCount++
			waited = true
		}
		p.waiters++
		start := time.Now()
		for len(p.list) == 0 && p.size == p.cap && !p.down && ctx.Err() == nil {
			p.cond.Wait()
		}
		p.waitDuration += time.Since(start)
		p.waiters--
	}

	if ctx.Err() != nil {
		if waited {
			p.waitCanceled++
		}
		deadPipe := deadFn()
		deadPipe.error.Store(&errs{error: ctx.Err()})
		v = &unreservedWire{wire: deadPipe}
		p.cond.L.Unlock()
		return v
	}

	if p.down {
		v = p.dead
		p.cond.L.Unlock()
		return v
	}
	if len(p.list) == 0 {
		p.size++
		p.dialing++
		// unlock before start to make a new wire
		// allowing others to make wires concurrently instead of waiting in line
		p.cond.L.Unlock()
		v = p.make(ctx)
		stopped := v.StopTimer()
		p.cond.L.Lock()
		p.dialing--
		if !stopped {
			p.size--
			v.Close()
			goto retry
		}
		p.cond.L.Unlock()
		return v
	}

	i := len(p.list) - 1
	v = p.list[i]
	p.list[i] = nil
	p.list = p.list[:i]
	if !v.StopTimer() || v.Error() != nil {
		p.size--
		v.Close()
		goto retry
	}
	p.cond.L.Unlock()
	return v
}

func (p *pool) Store(v wire) {
	p.cond.L.Lock()
	if !p.down && v.Error() == nil {
		p.list = append(p.list, v)
		p.startTimerIfNeeded()
		v.ResetTimer()
	} else {
		if _, ok := v.(*unreservedWire); !ok {
			p.size--
		}
		v.Close()
	}
	p.cond.L.Unlock()
	p.cond.Signal()
}

func (p *pool) Close() {
	p.cond.L.Lock()
	p.down = true
	p.stopTimer()
	for i, w := range p.list {
		w.Close()
		p.list[i] = nil
	}
	p.size -= len(p.list)
	p.list = p.list[:0]
	p.cond.L.Unlock()
	p.cond.Broadcast()
}

func (p *pool) stats() PoolStats {
	p.cond.L.Lock()
	stats := PoolStats{
		Capacity:     p.cap,
		Reserved:     p.size,
		Dialing:      p.dialing,
		Idle:         len(p.list),
		Waiters:      p.waiters,
		Closed:       p.down,
		WaitCount:    p.waitCount,
		WaitDuration: p.waitDuration,
		WaitCanceled: p.waitCanceled,
	}
	p.cond.L.Unlock()
	return stats
}

func (p *pool) startTimerIfNeeded() {
	if p.cleanup == 0 || p.timerOn || len(p.list) <= p.minSize {
		return
	}

	p.timerOn = true
	if p.timer == nil {
		p.timer = time.AfterFunc(p.cleanup, p.removeIdleConns)
	} else {
		p.timer.Reset(p.cleanup)
	}
}

func (p *pool) removeIdleConns() {
	p.cond.L.Lock()
	defer p.cond.L.Unlock()

	newLen := min(p.minSize, len(p.list))
	for i, w := range p.list[newLen:] {
		w.Close()
		p.list[newLen+i] = nil
		p.size--
	}

	p.list = p.list[:newLen]
	p.timerOn = false
}

func (p *pool) stopTimer() {
	p.timerOn = false
	if p.timer != nil {
		p.timer.Stop()
	}
}
