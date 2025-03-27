package valkey

import (
	"context"
	"errors"
	"sync"
	"time"
)

const (
	PoolTimeoutExceeded = "pool timeout exceeded"
)

var poolTimeoutError = errors.New(PoolTimeoutExceeded)

func newPool(cap int, dead wire, cleanup time.Duration, minSize int, poolTimeout time.Duration, makeFn func(context.Context) wire) *pool {
	if cap <= 0 {
		cap = DefaultPoolSize
	}

	return &pool{
		size:        0,
		minSize:     minSize,
		cap:         cap,
		dead:        dead,
		make:        makeFn,
		list:        make([]wire, 0, 4),
		cond:        sync.NewCond(&sync.Mutex{}),
		cleanup:     cleanup,
		poolTimeout: poolTimeout,
	}
}

type pool struct {
	dead        wire
	cond        *sync.Cond
	timer       *time.Timer
	make        func(ctx context.Context) wire
	list        []wire
	cleanup     time.Duration
	size        int
	minSize     int
	cap         int
	down        bool
	timerOn     bool
	poolTimeout time.Duration
}

func (p *pool) Acquire(ctx context.Context) (v wire) {
	p.cond.L.Lock()

	poolDeadline := time.Time{}
	if p.poolTimeout > 0 {
		poolDeadline = time.Now().Add(p.poolTimeout)
	}

	if ctxDeadline, ok := ctx.Deadline(); ok {
		if poolDeadline.IsZero() {
			poolDeadline = ctxDeadline
		} else if ctxDeadline.Before(poolDeadline) {
			poolDeadline = ctxDeadline
		}
	}

	var (
		poolCtx context.Context
		cancel  context.CancelFunc
	)
	if !poolDeadline.IsZero() {
		poolCtx, cancel = context.WithDeadline(context.Background(), poolDeadline)
		defer cancel()

		go func() {
			<-poolCtx.Done()
			if poolCtx.Err() == context.DeadlineExceeded { // signal the pool to stop waiting, only if the poolctx is deadline exceeded
				p.cond.Signal()
			}
		}()

	} else {
		poolCtx = ctx
	}

retry:
	for len(p.list) == 0 && p.size == p.cap && !p.down && ctx.Err() == nil && poolCtx.Err() == nil {
		p.cond.Wait()
	}
	if ctx.Err() != nil {

		if deadPipe, ok := p.dead.(*pipe); ok {
			deadPipe.error.Store(&errs{error: ctx.Err()})
			v = deadPipe
		} else {
			v = p.dead
		}
		p.cond.L.Unlock()
		return v
	} else if poolCtx.Err() != nil { // if poolCtx is timedout due to configured poolTimeout

		if deadPipe, ok := p.dead.(*pipe); ok {
			deadPipe.error.Store(&errs{error: poolTimeoutError})
			v = deadPipe
		} else {
			v = p.dead
		}
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
		// unlock before start to make a new wire
		// allowing others to make wires concurrently instead of waiting in line
		p.cond.L.Unlock()
		v = p.make(ctx)
		return v
	}

	i := len(p.list) - 1
	v = p.list[i]
	p.list[i] = nil
	p.list = p.list[:i]
	if v.Error() != nil {
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
	} else {
		p.size--
		v.Close()
	}
	p.cond.L.Unlock()
	p.cond.Signal()
}

func (p *pool) Close() {
	p.cond.L.Lock()
	p.down = true
	p.stopTimer()
	for _, w := range p.list {
		w.Close()
	}
	p.cond.L.Unlock()
	p.cond.Broadcast()
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
