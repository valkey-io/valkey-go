package valkey

import (
	"context"
)

type queuedCmd struct {
	ch    chan ValkeyResult
	one   Completed
	multi []Completed
	resps []ValkeyResult
}

type flowBuffer struct {
	f chan queuedCmd
	r chan queuedCmd
	w chan queuedCmd
	c *chan ValkeyResult
}

var _ queue = (*flowBuffer)(nil)

func newFlowBuffer(factor int) *flowBuffer {
	if factor <= 0 {
		factor = DefaultRingScale
	}
	size := 2 << (factor - 1)

	r := &flowBuffer{
		f: make(chan queuedCmd, size),
		r: make(chan queuedCmd, size),
		w: make(chan queuedCmd, size),
	}
	for i := 0; i < size; i++ {
		r.f <- queuedCmd{
			ch: make(chan ValkeyResult),
		}
	}
	return r
}

func (b *flowBuffer) PutOne(ctx context.Context, m Completed) (chan ValkeyResult, error) {
	select {
	case cmd := <-b.f:
		cmd.one = m
		b.w <- cmd
		return cmd.ch, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (b *flowBuffer) PutMulti(ctx context.Context, m []Completed, resps []ValkeyResult) (chan ValkeyResult, error) {
	select {
	case cmd := <-b.f:
		cmd.multi, cmd.resps = m, resps
		b.w <- cmd
		return cmd.ch, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// NextWriteCmd should be only called by one dedicated thread
func (b *flowBuffer) NextWriteCmd() (one Completed, multi []Completed, ch chan ValkeyResult) {
	select {
	case cmd := <-b.w:
		one, multi, ch = cmd.one, cmd.multi, cmd.ch
		b.r <- cmd
	default:
	}
	return
}

// WaitForWrite should be only called by one dedicated thread
func (b *flowBuffer) WaitForWrite() (one Completed, multi []Completed, ch chan ValkeyResult) {
	cmd := <-b.w
	one, multi, ch = cmd.one, cmd.multi, cmd.ch
	b.r <- cmd
	return
}

// NextResultCh should be only called by one dedicated thread
func (b *flowBuffer) NextResultCh() (one Completed, multi []Completed, ch chan ValkeyResult, resps []ValkeyResult) {
	select {
	case cmd := <-b.r:
		b.c = &cmd.ch
		one, multi, ch, resps = cmd.one, cmd.multi, cmd.ch, cmd.resps
	default:
	}
	return
}

// FinishResult should be only called by one dedicated thread
func (b *flowBuffer) FinishResult() {
	if b.c != nil {
		b.f <- queuedCmd{ch: *b.c}
		b.c = nil
	}
}
