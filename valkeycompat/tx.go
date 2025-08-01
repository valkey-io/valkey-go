package valkeycompat

import (
	"context"
	"errors"
	"time"
	"unsafe"

	"github.com/valkey-io/valkey-go"
)

var TxFailedErr = errors.New("valkey: transaction failed")

var _ Pipeliner = (*TxPipeline)(nil)

type rePipeline = Pipeline

func newTxPipeline(real valkey.Client) *TxPipeline {
	return &TxPipeline{rePipeline: newPipeline(real)}
}

type TxPipeline struct {
	*rePipeline
}

func (c *TxPipeline) Exec(ctx context.Context) ([]Cmder, error) {
	p := c.comp.client.(*proxy)
	if len(p.cmds) == 0 {
		return nil, nil
	}

	rets := c.rets
	cmds := p.cmds
	c.rets = nil
	p.cmds = nil

	cmds = append(cmds, c.comp.client.B().Multi().Build(), c.comp.client.B().Exec().Build())
	for i := len(cmds) - 2; i >= 1; i-- {
		j := i - 1
		cmds[j], cmds[i] = cmds[i], cmds[j]
	}

	resp := p.DoMulti(ctx, cmds...)
	results, err := resp[len(resp)-1].ToArray()
	if valkey.IsValkeyNil(err) {
		err = TxFailedErr
	}
	for i, r := range results {
		rets[i].SetErr(nil)
		rets[i].from(*(*valkey.ValkeyResult)(unsafe.Pointer(&proxyresult{
			err: resp[i+1].NonValkeyError(),
			val: r,
		})))
	}
	return rets, err
}

func (c *TxPipeline) Pipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	if err := fn(c); err != nil {
		return nil, err
	}
	return c.Exec(ctx)
}

func (c *TxPipeline) Pipeline() Pipeliner {
	return c
}

func (c *TxPipeline) TxPipelined(ctx context.Context, fn func(Pipeliner) error) ([]Cmder, error) {
	return c.Pipelined(ctx, fn)
}

func (c *TxPipeline) TxPipeline() Pipeliner {
	return c
}

var _ valkey.Client = (*txproxy)(nil)

type txproxy struct {
	valkey.CoreClient
}

func (p *txproxy) DoCache(_ context.Context, _ valkey.Cacheable, _ time.Duration) (resp valkey.ValkeyResult) {
	panic("not implemented")
}

func (p *txproxy) DoMultiCache(_ context.Context, _ ...valkey.CacheableTTL) (resp []valkey.ValkeyResult) {
	panic("not implemented")
}

func (p *txproxy) DoStream(_ context.Context, _ valkey.Completed) valkey.ValkeyResultStream {
	panic("not implemented")
}

func (p *txproxy) DoMultiStream(_ context.Context, _ ...valkey.Completed) valkey.MultiValkeyResultStream {
	panic("not implemented")
}

func (p *txproxy) Dedicated(_ func(valkey.DedicatedClient) error) (err error) {
	panic("not implemented")
}

func (p *txproxy) Dedicate() (client valkey.DedicatedClient, cancel func()) {
	panic("not implemented")
}

func (p *txproxy) Nodes() map[string]valkey.Client {
	panic("not implemented")
}

func (p *txproxy) Mode() valkey.ClientMode {
	panic("not implemented")
}

type Tx interface {
	CoreCmdable
	Watch(ctx context.Context, keys ...string) *StatusCmd
	Unwatch(ctx context.Context, keys ...string) *StatusCmd
	Close(ctx context.Context) error
}

func newTx(client valkey.DedicatedClient, cancel func()) *tx {
	return &tx{CoreCmdable: NewAdapter(&txproxy{CoreClient: client}), cancel: cancel}
}

type tx struct {
	CoreCmdable
	cancel func()
}

func (t *tx) Watch(ctx context.Context, keys ...string) *StatusCmd {
	ret := &StatusCmd{}
	if len(keys) != 0 {
		client := t.CoreCmdable.(*Compat).client
		ret.from(client.Do(ctx, client.B().Watch().Key(keys...).Build()))
	}
	return ret
}

func (t *tx) Unwatch(ctx context.Context, _ ...string) *StatusCmd {
	ret := &StatusCmd{}
	client := t.CoreCmdable.(*Compat).client
	ret.from(client.Do(ctx, client.B().Unwatch().Build()))
	return ret
}

func (t *tx) Close(_ context.Context) error {
	t.cancel()
	return nil
}
