package valkeyhook

import (
	"context"
	"time"
	"unsafe"

	"github.com/valkey-io/valkey-go"
)

var _ valkey.Client = (*hookclient)(nil)

// Hook allows user to intercept valkey.Client by using WithHook
type Hook interface {
	Do(client valkey.Client, ctx context.Context, cmd valkey.Completed) (resp valkey.ValkeyResult)
	DoMulti(client valkey.Client, ctx context.Context, multi ...valkey.Completed) (resps []valkey.ValkeyResult)
	DoCache(client valkey.Client, ctx context.Context, cmd valkey.Cacheable, ttl time.Duration) (resp valkey.ValkeyResult)
	DoMultiCache(client valkey.Client, ctx context.Context, multi ...valkey.CacheableTTL) (resps []valkey.ValkeyResult)
	Receive(client valkey.Client, ctx context.Context, subscribe valkey.Completed, fn func(msg valkey.PubSubMessage)) (err error)
	DoStream(client valkey.Client, ctx context.Context, cmd valkey.Completed) valkey.ValkeyResultStream
	DoMultiStream(client valkey.Client, ctx context.Context, multi ...valkey.Completed) valkey.MultiValkeyResultStream
}

// WithHook wraps valkey.Client with Hook and allows the user to intercept valkey.Client
func WithHook(client valkey.Client, hook Hook) valkey.Client {
	return &hookclient{client: client, hook: hook}
}

type hookclient struct {
	client valkey.Client
	hook   Hook
}

func (c *hookclient) B() valkey.Builder {
	return c.client.B()
}

func (c *hookclient) Do(ctx context.Context, cmd valkey.Completed) (resp valkey.ValkeyResult) {
	return c.hook.Do(c.client, ctx, cmd)
}

func (c *hookclient) DoMulti(ctx context.Context, multi ...valkey.Completed) (resp []valkey.ValkeyResult) {
	return c.hook.DoMulti(c.client, ctx, multi...)
}

func (c *hookclient) DoCache(ctx context.Context, cmd valkey.Cacheable, ttl time.Duration) (resp valkey.ValkeyResult) {
	return c.hook.DoCache(c.client, ctx, cmd, ttl)
}

func (c *hookclient) DoMultiCache(ctx context.Context, multi ...valkey.CacheableTTL) (resps []valkey.ValkeyResult) {
	return c.hook.DoMultiCache(c.client, ctx, multi...)
}

func (c *hookclient) DoStream(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResultStream {
	return c.hook.DoStream(c.client, ctx, cmd)
}

func (c *hookclient) DoMultiStream(ctx context.Context, multi ...valkey.Completed) valkey.MultiValkeyResultStream {
	return c.hook.DoMultiStream(c.client, ctx, multi...)
}

func (c *hookclient) Dedicated(fn func(valkey.DedicatedClient) error) (err error) {
	return c.client.Dedicated(func(client valkey.DedicatedClient) error {
		return fn(&dedicated{client: &extended{DedicatedClient: client}, hook: c.hook})
	})
}

func (c *hookclient) Dedicate() (valkey.DedicatedClient, func()) {
	client, cancel := c.client.Dedicate()
	return &dedicated{client: &extended{DedicatedClient: client}, hook: c.hook}, cancel
}

func (c *hookclient) Receive(ctx context.Context, subscribe valkey.Completed, fn func(msg valkey.PubSubMessage)) (err error) {
	return c.hook.Receive(c.client, ctx, subscribe, fn)
}

func (c *hookclient) Nodes() map[string]valkey.Client {
	nodes := c.client.Nodes()
	for addr, client := range nodes {
		nodes[addr] = &hookclient{client: client, hook: c.hook}
	}
	return nodes
}

func (c *hookclient) Mode() valkey.ClientMode {
	return c.client.Mode()
}

func (c *hookclient) Close() {
	c.client.Close()
}

var _ valkey.DedicatedClient = (*dedicated)(nil)

type dedicated struct {
	client *extended
	hook   Hook
}

func (d *dedicated) B() valkey.Builder {
	return d.client.B()
}

func (d *dedicated) Do(ctx context.Context, cmd valkey.Completed) (resp valkey.ValkeyResult) {
	return d.hook.Do(d.client, ctx, cmd)
}

func (d *dedicated) DoMulti(ctx context.Context, multi ...valkey.Completed) (resp []valkey.ValkeyResult) {
	return d.hook.DoMulti(d.client, ctx, multi...)
}

func (d *dedicated) Receive(ctx context.Context, subscribe valkey.Completed, fn func(msg valkey.PubSubMessage)) (err error) {
	return d.hook.Receive(d.client, ctx, subscribe, fn)
}

func (d *dedicated) SetPubSubHooks(hooks valkey.PubSubHooks) <-chan error {
	return d.client.SetPubSubHooks(hooks)
}

func (d *dedicated) Close() {
	d.client.Close()
}

var _ valkey.Client = (*extended)(nil)

type extended struct {
	valkey.DedicatedClient
}

func (e *extended) DoCache(ctx context.Context, cmd valkey.Cacheable, ttl time.Duration) (resp valkey.ValkeyResult) {
	panic("DoCache() is not allowed with valkey.DedicatedClient")
}

func (e *extended) DoMultiCache(ctx context.Context, multi ...valkey.CacheableTTL) (resp []valkey.ValkeyResult) {
	panic("DoMultiCache() is not allowed with valkey.DedicatedClient")
}
func (c *extended) DoStream(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResultStream {
	panic("DoStream() is not allowed with valkey.DedicatedClient")
}

func (c *extended) DoMultiStream(ctx context.Context, multi ...valkey.Completed) valkey.MultiValkeyResultStream {
	panic("DoMultiStream() is not allowed with valkey.DedicatedClient")
}

func (e *extended) Dedicated(fn func(valkey.DedicatedClient) error) (err error) {
	panic("Dedicated() is not allowed with valkey.DedicatedClient")
}

func (e *extended) Dedicate() (client valkey.DedicatedClient, cancel func()) {
	panic("Dedicate() is not allowed with valkey.DedicatedClient")
}

func (e *extended) Nodes() map[string]valkey.Client {
	panic("Nodes() is not allowed with valkey.DedicatedClient")
}

func (e *extended) Mode() valkey.ClientMode {
	panic("Mode() is not allowed with valkey.DedicatedClient")
}

type result struct {
	err error
	val valkey.ValkeyMessage
}

func NewErrorResult(err error) valkey.ValkeyResult {
	r := result{err: err}
	return *(*valkey.ValkeyResult)(unsafe.Pointer(&r))
}

type stream struct {
	p *int
	w *int
	e error
	n int
}

func NewErrorResultStream(err error) valkey.ValkeyResultStream {
	r := stream{e: err}
	return *(*valkey.ValkeyResultStream)(unsafe.Pointer(&r))
}
