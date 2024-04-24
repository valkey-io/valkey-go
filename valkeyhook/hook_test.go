package valkeyhook

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

type hook struct{}

func (h *hook) Do(client valkey.Client, ctx context.Context, cmd valkey.Completed) (resp valkey.ValkeyResult) {
	return client.Do(ctx, cmd)
}

func (h *hook) DoMulti(client valkey.Client, ctx context.Context, multi ...valkey.Completed) (resps []valkey.ValkeyResult) {
	return client.DoMulti(ctx, multi...)
}

func (h *hook) DoCache(client valkey.Client, ctx context.Context, cmd valkey.Cacheable, ttl time.Duration) (resp valkey.ValkeyResult) {
	return client.DoCache(ctx, cmd, ttl)
}

func (h *hook) DoMultiCache(client valkey.Client, ctx context.Context, multi ...valkey.CacheableTTL) (resps []valkey.ValkeyResult) {
	return client.DoMultiCache(ctx, multi...)
}

func (h *hook) Receive(client valkey.Client, ctx context.Context, subscribe valkey.Completed, fn func(msg valkey.PubSubMessage)) (err error) {
	return client.Receive(ctx, subscribe, fn)
}

func (h *hook) DoStream(client valkey.Client, ctx context.Context, cmd valkey.Completed) valkey.ValkeyResultStream {
	return client.DoStream(ctx, cmd)
}

func (h *hook) DoMultiStream(client valkey.Client, ctx context.Context, multi ...valkey.Completed) valkey.MultiValkeyResultStream {
	return client.DoMultiStream(ctx, multi...)
}

type wronghook struct {
	DoFn func(client valkey.Client)
}

func (w *wronghook) Do(client valkey.Client, ctx context.Context, cmd valkey.Completed) (resp valkey.ValkeyResult) {
	w.DoFn(client)
	return valkey.ValkeyResult{}
}

func (w *wronghook) DoMulti(client valkey.Client, ctx context.Context, multi ...valkey.Completed) (resps []valkey.ValkeyResult) {
	panic("implement me")
}

func (w *wronghook) DoCache(client valkey.Client, ctx context.Context, cmd valkey.Cacheable, ttl time.Duration) (resp valkey.ValkeyResult) {
	panic("implement me")
}

func (w *wronghook) DoMultiCache(client valkey.Client, ctx context.Context, multi ...valkey.CacheableTTL) (resps []valkey.ValkeyResult) {
	panic("implement me")
}

func (w *wronghook) Receive(client valkey.Client, ctx context.Context, subscribe valkey.Completed, fn func(msg valkey.PubSubMessage)) (err error) {
	panic("implement me")
}

func (w *wronghook) DoStream(client valkey.Client, ctx context.Context, cmd valkey.Completed) valkey.ValkeyResultStream {
	panic("implement me")
}

func (w *wronghook) DoMultiStream(client valkey.Client, ctx context.Context, multi ...valkey.Completed) valkey.MultiValkeyResultStream {
	panic("implement me")
}

func testHooked(t *testing.T, hooked valkey.Client, mocked *mock.Client) {
	ctx := context.Background()
	{
		mocked.EXPECT().Do(ctx, mock.Match("GET", "a")).Return(mock.Result(mock.ValkeyNil()))
		if err := hooked.Do(ctx, hooked.B().Get().Key("a").Build()).Error(); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		mocked.EXPECT().DoCache(ctx, mock.Match("GET", "b"), time.Second).Return(mock.Result(mock.ValkeyNil()))
		if err := hooked.DoCache(ctx, hooked.B().Get().Key("b").Cache(), time.Second).Error(); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		mocked.EXPECT().DoMulti(ctx, mock.Match("GET", "c")).Return([]valkey.ValkeyResult{mock.Result(mock.ValkeyNil())})
		for _, resp := range hooked.DoMulti(ctx, hooked.B().Get().Key("c").Build()) {
			if err := resp.Error(); !valkey.IsValkeyNil(err) {
				t.Fatalf("unexpected err %v", err)
			}
		}
	}
	{
		mocked.EXPECT().DoMultiCache(ctx, mock.Match("GET", "e")).Return([]valkey.ValkeyResult{mock.Result(mock.ValkeyNil())})
		for _, resp := range hooked.DoMultiCache(ctx, valkey.CT(hooked.B().Get().Key("e").Cache(), time.Second)) {
			if err := resp.Error(); !valkey.IsValkeyNil(err) {
				t.Fatalf("unexpected err %v", err)
			}
		}
	}
	{
		mocked.EXPECT().DoStream(ctx, mock.Match("GET", "e")).Return(mock.ValkeyResultStream(mock.ValkeyNil()))
		s := hooked.DoStream(ctx, hooked.B().Get().Key("e").Build())
		if _, err := s.WriteTo(io.Discard); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		mocked.EXPECT().DoMultiStream(ctx, mock.Match("GET", "e"), mock.Match("GET", "f")).Return(mock.MultiValkeyResultStream(mock.ValkeyNil()))
		s := hooked.DoMultiStream(ctx, hooked.B().Get().Key("e").Build(), hooked.B().Get().Key("f").Build())
		if _, err := s.WriteTo(io.Discard); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		mocked.EXPECT().Receive(ctx, mock.Match("SUBSCRIBE", "a"), gomock.Any()).DoAndReturn(func(ctx context.Context, cmd any, fn func(msg valkey.PubSubMessage)) error {
			fn(valkey.PubSubMessage{
				Channel: "s",
				Message: "s",
			})
			return errors.New("any")
		})
		if err := hooked.Receive(ctx, hooked.B().Subscribe().Channel("a").Build(), func(msg valkey.PubSubMessage) {
			if msg.Message != "s" && msg.Channel != "s" {
				t.Fatalf("unexpected val %v", msg)
			}
		}); err.Error() != "any" {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		mocked.EXPECT().Nodes().Return(map[string]valkey.Client{"addr": mocked})
		if nodes := hooked.Nodes(); nodes["addr"].(*hookclient).client != mocked {
			t.Fatalf("unexpected val %v", nodes)
		}
	}
	{
		ch := make(chan struct{})
		mocked.EXPECT().Close().Do(func() { close(ch) })
		hooked.Close()
		<-ch
	}
}

func testHookedDedicated(t *testing.T, hooked valkey.DedicatedClient, mocked *mock.DedicatedClient) {
	ctx := context.Background()
	{
		mocked.EXPECT().Do(ctx, mock.Match("GET", "a")).Return(mock.Result(mock.ValkeyNil()))
		if err := hooked.Do(ctx, hooked.B().Get().Key("a").Build()).Error(); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		mocked.EXPECT().DoMulti(ctx, mock.Match("GET", "c")).Return([]valkey.ValkeyResult{mock.Result(mock.ValkeyNil())})
		for _, resp := range hooked.DoMulti(ctx, hooked.B().Get().Key("c").Build()) {
			if err := resp.Error(); !valkey.IsValkeyNil(err) {
				t.Fatalf("unexpected err %v", err)
			}
		}
	}
	{
		mocked.EXPECT().Receive(ctx, mock.Match("SUBSCRIBE", "a"), gomock.Any()).DoAndReturn(func(ctx context.Context, cmd any, fn func(msg valkey.PubSubMessage)) error {
			fn(valkey.PubSubMessage{
				Channel: "s",
				Message: "s",
			})
			return errors.New("any")
		})
		if err := hooked.Receive(ctx, hooked.B().Subscribe().Channel("a").Build(), func(msg valkey.PubSubMessage) {
			if msg.Message != "s" && msg.Channel != "s" {
				t.Fatalf("unexpected val %v", msg)
			}
		}); err.Error() != "any" {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		mocked.EXPECT().SetPubSubHooks(valkey.PubSubHooks{})
		hooked.SetPubSubHooks(valkey.PubSubHooks{})
	}
	{
		ch := make(chan struct{})
		mocked.EXPECT().Close().Do(func() { close(ch) })
		hooked.Close()
		<-ch
	}
}

func TestWithHook(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mocked := mock.NewClient(ctrl)
	hooked := WithHook(mocked, &hook{})

	testHooked(t, hooked, mocked)
	{
		dc := mock.NewDedicatedClient(ctrl)
		mocked.EXPECT().Dedicate().Return(dc, func() {})
		c, _ := hooked.Dedicate()
		testHookedDedicated(t, c, dc)
	}
	{
		dc := mock.NewDedicatedClient(ctrl)
		cb := func(c valkey.DedicatedClient) error {
			testHookedDedicated(t, c, dc)
			return errors.New("any")
		}
		mocked.EXPECT().Dedicated(gomock.Any()).DoAndReturn(func(fn func(c valkey.DedicatedClient) error) error {
			return fn(dc)
		})
		if err := hooked.Dedicated(cb); err.Error() != "any" {
			t.Fatalf("unexpected err %v", err)
		}
	}
}

func TestForbiddenMethodForDedicatedClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mocked := mock.NewClient(ctrl)

	shouldpanic := func(fn func(client valkey.Client), msg string) {
		defer func() {
			if err := recover().(string); err != msg {
				t.Fatalf("unexpected err %v", err)
			}
		}()

		hooked := WithHook(mocked, &wronghook{DoFn: fn})
		mocked.EXPECT().Dedicated(gomock.Any()).DoAndReturn(func(fn func(c valkey.DedicatedClient) error) error {
			return fn(mock.NewDedicatedClient(ctrl))
		})
		hooked.Dedicated(func(client valkey.DedicatedClient) error {
			return client.Do(context.Background(), client.B().Get().Key("").Build()).Error()
		})
	}
	for _, c := range []struct {
		fn  func(client valkey.Client)
		msg string
	}{
		{
			fn: func(client valkey.Client) {
				client.DoCache(context.Background(), client.B().Get().Key("").Cache(), time.Second)
			},
			msg: "DoCache() is not allowed with valkey.DedicatedClient",
		}, {
			fn: func(client valkey.Client) {
				client.DoMultiCache(context.Background())
			},
			msg: "DoMultiCache() is not allowed with valkey.DedicatedClient",
		}, {
			fn: func(client valkey.Client) {
				client.Dedicated(func(client valkey.DedicatedClient) error { return nil })
			},
			msg: "Dedicated() is not allowed with valkey.DedicatedClient",
		}, {
			fn: func(client valkey.Client) {
				client.Dedicate()
			},
			msg: "Dedicate() is not allowed with valkey.DedicatedClient",
		}, {
			fn: func(client valkey.Client) {
				client.Nodes()
			},
			msg: "Nodes() is not allowed with valkey.DedicatedClient",
		}, {
			fn: func(client valkey.Client) {
				client.DoStream(context.Background(), client.B().Get().Key("").Build())
			},
			msg: "DoStream() is not allowed with valkey.DedicatedClient",
		}, {
			fn: func(client valkey.Client) {
				client.DoMultiStream(context.Background(), client.B().Get().Key("").Build())
			},
			msg: "DoMultiStream() is not allowed with valkey.DedicatedClient",
		},
	} {
		shouldpanic(c.fn, c.msg)
	}
}

func TestNewErrorResult(t *testing.T) {
	e := errors.New("err")
	r := NewErrorResult(e)
	if r.Error() != e {
		t.Fatal("unexpected err")
	}
}

func TestNewErrorResultStream(t *testing.T) {
	e := errors.New("err")
	r := NewErrorResultStream(e)
	if r.Error() != e {
		t.Fatal("unexpected err")
	}
	if r.HasNext() {
		t.Fatal("unexpected err")
	}
	if n, err := r.WriteTo(io.Discard); err != e || n != 0 {
		t.Fatal("unexpected err or n")
	}
}
