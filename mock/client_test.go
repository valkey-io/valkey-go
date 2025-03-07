package mock

import (
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"go.uber.org/mock/gomock"
)

func TestNewClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	client := NewClient(ctrl)
	{
		client.EXPECT().Mode().Return(valkey.ClientModeStandalone)
		if mode := client.Mode(); mode != valkey.ClientModeStandalone {
			t.Fatalf("unexpected val %v", mode)
		}
	}
	{
		client.EXPECT().Do(ctx, Match("GET", "a")).Return(Result(ValkeyNil()))
		if err := client.Do(ctx, client.B().Get().Key("a").Build()).Error(); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		client.EXPECT().DoCache(ctx, Match("GET", "b"), time.Second).Return(Result(ValkeyNil()))
		if err := client.DoCache(ctx, client.B().Get().Key("b").Cache(), time.Second).Error(); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		client.EXPECT().DoMulti(ctx,
			Match("GET", "c"),
			Match("GET", "d")).Return([]valkey.ValkeyResult{
			Result(ValkeyNil()),
			Result(ValkeyNil())})
		for _, resp := range client.DoMulti(ctx,
			client.B().Get().Key("c").Build(),
			client.B().Get().Key("d").Build()) {
			if err := resp.Error(); !valkey.IsValkeyNil(err) {
				t.Fatalf("unexpected err %v", err)
			}
		}
	}
	{
		client.EXPECT().DoMultiCache(ctx,
			Match("GET", "e"),
			Match("GET", "f")).Return([]valkey.ValkeyResult{
			Result(ValkeyNil()),
			Result(ValkeyNil())})
		for _, resp := range client.DoMultiCache(ctx,
			valkey.CT(client.B().Get().Key("e").Cache(), time.Second),
			valkey.CT(client.B().Get().Key("f").Cache(), time.Second)) {
			if err := resp.Error(); !valkey.IsValkeyNil(err) {
				t.Fatalf("unexpected err %v", err)
			}
		}
	}
	{
		client.EXPECT().DoStream(ctx, Match("GET", "e")).Return(ValkeyResultStream(ValkeyNil()))
		s := client.DoStream(ctx, client.B().Get().Key("e").Build())
		if _, err := s.WriteTo(io.Discard); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		client.EXPECT().DoMultiStream(ctx, Match("GET", "e"), Match("GET", "f")).Return(MultiValkeyResultStream(ValkeyNil()))
		s := client.DoMultiStream(ctx, client.B().Get().Key("e").Build(), client.B().Get().Key("f").Build())
		if _, err := s.WriteTo(io.Discard); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		client.EXPECT().Nodes().Return(map[string]valkey.Client{"addr": client})
		if nodes := client.Nodes(); nodes["addr"] != client {
			t.Fatalf("unexpected val %v", nodes)
		}
	}
	{
		ch := make(chan struct{})
		client.EXPECT().Close().Do(func() { close(ch) })
		client.Close()
		<-ch
	}
	{
		client.EXPECT().Receive(ctx, Match("SUBSCRIBE", "a"), gomock.Any()).DoAndReturn(func(ctx context.Context, cmd any, fn func(msg valkey.PubSubMessage)) error {
			fn(valkey.PubSubMessage{
				Channel: "s",
				Message: "s",
			})
			return errors.New("any")
		})
		if err := client.Receive(ctx, client.B().Subscribe().Channel("a").Build(), func(msg valkey.PubSubMessage) {
			if msg.Message != "s" && msg.Channel != "s" {
				t.Fatalf("unexpected val %v", msg)
			}
		}); err.Error() != "any" {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		dc := NewDedicatedClient(ctrl)
		client.EXPECT().Dedicate().Return(dc, func() {})
		if c, _ := client.Dedicate(); c != dc {
			t.Fatalf("unexpected val %v", c)
		}
	}
	{
		dc := NewDedicatedClient(ctrl)
		cb := func(c valkey.DedicatedClient) error {
			if c != dc {
				t.Fatalf("unexpected val %v", c)
			}
			return errors.New("any")
		}
		client.EXPECT().Dedicated(gomock.Any()).DoAndReturn(func(fn func(c valkey.DedicatedClient) error) error {
			return fn(dc)
		})
		if err := client.Dedicated(cb); err.Error() != "any" {
			t.Fatalf("unexpected err %v", err)
		}
	}
}

func TestNewDedicatedClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	client := NewDedicatedClient(ctrl)
	{
		client.EXPECT().Do(ctx, Match("GET", "a")).Return(Result(ValkeyNil()))
		if err := client.Do(ctx, client.B().Get().Key("a").Build()).Error(); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		client.EXPECT().DoMulti(ctx,
			Match("GET", "c"),
			Match("GET", "d")).Return([]valkey.ValkeyResult{
			Result(ValkeyNil()),
			Result(ValkeyNil())})
		for _, resp := range client.DoMulti(ctx,
			client.B().Get().Key("c").Build(),
			client.B().Get().Key("d").Build()) {
			if err := resp.Error(); !valkey.IsValkeyNil(err) {
				t.Fatalf("unexpected err %v", err)
			}
		}
	}
	{
		ch := make(chan struct{})
		client.EXPECT().Close().Do(func() { close(ch) })
		client.Close()
		<-ch
	}
	{
		client.EXPECT().Receive(ctx, Match("SUBSCRIBE", "a"), gomock.Any()).DoAndReturn(func(ctx context.Context, cmd any, fn func(msg valkey.PubSubMessage)) error {
			fn(valkey.PubSubMessage{
				Channel: "s",
				Message: "s",
			})
			return errors.New("any")
		})
		if err := client.Receive(ctx, client.B().Subscribe().Channel("a").Build(), func(msg valkey.PubSubMessage) {
			if msg.Message != "s" && msg.Channel != "s" {
				t.Fatalf("unexpected val %v", msg)
			}
		}); err.Error() != "any" {
			t.Fatalf("unexpected err %v", err)
		}
	}
	{
		client.EXPECT().SetPubSubHooks(gomock.Any()).Do(func(hooks valkey.PubSubHooks) {
			if hooks.OnMessage == nil || hooks.OnSubscription == nil {
				t.Fatalf("unexpected val %v", hooks)
			}
		})
		client.SetPubSubHooks(valkey.PubSubHooks{
			OnMessage:      func(m valkey.PubSubMessage) {},
			OnSubscription: func(s valkey.PubSubSubscription) {},
		})
	}
}

func TestNewClientAcrossSlot(t *testing.T) {
	t.Run("Default", func(t *testing.T) {
		cc := NewClient(nil)
		cc.B().Del().Key("{1}", "{2}").Build() // should not panic
		dc := NewDedicatedClient(nil)
		dc.B().Del().Key("{1}", "{2}").Build() // should not panic
	})
	t.Run("Enable Slot Check", func(t *testing.T) {
		defer func() {
			if msg := recover().(string); msg != "multi key command with different key slots are not allowed" {
				t.Fail()
			}
		}()
		cc := NewClient(nil, WithSlotCheck())
		cc.B().Del().Key("{1}", "{2}").Build() // should panic
	})
	t.Run("Enable Slot Check", func(t *testing.T) {
		defer func() {
			if msg := recover().(string); msg != "multi key command with different key slots are not allowed" {
				t.Fail()
			}
		}()
		cc := NewDedicatedClient(nil, WithSlotCheck())
		cc.B().Del().Key("{1}", "{2}").Build() // should panic
	})
}
