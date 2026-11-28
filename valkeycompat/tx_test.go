// Copyright (c) 2013 The github.com/go-redis/redis Authors.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package valkeycompat

import (
	"context"
	"errors"
	"testing"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
)

var _ = Describe("RESP3 TxPipeline Commands", func() {
	testAdapterTxPipeline(true)
})

var _ = Describe("RESP2 TxPipeline Commands", func() {
	testAdapterTxPipeline(false)
})

func testAdapterTxPipeline(resp3 bool) {
	var adapter Cmdable

	BeforeEach(func() {
		if resp3 {
			adapter = adapterresp3
		} else {
			adapter = adapterresp2
		}
		Expect(adapter.FlushDB(ctx).Err()).NotTo(HaveOccurred())
		Expect(adapter.FlushAll(ctx).Err()).NotTo(HaveOccurred())
	})

	It("should TxPipelined", func() {
		var echo, ping *StringCmd
		rets, err := adapter.TxPipelined(ctx, func(pipe Pipeliner) error {
			echo = pipe.Echo(ctx, "hello")
			ping = pipe.Ping(ctx)
			Expect(echo.Err()).To(MatchError(errPipelineNotExecuted))
			Expect(ping.Err()).To(MatchError(errPipelineNotExecuted))
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(rets).To(HaveLen(2))
		Expect(rets[0]).To(Equal(echo))
		Expect(rets[1]).To(Equal(ping))
		Expect(echo.Err()).NotTo(HaveOccurred())
		Expect(echo.Val()).To(Equal("hello"))
		Expect(ping.Err()).NotTo(HaveOccurred())
		Expect(ping.Val()).To(Equal("PONG"))
	})

	It("should TxPipeline", func() {
		pipe := adapter.TxPipeline()
		echo := pipe.Echo(ctx, "hello")
		ping := pipe.Ping(ctx)
		Expect(echo.Err()).To(MatchError(errPipelineNotExecuted))
		Expect(ping.Err()).To(MatchError(errPipelineNotExecuted))
		Expect(pipe.Len()).To(Equal(2))

		rets, err := pipe.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(pipe.Len()).To(Equal(0))
		Expect(rets).To(HaveLen(2))
		Expect(rets[0]).To(Equal(echo))
		Expect(rets[1]).To(Equal(ping))
		Expect(echo.Err()).NotTo(HaveOccurred())
		Expect(echo.Val()).To(Equal("hello"))
		Expect(ping.Err()).NotTo(HaveOccurred())
		Expect(ping.Val()).To(Equal("PONG"))
	})

	It("should Discard", func() {
		pipe := adapter.TxPipeline()
		echo := pipe.Echo(ctx, "hello")
		ping := pipe.Ping(ctx)

		pipe.Discard()
		Expect(pipe.Len()).To(Equal(0))

		rets, err := pipe.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(rets).To(HaveLen(0))

		Expect(echo.Err()).To(MatchError(errPipelineNotExecuted))
		Expect(ping.Err()).To(MatchError(errPipelineNotExecuted))
	})

	It("should Watch", func() {
		k1 := "_k1_"
		k2 := "_k2_"
		err := adapter.Watch(ctx, func(t Tx) error {
			if t.Get(ctx, k1).Err() != Nil {
				return errors.New("unclean")
			}
			if t.Get(ctx, k2).Err() != Nil {
				return errors.New("unclean")
			}
			_, err := t.TxPipelined(ctx, func(pipe Pipeliner) error {
				pipe.Set(ctx, k1, k1, 0)
				pipe.Set(ctx, k2, k2, 0)
				return nil
			})
			return err
		}, k1, k2)
		Expect(err).NotTo(HaveOccurred())
		Expect(adapter.Get(ctx, k1).Val()).To(Equal(k1))
		Expect(adapter.Get(ctx, k2).Val()).To(Equal(k2))
	})

	It("should Watch Abort", func() {
		k1 := "_k1_"
		ch := make(chan error)
		go func() {
			ch <- adapter.Watch(ctx, func(t Tx) error {
				ch <- nil
				<-ch
				_, err := t.TxPipelined(ctx, func(pipe Pipeliner) error {
					pipe.Del(ctx, k1)
					return nil
				})
				return err
			}, k1)
		}()
		<-ch
		Expect(adapter.Set(ctx, k1, k1, 0).Err()).NotTo(HaveOccurred())
		ch <- nil
		Expect(<-ch).To(MatchError(TxFailedErr))
	})

	It("should not leave keys watched when the callback skips EXEC", func() {
		addr := "127.0.0.1:6378"
		if !resp3 {
			addr = "127.0.0.1:6356"
		}
		// A pool of one makes the second Watch reuse the first one's connection.
		client, err := valkey.NewClient(valkey.ClientOption{
			InitAddress:      []string{addr},
			DisableCache:     true,
			BlockingPoolSize: 1,
		})
		Expect(err).NotTo(HaveOccurred())
		defer client.Close()
		a := NewAdapter(client)
		k1 := "_k1_"
		k2 := "_k2_"
		Expect(a.Watch(ctx, func(t Tx) error { return nil }, k1)).NotTo(HaveOccurred())
		Expect(a.Set(ctx, k1, k1, 0).Err()).NotTo(HaveOccurred())
		err = a.Watch(ctx, func(t Tx) error {
			_, err := t.TxPipelined(ctx, func(pipe Pipeliner) error {
				pipe.Set(ctx, k2, k2, 0)
				return nil
			})
			return err
		}, k2)
		Expect(err).NotTo(HaveOccurred())
		Expect(a.Get(ctx, k2).Val()).To(Equal(k2))
	})

	It("should Unwatch and Close", func() {
		k1 := "_k1_"
		err := adapter.Watch(ctx, func(t Tx) error {
			Expect(t.Unwatch(ctx).Err()).NotTo(HaveOccurred())
			Expect(t.Close(ctx)).NotTo(HaveOccurred())
			_, err := t.TxPipelined(ctx, func(pipe Pipeliner) error {
				pipe.Del(ctx, k1)
				return nil
			})
			return err
		}, k1)
		Expect(err).To(MatchError(valkey.ErrDedicatedClientRecycled))
	})

	if resp3 {
		It("should catch first cmder error", func() {
			k1 := "_k1_not_exists"
			var cmder *JSONCmd
			_, err := adapter.TxPipelined(ctx, func(pipe Pipeliner) error {
				pipe.Del(ctx, k1)
				cmder = pipe.JSONGet(ctx, k1)
				return nil
			})
			Expect(err).To(Equal(cmder.Err()))
		})
	}

	It("should panic for GetToBuffer in TxPipeline", func() {
		pipe := adapter.TxPipeline()

		Expect(func() {
			pipe.GetToBuffer(ctx, "key", make([]byte, 10))
		}).To(Panic())
	})
}

func TestWatchUnwatchesAfterCallback(t *testing.T) {
	for _, tc := range []struct {
		name string
		fn   func(ctx context.Context, cancel context.CancelFunc, t Tx) error
		err  error
	}{
		{
			name: "callback returns nil",
			fn:   func(context.Context, context.CancelFunc, Tx) error { return nil },
		},
		{
			name: "callback returns an error",
			fn:   func(context.Context, context.CancelFunc, Tx) error { return errors.New("boom") },
			err:  errors.New("boom"),
		},
		{
			name: "callback cancels the context",
			fn: func(_ context.Context, cancel context.CancelFunc, _ Tx) error {
				cancel()
				return nil
			},
		},
		{
			name: "callback closes the Tx",
			fn: func(ctx context.Context, _ context.CancelFunc, tx Tx) error {
				return tx.Close(ctx)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := mock.NewClient(ctrl)
			dc := mock.NewDedicatedClient(ctrl)
			released := false
			client.EXPECT().Dedicate().Return(dc, func() { released = true })
			gomock.InOrder(
				dc.EXPECT().Do(gomock.Any(), mock.Match("WATCH", "k")).Return(mock.Result(mock.ValkeyString("OK"))),
				dc.EXPECT().Do(gomock.Any(), mock.Match("UNWATCH")).DoAndReturn(func(ctx context.Context, _ valkey.Completed) valkey.ValkeyResult {
					if released {
						t.Error("UNWATCH sent after the dedicated client was released")
					}
					if err := ctx.Err(); err != nil {
						t.Errorf("UNWATCH sent with a done context: %v", err)
					}
					if _, ok := ctx.Deadline(); !ok {
						t.Error("UNWATCH sent without a deadline")
					}
					return mock.Result(mock.ValkeyString("OK"))
				}),
			)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			err := NewAdapter(client).Watch(ctx, func(tx Tx) error { return tc.fn(ctx, cancel, tx) }, "k")
			if (err == nil) != (tc.err == nil) || (err != nil && err.Error() != tc.err.Error()) {
				t.Fatalf("Watch() = %v, want %v", err, tc.err)
			}
			if !released {
				t.Error("dedicated client was not released")
			}
		})
	}
}

func TestWatchFailureSkipsUnwatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	client := mock.NewClient(ctrl)
	dc := mock.NewDedicatedClient(ctrl)
	client.EXPECT().Dedicate().Return(dc, func() {})
	dc.EXPECT().Do(gomock.Any(), mock.Match("WATCH", "k")).Return(mock.ErrorResult(errors.New("conn")))
	called := false
	err := NewAdapter(client).Watch(context.Background(), func(Tx) error { called = true; return nil }, "k")
	if err == nil || err.Error() != "conn" {
		t.Fatalf("Watch() = %v, want conn", err)
	}
	if called {
		t.Fatal("callback ran after WATCH failed")
	}
}

func TestWatchClosesConnectionWhenUnwatchFails(t *testing.T) {
	for _, tc := range []struct {
		name string
		res  valkey.ValkeyResult
	}{
		{name: "server error", res: mock.Result(mock.ValkeyError("NOPERM"))},
		{name: "connection error", res: mock.ErrorResult(errors.New("conn"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			client := mock.NewClient(ctrl)
			dc := mock.NewDedicatedClient(ctrl)
			released := false
			client.EXPECT().Dedicate().Return(dc, func() { released = true })
			gomock.InOrder(
				dc.EXPECT().Do(gomock.Any(), mock.Match("WATCH", "k")).Return(mock.Result(mock.ValkeyString("OK"))),
				dc.EXPECT().Do(gomock.Any(), mock.Match("UNWATCH")).Return(tc.res),
				dc.EXPECT().Close().Do(func() {
					if released {
						t.Error("dedicated client was released before it was closed")
					}
				}),
			)
			err := NewAdapter(client).Watch(context.Background(), func(Tx) error { return errors.New("boom") }, "k")
			if err == nil || err.Error() != "boom" {
				t.Fatalf("Watch() = %v, want boom", err)
			}
			if !released {
				t.Error("dedicated client was not released")
			}
		})
	}
}
