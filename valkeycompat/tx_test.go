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
	"testing"
	"github.com/valkey-io/valkey-go/mock"
	"go.uber.org/mock/gomock"
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/valkey-io/valkey-go"
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
		k1 := "{k}1"
		k2 := "{k}2"
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
		k1 := "{k}1"
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

	It("should Unwatch and Close", func() {
		k1 := "{k}1"
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
	It("should fast-fail on cross-slot", func() {
		pipe := adapter.TxPipeline()
		pipe.Set(ctx, "k1", "v1", 0)
		pipe.Set(ctx, "k2", "v2", 0)
		rets, err := pipe.Exec(ctx)
		Expect(err).To(MatchError(ErrCrossSlot))
		Expect(rets).To(BeNil())
	})

	It("should fast-fail on cross-slot MGET", func() {
		pipe := adapter.TxPipeline()
		pipe.MGet(ctx, "k1", "k2")
		rets, err := pipe.Exec(ctx)
		Expect(err).To(MatchError(ErrCrossSlot))
		Expect(rets).To(BeNil())
	})
}
func TestCrossSlotValidation(t *testing.T) {
	ctrl := gomock.NewController(t)
	mc := mock.NewClient(ctrl)
	adapter := NewAdapter(mc)
	
	ctx := context.Background()

	t.Run("same slot", func(t *testing.T) {
		pipe := adapter.TxPipeline()
		pipe.Set(ctx, "{user1}k1", "v1", 0)
		pipe.Get(ctx, "{user1}k2")
		
		resps := []valkey.ValkeyResult{
			valkey.NewErrorResult(errors.New("mock error")),
		}
		
		mc.EXPECT().DoMulti(ctx, gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).Return(resps)
		
		_, err := pipe.Exec(ctx)
		if err == ErrCrossSlot {
			t.Fatalf("expected no cross-slot error, got %v", err)
		}
	})

	t.Run("cross slot keys", func(t *testing.T) {
		pipe := adapter.TxPipeline()
		pipe.Set(ctx, "k1", "v1", 0)
		pipe.Set(ctx, "k2", "v2", 0)
		_, err := pipe.Exec(ctx)
		if err != ErrCrossSlot {
			t.Fatalf("expected ErrCrossSlot, got %v", err)
		}
	})

	t.Run("cross slot mget", func(t *testing.T) {
		pipe := adapter.TxPipeline()
		pipe.MGet(ctx, "k1", "k2")
		_, err := pipe.Exec(ctx)
		if err != ErrCrossSlot {
			t.Fatalf("expected ErrCrossSlot, got %v", err)
		}
	})
	
	t.Run("cross slot mset", func(t *testing.T) {
		pipe := adapter.TxPipeline()
		pipe.MSet(ctx, "k1", "v1", "k2", "v2")
		_, err := pipe.Exec(ctx)
		if err != ErrCrossSlot {
			t.Fatalf("expected ErrCrossSlot, got %v", err)
		}
	})
}
