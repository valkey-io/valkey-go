package valkey

import (
	"context"
	"errors"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

func TestNewStandaloneClientNoNode(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	if _, err := newStandaloneClient(
		&ClientOption{}, func(dst string, opt *ClientOption) conn {
			return nil
		}, newRetryer(defaultRetryDelayFn),
	); err != ErrNoAddr {
		t.Fatalf("unexpected err %v", err)
	}
}

func TestNewStandaloneClientError(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	v := errors.New("dial err")
	if _, err := newStandaloneClient(
		&ClientOption{InitAddress: []string{""}}, func(dst string, opt *ClientOption) conn { return &mockConn{DialFn: func() error { return v }} }, newRetryer(defaultRetryDelayFn),
	); err != v {
		t.Fatalf("unexpected err %v", err)
	}
}

func TestNewStandaloneClientReplicasError(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	v := errors.New("dial err")
	if _, err := newStandaloneClient(
		&ClientOption{
			InitAddress: []string{"1"},
			Standalone: StandaloneOption{
				ReplicaAddress: []string{"2", "3"}, // two replicas
			},
		}, func(dst string, opt *ClientOption) conn {
			return &mockConn{DialFn: func() error {
				if dst == "3" {
					return v
				}
				return nil
			}}
		}, newRetryer(defaultRetryDelayFn),
	); err != v {
		t.Fatalf("unexpected err %v", err)
	}
}

func TestNewStandaloneClientDelegation(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	w := &mockWire{}
	p := &mockConn{
		AddrFn: func() string {
			return "p"
		},
		DoFn: func(cmd Completed) ValkeyResult {
			return newErrResult(errors.New("primary"))
		},
		DoMultiFn: func(multi ...Completed) *valkeyresults {
			return &valkeyresults{s: []ValkeyResult{newErrResult(errors.New("primary"))}}
		},
		DoCacheFn: func(cmd Cacheable, ttl time.Duration) ValkeyResult {
			return newErrResult(errors.New("primary"))
		},
		DoMultiCacheFn: func(multi ...CacheableTTL) *valkeyresults {
			return &valkeyresults{s: []ValkeyResult{newErrResult(errors.New("primary"))}}
		},
		DoStreamFn: func(cmd Completed) ValkeyResultStream {
			return ValkeyResultStream{e: errors.New("primary")}
		},
		DoMultiStreamFn: func(cmd ...Completed) MultiValkeyResultStream {
			return MultiValkeyResultStream{e: errors.New("primary")}
		},
		ReceiveFn: func(ctx context.Context, subscribe Completed, fn func(message PubSubMessage)) error {
			return errors.New("primary")
		},
		AcquireFn: func() wire {
			return w
		},
	}
	r := &mockConn{
		AddrFn: func() string {
			return "r"
		},
		DoFn: func(cmd Completed) ValkeyResult {
			return newErrResult(errors.New("replica"))
		},
		DoMultiFn: func(multi ...Completed) *valkeyresults {
			return &valkeyresults{s: []ValkeyResult{newErrResult(errors.New("replica"))}}
		},
		DoStreamFn: func(cmd Completed) ValkeyResultStream {
			return ValkeyResultStream{e: errors.New("replica")}
		},
		DoMultiStreamFn: func(cmd ...Completed) MultiValkeyResultStream {
			return MultiValkeyResultStream{e: errors.New("replica")}
		},
		ReceiveFn: func(ctx context.Context, subscribe Completed, fn func(message PubSubMessage)) error {
			return errors.New("replica")
		},
	}

	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"p"},
		Standalone: StandaloneOption{
			ReplicaAddress: []string{"r"},
		},
		SendToReplicas: func(cmd Completed) bool {
			return cmd.IsReadOnly() && !cmd.IsUnsub()
		},
		DisableRetry: true,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "p" {
			return p
		}
		return r
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	defer c.Close()

	ctx := context.Background()
	if err := c.Do(ctx, c.B().Get().Key("k").Build()).Error(); err == nil || err.Error() != "replica" {
		t.Fatalf("unexpected err %v", err)
	}
	if err := c.Do(ctx, c.B().Set().Key("k").Value("v").Build()).Error(); err == nil || err.Error() != "primary" {
		t.Fatalf("unexpected err %v", err)
	}
	if err := c.DoCache(ctx, c.B().Get().Key("k").Cache(), time.Second).Error(); err == nil || err.Error() != "primary" {
		t.Fatalf("unexpected err %v", err)
	}
	if err := c.DoMulti(ctx, c.B().Get().Key("k").Build())[0].Error(); err == nil || err.Error() != "replica" {
		t.Fatalf("unexpected err %v", err)
	}
	if err := c.DoMulti(ctx, c.B().Set().Key("k").Value("v").Build())[0].Error(); err == nil || err.Error() != "primary" {
		t.Fatalf("unexpected err %v", err)
	}
	if err := c.DoMultiCache(ctx, CT(c.B().Get().Key("k").Cache(), time.Second))[0].Error(); err == nil || err.Error() != "primary" {
		t.Fatalf("unexpected err %v", err)
	}
	stream := c.DoStream(ctx, c.B().Get().Key("k").Build())
	if err := stream.Error(); err == nil || err.Error() != "replica" {
		t.Fatalf("unexpected err %v", err)
	}
	multiStream := c.DoMultiStream(ctx, c.B().Get().Key("k").Build())
	if err := multiStream.Error(); err == nil || err.Error() != "replica" {
		t.Fatalf("unexpected err %v", err)
	}
	stream = c.DoStream(ctx, c.B().Set().Key("k").Value("v").Build())
	if err := stream.Error(); err == nil || err.Error() != "primary" {
		t.Fatalf("unexpected err %v", err)
	}
	multiStream = c.DoMultiStream(ctx, c.B().Set().Key("k").Value("v").Build())
	if err := multiStream.Error(); err == nil || err.Error() != "primary" {
		t.Fatalf("unexpected err %v", err)
	}
	if err := c.Receive(ctx, c.B().Subscribe().Channel("ch").Build(), func(msg PubSubMessage) {}); err == nil || err.Error() != "replica" {
		t.Fatalf("unexpected err %v", err)
	}
	if err := c.Receive(ctx, c.B().Unsubscribe().Channel("ch").Build(), func(msg PubSubMessage) {}); err == nil || err.Error() != "primary" {
		t.Fatalf("unexpected err %v", err)
	}

	if err := c.Dedicated(func(dc DedicatedClient) error {
		if dc.(*dedicatedSingleClient).wire != w {
			return errors.New("wire")
		}
		return nil
	}); err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	if dc, cancel := c.Dedicate(); dc.(*dedicatedSingleClient).wire != w {
		t.Fatalf("unexpected wire %v", dc.(*dedicatedSingleClient).wire)
	} else {
		cancel()
	}

	if c.Mode() != ClientModeStandalone {
		t.Fatalf("unexpected mode: %v", c.Mode())
	}

	nodes := c.Nodes()
	if len(nodes) != 2 && nodes["p"].(*singleClient).conn != p && nodes["r"].(*singleClient).conn != r {
		t.Fatalf("unexpected nodes %v", nodes)
	}
}

func TestNewStandaloneClientMultiReplicasDelegation(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	var counts [2]int32

	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"p"},
		Standalone: StandaloneOption{
			ReplicaAddress: []string{"0", "1"},
		},
		SendToReplicas: func(cmd Completed) bool {
			return cmd.IsReadOnly()
		},
		DisableRetry: true,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "p" {
			return &mockConn{}
		}
		return &mockConn{
			DoFn: func(cmd Completed) ValkeyResult {
				i, _ := strconv.Atoi(dst)
				atomic.AddInt32(&counts[i], 1)
				return newErrResult(errors.New("replica"))
			},
		}
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}

	defer c.Close()

	ctx := context.Background()

	for i := 0; i < 1000; i++ {
		if err := c.Do(ctx, c.B().Get().Key("k").Build()).Error(); err == nil || err.Error() != "replica" {
			t.Fatalf("unexpected err %v", err)
		}
	}
	for i := 0; i < len(counts); i++ {
		if atomic.LoadInt32(&counts[i]) == 0 {
			t.Fatalf("replica %d was not called", i)
		}
	}
}

func TestStandaloneRedirectHandling(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	
	// Create a mock redirect response
	redirectErr := ValkeyError(strmsg('-', "REDIRECT 127.0.0.1:6380"))
	
	// Mock primary connection that returns redirect
	primaryConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			return newErrResult(&redirectErr)
		},
	}
	
	// Mock redirect target connection that returns success
	redirectConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			return ValkeyResult{val: strmsg('+', "OK")}
		},
	}
	
	// Track which connection is being used
	var connUsed string
	
	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
		DisableRetry: true,
	}, func(dst string, opt *ClientOption) conn {
		connUsed = dst
		if dst == "primary" {
			return primaryConn
		}
		return redirectConn
	}, newRetryer(defaultRetryDelayFn))
	
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()
	
	ctx := context.Background()
	result := s.Do(ctx, s.B().Get().Key("test").Build())
	
	if result.Error() != nil {
		t.Errorf("expected no error after redirect, got: %v", result.Error())
	}
	
	if str, _ := result.ToString(); str != "OK" {
		t.Errorf("expected OK response after redirect, got: %s", str)
	}
	
	// Verify that the redirect target was used
	if connUsed != "127.0.0.1:6380" {
		t.Errorf("expected redirect to use 127.0.0.1:6380, got: %s", connUsed)
	}
}

func TestStandaloneRedirectDisabled(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	
	// Create a mock redirect response
	redirectErr := ValkeyError(strmsg('-', "REDIRECT 127.0.0.1:6380"))
	
	// Mock primary connection that returns redirect
	primaryConn := &mockConn{
		DoFn: func(cmd Completed) ValkeyResult {
			return newErrResult(&redirectErr)
		},
	}
	
	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: false, // Redirect disabled
		},
		DisableRetry: true,
	}, func(dst string, opt *ClientOption) conn {
		return primaryConn
	}, newRetryer(defaultRetryDelayFn))
	
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()
	
	ctx := context.Background()
	result := s.Do(ctx, s.B().Get().Key("test").Build())
	
	// Should return the original redirect error since redirect is disabled
	if result.Error() == nil {
		t.Error("expected redirect error to be returned when redirect is disabled")
	}
	
	if result.Error().Error() != "REDIRECT 127.0.0.1:6380" {
		t.Errorf("expected redirect error, got: %v", result.Error())
	}
}
