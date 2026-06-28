package valkey

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go/internal/cmds"
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

	for range 1000 {
		if err := c.Do(ctx, c.B().Get().Key("k").Build()).Error(); err == nil || err.Error() != "replica" {
			t.Fatalf("unexpected err %v", err)
		}
	}
	for i := range len(counts) {
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

func TestStandaloneDoCacheRedirectHandling(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// Create a mock redirect response
	redirectErr := ValkeyError(strmsg('-', "REDIRECT 127.0.0.1:6380"))

	// Mock primary connection that returns redirect
	primaryConn := &mockConn{
		DoCacheFn: func(cmd Cacheable, ttl time.Duration) ValkeyResult {
			return newErrResult(&redirectErr)
		},
	}

	// Mock redirect target connection that returns success
	redirectConn := &mockConn{
		DoCacheFn: func(cmd Cacheable, ttl time.Duration) ValkeyResult {
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
	result := s.DoCache(ctx, s.B().Get().Key("test").Cache(), time.Second)

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

func TestStandaloneDoCacheRedirectDisabled(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// Create a mock redirect response
	redirectErr := ValkeyError(strmsg('-', "REDIRECT 127.0.0.1:6380"))

	// Mock primary connection that returns redirect
	primaryConn := &mockConn{
		DoCacheFn: func(cmd Cacheable, ttl time.Duration) ValkeyResult {
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
	result := s.DoCache(ctx, s.B().Get().Key("test").Cache(), time.Second)

	// Should return the original redirect error since redirect is disabled
	if result.Error() == nil {
		t.Error("expected redirect error to be returned when redirect is disabled")
	}

	if result.Error().Error() != "REDIRECT 127.0.0.1:6380" {
		t.Errorf("expected redirect error, got: %v", result.Error())
	}
}

func TestNewClientEnableRedirectPriority(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	// Test that EnableRedirect creates a standalone client
	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
	}, func(dst string, opt *ClientOption) conn {
		return &mockConn{
			DialFn: func() error { return nil },
		}
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Verify that we got a standalone client with redirect enabled
	if s.Mode() != ClientModeStandalone {
		t.Errorf("expected standalone client, got: %v", s.Mode())
	}

	// Verify that EnableRedirect is properly configured
	if !s.enableRedirect {
		t.Error("expected EnableRedirect to be true")
	}
}

func TestStandaloneDoStreamToReplica(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	replicaUsed := false
	primaryConn := &mockConn{
		DialFn: func() error { return nil },
		DoStreamFn: func(cmd Completed) ValkeyResultStream {
			return ValkeyResultStream{e: errors.New("primary")}
		},
	}

	replicaConn := &mockConn{
		DialFn: func() error { return nil },
		DoStreamFn: func(cmd Completed) ValkeyResultStream {
			replicaUsed = true
			return ValkeyResultStream{e: errors.New("replica")}
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress: []string{"replica"},
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
		DisableRetry:   true,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return replicaConn
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Test DoStream to replica
	stream := s.DoStream(context.Background(), s.B().Get().Key("k").Build())
	if stream.Error() == nil || stream.Error().Error() != "replica" {
		t.Errorf("expected replica error, got %v", stream.Error())
	}

	if !replicaUsed {
		t.Error("expected replica to be used")
	}
}

func TestStandaloneDoMultiStreamToReplica(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	replicaUsed := false
	primaryConn := &mockConn{
		DialFn: func() error { return nil },
		DoMultiStreamFn: func(multi ...Completed) MultiValkeyResultStream {
			return MultiValkeyResultStream{e: errors.New("primary")}
		},
	}

	replicaConn := &mockConn{
		DialFn: func() error { return nil },
		DoMultiStreamFn: func(multi ...Completed) MultiValkeyResultStream {
			replicaUsed = true
			return MultiValkeyResultStream{e: errors.New("replica")}
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress: []string{"replica"},
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
		DisableRetry:   true,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return replicaConn
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Test DoMultiStream to replica
	stream := s.DoMultiStream(context.Background(), s.B().Get().Key("k").Build())
	if stream.Error() == nil || stream.Error().Error() != "replica" {
		t.Errorf("expected replica error, got %v", stream.Error())
	}

	if !replicaUsed {
		t.Error("expected replica to be used")
	}
}

func TestStandalonePickReplica(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	primaryConn := &mockConn{
		DialFn: func() error { return nil },
	}

	replicaConn := &mockConn{
		DialFn: func() error { return nil },
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress: []string{"replica"},
		},
		DisableRetry: true,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return replicaConn
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Test that pick() returns the single replica
	client := s.pick(0)
	if client != s.state.Load().replicas[0] {
		t.Errorf("expected replica client, got different client")
	}
}

func TestNewStandaloneClientWithReplicasPartialFailure(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	dialCount := 0
	primaryConn := &mockConn{
		DialFn:  func() error { return nil },
		CloseFn: func() {},
	}

	replicaConn := &mockConn{
		DialFn: func() error {
			dialCount++
			if dialCount == 2 { // Second replica fails to dial
				return errors.New("replica 2 dial failed")
			}
			return nil
		},
		CloseFn: func() {},
	}

	_, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress: []string{"replica1", "replica2"},
		},
		DisableRetry: true,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return replicaConn
	}, newRetryer(defaultRetryDelayFn))

	if err == nil {
		t.Error("expected error due to replica failure")
	}

	if err.Error() != "replica 2 dial failed" {
		t.Errorf("expected replica 2 dial failed, got %v", err)
	}
}

func TestStandalonePickMultipleReplicas(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	primaryConn := &mockConn{
		DialFn: func() error { return nil },
	}

	replicaConn := &mockConn{
		DialFn: func() error { return nil },
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress: []string{"replica1", "replica2"},
		},
		DisableRetry: true,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return replicaConn
	}, newRetryer(defaultRetryDelayFn))

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Test that pick() returns a valid replica for multiple replicas
	st := s.state.Load()
	for range 10 {
		client := s.pick(0)
		if client != st.replicas[0] && client != st.replicas[1] {
			t.Errorf("expected one of the replica clients, got different client")
		}
	}
}

func TestStandaloneDoMultiWithRedirectRetry(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	redirectErr := ValkeyError(strmsg('-', "REDIRECT 127.0.0.1:6380"))
	attempts := 0

	primaryConn := &mockConn{
		DialFn: func() error { return nil },
		DoMultiFn: func(multi ...Completed) *valkeyresults {
			attempts++
			// First attempt returns redirect error, second returns success
			if attempts == 1 {
				return &valkeyresults{s: []ValkeyResult{newErrResult(&redirectErr)}}
			}
			return &valkeyresults{s: []ValkeyResult{ValkeyResult{val: strmsg('+', "OK")}}}
		},
	}

	redirectConnCalled := false
	redirectConn := &mockConn{
		DialFn: func() error {
			redirectConnCalled = true
			return nil
		},
		DoMultiFn: func(multi ...Completed) *valkeyresults {
			return &valkeyresults{s: []ValkeyResult{ValkeyResult{val: strmsg('+', "OK")}}}
		},
		CloseFn: func() {},
	}

	// Mock retry handler that allows one retry
	mockRetry := &mockRetryHandler{
		WaitOrSkipRetryFunc: func(ctx context.Context, attempts int, cmd Completed, err error) bool {
			return attempts < 2 // Allow one retry
		},
		RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
			return time.Millisecond
		},
		WaitForRetryFn: func(ctx context.Context, duration time.Duration) {
			time.Sleep(duration)
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
		DisableRetry: false,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return redirectConn
	}, mockRetry)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Create a simple command using the internal cmds package
	cmd := cmds.NewCompleted([]string{"SET", "k", "v"})

	// Test DoMulti with redirect and retry
	results := s.DoMulti(context.Background(), cmd)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Error() != nil {
		t.Errorf("expected success after retry, got error: %v", results[0].Error())
	}

	// The primary connection should have been called once, then redirected
	if attempts != 1 {
		t.Errorf("expected 1 attempt on primary before redirect, got %d", attempts)
	}

	if !redirectConnCalled {
		t.Error("expected redirect connection to be called")
	}
}

func TestStandaloneDoMultiWithRedirectRetryFailure(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	redirectErr := ValkeyError(strmsg('-', "REDIRECT 127.0.0.1:6380"))

	primaryConn := &mockConn{
		DialFn: func() error { return nil },
		DoMultiFn: func(multi ...Completed) *valkeyresults {
			return &valkeyresults{s: []ValkeyResult{newErrResult(&redirectErr)}}
		},
	}

	// Redirect connection fails to dial
	redirectConn := &mockConn{
		DialFn: func() error {
			return errors.New("redirect connection failed")
		},
	}

	// Mock retry handler that doesn't allow retries after connection failure
	mockRetry := &mockRetryHandler{
		WaitOrSkipRetryFunc: func(ctx context.Context, attempts int, cmd Completed, err error) bool {
			return false // Don't retry
		},
		RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
			return time.Millisecond
		},
		WaitForRetryFn: func(ctx context.Context, duration time.Duration) {
			time.Sleep(duration)
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
		DisableRetry: false,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return redirectConn
	}, mockRetry)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Create a simple command using the internal cmds package
	cmd := cmds.NewCompleted([]string{"SET", "k", "v"})

	// Test DoMulti with redirect failure
	results := s.DoMulti(context.Background(), cmd)
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Should return the original redirect error since retry is not allowed
	if results[0].Error() == nil {
		t.Error("expected error to be returned")
	}

	if verr, ok := results[0].Error().(*ValkeyError); !ok || !strings.Contains(verr.Error(), "REDIRECT") {
		t.Errorf("expected REDIRECT error, got %v", results[0].Error())
	}
}

func TestStandaloneDoMultiCacheWithRedirectRetry(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	redirectErr := ValkeyError(strmsg('-', "REDIRECT 127.0.0.1:6380"))
	attempts := 0

	primaryConn := &mockConn{
		DialFn: func() error { return nil },
		DoMultiCacheFn: func(multi ...CacheableTTL) *valkeyresults {
			attempts++
			// First attempt returns redirect error, second returns success
			if attempts == 1 {
				return &valkeyresults{s: []ValkeyResult{newErrResult(&redirectErr)}}
			}
			return &valkeyresults{s: []ValkeyResult{ValkeyResult{val: strmsg('+', "OK")}}}
		},
	}

	redirectConnCalled := false
	redirectConn := &mockConn{
		DialFn: func() error {
			redirectConnCalled = true
			return nil
		},
		DoMultiCacheFn: func(multi ...CacheableTTL) *valkeyresults {
			return &valkeyresults{s: []ValkeyResult{ValkeyResult{val: strmsg('+', "OK")}}}
		},
		CloseFn: func() {},
	}

	// Mock retry handler that allows one retry
	mockRetry := &mockRetryHandler{
		WaitOrSkipRetryFunc: func(ctx context.Context, attempts int, cmd Completed, err error) bool {
			return attempts < 2 // Allow one retry
		},
		RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
			return time.Millisecond
		},
		WaitForRetryFn: func(ctx context.Context, duration time.Duration) {
			time.Sleep(duration)
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
		DisableRetry: false,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return redirectConn
	}, mockRetry)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Create a simple command using the internal cmds package
	cmd := cmds.NewCompleted([]string{"SET", "k", "v"})

	// Test DoMulti with redirect and retry
	results := s.DoMultiCache(context.Background(), CT(Cacheable(cmd), time.Second))
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	if results[0].Error() != nil {
		t.Errorf("expected success after retry, got error: %v", results[0].Error())
	}

	// The primary connection should have been called once, then redirected
	if attempts != 1 {
		t.Errorf("expected 1 attempt on primary before redirect, got %d", attempts)
	}

	if !redirectConnCalled {
		t.Error("expected redirect connection to be called")
	}
}

func TestStandaloneDoMultiCacheWithRedirectRetryFailure(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	redirectErr := ValkeyError(strmsg('-', "REDIRECT 127.0.0.1:6380"))

	primaryConn := &mockConn{
		DialFn: func() error { return nil },
		DoMultiCacheFn: func(multi ...CacheableTTL) *valkeyresults {
			return &valkeyresults{s: []ValkeyResult{newErrResult(&redirectErr)}}
		},
	}

	// Redirect connection fails to dial
	redirectConn := &mockConn{
		DialFn: func() error {
			return errors.New("redirect connection failed")
		},
	}

	// Mock retry handler that doesn't allow retries after connection failure
	mockRetry := &mockRetryHandler{
		WaitOrSkipRetryFunc: func(ctx context.Context, attempts int, cmd Completed, err error) bool {
			return false // Don't retry
		},
		RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
			return time.Millisecond
		},
		WaitForRetryFn: func(ctx context.Context, duration time.Duration) {
			time.Sleep(duration)
		},
	}

	s, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			EnableRedirect: true,
		},
		DisableRetry: false,
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primaryConn
		}
		return redirectConn
	}, mockRetry)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer s.Close()

	// Create a simple command using the internal cmds package
	cmd := cmds.NewCompleted([]string{"SET", "k", "v"})

	// Test DoMulti with redirect failure
	results := s.DoMultiCache(context.Background(), CT(Cacheable(cmd), time.Second))
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}

	// Should return the original redirect error since retry is not allowed
	if results[0].Error() == nil {
		t.Error("expected error to be returned")
	}

	if verr, ok := results[0].Error().(*ValkeyError); !ok || !strings.Contains(verr.Error(), "REDIRECT") {
		t.Errorf("expected REDIRECT error, got %v", results[0].Error())
	}
}

func TestStandaloneReadNodeSelector(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	t.Run("ReadNodeSelector", func(t *testing.T) {
		primaryNodeConn := &mockConn{
			DoFn: func(cmd Completed) ValkeyResult {
				return newResult(strmsg('+', "primary"), nil)
			},
			AZFn: func() string {
				return "us-east-1a"
			},
		}
		replica1NodeConn := &mockConn{
			DoFn: func(cmd Completed) ValkeyResult {
				return newResult(strmsg('+', "replica1"), nil)
			},
			AZFn: func() string {
				return "us-east-1a" // Same AZ as client
			},
		}
		replica2NodeConn := &mockConn{
			DoFn: func(cmd Completed) ValkeyResult {
				return newResult(strmsg('+', "replica2"), nil)
			},
			AZFn: func() string {
				return "us-east-1b" // Different AZ
			},
		}

		client, err := newStandaloneClient(&ClientOption{
			InitAddress: []string{"primary"},
			Standalone: StandaloneOption{
				ReplicaAddress: []string{"replica1", "replica2"},
			},
			EnableReplicaAZInfo: true,
			SendToReplicas: func(cmd Completed) bool {
				return cmd.IsReadOnly()
			},
			ReadNodeSelector: func(slot uint16, nodes []NodeInfo) int {
				for i := 1; i < len(nodes); i++ {
					if nodes[i].AZ == "us-east-1a" {
						return i
					}
				}
				return -1
			},
			DisableRetry: true,
		}, func(dst string, opt *ClientOption) conn {
			if dst == "primary" {
				return primaryNodeConn
			}
			if dst == "replica1" {
				return replica1NodeConn
			}
			return replica2NodeConn
		}, newRetryer(defaultRetryDelayFn))

		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		defer client.Close()

		// Verify same-AZ replica (replica1) is selected when executing GET command
		for range 10 {
			result := client.Do(context.Background(), client.B().Get().Key("key").Build())
			if val, _ := result.ToString(); val != "replica1" {
				t.Fatalf("expected same-AZ replica1 to be selected, got %s", val)
			}
		}
	})
}

func TestStandaloneClientConnLifetime(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	setup := func() (*standalone, *mockConn, *mockConn) {
		primary := &mockConn{}
		replica := &mockConn{}
		client, err := newStandaloneClient(
			&ClientOption{
				InitAddress: []string{"primary"},
				Standalone: StandaloneOption{
					ReplicaAddress: []string{"replica"},
				},
				ConnLifetime:   5 * time.Second,
				SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
			},
			func(dst string, opt *ClientOption) conn {
				if dst == "replica" {
					return replica
				}
				return primary
			},
			newRetryer(defaultRetryDelayFn),
		)
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		return client, primary, replica
	}

	t.Run("Do recovery", func(t *testing.T) {
		client, primary, _ := setup()
		defer client.Close()
		attempts := 0
		primary.DoFn = func(cmd Completed) ValkeyResult {
			attempts++
			if attempts == 1 {
				return newErrResult(errConnExpired)
			}
			return newResult(strmsg('+', "OK"), nil)
		}
		if v, err := client.Do(context.Background(), client.B().Set().Key("k").Value("v").Build()).ToString(); err != nil || v != "OK" {
			t.Fatalf("unexpected response %v %v", v, err)
		}
	})

	t.Run("DoMulti errConnExpired at head", func(t *testing.T) {
		client, primary, _ := setup()
		defer client.Close()
		attempts := 0
		primary.DoMultiFn = func(multi ...Completed) *valkeyresults {
			attempts++
			if attempts == 1 {
				return &valkeyresults{s: []ValkeyResult{
					newErrResult(errConnExpired),
					newErrResult(errConnExpired),
				}}
			}
			return &valkeyresults{s: []ValkeyResult{
				newResult(strmsg('+', "1"), nil),
				newResult(strmsg('+', "2"), nil),
			}}
		}
		resps := client.DoMulti(context.Background(),
			client.B().Set().Key("k1").Value("v1").Build(),
			client.B().Set().Key("k2").Value("v2").Build(),
		)
		if len(resps) != 2 {
			t.Fatalf("unexpected response length %v", len(resps))
		}
		for i, resp := range resps {
			if err := resp.Error(); err != nil {
				t.Fatalf("results[%d] unexpected error %v", i, err)
			}
		}
	})

	t.Run("DoMulti errConnExpired in middle", func(t *testing.T) {
		client, primary, _ := setup()
		defer client.Close()
		attempts := 0
		primary.DoMultiFn = func(multi ...Completed) *valkeyresults {
			attempts++
			if attempts == 1 {
				return &valkeyresults{s: []ValkeyResult{
					newResult(strmsg('+', "OK"), nil),
					newErrResult(errConnExpired),
				}}
			}
			return &valkeyresults{s: []ValkeyResult{
				newResult(strmsg('+', "OK"), nil),
			}}
		}
		resps := client.DoMulti(context.Background(),
			client.B().Set().Key("k1").Value("v1").Build(),
			client.B().Set().Key("k2").Value("v2").Build(),
		)
		if len(resps) != 2 {
			t.Fatalf("unexpected response length %v", len(resps))
		}
		for i, resp := range resps {
			if err := resp.Error(); err != nil {
				t.Fatalf("results[%d] unexpected error %v", i, err)
			}
		}
	})

	t.Run("DoMulti replica errConnExpired at head", func(t *testing.T) {
		client, _, replica := setup()
		defer client.Close()
		attempts := 0
		replica.DoMultiFn = func(multi ...Completed) *valkeyresults {
			attempts++
			if attempts == 1 {
				return &valkeyresults{s: []ValkeyResult{newErrResult(errConnExpired)}}
			}
			return &valkeyresults{s: []ValkeyResult{newResult(strmsg('+', "OK"), nil)}}
		}
		resps := client.DoMulti(context.Background(),
			client.B().Get().Key("k").Build(),
		)
		if err := resps[0].Error(); err != nil {
			t.Fatalf("unexpected error %v", err)
		}
		if attempts != 2 {
			t.Fatalf("expected 2 attempts (1 errConnExpired + 1 retry), got %d", attempts)
		}
	})

	t.Run("redirectToPrimary hasLftm propagated", func(t *testing.T) {
		redirectConn := &mockConn{}
		attempts := 0
		redirectConn.DoMultiFn = func(multi ...Completed) *valkeyresults {
			attempts++
			if attempts == 1 {
				return &valkeyresults{s: []ValkeyResult{newErrResult(errConnExpired)}}
			}
			return &valkeyresults{s: []ValkeyResult{newResult(strmsg('+', "OK"), nil)}}
		}

		client, err := newStandaloneClient(
			&ClientOption{
				InitAddress:  []string{"primary"},
				ConnLifetime: 5 * time.Second,
			},
			func(dst string, opt *ClientOption) conn {
				if dst == "redirect" {
					return redirectConn
				}
				return &mockConn{}
			},
			newRetryer(defaultRetryDelayFn),
		)
		if err != nil {
			t.Fatalf("unexpected err %v", err)
		}
		defer client.Close()

		if err := client.recreatePrimaryConn("redirect"); err != nil {
			t.Fatalf("unexpected redirect err %v", err)
		}

		resps := client.DoMulti(context.Background(),
			client.B().Set().Key("k").Value("v").Build(),
		)
		if err := resps[0].Error(); err != nil {
			t.Fatalf("unexpected error after redirect: %v", err)
		}
		if attempts != 2 {
			t.Fatalf("expected 2 attempts (1 errConnExpired + 1 retry), got %d", attempts)
		}
	})
}

func mockConnWithRole(addr, role string) *mockConn {
	connRole := role
	if role == "slave" {
		connRole = "replica"
	}
	return &mockConn{
		AddrFn: func() string { return addr },
		AZFn:   func() string { return addr + "-az" },
		// Role() reads the role label cached from HELLO; the constructor
		// uses this for the cheap master dedup check.
		RoleFn: func() string { return connRole },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', role)})}
			}
			return ValkeyResult{}
		},
	}
}

func TestNewStandaloneClientReplicaAZInit(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	primaryConn := &mockConn{
		AddrFn: func() string { return "primary" },
		AZFn:   func() string { return "primary-az" },
	}
	r1 := mockConnWithRole("r1", "slave")
	r2 := mockConnWithRole("r2", "slave")

	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress: []string{"r1", "r2"},
		},
		SendToReplicas: func(cmd Completed) bool {
			return cmd.IsReadOnly()
		},
		EnableReplicaAZInfo: true,
		ReadNodeSelector: func(slot uint16, nodes []NodeInfo) int {
			return 1
		},
	}, func(dst string, opt *ClientOption) conn {
		switch dst {
		case "primary":
			return primaryConn
		case "r1":
			return r1
		case "r2":
			return r2
		default:
			return &mockConn{}
		}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	defer c.Close()

	st := c.state.Load()
	if st == nil {
		t.Fatal("expected non-nil state")
	}
	if len(st.replicas) != 2 {
		t.Fatalf("expected 2 replicas, got %d", len(st.replicas))
	}
	if c.primary.Load().conn != primaryConn {
		t.Fatal("primary should be InitAddress connection")
	}
	if st.nodes[0].Addr != "primary" || st.nodes[0].AZ != "primary-az" {
		t.Fatalf("unexpected primary node info %+v", st.nodes[0])
	}
	if st.nodes[1].Addr != "r1" || st.nodes[2].Addr != "r2" {
		t.Fatalf("unexpected replica node info %+v", st.nodes[1:])
	}
	if c.pick(0).conn != r1 {
		t.Fatal("expected pick to select r1 via ReadNodeSelector")
	}
}

func TestNewStandaloneClientReplicaAddressDedupPrimary(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	var dupClosed int32
	dup := mockConnWithRole("dup", "master")
	dup.CloseFn = func() {
		atomic.AddInt32(&dupClosed, 1)
	}
	r1 := mockConnWithRole("r1", "slave")

	// Lenient mode: a master listed in ReplicaAddress is silently deduped
	// against the primary at init time.
	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress:         []string{"dup", "r1"},
			ReplicaRefreshInterval: time.Hour, // enable lenient mode without firing the monitor
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
	}, func(dst string, opt *ClientOption) conn {
		switch dst {
		case "primary":
			return mockConnWithRole("primary", "master")
		case "dup":
			return dup
		case "r1":
			return r1
		default:
			return &mockConn{}
		}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	defer c.Close()

	if atomic.LoadInt32(&dupClosed) != 1 {
		t.Fatalf("expected duplicate primary node conn closed once, got %d", dupClosed)
	}
	st := c.state.Load()
	if len(st.replicas) != 1 {
		t.Fatalf("expected 1 replica, got %d", len(st.replicas))
	}
}

func TestNewStandaloneClientReplicaAddressDialErrorDropped(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	v := errors.New("dial err")
	primary := mockConnWithRole("primary", "master")
	// Lenient mode: a replica that fails to dial is dropped silently.
	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress:         []string{"r1", "r2"},
			ReplicaRefreshInterval: time.Hour,
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
	}, func(dst string, opt *ClientOption) conn {
		if dst == "primary" {
			return primary
		}
		if dst == "r2" {
			return &mockConn{DialFn: func() error { return v }}
		}
		return mockConnWithRole(dst, "slave")
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("expected init to succeed, got err %v", err)
	}
	defer c.Close()
	st := c.state.Load()
	if len(st.replicas) != 1 || st.nodes[1].Addr != "r1" {
		t.Fatalf("expected only r1 in replica pool, got %+v", st.nodes[1:])
	}
}

func TestNewStandaloneClientReplicaAddressRoleErrorTolerated(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	v := errors.New("role err")
	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress: []string{"r1"},
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
	}, func(dst string, opt *ClientOption) conn {
		if dst == "r1" {
			return &mockConn{
				AddrFn: func() string { return "r1" },
				DoFn: func(cmd Completed) ValkeyResult {
					if cmd == cmds.RoleCmd {
						return newErrResult(v)
					}
					return ValkeyResult{}
				},
			}
		}
		return &mockConn{AddrFn: func() string { return dst }}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("expected init to tolerate ROLE error, got err %v", err)
	}
	defer c.Close()
	if got := len(c.state.Load().replicas); got != 1 {
		t.Fatalf("expected replica kept when ROLE errors, got %d replicas", got)
	}
}

func TestStandaloneFailoverOnReadonly(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	var primaryRole atomic.Value
	primaryRole.Store("master")
	primary := &mockConn{
		AddrFn: func() string { return "primary" },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', primaryRole.Load().(string))})}
			}
			readonlyErr := ValkeyError(strmsg('-', "READONLY You can't write against a read only replica."))
			return newErrResult(&readonlyErr)
		},
	}
	var r1Role atomic.Value
	r1Role.Store("slave")
	r1 := &mockConn{
		AddrFn: func() string { return "r1" },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', r1Role.Load().(string))})}
			}
			return newResult(strmsg('+', "OK"), nil)
		},
	}
	r2 := mockConnWithRole("r2", "slave")

	// InitAddress[0] is a DNS endpoint that auto-resolves to the active
	// primary (as cloud vendors do); start it pointing at primary.
	var primaryDNS atomic.Pointer[mockConn]
	primaryDNS.Store(primary)

	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress:         []string{"r1", "r2"},
			ReplicaRefreshInterval: time.Hour,
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
		DisableRetry:   true,
	}, func(dst string, opt *ClientOption) conn {
		switch dst {
		case "primary":
			return primaryDNS.Load()
		case "r1":
			return r1
		case "r2":
			return r2
		default:
			return &mockConn{}
		}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	defer c.Close()

	// Simulate failover: primary is demoted, r1 is promoted, and the
	// DNS for the primary endpoint now resolves to r1.
	primaryRole.Store("slave")
	r1Role.Store("master")
	primaryDNS.Store(r1)

	if _, err := c.Do(context.Background(), c.B().Set().Key("k").Value("v").Build()).ToString(); err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	if c.primary.Load().conn != r1 {
		t.Fatal("expected r1 to become primary after failover")
	}
	st := c.state.Load()
	if st.nodes[0].Addr != "r1" {
		t.Fatalf("expected nodes[0] to be r1, got %s", st.nodes[0].Addr)
	}
	foundPrimary := false
	for _, rep := range st.replicas {
		if rep.conn == primary {
			foundPrimary = true
		}
		if rep.conn == r1 {
			t.Fatal("r1 should not remain in replica pool")
		}
	}
	if !foundPrimary {
		t.Fatal("expected former primary in replica pool")
	}
}

func TestStandaloneFailoverDropDownReplica(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	var primaryRole atomic.Value
	primaryRole.Store("master")
	primary := &mockConn{
		AddrFn: func() string { return "primary" },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', primaryRole.Load().(string))})}
			}
			readonlyErr := ValkeyError(strmsg('-', "READONLY"))
			return newErrResult(&readonlyErr)
		},
	}
	var r1Role atomic.Value
	r1Role.Store("slave")
	r1 := &mockConn{
		AddrFn: func() string { return "r1" },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', r1Role.Load().(string))})}
			}
			return newResult(strmsg('+', "OK"), nil)
		},
	}
	var r2RoleCalls int32
	r2 := &mockConn{
		AddrFn: func() string { return "r2" },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				if atomic.AddInt32(&r2RoleCalls, 1) > 1 {
					return newErrResult(errors.New("down"))
				}
				return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "slave")})}
			}
			return ValkeyResult{}
		},
	}

	// InitAddress[0] is a DNS endpoint that auto-resolves to the active
	// primary (as cloud vendors do); start it pointing at primary.
	var primaryDNS atomic.Pointer[mockConn]
	primaryDNS.Store(primary)

	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress:         []string{"r1", "r2"},
			ReplicaRefreshInterval: time.Hour,
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
		DisableRetry:   true,
	}, func(dst string, opt *ClientOption) conn {
		switch dst {
		case "primary":
			return primaryDNS.Load()
		case "r1":
			return r1
		case "r2":
			return r2
		default:
			return &mockConn{}
		}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	defer c.Close()

	// Simulate failover: primary is demoted, r1 is promoted, and the
	// DNS for the primary endpoint now resolves to r1.
	primaryRole.Store("slave")
	r1Role.Store("master")
	primaryDNS.Store(r1)

	if _, err := c.Do(context.Background(), c.B().Set().Key("k").Value("v").Build()).ToString(); err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	st := c.state.Load()
	if len(st.replicas) != 1 || st.replicas[0].conn != primary {
		t.Fatalf("expected only demoted primary in replica pool, got %d replicas", len(st.replicas))
	}
}

func TestStandaloneSelectorPrimaryAfterFailover(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	var primaryRole atomic.Value
	primaryRole.Store("master")
	primary := &mockConn{
		AddrFn: func() string { return "primary" },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', primaryRole.Load().(string))})}
			}
			readonlyErr := ValkeyError(strmsg('-', "READONLY"))
			return newErrResult(&readonlyErr)
		},
	}
	var r1Role atomic.Value
	r1Role.Store("slave")
	r1 := &mockConn{
		AddrFn: func() string { return "r1" },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', r1Role.Load().(string))})}
			}
			return newResult(strmsg('+', "OK"), nil)
		},
	}

	// InitAddress[0] is a DNS endpoint that auto-resolves to the active
	// primary (as cloud vendors do); start it pointing at primary.
	var primaryDNS atomic.Pointer[mockConn]
	primaryDNS.Store(primary)

	var seenPrimary string
	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress:         []string{"r1"},
			ReplicaRefreshInterval: time.Hour,
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
		ReadNodeSelector: func(slot uint16, nodes []NodeInfo) int {
			seenPrimary = nodes[0].Addr
			return 0
		},
		DisableRetry: true,
	}, func(dst string, opt *ClientOption) conn {
		switch dst {
		case "primary":
			return primaryDNS.Load()
		case "r1":
			return r1
		default:
			return &mockConn{}
		}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	defer c.Close()

	// Simulate failover: primary is demoted, r1 is promoted, and the
	// DNS for the primary endpoint now resolves to r1.
	primaryRole.Store("slave")
	r1Role.Store("master")
	primaryDNS.Store(r1)
	if _, err := c.Do(context.Background(), c.B().Set().Key("k").Value("v").Build()).ToString(); err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	c.Do(context.Background(), c.B().Get().Key("k").Build())
	if seenPrimary != "r1" {
		t.Fatalf("expected selector to see r1 as primary, got %q", seenPrimary)
	}
}

func TestStandaloneReconcileNoop(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	primary := mockConnWithRole("primary", "master")
	r1 := mockConnWithRole("r1", "slave")
	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress:         []string{"r1"},
			ReplicaRefreshInterval: time.Hour,
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
	}, func(dst string, opt *ClientOption) conn {
		switch dst {
		case "primary":
			return primary
		case "r1":
			return r1
		default:
			return &mockConn{}
		}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	defer c.Close()

	stBefore := c.state.Load()
	if err := c.reconcile(context.Background()); err != nil {
		t.Fatalf("unexpected reconcile err %v", err)
	}
	if c.primary.Load().conn != primary {
		t.Fatal("expected primary unchanged after noop reconcile")
	}
	stAfter := c.state.Load()
	if stAfter.nodes[0].Addr != stBefore.nodes[0].Addr || len(stAfter.replicas) != len(stBefore.replicas) {
		t.Fatal("expected state unchanged after noop reconcile")
	}
}

func slaveRoleResp(state string) ValkeyResult {
	return ValkeyResult{val: slicemsg('*', []ValkeyMessage{
		strmsg('+', "slave"),
		strmsg('+', "127.0.0.1"),
		{typ: ':', intlen: 6379},
		strmsg('+', state),
		{typ: ':', intlen: 0},
	})}
}

func TestNewStandaloneClientReplicaAddressDropsDisconnectedSlave(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	r1 := &mockConn{
		AddrFn: func() string { return "r1" },
		// HELLO reports it as a replica; ROLE later reveals it's not synced.
		RoleFn: func() string { return "replica" },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				return slaveRoleResp("connect") // not yet synced
			}
			return ValkeyResult{}
		},
	}
	r2 := mockConnWithRole("r2", "slave")
	primary := mockConnWithRole("primary", "master")

	// Refresh interval triggers reconcile at init, which uses ROLE to detect
	// the disconnected slave and exclude it from the routing pool.
	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress:         []string{"r1", "r2"},
			ReplicaRefreshInterval: time.Hour,
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
	}, func(dst string, opt *ClientOption) conn {
		switch dst {
		case "primary":
			return primary
		case "r1":
			return r1
		case "r2":
			return r2
		}
		return &mockConn{}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	defer c.Close()

	st := c.state.Load()
	if len(st.replicas) != 1 || st.nodes[1].Addr != "r2" {
		t.Fatalf("expected only r2 in replica pool, got %+v", st.nodes[1:])
	}
}

func TestStandaloneReplicaRefreshMonitor(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	var r1State atomic.Value
	r1State.Store("connected")
	var r1Closed int32
	r1 := &mockConn{
		AddrFn:  func() string { return "r1" },
		CloseFn: func() { atomic.AddInt32(&r1Closed, 1) },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				return slaveRoleResp(r1State.Load().(string))
			}
			return ValkeyResult{}
		},
	}
	primary := mockConnWithRole("primary", "master")

	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress:         []string{"r1"},
			ReplicaRefreshInterval: 25 * time.Millisecond,
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
	}, func(dst string, opt *ClientOption) conn {
		switch dst {
		case "primary":
			return primary
		case "r1":
			return r1
		}
		return &mockConn{}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	defer c.Close()

	if got := len(c.state.Load().replicas); got != 1 {
		t.Fatalf("expected 1 replica after init, got %d", got)
	}

	// Simulate r1 falling out of sync (still alive but disconnected from primary).
	r1State.Store("connect")

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if len(c.state.Load().replicas) == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := len(c.state.Load().replicas); got != 0 {
		t.Fatalf("expected monitor to drop disconnected replica, still %d", got)
	}
}

func TestStandaloneReplicaRefreshMonitorPromotesNewPrimary(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	var primaryRole atomic.Value
	primaryRole.Store("master")
	primary := &mockConn{
		AddrFn: func() string { return "primary" },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				if primaryRole.Load().(string) == "master" {
					return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "master")})}
				}
				return slaveRoleResp("connected")
			}
			return ValkeyResult{}
		},
	}
	var r1Role atomic.Value
	r1Role.Store("slave")
	r1 := &mockConn{
		AddrFn: func() string { return "r1" },
		DoFn: func(cmd Completed) ValkeyResult {
			if cmd == cmds.RoleCmd {
				if r1Role.Load().(string) == "master" {
					return ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "master")})}
				}
				return slaveRoleResp("connected")
			}
			return ValkeyResult{}
		},
	}

	// InitAddress[0] is a DNS endpoint that auto-resolves to the active
	// primary (as cloud vendors do); start it pointing at primary.
	var primaryDNS atomic.Pointer[mockConn]
	primaryDNS.Store(primary)

	c, err := newStandaloneClient(&ClientOption{
		InitAddress: []string{"primary"},
		Standalone: StandaloneOption{
			ReplicaAddress:         []string{"r1"},
			ReplicaRefreshInterval: 25 * time.Millisecond,
		},
		SendToReplicas: func(cmd Completed) bool { return cmd.IsReadOnly() },
	}, func(dst string, opt *ClientOption) conn {
		switch dst {
		case "primary":
			return primaryDNS.Load()
		case "r1":
			return r1
		}
		return &mockConn{}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	defer c.Close()

	if c.primary.Load().conn != primary {
		t.Fatal("expected primary at start")
	}

	// Simulate failover before any READONLY happens — monitor should pick it up.
	// DNS for the primary endpoint now resolves to r1.
	primaryRole.Store("slave")
	r1Role.Store("master")
	primaryDNS.Store(r1)

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.primary.Load().conn == r1 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if c.primary.Load().conn != r1 {
		t.Fatal("expected monitor to promote r1 to primary")
	}
}
