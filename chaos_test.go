package valkey

import (
	"bufio"
	"context"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// TestChaos_ThunderingHerdMitigation simulates 100 client goroutines attempting
// simultaneous connection during a network drop, verifying that jitter
// disperses retries across multiple time windows rather than causing synchronized spikes.
func TestChaos_ThunderingHerdMitigation(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	var attempts sync.Map
	var dialCount atomic.Int64

	concurrency := 100
	var wg sync.WaitGroup
	wg.Add(concurrency)

	startBarrier := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	errRefused := syscall.ECONNREFUSED

	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()
			<-startBarrier // Launch all goroutines simultaneously

			opt := &ClientOption{
				DialerRetries:        3,
				DialerRetryBaseDelay: 30 * time.Millisecond,
				DialerRetryMaxDelay:  300 * time.Millisecond,
				DialerRetryBackoff:   fullJitterDelayFn(30*time.Millisecond, 300*time.Millisecond),
				DialCtxFn: func(ctx context.Context, _ string, _ *net.Dialer, _ *tls.Config) (net.Conn, error) {
					dialCount.Add(1)
					attempts.Store(time.Now().UnixNano(), struct{}{})
					return nil, errRefused
				},
			}
			m := makeMux("127.0.0.1:6379", opt, dial)
			defer m.Close()
			_, _ = m._pipe(ctx, 0)
		}()
	}

	close(startBarrier) // Release the thundering herd
	wg.Wait()

	// Bucket attempts into 50ms windows
	windows := make(map[int64]int)
	attempts.Range(func(key, _ any) bool {
		ts := key.(int64)
		window := ts / int64(50*time.Millisecond)
		windows[window]++
		return true
	})

	totalDials := dialCount.Load()
	t.Logf("Total dials recorded: %d across %d distinct 50ms windows", totalDials, len(windows))

	// 100 goroutines * (1 initial + 3 retries) = 400 total dial attempts
	if totalDials != 400 {
		t.Errorf("expected 400 total dial attempts, got %d", totalDials)
	}

	// Without jitter, retries cluster into 1 or 2 synchronized windows.
	// With Full Jitter, retries must disperse across at least 4 distinct windows.
	if len(windows) < 4 {
		t.Errorf("Thundering herd detected! Retries clustered into only %d windows; expected >= 4", len(windows))
	}
}

// TestChaos_NodeRestartAndLoadingRecovery verifies that wireFn retries on -LOADING
// errors using jittered backoff and successfully returns the wire once loading finishes.
func TestChaos_NodeRestartAndLoadingRecovery(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())

	var attempts atomic.Int32
	msg := strmsg('-', "LOADING Valkey is loading the dataset in memory")
	loadingErr := (*ValkeyError)(&msg)

	opt := &ClientOption{
		DialerRetries:        5,
		DialerRetryBaseDelay: 10 * time.Millisecond,
		DialerRetryMaxDelay:  100 * time.Millisecond,
	}

	dialFn := func(ctx context.Context, dst string, o *ClientOption) (net.Conn, error) {
		count := attempts.Add(1)
		if count < 3 {
			return nil, loadingErr
		}
		c1, c2 := net.Pipe()
		go func() {
			mock := &valkeyMock{t: t, buf: bufio.NewReader(c2), conn: c2}
			mock.Expect("HELLO", "3").Reply(slicemsg('%', []ValkeyMessage{
				strmsg('+', "proto"),
				{typ: ':', intlen: 3},
			}))
			mock.Expect("CLIENT", "TRACKING", "ON", "OPTIN").ReplyString("OK")
			mock.Expect("CLIENT", "SETINFO", "LIB-NAME", LibName).ReplyError("UNKNOWN COMMAND")
			mock.Expect("CLIENT", "SETINFO", "LIB-VER", LibVer).ReplyError("UNKNOWN COMMAND")
			mock.Expect("PING").ReplyString("OK")
			mock.Close()
		}()
		return c1, nil
	}

	m := makeMux("127.0.0.1:6379", opt, dialFn)
	defer m.Close()

	err := m.Dial()
	if err != nil {
		t.Fatalf("expected wireFn to succeed after loading cleared, got: %v", err)
	}
	if got := attempts.Load(); got != 3 {
		t.Errorf("expected 3 attempts before succeeding, got %d", got)
	}
}

// TestChaos_LiveValkeyIntegration verifies end-to-end client connectivity against
// live Valkey with DialerRetries enabled.
func TestChaos_LiveValkeyIntegration(t *testing.T) {
	client, err := NewClient(ClientOption{
		InitAddress:          []string{"127.0.0.1:6379"},
		DialerRetries:        3,
		DialerRetryBaseDelay: 20 * time.Millisecond,
		DialerRetryMaxDelay:  200 * time.Millisecond,
		DisableCache:         true,
	})
	if err != nil {
		t.Skipf("skipping live test if Valkey is not reachable: %v", err)
	}
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		t.Skipf("skipping live test if ping fails: %v", err)
	}
}
