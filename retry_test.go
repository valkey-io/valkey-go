package valkey

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

type mockRetryHandler struct {
	RetryDelayFn        func(attempts int, _ Completed, err error) time.Duration
	WaitForRetryFn      func(ctx context.Context, duration time.Duration)
	WaitOrSkipRetryFunc func(ctx context.Context, attempts int, _ Completed, err error) bool
}

var _ retryHandler = (*mockRetryHandler)(nil)

func (m *mockRetryHandler) WaitOrSkipRetry(ctx context.Context, attempts int, cmd Completed, err error) bool {
	return m.WaitOrSkipRetryFunc(ctx, attempts, cmd, err)
}

func (m *mockRetryHandler) RetryDelay(attempts int, cmd Completed, err error) time.Duration {
	return m.RetryDelayFn(attempts, cmd, err)
}

func (m *mockRetryHandler) WaitForRetry(ctx context.Context, duration time.Duration) {
	m.WaitForRetryFn(ctx, duration)
}

func TestDefaultRetryDelay(t *testing.T) {
	for i := range 100 {
		err := errors.New("test")
		got := defaultRetryDelayFn(i, Completed{}, err)

		if got < 0 || got > defaultMaxRetryDelay {
			t.Errorf("defaultRetryDelayFn(%d, %v) = %v; want >= 0 and <= %v", i, err, got, defaultMaxRetryDelay)
		}
	}
}

func TestRetryer_RetryDelay(t *testing.T) {
	r := &retryer{
		RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
			return time.Second
		},
	}

	got := r.RetryDelay(0, Completed{}, nil)
	if got != time.Second {
		t.Errorf("RetryDelay() = %v; want %v", got, time.Second)
	}
}

func TestRetryer_WaitForRetry(t *testing.T) {
	t.Run("context is canceled", func(t *testing.T) {
		r := &retryer{}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		start := time.Now()
		r.WaitForRetry(ctx, time.Second)
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitForRetry() took %v; want < 100ms", elapsed)
		}
	})

	t.Run("context deadline is before duration", func(t *testing.T) {
		r := &retryer{}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		start := time.Now()
		r.WaitForRetry(ctx, time.Second)
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitForRetry() took %v; want < 100ms", elapsed)
		}
	})

	t.Run("wait until duration", func(t *testing.T) {
		r := &retryer{}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		start := time.Now()
		r.WaitForRetry(ctx, 50*time.Millisecond)
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitForRetry() took %v; want < 100ms", elapsed)
		}
	})

	t.Run("empty context", func(t *testing.T) {
		r := &retryer{}

		start := time.Now()
		r.WaitForRetry(context.Background(), 50*time.Millisecond)
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitForRetry() took %v; want < 100ms", elapsed)
		}
	})
}

func TestRetrier_WaitOrSkipRetry(t *testing.T) {
	t.Run("RetryDelayFn returns negative delay", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return -1 * time.Second
			},
		}

		shouldRetry := r.WaitOrSkipRetry(context.Background(), 0, Completed{}, nil)
		if shouldRetry {
			t.Error("WaitOrSkipRetry() = true; want false")
		}
	})

	t.Run("RetryDelayFn returns 0 delay", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return 0
			},
		}

		shouldRetry := r.WaitOrSkipRetry(context.Background(), 0, Completed{}, nil)
		if !shouldRetry {
			t.Error("WaitOrSkipRetry() = false; want true")
		}
	})

	t.Run("context is canceled", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return time.Second
			},
		}

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		shouldRetry := r.WaitOrSkipRetry(ctx, 0, Completed{}, nil)
		if !shouldRetry {
			t.Error("WaitOrSkipRetry() = false; want true")
		}
	})

	t.Run("context deadline is before delay", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return time.Second
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()

		start := time.Now()
		shouldRetry := r.WaitOrSkipRetry(ctx, 0, Completed{}, nil)
		if shouldRetry {
			t.Error("WaitOrSkipRetry() = true; want false")
		}
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitOrSkipRetry() took %v; want < 100ms", elapsed)
		}
	})

	t.Run("wait until next retry", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return 50 * time.Millisecond
			},
		}

		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()

		start := time.Now()
		shouldRetry := r.WaitOrSkipRetry(ctx, 0, Completed{}, nil)
		if !shouldRetry {
			t.Error("WaitOrSkipRetry() = false; want true")
		}
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitOrSkipRetry() took %v; want < 100ms", elapsed)
		}
	})

	t.Run("empty context", func(t *testing.T) {
		r := &retryer{
			RetryDelayFn: func(attempts int, _ Completed, err error) time.Duration {
				return 50 * time.Millisecond
			},
		}

		start := time.Now()
		shouldRetry := r.WaitOrSkipRetry(context.Background(), 0, Completed{}, nil)
		if !shouldRetry {
			t.Error("WaitOrSkipRetry() = false; want true")
		}
		elapsed := time.Since(start)

		if elapsed > 100*time.Millisecond {
			t.Errorf("WaitOrSkipRetry() took %v; want < 100ms", elapsed)
		}
	})
}

func TestFullJitterDelayFn(t *testing.T) {
	t.Run("default values when base or maxDelay <= 0", func(t *testing.T) {
		fn := fullJitterDelayFn(0, 0)
		for i := 0; i < 50; i++ {
			d := fn(i)
			if d < 0 || d > 3*time.Second {
				t.Fatalf("expected delay between 0 and 3s, got %v", d)
			}
		}
		for i := 0; i < 50; i++ {
			d := fn(0)
			if d < 0 || d > 10*time.Millisecond {
				t.Fatalf("expected attempt 0 delay <= 10ms, got %v", d)
			}
		}
	})

	t.Run("negative attempt returns 0", func(t *testing.T) {
		fn := fullJitterDelayFn(50*time.Millisecond, 500*time.Millisecond)
		if d := fn(-1); d != 0 {
			t.Fatalf("expected 0 for negative attempt, got %v", d)
		}
	})

	t.Run("bounds and variation", func(t *testing.T) {
		base := 50 * time.Millisecond
		maxDelay := 500 * time.Millisecond
		fn := fullJitterDelayFn(base, maxDelay)

		delays := make(map[time.Duration]bool)
		for i := 0; i < 100; i++ {
			d := fn(i)
			if d < 0 || d > maxDelay {
				t.Fatalf("attempt %d: delay %v out of bounds [0, %v]", i, d, maxDelay)
			}
			delays[d] = true
		}
		if len(delays) < 10 {
			t.Fatalf("expected diverse delays due to jitter, got only %d unique values", len(delays))
		}
	})

	t.Run("attempt scaling within ceiling", func(t *testing.T) {
		base := 10 * time.Millisecond
		maxDelay := 80 * time.Millisecond
		fn := fullJitterDelayFn(base, maxDelay)

		// attempt 0: temp = 10ms -> delay in [0, 10ms)
		for i := 0; i < 20; i++ {
			d := fn(0)
			if d < 0 || d > 10*time.Millisecond {
				t.Fatalf("attempt 0 delay %v exceeds base %v", d, base)
			}
		}

		// attempt 100: temp = maxDelay (80ms), does not overflow
		for i := 0; i < 20; i++ {
			d := fn(100)
			if d < 0 || d > maxDelay {
				t.Fatalf("attempt 100 delay %v exceeds maxDelay %v", d, maxDelay)
			}
		}
	})

	t.Run("concurrent safety", func(t *testing.T) {
		fn := fullJitterDelayFn(10*time.Millisecond, 100*time.Millisecond)
		var wg sync.WaitGroup
		for i := 0; i < 20; i++ {
			wg.Add(1)
			go func(attempt int) {
				defer wg.Done()
				for j := 0; j < 50; j++ {
					d := fn(attempt)
					if d < 0 || d > 100*time.Millisecond {
						t.Errorf("out of bounds: %v", d)
					}
				}
			}(i)
		}
		wg.Wait()
	})

	t.Run("default 3s ceiling and sub-millisecond safety", func(t *testing.T) {
		fn := fullJitterDelayFn(100*time.Millisecond, 3*time.Second)
		for i := 0; i < 50; i++ {
			d := fn(i)
			if d < 0 || d > 3*time.Second {
				t.Fatalf("delay %v out of bounds [0, 3s]", d)
			}
		}

		subMsFn := fullJitterDelayFn(500*time.Microsecond, 500*time.Microsecond)
		for i := 0; i < 20; i++ {
			d := subMsFn(i)
			if d < 0 || d > 500*time.Microsecond {
				t.Fatalf("sub-ms delay %v out of bounds", d)
			}
		}
	})

	t.Run("64-bit duration native resolution and no 32-bit overflow", func(t *testing.T) {
		// Large duration > math.MaxInt32 nanoseconds (which is ~2.147s)
		largeFn := fullJitterDelayFn(5*time.Second, 10*time.Second)
		for i := 0; i < 50; i++ {
			d := largeFn(i)
			if d < 0 || d > 10*time.Second {
				t.Fatalf("large delay %v out of bounds [0, 10s]", d)
			}
		}

		// Verify nanosecond resolution (not quantized to millisecond multiples)
		resFn := fullJitterDelayFn(10*time.Millisecond, 10*time.Millisecond)
		hasSubMs := false
		for i := 0; i < 50; i++ {
			d := resFn(0)
			if d%time.Millisecond != 0 {
				hasSubMs = true
				break
			}
		}
		if !hasSubMs {
			t.Fatalf("expected nanosecond resolution without millisecond quantization")
		}
	})
}

func TestFullJitterRetryDelayFn(t *testing.T) {
	t.Run("bounds and defaults", func(t *testing.T) {
		fn := fullJitterRetryDelayFn(0, 0)
		for i := 0; i < 50; i++ {
			d := fn(i, Completed{}, nil)
			if d < 0 || d > 3*time.Second {
				t.Fatalf("expected delay between 0 and 3s, got %v", d)
			}
		}
	})

	t.Run("custom base and maxDelay", func(t *testing.T) {
		base := 5 * time.Millisecond
		maxDelay := 100 * time.Millisecond
		fn := fullJitterRetryDelayFn(base, maxDelay)
		for i := 0; i < 50; i++ {
			d := fn(i, Completed{}, nil)
			if d < 0 || d > maxDelay {
				t.Fatalf("attempt %d: delay %v out of bounds [0, %v]", i, d, maxDelay)
			}
		}
	})
}

func TestFullJitterRetryDelayOption(t *testing.T) {
	for i := 0; i < 50; i++ {
		d := FullJitterRetryDelayFn(i, Completed{}, nil)
		if d < 0 || d > defaultMaxRetryDelay {
			t.Fatalf("FullJitterRetryDelayFn(%d) = %v; want [0, %v]", i, d, defaultMaxRetryDelay)
		}
	}
}
