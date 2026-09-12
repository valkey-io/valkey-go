package valkey

import (
	"context"
	"crypto/tls"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type dummyNetConn struct {
	net.Conn
}

func (d *dummyNetConn) Close() error { return nil }

func BenchmarkDial_HappyPath_NoRetry(b *testing.B) {
	conn := &dummyNetConn{}
	opt := &ClientOption{
		DialerRetries: 0,
		DialCtxFn: func(ctx context.Context, _ string, _ *net.Dialer, _ *tls.Config) (net.Conn, error) {
			return conn, nil
		},
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := dial(ctx, "127.0.0.1:6379", opt)
		if err != nil || c == nil {
			b.Fatalf("dial failed: %v", err)
		}
	}
}

func BenchmarkDial_HappyPath_WithRetry(b *testing.B) {
	conn := &dummyNetConn{}
	opt := &ClientOption{
		DialerRetries:        3,
		DialerRetryBaseDelay: 50 * time.Millisecond,
		DialerRetryMaxDelay:  500 * time.Millisecond,
		DialCtxFn: func(ctx context.Context, _ string, _ *net.Dialer, _ *tls.Config) (net.Conn, error) {
			return conn, nil
		},
	}
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c, err := dial(ctx, "127.0.0.1:6379", opt)
		if err != nil || c == nil {
			b.Fatalf("dial failed: %v", err)
		}
	}
}

func BenchmarkBackoff_DefaultRetryDelayFn_Sequential(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = defaultRetryDelayFn(i%20, Completed{}, nil)
	}
}

func BenchmarkBackoff_DefaultRetryDelayFn_Parallel(b *testing.B) {
	var count atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			attempt := int(count.Add(1) % 20)
			_ = defaultRetryDelayFn(attempt, Completed{}, nil)
		}
	})
}

func BenchmarkBackoff_FullJitterDelayFn_Sequential(b *testing.B) {
	fn := fullJitterDelayFn(100*time.Millisecond, 3*time.Second)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = fn(i%20 + 1)
	}
}

func BenchmarkBackoff_FullJitterDelayFn_Parallel(b *testing.B) {
	fn := fullJitterDelayFn(100*time.Millisecond, 3*time.Second)
	var count atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			attempt := int(count.Add(1)%20 + 1)
			_ = fn(attempt)
		}
	})
}

func BenchmarkBackoff_FullJitterRetryDelayFn_Parallel(b *testing.B) {
	var count atomic.Int64
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			attempt := int(count.Add(1)%20 + 1)
			_ = FullJitterRetryDelayFn(attempt, Completed{}, nil)
		}
	})
}
