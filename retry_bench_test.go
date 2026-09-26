package valkey

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"
)

type mockHandshakeConn struct {
	data []byte
}

func (m *mockHandshakeConn) Read(b []byte) (int, error) {
	if len(m.data) == 0 {
		return 0, io.EOF
	}
	n := copy(b, m.data)
	m.data = m.data[n:]
	return n, nil
}

func (m *mockHandshakeConn) Write(b []byte) (int, error) {
	return len(b), nil
}

func (m *mockHandshakeConn) Close() error                       { return nil }
func (m *mockHandshakeConn) LocalAddr() net.Addr                { return &net.TCPAddr{} }
func (m *mockHandshakeConn) RemoteAddr() net.Addr               { return &net.TCPAddr{} }
func (m *mockHandshakeConn) SetDeadline(t time.Time) error      { return nil }
func (m *mockHandshakeConn) SetReadDeadline(t time.Time) error  { return nil }
func (m *mockHandshakeConn) SetWriteDeadline(t time.Time) error { return nil }

const hello3Reply = "%1\r\n+proto\r\n:3\r\n"

func BenchmarkDial_HappyPath_NoRetry(b *testing.B) {
	opt := &ClientOption{
		DialerRetries: 0,
		DisableCache:  true,
		ClientSetInfo: []string{},
	}
	dialFn := func(ctx context.Context, _ string, _ *ClientOption) (net.Conn, error) {
		return &mockHandshakeConn{data: []byte(hello3Reply)}, nil
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := makeMux("127.0.0.1:6379", opt, dialFn)
		if err := m.Dial(); err != nil {
			b.Fatalf("dial failed: %v", err)
		}
	}
}

func BenchmarkDial_HappyPath_WithRetry(b *testing.B) {
	opt := &ClientOption{
		DialerRetries:        3,
		DialerRetryBaseDelay: 50 * time.Millisecond,
		DialerRetryMaxDelay:  500 * time.Millisecond,
		DisableCache:         true,
		ClientSetInfo:        []string{},
	}
	dialFn := func(ctx context.Context, _ string, _ *ClientOption) (net.Conn, error) {
		return &mockHandshakeConn{data: []byte(hello3Reply)}, nil
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		m := makeMux("127.0.0.1:6379", opt, dialFn)
		if err := m.Dial(); err != nil {
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
