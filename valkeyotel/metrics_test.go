package valkeyotel

import (
	"context"
	"crypto/tls"
	"errors"
	"net"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/valkey-io/valkey-go"
)

func TestNewClient(t *testing.T) {
	t.Run("client option only (no ctx)", func(t *testing.T) {
		c, err := NewClient(valkey.ClientOption{
			InitAddress: []string{"127.0.0.1:6379"},
			DialFn: func(dst string, dialer *net.Dialer, _ *tls.Config) (conn net.Conn, err error) {
				return dialer.Dial("tcp", dst)
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	})

	t.Run("client option only", func(t *testing.T) {
		c, err := NewClient(valkey.ClientOption{
			InitAddress: []string{"127.0.0.1:6379"},
			DialCtxFn: func(ctx context.Context, dst string, dialer *net.Dialer, _ *tls.Config) (conn net.Conn, err error) {
				return dialer.DialContext(ctx, "tcp", dst)
			},
		})
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	})

	t.Run("meter provider", func(t *testing.T) {
		mr := metric.NewManualReader()
		meterProvider := metric.NewMeterProvider(metric.WithReader(mr))
		c, err := NewClient(
			valkey.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
				DialCtxFn: func(ctx context.Context, dst string, dialer *net.Dialer, _ *tls.Config) (conn net.Conn, err error) {
					return dialer.DialContext(ctx, "tcp", dst)
				},
			},
			WithMeterProvider(meterProvider),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	})

	t.Run("dial latency histogram option", func(t *testing.T) {
		c, err := NewClient(
			valkey.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
				DialCtxFn: func(ctx context.Context, dst string, dialer *net.Dialer, _ *tls.Config) (conn net.Conn, err error) {
					return dialer.DialContext(ctx, "tcp", dst)
				},
			},
			WithHistogramOption(HistogramOption{
				Buckets: []float64{1, 2, 3},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
	})

	t.Run("DialFn by default", func(t *testing.T) {
		_, err := NewClient(valkey.ClientOption{
			InitAddress: []string{"127.0.0.1:6379"},
		},
		)
		if err != nil {
			t.Error(err)
		}
	})
}

func TestNewClientError(t *testing.T) {
	t.Run("invalid client option", func(t *testing.T) {
		_, err := NewClient(valkey.ClientOption{
			InitAddress: []string{""},
			DialCtxFn: func(ctx context.Context, dst string, dialer *net.Dialer, _ *tls.Config) (conn net.Conn, err error) {
				return dialer.DialContext(ctx, "tcp", dst)
			},
		})
		if err == nil {
			t.Error(err)
		}
	})
}

func TestNewClientMeterError(t *testing.T) {
	tests := []struct {
		name string
	}{
		{"valkey_dial_attempt"}, {"valkey_dial_success"}, {"valkey_do_cache_miss"},
		{"valkey_do_cache_hits"}, {"valkey_dial_conns"}, {"valkey_dial_latency"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			meterProvider := &MockMeterProvider{testName: tt.name}
			_, err := NewClient(
				valkey.ClientOption{
					InitAddress: []string{"127.0.0.1:6379"},
				},
				WithMeterProvider(meterProvider),
			)
			if !errors.Is(err, errMocked) || !strings.Contains(err.Error(), tt.name) {
				t.Errorf("mocked error: got %s, want %s", err, errMocked)
			}
		})
	}
}

func TestTrackDialing(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		mr := metric.NewManualReader()
		meterProvider := metric.NewMeterProvider(metric.WithReader(mr))
		c, err := NewClient(
			valkey.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
				DialCtxFn: func(ctx context.Context, dst string, dialer *net.Dialer, _ *tls.Config) (conn net.Conn, err error) {
					return dialer.DialContext(ctx, "tcp", dst)
				},
			},
			WithMeterProvider(meterProvider),
		)
		if err != nil {
			t.Fatal(err)
		}

		metrics := metricdata.ResourceMetrics{}
		if err := mr.Collect(context.Background(), &metrics); err != nil {
			t.Fatal(err)
		}
		attempt := int64CountMetric(metrics, "valkey_dial_attempt")
		if attempt != 1 {
			t.Errorf("attempt: got %d, want 1", attempt)
		}
		success := int64CountMetric(metrics, "valkey_dial_success")
		if success != 1 {
			t.Errorf("success: got %d, want 1", success)
		}
		conns := int64CountMetric(metrics, "valkey_dial_conns")
		if conns != 1 {
			t.Errorf("conns: got %d, want 1", conns)
		}
		dialLatency := float64HistogramMetric(metrics, "valkey_dial_latency")
		if dialLatency == 0 {
			t.Error("dial latency: got 0, want > 0")
		}

		c.Close()

		metrics = metricdata.ResourceMetrics{}
		if err := mr.Collect(context.Background(), &metrics); err != nil {
			t.Fatal(err)
		}
		conns = int64CountMetric(metrics, "valkey_dial_conns")
		if conns != 0 {
			t.Errorf("conns: got %d, want 0", conns)
		}
	})

	t.Run("deduplicated closed connection conns metric", func(t *testing.T) {
		mr := metric.NewManualReader()
		meterProvider := metric.NewMeterProvider(metric.WithReader(mr))
		c, err := NewClient(
			valkey.ClientOption{
				InitAddress: []string{"127.0.0.1:6379"},
				DialCtxFn: func(ctx context.Context, dst string, dialer *net.Dialer, _ *tls.Config) (conn net.Conn, err error) {
					return dialer.DialContext(ctx, "tcp", dst)
				},
			},
			WithMeterProvider(meterProvider),
		)
		if err != nil {
			t.Fatal(err)
		}

		c.Close()
		c.Close()

		metrics := metricdata.ResourceMetrics{}
		if err := mr.Collect(context.Background(), &metrics); err != nil {
			t.Fatal(err)
		}
		conns := int64CountMetric(metrics, "valkey_dial_conns")
		if conns != 0 {
			t.Errorf("conns: got %d, want 0", conns)
		}
	})

	t.Run("failed to dial", func(t *testing.T) {
		mr := metric.NewManualReader()
		meterProvider := metric.NewMeterProvider(metric.WithReader(mr))
		_, err := NewClient(
			valkey.ClientOption{
				InitAddress: []string{""},
				DialCtxFn: func(ctx context.Context, dst string, dialer *net.Dialer, _ *tls.Config) (conn net.Conn, err error) {
					return dialer.DialContext(ctx, "tcp", dst)
				},
			},
			WithMeterProvider(meterProvider),
		)
		if err == nil {
			t.Fatal(err)
		}

		metrics := metricdata.ResourceMetrics{}
		if err := mr.Collect(context.Background(), &metrics); err != nil {
			t.Fatal(err)
		}
		attempt := int64CountMetric(metrics, "valkey_dial_attempt")
		if attempt != 1 {
			t.Errorf("attempt: got %d, want 1", attempt)
		}
		success := int64CountMetric(metrics, "valkey_dial_success")
		if success != 0 {
			t.Errorf("success: got %d, want 0", success)
		}
		conns := int64CountMetric(metrics, "valkey_dial_conns")
		if conns != 0 {
			t.Errorf("conns: got %d, want 0", conns)
		}
		dialLatency := float64HistogramMetric(metrics, "valkey_dial_latency")
		if dialLatency != 0 {
			t.Error("dial latency: got 0, want 0")
		}
	})
}

func int64CountMetric(metrics metricdata.ResourceMetrics, name string) int64 {
	for _, sm := range metrics.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				data, ok := m.Data.(metricdata.Sum[int64])
				if !ok {
					return 0
				}
				return data.DataPoints[0].Value
			}
		}
	}
	return 0
}

func float64HistogramMetric(metrics metricdata.ResourceMetrics, name string) float64 {
	for _, sm := range metrics.ScopeMetrics {
		for _, m := range sm.Metrics {
			if m.Name == name {
				data := m.Data.(metricdata.Histogram[float64])
				return data.DataPoints[0].Sum
			}
		}
	}
	return 0
}
