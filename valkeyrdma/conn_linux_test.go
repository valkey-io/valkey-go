package valkeyrdma

import (
	"context"
	"crypto/tls"
	"math/rand/v2"
	"net"
	"strconv"
	"testing"

	"github.com/valkey-io/valkey-go"
)

func BenchmarkE2E(b *testing.B) {
	f := func(o valkey.ClientOption) func(b *testing.B) {
		return func(b *testing.B) {
			c, err := valkey.NewClient(o)
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					i := rand.Uint64N(100000000000)
					s := strconv.FormatUint(i, 10)
					r, err := c.Do(context.Background(), c.B().Echo().Message(s).Build()).ToString()
					if err != nil || r != s {
						b.Fatal(err)
					}
				}
			})
			b.StopTimer()
		}
	}
	b.Run("TCP", f(valkey.ClientOption{
		InitAddress: []string{"172.16.255.128:6379"},
	}))
	b.Run("RDMA", f(valkey.ClientOption{
		InitAddress: []string{"172.16.255.128:6378"},
		DialCtxFn: func(ctx context.Context, s string, dialer *net.Dialer, config *tls.Config) (conn net.Conn, err error) {
			return DialContext(ctx, s)
		},
	}))
}
