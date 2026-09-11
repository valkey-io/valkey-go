package redisrate_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeycompat"
	"github.com/valkey-io/valkey-go/valkeycompat/redisrate"
)

func TestRedisrate(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "Redisrate Suite")
}

func getTestClient() valkey.Client {
	addrs := []string{"127.0.0.1:6379", "127.0.0.1:6378"}
	for _, addr := range addrs {
		client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
		if err == nil {
			if err := client.Do(context.Background(), client.B().Ping().Build()).Error(); err == nil {
				return client
			}
			client.Close()
		}
	}
	return nil
}

var _ = Describe("Redisrate", func() {
	Describe("Limit Helpers", func() {
		It("formats and initializes PerSecond correctly", func() {
			s := redisrate.PerSecond(10)
			Expect(s.Rate).To(Equal(10))
			Expect(s.Burst).To(Equal(10))
			Expect(s.Period).To(Equal(time.Second))
			Expect(s.String()).To(Equal("10 req/s (burst 10)"))
			Expect(s.IsZero()).To(BeFalse())
		})

		It("formats and initializes PerMinute correctly", func() {
			m := redisrate.PerMinute(120)
			Expect(m.Rate).To(Equal(120))
			Expect(m.Burst).To(Equal(120))
			Expect(m.Period).To(Equal(time.Minute))
			Expect(m.String()).To(Equal("120 req/m (burst 120)"))
			Expect(m.IsZero()).To(BeFalse())
		})

		It("formats and initializes PerHour correctly", func() {
			h := redisrate.PerHour(3600)
			Expect(h.Rate).To(Equal(3600))
			Expect(h.Burst).To(Equal(3600))
			Expect(h.Period).To(Equal(time.Hour))
			Expect(h.String()).To(Equal("3600 req/h (burst 3600)"))
			Expect(h.IsZero()).To(BeFalse())
		})

		It("formats custom limits and detects IsZero", func() {
			custom := redisrate.Limit{Rate: 5, Burst: 15, Period: 2 * time.Second}
			Expect(custom.String()).To(Equal("5 req/2s (burst 15)"))

			empty := redisrate.Limit{}
			Expect(empty.IsZero()).To(BeTrue())
		})
	})

	Describe("Limiter Constructors & Nil Handling", func() {
		It("creates limiter instances via NewLimiter and NewLimiterFromClient", func() {
			client := getTestClient()
			if client == nil {
				Skip("skipping test: no live Valkey/Redis instance accessible on 127.0.0.1:6379 or 6378")
			}
			defer client.Close()

			adapter := valkeycompat.NewAdapter(client)
			l1 := redisrate.NewLimiter(adapter)
			Expect(l1).NotTo(BeNil())

			l2 := redisrate.NewLimiterFromClient(client)
			Expect(l2).NotTo(BeNil())

			l3 := redisrate.NewLimiter(nil)
			Expect(l3).NotTo(BeNil())
		})

		It("returns appropriate errors when client is nil", func() {
			l := redisrate.NewLimiter(nil)
			ctx := context.Background()
			limit := redisrate.PerSecond(10)

			_, err := l.Allow(ctx, "k", limit)
			Expect(err).To(HaveOccurred())

			_, err = l.AllowN(ctx, "k", limit, 1)
			Expect(err).To(HaveOccurred())

			_, err = l.AllowAtMost(ctx, "k", limit, 1)
			Expect(err).To(HaveOccurred())

			err = l.Reset(ctx, "k")
			Expect(err).To(HaveOccurred())
		})
	})

	Context("Live Valkey Rate Limiting Operations", func() {
		var (
			client  valkey.Client
			limiter *redisrate.Limiter
			ctx     context.Context
			testID  string
		)

		BeforeEach(func() {
			client = getTestClient()
			if client == nil {
				Skip("skipping test: no live Valkey/Redis instance accessible on 127.0.0.1:6379 or 6378")
			}
			limiter = redisrate.NewLimiter(valkeycompat.NewAdapter(client))
			ctx = context.Background()
			testID = fmt.Sprintf("ginkgo_rate_%d", time.Now().UnixNano())
		})

		AfterEach(func() {
			if client != nil {
				_ = limiter.Reset(ctx, testID)
				client.Close()
			}
		})

		It("executes Allow and Reset correctly", func() {
			limit := redisrate.PerSecond(10)

			// 1. First request
			res, err := limiter.Allow(ctx, testID, limit)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(1))
			Expect(res.Remaining).To(Equal(9))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 100*time.Millisecond, 30*time.Millisecond))

			// 2. Reset and request again
			err = limiter.Reset(ctx, testID)
			Expect(err).NotTo(HaveOccurred())

			res, err = limiter.Allow(ctx, testID, limit)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(1))
			Expect(res.Remaining).To(Equal(9))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 100*time.Millisecond, 30*time.Millisecond))

			// 3. AllowN with 2 tokens
			res, err = limiter.AllowN(ctx, testID, limit, 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(2))
			Expect(res.Remaining).To(Equal(7))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 300*time.Millisecond, 40*time.Millisecond))

			// 4. AllowN with remaining 7 tokens
			res, err = limiter.AllowN(ctx, testID, limit, 7)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(7))
			Expect(res.Remaining).To(Equal(0))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 999*time.Millisecond, 60*time.Millisecond))

			// 5. AllowN with 1000 tokens (exceeded)
			res, err = limiter.AllowN(ctx, testID, limit, 1000)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(0))
			Expect(res.Remaining).To(Equal(0))
			Expect(res.RetryAfter).To(BeNumerically("~", 99*time.Second, 2*time.Second))
			Expect(res.ResetAfter).To(BeNumerically("~", 999*time.Millisecond, 60*time.Millisecond))
		})

		It("peeks without consumption when increment is zero in AllowN", func() {
			limit := redisrate.PerSecond(10)

			// Non-existent key
			res, err := limiter.AllowN(ctx, testID, limit, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(0))
			Expect(res.Remaining).To(Equal(10))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(Equal(time.Duration(0)))

			// Consume 1 token
			res, err = limiter.Allow(ctx, testID, limit)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(1))
			Expect(res.Remaining).To(Equal(9))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 100*time.Millisecond, 30*time.Millisecond))

			// Peek again
			res, err = limiter.AllowN(ctx, testID, limit, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(0))
			Expect(res.Remaining).To(Equal(9))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 100*time.Millisecond, 30*time.Millisecond))
		})

		It("calculates RetryAfter accurately under high-frequency rates", func() {
			limit := redisrate.Limit{
				Rate:   1,
				Period: time.Millisecond,
				Burst:  1,
			}

			for i := 0; i < 100; i++ {
				res, err := limiter.Allow(ctx, testID, limit)
				Expect(err).NotTo(HaveOccurred())

				if res.Allowed > 0 {
					continue
				}

				Expect(int64(res.RetryAfter)).To(BeNumerically("<=", int64(2*time.Millisecond)))
			}
		})

		It("grants partial capacity when calling AllowAtMost", func() {
			limit := redisrate.PerSecond(10)

			// 1. Consume 1 token
			res, err := limiter.Allow(ctx, testID, limit)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(1))
			Expect(res.Remaining).To(Equal(9))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 100*time.Millisecond, 30*time.Millisecond))

			// 2. Consume 2 tokens via AllowAtMost
			res, err = limiter.AllowAtMost(ctx, testID, limit, 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(2))
			Expect(res.Remaining).To(Equal(7))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 300*time.Millisecond, 40*time.Millisecond))

			// 3. Peek with AllowN(0)
			res, err = limiter.AllowN(ctx, testID, limit, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(0))
			Expect(res.Remaining).To(Equal(7))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 300*time.Millisecond, 40*time.Millisecond))

			// 4. Request 10 with 7 remaining -> grants 7!
			res, err = limiter.AllowAtMost(ctx, testID, limit, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(7))
			Expect(res.Remaining).To(Equal(0))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 999*time.Millisecond, 60*time.Millisecond))

			// 5. Peek with AllowN(0)
			res, err = limiter.AllowN(ctx, testID, limit, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(0))
			Expect(res.Remaining).To(Equal(0))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 999*time.Millisecond, 60*time.Millisecond))

			// 6. Request 1000 with 0 remaining via AllowAtMost -> grants 0, RetryAfter ~99ms
			res, err = limiter.AllowAtMost(ctx, testID, limit, 1000)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(0))
			Expect(res.Remaining).To(Equal(0))
			Expect(res.RetryAfter).To(BeNumerically("~", 99*time.Millisecond, 30*time.Millisecond))
			Expect(res.ResetAfter).To(BeNumerically("~", 999*time.Millisecond, 60*time.Millisecond))

			// 7. Request 1000 with 0 remaining via AllowN -> rejected, RetryAfter ~99s
			res, err = limiter.AllowN(ctx, testID, limit, 1000)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(0))
			Expect(res.Remaining).To(Equal(0))
			Expect(res.RetryAfter).To(BeNumerically("~", 99*time.Second, 2*time.Second))
			Expect(res.ResetAfter).To(BeNumerically("~", 999*time.Millisecond, 60*time.Millisecond))
		})

		It("peeks without consumption when increment is zero in AllowAtMost", func() {
			limit := redisrate.PerSecond(10)

			// Non-existent key
			res, err := limiter.AllowAtMost(ctx, testID, limit, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(0))
			Expect(res.Remaining).To(Equal(10))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(Equal(time.Duration(0)))

			// Consume 1 token
			res, err = limiter.Allow(ctx, testID, limit)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(1))
			Expect(res.Remaining).To(Equal(9))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 100*time.Millisecond, 30*time.Millisecond))

			// Peek again
			res, err = limiter.AllowAtMost(ctx, testID, limit, 0)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(Equal(0))
			Expect(res.Remaining).To(Equal(9))
			Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(res.ResetAfter).To(BeNumerically("~", 100*time.Millisecond, 30*time.Millisecond))
		})
	})
})

func BenchmarkAllow(b *testing.B) {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6378"}})
	if err != nil {
		b.Skipf("cannot connect to 127.0.0.1:6378: %v", err)
	}
	defer client.Close()

	l := redisrate.NewLimiterFromClient(client)
	ctx := context.Background()
	limit := redisrate.PerSecond(1e6)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res, err := l.Allow(ctx, "bench_allow", limit)
		if err != nil {
			b.Fatal(err)
		}
		if res.Allowed == 0 {
			b.Fatal("rate limit exceeded during benchmark")
		}
	}
}

func BenchmarkAllowAtMost(b *testing.B) {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6378"}})
	if err != nil {
		b.Skipf("cannot connect to 127.0.0.1:6378: %v", err)
	}
	defer client.Close()

	l := redisrate.NewLimiterFromClient(client)
	ctx := context.Background()
	limit := redisrate.PerSecond(1e6)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		res, err := l.AllowAtMost(ctx, "bench_atmost", limit, 1)
		if err != nil {
			b.Fatal(err)
		}
		if res.Allowed == 0 {
			b.Fatal("rate limit exceeded during benchmark")
		}
	}
}
