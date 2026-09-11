package valkeylimiter_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"github.com/valkey-io/valkey-go/valkeylimiter"
	"go.uber.org/mock/gomock"
)

func TestValkeyLimiter(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "ValkeyLimiter Suite")
}

func getLiveRateLimiter(limit int, window time.Duration, burst ...int) valkeylimiter.RateLimiterClient {
	b := limit
	if len(burst) > 0 && burst[0] > 0 {
		b = burst[0]
	}
	for _, addr := range []string{"127.0.0.1:6379", "127.0.0.1:6378"} {
		limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
			ClientOption: valkey.ClientOption{
				InitAddress: []string{addr},
			},
			Limit:  limit,
			Window: window,
			Burst:  b,
		})
		if err == nil {
			res, err := limiter.Check(context.Background(), "live_probe")
			if err == nil && (res.Allowed || res.Remaining >= 0) {
				return limiter
			}
			limiter.Close()
		}
	}
	return nil
}

func getClusterRateLimiter(limit int, window time.Duration, burst ...int) valkeylimiter.RateLimiterClient {
	b := limit
	if len(burst) > 0 && burst[0] > 0 {
		b = burst[0]
	}
	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientOption: valkey.ClientOption{
			InitAddress: []string{"127.0.0.1:7010"},
		},
		Limit:  limit,
		Window: window,
		Burst:  b,
	})
	if err != nil {
		return nil
	}
	res, err := limiter.Check(context.Background(), "cluster_probe")
	if err != nil {
		limiter.Close()
		return nil
	}
	_ = res
	return limiter
}

var _ = Describe("RateLimiter", func() {
	Describe("NewRateLimiter Constructor", func() {
		It("initializes with default values", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return mock.NewClient(ctrl), nil
				},
				Limit:  1,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(limiter).NotTo(BeNil())
		})

		It("initializes with custom values", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return mock.NewClient(ctrl), nil
				},
				Limit:     100,
				Window:    time.Second,
				KeyPrefix: "test:",
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(limiter).NotTo(BeNil())
		})

		It("returns ErrInvalidWindow on negative window", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			_, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return mock.NewClient(ctrl), nil
				},
				Limit:  1,
				Window: -time.Second,
			})
			Expect(errors.Is(err, valkeylimiter.ErrInvalidWindow)).To(BeTrue())
		})

		It("returns ErrInvalidLimit on negative limit", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			_, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return mock.NewClient(ctrl), nil
				},
				Limit:  -1,
				Window: time.Second,
			})
			Expect(errors.Is(err, valkeylimiter.ErrInvalidLimit)).To(BeTrue())
		})

		It("handles empty key prefix", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return mock.NewClient(ctrl), nil
				},
				Limit:  1,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(limiter).NotTo(BeNil())
		})

		It("handles nil client builder by using ClientOption", func() {
			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientOption: valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
				Limit:        1,
				Window:       time.Second,
			})
			Expect(err).NotTo(HaveOccurred())
			defer limiter.Close()
		})

		It("propagates client builder error", func() {
			_, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return nil, errors.New("client error")
				},
				Limit:  1,
				Window: time.Second,
			})
			Expect(err).To(MatchError("client error"))
		})
	})

	Describe("Fixed Window Algorithm (Opt-in)", func() {
		var (
			ctrl   *gomock.Controller
			client *mock.Client
		)

		BeforeEach(func() {
			ctrl = gomock.NewController(GinkgoT())
			client = mock.NewClient(ctrl)
		})

		AfterEach(func() {
			ctrl.Finish()
		})

		It("rejects negative tokens in AllowN", func() {
			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Algorithm: valkeylimiter.AlgorithmFixedWindow,
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = limiter.AllowN(context.Background(), "test", -1)
			Expect(err).To(HaveOccurred())
		})

		It("allows requests with default limit", func() {
			resetTime := time.Now().Add(time.Second).UnixMilli()
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(1),
				mock.ValkeyInt64(resetTime),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Algorithm: valkeylimiter.AlgorithmFixedWindow,
			})
			Expect(err).NotTo(HaveOccurred())

			res, err := limiter.AllowN(context.Background(), "test", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(BeTrue())
			Expect(res.Remaining).To(Equal(int64(9)))
			Expect(res.ResetAtMs).To(Equal(resetTime))
		})

		It("allows requests with custom limit option", func() {
			resetTime := time.Now().Add(time.Second).UnixMilli()
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(5),
				mock.ValkeyInt64(resetTime),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Algorithm: valkeylimiter.AlgorithmFixedWindow,
			})
			Expect(err).NotTo(HaveOccurred())

			opt := valkeylimiter.WithCustomRateLimit(20, time.Second*2)
			res, err := limiter.AllowN(context.Background(), "test", 1, opt)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(BeTrue())
			Expect(res.Remaining).To(Equal(int64(15)))
			Expect(res.ResetAtMs).To(Equal(resetTime))
		})

		It("rejects requests when limit is exceeded", func() {
			resetTime := time.Now().Add(time.Second).UnixMilli()
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(11),
				mock.ValkeyInt64(resetTime),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Algorithm: valkeylimiter.AlgorithmFixedWindow,
			})
			Expect(err).NotTo(HaveOccurred())

			res, err := limiter.AllowN(context.Background(), "test", 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(BeFalse())
			Expect(res.Remaining).To(Equal(int64(0)))
			Expect(res.ResetAtMs).To(Equal(resetTime))
		})

		It("handles client and invalid response errors in AllowN", func() {
			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Algorithm: valkeylimiter.AlgorithmFixedWindow,
			})
			Expect(err).NotTo(HaveOccurred())

			// Redis error
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.ErrorResult(errors.New("redis error"))).Times(1)
			_, err = limiter.AllowN(context.Background(), "test", 1)
			Expect(err).To(HaveOccurred())

			// Invalid response type
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyString("invalid"))).Times(1)
			_, err = limiter.AllowN(context.Background(), "test", 1)
			Expect(err).To(HaveOccurred())

			// Invalid array length
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(mock.ValkeyInt64(1)))).Times(1)
			_, err = limiter.AllowN(context.Background(), "test", 1)
			Expect(err).To(HaveOccurred())

			// Invalid first element
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyString("invalid"),
				mock.ValkeyInt64(1),
			))).Times(1)
			_, err = limiter.AllowN(context.Background(), "test", 1)
			Expect(err).To(HaveOccurred())

			// Invalid second element
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(1),
				mock.ValkeyString("invalid"),
			))).Times(1)
			_, err = limiter.AllowN(context.Background(), "test", 1)
			Expect(err).To(HaveOccurred())
		})

		It("checks without incrementing in Check", func() {
			resetTime := time.Now().Add(time.Second).UnixMilli()
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(5),
				mock.ValkeyInt64(resetTime),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Algorithm: valkeylimiter.AlgorithmFixedWindow,
			})
			Expect(err).NotTo(HaveOccurred())

			res, err := limiter.Check(context.Background(), "test")
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(BeTrue())
			Expect(res.Remaining).To(Equal(int64(5)))
			Expect(res.ResetAtMs).To(Equal(resetTime))
		})

		It("increments single token in Allow", func() {
			resetTime := time.Now().Add(time.Second).UnixMilli()
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(1),
				mock.ValkeyInt64(resetTime),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Algorithm: valkeylimiter.AlgorithmFixedWindow,
			})
			Expect(err).NotTo(HaveOccurred())

			res, err := limiter.Allow(context.Background(), "test")
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(BeTrue())
			Expect(res.Remaining).To(Equal(int64(9)))
			Expect(res.ResetAtMs).To(Equal(resetTime))
		})

		It("returns the configured limit", func() {
			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:  42,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())
			Expect(limiter.Limit()).To(Equal(42))
		})

		It("supports Close", func() {
			client.EXPECT().Close().Times(1)
			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:  10,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())
			limiter.Close()
		})

		It("resets keys in Reset", func() {
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyInt64(2))).Times(1)
			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Algorithm: valkeylimiter.AlgorithmFixedWindow,
			})
			Expect(err).NotTo(HaveOccurred())

			err = limiter.Reset(context.Background(), "test_id")
			Expect(err).NotTo(HaveOccurred())
		})

		It("performs partial grant in AllowAtMost", func() {
			resetTime := time.Now().Add(time.Second).UnixMilli()
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(6),
				mock.ValkeyInt64(resetTime),
			))).Times(1)
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(10),
				mock.ValkeyInt64(resetTime),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Algorithm: valkeylimiter.AlgorithmFixedWindow,
			})
			Expect(err).NotTo(HaveOccurred())

			got, err := limiter.AllowAtMost(context.Background(), "test", 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Allowed).To(BeTrue())
			Expect(got.Granted).To(Equal(int64(4)))
			Expect(got.Remaining).To(Equal(int64(0)))
		})

		It("works with Dragonfly if available", func() {
			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientOption: valkey.ClientOption{
					InitAddress: []string{"127.0.0.1:6333"},
				},
				KeyPrefix: "dragonfly_allow_n_test",
				Limit:     2,
				Window:    time.Second,
			})
			if err != nil {
				Skip("cannot connect to Dragonfly on 127.0.0.1:6333")
			}
			defer limiter.Close()

			id := fmt.Sprintf("id-%d", time.Now().UnixNano())
			res, err := limiter.AllowN(context.Background(), id, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(BeTrue())
			Expect(res.Remaining).To(Equal(int64(1)))

			res, err = limiter.AllowN(context.Background(), id, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(BeTrue())
			Expect(res.Remaining).To(Equal(int64(0)))

			res, err = limiter.AllowN(context.Background(), id, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(res.Allowed).To(BeFalse())
		})
	})

	Describe("GCRA Response Parsers", func() {
		DescribeTable("DurFromSecString",
			func(input string, want time.Duration, wantErr bool) {
				got, err := valkeylimiter.DurFromSecString(input)
				if wantErr {
					Expect(err).To(HaveOccurred())
				} else {
					Expect(err).NotTo(HaveOccurred())
					Expect(got).To(Equal(want))
				}
			},
			Entry("negative one represents no retry", "-1", time.Duration(-1), false),
			Entry("zero seconds", "0", time.Duration(0), false),
			Entry("fractional seconds 100ms", "0.1", 100*time.Millisecond, false),
			Entry("one second", "1.0", time.Second, false),
			Entry("multi second float", "2.5", 2500*time.Millisecond, false),
			Entry("negative number other than -1", "-0.5", time.Duration(0), false),
			Entry("invalid float string", "not-a-number", time.Duration(0), true),
		)

		Describe("ParseGCRAResponse", func() {
			It("parses success allowed response", func() {
				resp := mock.Result(mock.ValkeyArray(
					mock.ValkeyInt64(1),
					mock.ValkeyInt64(9),
					mock.ValkeyString("-1"),
					mock.ValkeyString("0.1"),
				))
				got, err := valkeylimiter.ParseGCRAResponse(resp)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(valkeylimiter.GCRAResult{
					Allowed:    1,
					Remaining:  9,
					RetryAfter: -1,
					ResetAfter: 100 * time.Millisecond,
				}))
			})

			It("parses success rejected response", func() {
				resp := mock.Result(mock.ValkeyArray(
					mock.ValkeyInt64(0),
					mock.ValkeyInt64(0),
					mock.ValkeyString("0.5"),
					mock.ValkeyString("1.0"),
				))
				got, err := valkeylimiter.ParseGCRAResponse(resp)
				Expect(err).NotTo(HaveOccurred())
				Expect(got).To(Equal(valkeylimiter.GCRAResult{
					Allowed:    0,
					Remaining:  0,
					RetryAfter: 500 * time.Millisecond,
					ResetAfter: time.Second,
				}))
			})

			It("handles valkey error", func() {
				resp := mock.ErrorResult(errors.New("valkey cluster error"))
				_, err := valkeylimiter.ParseGCRAResponse(resp)
				Expect(err).To(HaveOccurred())
			})

			It("rejects invalid response structures", func() {
				// non-array
				_, err := valkeylimiter.ParseGCRAResponse(mock.Result(mock.ValkeyString("unexpected string")))
				Expect(errors.Is(err, valkeylimiter.ErrInvalidResponse)).To(BeTrue())

				// array length < 4
				_, err = valkeylimiter.ParseGCRAResponse(mock.Result(mock.ValkeyArray(
					mock.ValkeyInt64(1), mock.ValkeyInt64(9), mock.ValkeyString("-1"),
				)))
				Expect(errors.Is(err, valkeylimiter.ErrInvalidResponse)).To(BeTrue())

				// array length > 4
				_, err = valkeylimiter.ParseGCRAResponse(mock.Result(mock.ValkeyArray(
					mock.ValkeyInt64(1), mock.ValkeyInt64(9), mock.ValkeyString("-1"), mock.ValkeyString("0.1"), mock.ValkeyString("extra"),
				)))
				Expect(errors.Is(err, valkeylimiter.ErrInvalidResponse)).To(BeTrue())

				// invalid first element
				_, err = valkeylimiter.ParseGCRAResponse(mock.Result(mock.ValkeyArray(
					mock.ValkeyString("not-an-int"), mock.ValkeyInt64(9), mock.ValkeyString("-1"), mock.ValkeyString("0.1"),
				)))
				Expect(errors.Is(err, valkeylimiter.ErrInvalidResponse)).To(BeTrue())

				// invalid second element
				_, err = valkeylimiter.ParseGCRAResponse(mock.Result(mock.ValkeyArray(
					mock.ValkeyInt64(1), mock.ValkeyString("not-an-int"), mock.ValkeyString("-1"), mock.ValkeyString("0.1"),
				)))
				Expect(errors.Is(err, valkeylimiter.ErrInvalidResponse)).To(BeTrue())

				// invalid retry_after float
				_, err = valkeylimiter.ParseGCRAResponse(mock.Result(mock.ValkeyArray(
					mock.ValkeyInt64(1), mock.ValkeyInt64(9), mock.ValkeyString("invalid-float"), mock.ValkeyString("0.1"),
				)))
				Expect(errors.Is(err, valkeylimiter.ErrInvalidResponse)).To(BeTrue())

				// invalid reset_after float
				_, err = valkeylimiter.ParseGCRAResponse(mock.Result(mock.ValkeyArray(
					mock.ValkeyInt64(1), mock.ValkeyInt64(9), mock.ValkeyString("-1"), mock.ValkeyString("invalid-float"),
				)))
				Expect(errors.Is(err, valkeylimiter.ErrInvalidResponse)).To(BeTrue())
			})
		})

		It("executes ExecGCRAAllowN mock", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			client := mock.NewClient(ctrl)
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(2),
				mock.ValkeyInt64(8),
				mock.ValkeyString("-1"),
				mock.ValkeyString("0.2"),
			))).Times(1)

			got, err := valkeylimiter.ExecGCRAAllowN(context.Background(), client, "rate:{user:1}", 10, 10, time.Second, 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(valkeylimiter.GCRAResult{
				Allowed:    2,
				Remaining:  8,
				RetryAfter: -1,
				ResetAfter: 200 * time.Millisecond,
			}))
		})

		It("executes ExecGCRAAllowAtMost mock", func() {
			ctrl := gomock.NewController(GinkgoT())
			defer ctrl.Finish()

			client := mock.NewClient(ctrl)
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(4),
				mock.ValkeyInt64(0),
				mock.ValkeyString("-1"),
				mock.ValkeyString("0.6"),
			))).Times(1)

			got, err := valkeylimiter.ExecGCRAAllowAtMost(context.Background(), client, "rate:{user:2}", 10, 10, time.Second, 5)
			Expect(err).NotTo(HaveOccurred())
			Expect(got).To(Equal(valkeylimiter.GCRAResult{
				Allowed:    4,
				Remaining:  0,
				RetryAfter: -1,
				ResetAfter: 600 * time.Millisecond,
			}))
		})

		It("executes GCRA operations on live Valkey server", func() {
			var client valkey.Client
			for _, addr := range []string{"127.0.0.1:6379", "127.0.0.1:6378"} {
				c, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{addr}})
				if err == nil {
					if err := c.Do(context.Background(), c.B().Ping().Build()).Error(); err == nil {
						client = c
						break
					}
					c.Close()
				}
			}
			if client == nil {
				Skip("cannot connect to live Valkey/Redis instance")
			}
			defer client.Close()

			ctx := context.Background()
			testKey := fmt.Sprintf("test:gcra:live:%d", time.Now().UnixNano())
			defer func() {
				_ = client.Do(ctx, client.B().Del().Key(testKey).Build()).Error()
			}()

			res1, err := valkeylimiter.ExecGCRAAllowN(ctx, client, testKey, 10, 10, time.Second, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(res1.Allowed).To(Equal(int64(1)))
			Expect(res1.Remaining).To(Equal(int64(9)))
			Expect(res1.RetryAfter).To(Equal(time.Duration(-1)))

			res2, err := valkeylimiter.ExecGCRAAllowN(ctx, client, testKey, 10, 10, time.Second, 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(res2.Allowed).To(Equal(int64(2)))
			Expect(res2.Remaining).To(Equal(int64(7)))
			Expect(res2.RetryAfter).To(Equal(time.Duration(-1)))

			res3, err := valkeylimiter.ExecGCRAAllowN(ctx, client, testKey, 10, 10, time.Second, 100)
			Expect(err).NotTo(HaveOccurred())
			Expect(res3.Allowed).To(Equal(int64(0)))
			Expect(res3.Remaining).To(Equal(int64(0)))
			Expect(res3.RetryAfter).To(BeNumerically(">", 0))

			res4, err := valkeylimiter.ExecGCRAAllowAtMost(ctx, client, testKey, 10, 10, time.Second, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(res4.Allowed).To(Equal(int64(7)))
			Expect(res4.Remaining).To(Equal(int64(0)))
			Expect(res4.RetryAfter).To(Equal(time.Duration(-1)))

			res5, err := valkeylimiter.ExecGCRAAllowAtMost(ctx, client, testKey, 10, 10, time.Second, 1)
			Expect(err).NotTo(HaveOccurred())
			Expect(res5.Allowed).To(Equal(int64(0)))
			Expect(res5.Remaining).To(Equal(int64(0)))
			Expect(res5.RetryAfter).To(BeNumerically(">", 0))
		})
	})

	Describe("GCRA RateLimiterClient APIs", func() {
		var (
			ctrl   *gomock.Controller
			client *mock.Client
		)

		BeforeEach(func() {
			ctrl = gomock.NewController(GinkgoT())
			client = mock.NewClient(ctrl)
		})

		AfterEach(func() {
			ctrl.Finish()
		})

		It("defaults to GCRA and allows requests", func() {
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(1),
				mock.ValkeyInt64(9),
				mock.ValkeyString("-1"),
				mock.ValkeyString("0.1"),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:  10,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())

			got, err := limiter.Allow(context.Background(), "user1")
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Allowed).To(BeTrue())
			Expect(got.Remaining).To(Equal(int64(9)))
			Expect(got.Granted).To(Equal(int64(1)))
			Expect(got.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(got.ResetAfter).To(Equal(100 * time.Millisecond))
			Expect(got.ResetAtMs).To(BeNumerically(">", 0))
		})

		It("executes AllowN with multi-token deduction", func() {
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(3),
				mock.ValkeyInt64(7),
				mock.ValkeyString("-1"),
				mock.ValkeyString("0.3"),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Burst:     10,
				Algorithm: valkeylimiter.AlgorithmGCRA,
			})
			Expect(err).NotTo(HaveOccurred())

			got, err := limiter.AllowN(context.Background(), "user2", 3)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Allowed).To(BeTrue())
			Expect(got.Remaining).To(Equal(int64(7)))
			Expect(got.Granted).To(Equal(int64(3)))
			Expect(got.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(got.ResetAfter).To(Equal(300 * time.Millisecond))
		})

		It("executes AllowAtMost with capacity cap", func() {
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(7),
				mock.ValkeyInt64(0),
				mock.ValkeyString("-1"),
				mock.ValkeyString("1.0"),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:  10,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())

			got, err := limiter.AllowAtMost(context.Background(), "user3", 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Allowed).To(BeTrue())
			Expect(got.Remaining).To(Equal(int64(0)))
			Expect(got.Granted).To(Equal(int64(7)))
			Expect(got.RetryAfter).To(Equal(time.Duration(-1)))
			Expect(got.ResetAfter).To(Equal(time.Second))
		})

		It("peeks capacity via Check without consuming", func() {
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(0),
				mock.ValkeyInt64(5),
				mock.ValkeyString("-1"),
				mock.ValkeyString("0.5"),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:  10,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())

			got, err := limiter.Check(context.Background(), "user4")
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Allowed).To(BeTrue())
			Expect(got.Remaining).To(Equal(int64(5)))
			Expect(got.Granted).To(Equal(int64(0)))
			Expect(got.RetryAfter).To(Equal(time.Duration(-1)))
		})

		It("clears state via Reset", func() {
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyInt64(1))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:  10,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())

			err = limiter.Reset(context.Background(), "test_id")
			Expect(err).NotTo(HaveOccurred())
		})

		It("handles option overrides WithBurst and WithCustomRateLimitAndBurst", func() {
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(1),
				mock.ValkeyInt64(29),
				mock.ValkeyString("-1"),
				mock.ValkeyString("0.1"),
			))).Times(1)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:  10,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())

			opt := valkeylimiter.WithCustomRateLimitAndBurst(20, 2*time.Second, 30)
			got, err := limiter.Allow(context.Background(), "user-custom", opt)
			Expect(err).NotTo(HaveOccurred())
			Expect(got.Allowed).To(BeTrue())
			Expect(got.Remaining).To(Equal(int64(29)))

			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(1),
				mock.ValkeyInt64(49),
				mock.ValkeyString("-1"),
				mock.ValkeyString("0.1"),
			))).Times(1)

			gotBurst, err := limiter.Allow(context.Background(), "user-burst", valkeylimiter.WithBurst(50))
			Expect(err).NotTo(HaveOccurred())
			Expect(gotBurst.Allowed).To(BeTrue())
			Expect(gotBurst.Remaining).To(Equal(int64(49)))
		})

		It("validates parameters in AllowAtMost", func() {
			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:  10,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = limiter.AllowAtMost(context.Background(), "user-neg", -5)
			Expect(errors.Is(err, valkeylimiter.ErrInvalidTokens)).To(BeTrue())
		})

		It("propagates client errors", func() {
			client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.ErrorResult(errors.New("connection failed"))).Times(3)

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:  10,
				Window: time.Second,
			})
			Expect(err).NotTo(HaveOccurred())

			_, err = limiter.Allow(context.Background(), "user-err")
			Expect(err).To(HaveOccurred())

			_, err = limiter.AllowAtMost(context.Background(), "user-err", 5)
			Expect(err).To(HaveOccurred())

			err = limiter.Reset(context.Background(), "user-err")
			Expect(err).To(HaveOccurred())
		})

		It("executes full lifecycle on live server", func() {
			limiter := getLiveRateLimiter(10, time.Second)
			if limiter == nil {
				Skip("cannot connect to live Valkey/Redis instance")
			}
			defer limiter.Close()

			ctx := context.Background()
			testId := fmt.Sprintf("user-live-%d", time.Now().UnixNano())
			defer func() { _ = limiter.Reset(ctx, testId) }()

			// 1. Allow single token
			res1, err := limiter.Allow(ctx, testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(res1.Allowed).To(BeTrue())
			Expect(res1.Remaining).To(Equal(int64(9)))
			Expect(res1.Granted).To(Equal(int64(1)))

			// 2. AllowN 2 tokens
			res2, err := limiter.AllowN(ctx, testId, 2)
			Expect(err).NotTo(HaveOccurred())
			Expect(res2.Allowed).To(BeTrue())
			Expect(res2.Remaining).To(Equal(int64(7)))
			Expect(res2.Granted).To(Equal(int64(2)))

			// 3. AllowAtMost requesting 10 with 7 remaining -> grants 7
			res3, err := limiter.AllowAtMost(ctx, testId, 10)
			Expect(err).NotTo(HaveOccurred())
			Expect(res3.Allowed).To(BeTrue())
			Expect(res3.Remaining).To(Equal(int64(0)))
			Expect(res3.Granted).To(Equal(int64(7)))

			// 4. Next call rejected
			res4, err := limiter.Allow(ctx, testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(res4.Allowed).To(BeFalse())
			Expect(res4.Remaining).To(Equal(int64(0)))
			Expect(res4.RetryAfter).To(BeNumerically(">", 0))

			// 5. Reset
			err = limiter.Reset(ctx, testId)
			Expect(err).NotTo(HaveOccurred())

			// 6. Request after reset succeeds
			res5, err := limiter.Allow(ctx, testId)
			Expect(err).NotTo(HaveOccurred())
			Expect(res5.Allowed).To(BeTrue())
			Expect(res5.Remaining).To(Equal(int64(9)))
			Expect(res5.Granted).To(Equal(int64(1)))
		})
	})

	Describe("High Concurrency Contention", func() {
		It("preserves atomicity under 50 concurrent workers", func() {
			limiter := getLiveRateLimiter(10, 10*time.Second, 10)
			if limiter == nil {
				Skip("cannot connect to live Valkey/Redis instance")
			}
			defer limiter.Close()

			ctx := context.Background()
			testId := fmt.Sprintf("concurrent-test-50-%d", time.Now().UnixNano())
			defer func() { _ = limiter.Reset(ctx, testId) }()

			const totalWorkers = 50
			var allowedCount int64
			var rejectedCount int64

			startBarrier := make(chan struct{})
			doneCh := make(chan struct{}, totalWorkers)

			for i := 0; i < totalWorkers; i++ {
				go func() {
					<-startBarrier
					res, err := limiter.Allow(ctx, testId)
					if err == nil {
						if res.Allowed {
							atomic.AddInt64(&allowedCount, 1)
						} else {
							atomic.AddInt64(&rejectedCount, 1)
						}
					}
					doneCh <- struct{}{}
				}()
			}

			close(startBarrier)
			for i := 0; i < totalWorkers; i++ {
				<-doneCh
			}

			Expect(allowedCount).To(Equal(int64(10)))
			Expect(rejectedCount).To(Equal(int64(40)))
		})

		It("accurately throttles 100 simultaneous concurrent workers", func() {
			limiter := getLiveRateLimiter(20, 10*time.Second, 20)
			if limiter == nil {
				Skip("cannot connect to live Valkey/Redis instance")
			}
			defer limiter.Close()

			ctx := context.Background()
			testID := fmt.Sprintf("gcra-high-concurrency-100-%d", time.Now().UnixNano())
			defer func() { _ = limiter.Reset(ctx, testID) }()

			const numWorkers = 100
			var allowedCount int64
			var rejectedCount int64

			startBarrier := make(chan struct{})
			doneCh := make(chan struct{}, numWorkers)

			for i := 0; i < numWorkers; i++ {
				go func() {
					<-startBarrier
					res, err := limiter.Allow(ctx, testID)
					if err == nil {
						if res.Allowed {
							atomic.AddInt64(&allowedCount, 1)
							Expect(res.Granted).To(Equal(int64(1)))
							Expect(res.RetryAfter).To(Equal(time.Duration(-1)))
						} else {
							atomic.AddInt64(&rejectedCount, 1)
							Expect(res.Granted).To(Equal(int64(0)))
							Expect(res.RetryAfter).To(BeNumerically(">", 0))
						}
					}
					doneCh <- struct{}{}
				}()
			}

			close(startBarrier)
			for i := 0; i < numWorkers; i++ {
				<-doneCh
			}

			Expect(allowedCount).To(Equal(int64(20)))
			Expect(rejectedCount).To(Equal(int64(80)))
		})
	})

	Describe("Valkey Cluster Integration", func() {
		It("operates across cluster nodes and slots without CROSSSLOT errors", func() {
			limiter := getClusterRateLimiter(10, time.Second, 10)
			if limiter == nil {
				Skip("cannot connect to Valkey Cluster on 127.0.0.1:7010")
			}
			defer limiter.Close()

			ctx := context.Background()
			testKeys := []string{
				fmt.Sprintf("plain_key_a_%d", time.Now().UnixNano()),
				fmt.Sprintf("plain_key_b_%d", time.Now().UnixNano()),
				fmt.Sprintf("{tenant_alpha}:user_%d", time.Now().UnixNano()),
				fmt.Sprintf("{tenant_beta}:user_%d", time.Now().UnixNano()),
				fmt.Sprintf("{tenant_gamma}:user_%d", time.Now().UnixNano()),
			}

			for _, key := range testKeys {
				defer func(k string) { _ = limiter.Reset(ctx, k) }(key)

				// 1. Check
				chk, err := limiter.Check(ctx, key)
				Expect(err).NotTo(HaveOccurred())
				Expect(chk.Allowed).To(BeTrue())
				Expect(chk.Remaining).To(Equal(int64(10)))

				// 2. Allow 1
				res, err := limiter.Allow(ctx, key)
				Expect(err).NotTo(HaveOccurred())
				Expect(res.Allowed).To(BeTrue())
				Expect(res.Remaining).To(Equal(int64(9)))
				Expect(res.Granted).To(Equal(int64(1)))
				Expect(res.RetryAfter).To(Equal(time.Duration(-1)))

				// 3. AllowN 3
				resN, err := limiter.AllowN(ctx, key, 3)
				Expect(err).NotTo(HaveOccurred())
				Expect(resN.Allowed).To(BeTrue())
				Expect(resN.Remaining).To(Equal(int64(6)))
				Expect(resN.Granted).To(Equal(int64(3)))
				Expect(resN.RetryAfter).To(Equal(time.Duration(-1)))

				// 4. AllowAtMost 10 (grants remaining 6)
				resAtMost, err := limiter.AllowAtMost(ctx, key, 10)
				Expect(err).NotTo(HaveOccurred())
				Expect(resAtMost.Allowed).To(BeTrue())
				Expect(resAtMost.Remaining).To(Equal(int64(0)))
				Expect(resAtMost.Granted).To(Equal(int64(6)))
				Expect(resAtMost.RetryAfter).To(Equal(time.Duration(-1)))

				// 5. Throttled
				resRejected, err := limiter.Allow(ctx, key)
				Expect(err).NotTo(HaveOccurred())
				Expect(resRejected.Allowed).To(BeFalse())
				Expect(resRejected.Remaining).To(Equal(int64(0)))
				Expect(resRejected.RetryAfter).To(BeNumerically(">", 0))

				// 6. Reset
				err = limiter.Reset(ctx, key)
				Expect(err).NotTo(HaveOccurred())

				resRestored, err := limiter.Allow(ctx, key)
				Expect(err).NotTo(HaveOccurred())
				Expect(resRestored.Allowed).To(BeTrue())
				Expect(resRestored.Remaining).To(Equal(int64(9)))
			}
		})

		It("handles hash tags across distinct slots with zero CROSSSLOT errors", func() {
			limiter := getClusterRateLimiter(50, 10*time.Second, 50)
			if limiter == nil {
				Skip("cannot connect to Valkey Cluster on 127.0.0.1:7010")
			}
			defer limiter.Close()

			ctx := context.Background()
			hashTags := []string{"{tenant_1}", "{tenant_2}", "{tenant_3}", "{tenant_4}", "{tenant_5}"}

			for _, tag := range hashTags {
				id := fmt.Sprintf("%s:metric:%d", tag, time.Now().UnixNano())
				defer func(k string) { _ = limiter.Reset(ctx, k) }(id)

				res, err := limiter.Allow(ctx, id)
				Expect(err).NotTo(HaveOccurred())
				Expect(res.Allowed).To(BeTrue())

				partRes, err := limiter.AllowAtMost(ctx, id, 5)
				Expect(err).NotTo(HaveOccurred())
				Expect(partRes.Allowed).To(BeTrue())
				Expect(partRes.Granted).To(Equal(int64(5)))
			}
		})
	})
})

func BenchmarkAllowN(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	now := time.Now()
	resetTime := now.Add(time.Second).UnixMilli()

	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(1),
		mock.ValkeyInt64(resetTime),
	))).Times(b.N)

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:     1000,
		Window:    time.Second,
		Algorithm: valkeylimiter.AlgorithmFixedWindow,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := limiter.AllowN(context.Background(), "test", 1)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllowN_GCRA(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(1),
		mock.ValkeyInt64(999),
		mock.ValkeyString("-1"),
		mock.ValkeyString("0.001"),
	))).Times(b.N)

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:     1000,
		Window:    time.Second,
		Algorithm: valkeylimiter.AlgorithmGCRA,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := limiter.AllowN(context.Background(), "test", 1)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllowAtMost_GCRA(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(5),
		mock.ValkeyInt64(995),
		mock.ValkeyString("-1"),
		mock.ValkeyString("0.005"),
	))).Times(b.N)

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:     1000,
		Window:    time.Second,
		Algorithm: valkeylimiter.AlgorithmGCRA,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := limiter.AllowAtMost(context.Background(), "test", 5)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkAllow_Parallel_GCRA(b *testing.B) {
	ctrl := gomock.NewController(b)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(1),
		mock.ValkeyInt64(999),
		mock.ValkeyString("-1"),
		mock.ValkeyString("0.001"),
	))).AnyTimes()

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:     1000,
		Window:    time.Second,
		Algorithm: valkeylimiter.AlgorithmGCRA,
	})
	if err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := limiter.Allow(context.Background(), "test")
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
