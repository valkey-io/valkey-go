package valkeylimiter_test

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"github.com/valkey-io/valkey-go/valkeylimiter"
	"go.uber.org/mock/gomock"
)

func TestNewRateLimiter(t *testing.T) {
	tests := []struct {
		name    string
		opt     valkeylimiter.RateLimiterOption
		wantErr error
	}{
		{
			name: "default values",
			opt: valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return mock.NewClient(gomock.NewController(t)), nil
				},
				Limit:  1,
				Window: time.Second,
			},
		},
		{
			name: "custom values",
			opt: valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return mock.NewClient(gomock.NewController(t)), nil
				},
				Limit:     100,
				Window:    time.Second,
				KeyPrefix: "test:",
			},
		},
		{
			name: "invalid window",
			opt: valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return mock.NewClient(gomock.NewController(t)), nil
				},
				Limit:  1,
				Window: -time.Second,
			},
			wantErr: valkeylimiter.ErrInvalidWindow,
		},
		{
			name: "invalid limit",
			opt: valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return mock.NewClient(gomock.NewController(t)), nil
				},
				Limit:  -1,
				Window: time.Second,
			},
			wantErr: valkeylimiter.ErrInvalidLimit,
		},
		{
			name: "empty key prefix",
			opt: valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return mock.NewClient(gomock.NewController(t)), nil
				},
				Limit:  1,
				Window: time.Second,
			},
		},
		{
			name: "nil client builder",
			opt: valkeylimiter.RateLimiterOption{
				ClientOption: valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
				Limit:        1,
				Window:       time.Second,
			},
		},
		{
			name: "new client error",
			opt: valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return nil, errors.New("client error")
				},
				Limit:  1,
				Window: time.Second,
			},
			wantErr: errors.New("client error"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := valkeylimiter.NewRateLimiter(tt.opt)
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("NewRateLimiter() error = nil, wantErr %v", tt.wantErr)
				}
				if err.Error() != tt.wantErr.Error() {
					t.Fatalf("NewRateLimiter() error = %v, wantErr %v", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("NewRateLimiter() error = %v, wantErr nil", err)
			}
		})
	}
}

func TestRateLimiter_AllowN(t *testing.T) {
	now := time.Now()
	resetTime := now.Add(time.Second).UnixMilli()

	tests := []struct {
		name       string
		mockResp   valkey.ValkeyResult
		n          int64
		customOpt  *valkeylimiter.RateLimitOption
		wantResult valkeylimiter.Result
		wantErr    bool
		setupMock  bool
	}{
		{
			name:    "negative tokens",
			n:       -1,
			wantErr: true,
		},
		{
			name: "success with default limit",
			mockResp: mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(1),
				mock.ValkeyInt64(resetTime),
			)),
			n:         1,
			setupMock: true,
			wantResult: valkeylimiter.Result{
				Allowed:   true,
				Remaining: 9,
				ResetAtMs: resetTime,
			},
		},
		{
			name: "success with custom limit",
			mockResp: mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(5),
				mock.ValkeyInt64(resetTime),
			)),
			n:         1,
			setupMock: true,
			customOpt: func() *valkeylimiter.RateLimitOption {
				opt := valkeylimiter.WithCustomRateLimit(20, time.Second*2)
				return &opt
			}(),
			wantResult: valkeylimiter.Result{
				Allowed:   true,
				Remaining: 15,
				ResetAtMs: resetTime,
			},
		},
		{
			name: "limit exceeded",
			mockResp: mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(11),
				mock.ValkeyInt64(resetTime),
			)),
			n:         1,
			setupMock: true,
			wantResult: valkeylimiter.Result{
				Allowed:   false,
				Remaining: 0,
				ResetAtMs: resetTime,
			},
		},
		{
			name:      "redis error",
			mockResp:  mock.ErrorResult(errors.New("redis error")),
			n:         1,
			setupMock: true,
			wantErr:   true,
		},
		{
			name:      "invalid response type",
			mockResp:  mock.Result(mock.ValkeyString("invalid")),
			n:         1,
			setupMock: true,
			wantErr:   true,
		},
		{
			name:      "invalid array length",
			mockResp:  mock.Result(mock.ValkeyArray(mock.ValkeyInt64(1))),
			n:         1,
			setupMock: true,
			wantErr:   true,
		},
		{
			name: "invalid first element",
			mockResp: mock.Result(mock.ValkeyArray(
				mock.ValkeyString("invalid"),
				mock.ValkeyInt64(1),
			)),
			n:         1,
			setupMock: true,
			wantErr:   true,
		},
		{
			name: "invalid second element",
			mockResp: mock.Result(mock.ValkeyArray(
				mock.ValkeyInt64(1),
				mock.ValkeyString("invalid"),
			)),
			n:         1,
			setupMock: true,
			wantErr:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctrl := gomock.NewController(t)
			defer ctrl.Finish()

			client := mock.NewClient(ctrl)
			if tt.setupMock {
				client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(tt.mockResp).Times(1)
			}

			limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
				ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
					return client, nil
				},
				Limit:     10,
				Window:    time.Second,
				Algorithm: valkeylimiter.AlgorithmFixedWindow,
			})
			if err != nil {
				t.Fatal(err)
			}

			var got valkeylimiter.Result
			if tt.customOpt != nil {
				got, err = limiter.AllowN(context.Background(), "test", tt.n, *tt.customOpt)
			} else {
				got, err = limiter.AllowN(context.Background(), "test", tt.n)
			}

			if (err != nil) != tt.wantErr {
				t.Fatalf("AllowN() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err != nil {
				return
			}

			if got.Allowed != tt.wantResult.Allowed || got.Remaining != tt.wantResult.Remaining || got.ResetAtMs != tt.wantResult.ResetAtMs {
				t.Fatalf("AllowN() = %+v, want %+v", got, tt.wantResult)
			}
		})
	}
}

func TestRateLimiter_Check(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	resetTime := now.Add(time.Second).UnixMilli()

	client := mock.NewClient(ctrl)
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
	if err != nil {
		t.Fatal(err)
	}

	got, err := limiter.Check(context.Background(), "test")
	if err != nil {
		t.Fatalf("Check() error = %v", err)
	}

	want := valkeylimiter.Result{
		Allowed:   true,
		Remaining: 5,
		ResetAtMs: resetTime,
	}
	if got.Allowed != want.Allowed || got.Remaining != want.Remaining || got.ResetAtMs != want.ResetAtMs {
		t.Fatalf("Check() = %+v, want %+v", got, want)
	}
}

func TestRateLimiter_Allow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	resetTime := now.Add(time.Second).UnixMilli()

	client := mock.NewClient(ctrl)
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
	if err != nil {
		t.Fatal(err)
	}

	got, err := limiter.Allow(context.Background(), "test")
	if err != nil {
		t.Fatalf("Allow() error = %v", err)
	}

	want := valkeylimiter.Result{
		Allowed:   true,
		Remaining: 9,
		ResetAtMs: resetTime,
	}
	if got.Allowed != want.Allowed || got.Remaining != want.Remaining || got.ResetAtMs != want.ResetAtMs {
		t.Fatalf("Allow() = %+v, want %+v", got, want)
	}
}

func TestRateLimiter_Limit(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:  42,
		Window: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	if got := limiter.Limit(); got != 42 {
		t.Fatalf("Limit() = %v, want %v", got, 42)
	}
}

func TestRateLimiter_AllowN_Dragonfly(t *testing.T) {
	if testing.Short() {
		t.Skip()
	}

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientOption: valkey.ClientOption{
			InitAddress: []string{"127.0.0.1:6333"},
		},
		KeyPrefix: "dragonfly_allow_n_test",
		Limit:     2,
		Window:    time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	id := fmt.Sprintf("id-%d", time.Now().UnixNano())

	result, err := limiter.AllowN(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("AllowN() first call error = %v", err)
	}
	if !result.Allowed || result.Remaining != 1 {
		t.Fatalf("AllowN() first call = %+v, want Allowed=true Remaining=1", result)
	}

	result, err = limiter.AllowN(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("AllowN() second call error = %v", err)
	}
	if !result.Allowed || result.Remaining != 0 {
		t.Fatalf("AllowN() second call = %+v, want Allowed=true Remaining=0", result)
	}

	result, err = limiter.AllowN(context.Background(), id, 1)
	if err != nil {
		t.Fatalf("AllowN() third call error = %v", err)
	}
	if result.Allowed || result.Remaining != 0 {
		t.Fatalf("AllowN() third call = %+v, want Allowed=false Remaining=0", result)
	}
}

func TestRateLimiter_Close(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	client.EXPECT().Close().Times(1)

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:  10,
		Window: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	limiter.Close()
}

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
	for i := 0; i < b.N; i++ {
		_, err := limiter.AllowN(context.Background(), "test", 1)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestDurFromSecString(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    time.Duration
		wantErr bool
	}{
		{
			name:  "negative one represents no retry",
			input: "-1",
			want:  -1,
		},
		{
			name:  "zero seconds",
			input: "0",
			want:  0,
		},
		{
			name:  "fractional seconds 100ms",
			input: "0.1",
			want:  100 * time.Millisecond,
		},
		{
			name:  "one second",
			input: "1.0",
			want:  time.Second,
		},
		{
			name:  "multi second float",
			input: "2.5",
			want:  2500 * time.Millisecond,
		},
		{
			name:  "negative number other than -1",
			input: "-0.5",
			want:  0,
		},
		{
			name:    "invalid float string",
			input:   "not-a-number",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := valkeylimiter.DurFromSecString(tt.input)
			if (err != nil) != tt.wantErr {
				t.Fatalf("DurFromSecString(%q) error = %v, wantErr %v", tt.input, err, tt.wantErr)
			}
			if err != nil {
				return
			}
			if got != tt.want {
				t.Fatalf("DurFromSecString(%q) = %v, want %v", tt.input, got, tt.want)
			}
		})
	}
}

func TestParseGCRAResponse(t *testing.T) {
	t.Run("success allowed", func(t *testing.T) {
		resp := mock.Result(mock.ValkeyArray(
			mock.ValkeyInt64(1),
			mock.ValkeyInt64(9),
			mock.ValkeyString("-1"),
			mock.ValkeyString("0.1"),
		))

		got, err := valkeylimiter.ParseGCRAResponse(resp)
		if err != nil {
			t.Fatalf("ParseGCRAResponse() unexpected error: %v", err)
		}

		want := valkeylimiter.GCRAResult{
			Allowed:    1,
			Remaining:  9,
			RetryAfter: -1,
			ResetAfter: 100 * time.Millisecond,
		}
		if got != want {
			t.Fatalf("ParseGCRAResponse() = %+v, want %+v", got, want)
		}
	})

	t.Run("success rejected", func(t *testing.T) {
		resp := mock.Result(mock.ValkeyArray(
			mock.ValkeyInt64(0),
			mock.ValkeyInt64(0),
			mock.ValkeyString("0.5"),
			mock.ValkeyString("1.0"),
		))

		got, err := valkeylimiter.ParseGCRAResponse(resp)
		if err != nil {
			t.Fatalf("ParseGCRAResponse() unexpected error: %v", err)
		}

		want := valkeylimiter.GCRAResult{
			Allowed:    0,
			Remaining:  0,
			RetryAfter: 500 * time.Millisecond,
			ResetAfter: time.Second,
		}
		if got != want {
			t.Fatalf("ParseGCRAResponse() = %+v, want %+v", got, want)
		}
	})

	t.Run("valkey error", func(t *testing.T) {
		resp := mock.ErrorResult(errors.New("valkey cluster error"))
		_, err := valkeylimiter.ParseGCRAResponse(resp)
		if err == nil {
			t.Fatal("ParseGCRAResponse() expected error, got nil")
		}
	})

	t.Run("invalid non-array response", func(t *testing.T) {
		resp := mock.Result(mock.ValkeyString("unexpected string"))
		_, err := valkeylimiter.ParseGCRAResponse(resp)
		if !errors.Is(err, valkeylimiter.ErrInvalidResponse) {
			t.Fatalf("ParseGCRAResponse() error = %v, want ErrInvalidResponse", err)
		}
	})

	t.Run("invalid array length less than 4", func(t *testing.T) {
		resp := mock.Result(mock.ValkeyArray(
			mock.ValkeyInt64(1),
			mock.ValkeyInt64(9),
			mock.ValkeyString("-1"),
		))
		_, err := valkeylimiter.ParseGCRAResponse(resp)
		if !errors.Is(err, valkeylimiter.ErrInvalidResponse) {
			t.Fatalf("ParseGCRAResponse() error = %v, want ErrInvalidResponse", err)
		}
	})

	t.Run("invalid array length greater than 4", func(t *testing.T) {
		resp := mock.Result(mock.ValkeyArray(
			mock.ValkeyInt64(1),
			mock.ValkeyInt64(9),
			mock.ValkeyString("-1"),
			mock.ValkeyString("0.1"),
			mock.ValkeyString("extra"),
		))
		_, err := valkeylimiter.ParseGCRAResponse(resp)
		if !errors.Is(err, valkeylimiter.ErrInvalidResponse) {
			t.Fatalf("ParseGCRAResponse() error = %v, want ErrInvalidResponse", err)
		}
	})

	t.Run("invalid first element (not int64 or int string)", func(t *testing.T) {
		resp := mock.Result(mock.ValkeyArray(
			mock.ValkeyString("not-an-int"),
			mock.ValkeyInt64(9),
			mock.ValkeyString("-1"),
			mock.ValkeyString("0.1"),
		))
		_, err := valkeylimiter.ParseGCRAResponse(resp)
		if !errors.Is(err, valkeylimiter.ErrInvalidResponse) {
			t.Fatalf("ParseGCRAResponse() error = %v, want ErrInvalidResponse", err)
		}
	})

	t.Run("invalid second element (not int64 or int string)", func(t *testing.T) {
		resp := mock.Result(mock.ValkeyArray(
			mock.ValkeyInt64(1),
			mock.ValkeyString("not-an-int"),
			mock.ValkeyString("-1"),
			mock.ValkeyString("0.1"),
		))
		_, err := valkeylimiter.ParseGCRAResponse(resp)
		if !errors.Is(err, valkeylimiter.ErrInvalidResponse) {
			t.Fatalf("ParseGCRAResponse() error = %v, want ErrInvalidResponse", err)
		}
	})

	t.Run("invalid retry_after float string", func(t *testing.T) {
		resp := mock.Result(mock.ValkeyArray(
			mock.ValkeyInt64(1),
			mock.ValkeyInt64(9),
			mock.ValkeyString("invalid-float"),
			mock.ValkeyString("0.1"),
		))
		_, err := valkeylimiter.ParseGCRAResponse(resp)
		if !errors.Is(err, valkeylimiter.ErrInvalidResponse) {
			t.Fatalf("ParseGCRAResponse() error = %v, want ErrInvalidResponse", err)
		}
	})

	t.Run("invalid reset_after float string", func(t *testing.T) {
		resp := mock.Result(mock.ValkeyArray(
			mock.ValkeyInt64(1),
			mock.ValkeyInt64(9),
			mock.ValkeyString("-1"),
			mock.ValkeyString("invalid-float"),
		))
		_, err := valkeylimiter.ParseGCRAResponse(resp)
		if !errors.Is(err, valkeylimiter.ErrInvalidResponse) {
			t.Fatalf("ParseGCRAResponse() error = %v, want ErrInvalidResponse", err)
		}
	})
}

func TestExecGCRAAllowN_Mock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(2),
		mock.ValkeyInt64(8),
		mock.ValkeyString("-1"),
		mock.ValkeyString("0.2"),
	))).Times(1)

	got, err := valkeylimiter.ExecGCRAAllowN(context.Background(), client, "rate:{user:1}", 10, 10, time.Second, 2)
	if err != nil {
		t.Fatalf("ExecGCRAAllowN() unexpected error: %v", err)
	}

	want := valkeylimiter.GCRAResult{
		Allowed:    2,
		Remaining:  8,
		RetryAfter: -1,
		ResetAfter: 200 * time.Millisecond,
	}
	if got != want {
		t.Fatalf("ExecGCRAAllowN() = %+v, want %+v", got, want)
	}
}

func TestExecGCRAAllowAtMost_Mock(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(4),
		mock.ValkeyInt64(0),
		mock.ValkeyString("-1"),
		mock.ValkeyString("0.6"),
	))).Times(1)

	got, err := valkeylimiter.ExecGCRAAllowAtMost(context.Background(), client, "rate:{user:2}", 10, 10, time.Second, 5)
	if err != nil {
		t.Fatalf("ExecGCRAAllowAtMost() unexpected error: %v", err)
	}

	want := valkeylimiter.GCRAResult{
		Allowed:    4,
		Remaining:  0,
		RetryAfter: -1,
		ResetAfter: 600 * time.Millisecond,
	}
	if got != want {
		t.Fatalf("ExecGCRAAllowAtMost() = %+v, want %+v", got, want)
	}
}

func TestGCRA_LiveServer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live server test in short mode")
	}

	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:6378"},
	})
	if err != nil {
		t.Skipf("cannot connect to 127.0.0.1:6378: %v", err)
	}
	defer client.Close()

	ctx := context.Background()
	testKey := fmt.Sprintf("test:gcra:live:%d", time.Now().UnixNano())

	// Clean up key after test
	defer func() {
		_ = client.Do(ctx, client.B().Del().Key(testKey).Build()).Error()
	}()

	// Test AllowN: Limit 10 req / 1 sec, burst 10
	// 1. Initial request for 1 token: should succeed, allowed = 1, remaining = 9
	res1, err := valkeylimiter.ExecGCRAAllowN(ctx, client, testKey, 10, 10, time.Second, 1)
	if err != nil {
		t.Fatalf("ExecGCRAAllowN 1st call error: %v", err)
	}
	if res1.Allowed != 1 || res1.Remaining != 9 || res1.RetryAfter != -1 {
		t.Fatalf("ExecGCRAAllowN 1st call unexpected result: %+v", res1)
	}

	// 2. Request for 2 tokens: should succeed, allowed = 2, remaining = 7
	res2, err := valkeylimiter.ExecGCRAAllowN(ctx, client, testKey, 10, 10, time.Second, 2)
	if err != nil {
		t.Fatalf("ExecGCRAAllowN 2nd call error: %v", err)
	}
	if res2.Allowed != 2 || res2.Remaining != 7 || res2.RetryAfter != -1 {
		t.Fatalf("ExecGCRAAllowN 2nd call unexpected result: %+v", res2)
	}

	// 3. Request exceeding remaining tokens with AllowN (cost 100): should be rejected all-or-nothing
	res3, err := valkeylimiter.ExecGCRAAllowN(ctx, client, testKey, 10, 10, time.Second, 100)
	if err != nil {
		t.Fatalf("ExecGCRAAllowN 3rd call error: %v", err)
	}
	if res3.Allowed != 0 || res3.Remaining != 0 || res3.RetryAfter <= 0 {
		t.Fatalf("ExecGCRAAllowN 3rd call should be rejected: %+v", res3)
	}

	// 4. Test AllowAtMost with 7 remaining, asking for 10: should grant 7
	res4, err := valkeylimiter.ExecGCRAAllowAtMost(ctx, client, testKey, 10, 10, time.Second, 10)
	if err != nil {
		t.Fatalf("ExecGCRAAllowAtMost error: %v", err)
	}
	if res4.Allowed != 7 || res4.Remaining != 0 || res4.RetryAfter != -1 {
		t.Fatalf("ExecGCRAAllowAtMost unexpected result: %+v", res4)
	}

	// 5. Test AllowAtMost when exhausted: should be rejected
	res5, err := valkeylimiter.ExecGCRAAllowAtMost(ctx, client, testKey, 10, 10, time.Second, 1)
	if err != nil {
		t.Fatalf("ExecGCRAAllowAtMost exhausted error: %v", err)
	}
	if res5.Allowed != 0 || res5.Remaining != 0 || res5.RetryAfter <= 0 {
		t.Fatalf("ExecGCRAAllowAtMost exhausted should be rejected: %+v", res5)
	}
}

func TestRateLimiter_GCRA_DefaultAndAllow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(1),
		mock.ValkeyInt64(9),
		mock.ValkeyString("-1"),
		mock.ValkeyString("0.1"),
	))).Times(1)

	// NewRateLimiter without specifying Algorithm defaults to GCRA
	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:  10,
		Window: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	got, err := limiter.Allow(context.Background(), "user1")
	if err != nil {
		t.Fatalf("Allow() error: %v", err)
	}

	if !got.Allowed || got.Remaining != 9 || got.Granted != 1 || got.RetryAfter != -1 || got.ResetAfter != 100*time.Millisecond {
		t.Fatalf("Allow() unexpected result: %+v", got)
	}
	if got.ResetAtMs <= 0 {
		t.Fatalf("Allow() expected ResetAtMs > 0, got %d", got.ResetAtMs)
	}
}

func TestRateLimiter_GCRA_AllowN(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
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
	if err != nil {
		t.Fatal(err)
	}

	got, err := limiter.AllowN(context.Background(), "user2", 3)
	if err != nil {
		t.Fatalf("AllowN() error: %v", err)
	}

	if !got.Allowed || got.Remaining != 7 || got.Granted != 3 || got.RetryAfter != -1 || got.ResetAfter != 300*time.Millisecond {
		t.Fatalf("AllowN() unexpected result: %+v", got)
	}
}

func TestRateLimiter_GCRA_AllowAtMost(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
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
	if err != nil {
		t.Fatal(err)
	}

	got, err := limiter.AllowAtMost(context.Background(), "user3", 10)
	if err != nil {
		t.Fatalf("AllowAtMost() error: %v", err)
	}

	if !got.Allowed || got.Remaining != 0 || got.Granted != 7 || got.RetryAfter != -1 || got.ResetAfter != time.Second {
		t.Fatalf("AllowAtMost() unexpected result: %+v", got)
	}
}

func TestRateLimiter_GCRA_Check(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
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
	if err != nil {
		t.Fatal(err)
	}

	got, err := limiter.Check(context.Background(), "user4")
	if err != nil {
		t.Fatalf("Check() error: %v", err)
	}

	if !got.Allowed || got.Remaining != 5 || got.Granted != 0 || got.RetryAfter != -1 {
		t.Fatalf("Check() unexpected result: %+v", got)
	}
}

func TestRateLimiter_Reset_GCRA(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyInt64(1))).Times(1)

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:  10,
		Window: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = limiter.Reset(context.Background(), "test_id")
	if err != nil {
		t.Fatalf("Reset() error: %v", err)
	}
}

func TestRateLimiter_Reset_FixedWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyInt64(2))).Times(1)

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:     10,
		Window:    time.Second,
		Algorithm: valkeylimiter.AlgorithmFixedWindow,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = limiter.Reset(context.Background(), "test_id")
	if err != nil {
		t.Fatalf("Reset() error: %v", err)
	}
}

func TestRateLimiter_AllowAtMost_FixedWindow(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	resetTime := now.Add(time.Second).UnixMilli()

	client := mock.NewClient(ctrl)
	// Check call (n=0): returns current=6 (remaining=4)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(6),
		mock.ValkeyInt64(resetTime),
	))).Times(1)
	// Second call (grant=4): returns current=10 (remaining=0)
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
	if err != nil {
		t.Fatal(err)
	}

	// Request 10 tokens when only 4 are remaining
	got, err := limiter.AllowAtMost(context.Background(), "test", 10)
	if err != nil {
		t.Fatalf("AllowAtMost() error = %v", err)
	}

	if !got.Allowed || got.Granted != 4 || got.Remaining != 0 {
		t.Fatalf("AllowAtMost() = %+v, want Allowed=true Granted=4 Remaining=0", got)
	}
}

func TestRateLimiter_GCRA_LiveServer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping live server test in short mode")
	}

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientOption: valkey.ClientOption{
			InitAddress: []string{"127.0.0.1:6378"},
		},
		Limit:  10,
		Window: time.Second,
	})
	if err != nil {
		t.Skipf("cannot connect to 127.0.0.1:6378: %v", err)
	}
	defer limiter.Close()

	ctx := context.Background()
	testId := fmt.Sprintf("user-live-%d", time.Now().UnixNano())

	defer func() {
		_ = limiter.Reset(ctx, testId)
	}()

	// 1. Allow single token
	res1, err := limiter.Allow(ctx, testId)
	if err != nil {
		t.Fatalf("Allow() error: %v", err)
	}
	if !res1.Allowed || res1.Remaining != 9 || res1.Granted != 1 {
		t.Fatalf("Allow() unexpected: %+v", res1)
	}

	// 2. AllowN 2 tokens
	res2, err := limiter.AllowN(ctx, testId, 2)
	if err != nil {
		t.Fatalf("AllowN() error: %v", err)
	}
	if !res2.Allowed || res2.Remaining != 7 || res2.Granted != 2 {
		t.Fatalf("AllowN() unexpected: %+v", res2)
	}

	// 3. AllowAtMost requesting 10 with 7 remaining -> should grant 7
	res3, err := limiter.AllowAtMost(ctx, testId, 10)
	if err != nil {
		t.Fatalf("AllowAtMost() error: %v", err)
	}
	if !res3.Allowed || res3.Remaining != 0 || res3.Granted != 7 {
		t.Fatalf("AllowAtMost() unexpected: %+v", res3)
	}

	// 4. Next call should be rejected
	res4, err := limiter.Allow(ctx, testId)
	if err != nil {
		t.Fatalf("Allow() error: %v", err)
	}
	if res4.Allowed || res4.Remaining != 0 || res4.RetryAfter <= 0 {
		t.Fatalf("Allow() should be rejected: %+v", res4)
	}

	// 5. Reset the key
	err = limiter.Reset(ctx, testId)
	if err != nil {
		t.Fatalf("Reset() error: %v", err)
	}

	// 6. Request after reset should succeed with full capacity
	res5, err := limiter.Allow(ctx, testId)
	if err != nil {
		t.Fatalf("Allow() after reset error: %v", err)
	}
	if !res5.Allowed || res5.Remaining != 9 || res5.Granted != 1 {
		t.Fatalf("Allow() after reset unexpected: %+v", res5)
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
	for i := 0; i < b.N; i++ {
		_, err := limiter.AllowN(context.Background(), "test", 1)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func TestRateLimiter_GCRA_Concurrency_Atomicity(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping concurrency test in short mode")
	}

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientOption: valkey.ClientOption{
			InitAddress: []string{"127.0.0.1:6378"},
		},
		Limit:  10,
		Window: 10 * time.Second,
		Burst:  10,
	})
	if err != nil {
		t.Skipf("cannot connect to 127.0.0.1:6378: %v", err)
	}
	defer limiter.Close()

	ctx := context.Background()
	testId := fmt.Sprintf("concurrent-test-%d", time.Now().UnixNano())
	defer func() {
		_ = limiter.Reset(ctx, testId)
	}()

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

	if allowedCount != 10 {
		t.Fatalf("Concurrency atomicity violated: allowed %d, want exactly 10", allowedCount)
	}
	if rejectedCount != 40 {
		t.Fatalf("Concurrency atomicity violated: rejected %d, want exactly 40", rejectedCount)
	}
}

func TestRateLimiter_OptionOverrides(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	// Expect GCRA script with custom rate 20, burst 30, window 2s
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
	if err != nil {
		t.Fatal(err)
	}

	opt := valkeylimiter.WithCustomRateLimitAndBurst(20, 2*time.Second, 30)
	got, err := limiter.Allow(context.Background(), "user-custom", opt)
	if err != nil {
		t.Fatalf("Allow with custom options error: %v", err)
	}
	if !got.Allowed || got.Remaining != 29 {
		t.Fatalf("Allow with custom options unexpected: %+v", got)
	}

	// Expect GCRA script with custom burst 50 (rate remains default 10, window 1s)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(1),
		mock.ValkeyInt64(49),
		mock.ValkeyString("-1"),
		mock.ValkeyString("0.1"),
	))).Times(1)

	gotBurst, err := limiter.Allow(context.Background(), "user-burst", valkeylimiter.WithBurst(50))
	if err != nil {
		t.Fatalf("Allow with WithBurst error: %v", err)
	}
	if !gotBurst.Allowed || gotBurst.Remaining != 49 {
		t.Fatalf("Allow with WithBurst unexpected: %+v", gotBurst)
	}
}

func TestRateLimiter_AllowAtMost_ValidationErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:  10,
		Window: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Negative tokens
	_, err = limiter.AllowAtMost(context.Background(), "user-neg", -5)
	if !errors.Is(err, valkeylimiter.ErrInvalidTokens) {
		t.Fatalf("AllowAtMost with negative tokens error = %v, want ErrInvalidTokens", err)
	}
}

func TestRateLimiter_ClientErrors(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := mock.NewClient(ctrl)
	client.EXPECT().Do(gomock.Any(), gomock.Any()).Return(mock.ErrorResult(errors.New("connection failed"))).Times(3)

	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
			return client, nil
		},
		Limit:  10,
		Window: time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	// Allow error
	_, err = limiter.Allow(context.Background(), "user-err")
	if err == nil {
		t.Fatal("Allow expected error, got nil")
	}

	// AllowAtMost error
	_, err = limiter.AllowAtMost(context.Background(), "user-err", 5)
	if err == nil {
		t.Fatal("AllowAtMost expected error, got nil")
	}

	// Reset error
	err = limiter.Reset(context.Background(), "user-err")
	if err == nil {
		t.Fatal("Reset expected error, got nil")
	}
}



