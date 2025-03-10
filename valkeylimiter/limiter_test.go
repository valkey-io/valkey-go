package valkeylimiter_test

import (
	"context"
	"errors"
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
				Limit:  10,
				Window: time.Second,
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

			if got != tt.wantResult {
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
		Limit:  10,
		Window: time.Second,
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
	if got != want {
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
		Limit:  10,
		Window: time.Second,
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
	if got != want {
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
		Limit:  1000,
		Window: time.Second,
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
