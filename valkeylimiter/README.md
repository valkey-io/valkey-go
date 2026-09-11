
# valkeylimiter

`valkeylimiter` is a high-performance, distributed rate limiting module for Valkey and Redis.

By default, it uses the **Generic Cell Rate Algorithm (GCRA)** (leaky bucket) to provide smooth, continuous traffic pacing with burst tolerance, while also providing a fallback **Fixed Window** counter algorithm for legacy compatibility.

## Features

- **Generic Cell Rate Algorithm (GCRA) by Default**: Eliminates the 2x boundary burst spike ("double-dipping") inherent in fixed-window algorithms, pacing requests evenly over time.
- **Single-Key Architecture**: Operates on a single Valkey key (`rate:<identifier>`), cutting keyspace overhead in half compared to dual-key fixed-window limiters.
- **Server-Authoritative Clock**: Time is derived atomically inside Valkey using `redis.call('TIME')`, eliminating synchronization discrepancies caused by client NTP drift.
- **Valkey Cluster Safe (Zero `CROSSSLOT`)**: Because each operation touches strictly a single key (`KEYS[1]`), GCRA rate limiting never triggers `CROSSSLOT` errors, with or without hash tags (`{...}`).
- **Partial Allowance (`AllowAtMost`)**: Allows batch jobs or multi-token consumers to acquire "up to" available capacity without failing completely.
- **Programmatic Reset (`Reset`)**: Programmatically clears rate limiting state for an identifier.
- **Detailed Timing Diagnostics**: Provides `RetryAfter` (`-1` on success, duration to wait on throttle) and `ResetAfter` (duration until full capacity is restored).
- **Multi-Strategy Extensibility**: Switch seamlessly between GCRA (`AlgorithmGCRA = 0` default) and legacy Fixed Window (`AlgorithmFixedWindow = 1`).
- **Zero-Allocation Hot Path**: Leverages pooled buffers (`sync.Pool`) for high-throughput, low-allocation execution.

## Installation

To install the `valkeylimiter` module, run:

```bash
go get github.com/valkey-io/valkey-go/valkeylimiter
```

## Usage

### Basic Rate Limiting Example (GCRA Default)

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeylimiter"
)

func main() {
	// Initialize a limiter: 10 requests per minute with default burst capacity of 10
	limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
		ClientOption: valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
		Limit:        10,
		Window:       time.Minute,
	})
	if err != nil {
		panic(err)
	}
	defer limiter.Close()

	ctx := context.Background()
	userID := "user_12345"

	// Check without consuming capacity
	chk, err := limiter.Check(ctx, userID)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Initial state: Allowed=%v, Remaining=%d\n", chk.Allowed, chk.Remaining)

	// Consume 1 token
	res, err := limiter.Allow(ctx, userID)
	if err != nil {
		panic(err)
	}
	if res.Allowed {
		fmt.Printf("Allowed! Remaining: %d, ResetAfter: %v\n", res.Remaining, res.ResetAfter)
	} else {
		fmt.Printf("Rate limited! Retry after: %v\n", res.RetryAfter)
	}

	// Consume multiple tokens
	res, err = limiter.AllowN(ctx, userID, 3)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Allowed %d tokens! Remaining: %d\n", res.Granted, res.Remaining)
}
```

---

## Advanced Usage

### 1. Burst Capacity (`Burst` & `WithBurst`)

GCRA separates the sustained **rate** from the **burst tolerance**. By default, `Burst` equals `Limit`. You can configure a larger burst buffer:

```go
limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
	ClientOption: valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
	Limit:        10,            // 10 req/s sustained
	Window:       time.Second,
	Burst:        50,            // allows bursts up to 50 tokens
})

// Or override burst dynamically per request:
res, err := limiter.Allow(ctx, "user_123", valkeylimiter.WithBurst(100))
```

### 2. Partial Token Consumption (`AllowAtMost`)

For batch tasks, `AllowAtMost` grants whatever capacity is currently available up to `n` tokens:

```go
// Request up to 10 tokens; if only 4 remain, 4 are granted rather than failing completely
res, err := limiter.AllowAtMost(ctx, "batch_job", 10)
if err != nil {
	panic(err)
}

fmt.Printf("Allowed: %v, Granted: %d, Remaining: %d\n", res.Allowed, res.Granted, res.Remaining)
```

### 3. Programmatic State Clearance (`Reset`)

Clear rate limiting state immediately for a specific identifier:

```go
err := limiter.Reset(ctx, "user_123")
if err != nil {
	panic(err)
}
```

### 4. Valkey Cluster Usage (Zero `CROSSSLOT`)

Because GCRA operates strictly on a single key (`KEYS[1]`), it is inherently safe in Valkey Cluster mode and will never produce `CROSSSLOT` errors. You can also use explicit hash tags for multi-tenant isolation:

```go
limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
	ClientOption: valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:7010", "127.0.0.1:7011", "127.0.0.1:7012"},
	},
	Limit:  100,
	Window: time.Minute,
})
if err != nil {
	panic(err)
}
defer limiter.Close()

// Cluster routes by hash tag {tenant_1}; zero CROSSSLOT errors guaranteed
res, err := limiter.Allow(ctx, "{tenant_1}:user_42")
```

### 5. Opting Into Fixed Window Algorithm

If you need the legacy dual-key fixed-window counter:

```go
limiter, err := valkeylimiter.NewRateLimiter(valkeylimiter.RateLimiterOption{
	ClientOption: valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
	Limit:        100,
	Window:       time.Minute,
	Algorithm:    valkeylimiter.AlgorithmFixedWindow, // Opt-in
})
```

---

## API Reference

### Structs

#### `RateLimiterOption`
- `ClientOption (valkey.ClientOption)`: Valkey client connection options.
- `ClientBuilder`: Optional custom client constructor.
- `KeyPrefix (string)`: Key prefix (defaults to `"rate:"` for GCRA, `"valkeylimiter"` for Fixed Window).
- `Limit (int)`: Maximum requests permitted per window.
- `Window (time.Duration)`: Rate limit window period.
- `Burst (int)`: Maximum burst capacity (defaults to `Limit`).
- `Algorithm (Algorithm)`: Rate limit algorithm (`AlgorithmGCRA = 0` default, `AlgorithmFixedWindow = 1`).

#### `Result`
- `Allowed (bool)`: Whether the request was permitted.
- `Granted (int64)`: The number of tokens granted for this request (0 if rejected).
- `Remaining (int64)`: Tokens remaining in the current window / bucket.
- `RetryAfter (time.Duration)`: Time caller should wait before retrying (`-1` when `Allowed == true`).
- `ResetAfter (time.Duration)`: Time until the bucket is completely drained / refilled.
- `ResetAtMs (int64)`: Unix timestamp in milliseconds for window reset.

### Methods (`RateLimiterClient`)

- `Allow(ctx context.Context, id string, opts ...RateLimitOption) (Result, error)`: Consumes 1 token.
- `AllowN(ctx context.Context, id string, n int64, opts ...RateLimitOption) (Result, error)`: Consumes `n` tokens (all-or-nothing).
- `AllowAtMost(ctx context.Context, id string, n int64, opts ...RateLimitOption) (Result, error)`: Consumes up to `n` tokens based on available capacity.
- `Check(ctx context.Context, id string, opts ...RateLimitOption) (Result, error)`: Peeks at current capacity without consuming tokens.
- `Reset(ctx context.Context, id string) error`: Clears rate limit state for the identifier.
- `Limit() int`: Returns the configured default rate limit.
- `Close()`: Closes the underlying client connection.

### Option Modifiers

- `WithBurst(burst int)`: Overrides the burst capacity for the request.
- `WithAlgorithm(alg Algorithm)`: Overrides the rate limiting algorithm (`AlgorithmGCRA` or `AlgorithmFixedWindow`).
- `WithCustomRateLimit(limit int, window time.Duration)`: Overrides both limit and window.
- `WithCustomRateLimitAndBurst(limit int, window time.Duration, burst int)`: Overrides limit, window, and burst capacity.

---

## Implementation Details

The `valkeylimiter` module executes an atomic Lua script inside Valkey:
1. **Effects Replication (`redis.replicate_commands()`)**: Issued on Line 1 of the Lua script for compatibility with older Redis instances (e.g., Redis 6.2 on Memorystore/ElastiCache) where `redis.call('TIME')` is evaluated.
2. **Authoritative Server Time**: Derives the current timestamp via `redis.call('TIME')`, completely eliminating clock drift across distributed client hosts.
3. **Theoretical Arrival Time (TAT)**: Tracks the continuous arrival schedule using a single scalar timestamp. If an arrival is within burst tolerance ($\text{TAT} \le \text{now} + \tau$), tokens are granted and the new TAT is committed. Otherwise, the arrival is rejected without modifying state.
4. **Zero Keyspace Leaks**: Keys automatically expire based on the exact time required for the bucket to drain completely. Idle keys leave no residual state in Valkey.
