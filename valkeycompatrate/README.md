# valkeycompatrate

`valkeycompatrate` provides a 100% drop-in replacement for [`github.com/go-redis/redis_rate/v10`](https://github.com/go-redis/redis_rate), powered by `valkeylimiter`'s high-performance Generic Cell Rate Algorithm (GCRA) engine.

## Key Benefits

- **Drop-in Migration**: Retains the exact same struct fields, rate helpers, and method signatures as `redis_rate/v10`.
- **Zero Cache Misses on Migration**: Uses the identical key format (`"rate:" + key`), avoiding cold starts during rolling migrations from `go-redis`.
- **Zero `CROSSSLOT` Errors**: Single-key architecture (`KEYS[1]`) runs safely in Valkey Cluster mode with or without hash tags.
- **Server Clock Accuracy**: Uses server-side `TIME` in Lua, preventing client NTP drift discrepancies.
- **Bonus APIs**: Adds `AllowAtMost` (partial capacity grants) and `Reset` (state clearance).

## Installation

```bash
go get github.com/valkey-io/valkey-go/valkeycompatrate
```

## Migration Example

To migrate from `go-redis/redis_rate/v10`, simply swap the import and initialize the limiter:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeycompat"
	"github.com/valkey-io/valkey-go/valkeycompatrate"
)

func main() {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Initialize via valkeycompat.Adapter:
	rdb := valkeycompat.NewAdapter(client)
	limiter := valkeycompatrate.NewLimiter(rdb)

	// Or initialize directly from valkey.Client:
	// limiter := valkeycompatrate.NewLimiterFromClient(client)

	ctx := context.Background()

	// Define rate limit using standard helpers:
	limit := valkeycompatrate.PerMinute(10) // 10 req/min, burst 10

	// Allow a single request
	res, err := limiter.Allow(ctx, "project:123", limit)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Allowed: %d, Remaining: %d, RetryAfter: %v, ResetAfter: %v\n",
		res.Allowed, res.Remaining, res.RetryAfter, res.ResetAfter)

	// Consume multiple tokens (all-or-nothing)
	res, err = limiter.AllowN(ctx, "project:123", limit, 2)
	if err != nil {
		panic(err)
	}

	// Partial allowance (bonus API): grant up to 5 tokens based on available capacity
	res, err = limiter.AllowAtMost(ctx, "project:123", limit, 5)
	if err != nil {
		panic(err)
	}
	fmt.Printf("Granted %d tokens!\n", res.Allowed)

	// Reset state (bonus API)
	_ = limiter.Reset(ctx, "project:123")
}
```

## Upstream Field Mapping Reference

| `redis_rate/v10` Field | `valkeycompatrate` Value | Description |
| :--- | :--- | :--- |
| `Result.Allowed` (`int`) | `int(valkeyResult.Granted)` | Tokens granted for this request (`0` when rejected). |
| `Result.Remaining` (`int`) | `int(valkeyResult.Remaining)` | Remaining token count in the bucket. |
| `Result.RetryAfter` (`time.Duration`) | `valkeyResult.RetryAfter` | `-1` on success; duration to wait before retry when throttled. |
| `Result.ResetAfter` (`time.Duration`) | `valkeyResult.ResetAfter` | Duration until the bucket completely refills to maximum capacity. |

