## Go-redis like API Adapter

Though it is easier to know what command will be sent to valkey at first glance if the command is constructed by the command builder,
users may sometimes feel it too verbose to write.

For users who don't like the command builder, `valkeycompat.Adapter`, contributed mainly by [@418Coffee](https://github.com/418Coffee), is an alternative.
It is a high-level API that is close to go-redis's `Cmdable` interface.

### Migrating from go-redis

You can also try adapting `valkey` with existing go-redis code by replacing go-redis's `UniversalClient` with `valkeycompat.Adapter`.

### Client side caching example

To use client side caching with `valkeycompat.Adapter`, chain `Cache(ttl)` call in front of supported command.

```golang
package main

import (
	"context"
	"time"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeycompat"
)

func main() {
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	compat := valkeycompat.NewAdapter(client)
	ok, _ := compat.SetNX(ctx, "key", "val", time.Second).Result()

	// with client side caching
	res, _ := compat.Cache(time.Second).Get(ctx, "key").Result()
}
```

### Pipeline example

```golang
package main

import (
	"context"
	"fmt"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeycompat"
)

func main() {
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	rdb := valkeycompat.NewAdapter(client)
	cmds, err := rdb.Pipelined(ctx, func(pipe valkeycompat.Pipeliner) error {
		for i := 0; i < 100; i++ {
			pipe.Set(ctx, fmt.Sprintf("key%d", i), i, 0)
			pipe.Get(ctx, fmt.Sprintf("key%d", i))
		}
		return nil
	})
	if err != nil {
		panic(err)
	}
	for _, cmd := range cmds {
		fmt.Println(cmd.(*valkeycompat.StringCmd).Val())
	}
}
```

### Transaction example

```golang
package main

import (
	"context"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeycompat"
)

func main() {
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	key := "my_counter"
	rdb := valkeycompat.NewAdapter(client)
	txf := func(tx valkeycompat.Tx) error {
		n, err := tx.Get(ctx, key).Int()
		if err != nil && err != valkeycompat.Nil {
			return err
		}
		// Operation is committed only if the watched keys remain unchanged.
		_, err = tx.TxPipelined(ctx, func(pipe valkeycompat.Pipeliner) error {
			pipe.Set(ctx, key, n+1, 0)
			return nil
		})
		return err
	}
	for {
		err := rdb.Watch(ctx, txf, key)
		if err == nil {
			break
		} else if err == valkeycompat.TxFailedErr {
			// Optimistic lock lost. Retry if the key has been changed.
			continue
		}
		panic(err)
	}
}
```


### PubSub example

```golang
package main

import (
	"context"
	"fmt"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeycompat"
	"strconv"
)

func main() {
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	rdb := valkeycompat.NewAdapter(client)
	pubsub := rdb.Subscribe(ctx, "mychannel1")
	defer pubsub.Close()

	go func() {
		for i := 0; ; i++ {
			if err := rdb.Publish(ctx, "mychannel1", strconv.Itoa(i)).Err(); err != nil {
				panic(err)
			}
		}
	}()
	for {
		msg, err := pubsub.ReceiveMessage(ctx)
		if err != nil {
			panic(err)
		}
		fmt.Println(msg.Channel, msg.Payload)
	}
}
```

### Lua script example

```golang
package main

import (
	"context"
	"fmt"
	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeycompat"
)

var incrBy = valkeycompat.NewScript(`
local key = KEYS[1]
local change = ARGV[1]
local value = redis.call("GET", key)
if not value then
  value = 0
end
value = value + change
redis.call("SET", key, value)
return value
`)

func main() {
	ctx := context.Background()
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	rdb := valkeycompat.NewAdapter(client)
	keys := []string{"my_counter"}
	values := []interface{}{+1}
	fmt.Println(incrBy.Run(ctx, rdb, keys, values...).Int())
}
```

### Methods not yet implemented in the adapter

* `HExpire`, `HPExpire`, `HTTL`, and `HPTTL` related methods.
* `FTSearch`, `FTAggregate`, `FTCreate`, and `FTDropIndex` related methods.

For more details, please refer to those `TODO` marks in the [./adapter.go](./adapter.go)

---

## Rate Limiting (`redisrate` - `go-redis/redis_rate/v10` Parity)

`valkeycompat/redisrate` provides a 100% drop-in replacement for [`github.com/go-redis/redis_rate/v10`](https://github.com/go-redis/redis_rate), powered by `valkeylimiter`'s high-performance Generic Cell Rate Algorithm (GCRA) engine.

### Key Benefits

- **Drop-in Migration**: Retains the exact same struct fields, rate helpers, and method signatures as `redis_rate/v10`.
- **Zero Cache Misses on Migration**: Uses the identical key format (`"rate:" + key`), avoiding cold starts during rolling migrations from `go-redis`.
- **Zero `CROSSSLOT` Errors**: Single-key architecture (`KEYS[1]`) runs safely in Valkey Cluster mode with or without hash tags.
- **Server Clock Accuracy**: Uses server-side `TIME` in Lua, preventing client NTP drift discrepancies.
- **Bonus APIs**: Adds `AllowAtMost` (partial capacity grants) and `Reset` (state clearance).

### Migration Example

To migrate from `go-redis/redis_rate/v10`, simply swap the import and initialize the limiter:

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeycompat"
	"github.com/valkey-io/valkey-go/valkeycompat/redisrate"
)

func main() {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Initialize via valkeycompat.Adapter:
	rdb := valkeycompat.NewAdapter(client)
	limiter := redisrate.NewLimiter(rdb)

	// Or initialize directly from valkey.Client:
	// limiter := redisrate.NewLimiterFromClient(client)

	ctx := context.Background()

	// Define rate limit using standard helpers:
	limit := redisrate.PerMinute(10) // 10 req/min, burst 10

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

### Upstream Field Mapping Reference

| `redis_rate/v10` Field | `valkeycompat/redisrate` Value | Description |
| :--- | :--- | :--- |
| `Result.Allowed` (`int`) | `int(valkeyResult.Granted)` | Tokens granted for this request (`0` when rejected). |
| `Result.Remaining` (`int`) | `int(valkeyResult.Remaining)` | Remaining token count in the bucket. |
| `Result.RetryAfter` (`time.Duration`) | `valkeyResult.RetryAfter` | `-1` on success; duration to wait before retry when throttled. |
| `Result.ResetAfter` (`time.Duration`) | `valkeyResult.ResetAfter` | Duration until the bucket completely refills to maximum capacity. |
