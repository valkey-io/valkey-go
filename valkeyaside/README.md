# valkeyaside

A Cache-Aside pattern implementation enhanced by [Client Side Caching](https://redis.io/docs/manual/client-side-caching/).

## Features backed by the Valkey Client Side Caching

Cache-Aside is a widely used pattern to cache other data sources into Valkey. However, there are many issues to be considered when implementing it.

For example, an implementation without locking or versioning may cause a fresh cache be overridden by a stale one.
And if using a locking mechanism, how to get notified when a lock is released? If using versioning mechanism, how to version an empty value?

Thankfully, the above issues can be addressed better with the client-side caching along with the following additional benefits: 

* Avoiding unnecessary network round trips. Valkey will proactively invalidate the client-side cache.
* Avoiding Cache Stampede by locking keys with the client-side caching, the same technique used in [valkeylock](https://github.com/rueian/valkey-go/tree/main/valkeylock). Only the first cache missed call can update the cache and others will wait for notifications.

## Example

```go
package main

import (
	"context"
	"database/sql"
	"time"

	"github.com/rueian/valkey-go"
	"github.com/rueian/valkey-go/valkeyaside"
)

func main() {
	var db sql.DB
	client, err := valkeyaside.NewClient(valkeyaside.ClientOption{
		ClientOption: valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}},
	})
	if err != nil {
		panic(err)
	}
	val, err := client.Get(context.Background(), time.Minute, "mykey", func(ctx context.Context, key string) (val string, err error) {
		if err = db.QueryRowContext(ctx, "SELECT val FROM mytab WHERE id = ?", key).Scan(&val); err == sql.ErrNoRows {
			val = "_nil_" // cache nil to avoid penetration.
			err = nil     // clear err in case of sql.ErrNoRows.
		}
		return
	})
	if err != nil {
		panic(err)
	} else if val == "_nil_" {
		val = ""
		err = sql.ErrNoRows
	} else {
		// ...
	}
}
```

## Limitation

Currently, requires Valkey >= 7.0.