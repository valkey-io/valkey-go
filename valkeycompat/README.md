## Go-redis like API Adapter

Though it is easier to know what command will be sent to valkey at first glance if the command is constructed by the command builder,
users may sometimes feel it too verbose to write.

For users who don't like the command builder, `valkeycompat.Adapter`, contributed mainly by [@418Coffee](https://github.com/418Coffee), is an alternative.
It is a high level API which is close to go-redis's `Cmdable` interface.

### Migrating from go-redis

You can also try adapting `valkey` with existing go-redis code by replacing go-redis's `UniversalClient` with `valkeycompat.Adapter`.

### Client side caching

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