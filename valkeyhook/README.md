# valkeyhook

With `valkeyhook.WithHook`, users can easily intercept `valkey.Client` by implementing custom `valkeyhook.Hook` handler.

This can be useful to change the behavior of `valkey.Client` or add other integrations such as observability, APM, etc.

## Example

```go
package main

import (
	"context"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeyhook"
)

type hook struct{}

func (h *hook) Do(client valkey.Client, ctx context.Context, cmd valkey.Completed) (resp valkey.ValkeyResult) {
	// do whatever you want before a client.Do
	resp = client.Do(ctx, cmd)
	// do whatever you want after a client.Do
	return
}

func (h *hook) DoMulti(client valkey.Client, ctx context.Context, multi ...valkey.Completed) (resps []valkey.ValkeyResult) {
	// do whatever you want before a client.DoMulti
	resps = client.DoMulti(ctx, multi...)
	// do whatever you want after a client.DoMulti
	return
}

func (h *hook) DoCache(client valkey.Client, ctx context.Context, cmd valkey.Cacheable, ttl time.Duration) (resp valkey.ValkeyResult) {
	// do whatever you want before a client.DoCache
	resp = client.DoCache(ctx, cmd, ttl)
	// do whatever you want after a client.DoCache
	return
}

func (h *hook) DoMultiCache(client valkey.Client, ctx context.Context, multi ...valkey.CacheableTTL) (resps []valkey.ValkeyResult) {
	// do whatever you want before a client.DoMultiCache
	resps = client.DoMultiCache(ctx, multi...)
	// do whatever you want after a client.DoMultiCache
	return
}

func (h *hook) Receive(client valkey.Client, ctx context.Context, subscribe valkey.Completed, fn func(msg valkey.PubSubMessage)) (err error) {
	// do whatever you want before a client.Receive
	err = client.Receive(ctx, subscribe, fn)
	// do whatever you want after a client.Receive
	return
}

func main() {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
	if err != nil {
		panic(err)
	}
	client = valkeyhook.WithHook(client, &hook{})
	defer client.Close()
}
```