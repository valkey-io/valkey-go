# OpenTelemetry Tracing & Connection Metrics

Use `valkeyotel.NewClient` to create a client with OpenTelemetry Tracing and Connection Metrics enabled.
Builtin connection metrics are:
- `valkey_dial_attempt`: number of dial attempts
- `valkey_dial_success`: number of successful dials
- `valkey_dial_conns`: number of connections
- `valkey_dial_latency`: dial latency in seconds

Client side caching metrics:
- `valkey_do_cache_miss`: number of cache miss on client side
- `valkey_do_cache_hits`: number of cache hits on client side

These metrics can include additional labels using `Labeler` (see below).

Client side command metrics:
 - `valkey_command_duration_seconds`: histogram of command duration
 - `valkey_command_errors`: number of command errors

```golang
package main

import (
    "context"
    "time"

    "github.com/valkey-io/valkey-go"
    "github.com/valkey-io/valkey-go/valkeyotel"
    "go.opentelemetry.io/otel/attribute"
)

func main() {
    client, err := valkeyotel.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
    if err != nil {
        panic(err)
    }
    defer client.Close()

    // Basic usage
    ctx := context.Background()
    client.DoCache(ctx, client.B().Get().Key("mykey").Cache(), time.Minute)

    // Add custom labels to cache metrics using Labeler
    bookLabeler := &valkeyotel.Labeler{}
    bookLabeler.Add(attribute.String("key_pattern", "book"))
    ctxBook := valkeyotel.ContextWithLabeler(ctx, bookLabeler)
    client.DoCache(ctxBook, client.B().Get().Key("book:123").Cache(), time.Minute)

    // Track with multiple attributes
    authorLabeler := &valkeyotel.Labeler{}
    authorLabeler.Add(
        attribute.String("key_pattern", "author"),
        attribute.String("tenant", "acme"),
    )
    ctxAuthor := valkeyotel.ContextWithLabeler(ctx, authorLabeler)
    client.DoCache(ctxAuthor, client.B().Get().Key("author:456").Cache(), time.Minute)
}
```

See [valkeyhook](../valkeyhook) if you want more customizations.

Note: `valkeyotel.NewClient` is not supported on go1.18 and go1.19 builds. [Reference](https://github.com/redis/rueidis/issues/442#issuecomment-1886993707)