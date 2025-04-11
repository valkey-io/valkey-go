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

Client side commmand metrics:
 - `valkey_command_duration_seconds`: histogram of command duration
 - `valkey_command_errors`: number of command errors

```golang
package main

import (
    "github.com/valkey-io/valkey-go"
    "github.com/valkey-io/valkey-go/valkeyotel"
)

func main() {
    client, err := valkeyotel.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6379"}})
    if err != nil {
        panic(err)
    }
    defer client.Close()
}
```

See [valkeyhook](../valkeyhook) if you want more customizations.

Note: `valkeyotel.NewClient` is not supported on go1.18 and go1.19 builds. [Reference](https://github.com/redis/rueidis/issues/442#issuecomment-1886993707)