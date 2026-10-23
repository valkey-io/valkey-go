# valkeygcp

Zone-aware replica reads for valkey-go on Google Cloud, such as Memorystore cluster instances with replicas in several zones:

- Read-only commands go to a replica in the zone of the caller first, avoiding the latency and the cross-zone network charges
  of other zones, then to replicas in other zones, and then to the primary.
- Blocking commands, such as `XREAD` with `BLOCK`, and pub/sub commands always go to the primary.
- Optionally, reads of keys the client has just written go to the primary, so that the client reads its own writes
  despite asynchronous replication.

```go
package main

import (
	"context"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeygcp"
)

func main() {
	client, err := valkeygcp.NewClient(context.Background(), valkey.ClientOption{
		InitAddress: []string{"10.0.0.3:6379"},
	}, valkeygcp.WithReadYourWrites(500*time.Millisecond))
	if err != nil {
		panic(err)
	}
	defer client.Close()
}
```

## Zone Detection

`DetectCurrentGCPZone` returns the zone of the caller, such as `us-central1-a`. It uses the `GCP_ZONE` or `CLOUD_ZONE`
environment variable if set, and otherwise asks the Compute Engine metadata server, waiting at most 100ms.
A full resource path, such as `projects/123456789/zones/us-central1-a`, is reduced to the zone name.
It returns an empty string if the zone can't be determined, such as outside Google Cloud.

Zone matching needs the servers to report their availability zone. Valkey reports it when its `availability-zone` config is set.
Otherwise, reads are spread across all replicas.

## Using Another Client Constructor

`NewGCPClusterClientOption` returns the `valkey.ClientOption` used by `NewClient`, so that it can be passed to other
constructors, such as `valkeyotel.NewClient`:

```go
client, err := valkeyotel.NewClient(valkeygcp.NewGCPClusterClientOption(ctx, valkey.ClientOption{
	InitAddress: []string{"10.0.0.3:6379"},
}))
```

It enables `EnableReplicaAZInfo` and, unless already set, uses `valkey.DefaultSendToReplicas` as `SendToReplicas`
and `valkey.AZAffinityNodeSelector` with the detected zone as `ReadNodeSelector`.

## Read Your Writes

With `WithReadYourWrites(window)`, after the client writes to a hash slot, reads of that slot go to the primary for
`window`, or 500ms if `window <= 0`. Choose a window longer than the replication lag of your instance.

Only writes sent with `Do` and `DoMulti` of a cluster client are tracked: writes sent with `Dedicated`, `Dedicate`,
`DoStream` or `DoMultiStream`, and writes made by other clients, are not.

