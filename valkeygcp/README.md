# valkeygcp

Replica reads for valkey-go on Google Cloud Memorystore for Valkey, in both instance modes:

- **Cluster Mode Enabled:** read-only commands go to a replica in the zone of the caller first, avoiding the latency and
  the cross-zone network charges of other zones, then to replicas in other zones, and then to the primary.
- **Cluster Mode Disabled:** one client sends read-only commands to the instance's reader endpoint and the other
  commands to its primary endpoint, so there's no need for separate clients for reads and writes.
- In both modes, blocking commands, such as `XREAD` with `BLOCK`, and pub/sub commands always go to the primary.
- Optionally, reads of keys the client has just written go to the primary, so that the client reads its own writes
  despite asynchronous replication.

## Cluster Mode Enabled

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
		InitAddress: []string{"10.0.0.3:6379"}, // discovery endpoint
	}, valkeygcp.WithReadYourWrites(500*time.Millisecond))
	if err != nil {
		panic(err)
	}
	defer client.Close()
}
```

## Cluster Mode Disabled: Reader Endpoint

Give the primary endpoint in `InitAddress`, and the reader endpoint with `WithReaderEndpoint`:

```go
client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{
	InitAddress: []string{"10.0.0.4:6379"}, // primary endpoint
}, valkeygcp.WithReaderEndpoint("10.0.0.5:6379"))
```

`WithReaderEndpoint` sets valkey-go's own `Standalone.ReplicaAddress`, so this is the same as:

```go
client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{
	InitAddress: []string{"10.0.0.4:6379"},                                          // primary endpoint
	Standalone:  valkey.StandaloneOption{ReplicaAddress: []string{"10.0.0.5:6379"}}, // reader endpoint
})
```

The client sends:

| Commands | To |
|---|---|
| Read-only commands, such as `GET`, `MGET`, `HGETALL` and `EVAL_RO`, sent with `Do`, `DoStream`, or `DoMulti` with only reads | Reader endpoint |
| Writes, blocking commands, pub/sub commands, transactions, and `DoMulti` batches with a write | Primary endpoint |
| Commands sent with `DoCache`, `DoMultiCache`, `Dedicated` or `Dedicate` | Primary endpoint |

- **No replicas.** Memorystore keeps the reader endpoint of an instance without replicas, but refuses its connections
  with `ERR no replicas found`. If that happens when the client is created, `NewClient` connects to the primary endpoint
  only; create the client again to read from replicas added later. If all replicas go away while the client runs,
  reads return errors until a replica is back: Memorystore doesn't send reader endpoint connections to the primary.
- **Failovers.** Memorystore closes the connections of both endpoints with `ERR role change occurred`. Commands in
  flight fail, and valkey-go reconnects on the next command.
- **Spreading reads.** The reader endpoint balances connections, not commands, across the replicas. By default, a client
  opens up to 4 connections to it, fewer with less than 4 CPUs. Set `PipelineMultiplex` for more connections, and
  `ConnLifetime` so that connections, with their reads, also move to replicas added later.
- **Zones.** The reader endpoint, not the client, picks the replica, so the zone isn't detected in this mode.

## Zone Detection

In Cluster Mode Enabled, `DetectCurrentGCPZone` returns the zone of the caller, such as `us-central1-a`. It uses the
`GCP_ZONE` or `CLOUD_ZONE` environment variable if set, and otherwise asks the Compute Engine metadata server, waiting at
most 100ms. A full resource path, such as `projects/123456789/zones/us-central1-a`, is reduced to the zone name.
It returns an empty string if the zone can't be determined, such as outside Google Cloud.

Zone matching needs the servers to report their availability zone. Valkey reports it when its `availability-zone` config is set.
Otherwise, reads are spread across all replicas.

## Using Another Client Constructor

`NewGCPClusterClientOption` and `NewGCPStandaloneClientOption` return the `valkey.ClientOption` used by `NewClient`, so
that it can be passed to other constructors, such as `valkeyotel.NewClient`:

```go
// Cluster Mode Enabled
client, err := valkeyotel.NewClient(valkeygcp.NewGCPClusterClientOption(ctx, valkey.ClientOption{
	InitAddress: []string{"10.0.0.3:6379"},
}))

// Cluster Mode Disabled
client, err := valkeyotel.NewClient(valkeygcp.NewGCPStandaloneClientOption(valkey.ClientOption{
	InitAddress: []string{"10.0.0.4:6379"},
	Standalone:  valkey.StandaloneOption{ReplicaAddress: []string{"10.0.0.5:6379"}},
}))
```

`NewGCPClusterClientOption` enables `EnableReplicaAZInfo` and, unless already set, uses `valkey.DefaultSendToReplicas`
as `SendToReplicas` and `valkey.AZAffinityNodeSelector` with the detected zone as `ReadNodeSelector`.
`NewGCPStandaloneClientOption` uses `valkey.DefaultSendToReplicas` as `SendToReplicas` unless already set.
Unlike `NewClient`, other constructors don't fall back to the primary endpoint when the instance has no replicas.

## Read Your Writes

With `WithReadYourWrites(window)`, after the client writes to a hash slot, reads of that slot go to the primary for
`window`, or 500ms if `window <= 0`. Choose a window longer than the replication lag of your instance.
It works in both modes.

Only writes sent with `Do` and `DoMulti` are tracked: writes sent with `Dedicated`, `Dedicate`, `DoStream` or
`DoMultiStream`, and writes made by other clients, are not.
