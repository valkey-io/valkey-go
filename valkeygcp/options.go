package valkeygcp

import (
	"context"
	"time"

	"github.com/valkey-io/valkey-go"
)

// defaultReadYourWritesWindow is the window used by WithReadYourWrites when the given window is not positive.
const defaultReadYourWritesWindow = 500 * time.Millisecond

// Option configures NewClient.
type Option func(*options)

type options struct {
	readYourWritesWindow time.Duration
	readerEndpoint       string
}

// WithReadYourWrites makes the client send reads of a hash slot to its primary for window after the client
// writes to that slot, so that callers read their own writes despite asynchronous replication.
// A window <= 0 means 500ms. Only writes sent with Do and DoMulti are tracked.
func WithReadYourWrites(window time.Duration) Option {
	if window <= 0 {
		window = defaultReadYourWritesWindow
	}
	return func(o *options) {
		o.readYourWritesWindow = window
	}
}

// WithReaderEndpoint sets addr, the reader endpoint of a Memorystore instance in Cluster Mode Disabled, as
// ClientOption.Standalone.ReplicaAddress, so that NewClient sends read-only commands to it and the other
// commands to the primary endpoint in ClientOption.InitAddress. It replaces a ReplicaAddress already set.
// An empty addr changes nothing.
func WithReaderEndpoint(addr string) Option {
	return func(o *options) {
		o.readerEndpoint = addr
	}
}

// NewGCPClusterClientOption returns option set up to send read-only commands to replicas in the caller's GCP zone,
// then to replicas in other zones, and then to the primary.
// It enables ClientOption.EnableReplicaAZInfo and, unless already set, uses valkey.DefaultSendToReplicas as
// ClientOption.SendToReplicas and valkey.AZAffinityNodeSelector with the zone from DetectCurrentGCPZone as
// ClientOption.ReadNodeSelector.
// Zone matching needs the servers to report their availability zone; otherwise reads are spread across replicas.
func NewGCPClusterClientOption(ctx context.Context, option valkey.ClientOption) valkey.ClientOption {
	option.EnableReplicaAZInfo = true
	if option.SendToReplicas == nil {
		option.SendToReplicas = valkey.DefaultSendToReplicas
	}
	if option.ReadNodeSelector == nil {
		option.ReadNodeSelector = valkey.AZAffinityNodeSelector(DetectCurrentGCPZone(ctx))
	}
	return option
}

// NewGCPStandaloneClientOption returns option set up for a Memorystore instance in Cluster Mode Disabled, with the
// primary endpoint in ClientOption.InitAddress and the reader endpoint in ClientOption.Standalone.ReplicaAddress:
// read-only commands go to the reader endpoint, and writes, blocking commands and pub/sub commands go to the
// primary endpoint. Unless already set, it uses valkey.DefaultSendToReplicas as ClientOption.SendToReplicas.
// It doesn't detect the zone: the reader endpoint, not the client, picks the replica.
// Without a reader endpoint, option is returned unchanged.
func NewGCPStandaloneClientOption(option valkey.ClientOption) valkey.ClientOption {
	if len(option.Standalone.ReplicaAddress) > 0 && option.SendToReplicas == nil {
		option.SendToReplicas = valkey.DefaultSendToReplicas
	}
	return option
}

// NewClient creates a valkey.Client for a Memorystore instance:
//   - With a reader endpoint, given with WithReaderEndpoint or in ClientOption.Standalone.ReplicaAddress, it uses
//     NewGCPStandaloneClientOption: read-only commands go to the reader endpoint, and the other commands to the
//     primary endpoint in ClientOption.InitAddress. If the reader endpoint refuses connections because the
//     instance has no replicas, the client uses the primary endpoint only.
//   - Otherwise, it uses NewGCPClusterClientOption: read-only commands go to replicas in the caller's zone first.
func NewClient(ctx context.Context, option valkey.ClientOption, opts ...Option) (valkey.Client, error) {
	var o options
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if o.readerEndpoint != "" {
		option.Standalone.ReplicaAddress = []string{o.readerEndpoint}
	}
	if len(option.Standalone.ReplicaAddress) > 0 {
		option = NewGCPStandaloneClientOption(option)
	} else {
		option = NewGCPClusterClientOption(ctx, option)
	}
	return connect(option, o.readYourWritesWindow)
}
