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
}

// WithReadYourWrites makes the client send reads of a hash slot to its primary for window after the client
// writes to that slot, so that callers read their own writes despite asynchronous replication.
// A window <= 0 means 500ms. Only writes sent with Do and DoMulti of cluster clients are tracked.
func WithReadYourWrites(window time.Duration) Option {
	if window <= 0 {
		window = defaultReadYourWritesWindow
	}
	return func(o *options) {
		o.readYourWritesWindow = window
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

// NewClient creates a valkey.Client from NewGCPClusterClientOption(ctx, option) and opts.
func NewClient(ctx context.Context, option valkey.ClientOption, opts ...Option) (valkey.Client, error) {
	var o options
	for _, opt := range opts {
		opt(&o)
	}
	option = NewGCPClusterClientOption(ctx, option)
	if o.readYourWritesWindow == 0 {
		return valkey.NewClient(option)
	}
	return newReadYourWrites(option, o.readYourWritesWindow)
}
