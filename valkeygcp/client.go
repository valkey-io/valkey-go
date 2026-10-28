package valkeygcp

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/valkey-io/valkey-go"
)

// Option configures what NewClient and ResolveClientOption add to a valkey.ClientOption:
// IAM credentials (WithIAMOptions, WithDefaultIAM). Settings valkey-go already has a field
// for, such as addresses and timeouts, are set on the valkey.ClientOption itself.
// Later options override earlier ones of the same kind.
type Option func(*config)

// config collects the Options of one NewClient or ResolveClientOption call.
type config struct {
	iam *IAMAuthOptions // IAM settings (WithIAMOptions, WithDefaultIAM); nil when not given
}

// newConfig applies opts in order, skipping nil ones.
func newConfig(opts []Option) config {
	var c config
	for _, o := range opts {
		if o != nil {
			o(&c)
		}
	}
	return c
}

// Connection defaults NewClient fills into the valkey.ClientOption fields left at zero.
const (
	defaultDialTimeout      = 2 * time.Second
	defaultKeepAlive        = 1 * time.Second
	defaultConnWriteTimeout = 3 * time.Second
	// defaultShardsRefreshInterval is how often a cluster client re-reads the cluster
	// topology. valkey-go starts each refresh after a short random delay, so many clients
	// don't query the cluster at the same moment.
	defaultShardsRefreshInterval = 10 * time.Second
	// setupTimeout caps one-off setup calls (credential discovery) when the caller's context
	// has no earlier deadline.
	setupTimeout = 15 * time.Second
)

var errNoEndpoint = errors.New("valkeygcp: no endpoint: set ClientOption.InitAddress")

// NewClient returns a valkey.Client for Memorystore for Valkey. It takes the same
// valkey.ClientOption as valkey.NewClient: InitAddress holds the discovery endpoint (cluster
// mode) or the primary endpoint (cluster mode disabled), and Standalone.ReplicaAddress the
// reader endpoint, if any. NewClient fills the Memorystore defaults into the fields left at
// their zero value, adds the credentials opts ask for, and connects.
//
// ctx bounds credential setup; a ctx that has ended by then stops NewClient before it
// connects. valkey-go connects and authenticates while it builds the client, so setup needs
// working credentials. Token refreshes never use ctx.
func NewClient(ctx context.Context, option valkey.ClientOption, opts ...Option) (valkey.Client, error) {
	opt, err := ResolveClientOption(ctx, option, opts...)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err // valkey.NewClient takes no context
	}
	return valkey.NewClient(opt)
}

// ResolveClientOption returns the valkey.ClientOption NewClient would connect with, without
// connecting, for callers who build the client themselves (valkey.NewClient,
// valkeyotel.NewClient) or want to inspect it.
func ResolveClientOption(ctx context.Context, option valkey.ClientOption, opts ...Option) (valkey.ClientOption, error) {
	cfg := newConfig(opts)
	if len(option.InitAddress) == 0 {
		return valkey.ClientOption{}, errNoEndpoint
	}
	if field := callerAuthField(option); field != "" && cfg.iam != nil {
		return valkey.ClientOption{}, fmt.Errorf("valkeygcp: ClientOption.%s conflicts with WithIAMOptions, which supplies the AUTH username (IAMAuthOptions.Username) and token", field)
	}
	opt := withDefaults(option)
	if cfg.iam != nil {
		p, err := NewGCPTokenProvider(ctx, *cfg.iam)
		if err != nil {
			return valkey.ClientOption{}, err
		}
		opt.AuthCredentialsFn = p.AuthCredentialsFn()
	}
	return opt, nil
}

// withDefaults fills the Memorystore defaults into the fields of opt left at their zero
// value.
func withDefaults(opt valkey.ClientOption) valkey.ClientOption {
	if len(opt.Standalone.ReplicaAddress) > 0 {
		if opt.SendToReplicas == nil {
			opt.SendToReplicas = sendReadsToReplicas
		}
	} else {
		// Cluster mode, or not known before connecting. valkey-go reads these only in its
		// cluster client, so they are harmless when it falls back to a single-node client.
		// A false bool can't be told apart from an unset one, so the bools are always on.
		opt.ClusterOption.PreferInitAddressRefresh = true // topology only through InitAddress
		opt.ClusterOption.PreferClusterShards = true
		if opt.ClusterOption.ShardsRefreshInterval == 0 {
			opt.ClusterOption.ShardsRefreshInterval = defaultShardsRefreshInterval
		}
	}
	if opt.Dialer.Timeout == 0 {
		opt.Dialer.Timeout = defaultDialTimeout
	}
	if opt.Dialer.KeepAlive == 0 {
		opt.Dialer.KeepAlive = defaultKeepAlive
	}
	if opt.ConnWriteTimeout == 0 {
		opt.ConnWriteTimeout = defaultConnWriteTimeout
	}
	if opt.RetryDelay == nil {
		opt.RetryDelay = memorystoreRetryDelay // exponential backoff with full jitter
	}
	return opt
}

// callerAuthField names the ClientOption credential field the caller set, if any.
// Caller-managed credentials are used as is; an IAM option next to them is an error.
// Username counts too: valkey-go ignores it once AuthCredentialsFn is set, so next to an
// IAM option it would be dropped without a word.
func callerAuthField(o valkey.ClientOption) string {
	switch {
	case o.AuthCredentialsFn != nil:
		return "AuthCredentialsFn"
	case o.Password != "":
		return "Password"
	case o.Username != "":
		return "Username"
	}
	return ""
}

// sendReadsToReplicas routes read-only commands to the reader endpoint, except blocking
// ones.
func sendReadsToReplicas(cmd valkey.Completed) bool {
	// A blocking read, such as XREAD with BLOCK, waits for new data. New data reaches the
	// primary first, so blocking reads stay on the primary.
	return cmd.IsReadOnly() && !cmd.IsBlock()
}
