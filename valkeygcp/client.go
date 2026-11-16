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

// Defaults NewClient fills into the valkey.ClientOption fields left at zero. Timeouts are
// not among them: Dialer.Timeout, Dialer.KeepAlive and ConnWriteTimeout keep valkey-go's
// defaults.
const (
	// defaultShardsRefreshInterval is how often a cluster client re-reads the cluster
	// topology. valkey-go starts each refresh after a short random delay, so many clients
	// don't query the cluster at the same moment.
	defaultShardsRefreshInterval = 10 * time.Second
	// setupTimeout caps one-off setup calls (finding credentials, impersonation, the
	// principal-email lookup) and connecting, when the caller's context has no earlier
	// deadline.
	setupTimeout = 15 * time.Second
)

var errNoEndpoint = errors.New("valkeygcp: no endpoint: set ClientOption.InitAddress")

// NewClient returns a valkey.Client for Memorystore for Valkey. It takes the same
// valkey.ClientOption as valkey.NewClient: InitAddress holds the discovery endpoint (cluster
// mode) or the primary endpoint (cluster mode disabled), and Standalone.ReplicaAddress the
// reader endpoint, if any. NewClient fills the Memorystore defaults into the fields left at
// their zero value, adds the credentials opts ask for, and connects.
//
// ctx bounds credential setup and connecting, each of which also stops after 15s when ctx has
// no earlier deadline; ctx is checked between connection attempts, not during one. valkey-go
// connects and authenticates while it builds the client, so setup needs working credentials.
// Memorystore IAM backend errors (ERR_IAM_EXHAUSTED, ERR_IAM_OTHER) during the handshake are
// retried with backoff. Memorystore also answers an invalid username with ERR_IAM_OTHER, so
// that error is retried only a few times for a username other than "default" (or none) that
// setup couldn't check against the IAM principal, which includes any such username of
// credentials set on the ClientOption. When an ACL rule refuses CLIENT TRACKING, the error
// says so. Token refreshes never use ctx.
func NewClient(ctx context.Context, option valkey.ClientOption, opts ...Option) (valkey.Client, error) {
	opt, usernameValid, err := resolveClientOption(ctx, option, opts...)
	if err != nil {
		return nil, err
	}
	return connect(ctx, opt, usernameValid)
}

// ResolveClientOption returns the valkey.ClientOption NewClient would connect with, without
// connecting, for callers who build the client themselves (valkey.NewClient,
// valkeyotel.NewClient) or want to inspect it. The option keeps the credentials, the
// defaults and the dial gate of WithIAMOptions. Building the client that way skips
// NewClient's connect-time handling: the retries of Memorystore IAM backend errors during
// the handshake, and the CLIENT TRACKING hint.
func ResolveClientOption(ctx context.Context, option valkey.ClientOption, opts ...Option) (valkey.ClientOption, error) {
	opt, _, err := resolveClientOption(ctx, option, opts...)
	return opt, err
}

// resolveClientOption is ResolveClientOption. It also reports whether setup knows that
// Memorystore accepts the IAM username (see checkIAMUsername), which tells connect how long to
// retry ERR_IAM_OTHER. With credentials set on the ClientOption instead, such as the ones
// ResolveClientOption returns, it doesn't know, and connect looks at the username they send.
func resolveClientOption(ctx context.Context, option valkey.ClientOption, opts ...Option) (valkey.ClientOption, bool, error) {
	cfg := newConfig(opts)
	if len(option.InitAddress) == 0 {
		return valkey.ClientOption{}, false, errNoEndpoint
	}
	if field := callerAuthField(option); field != "" && cfg.iam != nil {
		return valkey.ClientOption{}, false, fmt.Errorf("valkeygcp: ClientOption.%s conflicts with WithIAMOptions, which supplies the AUTH token", field)
	}
	opt := withDefaults(option)
	usernameValid := false
	if cfg.iam != nil {
		iam, err := withCallerUsername(*cfg.iam, option.Username)
		if err != nil {
			return valkey.ClientOption{}, false, err
		}
		p, err := newIAMTokenProvider(ctx, iam)
		if err != nil {
			return valkey.ClientOption{}, false, err
		}
		opt.AuthCredentialsFn = p.AuthCredentialsFn()
		installDialGate(&opt) // after an ERR_IAM_* rejection, new dials wait with backoff
		usernameValid = p.usernameValid
	}
	return opt, usernameValid, nil
}

// withDefaults fills the Memorystore defaults into the fields of opt left at their zero
// value.
func withDefaults(opt valkey.ClientOption) valkey.ClientOption {
	if len(opt.Standalone.ReplicaAddress) > 0 {
		if opt.SendToReplicas == nil {
			opt.SendToReplicas = sendReadsToReplicas
		}
	} else if opt.ClusterOption == (valkey.ClusterOption{}) {
		// Cluster mode, or not known before connecting. valkey-go reads these only in its
		// cluster client, so they are harmless when it falls back to a single-node client.
		// A false bool can't be told apart from an unset one, so ClusterOption is filled
		// only when the caller left all of it at zero: setting any of its fields leaves the
		// whole ClusterOption to the caller, who can then turn each of these off.
		opt.ClusterOption = valkey.ClusterOption{
			ShardsRefreshInterval:    defaultShardsRefreshInterval,
			PreferInitAddressRefresh: true, // topology only through InitAddress
			PreferClusterShards:      true,
		}
	}
	if opt.RetryDelay == nil {
		opt.RetryDelay = memorystoreRetryDelay // exponential backoff with full jitter
	}
	return opt
}

// callerAuthField names the ClientOption credential field the caller set, if any.
// Caller-managed credentials are used as is; an IAM option next to them is an error.
// Username is not one of them: next to an IAM option it is the IAM username (see
// withCallerUsername).
func callerAuthField(o valkey.ClientOption) string {
	switch {
	case o.AuthCredentialsFn != nil:
		return "AuthCredentialsFn"
	case o.Password != "":
		return "Password"
	}
	return ""
}

// withCallerUsername returns iam with ClientOption.Username as the IAM username when
// IAMAuthOptions.Username is empty. valkey-go ignores ClientOption.Username once
// AuthCredentialsFn is set, so this keeps it from being dropped without a word. Two
// different usernames are an error: only one can be sent. iam is a copy, so the
// caller's Option is not changed.
func withCallerUsername(iam IAMAuthOptions, username string) (IAMAuthOptions, error) {
	switch {
	case username == "" || username == iam.Username:
	case iam.Username == "":
		iam.Username = username
	default:
		return IAMAuthOptions{}, fmt.Errorf("valkeygcp: ClientOption.Username %q and IAMAuthOptions.Username %q differ; set only one of them", username, iam.Username)
	}
	return iam, nil
}

// sendReadsToReplicas routes read-only commands to the reader endpoint, except blocking
// ones.
func sendReadsToReplicas(cmd valkey.Completed) bool {
	// A blocking read, such as XREAD with BLOCK, waits for new data. New data reaches the
	// primary first, so blocking reads stay on the primary.
	return cmd.IsReadOnly() && !cmd.IsBlock()
}
