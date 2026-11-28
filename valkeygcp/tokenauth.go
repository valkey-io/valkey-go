package valkeygcp

import (
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"regexp"
	"strings"
	"sync"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/api/option"
)

// DefaultTokenReloadInterval is how often Token Auth re-reads the token by default, so a
// rotated token reaches live connections.
const DefaultTokenReloadInterval = 5 * time.Minute

// tokenFetchTimeout limits one token read made while connecting or re-authenticating.
const tokenFetchTimeout = 5 * time.Second

// minTokenReloadInterval is the shortest positive TokenAuthOptions.ReloadInterval. A shorter
// one is almost always a unit slip, such as 5 for 5*time.Minute, and would make every
// connection re-AUTH, and the token be read, in a tight loop. It is a variable so tests can
// use shorter intervals.
var minTokenReloadInterval = time.Second

var errNoTokenSource = errors.New("valkeygcp: WithTokenAuth needs TokenAuthOptions.SecretVersion, Token or Source")

// TokenAuthOptions configures Token Auth, for instances whose authorization mode is Token
// Auth: clients log in with an auth token that Memorystore issues to a token auth user. Set
// one token source: SecretVersion (recommended), Token or Source.
type TokenAuthOptions struct {
	// Username is the token auth user. Empty means ClientOption.Username, and when that is
	// empty too, the default user: valkey-go then sends AUTH default <token>.
	Username string
	// SecretVersion is the Secret Manager secret version that holds the token, for example
	// "projects/P/secrets/valkey-token/versions/latest", or
	// "projects/P/locations/L/secrets/valkey-token/versions/latest" for a regional secret.
	// It is read with the Secret Manager client library and Application Default
	// Credentials, whose identity needs roles/secretmanager.secretAccessor on the secret.
	// For another endpoint or other credentials, use Source.
	SecretVersion string
	// Token is a fixed token, for local development. A fixed token can't follow a rotation.
	Token string
	// Source reads the token your own way; it takes precedence over SecretVersion and Token.
	// It should return when ctx ends: reads while connecting or re-authenticating get a 5s
	// deadline, and the first read gets NewClient's ctx, capped at 15s. A read still running
	// at its deadline is abandoned and counts as a failed read, so a later read may run
	// while it is still going.
	Source func(ctx context.Context) (string, error)
	// ReloadInterval is how often live connections re-AUTH with the current token, so a
	// rotated token reaches them. The token itself is read at most once per half interval,
	// however many connections ask for it. Zero means DefaultTokenReloadInterval, or no
	// reloading when the source is Token, which can't change; negative turns reloading off.
	// A positive interval must be at least 1s.
	ReloadInterval time.Duration
	// OnCredentialError receives the token read errors that valkeygcp works around instead
	// of returning: when a re-read fails, connections keep the last token read successfully,
	// and the read is retried within 30s. Use it for logs and metrics.
	OnCredentialError func(error)
}

// tokenAuthProvider hands out the Token Auth credentials of one NewClient or
// ResolveClientOption call.
type tokenAuthProvider struct {
	opt       TokenAuthOptions
	fetch     func(ctx context.Context) (string, error)
	mu        sync.Mutex
	last      string    // the last token read successfully
	nextFetch time.Time // when the token may be read again
	failing   bool      // the last read failed: connections re-AUTH within the retry interval
}

// newSecretSource returns a reader for one Secret Manager secret version. It is a variable
// so tests can replace Secret Manager.
var newSecretSource = func(ctx context.Context, version string) (func(context.Context) (string, error), error) {
	m := secretVersionName.FindStringSubmatch(version)
	if m == nil {
		return nil, fmt.Errorf("valkeygcp: secret version %q is not projects/P/secrets/S/versions/V or projects/P/locations/L/secrets/S/versions/V", version)
	}
	client, err := sharedSecretClient(ctx, m[2])
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) (string, error) { return accessSecretVersion(ctx, client, version) }, nil
}

// secretVersionName matches a secret version name, global or regional. Group 2 is the
// location of a regional secret, which is read through that location's endpoint. Project
// IDs can hold a domain, as in example.com:p.
var secretVersionName = regexp.MustCompile(`^projects/\w[\w.:-]*/(locations/([\w-]+)/)?secrets/[\w-]+/versions/[\w-]+$`)

// crc32c checks secret payloads against the checksum Secret Manager sends with them.
var crc32c = crc32.MakeTable(crc32.Castagnoli)

// regionalEndpoint is the Secret Manager endpoint for the regional secrets of location.
func regionalEndpoint(location string) string {
	return "secretmanager." + location + ".rep.googleapis.com:443"
}

// newSecretManagerClient creates a Secret Manager client with Application Default
// Credentials, for the global endpoint, or for location's endpoint when location isn't
// empty. It is a variable so tests can use a local server.
var newSecretManagerClient = func(ctx context.Context, location string) (*secretmanager.Client, error) {
	var opts []option.ClientOption
	if location != "" {
		opts = append(opts, option.WithEndpoint(regionalEndpoint(location)))
	}
	return secretmanager.NewClient(ctx, opts...)
}

// accessSecretVersion reads one secret version. The client library retries a busy or
// unreachable API within ctx's deadline, and returns other errors, such as
// PERMISSION_DENIED or NOT_FOUND, at once.
func accessSecretVersion(ctx context.Context, c *secretmanager.Client, version string) (string, error) {
	resp, err := c.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: version})
	if err != nil {
		return "", fmt.Errorf("secret manager: %w", err)
	}
	payload := resp.GetPayload()
	if sum := payload.DataCrc32C; sum != nil && *sum != int64(crc32.Checksum(payload.GetData(), crc32c)) {
		return "", errors.New("secret manager: payload checksum mismatch")
	}
	return string(payload.GetData()), nil
}

// One Secret Manager client per endpoint, the global one and one per location of a
// regional secret, serves every Token Auth provider in the process, with one connection and
// one Application Default Credentials token cache: valkey.Client has no close hook that
// could close a client per provider. A client is created on first use and kept for the life
// of the process; a failed creation is retried on the next call. secretMu guards
// secretClients only, not their creation: finding the credentials can hang, and a setup
// that has given up on it must not block later ones.
var (
	secretMu      sync.Mutex
	secretClients = map[string]*secretmanager.Client{}
)

// sharedSecretClient returns the process-wide client for location ("" for the global
// endpoint), creating it on first use. Callers that find none each create one; the first
// one stored is kept and the others are closed.
func sharedSecretClient(ctx context.Context, location string) (*secretmanager.Client, error) {
	secretMu.Lock()
	c := secretClients[location]
	secretMu.Unlock()
	if c != nil {
		return c, nil
	}
	// The client outlives ctx, so it must not inherit ctx's cancellation.
	c, err := newSecretManagerClient(context.WithoutCancel(ctx), location)
	if err != nil {
		return nil, fmt.Errorf("valkeygcp: secret manager client: %w", err)
	}
	secretMu.Lock()
	kept := secretClients[location]
	if kept == nil {
		secretClients[location] = c
	}
	secretMu.Unlock()
	if kept != nil {
		c.Close() // another setup stored its client first
		return kept, nil
	}
	return c, nil
}

// newTokenAuthProvider builds the Token Auth provider and reads the token once, so a wrong
// secret fails NewClient instead of the first connection. ctx bounds the setup and that
// first read, which also stop after 15s when ctx has no earlier deadline.
func newTokenAuthProvider(ctx context.Context, opt TokenAuthOptions) (*tokenAuthProvider, error) {
	switch {
	case opt.ReloadInterval == 0 && opt.Source == nil && opt.SecretVersion == "" && opt.Token != "":
		opt.ReloadInterval = -1 // a fixed token can't change, so re-AUTHing would resend it
	case opt.ReloadInterval == 0:
		opt.ReloadInterval = DefaultTokenReloadInterval
	case opt.ReloadInterval > 0 && opt.ReloadInterval < minTokenReloadInterval:
		return nil, fmt.Errorf("valkeygcp: TokenAuthOptions.ReloadInterval %v is below the %v minimum", opt.ReloadInterval, minTokenReloadInterval)
	}
	setupCtx, cancel := context.WithTimeout(ctx, setupTimeout)
	defer cancel()
	p := &tokenAuthProvider{opt: opt}
	switch {
	case opt.Source != nil:
		p.fetch = abandonAtDeadline(opt.Source)
	case opt.SecretVersion != "":
		// Finding the credentials can take a while, so setupCtx bounds it like the first read.
		fetch, err := callWithContext(setupCtx, "secret manager setup", func() (func(context.Context) (string, error), error) {
			return newSecretSource(ctx, opt.SecretVersion)
		})
		if err != nil {
			return nil, err
		}
		// The request honors ctx, but fetching an access token for it may not.
		p.fetch = abandonAtDeadline(fetch)
	case opt.Token != "":
		p.fetch = func(context.Context) (string, error) { return opt.Token, nil }
	default:
		return nil, errNoTokenSource
	}
	if _, _, err := p.token(setupCtx); err != nil { // fail before the first connection
		return nil, fmt.Errorf("valkeygcp: initial token fetch: %w", err)
	}
	return p, nil
}

// token returns the current token, and whether the last read failed. A read error that the
// last token covers goes to OnCredentialError, called outside the lock because the callback
// may log or block.
func (p *tokenAuthProvider) token(ctx context.Context) (string, bool, error) {
	tok, failing, absorbed, err := p.cachedToken(ctx)
	if absorbed != nil && p.opt.OnCredentialError != nil {
		p.opt.OnCredentialError(absorbed)
	}
	return tok, failing, err
}

// cachedToken returns the token, reading it again at most once per half reload interval.
// failing reports that the last read failed; absorbed is the error of a read that failed
// just now, covered by the last token read successfully.
func (p *tokenAuthProvider) cachedToken(ctx context.Context) (tok string, failing bool, absorbed, err error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if p.last != "" && (p.opt.ReloadInterval < 0 || now.Before(p.nextFetch)) {
		return p.last, p.failing, nil, nil // no read for every new connection
	}
	tok, err = p.fetch(ctx)
	tok = strings.TrimSpace(tok) // secret payloads often end with a newline
	if err == nil && tok == "" {
		err = errors.New("empty token")
	}
	if err != nil {
		if p.last == "" {
			return "", false, nil, err
		}
		// Keep the last token: during a rotation the old and the new token both work for a
		// while. Read again after the retry interval instead of on every new connection.
		// Connections come back within that interval (see AuthCredentialsFn), and even the
		// earliest of them, whose jitter took 10% off, finds the read allowed.
		retry := p.retryInterval()
		p.failing, p.nextFetch = true, now.Add(retry-retry/10)
		return p.last, true, err, nil
	}
	p.last, p.failing, p.nextFetch = tok, false, now.Add(p.opt.ReloadInterval/2)
	return tok, false, nil, nil
}

// abandonAtDeadline returns read, but stops waiting for it when ctx ends. cachedToken holds
// its lock while it reads, so a read that ignores ctx would otherwise block every new
// connection and re-AUTH for as long as it hangs. The abandoned read finishes in the
// background and its result is dropped.
func abandonAtDeadline(read func(context.Context) (string, error)) func(context.Context) (string, error) {
	return func(ctx context.Context) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err // ended already: don't start a read
		}
		return callWithContext(ctx, "token read", func() (string, error) { return read(ctx) })
	}
}

// retryInterval is how soon a failed read is tried again: 30s, or half the reload interval
// when that is shorter.
func (p *tokenAuthProvider) retryInterval() time.Duration {
	return min(credentialRetry, p.opt.ReloadInterval/2)
}

// AuthCredentialsFn returns the valkey.ClientOption.AuthCredentialsFn of this provider:
// valkey-go calls it for every new connection and at every scheduled re-AUTH.
func (p *tokenAuthProvider) AuthCredentialsFn() func(valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
	return func(valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
		// valkey-go passes no context here, so each read gets its own deadline.
		ctx, cancel := context.WithTimeout(context.Background(), tokenFetchTimeout)
		defer cancel()
		tok, failing, err := p.token(ctx)
		if err != nil {
			return valkey.AuthCredentials{}, err
		}
		creds := valkey.AuthCredentials{Username: p.opt.Username, Password: tok} // empty Username: AUTH default <token>
		if p.opt.ReloadInterval > 0 {
			next := p.opt.ReloadInterval
			if failing {
				// Reads are failing: come back within the retry interval, as IAM does, so the
				// token is read again soon and reaches this connection once reads work.
				next = p.retryInterval()
			}
			creds.RefreshAfter = time.Now().Add(jitter(next))
		}
		return creds, nil
	}
}

// WithTokenAuth authenticates with Token Auth, using the token from opt's source. NewClient's
// ctx bounds the first token read. It can't be combined with WithIAMOptions or
// WithDefaultIAM: an instance uses either IAM or Token Auth.
func WithTokenAuth(opt TokenAuthOptions) Option {
	return func(c *config) { c.token = &opt }
}
