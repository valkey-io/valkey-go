package valkeygcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/valkey-io/valkey-go"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
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
	// It is read through Secret Manager's REST API with Application Default Credentials,
	// whose identity needs roles/secretmanager.secretAccessor on the secret. For another
	// endpoint or other credentials, use Source.
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
	// however many connections ask for it. Zero means DefaultTokenReloadInterval; negative
	// turns reloading off. A positive interval must be at least 1s.
	ReloadInterval time.Duration
	// OnCredentialError receives the token read errors that valkeygcp works around instead
	// of returning: when a re-read fails, connections keep the last token that worked, and
	// the read is retried within 30s. Use it for logs and metrics.
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
	read, err := sharedSecretReader(ctx)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context) (string, error) { return read(ctx, version) }, nil
}

// The Secret Manager API endpoints, global and regional (%s is the location). They are
// variables so tests can use a local server.
var (
	secretManagerEndpoint         = "https://secretmanager.googleapis.com"
	regionalSecretManagerEndpoint = "https://secretmanager.%s.rep.googleapis.com"
)

// secretVersionName matches a secret version name, global or regional. Group 2 is the
// location of a regional secret. Project IDs can hold a domain, as in example.com:p.
var secretVersionName = regexp.MustCompile(`^projects/\w[\w.:-]*/(locations/([\w-]+)/)?secrets/[\w-]+/versions/[\w-]+$`)

// secretReadAttempts is how many requests one secret read makes when Secret Manager is busy
// or unreachable, and secretRetryBackoff the wait before the first retry, doubled for each
// later one; the Secret Manager client library retries such errors too. All of it stays
// within the read's deadline. secretRetryBackoff is a variable so tests can shorten it.
const secretReadAttempts = 3

var secretRetryBackoff = 250 * time.Millisecond

// crc32c checks secret payloads against the checksum Secret Manager sends with them.
var crc32c = crc32.MakeTable(crc32.Castagnoli)

// newSecretManagerReader creates the reader behind sharedSecretReader: it reads secret
// versions through Secret Manager's REST API with Application Default Credentials. Plain
// HTTPS keeps the Secret Manager client library, and the gRPC stack it brings, out of every
// program that imports valkeygcp. It is a variable so tests can replace it.
var newSecretManagerReader = func(ctx context.Context) (func(ctx context.Context, version string) (string, error), error) {
	creds, err := findDefaultCredentials(ctx, google.CredentialsParams{Scopes: []string{DefaultCloudPlatformScope}})
	if err != nil {
		return nil, fmt.Errorf("finding Application Default Credentials: %w", err)
	}
	hc := oauth2.NewClient(ctx, creds.TokenSource) // ctx outlives every read (see sharedSecretReader)
	return func(ctx context.Context, version string) (string, error) {
		return accessSecretVersion(ctx, hc, version)
	}, nil
}

// accessSecretVersion reads one secret version (AccessSecretVersion) through Secret
// Manager's REST API, retrying when the API is busy or can't be reached.
func accessSecretVersion(ctx context.Context, hc *http.Client, version string) (string, error) {
	m := secretVersionName.FindStringSubmatch(version)
	if m == nil {
		return "", fmt.Errorf("secret version %q is not projects/P/secrets/S/versions/V or projects/P/locations/L/secrets/S/versions/V", version)
	}
	endpoint := secretManagerEndpoint
	if m[2] != "" {
		endpoint = fmt.Sprintf(regionalSecretManagerEndpoint, m[2])
	}
	reqURL := endpoint + "/v1/" + version + ":access"
	backoff := secretRetryBackoff
	for attempt := 1; ; attempt++ {
		data, retry, err := accessOnce(ctx, hc, reqURL)
		if err == nil || !retry || attempt == secretReadAttempts {
			return data, err
		}
		select {
		case <-ctx.Done():
			return "", err
		case <-time.After(jitter(backoff)):
		}
		backoff *= 2
	}
}

// accessOnce makes one AccessSecretVersion request. retry reports an error worth another
// attempt.
func accessOnce(ctx context.Context, hc *http.Client, reqURL string) (data string, retry bool, err error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return "", false, err
	}
	resp, err := hc.Do(req)
	if err != nil {
		return "", ctx.Err() == nil, err // the network or the credentials, unless ctx ended
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20)) // a payload is at most 64 KiB
	if err != nil {
		return "", ctx.Err() == nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return "", retryableStatus(resp.StatusCode), secretManagerError(resp.Status, body)
	}
	var r struct {
		Payload struct {
			Data       []byte          `json:"data"`       // base64 in JSON
			DataCrc32c json.RawMessage `json:"dataCrc32c"` // an int64: a JSON string, or a number
		} `json:"payload"`
	}
	if err := json.Unmarshal(body, &r); err != nil {
		return "", false, fmt.Errorf("secret manager: unreadable response: %w", err)
	}
	if sum := strings.Trim(string(r.Payload.DataCrc32c), `"`); sum != "" && sum != "null" {
		want, err := strconv.ParseInt(sum, 10, 64)
		if err != nil || want != int64(crc32.Checksum(r.Payload.Data, crc32c)) {
			return "", true, errors.New("secret manager: payload checksum mismatch")
		}
	}
	return string(r.Payload.Data), false, nil
}

// retryableStatus reports whether a Secret Manager response status is worth retrying: the
// API is busy or briefly unavailable.
func retryableStatus(code int) bool {
	switch code {
	case http.StatusTooManyRequests, http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	}
	return false
}

// secretManagerError turns an error response into an error, with Google's error status and
// message when the body has them.
func secretManagerError(status string, body []byte) error {
	var e struct {
		Error struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		return fmt.Errorf("secret manager: %s %s: %s", status, e.Error.Status, e.Error.Message)
	}
	return fmt.Errorf("secret manager: %s", status)
}

// One reader, with one HTTP client and one Application Default Credentials token cache,
// serves every Token Auth provider in the process: valkey.Client has no close hook that
// could release a reader per provider. The reader is created on first use and kept for the
// life of the process; a failed creation is retried on the next call.
var (
	secretMu     sync.Mutex
	secretReader func(ctx context.Context, version string) (string, error)
)

// sharedSecretReader returns the process-wide Secret Manager reader, creating it on first
// use.
func sharedSecretReader(ctx context.Context) (func(context.Context, string) (string, error), error) {
	secretMu.Lock()
	defer secretMu.Unlock()
	if secretReader == nil {
		// The reader outlives ctx, so it must not inherit ctx's cancellation.
		read, err := newSecretManagerReader(context.WithoutCancel(ctx))
		if err != nil {
			return nil, fmt.Errorf("valkeygcp: secret manager client: %w", err)
		}
		secretReader = read
	}
	return secretReader, nil
}

// newTokenAuthProvider builds the Token Auth provider and reads the token once, so a wrong
// secret fails NewClient instead of the first connection. ctx bounds the setup and that
// first read, which also stop after 15s when ctx has no earlier deadline.
func newTokenAuthProvider(ctx context.Context, opt TokenAuthOptions) (*tokenAuthProvider, error) {
	switch {
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
		// The HTTP request honors ctx, but fetching an access token for it does not.
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
// just now, covered by the last token that worked.
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
