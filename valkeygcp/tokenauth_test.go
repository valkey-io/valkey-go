package valkeygcp

import (
	"cmp"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// authOnce calls p's AuthCredentialsFn once, as valkey-go does for a new connection.
func authOnce(t *testing.T, p *tokenAuthProvider) valkey.AuthCredentials {
	t.Helper()
	creds, err := p.AuthCredentialsFn()(valkey.AuthCredentialsContext{})
	if err != nil {
		t.Fatal(err)
	}
	return creds
}

// tokenOption resolves a ClientOption with only WithTokenAuth(opt), without connecting.
func tokenOption(t *testing.T, opt TokenAuthOptions) (valkey.ClientOption, error) {
	t.Helper()
	return ResolveClientOption(context.Background(), valkey.ClientOption{InitAddress: []string{"127.0.0.1:0"}}, WithTokenAuth(opt))
}

// allowShortReloads lets a test use reload intervals below the 1s minimum.
func allowShortReloads(t *testing.T) {
	t.Helper()
	orig := minTokenReloadInterval
	minTokenReloadInterval = time.Millisecond
	t.Cleanup(func() { minTokenReloadInterval = orig })
}

// A fixed token without a username logs in as the default user, and the next re-AUTH is
// scheduled within the default reload interval.
func TestTokenAuth_StaticDefaultUser(t *testing.T) {
	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{Token: "static-token"})
	if err != nil {
		t.Fatal(err)
	}
	creds := authOnce(t, p)
	// An empty Username makes valkey-go send HELLO 3 AUTH default <token>.
	if creds.Username != "" || creds.Password != "static-token" {
		t.Fatalf("creds = %+v", creds)
	}
	if d := time.Until(creds.RefreshAfter); d <= 0 || d > DefaultTokenReloadInterval {
		t.Errorf("RefreshAfter in %v, want within the default reload interval", d)
	}
}

// TokenAuthOptions.Username is the user sent with the token.
func TestTokenAuth_NamedUser(t *testing.T) {
	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{Username: "app-user", Token: "t"})
	if err != nil {
		t.Fatal(err)
	}
	if creds := authOnce(t, p); creds.Username != "app-user" || creds.Password != "t" {
		t.Fatalf("creds = %+v", creds)
	}
}

// The token is read again once the cache window, half the reload interval, has passed, and
// the next re-AUTH comes 90-100% of the reload interval later.
func TestTokenAuth_ReloadAfterCacheWindow(t *testing.T) {
	allowShortReloads(t)
	const reload = 200 * time.Millisecond
	var calls atomic.Int32
	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{
		ReloadInterval: reload,
		Source: func(context.Context) (string, error) {
			if calls.Add(1) == 1 {
				return "v1", nil
			}
			return "v2", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if creds := authOnce(t, p); creds.Password != "v1" {
		t.Fatalf("inside the cache window: got %q", creds.Password)
	}
	time.Sleep(reload/2 + 20*time.Millisecond) // past the cache window (reload/2)
	before := time.Now()
	creds := authOnce(t, p)
	if creds.Password != "v2" {
		t.Fatalf("after the cache window: got %q", creds.Password)
	}
	if d := creds.RefreshAfter.Sub(before); d < reload*9/10 || d > reload+50*time.Millisecond {
		t.Errorf("RefreshAfter in %v, want 90-100%% of %v", d, reload)
	}
}

// A failed re-read keeps the last token, reports the error once, and isn't retried for every
// new connection.
func TestTokenAuth_LastKnownGood(t *testing.T) {
	allowShortReloads(t)
	var fail atomic.Bool
	var calls atomic.Int32
	var absorbed []error
	var mu sync.Mutex
	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{
		ReloadInterval: 200 * time.Millisecond,
		Source: func(context.Context) (string, error) {
			calls.Add(1)
			if fail.Load() {
				return "", errors.New("secret manager unavailable")
			}
			return "good", nil
		},
		OnCredentialError: func(err error) {
			mu.Lock()
			absorbed = append(absorbed, err)
			mu.Unlock()
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	time.Sleep(120 * time.Millisecond) // past the 100ms cache window
	if creds := authOnce(t, p); creds.Password != "good" {
		t.Fatalf("got %q, want the last token that worked", creds.Password)
	}
	mu.Lock()
	n := len(absorbed)
	mu.Unlock()
	if n != 1 {
		t.Fatalf("OnCredentialError called %d times, want 1", n)
	}
	// The failed read is not retried on every new connection.
	before := calls.Load()
	for range 10 {
		authOnce(t, p)
	}
	if calls.Load() != before {
		t.Errorf("source called %d more times during the retry back-off", calls.Load()-before)
	}
}

// Setup fails when the first read fails or returns a blank token, and without a token source.
func TestTokenAuth_InitialFetchFails(t *testing.T) {
	_, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{
		Source: func(context.Context) (string, error) { return "", errors.New("permission denied") },
	})
	if err == nil || !strings.Contains(err.Error(), "initial token fetch") || !strings.Contains(err.Error(), "permission denied") {
		t.Fatalf("got %v", err)
	}
	if _, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{Source: func(context.Context) (string, error) { return " \n", nil }}); err == nil {
		t.Fatal("a blank token must be rejected")
	}
	if _, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{}); !errors.Is(err, errNoTokenSource) {
		t.Fatalf("no source: got %v", err)
	}
}

// A secret version's payload is trimmed, and Source takes precedence over SecretVersion and
// Token.
func TestTokenAuth_SecretManagerTrimmed(t *testing.T) {
	orig := newSecretSource
	defer func() { newSecretSource = orig }()
	var gotVersion string
	newSecretSource = func(_ context.Context, version string) (func(context.Context) (string, error), error) {
		gotVersion = version
		return func(context.Context) (string, error) { return "secret-token\n", nil }, nil
	}
	const version = "projects/p/secrets/valkey-default-token/versions/latest"
	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{SecretVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	if creds := authOnce(t, p); creds.Password != "secret-token" {
		t.Fatalf("got %q, want the trimmed token", creds.Password)
	}
	if gotVersion != version {
		t.Errorf("read %q, want %q", gotVersion, version)
	}

	// Source takes precedence over SecretVersion and Token.
	p, err = newTokenAuthProvider(context.Background(), TokenAuthOptions{
		SecretVersion: version, Token: "static",
		Source: func(context.Context) (string, error) { return "from-source", nil },
	})
	if err != nil || authOnce(t, p).Password != "from-source" {
		t.Fatalf("Source precedence: %v", err)
	}
}

// Many connections dialing at once share one token read.
func TestTokenAuth_OneFetchForManyDials(t *testing.T) {
	var calls atomic.Int32
	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{
		Source: func(context.Context) (string, error) {
			calls.Add(1)
			return "tok", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	fn := p.AuthCredentialsFn()
	var wg sync.WaitGroup
	for range 1000 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if creds, err := fn(valkey.AuthCredentialsContext{}); err != nil || creds.Password != "tok" {
				t.Errorf("creds = %+v, %v", creds, err)
			}
		}()
	}
	wg.Wait()
	if n := calls.Load(); n != 1 {
		t.Fatalf("upstream fetches = %d, want exactly 1", n)
	}
}

// A negative ReloadInterval reads the token once and schedules no re-AUTH.
func TestTokenAuth_ReloadDisabled(t *testing.T) {
	var calls atomic.Int32
	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{
		ReloadInterval: -1,
		Source: func(context.Context) (string, error) {
			calls.Add(1)
			return "tok", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if creds := authOnce(t, p); !creds.RefreshAfter.IsZero() {
		t.Errorf("RefreshAfter = %v, want zero with reload disabled", creds.RefreshAfter)
	}
	if calls.Load() != 1 {
		t.Errorf("source called %d times, want 1", calls.Load())
	}
}

// Without a caller deadline the first read is still capped (setupTimeout), and reads while
// connecting or re-authenticating get tokenFetchTimeout.
func TestTokenAuth_ReadDeadlines(t *testing.T) {
	allowShortReloads(t)
	var deadlines []time.Duration
	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{
		ReloadInterval: 2 * time.Millisecond, // re-read on the next AUTH
		Source: func(ctx context.Context) (string, error) {
			d, ok := ctx.Deadline()
			if !ok {
				return "", errors.New("read without a deadline")
			}
			deadlines = append(deadlines, time.Until(d))
			return "tok", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(5 * time.Millisecond)
	authOnce(t, p)
	if len(deadlines) != 2 || deadlines[0] > setupTimeout || deadlines[0] <= tokenFetchTimeout || deadlines[1] > tokenFetchTimeout {
		t.Fatalf("read deadlines = %v, want ~%v (setup) then <= %v (connection path)", deadlines, setupTimeout, tokenFetchTimeout)
	}
}

// One Secret Manager client serves every provider; a failed creation isn't cached.
func TestSharedSecretReader(t *testing.T) {
	origNew := newSecretManagerReader
	secretMu.Lock()
	origReader := secretReader
	secretReader = nil
	secretMu.Unlock()
	t.Cleanup(func() {
		newSecretManagerReader = origNew
		secretMu.Lock()
		secretReader = origReader
		secretMu.Unlock()
	})
	created, fail := 0, true
	newSecretManagerReader = func(context.Context) (func(context.Context, string) (string, error), error) {
		created++ // called under secretMu
		if fail {
			return nil, errors.New("no credentials")
		}
		return func(_ context.Context, version string) (string, error) { return "tok-" + version, nil }, nil
	}
	if _, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{SecretVersion: "v1"}); err == nil || !strings.Contains(err.Error(), "secret manager client") {
		t.Fatalf("got %v, want the client creation error", err)
	}
	fail = false
	for _, version := range []string{"v1", "v2", "v1"} {
		p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{SecretVersion: version})
		if err != nil {
			t.Fatal(err)
		}
		if got := authOnce(t, p).Password; got != "tok-"+version {
			t.Fatalf("%s: got %q", version, got)
		}
	}
	if created != 2 {
		t.Fatalf("Secret Manager clients created = %d, want 2 (one failed, then one shared)", created)
	}
}

// WithTokenAuth sets AuthCredentialsFn and the Memorystore defaults, and needs a token source.
func TestWithTokenAuth(t *testing.T) {
	opt, err := tokenOption(t, TokenAuthOptions{Token: "tok"})
	if err != nil || opt.AuthCredentialsFn == nil {
		t.Fatalf("WithTokenAuth: %v", err)
	}
	creds, err := opt.AuthCredentialsFn(valkey.AuthCredentialsContext{})
	if err != nil || creds.Username != "" || creds.Password != "tok" {
		t.Fatalf("creds = %+v, %v", creds, err)
	}
	if opt.RetryDelay == nil || opt.ClusterOption.ShardsRefreshInterval != defaultShardsRefreshInterval {
		t.Errorf("Memorystore defaults not filled in: retry delay set %v, shards refresh %v", opt.RetryDelay != nil, opt.ClusterOption.ShardsRefreshInterval)
	}
	if _, err := tokenOption(t, TokenAuthOptions{}); !errors.Is(err, errNoTokenSource) {
		t.Fatalf("got %v", err)
	}
}

// NewClient's ctx bounds the first token read made for WithTokenAuth.
func TestWithTokenAuth_HonorsContext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := NewClient(ctx, valkey.ClientOption{InitAddress: []string{"127.0.0.1:0"}}, WithTokenAuth(TokenAuthOptions{
		Source: func(ctx context.Context) (string, error) {
			<-ctx.Done() // a Secret Manager read that never answers
			return "", ctx.Err()
		},
	}))
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "initial token fetch") {
		t.Fatalf("got %v, want the caller's deadline on the first read", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("setup took %v despite a 50ms deadline", d)
	}
}

// WithTokenAuth reports read errors covered by the last token that worked.
func TestWithTokenAuth_OnCredentialError(t *testing.T) {
	allowShortReloads(t)
	var fail atomic.Bool
	var absorbed atomic.Int32
	opt, err := tokenOption(t, TokenAuthOptions{
		ReloadInterval: 40 * time.Millisecond,
		Source: func(context.Context) (string, error) {
			if fail.Load() {
				return "", errors.New("secret manager unavailable")
			}
			return "good", nil
		},
		OnCredentialError: func(error) { absorbed.Add(1) },
	})
	if err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	time.Sleep(30 * time.Millisecond) // past the cache window (ReloadInterval/2)
	creds, err := opt.AuthCredentialsFn(valkey.AuthCredentialsContext{})
	if err != nil || creds.Password != "good" || absorbed.Load() != 1 {
		t.Fatalf("creds = %+v, %v; OnCredentialError calls = %d, want the last token and 1 call", creds, err, absorbed.Load())
	}
}

// WithTokenAuth can't be combined with IAM or with credentials on the ClientOption, and the
// last WithTokenAuth wins. ClientOption.Username is the token auth user unless
// TokenAuthOptions.Username sets one; two different usernames are an error.
func TestWithTokenAuth_Conflicts(t *testing.T) {
	ctx := context.Background()
	addr := []string{"127.0.0.1:0"}
	tok := WithTokenAuth(TokenAuthOptions{Token: "tok"})

	for _, opts := range [][]Option{{WithDefaultIAM(), tok}, {tok, WithIAMOptions(IAMAuthOptions{})}} {
		if _, err := ResolveClientOption(ctx, valkey.ClientOption{InitAddress: addr}, opts...); err == nil || !strings.Contains(err.Error(), "can't be combined") {
			t.Errorf("IAM and Token Auth together: got %v", err)
		}
	}
	for field, option := range map[string]valkey.ClientOption{
		"Password": {InitAddress: addr, Password: "p"},
		"AuthCredentialsFn": {InitAddress: addr, AuthCredentialsFn: func(valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
			return valkey.AuthCredentials{}, nil
		}},
	} {
		if _, err := ResolveClientOption(ctx, option, tok); err == nil || !strings.Contains(err.Error(), "ClientOption."+field+" conflicts with WithTokenAuth") {
			t.Errorf("%s: got %v", field, err)
		}
	}
	for _, c := range []struct{ caller, own, want string }{
		{"u", "", "u"},  // ClientOption.Username is the token auth user
		{"u", "u", "u"}, // the same name twice is fine
		{"", "v", "v"},
	} {
		opt, err := ResolveClientOption(ctx, valkey.ClientOption{InitAddress: addr, Username: c.caller},
			WithTokenAuth(TokenAuthOptions{Username: c.own, Token: "tok"}))
		if err != nil {
			t.Fatalf("%+v: %v", c, err)
		}
		if creds, err := opt.AuthCredentialsFn(valkey.AuthCredentialsContext{}); err != nil || creds.Username != c.want {
			t.Errorf("%+v: creds = %+v, %v; want user %q", c, creds, err, c.want)
		}
	}
	if _, err := ResolveClientOption(ctx, valkey.ClientOption{InitAddress: addr, Username: "u"},
		WithTokenAuth(TokenAuthOptions{Username: "v", Token: "tok"})); err == nil || !strings.Contains(err.Error(), "differ") {
		t.Errorf("two different usernames: got %v", err)
	}
	opt, err := ResolveClientOption(ctx, valkey.ClientOption{InitAddress: addr},
		WithTokenAuth(TokenAuthOptions{Token: "first"}), WithTokenAuth(TokenAuthOptions{Token: "last"}))
	if err != nil {
		t.Fatal(err)
	}
	if creds, err := opt.AuthCredentialsFn(valkey.AuthCredentialsContext{}); err != nil || creds.Password != "last" {
		t.Fatalf("creds = %+v, %v; want the last WithTokenAuth", creds, err)
	}
}

// While reads fail, connections come back within the retry interval instead of the reload
// interval, and the earliest of them finds the read allowed. Once a read works again, they
// go back to the reload interval.
func TestTokenAuth_RetrySoonAfterFailedRead(t *testing.T) {
	allowShortReloads(t)
	const reload = 400 * time.Millisecond // retry interval: reload/2 = 200ms
	const retry = reload / 2
	var fail atomic.Bool
	var reads atomic.Int32
	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{
		ReloadInterval: reload,
		Source: func(context.Context) (string, error) {
			reads.Add(1)
			if fail.Load() {
				return "", errors.New("secret manager unavailable")
			}
			return "good", nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	within := func(what string, creds valkey.AuthCredentials, before time.Time, lo, hi time.Duration) {
		t.Helper()
		if d := creds.RefreshAfter.Sub(before); d < lo || d > hi {
			t.Errorf("%s: RefreshAfter in %v, want %v-%v", what, d, lo, hi)
		}
	}
	fail.Store(true)
	time.Sleep(reload/2 + 20*time.Millisecond) // past the cache window
	before := time.Now()
	within("the failed read", authOnce(t, p), before, retry*9/10, retry+50*time.Millisecond)
	before = time.Now()
	within("a connection inside the retry interval", authOnce(t, p), before, retry*8/10, retry+50*time.Millisecond)
	if n := reads.Load(); n != 2 {
		t.Fatalf("reads = %d, want 2 (the first read and one failed re-read)", n)
	}

	time.Sleep(retry * 9 / 10) // the earliest jittered re-AUTH
	fail.Store(false)
	before = time.Now()
	creds := authOnce(t, p)
	if n := reads.Load(); n != 3 || creds.Password != "good" {
		t.Fatalf("reads = %d, token %q: the earliest re-AUTH must read again", n, creds.Password)
	}
	within("after the read works again", creds, before, reload*9/10, reload+50*time.Millisecond)
}

// A positive ReloadInterval below 1s is almost always a unit slip, such as 5 for
// 5*time.Minute, and is rejected.
func TestTokenAuth_MinReloadInterval(t *testing.T) {
	for _, d := range []time.Duration{5, 999 * time.Millisecond} {
		if _, err := tokenOption(t, TokenAuthOptions{Token: "tok", ReloadInterval: d}); err == nil || !strings.Contains(err.Error(), "below the 1s minimum") {
			t.Errorf("ReloadInterval %v: got %v, want the minimum error", d, err)
		}
	}
	for _, d := range []time.Duration{0, -1, time.Second} {
		if _, err := tokenOption(t, TokenAuthOptions{Token: "tok", ReloadInterval: d}); err != nil {
			t.Errorf("ReloadInterval %v: %v", d, err)
		}
	}
}

// A Source that ignores ctx can't hold the token lock past the read's deadline.
func TestTokenAuth_SourceIgnoresContext(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := newTokenAuthProvider(ctx, TokenAuthOptions{
		Source: func(context.Context) (string, error) {
			<-release // hangs, whatever ctx says
			return "late", nil
		},
	})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the read abandoned at the deadline", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("setup took %v despite a 50ms deadline", d)
	}
}

// secretManagerServer serves AccessSecretVersion for a test and points the Secret Manager
// endpoints at it: the global one at /, the regional ones at /<location>/.
func secretManagerServer(t *testing.T, handler http.HandlerFunc) {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	origGlobal, origRegional, origBackoff := secretManagerEndpoint, regionalSecretManagerEndpoint, secretRetryBackoff
	secretManagerEndpoint, regionalSecretManagerEndpoint, secretRetryBackoff = srv.URL, srv.URL+"/%s", time.Millisecond
	t.Cleanup(func() {
		secretManagerEndpoint, regionalSecretManagerEndpoint, secretRetryBackoff = origGlobal, origRegional, origBackoff
	})
}

// secretPayload is an AccessSecretVersion response body for data, with its CRC32C checksum.
func secretPayload(data string) string {
	sum := crc32.Checksum([]byte(data), crc32.MakeTable(crc32.Castagnoli))
	return fmt.Sprintf(`{"name":"projects/1/secrets/s/versions/3","payload":{"data":%q,"dataCrc32c":"%d"}}`,
		base64.StdEncoding.EncodeToString([]byte(data)), sum)
}

// accessSecretVersion reads global and regional secret versions, rejects bad names and
// checksums, reports Google's error messages, and retries only a busy API.
func TestAccessSecretVersion(t *testing.T) {
	var mu sync.Mutex
	var paths []string      // the paths requested by the current read
	var status atomic.Int32 // the status of the next response; 0 means 200
	var body atomic.Value
	secretManagerServer(t, func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path)
		mu.Unlock()
		if code := status.Load(); code != 0 {
			w.WriteHeader(int(code))
		}
		fmt.Fprint(w, body.Load())
	})
	ctx := context.Background()
	requested := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return paths
	}
	read := func(version string) (string, error) {
		t.Helper()
		mu.Lock()
		paths = nil
		mu.Unlock()
		return accessSecretVersion(ctx, http.DefaultClient, version)
	}

	body.Store(secretPayload("token-1\n"))
	if got, err := read("projects/p/secrets/s/versions/latest"); err != nil || got != "token-1\n" {
		t.Fatalf("global secret: %q, %v", got, err)
	}
	if want, got := "/v1/projects/p/secrets/s/versions/latest:access", requested(); len(got) != 1 || got[0] != want {
		t.Errorf("global secret: requests %v, want [%s]", got, want)
	}
	if got, err := read("projects/example.com:p/locations/us-east1/secrets/s_2/versions/3"); err != nil || got != "token-1\n" {
		t.Fatalf("regional secret: %q, %v", got, err)
	}
	if want, got := "/us-east1/v1/projects/example.com:p/locations/us-east1/secrets/s_2/versions/3:access", requested(); len(got) != 1 || got[0] != want {
		t.Errorf("regional secret: requests %v, want [%s] on the regional endpoint", got, want)
	}
	for _, version := range []string{"v1", "projects/p/secrets/s", "projects/p/secrets/s/versions/1?x=y", "projects/p/secrets/../versions/1", "projects/../secrets/s/versions/1"} {
		if _, err := read(version); err == nil || !strings.Contains(err.Error(), "is not projects/P/secrets/S/versions/V") || len(requested()) != 0 {
			t.Errorf("%q: got %v after %d requests, want a name error and no request", version, err, len(requested()))
		}
	}

	// A checksum sent as a JSON number, or none, is accepted too.
	sum := crc32.Checksum([]byte("tok"), crc32.MakeTable(crc32.Castagnoli))
	body.Store(fmt.Sprintf(`{"payload":{"data":"dG9r","dataCrc32c":%d}}`, sum))
	if got, err := read("projects/p/secrets/s/versions/1"); err != nil || got != "tok" {
		t.Errorf("numeric checksum: %q, %v", got, err)
	}
	body.Store(`{"payload":{"data":"dG9r"}}`)
	if got, err := read("projects/p/secrets/s/versions/1"); err != nil || got != "tok" {
		t.Errorf("no checksum: %q, %v", got, err)
	}
	body.Store(`{"payload":{"data":"dG9r","dataCrc32c":"1"}}`)
	if _, err := read("projects/p/secrets/s/versions/1"); err == nil || !strings.Contains(err.Error(), "checksum mismatch") || len(requested()) != secretReadAttempts {
		t.Errorf("bad checksum: got %v after %d requests", err, len(requested()))
	}

	// An error with Google's error body is reported with its status and message, and a
	// permission error is not retried.
	status.Store(http.StatusForbidden)
	body.Store(`{"error":{"code":403,"message":"Permission 'secretmanager.versions.access' denied","status":"PERMISSION_DENIED"}}`)
	if _, err := read("projects/p/secrets/s/versions/1"); err == nil || !strings.Contains(err.Error(), "PERMISSION_DENIED: Permission 'secretmanager.versions.access' denied") || len(requested()) != 1 {
		t.Errorf("permission error: got %v after %d requests, want its message and no retry", err, len(requested()))
	}
	// A busy API is retried, up to secretReadAttempts requests.
	status.Store(http.StatusServiceUnavailable)
	body.Store("unavailable")
	if _, err := read("projects/p/secrets/s/versions/1"); err == nil || !strings.Contains(err.Error(), "503") || len(requested()) != secretReadAttempts {
		t.Errorf("unavailable: got %v after %d requests, want %d", err, len(requested()), secretReadAttempts)
	}
}

// A read that Secret Manager turns away as busy succeeds on a later attempt.
func TestAccessSecretVersion_RetryThenSuccess(t *testing.T) {
	var requests atomic.Int32
	secretManagerServer(t, func(w http.ResponseWriter, r *http.Request) {
		if requests.Add(1) < secretReadAttempts {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
		fmt.Fprint(w, secretPayload("tok"))
	})
	if got, err := accessSecretVersion(context.Background(), http.DefaultClient, "projects/p/secrets/s/versions/1"); err != nil || got != "tok" {
		t.Fatalf("got %q, %v after %d requests", got, err, requests.Load())
	}
}

// SecretVersion reads through the shared reader with Application Default Credentials,
// which the access token on each request comes from.
func TestTokenAuth_SecretVersionThroughREST(t *testing.T) {
	var auth, path atomic.Value
	secretManagerServer(t, func(w http.ResponseWriter, r *http.Request) {
		auth.Store(r.Header.Get("Authorization"))
		path.Store(r.URL.Path)
		fmt.Fprint(w, secretPayload("secret-token\n"))
	})
	origFind := findDefaultCredentials
	secretMu.Lock()
	origReader := secretReader
	secretReader = nil
	secretMu.Unlock()
	t.Cleanup(func() {
		findDefaultCredentials = origFind
		secretMu.Lock()
		secretReader = origReader
		secretMu.Unlock()
	})
	var scopes []string
	findDefaultCredentials = func(_ context.Context, params google.CredentialsParams) (*google.Credentials, error) {
		scopes = params.Scopes
		return &google.Credentials{TokenSource: oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "adc-token"})}, nil
	}

	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{SecretVersion: "projects/p/secrets/valkey-token/versions/latest"})
	if err != nil {
		t.Fatal(err)
	}
	if creds := authOnce(t, p); creds.Password != "secret-token" {
		t.Fatalf("got %q, want the trimmed secret", creds.Password)
	}
	if got := auth.Load(); got != "Bearer adc-token" {
		t.Errorf("Authorization = %q, want the ADC access token", got)
	}
	if got := path.Load(); got != "/v1/projects/p/secrets/valkey-token/versions/latest:access" {
		t.Errorf("path = %q", got)
	}
	if len(scopes) != 1 || scopes[0] != DefaultCloudPlatformScope {
		t.Errorf("ADC scopes = %v, want [%s]", scopes, DefaultCloudPlatformScope)
	}

	// Without ADC, setup fails with the reason, and the next call tries again.
	secretMu.Lock()
	secretReader = nil
	secretMu.Unlock()
	findDefaultCredentials = func(context.Context, google.CredentialsParams) (*google.Credentials, error) {
		return nil, errors.New("could not find default credentials")
	}
	if _, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{SecretVersion: "projects/p/secrets/s/versions/1"}); err == nil ||
		!strings.Contains(err.Error(), "secret manager client: finding Application Default Credentials: could not find default credentials") {
		t.Fatalf("got %v, want the ADC error", err)
	}
}

// Finding the credentials for SecretVersion can hang and ignore ctx. Setup still stops at
// NewClient's deadline, also for a caller waiting behind another one's setup, and the shared
// reader is kept for later providers once it is ready.
func TestTokenAuth_SecretManagerSetupBounded(t *testing.T) {
	origNew := newSecretManagerReader
	secretMu.Lock()
	origReader := secretReader
	secretReader = nil
	secretMu.Unlock()
	release := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		newSecretManagerReader = origNew
		secretMu.Lock()
		secretReader = origReader
		secretMu.Unlock()
	})
	var created atomic.Int32
	newSecretManagerReader = func(context.Context) (func(context.Context, string) (string, error), error) {
		created.Add(1)
		<-release // finding the credentials hangs, whatever ctx says
		return func(context.Context, string) (string, error) { return "tok", nil }, nil
	}
	const version = "projects/p/secrets/s/versions/1"

	// Two setups at once: one waits for the hung creation, the other for the first to finish.
	// They report on a buffered channel, so a setup that outlives a failed test never blocks.
	type result struct {
		err  error
		took time.Duration
	}
	results := make(chan result, 2)
	for range 2 {
		go func() {
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			start := time.Now()
			_, err := newTokenAuthProvider(ctx, TokenAuthOptions{SecretVersion: version})
			results <- result{err, time.Since(start)}
		}()
	}
	for range 2 {
		select {
		case r := <-results:
			if !errors.Is(r.err, context.DeadlineExceeded) || !strings.Contains(r.err.Error(), "secret manager setup") {
				t.Errorf("got %v, want the setup stopped at the deadline", r.err)
			}
			if r.took > 2*time.Second {
				t.Errorf("setup took %v despite a 50ms deadline", r.took)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("setup didn't stop at its deadline while finding the credentials hung")
		}
	}

	close(release) // the creation finishes in the background and keeps its reader
	p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{SecretVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	if got := authOnce(t, p).Password; got != "tok" {
		t.Fatalf("got %q", got)
	}
	if n := created.Load(); n != 1 {
		t.Errorf("readers created = %d, want 1: the abandoned creation's reader is shared", n)
	}
}

// TestTokenAuth_LiveRotation rotates a token on a real Valkey while a client is connected:
// the live connection re-AUTHs with the new token and stays the same connection, no AUTH is
// rejected, and a new connection gets the new token. valkey-go ignores the reply to a
// re-AUTH, so the server's ACL log tells whether one was rejected. It needs a local Valkey
// whose default user has no password (docker compose up -d valkey), at 127.0.0.1:6379 or
// VALKEYGCP_TEST_ADDR, and is skipped without one. It creates and deletes its own ACL user
// and key, named for each run.
func TestTokenAuth_LiveRotation(t *testing.T) {
	allowShortReloads(t)
	addr := cmp.Or(os.Getenv("VALKEYGCP_TEST_ADDR"), "127.0.0.1:6379")
	admin, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:       []string{addr},
		ForceSingleClient: true,
		DisableCache:      true,
		Dialer:            net.Dialer{Timeout: 300 * time.Millisecond},
	})
	if err != nil {
		t.Skipf("no local Valkey without a password at %s: %v", addr, err)
	}
	t.Cleanup(admin.Close)
	ctx := context.Background()
	// Names of its own, so the test never resets or deletes a user, or overwrites a key, that
	// it didn't create, even when two runs share a server.
	user := fmt.Sprintf("valkeygcp-token-test-%d-%d", os.Getpid(), time.Now().UnixNano())
	key := "valkeygcp:token-rotation:" + user
	acl := func(args ...string) {
		t.Helper()
		if err := admin.Do(ctx, admin.B().Arbitrary("ACL").Args(args...).Build()).Error(); err != nil {
			t.Fatalf("ACL %v: %v", args, err)
		}
	}
	acl("SETUSER", user, "reset", "on", ">token-1", "~*", "+@all")
	t.Cleanup(func() {
		_ = admin.Do(context.Background(), admin.B().Del().Key(key).Build()).Error()
		_ = admin.Do(context.Background(), admin.B().Arbitrary("ACL").Args("DELUSER", user).Build()).Error()
	})

	const reload = 400 * time.Millisecond
	var current atomic.Value
	current.Store("token-1")
	opt, err := ResolveClientOption(ctx, valkey.ClientOption{
		InitAddress:       []string{addr},
		ForceSingleClient: true,
		PipelineMultiplex: -1, // one connection, so CLIENT ID tells whether it was replaced
		DisableCache:      true,
	}, WithTokenAuth(TokenAuthOptions{
		Username:       user,
		ReloadInterval: reload,
		Source:         func(context.Context) (string, error) { return current.Load().(string), nil },
	}))
	if err != nil {
		t.Fatal(err)
	}
	// Record what each AUTH hands out. With one connection, the last one is its latest re-AUTH.
	var auths atomic.Int32
	var handed atomic.Value
	authFn := opt.AuthCredentialsFn
	opt.AuthCredentialsFn = func(c valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
		creds, err := authFn(c)
		auths.Add(1)
		handed.Store(creds.Password)
		return creds, err
	}
	client, err := valkey.NewClient(opt)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	id, err := client.Do(ctx, client.B().ClientId().Build()).AsInt64()
	if err != nil {
		t.Fatal(err)
	}
	if who, err := client.Do(ctx, client.B().AclWhoami().Build()).ToString(); err != nil || who != user {
		t.Fatalf("ACL WHOAMI = %q, %v; want %q", who, err, user)
	}

	// Rotate: the user accepts both tokens for a while, and the source hands out the new one.
	acl("SETUSER", user, ">token-2")
	current.Store("token-2")
	time.Sleep(4 * reload) // the cache re-reads within reload/2, and the connection re-AUTHs within reload
	// Retire the old token. valkey-go ignores the reply to a re-AUTH, so a re-AUTH that still
	// sent the old token would not close the connection; the server records it in the ACL log.
	// The log may be shared, so it isn't cleared: only this user's rejected AUTHs are counted,
	// before and after.
	rejectedBefore := aclAuthFailures(t, admin, user)
	acl("SETUSER", user, "<token-1")
	before := auths.Load()
	time.Sleep(3 * reload)

	if n := auths.Load() - before; n == 0 {
		t.Fatal("no re-AUTH on the live connection after the rotation")
	}
	if got := handed.Load(); got != "token-2" {
		t.Fatalf("the latest re-AUTH sent %q, want token-2", got)
	}
	if n := aclAuthFailures(t, admin, user) - rejectedBefore; n > 0 {
		t.Fatalf("ACL LOG recorded %d rejected AUTHs for %s after token-1 was retired", n, user)
	}
	if got, err := client.Do(ctx, client.B().ClientId().Build()).AsInt64(); err != nil || got != id {
		t.Fatalf("CLIENT ID = %d, %v; want %d: the connection was replaced", got, err, id)
	}
	if err := client.Do(ctx, client.B().Set().Key(key).Value("ok").Build()).Error(); err != nil {
		t.Fatalf("SET after the rotation: %v", err)
	}
	fresh, err := valkey.NewClient(opt) // a new connection, with the same provider
	if err != nil {
		t.Fatalf("new connection after the rotation: %v", err)
	}
	fresh.Close()
}

// aclAuthFailures counts the rejected AUTHs that ACL LOG recorded for user. Plain ACL LOG
// lists only the 10 newest entries, so it asks for more.
func aclAuthFailures(t *testing.T, admin valkey.Client, user string) int64 {
	t.Helper()
	entries, err := admin.Do(context.Background(), admin.B().Arbitrary("ACL").Args("LOG", "1000").Build()).ToArray()
	if err != nil {
		t.Fatalf("ACL LOG: %v", err)
	}
	var n int64
	for _, e := range entries {
		m, err := e.AsMap()
		if err != nil {
			t.Fatalf("ACL LOG entry: %v", err)
		}
		reason, name, count := m["reason"], m["username"], m["count"]
		r, _ := reason.ToString()
		u, _ := name.ToString()
		if c, _ := count.AsInt64(); r == "auth" && u == user {
			n += c
		}
	}
	return n
}
