package valkeygcp

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	secretmanager "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/googleapis/gax-go/v2"
	"github.com/valkey-io/valkey-go"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
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

// A fixed token without a username logs in as the default user. A fixed token can't change,
// so no re-AUTH is scheduled unless ReloadInterval asks for one.
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
	if !creds.RefreshAfter.IsZero() {
		t.Errorf("RefreshAfter = %v, want none: re-AUTHing would resend the same token", creds.RefreshAfter)
	}

	p, err = newTokenAuthProvider(context.Background(), TokenAuthOptions{Token: "static-token", ReloadInterval: time.Minute})
	if err != nil {
		t.Fatal(err)
	}
	if d := time.Until(authOnce(t, p).RefreshAfter); d <= 0 || d > time.Minute {
		t.Errorf("RefreshAfter in %v, want within the 1m ReloadInterval that was set", d)
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
		t.Fatalf("got %q, want the last token read successfully", creds.Password)
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

// WithTokenAuth reports read errors covered by the last token read successfully.
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

// fakeSecretManager is a Secret Manager server for tests: it answers AccessSecretVersion
// with respond and records the names asked for.
type fakeSecretManager struct {
	secretmanagerpb.UnimplementedSecretManagerServiceServer
	mu      sync.Mutex
	names   []string
	respond func(name string) (*secretmanagerpb.AccessSecretVersionResponse, error)
}

func (f *fakeSecretManager) AccessSecretVersion(_ context.Context, req *secretmanagerpb.AccessSecretVersionRequest) (*secretmanagerpb.AccessSecretVersionResponse, error) {
	f.mu.Lock()
	f.names = append(f.names, req.GetName())
	respond := f.respond
	f.mu.Unlock()
	return respond(req.GetName())
}

// answer sets the response to every request.
func (f *fakeSecretManager) answer(respond func(name string) (*secretmanagerpb.AccessSecretVersionResponse, error)) {
	f.mu.Lock()
	f.respond = respond
	f.mu.Unlock()
}

// requests returns the names asked for since the last call.
func (f *fakeSecretManager) requests() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	names := f.names
	f.names = nil
	return names
}

// fakeSecretManagerClient serves f on a local port and returns a client for it, without
// credentials, with the client library's retry backoff shortened to a millisecond.
func fakeSecretManagerClient(t *testing.T, f *fakeSecretManager) *secretmanager.Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	secretmanagerpb.RegisterSecretManagerServiceServer(srv, f)
	go srv.Serve(lis)
	t.Cleanup(srv.Stop)
	c, err := secretmanager.NewClient(context.Background(), option.WithEndpoint(lis.Addr().String()), option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	c.CallOptions.AccessSecretVersion = append(c.CallOptions.AccessSecretVersion, gax.WithRetry(func() gax.Retryer {
		return gax.OnCodes([]codes.Code{codes.Unavailable, codes.ResourceExhausted}, gax.Backoff{Initial: time.Millisecond, Max: time.Millisecond})
	}))
	return c
}

// secretPayload is an AccessSecretVersion response for data, with its CRC32C checksum.
func secretPayload(data string) *secretmanagerpb.AccessSecretVersionResponse {
	sum := int64(crc32.Checksum([]byte(data), crc32.MakeTable(crc32.Castagnoli)))
	return &secretmanagerpb.AccessSecretVersionResponse{
		Name:    "projects/1/secrets/s/versions/3",
		Payload: &secretmanagerpb.SecretPayload{Data: []byte(data), DataCrc32C: &sum},
	}
}

// resetSecretClients starts a test with no shared Secret Manager clients, and puts the
// previous ones back after it.
func resetSecretClients(t *testing.T) {
	t.Helper()
	secretMu.Lock()
	orig := secretClients
	secretClients = map[string]*secretmanager.Client{}
	secretMu.Unlock()
	t.Cleanup(func() {
		secretMu.Lock()
		secretClients = orig
		secretMu.Unlock()
	})
}

// accessSecretVersion returns the payload, checks its checksum when there is one, and
// leaves retries to the client library: a busy API is retried, a permission error is not.
func TestAccessSecretVersion(t *testing.T) {
	f := &fakeSecretManager{}
	c := fakeSecretManagerClient(t, f)
	const version = "projects/p/secrets/s/versions/1"
	read := func(ctx context.Context) (string, error) {
		f.requests()
		return accessSecretVersion(ctx, c, version)
	}
	ctx := context.Background()

	f.answer(func(string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
		return secretPayload("token-1\n"), nil
	})
	if got, err := read(ctx); err != nil || got != "token-1\n" {
		t.Fatalf("got %q, %v", got, err)
	}
	if got := f.requests(); len(got) != 1 || got[0] != version {
		t.Errorf("requests %v, want one for %s", got, version)
	}

	// A payload without a checksum is accepted; one with a wrong checksum is not.
	f.answer(func(string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
		return &secretmanagerpb.AccessSecretVersionResponse{Payload: &secretmanagerpb.SecretPayload{Data: []byte("tok")}}, nil
	})
	if got, err := read(ctx); err != nil || got != "tok" {
		t.Errorf("no checksum: %q, %v", got, err)
	}
	wrong := int64(1)
	f.answer(func(string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
		return &secretmanagerpb.AccessSecretVersionResponse{Payload: &secretmanagerpb.SecretPayload{Data: []byte("tok"), DataCrc32C: &wrong}}, nil
	})
	if _, err := read(ctx); err == nil || !strings.Contains(err.Error(), "checksum mismatch") {
		t.Errorf("wrong checksum: got %v", err)
	}

	// A permission error is returned at once, with Secret Manager's message.
	f.answer(func(string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
		return nil, status.Error(codes.PermissionDenied, "Permission 'secretmanager.versions.access' denied")
	})
	if _, err := read(ctx); status.Code(err) != codes.PermissionDenied || !strings.Contains(err.Error(), "secretmanager.versions.access") {
		t.Errorf("permission error: got %v", err)
	}
	if n := len(f.requests()); n != 1 {
		t.Errorf("permission error: %d requests, want 1: it isn't retried", n)
	}

	// A busy API is retried until it answers...
	var calls atomic.Int32
	f.answer(func(string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
		if calls.Add(1) < 3 {
			return nil, status.Error(codes.Unavailable, "try again")
		}
		return secretPayload("tok"), nil
	})
	if got, err := read(ctx); err != nil || got != "tok" || calls.Load() != 3 {
		t.Errorf("busy, then answering: %q, %v after %d requests", got, err, calls.Load())
	}
	// ...and the read fails at its deadline if it never does.
	f.answer(func(string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
		return nil, status.Error(codes.Unavailable, "down")
	})
	short, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer cancel()
	if _, err := read(short); err == nil {
		t.Error("read succeeded while the API is down")
	}
	if n := len(f.requests()); n < 2 {
		t.Errorf("down: %d requests, want retries until the deadline", n)
	}
}

// newSecretSource rejects a bad name before creating a client, reads a regional secret
// through its location's endpoint, and shares one client per endpoint.
func TestNewSecretSource(t *testing.T) {
	f := &fakeSecretManager{respond: func(name string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
		return secretPayload("tok-" + name), nil
	}}
	c := fakeSecretManagerClient(t, f)
	resetSecretClients(t)
	origNew := newSecretManagerClient
	t.Cleanup(func() { newSecretManagerClient = origNew })
	var locations []string
	newSecretManagerClient = func(_ context.Context, location string) (*secretmanager.Client, error) {
		locations = append(locations, location) // the setups below run one at a time
		return c, nil
	}
	for _, version := range []string{"v1", "projects/p/secrets/s", "projects/p/secrets/s/versions/1?x=y", "projects/p/secrets/../versions/1", "projects/../secrets/s/versions/1"} {
		if _, err := newSecretSource(context.Background(), version); err == nil || !strings.Contains(err.Error(), "is not projects/P/secrets/S/versions/V") {
			t.Errorf("%q: got %v, want a name error", version, err)
		}
	}
	if len(locations) != 0 {
		t.Errorf("clients created for bad names: %q", locations)
	}
	for _, version := range []string{
		"projects/p/secrets/s/versions/latest",
		"projects/example.com:p/locations/us-east1/secrets/s_2/versions/3",
		"projects/p/secrets/t/versions/1",
		"projects/p/locations/us-east1/secrets/u/versions/2",
	} {
		read, err := newSecretSource(context.Background(), version)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := read(context.Background()); err != nil || got != "tok-"+version {
			t.Errorf("%s: got %q, %v", version, got, err)
		}
	}
	if len(locations) != 2 || locations[0] != "" || locations[1] != "us-east1" {
		t.Errorf("clients created for %q, want one global (\"\") and one for us-east1", locations)
	}
	if got, want := regionalEndpoint("us-east1"), "secretmanager.us-east1.rep.googleapis.com:443"; got != want {
		t.Errorf("regional endpoint %q, want %q", got, want)
	}
}

// A failed client creation fails setup with the reason and isn't kept, so the next setup
// tries again. SecretVersion then reads through one shared client, and the payload is
// trimmed.
func TestSharedSecretClient(t *testing.T) {
	c := fakeSecretManagerClient(t, &fakeSecretManager{respond: func(string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
		return secretPayload("secret-token\n"), nil
	}})
	resetSecretClients(t)
	origNew := newSecretManagerClient
	t.Cleanup(func() { newSecretManagerClient = origNew })
	created, fail := 0, true
	newSecretManagerClient = func(context.Context, string) (*secretmanager.Client, error) {
		created++ // the setups below run one at a time
		if fail {
			return nil, errors.New("could not find default credentials")
		}
		return c, nil
	}
	const version = "projects/p/secrets/valkey-token/versions/latest"
	if _, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{SecretVersion: version}); err == nil ||
		!strings.Contains(err.Error(), "secret manager client: could not find default credentials") {
		t.Fatalf("got %v, want the client creation error", err)
	}
	fail = false
	for range 3 {
		p, err := newTokenAuthProvider(context.Background(), TokenAuthOptions{SecretVersion: version})
		if err != nil {
			t.Fatal(err)
		}
		if got := authOnce(t, p).Password; got != "secret-token" {
			t.Fatalf("got %q, want the trimmed secret", got)
		}
	}
	if created != 2 {
		t.Fatalf("Secret Manager clients created = %d, want 2 (one failed, then one shared)", created)
	}
}

// Creating the Secret Manager client, which finds the credentials, can hang and ignore ctx.
// Setup still stops at NewClient's deadline, and the hung creation doesn't hold up later
// setups: they create a client themselves. When the hung creation returns, the client
// stored first is kept and the late one is closed.
func TestTokenAuth_SecretManagerSetupBounded(t *testing.T) {
	late := fakeSecretManagerClient(t, &fakeSecretManager{respond: func(string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
		return secretPayload("late"), nil
	}})
	kept := fakeSecretManagerClient(t, &fakeSecretManager{respond: func(string) (*secretmanagerpb.AccessSecretVersionResponse, error) {
		return secretPayload("tok"), nil
	}})
	resetSecretClients(t)
	origNew, origSource := newSecretManagerClient, newSecretSource
	release, entered := make(chan struct{}), make(chan struct{})
	abandoned := make(chan struct{}) // closed when the first setup's newSecretSource returns
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
		// Let the abandoned setup finish, so it can't store its client after the restore.
		select {
		case <-abandoned:
		case <-time.After(5 * time.Second):
		}
		newSecretManagerClient, newSecretSource = origNew, origSource
	})
	var sources atomic.Int32
	newSecretSource = func(ctx context.Context, version string) (func(context.Context) (string, error), error) {
		if sources.Add(1) == 1 {
			defer close(abandoned)
		}
		return origSource(ctx, version)
	}
	var created atomic.Int32
	newSecretManagerClient = func(context.Context, string) (*secretmanager.Client, error) {
		if created.Add(1) == 1 {
			close(entered)
			<-release // the first creation hangs, whatever ctx says
			return late, nil
		}
		return kept, nil
	}
	const version = "projects/p/secrets/s/versions/1"

	// The first setup stops at its deadline while its client creation hangs. It reports on
	// a buffered channel, so a setup that outlives a failed test never blocks.
	type result struct {
		err  error
		took time.Duration
	}
	first := make(chan result, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancel()
		start := time.Now()
		_, err := newTokenAuthProvider(ctx, TokenAuthOptions{SecretVersion: version})
		first <- result{err, time.Since(start)}
	}()
	select {
	case r := <-first:
		if !errors.Is(r.err, context.DeadlineExceeded) || !strings.Contains(r.err.Error(), "secret manager setup") {
			t.Fatalf("got %v, want the setup stopped at the deadline", r.err)
		}
		if r.took > 2*time.Second {
			t.Errorf("setup took %v despite a 50ms deadline", r.took)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("setup didn't stop at its deadline while creating the client hung")
	}

	// A later setup doesn't wait for the hung creation: it creates a client itself.
	<-entered // the first creation is underway, so this setup is the second one
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	p, err := newTokenAuthProvider(ctx, TokenAuthOptions{SecretVersion: version})
	if err != nil {
		t.Fatalf("setup while another creation hangs: %v", err)
	}
	if got := authOnce(t, p).Password; got != "tok" {
		t.Fatalf("got %q", got)
	}

	// The hung creation returns: the client stored first is kept, and the late one closed.
	close(release)
	select {
	case <-abandoned: // the abandoned setup has tried to store its client
	case <-time.After(5 * time.Second):
		t.Fatal("the abandoned setup didn't return after its client creation did")
	}
	p, err = newTokenAuthProvider(context.Background(), TokenAuthOptions{SecretVersion: version})
	if err != nil {
		t.Fatal(err)
	}
	if got := authOnce(t, p).Password; got != "tok" {
		t.Errorf("got %q, want the token from the client stored first", got)
	}
	if _, err := accessSecretVersion(context.Background(), late, version); err == nil {
		t.Error("the late client is still open")
	}
	if n := created.Load(); n != 2 {
		t.Errorf("clients created = %d, want 2: the hung one and the later setup's", n)
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
