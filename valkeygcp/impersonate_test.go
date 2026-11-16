package valkeygcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// iamRequest is one generateAccessToken request that the fake IAM Credentials API received.
type iamRequest struct {
	path, auth string
	delegates  []string
	scope      []string
	lifetime   string
}

// fakeIAMCredentials serves generateAccessToken on a local server and points
// iamCredentialsEndpoint at it. reply answers the n-th request, counted from 1. The returned
// function lists the requests received so far.
func fakeIAMCredentials(t *testing.T, reply func(n int, w http.ResponseWriter)) func() []iamRequest {
	t.Helper()
	var mu sync.Mutex
	var reqs []iamRequest
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Delegates []string `json:"delegates"`
			Scope     []string `json:"scope"`
			Lifetime  string   `json:"lifetime"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		reqs = append(reqs, iamRequest{r.Method + " " + r.URL.Path, r.Header.Get("Authorization"), body.Delegates, body.Scope, body.Lifetime})
		n := len(reqs)
		mu.Unlock()
		reply(n, w)
	}))
	t.Cleanup(srv.Close)
	orig := iamCredentialsEndpoint
	iamCredentialsEndpoint = srv.URL
	t.Cleanup(func() { iamCredentialsEndpoint = orig })
	return func() []iamRequest {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(reqs)
	}
}

// issueTokens answers like generateAccessToken: the n-th token is imp-n, and expires after
// expiresIn.
func issueTokens(expiresIn time.Duration) func(int, http.ResponseWriter) {
	return func(n int, w http.ResponseWriter) {
		_ = json.NewEncoder(w).Encode(map[string]string{
			"accessToken": fmt.Sprintf("imp-%d", n),
			"expireTime":  time.Now().Add(expiresIn).UTC().Format(time.RFC3339Nano),
		})
	}
}

// fakeADC makes Application Default Credentials return ts. It returns the scopes they were
// asked for.
func fakeADC(t *testing.T, ts oauth2.TokenSource) *[]string {
	t.Helper()
	orig := findDefaultCredentials
	t.Cleanup(func() { findDefaultCredentials = orig })
	var scopes []string
	findDefaultCredentials = func(_ context.Context, params google.CredentialsParams) (*google.Credentials, error) {
		scopes = params.Scopes
		return &google.Credentials{TokenSource: ts}, nil
	}
	return &scopes
}

// ImpersonateSA tokens come from generateAccessToken, called with the impersonating identity's
// token (here from ADC) for the account's resource name, with the delegation chain as resource
// names, the scopes and the lifetime in seconds. The impersonating identity asks for the
// cloud-platform scope, which the API needs. Setup asks for no token, and a token that is
// still fresh is reused. UsernamePrincipalEmail sends the impersonated account.
func TestImpersonation_Request(t *testing.T) {
	requests := fakeIAMCredentials(t, issueTokens(time.Hour))
	adcScopes := fakeADC(t, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "adc-token"}))
	const sa = "reader@db-proj.iam.gserviceaccount.com"
	p, err := NewIAMTokenProvider(context.Background(), IAMAuthOptions{
		ImpersonateSA:       sa,
		Delegates:           []string{"hop@p.iam.gserviceaccount.com"},
		ImpersonateLifetime: 2 * time.Hour,
		Scopes:              []string{"scope-a"},
		UsernameStrategy:    UsernamePrincipalEmail,
	})
	if err != nil {
		t.Fatal(err)
	}
	if n := len(requests()); n != 0 {
		t.Fatalf("setup made %d generateAccessToken requests, want 0", n)
	}
	for range 2 {
		tok, err := p.Token(context.Background())
		if err != nil || tok.AccessToken != "imp-1" || time.Until(tok.Expiry) < 59*time.Minute {
			t.Fatalf("token = %+v, %v; want imp-1, expiring in about an hour", tok, err)
		}
	}
	want := iamRequest{
		path:      "POST /v1/projects/-/serviceAccounts/" + sa + ":generateAccessToken",
		auth:      "Bearer adc-token",
		delegates: []string{"projects/-/serviceAccounts/hop@p.iam.gserviceaccount.com"},
		scope:     []string{"scope-a"},
		lifetime:  "7200s",
	}
	if reqs := requests(); len(reqs) != 1 || !reflect.DeepEqual(reqs[0], want) {
		t.Fatalf("requests = %+v, want only %+v", reqs, want)
	}
	if !slices.Equal(*adcScopes, []string{DefaultCloudPlatformScope}) {
		t.Errorf("ADC scopes = %v, want cloud-platform for the IAM Credentials API", *adcScopes)
	}
	if creds, _ := p.AuthCredentialsFn()(valkey.AuthCredentialsContext{}); creds.Username != sa || creds.Password != "imp-1" {
		t.Errorf("credentials = %q, %q; want the impersonated account and its token", creds.Username, creds.Password)
	}

	// The defaults: no delegates, a lifetime of 1h, the cloud-platform scope.
	p, err = NewIAMTokenProvider(context.Background(), IAMAuthOptions{ImpersonateSA: sa})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	reqs := requests()
	if last := reqs[len(reqs)-1]; last.delegates != nil || last.lifetime != "3600s" || !slices.Equal(last.scope, []string{DefaultCloudPlatformScope}) {
		t.Errorf("default request = %+v, want no delegates, 3600s and cloud-platform", last)
	}
}

// Each new token takes a new generateAccessToken call with the lifetime asked for, and the
// provider's cache decides when: a token within RefreshBuffer of expiring is replaced on the
// next fetch. No token is kept from setup, so the first token handed out can't be one that
// expired while the provider waited to be used. Setup refuses a negative lifetime, one not
// longer than RefreshBuffer, and one over 12h, the API's maximum, before ADC is looked up.
func TestImpersonation_RenewsTokens(t *testing.T) {
	fakeADC(t, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "adc-token"}))
	requests := fakeIAMCredentials(t, issueTokens(time.Minute)) // inside the 3m45s refresh buffer
	const sa = "sa@p.iam.gserviceaccount.com"
	p, err := NewIAMTokenProvider(context.Background(), IAMAuthOptions{ImpersonateSA: sa, ImpersonateLifetime: 12 * time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []string{"imp-1", "imp-2"} {
		tok, err := p.Token(context.Background())
		if err != nil || tok.AccessToken != want {
			t.Fatalf("token %d = %v, %v; want %s, a new one each time", i, tok, err, want)
		}
	}
	if reqs := requests(); len(reqs) != 2 || reqs[0].lifetime != "43200s" || reqs[1].lifetime != "43200s" {
		t.Fatalf("requests = %+v, want 2, each asking for 43200s", reqs)
	}
	orig := findDefaultCredentials
	t.Cleanup(func() { findDefaultCredentials = orig })
	findDefaultCredentials = func(context.Context, google.CredentialsParams) (*google.Credentials, error) {
		return nil, errors.New("ADC must not be looked up for a refused lifetime")
	}
	for lifetime, want := range map[time.Duration]string{
		-time.Minute:               "must be longer than RefreshBuffer",
		DefaultRefreshBuffer:       "must be longer than RefreshBuffer",
		12*time.Hour + time.Second: "must be at most 12h",
		13 * time.Hour:             "must be at most 12h",
	} {
		if _, err := NewIAMTokenProvider(context.Background(), IAMAuthOptions{ImpersonateSA: sa, ImpersonateLifetime: lifetime}); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("ImpersonateLifetime %v: got %v, want an error containing %q", lifetime, err, want)
		}
	}
}

// With CredentialsJSON, the identity in it impersonates ImpersonateSA, here user credentials
// whose token endpoint is a local server; ADC isn't looked up.
func TestImpersonation_CredentialsJSONImpersonates(t *testing.T) {
	tokenSrv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"access_token":"user-token","token_type":"Bearer","expires_in":3600}`))
	}))
	defer tokenSrv.Close()
	orig := findDefaultCredentials
	t.Cleanup(func() { findDefaultCredentials = orig })
	findDefaultCredentials = func(context.Context, google.CredentialsParams) (*google.Credentials, error) {
		return nil, errors.New("ADC must not be looked up")
	}
	requests := fakeIAMCredentials(t, issueTokens(time.Hour))
	user := fmt.Sprintf(`{"type":"authorized_user","client_id":"c","client_secret":"s","refresh_token":"r","token_uri":%q}`, tokenSrv.URL)
	p, err := NewIAMTokenProvider(context.Background(), IAMAuthOptions{ImpersonateSA: "sa@p.iam.gserviceaccount.com", CredentialsJSON: []byte(user)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := p.Token(context.Background()); err != nil {
		t.Fatal(err)
	}
	if reqs := requests(); len(reqs) != 1 || reqs[0].auth != "Bearer user-token" {
		t.Fatalf("requests = %+v, want one, made with the user's token", reqs)
	}
}

// generateAccessToken errors reach the token fetch with Google's status and message. A
// response without a token, a cut-off response, an impersonating identity without a token, an
// unusable endpoint and missing ADC fail too.
func TestImpersonation_Errors(t *testing.T) {
	ctx := context.Background()
	const sa = "sa@p.iam.gserviceaccount.com"
	fakeADC(t, oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "adc-token"}))
	for _, c := range []struct {
		name  string
		reply func(int, http.ResponseWriter)
		want  string
	}{
		{"permission denied", func(_ int, w http.ResponseWriter) {
			w.WriteHeader(http.StatusForbidden)
			_, _ = w.Write([]byte(`{"error":{"code":403,"message":"Permission 'iam.serviceAccounts.getAccessToken' denied on resource (or it may not exist).","status":"PERMISSION_DENIED"}}`))
		}, "impersonating " + sa + ": 403 Forbidden PERMISSION_DENIED: Permission 'iam.serviceAccounts.getAccessToken' denied"},
		{"error without details", func(_ int, w http.ResponseWriter) {
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("<html>bad gateway</html>"))
		}, "impersonating " + sa + ": 502 Bad Gateway"},
		{"no token", func(_ int, w http.ResponseWriter) {
			_, _ = w.Write([]byte(`{"expireTime":"2030-01-01T00:00:00Z"}`))
		}, "unreadable generateAccessToken response"},
		{"not JSON", func(_ int, w http.ResponseWriter) {
			_, _ = w.Write([]byte("ok"))
		}, "unreadable generateAccessToken response"},
		{"cut off", func(_ int, w http.ResponseWriter) {
			w.Header().Set("Content-Length", "100")
			_, _ = w.Write([]byte(`{"accessToken":`))
		}, "unexpected EOF"},
	} {
		t.Run(c.name, func(t *testing.T) {
			fakeIAMCredentials(t, c.reply)
			p, err := NewIAMTokenProvider(ctx, IAMAuthOptions{ImpersonateSA: sa})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := p.Token(ctx); err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("got %v, want %q", err, c.want)
			}
		})
	}

	t.Run("impersonating identity without a token", func(t *testing.T) {
		fakeADC(t, tokenSourceFunc(func() (*oauth2.Token, error) { return nil, errors.New("metadata server unreachable") }))
		requests := fakeIAMCredentials(t, issueTokens(time.Hour))
		p, err := NewIAMTokenProvider(ctx, IAMAuthOptions{ImpersonateSA: sa})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Token(ctx); err == nil || !strings.Contains(err.Error(), "metadata server unreachable") || len(requests()) != 0 {
			t.Fatalf("got %v after %d requests, want the token error before any request", err, len(requests()))
		}
	})

	t.Run("unusable endpoint", func(t *testing.T) {
		orig := iamCredentialsEndpoint
		t.Cleanup(func() { iamCredentialsEndpoint = orig })
		iamCredentialsEndpoint = "http://[::1" // not a URL
		p, err := NewIAMTokenProvider(ctx, IAMAuthOptions{ImpersonateSA: sa})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := p.Token(ctx); err == nil || !strings.Contains(err.Error(), "impersonating "+sa) {
			t.Fatalf("got %v, want the request error", err)
		}
	})

	t.Run("no ADC", func(t *testing.T) {
		orig := findDefaultCredentials
		t.Cleanup(func() { findDefaultCredentials = orig })
		findDefaultCredentials = func(context.Context, google.CredentialsParams) (*google.Credentials, error) {
			return nil, errors.New("could not find default credentials")
		}
		if _, err := NewIAMTokenProvider(ctx, IAMAuthOptions{ImpersonateSA: sa}); err == nil ||
			!strings.Contains(err.Error(), "Application Default Credentials not found") {
			t.Fatalf("got %v, want the ADC error", err)
		}
	})
}

// Each generateAccessToken request stops after FetchTimeout, so a request that hangs doesn't
// outlive the fetch that gave up on it.
func TestImpersonation_RequestTimeout(t *testing.T) {
	release := make(chan struct{})
	fakeIAMCredentials(t, func(int, http.ResponseWriter) { <-release }) // an API that never answers
	defer close(release)                                                // before the server closes
	src := newImpersonatedSource(context.Background(), oauth2.StaticTokenSource(&oauth2.Token{AccessToken: "adc-token"}), IAMAuthOptions{
		ImpersonateSA: "sa@p.iam.gserviceaccount.com",
		Scopes:        []string{DefaultCloudPlatformScope},
		FetchTimeout:  50 * time.Millisecond,
	})
	start := time.Now()
	if _, err := src.Token(); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want the request deadline", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("request took %v despite a 50ms FetchTimeout", d)
	}
}

// A hung lookup of the impersonating identity's credentials stops at the caller's deadline.
func TestNewIAMTokenProvider_ImpersonationSetupHonorsContext(t *testing.T) {
	orig := findDefaultCredentials
	entered, release := make(chan struct{}), make(chan struct{})
	findDefaultCredentials = func(context.Context, google.CredentialsParams) (*google.Credentials, error) {
		close(entered)
		<-release // a lookup that never answers
		return nil, errors.New("released")
	}
	t.Cleanup(func() {
		select { // the stub has read findDefaultCredentials before it is restored
		case <-entered:
		case <-time.After(5 * time.Second):
		}
		close(release)
		findDefaultCredentials = orig
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := NewIAMTokenProvider(ctx, IAMAuthOptions{ImpersonateSA: "sa@p.iam.gserviceaccount.com"})
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "credential setup") {
		t.Fatalf("got %v, want the caller's deadline", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("setup took %v despite a 50ms deadline", d)
	}
}
