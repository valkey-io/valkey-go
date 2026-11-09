package valkeygcp

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/valkey-io/valkey-go"
	"golang.org/x/oauth2/google"
)

// fakeMetadata points the metadata seams at a test metadata server, or at none when srv is nil.
func fakeMetadata(t *testing.T, srv *httptest.Server) {
	t.Helper()
	origOnGCE, origEmail := metadataOnGCE, metadataEmail
	t.Cleanup(func() { metadataOnGCE, metadataEmail = origOnGCE, origEmail })
	if srv == nil {
		metadataOnGCE = func(context.Context) bool { return false }
		return
	}
	// The real metadata client honors GCE_METADATA_HOST on every request.
	t.Setenv("GCE_METADATA_HOST", srv.Listener.Addr().String())
	metadataOnGCE = func(context.Context) bool { return true }
}

func TestUsername_Strategies(t *testing.T) {
	fakeMetadata(t, nil)
	const (
		saKey = `{"type":"service_account","client_email":"key@p.iam.gserviceaccount.com"}`
		wif   = `{"type":"external_account","service_account_impersonation_url":"https://iamcredentials.googleapis.com/v1/projects/-/serviceAccounts/wif@p.iam.gserviceaccount.com:generateAccessToken"}`
	)
	cases := []struct {
		id   string
		opt  IAMAuthOptions
		json string
		want string
	}{
		{"explicit username wins", IAMAuthOptions{Username: "explicit", UsernameStrategy: UsernamePrincipalEmail, ImpersonateSA: "sa@p.iam.gserviceaccount.com"}, saKey, "explicit"},
		{"default strategy", IAMAuthOptions{}, saKey, DefaultIAMUsername},
		{"impersonated service account", IAMAuthOptions{UsernameStrategy: UsernamePrincipalEmail, ImpersonateSA: "sa@p.iam.gserviceaccount.com"}, saKey, "sa@p.iam.gserviceaccount.com"},
		{"service account key email", IAMAuthOptions{UsernameStrategy: UsernamePrincipalEmail}, saKey, "key@p.iam.gserviceaccount.com"},
		{"workload identity federation", IAMAuthOptions{UsernameStrategy: UsernamePrincipalEmail}, wif, "wif@p.iam.gserviceaccount.com"},
	}
	for _, c := range cases {
		got, err := resolveUsername(context.Background(), c.opt, []byte(c.json))
		if err != nil || got != c.want {
			t.Errorf("%s: got %q, %v; want %q", c.id, got, err, c.want)
		}
	}
}

// On Google Cloud, when no credentials JSON names an email, the principal-email strategy
// reads the email of the default service account from the metadata server.
func TestUsername_MetadataServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Metadata-Flavor") != "Google" || r.URL.Path != "/computeMetadata/v1/instance/service-accounts/default/email" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte("workload@p.iam.gserviceaccount.com"))
	}))
	defer srv.Close()
	fakeMetadata(t, srv)

	got, err := resolveUsername(context.Background(), IAMAuthOptions{UsernameStrategy: UsernamePrincipalEmail}, nil)
	if err != nil || got != "workload@p.iam.gserviceaccount.com" {
		t.Fatalf("got %q, %v", got, err)
	}
	// User credentials JSON carries no email: fall through to the metadata server.
	got, err = resolveUsername(context.Background(), IAMAuthOptions{UsernameStrategy: UsernamePrincipalEmail}, []byte(`{"type":"authorized_user"}`))
	if err != nil || got != "workload@p.iam.gserviceaccount.com" {
		t.Fatalf("authorized_user: got %q, %v", got, err)
	}
}

func TestUsername_MetadataServerErrors(t *testing.T) {
	srv := httptest.NewServer(http.NotFoundHandler())
	t.Cleanup(srv.Close)
	fakeMetadata(t, srv)
	metadataEmail = func(context.Context, string) (string, error) { return "", errors.New("metadata down") }
	if _, err := resolveUsername(context.Background(), IAMAuthOptions{UsernameStrategy: UsernamePrincipalEmail}, nil); err == nil ||
		!strings.Contains(err.Error(), "metadata down") {
		t.Fatalf("got %v", err)
	}
	metadataEmail = func(context.Context, string) (string, error) { return "", nil }
	if _, err := resolveUsername(context.Background(), IAMAuthOptions{UsernameStrategy: UsernamePrincipalEmail}, nil); err == nil {
		t.Fatal("an empty email must not become the username")
	}
}

// When the principal email can't be found (no usable credentials JSON and no metadata
// server), the principal-email strategy fails with an error that points to both username
// fields, ClientOption.Username and IAMAuthOptions.Username, and so does NewIAMTokenProvider.
func TestUsername_Unresolvable(t *testing.T) {
	fakeMetadata(t, nil)
	_, err := resolveUsername(context.Background(), IAMAuthOptions{UsernameStrategy: UsernamePrincipalEmail}, []byte(`{ not json`))
	if err == nil || !strings.Contains(err.Error(), "ClientOption.Username or IAMAuthOptions.Username") {
		t.Fatalf("got %v", err)
	}
	// The provider surfaces the same error at construction.
	_, err = NewIAMTokenProvider(context.Background(), IAMAuthOptions{CustomTokenSource: staticIAM().CustomTokenSource, UsernameStrategy: UsernamePrincipalEmail})
	if err == nil {
		t.Fatal("NewIAMTokenProvider accepted an unresolvable principal email")
	}
}

func TestCredTypeFromJSON(t *testing.T) {
	for in, want := range map[string]google.CredentialsType{
		`{"type":"external_account"}`: google.ExternalAccount,
		`{"type":"authorized_user"}`:  google.AuthorizedUser,
		`{}`:                          google.ServiceAccount,
		`{ invalid`:                   google.ServiceAccount,
	} {
		if got := credTypeFromJSON([]byte(in)); got != want {
			t.Errorf("%s: got %q, want %q", in, got, want)
		}
	}
}

// An explicit IAM username must be "default" or the principal's email: Memorystore answers
// any other username with ERR_IAM_OTHER. checkIAMUsername catches a mismatch during setup
// instead of on every dial, and lets the username through when the principal is unknown.
func TestCheckIAMUsername(t *testing.T) {
	fakeMetadata(t, nil) // not on GCE: principal unknown without JSON or impersonation
	const saKey = `{"type":"service_account","client_email":"key@p.iam.gserviceaccount.com"}`
	cases := []struct {
		name    string
		opt     IAMAuthOptions
		json    string
		wantErr bool
	}{
		{"empty", IAMAuthOptions{}, saKey, false},
		{"default", IAMAuthOptions{Username: DefaultIAMUsername}, saKey, false},
		{"key email", IAMAuthOptions{Username: "key@p.iam.gserviceaccount.com"}, saKey, false},
		{"key email, other case", IAMAuthOptions{Username: "KEY@p.iam.gserviceaccount.com"}, saKey, false},
		{"other email", IAMAuthOptions{Username: "other@p.iam.gserviceaccount.com"}, saKey, true},
		{"not an email", IAMAuthOptions{Username: "app-user"}, saKey, true},
		{"impersonated email", IAMAuthOptions{Username: "t@p.iam.gserviceaccount.com", ImpersonateSA: "t@p.iam.gserviceaccount.com"}, "", false},
		{"impersonation mismatch", IAMAuthOptions{Username: "key@p.iam.gserviceaccount.com", ImpersonateSA: "t@p.iam.gserviceaccount.com"}, saKey, true},
		{"custom source, principal unknown", IAMAuthOptions{Username: "app-user", CustomTokenSource: staticIAM().CustomTokenSource}, "", false},
		{"principal unresolvable", IAMAuthOptions{Username: "app-user"}, "", false},
	}
	for _, c := range cases {
		err := checkIAMUsername(context.Background(), c.opt, []byte(c.json))
		if (err != nil) != c.wantErr {
			t.Errorf("%s: got %v, want error %v", c.name, err, c.wantErr)
		}
		if err != nil && !strings.Contains(err.Error(), "isn't valid") {
			t.Errorf("%s: unexpected message %v", c.name, err)
		}
	}
}

// The check runs in NewIAMTokenProvider, before any token is handed out. Here the key in
// CredentialsJSON is the identity.
func TestNewIAMTokenProvider_RejectsInvalidUsername(t *testing.T) {
	fakeMetadata(t, nil)
	opt := IAMAuthOptions{
		CredentialsJSON: []byte(`{"type":"service_account","client_email":"key@p.iam.gserviceaccount.com"}`),
		Username:        "someone-else@p.iam.gserviceaccount.com",
	}
	if _, err := NewIAMTokenProvider(context.Background(), opt); err == nil || !strings.Contains(err.Error(), "key@p.iam.gserviceaccount.com") {
		t.Fatalf("got %v, want the username error naming the principal", err)
	}
}

// On GKE, a Kubernetes ServiceAccount linked to an IAM service account gets
// SERVICEACCOUNT_NAME.svc.id.goog from the metadata server unless the
// iam.gke.io/return-principal-id-as-email annotation is set. That isn't an email, so the
// principal-email strategy fails with the annotation hint, and the explicit-username check
// stays silent instead of rejecting a correct email.
func TestUsername_GKENonEmailIdentifier(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("my-ksa.svc.id.goog"))
	}))
	defer srv.Close()
	fakeMetadata(t, srv)

	_, err := resolveUsername(context.Background(), IAMAuthOptions{UsernameStrategy: UsernamePrincipalEmail}, nil)
	if err == nil || !strings.Contains(err.Error(), "iam.gke.io/return-principal-id-as-email") {
		t.Fatalf("got %v, want the GKE annotation hint", err)
	}
	if err := checkIAMUsername(context.Background(), IAMAuthOptions{Username: "sa@p.iam.gserviceaccount.com"}, nil); err != nil {
		t.Fatalf("explicit email rejected although the principal isn't known as an email: %v", err)
	}
}

// With a CustomTokenSource, only CredentialsJSON can say whose tokens it returns. The
// principal-email strategy takes the email from it, and without it fails, even on Google
// Cloud, where the metadata server would name the machine's own account instead. ImpersonateSA
// doesn't name it either, because CustomTokenSource takes precedence over it.
func TestUsername_CustomTokenSource(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("vm-default@p.iam.gserviceaccount.com"))
	}))
	defer srv.Close()
	fakeMetadata(t, srv)

	custom := staticIAM()
	custom.UsernameStrategy = UsernamePrincipalEmail
	if _, err := NewIAMTokenProvider(context.Background(), custom); !errors.Is(err, errCustomSourcePrincipal) {
		t.Fatalf("without CredentialsJSON: got %v, want errCustomSourcePrincipal", err)
	}

	withJSON := custom
	withJSON.CredentialsJSON = []byte(`{"type":"service_account","client_email":"custom@p.iam.gserviceaccount.com"}`)
	p, err := NewIAMTokenProvider(context.Background(), withJSON)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := p.AuthCredentialsFn()(valkey.AuthCredentialsContext{})
	if err != nil || creds.Username != "custom@p.iam.gserviceaccount.com" {
		t.Fatalf("with CredentialsJSON: username %q, %v; want the email in CredentialsJSON", creds.Username, err)
	}

	withSA := custom
	withSA.ImpersonateSA = "other@p.iam.gserviceaccount.com"
	if _, err := NewIAMTokenProvider(context.Background(), withSA); !errors.Is(err, errCustomSourcePrincipal) {
		t.Fatalf("with ImpersonateSA: got %v, want errCustomSourcePrincipal", err)
	}
}
