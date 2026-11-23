package valkeygcp

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
)

// iamCredentialsEndpoint is the IAM Service Account Credentials API, which issues the access
// tokens of an impersonated service account. It is a variable so tests can use a local
// server.
var iamCredentialsEndpoint = "https://iamcredentials.googleapis.com"

// defaultImpersonateLifetime is the lifetime asked for when ImpersonateLifetime is zero: the
// API's default, and its maximum unless an organization policy allows longer tokens.
const defaultImpersonateLifetime = time.Hour

// maxImpersonateLifetime is the longest lifetime generateAccessToken issues, for accounts
// that the organization policy constraints/iam.allowServiceAccountCredentialLifetimeExtension
// lists. withIAMDefaults refuses longer ones at setup, instead of the first token fetch.
const maxImpersonateLifetime = 12 * time.Hour

// impersonatorSource returns the token source of the identity that impersonates
// ImpersonateSA: CredentialsJSON when set, otherwise Application Default Credentials. Its
// tokens only call generateAccessToken, which needs the cloud-platform (or iam) scope,
// whatever Scopes asks of the impersonated tokens.
func impersonatorSource(ctx context.Context, opt IAMAuthOptions) (oauth2.TokenSource, error) {
	if len(opt.CredentialsJSON) > 0 {
		creds, err := google.CredentialsFromJSONWithType(ctx, opt.CredentialsJSON, credTypeFromJSON(opt.CredentialsJSON), DefaultCloudPlatformScope)
		if err != nil {
			return nil, fmt.Errorf("valkeygcp: invalid credentials JSON: %w", err)
		}
		return creds.TokenSource, nil
	}
	creds, err := findDefaultCredentials(ctx, google.CredentialsParams{Scopes: []string{DefaultCloudPlatformScope}})
	if err != nil {
		return nil, fmt.Errorf("valkeygcp: Application Default Credentials not found: %w", err)
	}
	return creds.TokenSource, nil
}

// impersonatedSource gets access tokens for a service account from the IAM Service Account
// Credentials API (projects.serviceAccounts.generateAccessToken), which it calls as the
// identity that impersonates. Every call asks for a new token with the configured lifetime;
// the provider's cache decides when a new one is needed, so no token is kept here that could
// be handed out after it expired. Plain HTTPS keeps google.golang.org/api, and the gRPC and
// OpenTelemetry stack it brings, out of every program that imports valkeygcp.
type impersonatedSource struct {
	ctx     context.Context // never cancelled: the source lives as long as the client
	client  *http.Client    // sends the impersonating identity's token with each request
	reqURL  string          // the account's generateAccessToken URL
	body    []byte          // the generateAccessToken request body
	target  string          // ImpersonateSA, for errors
	timeout time.Duration   // limits each request, so an abandoned one doesn't linger
}

// newImpersonatedSource returns the token source of opt.ImpersonateSA, impersonated by the
// identity whose tokens src returns. opt must have its defaults filled in.
func newImpersonatedSource(ctx context.Context, src oauth2.TokenSource, opt IAMAuthOptions) *impersonatedSource {
	lifetime := opt.ImpersonateLifetime
	if lifetime == 0 {
		lifetime = defaultImpersonateLifetime
	}
	req := struct {
		Delegates []string `json:"delegates,omitempty"` // only for a delegation chain
		Scope     []string `json:"scope"`
		Lifetime  string   `json:"lifetime"` // seconds, ending with "s", such as "3600s"
	}{Scope: opt.Scopes, Lifetime: strconv.FormatFloat(lifetime.Seconds(), 'f', -1, 64) + "s"}
	for _, d := range opt.Delegates {
		req.Delegates = append(req.Delegates, serviceAccountName(d))
	}
	body, _ := json.Marshal(req) // strings only: it can't fail
	return &impersonatedSource{
		ctx: ctx,
		// oauth2's transport fetches src's token without a context, holding its cache's lock,
		// so that fetch is capped like any other: a hung one mustn't block later requests.
		client:  oauth2.NewClient(ctx, withFetchTimeout(src, opt.FetchTimeout)),
		reqURL:  iamCredentialsEndpoint + "/v1/" + serviceAccountName(url.PathEscape(opt.ImpersonateSA)) + ":generateAccessToken",
		body:    body,
		target:  opt.ImpersonateSA,
		timeout: fetchTimeout(opt.FetchTimeout),
	}
}

// serviceAccountName is the name the IAM Credentials API gives a service account, in the
// request URL and in delegates: projects/-/serviceAccounts/EMAIL. The "-" wildcard is
// required; a project ID there is invalid.
func serviceAccountName(email string) string {
	return "projects/-/serviceAccounts/" + email
}

// Token asks the IAM Credentials API for a new access token for the service account.
func (s *impersonatedSource) Token() (*oauth2.Token, error) {
	ctx, cancel := context.WithTimeout(s.ctx, s.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.reqURL, bytes.NewReader(s.body))
	if err != nil {
		return nil, fmt.Errorf("impersonating %s: %w", s.target, err)
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req) // also fails when the impersonating identity gets no token
	if err != nil {
		return nil, fmt.Errorf("impersonating %s: %w", s.target, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	if err != nil {
		return nil, fmt.Errorf("impersonating %s: %w", s.target, err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("impersonating %s: %w", s.target, iamCredentialsError(resp.Status, body))
	}
	var r struct {
		AccessToken string    `json:"accessToken"`
		ExpireTime  time.Time `json:"expireTime"` // RFC 3339; always set
	}
	if err := json.Unmarshal(body, &r); err != nil || r.AccessToken == "" {
		return nil, fmt.Errorf("impersonating %s: unreadable generateAccessToken response", s.target)
	}
	return &oauth2.Token{AccessToken: r.AccessToken, TokenType: "Bearer", Expiry: r.ExpireTime}, nil
}

// iamCredentialsError turns an error response into an error, with Google's error status and
// message when the body has them, such as PERMISSION_DENIED when the impersonating identity
// lacks iam.serviceAccounts.getAccessToken on the account.
func iamCredentialsError(status string, body []byte) error {
	var e struct {
		Error struct {
			Status  string `json:"status"`
			Message string `json:"message"`
		} `json:"error"`
	}
	if json.Unmarshal(body, &e) == nil && e.Error.Message != "" {
		return fmt.Errorf("%s %s: %s", status, e.Error.Status, e.Error.Message)
	}
	return errors.New(status)
}
