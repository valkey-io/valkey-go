package valkeygcp

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"cloud.google.com/go/compute/metadata"
	"golang.org/x/oauth2/google"
)

// UsernameStrategy selects the username sent with IAM AUTH when none is set:
// IAMAuthOptions.Username is empty, and so is ClientOption.Username with NewClient or
// ResolveClientOption.
type UsernameStrategy int

// UsernameStrategy values.
const (
	UsernameDefault        UsernameStrategy = iota // "default"
	UsernamePrincipalEmail                         // the IAM principal's email, which Memorystore also accepts
)

// Metadata server lookups. They are variables because metadata.OnGCEWithContext caches its
// answer for the whole process, which tests need to bypass.
var (
	metadataOnGCE = metadata.OnGCEWithContext
	metadataEmail = metadata.EmailWithContext
)

// setUsername is the fix the principal-email errors suggest. With NewClient and
// ResolveClientOption, ClientOption.Username is the IAM username too (see withCallerUsername).
const setUsername = "set the username (ClientOption.Username or IAMAuthOptions.Username)"

// errCustomSourcePrincipal is returned for UsernamePrincipalEmail with a CustomTokenSource and
// no CredentialsJSON: nothing tells whose tokens the source returns.
var errCustomSourcePrincipal = errors.New("valkeygcp: UsernamePrincipalEmail can't tell whose tokens CustomTokenSource returns; " + setUsername + " to that principal's email, or CredentialsJSON to its credentials")

// resolveUsername returns the AUTH username: Username when set, otherwise the one
// UsernameStrategy picks. For UsernamePrincipalEmail the email comes from, in order,
// ImpersonateSA, the key file or federation file in credsJSON, and the metadata server.
// credsJSON is IAMAuthOptions.CredentialsJSON or the JSON of the Application Default
// Credentials (nil on the metadata server).
//
// With a CustomTokenSource, only CredentialsJSON can name the principal: the tokens don't
// come from ImpersonateSA, and the metadata server would name the machine's own account,
// which may not be the one the source returns tokens for.
func resolveUsername(ctx context.Context, opt IAMAuthOptions, credsJSON []byte) (string, error) {
	if opt.Username != "" {
		return opt.Username, nil
	}
	if opt.UsernameStrategy != UsernamePrincipalEmail {
		return DefaultIAMUsername, nil
	}
	if opt.CustomTokenSource != nil {
		if email := emailFromJSON(credsJSON); email != "" {
			return email, nil
		}
		return "", errCustomSourcePrincipal
	}
	if opt.ImpersonateSA != "" {
		return opt.ImpersonateSA, nil
	}
	if email := emailFromJSON(credsJSON); email != "" {
		return email, nil
	}
	if metadataOnGCE(ctx) { // GCE, GKE Workload Identity, Cloud Run
		email, err := metadataEmail(ctx, "default")
		if err != nil {
			return "", fmt.Errorf("valkeygcp: read the principal email from the metadata server (or %s): %w", setUsername, err)
		}
		if email == "" {
			return "", errors.New("valkeygcp: the metadata server returned no principal email; " + setUsername)
		}
		if !strings.Contains(email, "@") {
			// GKE returns SERVICEACCOUNT_NAME.svc.id.goog for Kubernetes ServiceAccounts linked to
			// IAM service accounts unless iam.gke.io/return-principal-id-as-email is set.
			return "", fmt.Errorf("valkeygcp: the metadata server returned %q, which isn't an email; on GKE add the annotation iam.gke.io/return-principal-id-as-email: \"true\" to the Kubernetes ServiceAccount, or %s", email, setUsername)
		}
		return email, nil
	}
	return "", errors.New("valkeygcp: cannot resolve the principal email; " + setUsername)
}

// emailFromJSON returns the principal email in credentials JSON: the client_email of a
// service account key, or the account in the impersonation URL of a Workload Identity
// Federation config. It returns "" when there is none, for example for user credentials.
func emailFromJSON(b []byte) string {
	var f struct {
		ClientEmail                    string `json:"client_email"`                      // service_account keys
		ServiceAccountImpersonationURL string `json:"service_account_impersonation_url"` // Workload Identity Federation configs
	}
	if len(b) == 0 || json.Unmarshal(b, &f) != nil {
		return ""
	}
	if f.ClientEmail != "" {
		return f.ClientEmail
	}
	if i := strings.LastIndex(f.ServiceAccountImpersonationURL, "/serviceAccounts/"); i >= 0 {
		email := f.ServiceAccountImpersonationURL[i+len("/serviceAccounts/"):]
		return strings.TrimSuffix(email, ":generateAccessToken")
	}
	return ""
}

// checkIAMUsername rejects an explicit IAM username that Memorystore would refuse with
// ERR_IAM_OTHER: "If you specify a username, then you must use either the default
// username or the IAM principal email address". It stays silent when the principal can't
// be determined (a custom token source without credentials JSON, or user credentials off
// Google Cloud), because the username may then be right.
func checkIAMUsername(ctx context.Context, opt IAMAuthOptions, credsJSON []byte) error {
	if opt.Username == "" || opt.Username == DefaultIAMUsername {
		return nil
	}
	if opt.CustomTokenSource != nil && len(credsJSON) == 0 {
		return nil
	}
	email, err := resolveUsername(ctx, IAMAuthOptions{
		UsernameStrategy:  UsernamePrincipalEmail,
		ImpersonateSA:     opt.ImpersonateSA,
		CustomTokenSource: opt.CustomTokenSource,
	}, credsJSON)
	if err != nil || !strings.Contains(email, "@") || strings.EqualFold(email, opt.Username) {
		return nil // principal unknown, or not an email (resolveUsername rejects those)
	}
	return fmt.Errorf("valkeygcp: IAM username %q isn't valid for %s: use %q (or no username) or the principal's email", opt.Username, email, DefaultIAMUsername)
}

// credTypeFromJSON reads the credential type (the "type" field) from credentials JSON, for
// google.CredentialsFromJSONWithType, which replaces the deprecated google.CredentialsFromJSON.
func credTypeFromJSON(b []byte) google.CredentialsType {
	var h struct {
		Type google.CredentialsType `json:"type"`
	}
	if json.Unmarshal(b, &h) != nil || h.Type == "" {
		return google.ServiceAccount // invalid JSON still fails inside CredentialsFromJSONWithType
	}
	return h.Type
}
