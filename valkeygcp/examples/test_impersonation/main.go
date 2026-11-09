// Command test_impersonation checks service account impersonation: it builds an IAM token
// provider with IAMAuthOptions.ImpersonateSA set to the service account in
// VALKEY_GCP_IMPERSONATE_SA, fetches one token for it and prints its length and expiry;
// the token itself is never printed. Impersonation lets a workload authenticate as another service account, for
// example one that may connect to an instance in another project.
//
// The identity that impersonates is Application Default Credentials; it needs
// roles/iam.serviceAccountTokenCreator on the target service account. The program doesn't
// connect to Memorystore. Run it from the valkeygcp/examples directory:
//
//	VALKEY_GCP_IMPERSONATE_SA=sa@proj.iam.gserviceaccount.com go run ./test_impersonation
package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/valkey-io/valkey-go/valkeygcp"
)

func main() {
	ctx := context.Background()
	targetSA := os.Getenv("VALKEY_GCP_IMPERSONATE_SA")
	if targetSA == "" {
		panic("VALKEY_GCP_IMPERSONATE_SA environment variable is required")
	}

	provider, err := valkeygcp.NewIAMTokenProvider(ctx, valkeygcp.IAMAuthOptions{
		ImpersonateSA: targetSA,
	})
	if err != nil {
		panic(err)
	}

	tok, err := provider.Token(ctx)
	if err != nil {
		panic(fmt.Sprintf("Impersonation failed: %v", err))
	}

	fmt.Printf("✅ Successfully acquired impersonated token for: %s\n", targetSA)
	// The token is a credential, so only its length and expiry are shown.
	fmt.Printf("Token Length: %d characters (not printed)\n", len(tok.AccessToken))
	fmt.Printf("Expiration:   %s (in %v)\n", tok.Expiry.Format(time.RFC3339), time.Until(tok.Expiry).Round(time.Second))
}
