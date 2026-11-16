// Package valkeygcp connects valkey-go to Google Cloud Memorystore for Valkey with native
// Google Cloud authentication.
//
// NewClient takes the same valkey.ClientOption as valkey.NewClient, plus Options, and
// returns a ready valkey.Client:
//
//	client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{
//		InitAddress: []string{"10.0.0.15:6379"}, // the instance's discovery endpoint
//	}, valkeygcp.WithDefaultIAM())
//
// NewClient fills Memorystore defaults (retry backoff, cluster topology refresh through the
// discovery endpoint, reads sent to the reader endpoint) into the fields left empty, and
// adds the credentials its Options ask for:
//   - WithDefaultIAM authenticates with IAM access tokens from Application Default
//     Credentials, which are replaced on live connections before they expire.
//   - WithIAMOptions does the same with another identity (a key file, a Workload Identity
//     Federation file, an impersonated service account or your own token source), the
//     principal's email as the username, or other refresh settings.
//
// NewClient also retries Memorystore's IAM backend errors (ERR_IAM_EXHAUSTED, ERR_IAM_OTHER)
// while it connects, and after such an error makes new connections wait before dialing.
//
// ResolveClientOption returns the same valkey.ClientOption without connecting, for
// valkey.NewClient or other valkey-go modules such as valkeyotel.
package valkeygcp
