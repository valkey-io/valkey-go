# valkeygcp: Google Cloud Memorystore Integration for Valkey-Go

[![Go Reference](https://pkg.go.dev/badge/github.com/valkey-io/valkey-go/valkeygcp.svg)](https://pkg.go.dev/github.com/valkey-io/valkey-go/valkeygcp)

`valkeygcp` is the official Google Cloud sub-module for [`valkey-go`](https://github.com/valkey-io/valkey-go), integrating it with **Google Cloud Memorystore for Valkey**.

---

## 1. Native Google Cloud IAM Authentication

`valkeygcp` connects valkey-go to Memorystore for Valkey with IAM authentication instead of a password. `valkeygcp.NewClient` takes the same `valkey.ClientOption` as `valkey.NewClient`, as `valkeyotel.NewClient` does, fills in Memorystore-friendly defaults, and adds IAM credentials: `WithDefaultIAM()` uses Application Default Credentials (ADC), and `WithIAMOptions` lets you choose the identity (a key file, a federation file, an impersonated service account or your own token source), the username and the refresh settings.

### 1.1 Features

* **The valkey-go way:** pass any `valkey.ClientOption`, with as many addresses as you need in `InitAddress` and `Standalone.ReplicaAddress`.
* **IAM authentication:** OAuth2 access tokens from ADC (GKE Workload Identity, the metadata server, or a service account key in `GOOGLE_APPLICATION_CREDENTIALS`), or from your own `oauth2.TokenSource`.
* **Other identities:** a service account key, or a Workload Identity Federation file that impersonates a service account (`CredentialsJSON`), or service account impersonation (`ImpersonateSA`, with `Delegates` and tokens lasting up to 12 hours), for workloads that don't run as the identity with access: cross-project access and key-based setups.
* **The principal's email as the username:** `UsernamePrincipalEmail` sends the IAM principal's email instead of `"default"`; Memorystore accepts either. A username Memorystore would refuse fails at setup when the principal is known, and an ACL rule that blocks `CLIENT TRACKING` gets an error saying so.
* **IAM backend errors:** Memorystore answers `ERR_IAM_EXHAUSTED` when its IAM backend is over quota, and `ERR_IAM_OTHER` for an invalid username or a transient IAM backend error; Google recommends retrying them with exponential backoff. `NewClient` retries those errors while connecting, with exponential backoff, and after such an error new connections wait before dialing.
* **Token rotation on live connections:** connections re-AUTH with a new token before the old one expires, spread out so they don't all re-AUTH at once.
* **Refresh failure tolerance:** if a refresh fails, live connections keep the last valid token, and `OnCredentialError` reports the error.
* **Bounded setup and token fetches:** `ctx` bounds setup, and every token fetch is capped by `FetchTimeout`.
* **Memorystore defaults:** retry backoff with jitter, regular cluster topology refresh through the discovery endpoint, and reads sent to the reader endpoint. Timeouts stay valkey-go's.
* **Works with other valkey-go modules:** `valkeylock`, `valkeylimiter`, `valkeyaside` and `valkeyotel`.
* **No new dependencies for valkey-go:** the Google Cloud dependencies stay in this sub-module.

### 1.2 Installation

```bash
go get github.com/valkey-io/valkey-go/valkeygcp
```

### 1.3 Quickstart

#### Connect with Default IAM (Cluster Mode)

```go
package main

import (
	"context"
	"log"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeygcp"
)

func main() {
	ctx := context.Background()

	// IAM access tokens from Application Default Credentials (ADC) or GKE Workload Identity:
	client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{
		InitAddress: []string{"10.0.0.15:6379"}, // discovery endpoint
	}, valkeygcp.WithDefaultIAM())
	if err != nil {
		log.Fatalf("Failed to connect to Memorystore: %v", err)
	}
	defer client.Close()

	// Execute Valkey commands normally - tokens are refreshed in the background:
	err = client.Do(ctx, client.B().Set().Key("mykey").Value("myval").Build()).Error()
	if err != nil {
		log.Fatalf("SET failed: %v", err)
	}
}
```

`InitAddress` holds the discovery endpoint of a cluster-mode instance, or the primary endpoint of a cluster-disabled instance; valkey-go detects which one it is when it connects. Any other `valkey.ClientOption` field works as with `valkey.NewClient`.

#### Cluster Mode Disabled with a Reader Endpoint

```go
client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{
	InitAddress: []string{"10.0.0.2:6379"},                                         // primary endpoint
	Standalone:  valkey.StandaloneOption{ReplicaAddress: []string{"10.0.0.3:6379"}}, // reader endpoint
}, valkeygcp.WithDefaultIAM())
```

Read-only commands that don't block go to the reader endpoint. Leave out `Standalone.ReplicaAddress` for an instance without replicas.

#### Connect with Custom IAM Options

`WithDefaultIAM()` is `WithIAMOptions(valkeygcp.IAMAuthOptions{})`: ADC with every default. Pass `WithIAMOptions` to change any of them:

```go
client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{
	InitAddress: []string{"10.0.0.15:6379"},
}, valkeygcp.WithIAMOptions(valkeygcp.IAMAuthOptions{
	CustomTokenSource: tokenSource, // any oauth2.TokenSource instead of ADC
	OnCredentialError: func(err error) { log.Printf("IAM token refresh failed, keeping the last token: %v", err) },
	MaxAuthPerSecond:  50, // pace AUTHs when many connections reconnect at once
}))
```

| `IAMAuthOptions` field   | Default                                                                | Meaning                                                                                                  |
| :------------------------- | :--------------------------------------------------------------------- | :------------------------------------------------------------------------------------------------------- |
| `CustomTokenSource`      | ADC                                                                    | Where access tokens come from                                                                            |
| `Username`               | `ClientOption.Username`, else picked by `UsernameStrategy` (`"default"`) | The AUTH username; Memorystore accepts only `"default"` or the principal's email                       |
| `UsernameStrategy`       | `UsernameDefault`                                                    | `UsernamePrincipalEmail` sends the principal's email when `Username` is empty (see 1.7)              |
| `CredentialsJSON`        | none (ADC)                                                             | A service account key, or a Workload Identity Federation file that impersonates a service account, used instead of ADC |
| `ImpersonateSA`          | none                                                                   | A service account to impersonate: the tokens are issued for it                                           |
| `Delegates`              | none                                                                   | Delegation chain for `ImpersonateSA`                                                                   |
| `ImpersonateLifetime`    | 1h                                                                     | How long impersonated tokens last, up to 12h; must be longer than `RefreshBuffer`                      |
| `Scopes`                 | `cloud-platform` (`DefaultCloudPlatformScope`)                     | OAuth2 scopes requested for the tokens; not used with `CustomTokenSource`                              |
| `RefreshBuffer`          | 3m45s (`DefaultRefreshBuffer`)                                       | How long before expiry a token is replaced; live connections re-AUTH in the first half of that window    |
| `FetchTimeout`           | 10s (`DefaultTokenFetchTimeout`)                                     | Cap on one token fetch; zero or negative means the default                                               |
| `DisableProactiveReauth` | `false`                                                              | When true, live connections aren't re-AUTHed; every new connection still gets a fresh token              |
| `MaxAuthPerSecond`       | 0 (off)                                                                | Limits how many AUTHs the client sends per second; N connections at once wait up to about N/rate seconds |
| `OnCredentialError`      | none                                                                   | Receives token fetch errors while live connections keep the last valid token                             |

#### Use Another Identity

Impersonate a service account, for example one that can reach an instance in another project:

```go
client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}},
	valkeygcp.WithIAMOptions(valkeygcp.IAMAuthOptions{
		ImpersonateSA: "memorystore-reader@database-project.iam.gserviceaccount.com",
	}),
)
```

The identity that impersonates is ADC, or `CredentialsJSON` when you set it. It needs `roles/iam.serviceAccountTokenCreator` on the impersonated account, and the impersonated account needs `roles/memorystore.dbConnectionUser`. With `ImpersonateLifetime`, impersonated tokens last up to 12 hours (more than 1 hour needs the organization policy constraint `constraints/iam.allowServiceAccountCredentialLifetimeExtension`); they are renewed like any other token.

Or use a key file, or a Workload Identity Federation file that impersonates a service account, instead of ADC:

```go
credentials, err := os.ReadFile("/secrets/valkey-client.json")
if err != nil {
	log.Fatal(err)
}
client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}},
	valkeygcp.WithIAMOptions(valkeygcp.IAMAuthOptions{CredentialsJSON: credentials}))
```

#### Use valkey.NewClient Directly

`NewIAMTokenProvider` builds the IAM token provider on its own. Its `AuthCredentialsFn()` plugs into `valkey.ClientOption`:

```go
provider, err := valkeygcp.NewIAMTokenProvider(ctx, valkeygcp.IAMAuthOptions{})
if err != nil {
	log.Fatal(err)
}
client, err := valkey.NewClient(valkey.ClientOption{
	InitAddress:       []string{"10.0.0.15:6379"},
	AuthCredentialsFn: provider.AuthCredentialsFn(),
})
```

This skips the Memorystore defaults; `valkeygcp.ResolveClientOption` returns a `valkey.ClientOption` with the defaults, the credentials and the dial gate (see 1.6). A client built from it skips `NewClient`'s retries while connecting and its `CLIENT TRACKING` hint. `provider.Token(ctx)` returns the current access token, for example to check that ADC works.

#### Use with Other valkey-go Modules

```go
// valkeylock, valkeylimiter and valkeyaside build their client through ClientBuilder:
locker, err := valkeylock.NewLocker(valkeylock.LockerOption{
	ClientOption: valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}},
	ClientBuilder: func(option valkey.ClientOption) (valkey.Client, error) {
		return valkeygcp.NewClient(ctx, option, valkeygcp.WithDefaultIAM())
	},
})

// valkeyotel builds the client itself, so pass it the ClientOption valkeygcp prepared:
option, err := valkeygcp.ResolveClientOption(ctx, valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}},
	valkeygcp.WithDefaultIAM())
if err != nil {
	log.Fatal(err) // the credential setup failed: don't build a client from the empty option
}
client, err := valkeyotel.NewClient(option)
```

### 1.4 Defaults

`NewClient` and `ResolveClientOption` fill these into the `valkey.ClientOption` fields you leave at zero:

| Field              | Default                                                                                                                                                                                                                                                                                    |
| :----------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RetryDelay`     | Exponential backoff with full jitter, up to 2s; stops after 20 retries                                                                                                                                                                                                                     |
| `ClusterOption`  | Only when left entirely at zero: `ShardsRefreshInterval` 10s, `PreferInitAddressRefresh` and `PreferClusterShards` on, so the topology is refreshed through the discovery endpoint with `CLUSTER SHARDS`. Set any `ClusterOption` field and the whole `ClusterOption` is yours |
| `SendToReplicas` | Read-only commands that don't block go to the replicas; set only with `Standalone.ReplicaAddress`                                                                                                                                                                                        |
| `DialCtxFn` (with an IAM option) | Wrapped by the dial gate (see 1.6); your own `DialCtxFn` or `DialFn` still does the dialing                                                                                                                                                                                            |

The cluster settings are left out when `Standalone.ReplicaAddress` is set. valkey-go ignores them when it ends up with a single-node client.

Each of these follows Google's [Valkey client best practices](https://docs.cloud.google.com/memorystore/docs/valkey/general-best-practices#valkey_client_best_practices): reconnect with exponential backoff and jitter, discover nodes through the discovery endpoint with `CLUSTER SHARDS`, refresh the topology periodically, and read from the reader endpoint. Timeouts are not changed: `Dialer.Timeout`, `Dialer.KeepAlive` and `ConnWriteTimeout` keep valkey-go's defaults (5s, 1s and 10s).

### 1.5 Token Refresh and Failures

* **When tokens are refreshed.** A token is replaced `RefreshBuffer` (3m45s) before it expires. Live connections re-AUTH with the new token at a random time in the first half of that window, so they don't all re-AUTH in the same second. `DisableProactiveReauth` turns this off; new connections still get a fresh token.
* **Why 3m45s.** It is the margin Google's own auth library uses ([`defaultExpiryDelta` in `cloud.google.com/go/auth`](https://github.com/googleapis/google-cloud-go/blob/auth/v0.24.0/auth/auth.go#L47-L49)): the metadata server can keep handing out the old token until 4 minutes before it expires, so asking earlier could get the same token again. valkeygcp passes the same margin to Application Default Credentials (`EarlyTokenRefresh`), so their own token cache renews at the same point. The Go OAuth2 library applies it only to metadata-server credentials.
* **Impersonated tokens** with `ImpersonateLifetime` are renewed the same way, `RefreshBuffer` before they expire.
* **Custom token sources.** valkeygcp asks for a new token once the current one has less than `RefreshBuffer` left. A `CustomTokenSource` that caches tokens must hand out a new one by then; otherwise live connections re-AUTH with the old token.
* **When a refresh fails.** Live connections keep the last valid token, and the refresh is retried about every 30 seconds. `OnCredentialError` receives each error. If the last token expires before a refresh succeeds, live connections stay authenticated (their re-AUTH is skipped), but new connections fail with `NOAUTH` until a token can be fetched again. Those `NOAUTH` errors go to the commands that opened the connections, not to `OnCredentialError`. With `DisableProactiveReauth`, an expired token isn't handed out at all: new connections fail with the token fetch error itself (`valkeygcp: token acquisition failed: …`), not `NOAUTH`, and `OnCredentialError` isn't called. The same happens before the first fetch has succeeded.
* **Token fetches are capped.** Each fetch waits at most `FetchTimeout` (10s); a fetch that times out counts as a failed one. While a timed-out fetch is still hanging, the next fetches fail at once instead of waiting another `FetchTimeout`, and a new fetch is tried after 3×`FetchTimeout`.
* **A rejected re-AUTH closes the connection.** If Memorystore rejects the re-AUTH of a live connection (for example with `ERR_IAM_EXHAUSTED`), valkey-go closes that connection and the next command dials a new one, although Memorystore would have kept the authenticated connection working.

### 1.6 Contexts and Setup Failures

* **`NewClient`'s `ctx` bounds setup only.** Finding the credentials, impersonation, the principal-email lookup and connecting stop when it is cancelled or its deadline passes, and after 15 seconds if it has no deadline. Token refreshes never use it, because they run for the life of the client.
* **IAM backend errors are retried while connecting.** `ERR_IAM_EXHAUSTED` and `ERR_IAM_OTHER` during the connection handshake are retried with exponential backoff (250ms doubling, up to 10s between attempts) within that time; the final error keeps Memorystore's reply.
* **Re-dials wait after IAM errors.** After Memorystore rejects a new connection with `ERR_IAM_*`, the next connections wait with the same backoff before dialing, so re-dials don't add to the load that caused the error. This dial gate wraps `ClientOption.DialCtxFn`, and only applies after the client has connected once.
* **Setup needs working credentials.** valkey-go connects and authenticates while it builds the client, so missing ADC, an invalid key file or a denied impersonation fails `NewClient` immediately, with the underlying error. Without ADC, set `CredentialsJSON` or `CustomTokenSource`.
* **`InitAddress` is required.** `NewClient` and `ResolveClientOption` return an error without it.
* **Credentials on the `ClientOption`** (`Password` or `AuthCredentialsFn`) are used as is; passing them together with `WithIAMOptions` or `WithDefaultIAM` is an error. With an IAM option, `ClientOption.Username` is the IAM username unless `IAMAuthOptions.Username` sets one; if both are set, they must match.

### 1.7 IAM Username and ACL Policies

The IAM username is `"default"` unless you set one. Memorystore accepts `"default"` or the IAM principal's email address, and `IAMAuthOptions.UsernameStrategy: valkeygcp.UsernamePrincipalEmail` sends the email. It is taken from, in order:

1. the impersonated account (`ImpersonateSA`);
2. the `client_email` of a service account key;
3. the impersonation URL of a Workload Identity Federation file;
4. the metadata server (Compute Engine, GKE Workload Identity, Cloud Run).

With a `CustomTokenSource`, valkeygcp can't tell whose tokens it returns: set `CredentialsJSON` too, to read the email from it, or set `Username`. On GKE, for a Kubernetes ServiceAccount linked to an IAM service account, the metadata server returns `NAME.svc.id.goog` instead of an email unless the Kubernetes ServiceAccount has the annotation `iam.gke.io/return-principal-id-as-email: "true"`.

An explicit username, `IAMAuthOptions.Username` or `ClientOption.Username`, always wins. Memorystore accepts only `"default"` or the principal's email, so another value fails at setup when the principal is known.

With an ACL policy, Memorystore matches its rules to the IAM principal that authenticates; Google recommends principal emails as the rules' usernames, and a principal without a rule gets the `default` user's permissions. A rule must also allow what valkey-go sends besides the application's commands: `client|tracking` on every new connection, unless `ClientOption.DisableCache` is set, and on cluster-mode instances `cluster|shards`, to read the topology. Without `client|tracking`, `NewClient` fails with an error that says so. Without `cluster|shards`, valkey-go falls back to a single-node client when `InitAddress` has one address, and keys on other shards then fail with `MOVED`.

### 1.8 Required Roles

| Mechanism | Role                                                                       |
| :-------- | :------------------------------------------------------------------------- |
| IAM Auth  | `roles/memorystore.dbConnectionUser` (`memorystore.instances.connect`) |
| Impersonation | `roles/iam.serviceAccountTokenCreator` on the impersonated account       |

Grant `roles/memorystore.dbConnectionUser` to the identity behind the access tokens: the ADC identity, the identity in `CredentialsJSON`, or the identity of `CustomTokenSource`. With `ImpersonateSA`, grant it to the impersonated account instead, and give the identity that impersonates (ADC, or `CredentialsJSON`) `roles/iam.serviceAccountTokenCreator` on that account. With `Delegates`, each service account in the chain needs that role on the next one, and the last one on the impersonated account.

### 1.9 Testing

Run the unit tests with Go's race detector:

```bash
go test -v -race .
```

The unit tests need no cloud access. `TestRefreshTolerance_LiveConnection` also runs against a local Valkey on `127.0.0.1:6379` (the repository's `docker compose` setup) and is skipped when none is running.

Programs for manual checks live in `examples/`, a separate Go module, so they aren't part of the `valkeygcp` module. Run them from the `examples` directory, for example `go run ./accelerated_test`:

| Program              | What it does                                                               | Needs                                                                            |
| :------------------- | :------------------------------------------------------------------------- | :------------------------------------------------------------------------------- |
| `test_adc`         | Fetches one IAM access token through ADC and prints its type and expiry    | ADC                                                                              |
| `accelerated_test` | Simulates token rotation with tokens that expire after 10 seconds          | Nothing                                                                          |
| `live_e2e`         | Sends SET/GET traffic to an instance until stopped, across token rotations | ADC, network access to the instance, `MEMORYSTORE_TEST_DISCOVERY_ENDPOINT` set; optionally `VALKEY_GCP_IMPERSONATE_SA` to connect through impersonation |
| `test_impersonation` | Fetches one token for the service account in `VALKEY_GCP_IMPERSONATE_SA` and prints its length and expiry | ADC with `roles/iam.serviceAccountTokenCreator` on that account                |
| `iamprobe`         | Shows the `ERR_IAM_*` retries and the dial gate against a local fake server | Nothing                                                                          |
