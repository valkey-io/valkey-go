# valkeygcp: Google Cloud Memorystore Integration for Valkey-Go

[![Go Reference](https://pkg.go.dev/badge/github.com/valkey-io/valkey-go/valkeygcp.svg)](https://pkg.go.dev/github.com/valkey-io/valkey-go/valkeygcp)

`valkeygcp` is the official Google Cloud sub-module for [`valkey-go`](https://github.com/valkey-io/valkey-go), integrating it with **Google Cloud Memorystore for Valkey**.

---

## 1. Native Google Cloud IAM Authentication

`valkeygcp` connects valkey-go to Memorystore for Valkey with IAM authentication instead of a password. `valkeygcp.NewClient` takes the same `valkey.ClientOption` as `valkey.NewClient`, as `valkeyotel.NewClient` does, fills in Memorystore-friendly defaults, and adds IAM credentials: `WithDefaultIAM()` uses Application Default Credentials (ADC), and `WithIAMOptions` lets you choose the token source, username and refresh settings. For instances with Token Auth, `WithTokenAuth` adds the auth token instead.

### 1.1 Features

* **The valkey-go way:** pass any `valkey.ClientOption`, with as many addresses as you need in `InitAddress` and `Standalone.ReplicaAddress`.
* **IAM authentication:** OAuth2 access tokens from ADC (GKE Workload Identity, the metadata server, or a service account key in `GOOGLE_APPLICATION_CREDENTIALS`), or from your own `oauth2.TokenSource`.
* **Token Auth:** for instances with Token Auth, the auth token comes from Secret Manager (recommended), a fixed value or your own source, and is re-read regularly so a rotated token reaches live connections.
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
| `Username`               | `ClientOption.Username`, else `"default"` (`DefaultIAMUsername`) | The AUTH username                                                                                        |
| `Scopes`                 | `cloud-platform` (`DefaultCloudPlatformScope`)                     | OAuth2 scopes requested from ADC; not used with `CustomTokenSource`                                    |
| `RefreshBuffer`          | 3m45s (`DefaultRefreshBuffer`)                                       | How long before expiry a token is replaced; live connections re-AUTH in the first half of that window    |
| `FetchTimeout`           | 10s (`DefaultTokenFetchTimeout`)                                     | Cap on one token fetch; zero or negative means the default                                               |
| `DisableProactiveReauth` | `false`                                                              | When true, live connections aren't re-AUTHed; every new connection still gets a fresh token              |
| `MaxAuthPerSecond`       | 0 (off)                                                                | Limits how many AUTHs the client sends per second; N connections at once wait up to about N/rate seconds |
| `OnCredentialError`      | none                                                                   | Receives token fetch errors while live connections keep the last valid token                             |

#### Connect with Token Auth

For an instance whose authorization mode is Token Auth, use `WithTokenAuth` instead of the IAM options. Memorystore issues the auth tokens for the instance's token auth users; keep the token in Secret Manager and point `SecretVersion` at it:

```go
client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{
	InitAddress: []string{"10.0.0.15:6379"}, // discovery endpoint
}, valkeygcp.WithTokenAuth(valkeygcp.TokenAuthOptions{
	SecretVersion:     "projects/my-project/secrets/valkey-token/versions/latest",
	OnCredentialError: func(err error) { log.Printf("token re-read failed, keeping the last token: %v", err) },
}))
```

| `TokenAuthOptions` field | Default                                        | Meaning                                                                                                                                                                |
| :----------------------- | :--------------------------------------------- | :--------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `SecretVersion`          | none                                           | Secret Manager secret version that holds the token, global or regional (`projects/P/locations/L/...`), read with ADC. Use `versions/latest` so a rotation is picked up |
| `Token`                  | none                                           | A fixed token, for local development; it can't follow a rotation                                                                                                       |
| `Source`                 | none                                           | Your own reader, for example for another Secret Manager endpoint or other credentials; takes precedence over `SecretVersion` and `Token`                               |
| `Username`               | `ClientOption.Username`, else the default user | The token auth user (`AUTH <user> <token>`); empty sends `AUTH default <token>`                                                                                        |
| `ReloadInterval`         | 5m (`DefaultTokenReloadInterval`)              | How often live connections re-AUTH with the current token; the token is read at most once per half interval. At least 1s; negative turns reloading off                 |
| `OnCredentialError`      | none                                           | Receives token read errors while live connections keep the last token that worked                                                                                      |

Set exactly one source. `WithTokenAuth` can't be combined with `WithIAMOptions` or `WithDefaultIAM`.

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

This skips the Memorystore defaults; `valkeygcp.ResolveClientOption` returns a `valkey.ClientOption` with both the defaults and the credentials. `provider.Token(ctx)` returns the current access token, for example to check that ADC works.

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
client, err := valkeyotel.NewClient(option)
```

### 1.4 Defaults

`NewClient` and `ResolveClientOption` fill these into the `valkey.ClientOption` fields you leave at zero:

| Field              | Default                                                                                                                                                                                                                                                                                    |
| :----------------- | :----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `RetryDelay`     | Exponential backoff with full jitter, up to 2s; stops after 20 retries                                                                                                                                                                                                                     |
| `ClusterOption`  | Only when left entirely at zero: `ShardsRefreshInterval` 10s, `PreferInitAddressRefresh` and `PreferClusterShards` on, so the topology is refreshed through the discovery endpoint with `CLUSTER SHARDS`. Set any `ClusterOption` field and the whole `ClusterOption` is yours |
| `SendToReplicas` | Read-only commands that don't block go to the replicas; set only with `Standalone.ReplicaAddress`                                                                                                                                                                                        |

The cluster settings are left out when `Standalone.ReplicaAddress` is set. valkey-go ignores them when it ends up with a single-node client.

Each of these follows Google's [Valkey client best practices](https://docs.cloud.google.com/memorystore/docs/valkey/general-best-practices#valkey_client_best_practices): reconnect with exponential backoff and jitter, discover nodes through the discovery endpoint with `CLUSTER SHARDS`, refresh the topology periodically, and read from the reader endpoint. Timeouts are not changed: `Dialer.Timeout`, `Dialer.KeepAlive` and `ConnWriteTimeout` keep valkey-go's defaults (5s, 1s and 10s).

### 1.5 Token Refresh and Failures

* **When tokens are refreshed.** A token is replaced `RefreshBuffer` (3m45s) before it expires. Live connections re-AUTH with the new token at a random time in the first half of that window, so they don't all re-AUTH in the same second. `DisableProactiveReauth` turns this off; new connections still get a fresh token.
* **Why 3m45s.** It is the margin Google's own auth library uses ([`defaultExpiryDelta` in `cloud.google.com/go/auth`](https://github.com/googleapis/google-cloud-go/blob/auth/v0.24.0/auth/auth.go#L47-L49)): the metadata server can keep handing out the old token until 4 minutes before it expires, so asking earlier could get the same token again. valkeygcp passes the same margin to Application Default Credentials (`EarlyTokenRefresh`), so their own token cache renews at the same point. The Go OAuth2 library applies it only to metadata-server credentials.
* **Custom token sources.** valkeygcp asks for a new token once the current one has less than `RefreshBuffer` left. A `CustomTokenSource` that caches tokens must hand out a new one by then; otherwise live connections re-AUTH with the old token.
* **When a refresh fails.** Live connections keep the last valid token, and the refresh is retried about every 30 seconds. `OnCredentialError` receives each error. If the last token expires before a refresh succeeds, live connections stay authenticated (their re-AUTH is skipped), but new connections fail with `NOAUTH` until a token can be fetched again. Those `NOAUTH` errors go to the commands that opened the connections, not to `OnCredentialError`. With `DisableProactiveReauth`, an expired token isn't handed out at all: new connections fail with the token fetch error itself (`valkeygcp: token acquisition failed: …`), not `NOAUTH`, and `OnCredentialError` isn't called. The same happens before the first fetch has succeeded.
* **Token fetches are capped.** Each fetch waits at most `FetchTimeout` (10s); a fetch that times out counts as a failed one. While a timed-out fetch is still hanging, the next fetches fail at once instead of waiting another `FetchTimeout`, and a new fetch is tried after 3×`FetchTimeout`.
* **A rejected re-AUTH doesn't close the connection.** If Memorystore rejects the re-AUTH of a live connection (for example with `ERR_IAM_EXHAUSTED`), valkey-go ignores the reply and the connection keeps the login it already has. New connections still need a token that Memorystore accepts.

#### Token Auth

* **How a rotated token reaches clients.** Each live connection re-AUTHs with the current token every `ReloadInterval` (5m, minus up to 10% so connections spread out). The token is read from its source at most once per half interval, however many connections ask, so a new secret version reaches every live connection within about 1.5 × `ReloadInterval`.
* **Rotating safely.** This applies when the token is re-read: `SecretVersion` or `Source`, with reloading on. A token auth user can have at most two auth tokens, so delete the old token at the end of each rotation. Create the new token, add it as a new version of the secret, and check that a newly started client logs in with it. Then wait at least 2 × `ReloadInterval` (10 minutes with the default) so live connections re-AUTH with it, and only then delete the old token. New connections can't log in with a deleted token; a live connection whose re-AUTH is rejected keeps the login it already has, because valkey-go ignores the reply. With a fixed `Token`, or with reloading off, restart the clients with the new token before deleting the old one.
* **When a read fails.** Connections keep the last token that worked, and `OnCredentialError` receives each error. Until a read works again, live connections re-AUTH within 30 seconds instead of every `ReloadInterval`, so the read is retried within 30 seconds and the token it gets reaches them soon after. A failed read never closes a connection.
* **Token reads are capped.** Reads while connecting or re-authenticating stop after 5 seconds, even when a `Source` ignores its `ctx`; a busy or unreachable Secret Manager is retried within that time. The first read stops when `NewClient`'s `ctx` ends, and after 15 seconds at most.
* **How the secret is read.** `SecretVersion` is read over HTTPS from Secret Manager's REST API, at `secretmanager.googleapis.com`, or `secretmanager.LOCATION.rep.googleapis.com` for a regional secret, with Application Default Credentials. valkeygcp doesn't use the Secret Manager client library, so it adds no gRPC dependencies to your program. For another endpoint or other credentials, use `Source`.

### 1.6 Contexts and Setup Failures

* **`NewClient`'s `ctx` bounds setup only.** Credential discovery and the first Token Auth read stop when it is cancelled or its deadline passes, and after 15 seconds if it has no deadline; a `ctx` that has ended by then stops `NewClient` before it connects. Token refreshes never use it, because they run for the life of the client.
* **Setup needs working credentials.** valkey-go connects and authenticates while it builds the client, so missing ADC fails `NewClient` immediately, with the underlying error. Without ADC, pass a token source in `IAMAuthOptions.CustomTokenSource`. With Token Auth, `NewClient` reads the token once before connecting, so a missing secret or permission fails it right away.
* **`InitAddress` is required.** `NewClient` and `ResolveClientOption` return an error without it.
* **Credentials on the `ClientOption`** (`Password` or `AuthCredentialsFn`) are used as is; passing them together with `WithIAMOptions`, `WithDefaultIAM` or `WithTokenAuth` is an error. With one of those options, `ClientOption.Username` is the username it sends unless `IAMAuthOptions.Username` or `TokenAuthOptions.Username` sets one; if both are set, they must match.

### 1.7 Required Roles

| Mechanism  | Role                                                                                  |
| :--------- | :------------------------------------------------------------------------------------ |
| IAM Auth   | `roles/memorystore.dbConnectionUser` (`memorystore.instances.connect`)                |
| Token Auth | `roles/secretmanager.secretAccessor` on the token's secret, with `SecretVersion` only |

Grant it to the identity behind the access tokens: the ADC identity, or the identity of `IAMAuthOptions.CustomTokenSource` when you set one. With Token Auth the token itself is the credential, so connecting needs no Memorystore role; the Secret Manager role goes to the ADC identity.

### 1.8 Testing

Run the unit tests with Go's race detector:

```bash
go test -v -race .
```

The unit tests need no cloud access. `TestRefreshTolerance_LiveConnection` also runs against a local Valkey on `127.0.0.1:6379` (the repository's `docker compose` setup) and is skipped when none is running. `TestTokenAuth_LiveRotation` rotates a token on that local Valkey with an ACL user and a key named for each run, which it deletes afterwards, and checks with `ACL LOG` that no AUTH failed; set `VALKEYGCP_TEST_ADDR` to use another address.

Programs for manual checks live in `examples/`, a separate Go module, so they aren't part of the `valkeygcp` module. Run them from the `examples` directory, for example `go run ./accelerated_test`:

| Program              | What it does                                                               | Needs                                                                            |
| :------------------- | :------------------------------------------------------------------------- | :------------------------------------------------------------------------------- |
| `test_adc`         | Fetches one IAM access token through ADC and prints its type and expiry    | ADC                                                                              |
| `accelerated_test` | Simulates token rotation with tokens that expire after 10 seconds          | Nothing                                                                          |
| `live_e2e`         | Sends SET/GET traffic to an instance until stopped, across token rotations | ADC, network access to the instance, `MEMORYSTORE_TEST_DISCOVERY_ENDPOINT` set |
| `token_e2e`        | The same with Token Auth, logging each token change and re-read error      | Network access to the instance, `MEMORYSTORE_TEST_DISCOVERY_ENDPOINT` and a token source set (see its package comment) |
