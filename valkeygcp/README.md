# valkeygcp: Google Cloud Memorystore Integration for Valkey-Go

[![Go Reference](https://pkg.go.dev/badge/github.com/valkey-io/valkey-go/valkeygcp.svg)](https://pkg.go.dev/github.com/valkey-io/valkey-go/valkeygcp)

`valkeygcp` is the official Google Cloud sub-module for [`valkey-go`](https://github.com/valkey-io/valkey-go), integrating it with **Google Cloud Memorystore for Valkey**.

---

## 1. Native Google Cloud IAM Authentication

`valkeygcp` connects valkey-go to Memorystore for Valkey with IAM authentication instead of a password. `valkeygcp.NewClient` takes the same `valkey.ClientOption` as `valkey.NewClient`, as `valkeyotel.NewClient` does, fills in Memorystore-friendly defaults, and adds IAM credentials: `WithDefaultIAM()` uses Application Default Credentials (ADC), and `WithIAMOptions` lets you choose the token source, username and refresh settings. For instances with Token Auth, `WithTokenAuth` adds the auth token instead.

### 1.1 Features

* **The valkey-go way:** pass any `valkey.ClientOption`, with as many addresses as you need in `InitAddress` and `Standalone.ReplicaAddress`.
* **IAM authentication:** OAuth2 access tokens from ADC (GKE Workload Identity, the metadata server, or a service account key in `GOOGLE_APPLICATION_CREDENTIALS`), or from your own `oauth2.TokenSource`.
* **Token Auth:** for instances with Token Auth, the auth token comes from Secret Manager (recommended), a fixed value or your own source. Secret Manager and your own source are re-read regularly, so a rotated token reaches live connections.
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
| `ReloadInterval`         | 5m (`DefaultTokenReloadInterval`)              | How often live connections re-AUTH with the current token; the token is read at most once per half interval. At least 1s; negative, or zero with `Token`, turns it off |
| `OnCredentialError`      | none                                           | Receives token read errors while live connections keep the last token read successfully                                                                                |

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
| `ClusterOption`  | `ShardsRefreshInterval` 10s, `PreferInitAddressRefresh` on and `PreferClusterShards` on, so every 10s the client reads the cluster topology from the discovery endpoint with `CLUSTER SHARDS`. Used only if you set none of the three. Other `ClusterOption` fields, such as `MaxMovedRedirections`, don't affect this |
| `SendToReplicas` | Read-only commands that don't block go to the replicas; set only with `Standalone.ReplicaAddress`                                                                                                                                                                                        |

The cluster settings are left out when `Standalone.ReplicaAddress` is set. valkey-go ignores them when it ends up with a single-node client.

If you set any of the three yourself, valkeygcp sets none of them, and the ones you leave out stay off. This is because valkeygcp can't tell a `false` you set from one you left out. For example, with only `ClusterOption{PreferClusterShards: true}`, the client doesn't refresh the topology every 10s and doesn't read it from the discovery endpoint. To change one of the three, set all three:

```go
option.ClusterOption = valkey.ClusterOption{
	ShardsRefreshInterval:    30 * time.Second, // changed from 10s
	PreferInitAddressRefresh: true,
	PreferClusterShards:      true,
}
```

To turn all three off, take the option that `ResolveClientOption` returns, set the three to zero, and build the client with `valkey.NewClient`:

```go
opt, err := valkeygcp.ResolveClientOption(ctx, option)
if err != nil {
	return err
}
opt.ClusterOption.ShardsRefreshInterval = 0
opt.ClusterOption.PreferInitAddressRefresh = false
opt.ClusterOption.PreferClusterShards = false
client, err := valkey.NewClient(opt)
```

Each of these follows Google's [Valkey client best practices](https://docs.cloud.google.com/memorystore/docs/valkey/general-best-practices#valkey_client_best_practices): reconnect with exponential backoff and jitter, discover nodes through the discovery endpoint with `CLUSTER SHARDS`, refresh the topology periodically, and read from the reader endpoint. Timeouts are not changed: `Dialer.Timeout`, `Dialer.KeepAlive` and `ConnWriteTimeout` keep valkey-go's defaults (5s, 1s and 10s).

### 1.5 Token Refresh and Failures

* **When tokens are refreshed.** A token is replaced `RefreshBuffer` (3m45s) before it expires. Live connections re-AUTH with the new token at a random time in the first half of that window, so they don't all re-AUTH in the same second. `DisableProactiveReauth` turns this off; new connections still get a fresh token.
* **Why 3m45s.** It is the margin Google's own auth library uses ([`defaultExpiryDelta` in `cloud.google.com/go/auth`](https://github.com/googleapis/google-cloud-go/blob/auth/v0.24.0/auth/auth.go#L47-L49)): the metadata server can keep handing out the old token until 4 minutes before it expires, so asking earlier could get the same token again. valkeygcp passes the same margin to Application Default Credentials (`EarlyTokenRefresh`), so their own token cache renews at the same point. The Go OAuth2 library applies it only to metadata-server credentials.
* **Custom token sources.** valkeygcp asks for a new token once the current one has less than `RefreshBuffer` left. A `CustomTokenSource` that caches tokens must hand out a new one by then; otherwise live connections re-AUTH with the old token.
* **When a refresh fails.** Live connections keep the last valid token, and the refresh is retried about every 30 seconds. `OnCredentialError` receives each error. If the last token expires before a refresh succeeds, live connections stay authenticated (their re-AUTH is skipped), but new connections fail with `NOAUTH` until a token can be fetched again. Those `NOAUTH` errors go to the commands that opened the connections, not to `OnCredentialError`. With `DisableProactiveReauth`, an expired token isn't handed out at all: new connections fail with the token fetch error itself (`valkeygcp: token acquisition failed: …`), not `NOAUTH`, and `OnCredentialError` isn't called. The same happens before the first fetch has succeeded.
* **Token fetches are capped.** Each fetch waits at most `FetchTimeout` (10s); a fetch that times out counts as a failed one. While a timed-out fetch is still hanging, the next fetches fail at once instead of waiting another `FetchTimeout`, and a new fetch is tried after 3×`FetchTimeout`.
* **A rejected re-AUTH doesn't close the connection.** If Memorystore rejects the re-AUTH of a live connection (for example with `ERR_IAM_EXHAUSTED`), valkey-go ignores the reply and the connection keeps the login it already has. New connections still need a token that Memorystore accepts.

#### Token Auth

* **How a rotated token reaches clients.** Each live connection re-AUTHs with the current token every `ReloadInterval` (5m by default, minus up to 10% so connections spread out). The token is read from its source at most once per half interval, however many connections ask. So with `versions/latest`, a new secret version reaches every live connection within about 1.5 × `ReloadInterval`.
* **Rotating safely.** This applies when a re-read picks up the new token: `Source`, or a `SecretVersion` ending in `versions/latest`, with reloading on. A token auth user can have at most two auth tokens, so delete the old token at the end of each rotation. Create the new token, add it as a new version of the secret, and check that a newly started client logs in with it. Then wait at least 2 × `ReloadInterval` (10 minutes with the default) so live connections re-AUTH with it, and check that no client reported a read error to `OnCredentialError` in that time: a client whose reads fail may still use the old token. Only then delete the old token. New connections can't log in with a deleted token; a live connection whose re-AUTH is rejected keeps the login it already has, because valkey-go ignores the reply. With a fixed `Token`, a `SecretVersion` pinned to a number such as `versions/3`, or reloading off, point the clients at the new token and restart them before deleting the old one. To switch at once, for example after deleting a leaked token, restart the clients so they read the new token immediately.
* **When a read fails.** Connections keep the last token read successfully, and `OnCredentialError` receives each error. Until a read works again, live connections re-AUTH within 30 seconds instead of every `ReloadInterval`, so the read is retried within 30 seconds and the token it gets reaches them soon after. A failed read never closes a connection.
* **Token reads are capped.** Reads while connecting or re-authenticating stop after 5 seconds, even when a `Source` ignores its `ctx`; a busy or unreachable Secret Manager is retried within that time. The first read stops when `NewClient`'s `ctx` ends, and after 15 seconds at most.
* **How the secret is read.** `SecretVersion` is read with the Secret Manager client library ([AccessSecretVersion](https://docs.cloud.google.com/secret-manager/docs/access-secret-version)), at `secretmanager.googleapis.com`, or `secretmanager.LOCATION.rep.googleapis.com` for a regional secret, with Application Default Credentials. The library retries a busy or unreachable API by itself, within the read's deadline; other errors, such as a missing permission, are returned at once. For another endpoint or other credentials, use `Source`.

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
| `live_tls`         | Connects over TLS, optionally with IAM, PINGs every node and runs SET/GET   | Network access to an instance with in-transit encryption, and its server CA file |

---

## 2. In-Transit Encryption (TLS)

Instances with in-transit encryption turned on accept only TLS connections. valkey-go supports TLS through `ClientOption.TLSConfig`: set it to a `tls.Config` that trusts the instance's server CA certificates. `NewClient` and `ResolveClientOption` keep the `TLSConfig` you set, so TLS works the same with IAM authentication, without it, and with `valkey.NewClient`. TLS needs no extra role on the workload identity.

### 2.1 Get the Server CA Certificates

The instance's server CA mode decides where the certificates come from:

```bash
gcloud memorystore instances describe INSTANCE --location=REGION \
  --format="value(transitEncryptionMode,serverCaMode,serverCaPool)"
```

Save them with the command for that mode. Keep every certificate the command returns:

```bash
# GOOGLE_MANAGED_PER_INSTANCE_CA, or empty on older instances
gcloud memorystore instances get-certificate-authority INSTANCE --location=REGION --format=json \
  | jq -r '.. | strings | select(contains("BEGIN CERTIFICATE"))' > server-ca.pem

# GOOGLE_MANAGED_SHARED_CA
gcloud memorystore instances get-shared-regional-certificate-authority --location=REGION --format=json \
  | jq -r '.. | strings | select(contains("BEGIN CERTIFICATE"))' > server-ca.pem

# CUSTOMER_MANAGED_CAS_CA (SERVER_CA_POOL is the instance's serverCaPool)
gcloud privateca pools get-ca-certs SERVER_CA_POOL --output-file=server-ca.pem
```

The Memorystore commands need read access to the instance, for example `roles/memorystore.viewer`. The CA Service command needs `privateca.caPools.get` on the pool (`roles/privateca.poolReader`).

### 2.2 Connect with TLS

Build a `tls.Config` from the saved file:

```go
pem, err := os.ReadFile("/etc/memorystore/server-ca.pem")
if err != nil {
	return err
}
roots := x509.NewCertPool()
if !roots.AppendCertsFromPEM(pem) {
	return errors.New("server-ca.pem holds no certificate")
}
tlsConfig := &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
```

Then set it as `ClientOption.TLSConfig`:

```go
// Cluster mode with IAM authentication:
client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{
	InitAddress: []string{"10.0.0.15:6379"}, // discovery endpoint
	TLSConfig:   tlsConfig,
}, valkeygcp.WithDefaultIAM())

// Cluster mode disabled with a reader endpoint, no authentication:
client, err := valkeygcp.NewClient(ctx, valkey.ClientOption{
	InitAddress: []string{"10.0.0.2:6379"},                                         // primary endpoint
	Standalone:  valkey.StandaloneOption{ReplicaAddress: []string{"10.0.0.3:6379"}}, // reader endpoint
	TLSConfig:   tlsConfig,
})
```

* **Every certificate in the file is trusted.** A file saved while the instance has more than one CA, for example during a server CA rotation, verifies nodes signed by any of them. `AppendCertsFromPEM` skips blocks it can't parse and only reports whether it added at least one certificate, so check that the file holds every certificate the command returned, for example with `grep -c 'BEGIN CERTIFICATE' server-ca.pem`.
* **The file is read once**, when you build the `tls.Config`. After replacing it, create a new client. Per-instance CAs are valid for ten years and the next one can be downloaded about five years before the current one expires, so save the file again from time to time.
* **Nodes are checked by IP address.** Leave `ServerName` empty: valkey-go then checks each node's certificate against the IP address it dials, including the node addresses it finds in cluster mode. Set `ServerName` only to check against another name.
* **Keep verification on.** A certificate from another CA fails the handshake with `x509: certificate signed by unknown authority`. Don't set `InsecureSkipVerify`.

### 2.3 Live Check

`live_tls` (in `examples/`) builds its `TLSConfig` this way, connects to a real instance the way `NewClient` does, PINGs every node (each one a separate TLS handshake) and runs SET and GET. Run it from the `examples` directory, on a VM in the instance's VPC network:

```bash
go run ./live_tls -primary=10.0.0.2:6379 -reader=10.0.0.3:6379 -ca-file=server-ca.pem
go run ./live_tls -discovery=10.0.0.15:6379 -ca-file=server-ca.pem -iam
```

The last line is `RESULT {"ok":true,...}` when every node answered over TLS. `-reader` is optional with `-primary`. `-duration=70m` keeps SET/GET traffic going and logs `STATS` every minute. With `-iam`, every login is logged as an `AUTH` line with a short hash of the token, never the token itself, so a long run shows when the token is renewed. `-conn-lifetime=5m` closes and reopens every connection every five minutes, so new logins and TLS handshakes keep happening during the run.
