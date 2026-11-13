// Command token_e2e sends SET/GET traffic to a Memorystore for Valkey instance that uses
// Token Auth, to check that requests keep succeeding while the token is rotated. It logs
// every token change (as a short fingerprint, never the token itself), every token re-read
// error and every failed request.
//
// Set the endpoint and one token source, then run it from the valkeygcp/examples directory
// and stop it with Ctrl+C:
//
//	MEMORYSTORE_TEST_DISCOVERY_ENDPOINT=10.0.0.15:6379 \
//	MEMORYSTORE_TEST_TOKEN_SECRET=projects/P/secrets/S/versions/latest go run ./token_e2e
//
// Token sources (set one):
//   - MEMORYSTORE_TEST_TOKEN_SECRET: a Secret Manager secret version, read with Application
//     Default Credentials, whose identity needs roles/secretmanager.secretAccessor on it.
//   - MEMORYSTORE_TEST_TOKEN_FILE: a file holding the token, re-read like a secret. Handy for
//     trying a rotation against a local Valkey.
//   - MEMORYSTORE_TEST_TOKEN: a fixed token.
//
// Optional: MEMORYSTORE_TEST_TOKEN_USERNAME, the token auth user (empty means the default
// user), and MEMORYSTORE_TEST_TOKEN_RELOAD, the reload interval (for example 1m; default 5m).
//
// It runs one SET/GET pair every 200ms, each limited to 5 seconds, and logs the counts after
// every 300 successful pairs, about once a minute. When stopped, it prints the totals and
// exits with status 1 if any pair failed.
package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeygcp"
)

// main reads the settings from the environment, connects with Token Auth and sends traffic
// until Ctrl+C.
func main() {
	endpoint := os.Getenv("MEMORYSTORE_TEST_DISCOVERY_ENDPOINT")
	if endpoint == "" {
		log.Fatal("MEMORYSTORE_TEST_DISCOVERY_ENDPOINT environment variable must be set (e.g. 10.0.0.15:6379)")
	}
	tokenOpt := valkeygcp.TokenAuthOptions{
		Username:      os.Getenv("MEMORYSTORE_TEST_TOKEN_USERNAME"),
		SecretVersion: os.Getenv("MEMORYSTORE_TEST_TOKEN_SECRET"),
		Token:         os.Getenv("MEMORYSTORE_TEST_TOKEN"),
		OnCredentialError: func(err error) {
			log.Printf("⚠️ token re-read failed, keeping the last token: %v", err)
		},
	}
	if file := os.Getenv("MEMORYSTORE_TEST_TOKEN_FILE"); file != "" {
		tokenOpt.Source = func(context.Context) (string, error) {
			b, err := os.ReadFile(file)
			return string(b), err
		}
	}
	if s := os.Getenv("MEMORYSTORE_TEST_TOKEN_RELOAD"); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			log.Fatalf("MEMORYSTORE_TEST_TOKEN_RELOAD=%q: %v", s, err)
		}
		tokenOpt.ReloadInterval = d
	}

	ctx := context.Background()
	log.Printf("Connecting to Memorystore at %s with Token Auth...", endpoint)
	option, err := valkeygcp.ResolveClientOption(ctx, valkey.ClientOption{
		InitAddress: []string{endpoint}, // discovery endpoint, or the primary endpoint
	}, valkeygcp.WithTokenAuth(tokenOpt))
	if err != nil {
		log.Fatalf("Token Auth setup failed: %v", err)
	}

	// Count the AUTHs and log each token change, without printing the token.
	var auths atomic.Int64
	var inUse atomic.Value // fingerprint of the last token handed out
	authFn := option.AuthCredentialsFn
	option.AuthCredentialsFn = func(c valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
		creds, err := authFn(c)
		if err != nil {
			return creds, err
		}
		auths.Add(1)
		fp := fingerprint(creds.Password)
		switch old, _ := inUse.Swap(fp).(string); {
		case old == "":
			log.Printf("🔑 token %s in use, user %q", fp, userOrDefault(creds.Username))
		case old != fp:
			log.Printf("🔄 token changed: %s -> %s", old, fp)
		}
		return creds, nil
	}

	client, err := valkey.NewClient(option)
	if err != nil {
		log.Fatalf("Failed to create Memorystore client: %v", err)
	}
	defer client.Close()

	if err := client.Do(ctx, client.B().Ping().Build()).Error(); err != nil {
		log.Fatalf("Initial PING failed! Authentication rejected: %v", err)
	}
	log.Println("✅ Initial RESP3 HELLO 3 AUTH handshake succeeded!")

	var successCount, errorCount uint64
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)
	log.Println("Running continuous traffic test across token reloads (press Ctrl+C to stop)...")

	startTime := time.Now()
	for {
		select {
		case <-ticker.C:
			if err := setGet(ctx, client); err != nil {
				errorCount++
				log.Printf("❌ %v", err)
				continue
			}
			successCount++
			if successCount%300 == 0 {
				log.Printf("[Elapsed: %v] Operations: %d succeeded, %d failed; AUTHs sent: %d",
					time.Since(startTime).Round(time.Second), successCount, errorCount, auths.Load())
			}
		case <-stopChan:
			log.Printf("Stopping test: %d succeeded, %d failed, %d AUTHs in %v",
				successCount, errorCount, auths.Load(), time.Since(startTime).Round(time.Second))
			if errorCount > 0 {
				client.Close()
				os.Exit(1) // any failed pair fails the run
			}
			return
		}
	}
}

// pairTimeout bounds one SET/GET pair, so a network problem, and valkey-go's retries of it,
// can't keep the loop from reporting the failure or noticing Ctrl+C.
const pairTimeout = 5 * time.Second

// setGet runs one SET/GET pair and checks that GET returns the value SET wrote.
func setGet(ctx context.Context, client valkey.Client) error {
	ctx, cancel := context.WithTimeout(ctx, pairTimeout)
	defer cancel()
	key := fmt.Sprintf("token-e2e-key-%d", time.Now().UnixNano()%100)
	val := fmt.Sprintf("val-%d", time.Now().UnixNano())
	if err := client.Do(ctx, client.B().Set().Key(key).Value(val).Ex(60*time.Second).Build()).Error(); err != nil {
		return fmt.Errorf("SET failed: %w", err)
	}
	got, err := client.Do(ctx, client.B().Get().Key(key).Build()).ToString()
	if err != nil {
		return fmt.Errorf("GET failed: %w", err)
	}
	if got != val {
		return fmt.Errorf("GET returned %q, want %q", got, val)
	}
	return nil
}

// fingerprint identifies a token in the logs without revealing it.
func fingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return hex.EncodeToString(sum[:4])
}

// userOrDefault names the user a login goes in as: valkey-go sends "default" when the
// username is empty.
func userOrDefault(user string) string {
	if user == "" {
		return "default"
	}
	return user
}
