// Command live_tls checks valkeygcp.WithTLS against a real Memorystore for Valkey instance
// with in-transit encryption. It builds the client the way valkeygcp.NewClient does
// (ResolveClientOption, then valkey.NewClient), PINGs every node (each node is a separate
// connection and TLS handshake, checked against the server CA), runs SET and GET, and logs a
// final machine-readable RESULT line. With -iam it also logs every login as an AUTH line,
// with a short hash of the token, never the token.
//
// Run it on a VM in the instance's VPC network, with the server CA certificates saved as a
// PEM file (see the README):
//
//	live_tls -primary=10.0.0.2:6379 -reader=10.0.0.3:6379 -ca-file=server-ca.pem -tls-mode=shared
//	live_tls -discovery=10.0.0.15:6379 -ca-file=server-ca.pem -tls-mode=per-instance -iam
//	live_tls -discovery=10.0.0.15:6379 -ca-file=server-ca.pem -iam -duration=70m -conn-lifetime=5m
package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/valkeygcp"
)

var (
	discovery    = flag.String("discovery", "", "discovery endpoint IP:port (cluster mode)")
	primary      = flag.String("primary", "", "primary endpoint IP:port (cluster mode disabled)")
	reader       = flag.String("reader", "", "reader endpoint IP:port (cluster mode disabled, optional with -primary)")
	caFile       = flag.String("ca-file", "", "server CA certificates (PEM file)")
	tlsMode      = flag.String("tls-mode", "auto", "auto | per-instance | shared | custom | disabled")
	serverName   = flag.String("server-name", "", "check node certificates against this name instead of the dialed IP")
	useIAM       = flag.Bool("iam", false, "authenticate with IAM access tokens from Application Default Credentials")
	duration     = flag.Duration("duration", 0, "keep SET/GET traffic going this long (0: check once)")
	interval     = flag.Duration("interval", 200*time.Millisecond, "pause between SET/GET pairs with -duration")
	timeout      = flag.Duration("setup-timeout", 30*time.Second, "deadline for creating the client")
	connLifetime = flag.Duration("conn-lifetime", 0, "close and reopen every connection after this long, so a long run keeps making new logins and TLS handshakes (0: keep connections open)")
)

func main() {
	flag.Parse()
	log.SetFlags(log.Ldate | log.Ltime | log.Lmicroseconds | log.LUTC)
	start := time.Now()
	res := map[string]any{}
	err := run(res)
	res["ok"] = err == nil
	if err != nil {
		res["error"] = err.Error()
	}
	res["elapsed"] = time.Since(start).Round(time.Millisecond).String()
	b, _ := json.Marshal(res)
	log.Printf("RESULT %s", b)
	if err != nil {
		os.Exit(1)
	}
}

func run(res map[string]any) error {
	mode, err := parseTLSMode(*tlsMode)
	if err != nil {
		return err
	}
	if *caFile != "" {
		b, err := os.ReadFile(*caFile)
		if err != nil {
			return err
		}
		log.Printf("CA_FILE %s certificates=%d", *caFile, strings.Count(string(b), "BEGIN CERTIFICATE"))
	}
	var option valkey.ClientOption
	switch {
	case *discovery != "" && *primary == "" && *reader == "":
		res["topology"] = "cluster"
		option.InitAddress = []string{*discovery}
	case *primary != "" && *discovery == "":
		res["topology"] = "cluster-disabled"
		option.InitAddress = []string{*primary}
		if *reader != "" {
			option.Standalone.ReplicaAddress = []string{*reader}
		}
	default:
		return errors.New("set -discovery (cluster mode) or -primary, with an optional -reader (cluster mode disabled)")
	}
	option.ConnLifetime = *connLifetime
	opts := []valkeygcp.Option{valkeygcp.WithTLS(valkeygcp.TLSOptions{Mode: mode, CAFile: *caFile, ServerName: *serverName})}
	if *useIAM {
		opts = append(opts, valkeygcp.WithIAMOptions(valkeygcp.IAMAuthOptions{
			OnCredentialError: func(err error) {
				log.Printf("CREDENTIAL_ERROR absorbed (last valid token kept): %v", err)
			},
		}))
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()
	opt, err := valkeygcp.ResolveClientOption(ctx, option, opts...)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	logAuth(&opt)
	client, err := valkey.NewClient(opt)
	if err != nil {
		return fmt.Errorf("setup: %w", err)
	}
	defer client.Close()
	log.Printf("CONNECTED mode=%s nodes=%d tls=%t tlsMode=%s", client.Mode(), len(client.Nodes()), mode != valkeygcp.TLSDisabled, mode)

	nodes, failed := pingNodes(ctx, client)
	res["nodes"], res["failedNodes"] = nodes, failed
	if failed > 0 {
		return fmt.Errorf("%d of %d nodes failed PING", failed, len(nodes))
	}
	if *duration <= 0 {
		key := fmt.Sprintf("live_tls:once:%d", time.Now().UnixNano())
		if err := setGet(ctx, client, key, "ok"); err != nil {
			return err
		}
		log.Printf("SET/GET %s ok", key)
		return nil
	}
	return soak(client, res)
}

func parseTLSMode(s string) (valkeygcp.TLSMode, error) {
	switch s {
	case "auto":
		return valkeygcp.TLSAuto, nil
	case "per-instance":
		return valkeygcp.TLSPerInstanceCA, nil
	case "shared":
		return valkeygcp.TLSSharedCA, nil
	case "custom":
		return valkeygcp.TLSCustomCA, nil
	case "disabled":
		return valkeygcp.TLSDisabled, nil
	}
	return valkeygcp.TLSAuto, fmt.Errorf("-tls-mode=%q: want auto, per-instance, shared, custom or disabled", s)
}

// logAuth wraps the IAM credentials function set by WithIAMOptions, if any, to log every
// call: one when a connection opens and one each time an open connection logs in again with
// a renewed token. Only the first 4 bytes of the token's SHA-256 are logged, enough to see
// when the token changes. renew_at is when that connection will log in again, normally 2.5
// to 5 minutes before the token expires.
func logAuth(o *valkey.ClientOption) {
	next := o.AuthCredentialsFn
	if next == nil {
		return
	}
	var n atomic.Int64
	o.AuthCredentialsFn = func(c valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
		cred, err := next(c)
		addr := "unknown"
		if c.Address != nil {
			addr = c.Address.String()
		}
		renewAt := "never"
		if !cred.RefreshAfter.IsZero() {
			renewAt = cred.RefreshAfter.UTC().Format(time.TimeOnly)
		}
		switch {
		case err != nil:
			log.Printf("AUTH_FAIL n=%d addr=%s err=%v", n.Add(1), addr, err)
		case cred.Password == "":
			log.Printf("AUTH n=%d addr=%s token=none renew_at=%s", n.Add(1), addr, renewAt)
		default:
			sum := sha256.Sum256([]byte(cred.Password))
			log.Printf("AUTH n=%d addr=%s user=%s token=sha256:%x renew_at=%s", n.Add(1), addr, cred.Username, sum[:4], renewAt)
		}
		return cred, err
	}
}

// pingNodes PINGs every node through its own connection, so each one shows a separate TLS
// handshake and certificate check.
func pingNodes(ctx context.Context, client valkey.Client) ([]string, int) {
	nodes := client.Nodes()
	addrs := make([]string, 0, len(nodes))
	for a := range nodes {
		addrs = append(addrs, a)
	}
	sort.Strings(addrs)
	failed := 0
	for _, a := range addrs {
		n := nodes[a]
		if err := n.Do(ctx, n.B().Ping().Build()).Error(); err != nil {
			failed++
			log.Printf("NODE_FAIL addr=%s err=%v", a, err)
			continue
		}
		log.Printf("NODE_OK addr=%s", a)
	}
	return addrs, failed
}

// setGet writes key and reads it back. With a reader endpoint the GET goes to a replica,
// which can lag, so a missing or older value is read again for up to 2s.
func setGet(ctx context.Context, c valkey.Client, key, val string) error {
	if err := c.Do(ctx, c.B().Set().Key(key).Value(val).Ex(2*time.Minute).Build()).Error(); err != nil {
		return fmt.Errorf("SET %s: %w", key, err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := c.Do(ctx, c.B().Get().Key(key).Build()).ToString()
		switch {
		case err == nil && got == val:
			return nil
		case err != nil && !valkey.IsValkeyNil(err):
			return fmt.Errorf("GET %s: %w", key, err)
		case time.Now().After(deadline):
			return fmt.Errorf("GET %s: got %q, want %q", key, got, val)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// soak keeps SET/GET traffic going for -duration (or until Ctrl+C) and logs STATS every minute.
func soak(client valkey.Client, res map[string]any) error {
	sigCtx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	runCtx, cancel := context.WithTimeout(sigCtx, *duration)
	defer cancel()
	host, _ := os.Hostname()
	tick := time.NewTicker(*interval)
	defer tick.Stop()
	report := time.NewTicker(time.Minute)
	defer report.Stop()

	var n, ok, fail int64
	for {
		select {
		case <-runCtx.Done():
			log.Printf("STATS ops_ok=%d ops_failed=%d", ok, fail)
			res["opsOK"], res["opsFailed"] = ok, fail
			if fail > 0 || ok == 0 {
				return fmt.Errorf("%d of %d SET/GET pairs failed", fail, ok+fail)
			}
			return nil
		case <-report.C:
			log.Printf("STATS ops_ok=%d ops_failed=%d", ok, fail)
		case <-tick.C:
			n++
			opCtx, cancelOp := context.WithTimeout(runCtx, 5*time.Second)
			err := setGet(opCtx, client, fmt.Sprintf("live_tls:soak:%s:%d", host, n%100), strconv.FormatInt(n, 10))
			cancelOp()
			switch {
			case err == nil:
				ok++
			case runCtx.Err() != nil:
				// Stopping: an operation cut short by the end of the run isn't a failure.
			default:
				fail++
				log.Printf("OP_FAIL n=%d err=%v", n, err)
			}
		}
	}
}
