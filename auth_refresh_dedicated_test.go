package valkey_test

// A dedicated RESP3 subscription against a server that leaves the credential refresh AUTH
// unanswered. The server here is a few lines of in-memory RESP3 over net.Pipe: it answers
// everything except an AUTH that arrives on a connection holding a subscription, which it
// drops without a reply while it keeps serving that connection.
//
// Ignoring the reply of a refresh AUTH (the v1.0.80 behavior) is only safe when the server
// replies, because a rejection is still a reply on the wire. If the server stays silent, the
// reply queue shifts by one and every later command is answered with the reply to the command
// after it. ClientOption.DisableAuthRefreshOnDedicated keeps the refresh off dedicated
// connections so their replies keep matching the commands they belong to.

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	valkey "github.com/valkey-io/valkey-go"
)

// silentAuthServer counts the AUTH commands it receives by whether the connection that sent
// them held a subscription at the time.
type silentAuthServer struct {
	authSubscribed, authPlain atomic.Int64
}

func (s *silentAuthServer) serve(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)
	subscriptions := 0
	for {
		args, err := readRESPCommand(r)
		if err != nil {
			return
		}
		var reply string
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			reply = "%3\r\n+server\r\n+valkey\r\n+version\r\n+9.1.0\r\n+proto\r\n:3\r\n"
		case "CLIENT":
			reply = "+OK\r\n"
		case "SUBSCRIBE":
			subscriptions++
			reply = fmt.Sprintf(">3\r\n+subscribe\r\n$%d\r\n%s\r\n:%d\r\n", len(args[1]), args[1], subscriptions)
		case "AUTH":
			if subscriptions > 0 {
				s.authSubscribed.Add(1)
				continue // no reply, and the connection stays usable
			}
			s.authPlain.Add(1)
			reply = "+OK\r\n"
		case "PING":
			reply = "+PONG\r\n"
		case "GET":
			v := "value-of-" + args[1]
			reply = fmt.Sprintf("$%d\r\n%s\r\n", len(v), v)
		default:
			reply = "-ERR unknown command\r\n"
		}
		if _, err := io.WriteString(conn, reply); err != nil {
			return
		}
	}
}

// readRESPCommand reads one RESP array of bulk strings.
func readRESPCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil {
		return nil, err
	}
	args := make([]string, n)
	for i := range args {
		if line, err = r.ReadString('\n'); err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "$")))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args[i] = string(buf[:size])
	}
	return args, nil
}

const silentAuthFirstRefresh = 200 * time.Millisecond

// newSilentAuthClient connects a client to srv. With refresh set, each of its connections
// refreshes its credentials once, shortly after it opens; without, none ever does.
func newSilentAuthClient(t *testing.T, srv *silentAuthServer, refresh, disableOnDedicated bool) valkey.Client {
	t.Helper()
	var calls atomic.Int64
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress:       []string{"in-memory:6379"},
		ForceSingleClient: true,
		DisableCache:      true,
		// No background PING, so that the only commands on the wire are the test's.
		Dialer:                        net.Dialer{KeepAlive: -1},
		DisableAuthRefreshOnDedicated: disableOnDedicated,
		DialCtxFn: func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
			c, s := net.Pipe()
			go srv.serve(s)
			return c, nil
		},
		AuthCredentialsFn: func(valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
			creds := valkey.AuthCredentials{Username: "u", Password: "p"}
			if refresh {
				creds.RefreshAfter = time.Now().Add(time.Hour)
				if calls.Add(1) <= 2 {
					creds.RefreshAfter = time.Now().Add(silentAuthFirstRefresh)
				}
			}
			return creds, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	return client
}

// subscribeDedicated returns a dedicated connection of client subscribed to channel "a".
func subscribeDedicated(t *testing.T, client valkey.Client) valkey.DedicatedClient {
	t.Helper()
	ded, release := client.Dedicate()
	t.Cleanup(release)
	ded.SetPubSubHooks(valkey.PubSubHooks{OnMessage: func(valkey.PubSubMessage) {}})
	if err := ded.Do(t.Context(), ded.B().Subscribe().Channel("a").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	return ded
}

// setupSilentAuth returns a dedicated connection that is subscribed to channel "a" and has
// been through its first credential refresh time, with the server it talks to.
func setupSilentAuth(t *testing.T, disableOnDedicated bool) (*silentAuthServer, valkey.DedicatedClient) {
	t.Helper()
	srv := &silentAuthServer{}
	client := newSilentAuthClient(t, srv, true, disableOnDedicated)
	if err := client.Do(t.Context(), client.B().Ping().Build()).Error(); err != nil { // opens the shared connection
		t.Fatal(err)
	}
	ded := subscribeDedicated(t, client)
	time.Sleep(3 * silentAuthFirstRefresh) // both connections are past their refresh time
	return srv, ded
}

// getSilentAuth runs GET key with a deadline and reports the value, or the error.
func getSilentAuth(t *testing.T, ded valkey.DedicatedClient, key string) (string, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	return ded.Do(ctx, ded.B().Get().Key(key).Build()).ToString()
}

// A dedicated subscribed connection of a client that opted out must not send the refresh AUTH,
// the shared connection of the same client must still renew, and a reply must reach the command
// that issued it.
func TestDedicatedAuthRefreshSilentAUTHDisabled(t *testing.T) {
	srv, ded := setupSilentAuth(t, true)
	if n := srv.authSubscribed.Load(); n != 0 {
		t.Errorf("%d refresh AUTH sent on the subscribed connection", n)
	}
	if n := srv.authPlain.Load(); n == 0 {
		t.Errorf("the connection without a subscription did not refresh")
	}
	if v, err := getSilentAuth(t, ded, "k1"); err != nil || v != "value-of-k1" {
		t.Errorf("GET k1 = %q, %v", v, err)
	}
	// A SUBSCRIBE after the first refresh must not trip the out-of-order panic.
	if err := ded.Do(t.Context(), ded.B().Subscribe().Channel("b").Build()).Error(); err != nil {
		t.Errorf("SUBSCRIBE b = %v", err)
	}
}

// The default keeps the v1.0.80 behavior of refreshing credentials on dedicated connections:
// a server that does answer the AUTH is the case this option must not remove.
func TestDedicatedAuthRefreshSilentAUTHDefault(t *testing.T) {
	srv, _ := setupSilentAuth(t, false)
	for deadline := time.Now().Add(2 * time.Second); srv.authSubscribed.Load() == 0; time.Sleep(10 * time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the refresh AUTH was not sent on the subscribed connection")
		}
	}
}
