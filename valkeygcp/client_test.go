package valkeygcp

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/internal/cmds"
	"golang.org/x/oauth2"
)

// NewClient fits the ClientBuilder of valkeylock and valkeyaside. A compile-time check: the
// builder is never called.
var _ func(valkey.ClientOption) (valkey.Client, error) = func(o valkey.ClientOption) (valkey.Client, error) {
	return NewClient(context.Background(), o, WithDefaultIAM())
}

// staticTokenIAM returns IAM options whose tokens come from a fixed token source.
func staticTokenIAM(token string) IAMAuthOptions {
	return IAMAuthOptions{CustomTokenSource: &mockTokenSource{defaultTok: &oauth2.Token{
		AccessToken: token,
		TokenType:   "Bearer",
		Expiry:      time.Now().Add(1 * time.Hour),
	}}}
}

// With a discovery endpoint alone, the cluster defaults and the connection defaults are
// filled in, no reader routing is set, and the IAM token becomes the AUTH password.
func TestResolveClientOption_ClusterWiring(t *testing.T) {
	opt, err := ResolveClientOption(context.Background(), valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}},
		WithIAMOptions(staticTokenIAM("cluster-token")))
	if err != nil {
		t.Fatal(err)
	}
	if len(opt.InitAddress) != 1 || opt.InitAddress[0] != "10.0.0.15:6379" || !opt.ClusterOption.PreferInitAddressRefresh ||
		!opt.ClusterOption.PreferClusterShards || opt.ClusterOption.ShardsRefreshInterval != defaultShardsRefreshInterval {
		t.Errorf("cluster options = %+v / %+v", opt.InitAddress, opt.ClusterOption)
	}
	if opt.Dialer.Timeout != defaultDialTimeout || opt.Dialer.KeepAlive != defaultKeepAlive || opt.ConnWriteTimeout != defaultConnWriteTimeout {
		t.Errorf("connection defaults = %v / %v / %v", opt.Dialer.Timeout, opt.Dialer.KeepAlive, opt.ConnWriteTimeout)
	}
	if len(opt.Standalone.ReplicaAddress) != 0 || opt.SendToReplicas != nil {
		t.Errorf("reader routing set without a reader endpoint: %v", opt.Standalone.ReplicaAddress)
	}
	creds, err := opt.AuthCredentialsFn(valkey.AuthCredentialsContext{})
	if err != nil || creds.Username != DefaultIAMUsername || creds.Password != "cluster-token" {
		t.Fatalf("creds = %+v, %v", creds, err)
	}
}

// With a reader endpoint, reads are routed to it and the cluster defaults are left out.
func TestResolveClientOption_StandaloneWiring(t *testing.T) {
	opt, err := ResolveClientOption(context.Background(), valkey.ClientOption{
		InitAddress: []string{"10.0.0.2:6379"},
		Standalone:  valkey.StandaloneOption{ReplicaAddress: []string{"10.0.0.3:6379"}},
	}, WithIAMOptions(staticTokenIAM("standalone-token")))
	if err != nil {
		t.Fatal(err)
	}
	if len(opt.InitAddress) != 1 || opt.InitAddress[0] != "10.0.0.2:6379" ||
		len(opt.Standalone.ReplicaAddress) != 1 || opt.Standalone.ReplicaAddress[0] != "10.0.0.3:6379" || opt.SendToReplicas == nil {
		t.Errorf("standalone options = %+v / %+v", opt.InitAddress, opt.Standalone)
	}
	if opt.ClusterOption != (valkey.ClusterOption{}) {
		t.Errorf("cluster defaults set with a reader endpoint: %+v", opt.ClusterOption)
	}
	creds, err := opt.AuthCredentialsFn(valkey.AuthCredentialsContext{})
	if err != nil || creds.Username != DefaultIAMUsername || creds.Password != "standalone-token" {
		t.Fatalf("creds = %+v, %v", creds, err)
	}
}

// Every InitAddress and Standalone.ReplicaAddress value is kept as given.
func TestResolveClientOption_AddressesKept(t *testing.T) {
	seeds := []string{"10.0.0.15:6379", "10.0.0.16:6379", "10.0.0.17:6379"}
	opt, err := ResolveClientOption(context.Background(), valkey.ClientOption{InitAddress: seeds})
	if err != nil || !slices.Equal(opt.InitAddress, seeds) {
		t.Fatalf("InitAddress = %v, %v; want %v", opt.InitAddress, err, seeds)
	}
	replicas := []string{"10.0.0.3:6379", "10.0.0.4:6379"}
	opt, err = ResolveClientOption(context.Background(), valkey.ClientOption{
		InitAddress: seeds[:1],
		Standalone:  valkey.StandaloneOption{ReplicaAddress: replicas},
	})
	if err != nil || !slices.Equal(opt.Standalone.ReplicaAddress, replicas) {
		t.Fatalf("ReplicaAddress = %v, %v; want %v", opt.Standalone.ReplicaAddress, err, replicas)
	}
}

// A primary endpoint without a reader endpoint (an instance without replicas) needs no
// Standalone.ReplicaAddress, and no reader routing is set up.
func TestResolveClientOption_NoReader(t *testing.T) {
	opt, err := ResolveClientOption(context.Background(), valkey.ClientOption{InitAddress: []string{"10.0.0.2:6379"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(opt.Standalone.ReplicaAddress) != 0 || opt.SendToReplicas != nil {
		t.Fatalf("reader routing = %v / %v, want none", opt.Standalone.ReplicaAddress, opt.SendToReplicas != nil)
	}
}

// Without InitAddress there is nothing to connect to, so ResolveClientOption fails.
func TestResolveClientOption_NoEndpoint(t *testing.T) {
	if _, err := ResolveClientOption(context.Background(), valkey.ClientOption{}, WithDefaultIAM()); !errors.Is(err, errNoEndpoint) {
		t.Fatalf("got %v, want %v", err, errNoEndpoint)
	}
}

// Values set on the ClientOption beat the Memorystore defaults.
func TestResolveClientOption_CallerValuesWin(t *testing.T) {
	retry := func(int, valkey.Completed, error) time.Duration { return time.Second }
	opt, err := ResolveClientOption(context.Background(), valkey.ClientOption{
		InitAddress:      []string{"10.0.0.15:6379"},
		Dialer:           net.Dialer{Timeout: 7 * time.Second, KeepAlive: 9 * time.Second},
		ConnWriteTimeout: 11 * time.Second,
		ClusterOption:    valkey.ClusterOption{ShardsRefreshInterval: time.Minute},
		RetryDelay:       retry,
	})
	if err != nil {
		t.Fatal(err)
	}
	switch {
	case opt.Dialer.Timeout != 7*time.Second || opt.Dialer.KeepAlive != 9*time.Second:
		t.Errorf("Dialer = %v / %v", opt.Dialer.Timeout, opt.Dialer.KeepAlive)
	case opt.ConnWriteTimeout != 11*time.Second:
		t.Errorf("ConnWriteTimeout = %v", opt.ConnWriteTimeout)
	case opt.ClusterOption.ShardsRefreshInterval != time.Minute:
		t.Errorf("ShardsRefreshInterval = %v", opt.ClusterOption.ShardsRefreshInterval)
	case opt.RetryDelay(0, valkey.Completed{}, nil) != time.Second:
		t.Error("RetryDelay replaced")
	}

	everything := func(valkey.Completed) bool { return true }
	opt, err = ResolveClientOption(context.Background(), valkey.ClientOption{
		InitAddress:    []string{"10.0.0.2:6379"},
		Standalone:     valkey.StandaloneOption{ReplicaAddress: []string{"10.0.0.3:6379"}},
		SendToReplicas: everything,
	})
	if err != nil || !opt.SendToReplicas(cmds.NewBuilder(cmds.NoSlot).Set().Key("k").Value("v").Build()) {
		t.Fatalf("SendToReplicas replaced: %v", err)
	}
}

// Credentials set on the ClientOption are used as is; next to an IAM option they are an
// error. That includes Username, which valkey-go would otherwise drop without a word.
func TestResolveClientOption_CallerCredentials(t *testing.T) {
	ownFn := func(valkey.AuthCredentialsContext) (valkey.AuthCredentials, error) {
		return valkey.AuthCredentials{}, nil
	}
	for field, option := range map[string]valkey.ClientOption{
		"Password":          {InitAddress: []string{"10.0.0.15:6379"}, Password: "secret"},
		"AuthCredentialsFn": {InitAddress: []string{"10.0.0.15:6379"}, AuthCredentialsFn: ownFn},
		"Username":          {InitAddress: []string{"10.0.0.15:6379"}, Username: "alice"},
	} {
		_, err := ResolveClientOption(context.Background(), option, WithIAMOptions(staticTokenIAM("t")))
		if err == nil || !strings.Contains(err.Error(), "ClientOption."+field+" conflicts with WithIAMOptions") {
			t.Errorf("%s with WithIAMOptions: got %v", field, err)
		}
		opt, err := ResolveClientOption(context.Background(), option)
		if err != nil || opt.Username != option.Username || opt.Password != option.Password ||
			(opt.AuthCredentialsFn != nil) != (option.AuthCredentialsFn != nil) {
			t.Errorf("%s alone: not kept as is (%v)", field, err)
		}
	}
}

// Later options override earlier ones; a nil option is skipped.
func TestResolveClientOption_LastIAMOptionWins(t *testing.T) {
	opt, err := ResolveClientOption(context.Background(), valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}},
		WithDefaultIAM(), nil, WithIAMOptions(staticTokenIAM("last")))
	if err != nil {
		t.Fatal(err) // WithDefaultIAM would need ADC
	}
	if creds, err := opt.AuthCredentialsFn(valkey.AuthCredentialsContext{}); err != nil || creds.Password != "last" {
		t.Fatalf("creds = %+v, %v", creds, err)
	}
}

// The default routing sends read-only commands to the reader endpoint, and writes and
// blocking commands to the primary.
func TestResolveClientOption_SendToReplicasLogic(t *testing.T) {
	capturedOpt, err := ResolveClientOption(context.Background(), valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:0"},
		Standalone:  valkey.StandaloneOption{ReplicaAddress: []string{"127.0.0.1:0"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if capturedOpt.SendToReplicas == nil {
		t.Fatal("expected SendToReplicas to be configured")
	}

	builder := cmds.NewBuilder(cmds.NoSlot)

	// Read-only command (GET) -> should route to replica
	getCmd := builder.Get().Key("k").Build()
	if !capturedOpt.SendToReplicas(getCmd) {
		t.Errorf("expected GET to route to replica, got false")
	}

	// Write command (SET) -> should NOT route to replica
	setCmd := builder.Set().Key("k").Value("v").Build()
	if capturedOpt.SendToReplicas(setCmd) {
		t.Errorf("expected SET NOT to route to replica, got true")
	}

	// Blocking command (BLPOP) -> should NOT route to replica. BLPOP pops an element, so it
	// isn't read-only either.
	blpopCmd := builder.Blpop().Key("k").Timeout(1).Build()
	if capturedOpt.SendToReplicas(blpopCmd) {
		t.Errorf("expected BLPOP NOT to route to replica, got true")
	}

	// Blocking read (XREAD ... BLOCK) -> should NOT route to replica. It is read-only, so
	// only the blocking check keeps it on the primary.
	xreadCmd := builder.Xread().Block(1000).Streams().Key("s").Id("$").Build()
	if capturedOpt.SendToReplicas(xreadCmd) {
		t.Errorf("expected XREAD BLOCK NOT to route to replica, got true")
	}
}

// Both cluster and standalone options get memorystoreRetryDelay as RetryDelay.
func TestResolveClientOption_MemorystoreRetryDelay(t *testing.T) {
	for name, option := range map[string]valkey.ClientOption{
		"cluster":    {InitAddress: []string{"127.0.0.1:0"}},
		"standalone": {InitAddress: []string{"127.0.0.1:0"}, Standalone: valkey.StandaloneOption{ReplicaAddress: []string{"127.0.0.1:0"}}},
	} {
		opt, err := ResolveClientOption(context.Background(), option)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if opt.RetryDelay == nil || opt.RetryDelay(maxRetryAttempts, valkey.Completed{}, nil) >= 0 {
			t.Errorf("%s: RetryDelay is not memorystoreRetryDelay", name)
		}
	}
}

// A ctx that has ended during setup stops NewClient before it dials
// (valkey.NewClient takes no context).
func TestNewClient_ContextEndedBeforeDial(t *testing.T) {
	var dials atomic.Int32
	dial := func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("dialed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewClient(ctx, valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}, DialCtxFn: dial},
		WithIAMOptions(staticTokenIAM("t")))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled before dialing", err)
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("dialed %d times with an ended ctx", n)
	}
}

// Without an IAM option setup doesn't use ctx, so it is the check after setup that stops
// NewClient from dialing with an ended ctx.
func TestNewClient_ContextEndedAfterSetup(t *testing.T) {
	var dials atomic.Int32
	dial := func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
		dials.Add(1)
		return nil, errors.New("dialed")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := NewClient(ctx, valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}, DialCtxFn: dial})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled before dialing", err)
	}
	if n := dials.Load(); n != 0 {
		t.Fatalf("dialed %d times with an ended ctx", n)
	}
}

// NewClient connects, and the handshake carries the IAM username and token.
func TestNewClient_ConnectsWithIAMToken(t *testing.T) {
	srv := newFakeValkey(t)
	client, err := NewClient(context.Background(), valkey.ClientOption{
		InitAddress:       []string{srv.addr()},
		ForceSingleClient: true, // the fake server doesn't answer CLUSTER commands
	}, WithIAMOptions(staticTokenIAM("conn-token")))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	hello := srv.command("HELLO")
	if want := []string{"HELLO", "3", "AUTH", DefaultIAMUsername, "conn-token"}; !slices.Equal(hello[:min(len(hello), len(want))], want) {
		t.Fatalf("HELLO = %q, want it to start with %q", hello, want)
	}
}

// Without an IAM option, NewClient connects without AUTH, as instances with authentication
// disabled expect.
func TestNewClient_ConnectsWithoutAuth(t *testing.T) {
	srv := newFakeValkey(t)
	client, err := NewClient(context.Background(), valkey.ClientOption{
		InitAddress:       []string{srv.addr()},
		ForceSingleClient: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if hello := srv.command("HELLO"); hello == nil || slices.Contains(hello, "AUTH") {
		t.Fatalf("HELLO = %q, want one without AUTH", hello)
	}
}

// fakeValkey is a minimal RESP3 server for the NewClient tests. It answers HELLO with the
// server information valkey-go reads and every other command with +OK, and records the
// commands it receives.
type fakeValkey struct {
	ln   net.Listener
	mu   sync.Mutex
	cmds [][]string
}

// newFakeValkey starts a fakeValkey on loopback; it stops when the test ends.
func newFakeValkey(t *testing.T) *fakeValkey {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeValkey{ln: ln}
	t.Cleanup(func() { _ = ln.Close() })
	go f.serve()
	return f
}

func (f *fakeValkey) addr() string { return f.ln.Addr().String() }

// command returns the first command received with the given name, or nil.
func (f *fakeValkey) command(name string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, c := range f.cmds {
		if strings.EqualFold(c[0], name) {
			return c
		}
	}
	return nil
}

func (f *fakeValkey) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(c)
	}
}

// helloReply is the RESP3 map valkey-go reads from HELLO: the server version and protocol.
const helloReply = "%3\r\n+server\r\n+valkey\r\n+version\r\n+8.0.0\r\n+proto\r\n:3\r\n"

func (f *fakeValkey) handle(c net.Conn) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	for {
		args, err := readCommand(r)
		if err != nil || len(args) == 0 {
			return
		}
		f.mu.Lock()
		f.cmds = append(f.cmds, args)
		f.mu.Unlock()
		reply := "+OK\r\n"
		if strings.EqualFold(args[0], "HELLO") {
			reply = helloReply
		}
		if _, err := io.WriteString(c, reply); err != nil {
			return
		}
	}
}

// readCommand reads one command: a RESP array of bulk strings.
func readCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for range n {
		hdr, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(hdr, "$")))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2) // the value and its \r\n
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		args = append(args, string(buf[:size]))
	}
	return args, nil
}

// Missing Application Default Credentials (ADC) fail setup with the real error, before any
// dial.
func TestWithDefaultIAM(t *testing.T) {
	// A credentials file that doesn't exist makes ADC fail at once, without probing the
	// metadata server, so the result is the same on laptops, CI and Google Cloud.
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", filepath.Join(t.TempDir(), "missing.json"))
	option := valkey.ClientOption{InitAddress: []string{"127.0.0.1:0"}}
	opt, err := ResolveClientOption(context.Background(), option, WithDefaultIAM())
	if err == nil || !strings.Contains(err.Error(), "Application Default Credentials not found") {
		t.Fatalf("got %v, want the ADC error", err)
	}
	if opt.AuthCredentialsFn != nil {
		t.Error("AuthCredentialsFn set even though setup failed")
	}
	_, err = NewClient(context.Background(), option, WithDefaultIAM())
	if err == nil || !strings.Contains(err.Error(), "Application Default Credentials not found") {
		t.Fatalf("NewClient: got %v, want the ADC error", err)
	}
}
