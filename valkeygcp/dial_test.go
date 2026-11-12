package valkeygcp

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"math/big"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

var errRefused = errors.New("connection refused")

// After a failed dial, new dials to the same address fail at once with the error of that
// dial, without dialing. Other addresses are dialed as usual.
func TestDialWait_FailsAtOnceAfterAFailedDial(t *testing.T) {
	var dials atomic.Int32
	w := newDialWait()
	w.dial = func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
		dials.Add(1)
		return nil, errRefused
	}
	w.wait = func(int) time.Duration { return time.Hour }
	ctx := context.Background()
	if _, err := w.DialCtxFn(ctx, "10.0.0.5:6379", &net.Dialer{}, nil); err != errRefused {
		t.Fatalf("first dial: got %v, want the dial error", err)
	}
	_, err := w.DialCtxFn(ctx, "10.0.0.5:6379", &net.Dialer{}, nil)
	var waitErr *dialWaitError
	if !errors.As(err, &waitErr) || !errors.Is(err, errRefused) || waitErr.failures != 1 || waitErr.addr != "10.0.0.5:6379" {
		t.Fatalf("second dial: got %v, want a dialWaitError that wraps the dial error", err)
	}
	if n := dials.Load(); n != 1 {
		t.Fatalf("dialed %d times, want 1: the second dial should not be tried", n)
	}
	if _, err := w.DialCtxFn(ctx, "10.0.0.6:6379", &net.Dialer{}, nil); err != errRefused || dials.Load() != 2 {
		t.Fatalf("another address: got %v after %d dials, want it dialed", err, dials.Load())
	}
}

// The wait grows with each failed dial in a row, a dial is tried again once it ends, and a
// successful dial starts over.
func TestDialWait_GrowsAndResets(t *testing.T) {
	now := time.Unix(1000, 0)
	var waits []int
	fail := true
	w := newDialWait()
	w.now = func() time.Time { return now }
	w.wait = func(n int) time.Duration {
		waits = append(waits, n)
		return time.Duration(n) * time.Second
	}
	w.dial = func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
		if fail {
			return nil, errRefused
		}
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return c1, nil
	}
	dial := func() error {
		conn, err := w.DialCtxFn(context.Background(), "10.0.0.5:6379", &net.Dialer{}, nil)
		if conn != nil {
			_ = conn.Close()
		}
		return err
	}
	var waitErr *dialWaitError

	if err := dial(); err != errRefused { // 1st failure: wait 1s
		t.Fatalf("got %v", err)
	}
	now = now.Add(time.Second)
	if err := dial(); err != errRefused { // the wait has ended: 2nd failure, wait 2s
		t.Fatalf("got %v", err)
	}
	now = now.Add(time.Second)
	if err := dial(); !errors.As(err, &waitErr) || waitErr.left != time.Second || waitErr.failures != 2 {
		t.Fatalf("got %v, want a dialWaitError with 1s left after 2 failures", err)
	}
	now = now.Add(time.Second)
	fail = false
	if err := dial(); err != nil {
		t.Fatalf("got %v, want the dial tried and successful", err)
	}
	fail = true
	if err := dial(); err != errRefused { // after a success, the next failure is the 1st again
		t.Fatalf("got %v", err)
	}
	if want := []int{1, 2, 1}; len(waits) != len(want) || waits[0] != want[0] || waits[1] != want[1] || waits[2] != want[2] {
		t.Fatalf("waits for %v failures, want %v", waits, want)
	}
}

// The default wait is random, at most 50ms after the first failure, doubling with each one,
// up to 1s.
func TestDialWait_DefaultWait(t *testing.T) {
	w := newDialWait()
	for n, limit := range map[int]time.Duration{1: 50 * time.Millisecond, 2: 100 * time.Millisecond, 5: 800 * time.Millisecond, 6: time.Second, 30: time.Second} {
		for range 100 {
			if d := w.wait(n); d <= 0 || d > limit {
				t.Fatalf("wait after %d failures = %v, want it in (0, %v]", n, d, limit)
			}
		}
	}
}

// An address whose wait ended more than a minute ago is forgotten when a failure is recorded,
// so the map keeps only addresses that failed recently, and the next failure there starts
// over. Within the minute, the failures in a row keep counting.
func TestDialWait_ForgetsOldFailures(t *testing.T) {
	now := time.Unix(1000, 0)
	w := newDialWait()
	w.now = func() time.Time { return now }
	w.wait = func(int) time.Duration { return time.Second }
	w.dial = func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
		return nil, errRefused
	}
	fail := func(addr string) {
		t.Helper()
		if _, err := w.DialCtxFn(context.Background(), addr, &net.Dialer{}, nil); err != errRefused {
			t.Fatalf("dial %s: got %v, want the dial error", addr, err)
		}
	}

	fail("10.0.0.5:6379") // A fails; its wait ends 1s later
	now = now.Add(30 * time.Second)
	fail("10.0.0.6:6379") // B fails: A's wait ended 29s ago, so A is kept
	if len(w.addrs) != 2 {
		t.Fatalf("%d addresses kept, want 2", len(w.addrs))
	}
	now = now.Add(2 * time.Second)
	fail("10.0.0.6:6379") // B fails again within the minute: 2 in a row
	if n := w.addrs["10.0.0.6:6379"].n; n != 2 {
		t.Fatalf("B has %d failures in a row, want 2", n)
	}

	now = now.Add(2 * time.Minute)
	fail("10.0.0.6:6379") // both waits ended over a minute ago: A is dropped, B starts over
	if _, ok := w.addrs["10.0.0.5:6379"]; ok {
		t.Fatal("A was kept, more than a minute after its wait ended")
	}
	if n := w.addrs["10.0.0.6:6379"].n; n != 1 || len(w.addrs) != 1 {
		t.Fatalf("B has %d failures in a row and %d addresses are kept, want 1 and 1", n, len(w.addrs))
	}
}

// Dials that run at the same time and fail together count as one failure, so many
// connections dialing one address don't make its wait grow at once.
func TestDialWait_ConcurrentFailuresCountOnce(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	w := newDialWait()
	w.dial = func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
		started <- struct{}{}
		<-release
		return nil, errRefused
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, _ = w.DialCtxFn(context.Background(), "10.0.0.5:6379", &net.Dialer{}, nil)
		}()
	}
	for range 4 {
		<-started
	}
	close(release)
	wg.Wait()
	if n := w.addrs["10.0.0.5:6379"].n; n != 1 {
		t.Fatalf("%d failures counted, want 1", n)
	}
}

// A dial that starts after the wait ended counts as a new failure in a row, even when an
// older, slower dial that ran at the same time as the last counted failure fails first. Slow
// failures, such as dial timeouts, keep the wait growing.
func TestDialWait_StaggeredFailuresKeepCounting(t *testing.T) {
	var clock atomic.Int64
	at := func(d time.Duration) { clock.Store(int64(d)) }
	w := newDialWait()
	w.now = func() time.Time { return time.Unix(0, clock.Load()) }
	w.wait = func(int) time.Duration { return 50 * time.Millisecond }
	started := make(chan chan struct{})
	w.dial = func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
		release := make(chan struct{})
		started <- release
		<-release
		return nil, errRefused
	}
	const addr = "10.0.0.5:6379"
	// dial starts a dial and returns once it has started, with the channel that ends it.
	dial := func() (chan struct{}, chan error) {
		done := make(chan error, 1)
		go func() {
			_, err := w.DialCtxFn(context.Background(), addr, &net.Dialer{}, nil)
			done <- err
		}()
		return <-started, done
	}

	at(0)
	endA, doneA := dial() // A starts at 0s
	at(time.Second)
	endB, doneB := dial() // B starts at 1s, while A runs
	at(5 * time.Second)
	close(endA) // A fails at 5s: the first failure, the wait ends at 5.05s
	<-doneA
	at(5100 * time.Millisecond)
	endC, doneC := dial() // C starts at 5.1s, after the wait ended
	at(6 * time.Second)
	close(endB) // B fails at 6s: it ran at the same time as A, so it doesn't count
	<-doneB
	at(10100 * time.Millisecond)
	close(endC) // C fails at 10.1s: the second failure in a row
	<-doneC

	w.mu.Lock()
	n := w.addrs[addr].n
	w.mu.Unlock()
	if n != 2 {
		t.Fatalf("%d failures in a row, want 2: C started after the wait ended", n)
	}
	var waitErr *dialWaitError
	if _, err := w.DialCtxFn(context.Background(), addr, &net.Dialer{}, nil); !errors.As(err, &waitErr) {
		t.Fatalf("a dial right after C failed: got %v, want a dialWaitError", err)
	}
}

// A dial that fails because the caller's context ended says nothing about the node, so it
// doesn't make the next dial wait.
func TestDialWait_CallerContextDoesNotCount(t *testing.T) {
	var dials atomic.Int32
	w := newDialWait()
	w.dial = func(ctx context.Context, _ string, _ *net.Dialer, _ *tls.Config) (net.Conn, error) {
		dials.Add(1)
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		return nil, errRefused
	}
	w.wait = func(int) time.Duration { return time.Hour }
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := w.DialCtxFn(ctx, "10.0.0.5:6379", &net.Dialer{}, nil); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if _, err := w.DialCtxFn(context.Background(), "10.0.0.5:6379", &net.Dialer{}, nil); err != errRefused || dials.Load() != 2 {
		t.Fatalf("got %v after %d dials, want the second dial tried", err, dials.Load())
	}
	// During the wait that this failure started, a dial with an ended context reports that,
	// as a dial would, rather than the earlier dial error.
	if _, err := w.DialCtxFn(ctx, "10.0.0.5:6379", &net.Dialer{}, nil); !errors.Is(err, context.Canceled) || dials.Load() != 2 {
		t.Fatalf("during a wait: got %v after %d dials, want context.Canceled without a dial", err, dials.Load())
	}
}

// dialDirect dials TCP, and TLS when given a tls.Config, as valkey-go does without a
// DialCtxFn.
func TestDialDirect(t *testing.T) {
	serverTLS, roots := selfSignedTLS(t)
	for _, tc := range []struct {
		name      string
		listen    func() (net.Listener, error)
		clientTLS *tls.Config
	}{
		{"TCP", func() (net.Listener, error) { return net.Listen("tcp", "127.0.0.1:0") }, nil},
		{"TLS", func() (net.Listener, error) { return tls.Listen("tcp", "127.0.0.1:0", serverTLS) },
			&tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ln, err := tc.listen()
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = ln.Close() }()
			go func() {
				if c, err := ln.Accept(); err == nil {
					if tc, ok := c.(*tls.Conn); ok {
						_ = tc.Handshake()
					}
					_ = c.Close()
				}
			}()
			conn, err := dialDirect(context.Background(), ln.Addr().String(), &net.Dialer{Timeout: 5 * time.Second}, tc.clientTLS)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			if _, isTLS := conn.(*tls.Conn); isTLS != (tc.clientTLS != nil) {
				t.Fatalf("got a %T", conn)
			}
		})
	}
}

// selfSignedTLS returns a server tls.Config with a certificate for 127.0.0.1, and a pool
// that trusts it.
func selfSignedTLS(t *testing.T) (*tls.Config, *x509.CertPool) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "valkeygcp test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1)},
		KeyUsage:              x509.KeyUsageDigitalSignature | x509.KeyUsageCertSign,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(cert)
	return &tls.Config{Certificates: []tls.Certificate{{Certificate: [][]byte{der}, PrivateKey: key}}, MinVersion: tls.VersionTLS12}, roots
}

// NewClient and ResolveClientOption set the dial wait, unless the caller set a dialer.
func TestResolveClientOption_DialWait(t *testing.T) {
	opt, err := ResolveClientOption(context.Background(), valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}})
	if err != nil || opt.DialCtxFn == nil {
		t.Fatalf("DialCtxFn not set: %v", err)
	}

	var own atomic.Int32
	opt, err = ResolveClientOption(context.Background(), valkey.ClientOption{
		InitAddress: []string{"10.0.0.15:6379"},
		DialCtxFn: func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
			own.Add(1)
			return nil, errRefused
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 2 {
		_, _ = opt.DialCtxFn(context.Background(), "10.0.0.15:6379", &net.Dialer{}, nil)
	}
	if own.Load() != 2 {
		t.Fatal("the caller's DialCtxFn was replaced or wrapped")
	}

	// A caller's DialFn must keep working.
	opt, err = ResolveClientOption(context.Background(), valkey.ClientOption{
		InitAddress: []string{"10.0.0.15:6379"},
		DialFn:      func(string, *net.Dialer, *tls.Config) (net.Conn, error) { return nil, errRefused },
	})
	if err != nil || opt.DialCtxFn != nil {
		t.Fatalf("DialCtxFn set next to the caller's DialFn, which it would replace: %v", err)
	}
}

// Through valkey-go: once a server is gone, a client keeps dialing it for nearly every
// command, and with the dial wait only a few times.
func TestNewClient_DialWaitAfterServerIsGone(t *testing.T) {
	run := func(t *testing.T, withWait bool) int32 {
		srv := newFakeValkey(t)
		var dials atomic.Int32
		var mu sync.Mutex
		var conns []net.Conn
		counting := func(ctx context.Context, dst string, dialer *net.Dialer, tlsConfig *tls.Config) (net.Conn, error) {
			dials.Add(1)
			conn, err := dialDirect(ctx, dst, dialer, tlsConfig)
			if err == nil {
				mu.Lock()
				conns = append(conns, conn)
				mu.Unlock()
			}
			return conn, err
		}
		opt, err := ResolveClientOption(context.Background(), valkey.ClientOption{
			InitAddress:       []string{srv.addr()},
			ForceSingleClient: true,
		})
		if err != nil {
			t.Fatal(err)
		}
		opt.DialCtxFn = counting
		if withWait {
			w := newDialWait()
			w.dial = counting
			// A fixed long wait keeps the count independent of how fast the runner is; the
			// growth and the end of the wait have their own tests.
			w.wait = func(int) time.Duration { return time.Hour }
			opt.DialCtxFn = w.DialCtxFn
		}
		client, err := valkey.NewClient(opt)
		if err != nil {
			t.Fatal(err)
		}
		defer client.Close()
		if err := client.Do(context.Background(), client.B().Set().Key("k").Value("v").Build()).Error(); err != nil {
			t.Fatal(err)
		}

		// The server goes away: nothing listens on its port, and its connections break.
		_ = srv.ln.Close()
		mu.Lock()
		for _, c := range conns {
			_ = c.Close()
		}
		mu.Unlock()
		dials.Store(0)
		for range 300 {
			if client.Do(context.Background(), client.B().Set().Key("k").Value("v").Build()).Error() == nil {
				t.Fatal("a command succeeded after the server was gone")
			}
		}
		return dials.Load()
	}
	without := run(t, false)
	with := run(t, true)
	t.Logf("dials for 300 commands after the server was gone: %d without the dial wait, %d with it", without, with)
	if without < 100 {
		t.Fatalf("only %d dials without the dial wait; the test no longer shows the problem", without)
	}
	if with > 10 {
		t.Fatalf("%d dials with the dial wait, want a few", with)
	}
}
