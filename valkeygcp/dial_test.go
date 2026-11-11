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
	"syscall"
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

// The endpoints given to newDialWait are dialed every time, even right after a failed dial,
// and leave no state. Other addresses still wait after a failure.
func TestDialWait_DialsEndpointsEveryTime(t *testing.T) {
	endpoints := []string{"10.0.0.15:6379", "10.0.0.3:6379"}
	const node = "10.0.0.5:6379"
	dials := map[string]int{}
	w := newDialWait(endpoints...)
	w.dial = func(_ context.Context, dst string, _ *net.Dialer, _ *tls.Config) (net.Conn, error) {
		dials[dst]++
		return nil, errRefused
	}
	w.wait = func(int) time.Duration { return time.Hour }
	ctx := context.Background()
	for _, endpoint := range endpoints {
		for i := range 5 {
			if _, err := w.DialCtxFn(ctx, endpoint, &net.Dialer{}, nil); err != errRefused {
				t.Fatalf("dial %d to the endpoint %s: got %v, want the dial error, not a dialWaitError", i+1, endpoint, err)
			}
		}
		if dials[endpoint] != 5 {
			t.Fatalf("the endpoint %s was dialed %d times in 5 tries, want 5", endpoint, dials[endpoint])
		}
	}
	if len(w.addrs) != 0 {
		t.Fatalf("%d addresses kept after dials to the endpoints only, want 0", len(w.addrs))
	}

	if _, err := w.DialCtxFn(ctx, node, &net.Dialer{}, nil); err != errRefused {
		t.Fatalf("first dial to a node: got %v, want the dial error", err)
	}
	var waitErr *dialWaitError
	if _, err := w.DialCtxFn(ctx, node, &net.Dialer{}, nil); !errors.As(err, &waitErr) || dials[node] != 1 {
		t.Fatalf("second dial to a node: got %v after %d dials, want a dialWaitError without a dial", err, dials[node])
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
	if f := w.addrs["10.0.0.5:6379"]; f == nil || f.n != 0 || f.until.After(now) {
		t.Fatalf("after a success: %+v, want the success kept, with no failures and no wait", f)
	}
	fail = true
	if err := dial(); err != errRefused { // the next dial is tried, and its failure is the 1st again
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
// and the next failure there starts over. Within the minute, the failures in a row keep
// counting.
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

// A successful dial also forgets the addresses whose wait ended more than a minute ago, and
// an address is forgotten a minute after its last success, even when no dial fails.
func TestDialWait_SuccessForgetsOldAddresses(t *testing.T) {
	now := time.Unix(1000, 0)
	w := newDialWait()
	w.now = func() time.Time { return now }
	w.wait = func(int) time.Duration { return time.Second }
	w.dial = func(_ context.Context, dst string, _ *net.Dialer, _ *tls.Config) (net.Conn, error) {
		if dst == "10.0.0.5:6379" {
			return nil, errRefused
		}
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return c1, nil
	}
	dial := func(addr string) error {
		conn, err := w.DialCtxFn(context.Background(), addr, &net.Dialer{}, nil)
		if conn != nil {
			_ = conn.Close()
		}
		return err
	}

	if err := dial("10.0.0.5:6379"); err != errRefused { // A fails; its wait ends 1s later
		t.Fatalf("A: got %v, want the dial error", err)
	}
	now = now.Add(2 * time.Minute)
	if err := dial("10.0.0.6:6379"); err != nil { // B succeeds: A's wait ended over a minute ago
		t.Fatalf("B: got %v, want a success", err)
	}
	if _, ok := w.addrs["10.0.0.5:6379"]; ok {
		t.Fatal("A was kept after a success elsewhere, more than a minute after its wait ended")
	}
	if f := w.addrs["10.0.0.6:6379"]; f == nil || f.n != 0 || f.until.After(now) || len(w.addrs) != 1 {
		t.Fatalf("B is %+v and %d addresses are kept, want B's success and 1", f, len(w.addrs))
	}
	now = now.Add(30 * time.Second)
	if err := dial("10.0.0.7:6379"); err != nil { // C succeeds: B's success was 30s ago, so B is kept
		t.Fatalf("C: got %v, want a success", err)
	}
	if len(w.addrs) != 2 {
		t.Fatalf("%d addresses kept, want 2", len(w.addrs))
	}
	now = now.Add(time.Minute)
	if err := dial("10.0.0.7:6379"); err != nil { // C succeeds again: B's success was 90s ago
		t.Fatalf("C: got %v, want a success", err)
	}
	if _, ok := w.addrs["10.0.0.6:6379"]; ok || len(w.addrs) != 1 {
		t.Fatalf("%d addresses kept, want only C: B was kept more than a minute after its success", len(w.addrs))
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

// A dial that started before a success and fails after it doesn't count: the node answered
// since it started. So the next dial is tried, and a dial that starts after the success and
// fails is the first failure in a row.
func TestDialWait_OlderFailureAfterSuccessDoesNotCount(t *testing.T) {
	var clock atomic.Int64
	at := func(d time.Duration) { clock.Store(int64(d)) }
	w := newDialWait()
	w.now = func() time.Time { return time.Unix(0, clock.Load()) }
	w.wait = func(int) time.Duration { return 50 * time.Millisecond }
	started := make(chan chan error)
	w.dial = func(context.Context, string, *net.Dialer, *tls.Config) (net.Conn, error) {
		result := make(chan error)
		started <- result
		if err := <-result; err != nil {
			return nil, err
		}
		c1, c2 := net.Pipe()
		_ = c2.Close()
		return c1, nil
	}
	const addr = "10.0.0.5:6379"
	// dial starts a dial and returns once it has started, with the channel that ends it: nil
	// for a success, or the error of a failure. Every dial in this test must be tried.
	dial := func() (chan error, chan error) {
		t.Helper()
		done := make(chan error, 1)
		go func() {
			conn, err := w.DialCtxFn(context.Background(), addr, &net.Dialer{}, nil)
			if conn != nil {
				_ = conn.Close()
			}
			done <- err
		}()
		select {
		case end := <-started:
			return end, done
		case err := <-done:
			t.Fatalf("got %v without a dial, want the dial tried", err)
			return nil, nil
		}
	}

	at(0)
	endA, doneA := dial() // A starts at 0s, while the node is down
	at(time.Second)
	endB, doneB := dial() // B starts at 1s, as the node comes back
	at(2 * time.Second)
	endB <- nil // B succeeds at 2s
	if err := <-doneB; err != nil {
		t.Fatalf("B: got %v, want a success", err)
	}
	at(3 * time.Second)
	endA <- errRefused // A fails at 3s: it started before B's success, so it doesn't count
	if err := <-doneA; err != errRefused {
		t.Fatalf("A: got %v, want the dial error", err)
	}
	w.mu.Lock()
	f := *w.addrs[addr]
	w.mu.Unlock()
	if f.n != 0 || f.until.After(time.Unix(0, clock.Load())) {
		t.Fatalf("after A failed: %d failures in a row and a wait until %v, want 0 and no wait", f.n, f.until)
	}

	endC, doneC := dial() // C starts at 3s, right after A failed: it is tried
	endC <- errRefused    // C fails: it started after B's success, so it is the first failure
	if err := <-doneC; err != errRefused {
		t.Fatalf("C: got %v, want the dial error", err)
	}
	w.mu.Lock()
	n := w.addrs[addr].n
	w.mu.Unlock()
	if n != 1 {
		t.Fatalf("%d failures in a row after C failed, want 1", n)
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

// NewClient and ResolveClientOption set the dial wait, with the endpoints dialed as usual,
// unless the caller set a dialer.
func TestResolveClientOption_DialWait(t *testing.T) {
	opt, err := ResolveClientOption(context.Background(), valkey.ClientOption{InitAddress: []string{"10.0.0.15:6379"}})
	if err != nil || opt.DialCtxFn == nil {
		t.Fatalf("DialCtxFn not set: %v", err)
	}
	// The default is the dial wait: after a refused dial to an address that is not an
	// endpoint, new dials to it fail at once without dialing. The Dialer counts the real dials.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	refused := ln.Addr().String()
	_ = ln.Close()
	var dials atomic.Int32
	counted := &net.Dialer{Control: func(string, string, syscall.RawConn) error {
		dials.Add(1)
		return nil
	}}
	skipped := 0
	for range 20 {
		_, err := opt.DialCtxFn(context.Background(), refused, counted, nil)
		var waitErr *dialWaitError
		if errors.As(err, &waitErr) {
			skipped++
		}
	}
	if skipped == 0 {
		t.Fatalf("the default DialCtxFn dialed a refused address %d times in 20 tries and skipped none", dials.Load())
	}

	// The endpoints are dialed every time, from InitAddress and from Standalone.ReplicaAddress.
	seeds := append(make([]string, 0, 2), "10.0.0.2:6379") // with room for an append, which must stay unused
	for name, option := range map[string]valkey.ClientOption{
		"InitAddress":               {InitAddress: []string{refused}},
		"Standalone.ReplicaAddress": {InitAddress: seeds, Standalone: valkey.StandaloneOption{ReplicaAddress: []string{refused}}},
	} {
		opt, err := ResolveClientOption(context.Background(), option)
		if err != nil || opt.DialCtxFn == nil {
			t.Fatalf("%s: DialCtxFn not set: %v", name, err)
		}
		dials.Store(0)
		for i := range 20 {
			_, err := opt.DialCtxFn(context.Background(), refused, counted, nil)
			var waitErr *dialWaitError
			if errors.As(err, &waitErr) {
				t.Fatalf("%s: dial %d to the endpoint was skipped: %v", name, i+1, err)
			}
		}
		if n := dials.Load(); n != 20 {
			t.Fatalf("%s: the endpoint was dialed %d times in 20 tries, want 20", name, n)
		}
	}
	if extra := seeds[:2][1]; extra != "" {
		t.Fatalf("the caller's InitAddress was appended to: %q", extra)
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
// command, and with the dial wait only a few times. The server stands for a node address:
// through the defaults, the single server would be the client's endpoint, which is dialed as
// usual.
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
			// The dial wait that the default installs (TestResolveClientOption_DialWait checks
			// that it does), but without endpoints, and with a fixed long wait so that the
			// count doesn't depend on how fast the runner is; the growth and the end of the
			// wait have their own tests.
			w := newDialWait()
			w.dial = counting
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
