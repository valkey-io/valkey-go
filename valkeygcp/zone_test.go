package valkeygcp

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/internal/cmds"
)

func TestWithZoneAwareReads(t *testing.T) {
	t.Setenv("GCP_ZONE", "projects/1234567890/zones/us-central1-a")
	option, err := ResolveClientOption(context.Background(), valkey.ClientOption{InitAddress: []string{"127.0.0.1:7001"}},
		WithZoneAwareReads())
	if err != nil {
		t.Fatal(err)
	}
	if !option.EnableReplicaAZInfo {
		t.Fatal("EnableReplicaAZInfo should be enabled")
	}
	b := cmds.NewBuilder(cmds.InitSlot)
	if !option.SendToReplicas(b.Get().Key("k").Build()) || option.SendToReplicas(b.Xread().Block(0).Streams().Key("k").Id("$").Build()) {
		t.Fatal("SendToReplicas should send the reads that don't block to replicas")
	}
	nodes := []valkey.NodeInfo{{AZ: "us-central1-a"}, {AZ: "us-central1-b"}, {AZ: "us-central1-a"}} // nodes[0] is the primary
	if got := option.ReadNodeSelector(0, nodes); got != 2 {
		t.Fatalf("ReadNodeSelector picked node %d, want the replica in us-central1-a (2)", got)
	}
}

func TestWithZoneAwareReadsKeepsUserFunctions(t *testing.T) {
	fakeMetadataServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected metadata server request")
	})
	option, err := ResolveClientOption(context.Background(), valkey.ClientOption{
		InitAddress:      []string{"127.0.0.1:7001"},
		SendToReplicas:   func(valkey.Completed) bool { return false },
		ReadNodeSelector: func(uint16, []valkey.NodeInfo) int { return 1 },
	}, WithZoneAwareReads())
	if err != nil {
		t.Fatal(err)
	}
	if option.SendToReplicas(cmds.NewBuilder(cmds.InitSlot).Get().Key("k").Build()) || option.ReadNodeSelector(0, nil) != 1 {
		t.Fatal("SendToReplicas and ReadNodeSelector should be kept")
	}
}

// valkey-go rejects a ReadNodeSelector next to a ReplicaSelector, so the ReplicaSelector picks the replica.
func TestWithZoneAwareReadsKeepsReplicaSelector(t *testing.T) {
	fakeMetadataServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected metadata server request")
	})
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close() // nothing listens there, so NewClient fails to dial once valkey-go has accepted the options
	ctx := context.Background()
	option := valkey.ClientOption{
		InitAddress:     []string{addr},
		ReplicaSelector: func(uint16, []valkey.NodeInfo) int { return 1 },
	}
	resolved, err := ResolveClientOption(ctx, option, WithZoneAwareReads())
	if err != nil {
		t.Fatal(err)
	}
	if resolved.ReadNodeSelector != nil || resolved.ReplicaSelector(0, nil) != 1 {
		t.Fatal("the ReplicaSelector should be kept, without a ReadNodeSelector")
	}
	if !resolved.EnableReplicaAZInfo || resolved.SendToReplicas == nil {
		t.Fatal("EnableReplicaAZInfo and SendToReplicas should be set")
	}
	if _, err := NewClient(ctx, option, WithZoneAwareReads()); err == nil ||
		errors.Is(err, valkey.ErrReplicaSelectorConflictWithReadNodeSelector) {
		t.Fatalf("NewClient should fail to dial, not reject the options: %v", err)
	}
}

// valkey-go rejects ReplicaOnly next to the SendToReplicas and ReadNodeSelector that WithZoneAwareReads sets.
func TestWithZoneAwareReadsReplicaOnly(t *testing.T) {
	fakeMetadataServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected metadata server request")
	})
	ctx := context.Background()
	option := valkey.ClientOption{InitAddress: []string{"127.0.0.1:7001"}, ReplicaOnly: true}
	if _, err := ResolveClientOption(ctx, option, WithZoneAwareReads()); !errors.Is(err, errZoneAwareReadsReplicaOnly) {
		t.Fatalf("ResolveClientOption: got %v, want %v", err, errZoneAwareReadsReplicaOnly)
	}
	if _, err := NewClient(ctx, option, WithZoneAwareReads()); !errors.Is(err, errZoneAwareReadsReplicaOnly) {
		t.Fatalf("NewClient: got %v, want %v", err, errZoneAwareReadsReplicaOnly)
	}
	if _, err := ResolveClientOption(ctx, option); err != nil {
		t.Fatalf("ReplicaOnly without WithZoneAwareReads: %v", err)
	}
	// With a reader endpoint, WithZoneAwareReads changes nothing, so it doesn't conflict with ReplicaOnly.
	option.Standalone.ReplicaAddress = []string{"127.0.0.1:7002"}
	if _, err := ResolveClientOption(ctx, option, WithZoneAwareReads()); err != nil {
		t.Fatalf("ReplicaOnly with a reader endpoint: %v", err)
	}
}

// Without a zone, WithZoneAwareReads fails rather than create a client that ignores zones.
func TestWithZoneAwareReadsWithoutZone(t *testing.T) {
	fakeMetadataServer(t, func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	})
	option := valkey.ClientOption{InitAddress: []string{"127.0.0.1:7001"}}
	for name, connect := range map[string]func(context.Context) error{
		"ResolveClientOption": func(ctx context.Context) error {
			_, err := ResolveClientOption(ctx, option, WithZoneAwareReads())
			return err
		},
		"NewClient": func(ctx context.Context) error {
			_, err := NewClient(ctx, option, WithZoneAwareReads())
			return err
		},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		if err := connect(ctx); err == nil || !strings.Contains(err.Error(), "GCP_ZONE") {
			t.Errorf("%s: got %v, want an error about the zone", name, err)
		}
		cancel()
	}
}

func TestWithZoneAwareReadsReaderEndpoint(t *testing.T) {
	fakeMetadataServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected metadata server request")
	})
	option, err := ResolveClientOption(context.Background(), valkey.ClientOption{
		InitAddress: []string{"10.0.0.2:6379"},
		Standalone:  valkey.StandaloneOption{ReplicaAddress: []string{"10.0.0.3:6379"}},
	}, WithZoneAwareReads())
	if err != nil {
		t.Fatal(err)
	}
	if option.EnableReplicaAZInfo || option.ReadNodeSelector != nil {
		t.Fatal("zone options should not be set: the reader endpoint picks the replica")
	}
}

func TestWithoutZoneAwareReads(t *testing.T) {
	fakeMetadataServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected metadata server request")
	})
	option, err := ResolveClientOption(context.Background(), valkey.ClientOption{InitAddress: []string{"127.0.0.1:7001"}})
	if err != nil {
		t.Fatal(err)
	}
	if option.EnableReplicaAZInfo || option.SendToReplicas != nil || option.ReadNodeSelector != nil {
		t.Fatal("replica reads should be left off without WithZoneAwareReads")
	}
}

func TestNewClientZoneAwareReads(t *testing.T) {
	const addr = "127.0.0.1:7001" // the cluster of the repository's docker compose setup
	dialer := net.Dialer{Timeout: 300 * time.Millisecond}
	if c, err := dialer.DialContext(context.Background(), "tcp", addr); err != nil {
		t.Skipf("no local Valkey cluster at %s: %v", addr, err)
	} else {
		_ = c.Close()
	}
	t.Setenv("GCP_ZONE", "us-central1-a")
	ctx := context.Background()
	for _, opts := range [][]Option{{WithZoneAwareReads()}, {WithZoneAwareReads(), WithReadYourWrites(0)}} {
		client, err := NewClient(ctx, valkey.ClientOption{InitAddress: []string{addr}}, opts...)
		if err != nil {
			t.Fatal(err)
		}
		if r, ok := client.(*readYourWrites); ok != (len(opts) > 1) || ok && r.window != defaultReadYourWritesWindow {
			t.Fatalf("got client %T, want read-your-writes: %v", client, len(opts) > 1)
		}
		if err := client.Do(ctx, client.B().Set().Key("valkeygcp").Value("1").Build()).Error(); err != nil {
			t.Fatal(err)
		}
		resps := client.DoMulti(ctx, client.B().Incr().Key("valkeygcp").Build(), client.B().Get().Key("valkeygcp").Build())
		if v, err := resps[1].ToString(); err != nil || v != "2" {
			t.Fatalf("got %q, %v, want %q", v, err, "2")
		}
		client.Close()
	}
}

// setMetadataZoneURL points DetectCurrentGCPZone at url and clears the environment variables that take precedence.
func setMetadataZoneURL(t *testing.T, url string) {
	t.Setenv("GCP_ZONE", "")
	t.Setenv("CLOUD_ZONE", "")
	prev := metadataZoneURL
	metadataZoneURL = url
	t.Cleanup(func() { metadataZoneURL = prev })
}

// fakeMetadataServer points DetectCurrentGCPZone at a metadata server served by handler.
func fakeMetadataServer(t *testing.T, handler http.HandlerFunc) {
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	setMetadataZoneURL(t, server.URL)
}

func TestDetectCurrentGCPZoneFromEnv(t *testing.T) {
	fakeMetadataServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected metadata server request")
	})
	for _, tc := range []struct {
		gcpZone, cloudZone, want string
	}{
		{"us-central1-a", "", "us-central1-a"},
		{"projects/1234567890/zones/us-central1-b\n", "", "us-central1-b"},
		{"", "us-east1-c", "us-east1-c"},
		{" ", "projects/1234567890/zones/us-east1-d", "us-east1-d"},
		{"us-central1-a", "us-east1-c", "us-central1-a"},
	} {
		t.Setenv("GCP_ZONE", tc.gcpZone)
		t.Setenv("CLOUD_ZONE", tc.cloudZone)
		if got, err := DetectCurrentGCPZone(context.Background()); err != nil || got != tc.want {
			t.Errorf("GCP_ZONE=%q CLOUD_ZONE=%q: got %q, %v, want %q", tc.gcpZone, tc.cloudZone, got, err, tc.want)
		}
	}
}

func TestDetectCurrentGCPZoneFromMetadataServer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string // "" for an error
	}{
		{"resource path", http.StatusOK, "projects/987654321/zones/us-central1-f", "us-central1-f"},
		{"trailing newline", http.StatusOK, "projects/987654321/zones/us-central1-a\n", "us-central1-a"},
		{"surrounding spaces", http.StatusOK, "  projects/987654321/zones/us-east1-b  ", "us-east1-b"},
		{"zone name", http.StatusOK, "europe-west1-c", "europe-west1-c"},
		{"empty", http.StatusOK, "", ""},
		{"not found", http.StatusNotFound, "projects/987654321/zones/us-central1-a", ""},
		{"server error", http.StatusInternalServerError, "projects/987654321/zones/us-central1-a", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeMetadataServer(t, func(w http.ResponseWriter, r *http.Request) {
				if r.Header.Get("Metadata-Flavor") != "Google" {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				w.WriteHeader(tc.status)
				w.Write([]byte(tc.body))
			})
			// Failed lookups are retried until ctx ends.
			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			if got, err := DetectCurrentGCPZone(ctx); got != tc.want || (err != nil) != (tc.want == "") {
				t.Fatalf("got %q, %v, want %q", got, err, tc.want)
			}
		})
	}
}

// The GKE metadata server can take a few seconds to answer on a new Pod.
func TestDetectCurrentGCPZoneRetries(t *testing.T) {
	var requests atomic.Int32
	fakeMetadataServer(t, func(w http.ResponseWriter, _ *http.Request) {
		if requests.Add(1) <= 2 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		w.Write([]byte("projects/987654321/zones/us-central1-f"))
	})
	if got, err := DetectCurrentGCPZone(context.Background()); err != nil || got != "us-central1-f" {
		t.Fatalf("got %q, %v, want %q", got, err, "us-central1-f")
	}
	if n := requests.Load(); n != 3 {
		t.Fatalf("got %d requests, want 3", n)
	}
}

func TestDetectCurrentGCPZoneTimeout(t *testing.T) {
	for _, tc := range []struct {
		name    string
		handler http.HandlerFunc
	}{
		{"no response", func(_ http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}},
		{"no body", func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.(http.Flusher).Flush()
			<-r.Context().Done()
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fakeMetadataServer(t, tc.handler)
			const timeout = 200 * time.Millisecond
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			start := time.Now()
			if got, err := DetectCurrentGCPZone(ctx); got != "" || !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("got %q, %v, want a deadline error", got, err)
			}
			if elapsed := time.Since(start); elapsed > timeout+time.Second {
				t.Fatalf("took %v, want about %v", elapsed, timeout)
			}
		})
	}
}

func TestDetectCurrentGCPZoneUnreachable(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	for _, url := range []string{closed.URL, "://invalid"} {
		setMetadataZoneURL(t, url)
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		got, err := DetectCurrentGCPZone(ctx)
		cancel()
		if got != "" || err == nil {
			t.Errorf("metadata server %q: got %q, %v, want an error", url, got, err)
		}
	}
}
