package valkeygcp

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
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
		if got := DetectCurrentGCPZone(context.Background()); got != tc.want {
			t.Errorf("GCP_ZONE=%q CLOUD_ZONE=%q: got %q, want %q", tc.gcpZone, tc.cloudZone, got, tc.want)
		}
	}
}

func TestDetectCurrentGCPZoneFromMetadataServer(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status int
		body   string
		want   string
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
			if got := DetectCurrentGCPZone(context.Background()); got != tc.want {
				t.Fatalf("got %q, want %q", got, tc.want)
			}
		})
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
			start := time.Now()
			if got := DetectCurrentGCPZone(context.Background()); got != "" {
				t.Fatalf("got %q, want empty", got)
			}
			if elapsed := time.Since(start); elapsed > time.Second {
				t.Fatalf("took %v, want about %v", elapsed, metadataTimeout)
			}
		})
	}
}

func TestDetectCurrentGCPZoneUnreachable(t *testing.T) {
	closed := httptest.NewServer(http.NotFoundHandler())
	closed.Close()
	for _, url := range []string{closed.URL, "://invalid"} {
		setMetadataZoneURL(t, url)
		if got := DetectCurrentGCPZone(context.Background()); got != "" {
			t.Errorf("metadata server %q: got %q, want empty", url, got)
		}
	}
}
