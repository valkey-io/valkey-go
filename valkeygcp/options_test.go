package valkeygcp

import (
	"context"
	"net/http"
	"testing"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/internal/cmds"
)

func TestNewGCPClusterClientOption(t *testing.T) {
	t.Setenv("GCP_ZONE", "projects/1234567890/zones/us-central1-a")
	option := NewGCPClusterClientOption(context.Background(), valkey.ClientOption{InitAddress: []string{"127.0.0.1:7001"}})
	if !option.EnableReplicaAZInfo {
		t.Fatal("EnableReplicaAZInfo should be enabled")
	}
	b := cmds.NewBuilder(cmds.InitSlot)
	if !option.SendToReplicas(b.Get().Key("k").Build()) || option.SendToReplicas(b.Xread().Block(0).Streams().Key("k").Id("$").Build()) {
		t.Fatal("SendToReplicas should be valkey.DefaultSendToReplicas")
	}
	nodes := []valkey.NodeInfo{{AZ: "us-central1-a"}, {AZ: "us-central1-b"}, {AZ: "us-central1-a"}} // nodes[0] is the primary
	if got := option.ReadNodeSelector(0, nodes); got != 2 {
		t.Fatalf("ReadNodeSelector picked node %d, want the replica in us-central1-a (2)", got)
	}
}

func TestNewGCPClusterClientOptionKeepsUserFunctions(t *testing.T) {
	fakeMetadataServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected metadata server request")
	})
	option := NewGCPClusterClientOption(context.Background(), valkey.ClientOption{
		SendToReplicas:   func(valkey.Completed) bool { return false },
		ReadNodeSelector: func(uint16, []valkey.NodeInfo) int { return 1 },
	})
	if option.SendToReplicas(cmds.NewBuilder(cmds.InitSlot).Get().Key("k").Build()) || option.ReadNodeSelector(0, nil) != 1 {
		t.Fatal("SendToReplicas and ReadNodeSelector should be kept")
	}
}

func TestNewGCPStandaloneClientOption(t *testing.T) {
	fakeMetadataServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected metadata server request")
	})
	option := NewGCPStandaloneClientOption(valkey.ClientOption{
		InitAddress: []string{"10.0.0.2:6379"},
		Standalone:  valkey.StandaloneOption{ReplicaAddress: []string{"10.0.0.3:6379"}},
	})
	b := cmds.NewBuilder(cmds.NoSlot) // the builder of clients with a reader endpoint
	for _, tc := range []struct {
		cmd  valkey.Completed
		want bool
	}{
		{b.Get().Key("k").Build(), true},
		{b.Set().Key("k").Value("v").Build(), false},
		{b.Xread().Block(0).Streams().Key("k").Id("$").Build(), false},
		{b.Subscribe().Channel("c").Build(), false},
	} {
		if got := option.SendToReplicas(tc.cmd); got != tc.want {
			t.Errorf("SendToReplicas(%v) = %v, want %v", tc.cmd.Commands(), got, tc.want)
		}
	}
	if option.EnableReplicaAZInfo || option.ReadNodeSelector != nil {
		t.Fatal("zone options should not be set: the reader endpoint picks the replica")
	}
}

func TestNewGCPStandaloneClientOptionKeepsSendToReplicas(t *testing.T) {
	option := NewGCPStandaloneClientOption(valkey.ClientOption{
		InitAddress:    []string{"10.0.0.2:6379"},
		Standalone:     valkey.StandaloneOption{ReplicaAddress: []string{"10.0.0.3:6379"}},
		SendToReplicas: func(valkey.Completed) bool { return false },
	})
	if option.SendToReplicas(cmds.NewBuilder(cmds.NoSlot).Get().Key("k").Build()) {
		t.Fatal("SendToReplicas should be kept")
	}
}

func TestNewGCPStandaloneClientOptionWithoutReaderEndpoint(t *testing.T) {
	if option := NewGCPStandaloneClientOption(valkey.ClientOption{InitAddress: []string{"10.0.0.2:6379"}}); option.SendToReplicas != nil {
		t.Fatal("SendToReplicas should not be set without a reader endpoint")
	}
}

func TestNewClientError(t *testing.T) {
	t.Setenv("GCP_ZONE", "us-central1-a")
	for _, opts := range [][]Option{nil, {WithReadYourWrites(0)}, {WithReaderEndpoint("127.0.0.1:0")}} {
		if client, err := NewClient(context.Background(), valkey.ClientOption{}, opts...); client != nil || err != valkey.ErrNoAddr {
			t.Fatalf("got %v, %v, want nil, %v", client, err, valkey.ErrNoAddr)
		}
	}
}

func TestNewClient(t *testing.T) {
	t.Setenv("GCP_ZONE", "us-central1-a")
	ctx := context.Background()
	for _, opts := range [][]Option{nil, {WithReadYourWrites(0)}} {
		client, err := NewClient(ctx, valkey.ClientOption{InitAddress: []string{"127.0.0.1:7001"}}, opts...)
		if err != nil {
			t.Fatal(err)
		}
		if r, ok := client.(*readYourWrites); ok != (len(opts) > 0) || ok && r.window != defaultReadYourWritesWindow {
			t.Fatalf("got client %T, want read-your-writes: %v", client, len(opts) > 0)
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
