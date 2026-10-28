package valkeygcp

import (
	"bufio"
	"context"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
)

func TestNewClientReaderEndpoint(t *testing.T) {
	noMetadataServer(t)
	primary, reader := newFakeEndpoint(t, ""), newFakeEndpoint(t, "")
	for name, tc := range map[string]struct {
		option valkey.ClientOption
		opts   []Option
	}{
		"WithReaderEndpoint": {
			option: valkey.ClientOption{InitAddress: []string{primary.addr()}},
			opts:   []Option{WithReaderEndpoint(reader.addr())},
		},
		"ReplicaAddress": {
			option: valkey.ClientOption{
				InitAddress: []string{primary.addr()},
				Standalone:  valkey.StandaloneOption{ReplicaAddress: []string{reader.addr()}},
			},
		},
	} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			client, err := NewClient(ctx, tc.option, tc.opts...)
			if err != nil {
				t.Fatal(err)
			}
			defer client.Close()

			k := name // keys of this subtest
			for _, cmd := range []valkey.Completed{
				client.B().Get().Key(k).Build(),
				client.B().Set().Key(k).Value("v").Build(),
				client.B().Xread().Block(1).Streams().Key(k).Id("$").Build(),
			} {
				if err := client.Do(ctx, cmd).Error(); err != nil {
					t.Fatal(err)
				}
			}
			for _, multi := range [][]valkey.Completed{
				{client.B().Multi().Build(), client.B().Get().Key(k + ":tx").Build(), client.B().Exec().Build()},
				{client.B().Get().Key(k + ":a").Build(), client.B().Get().Key(k + ":b").Build()},
			} {
				for _, resp := range client.DoMulti(ctx, multi...) {
					if err := resp.Error(); err != nil {
						t.Fatal(err)
					}
				}
			}

			for _, want := range []struct {
				cmd      []string
				toReader bool
			}{
				{[]string{"GET", k}, true},
				{[]string{"SET", k, "v"}, false},
				{[]string{"XREAD", "BLOCK", "1", "STREAMS", k, "$"}, false}, // blocking
				{[]string{"GET", k + ":tx"}, false},                         // in a transaction
				{[]string{"GET", k + ":a"}, true},                           // in a batch of reads
			} {
				if onReader, onPrimary := reader.got(want.cmd...), primary.got(want.cmd...); onReader != want.toReader || onPrimary == want.toReader {
					t.Errorf("%q went to the reader endpoint: %v, the primary endpoint: %v; want the reader endpoint: %v",
						want.cmd, onReader, onPrimary, want.toReader)
				}
			}
		})
	}
}

func TestNewClientReaderEndpointWithoutReplicas(t *testing.T) {
	noMetadataServer(t)
	primary, reader := newFakeEndpoint(t, ""), newFakeEndpoint(t, "ERR no replicas found")
	ctx := context.Background()
	client, err := NewClient(ctx, valkey.ClientOption{InitAddress: []string{primary.addr()}}, WithReaderEndpoint(reader.addr()))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if !reader.gotCommand("HELLO") {
		t.Fatal("NewClient should have tried the reader endpoint")
	}
	if err := client.Do(ctx, client.B().Get().Key("k").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if !primary.got("GET", "k") || reader.got("GET", "k") {
		t.Fatal("without replicas, reads should go to the primary endpoint")
	}
}

func TestNewClientReaderEndpointError(t *testing.T) {
	noMetadataServer(t)
	primary, reader := newFakeEndpoint(t, ""), newFakeEndpoint(t, "ERR something else")
	client, err := NewClient(context.Background(), valkey.ClientOption{InitAddress: []string{primary.addr()}}, WithReaderEndpoint(reader.addr()))
	if err == nil {
		client.Close()
		t.Fatal("NewClient should return the other errors of the reader endpoint")
	}
	if !strings.Contains(err.Error(), "something else") {
		t.Fatalf("got %v, want the error of the reader endpoint", err)
	}
}

func TestNewClientReaderEndpointReadYourWrites(t *testing.T) {
	noMetadataServer(t)
	primary, reader := newFakeEndpoint(t, ""), newFakeEndpoint(t, "")
	ctx := context.Background()
	client, err := NewClient(ctx, valkey.ClientOption{InitAddress: []string{primary.addr()}},
		WithReaderEndpoint(reader.addr()), WithReadYourWrites(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, cmd := range []valkey.Completed{
		client.B().Set().Key("{a}k").Value("v").Build(),
		client.B().Get().Key("{a}k").Build(),
		client.B().Get().Key("{b}k").Build(),
	} {
		if err := client.Do(ctx, cmd).Error(); err != nil {
			t.Fatal(err)
		}
	}
	if !primary.got("GET", "{a}k") || reader.got("GET", "{a}k") {
		t.Error("a read of a key just written should go to the primary endpoint")
	}
	if !reader.got("GET", "{b}k") || primary.got("GET", "{b}k") {
		t.Error("a read of another slot should go to the reader endpoint")
	}
}

// noMetadataServer fails the test if the zone is detected: with a reader endpoint, the reader endpoint, not the
// client, picks the replica.
func noMetadataServer(t *testing.T) {
	fakeMetadataServer(t, func(http.ResponseWriter, *http.Request) {
		t.Error("unexpected metadata server request")
	})
}

// fakeEndpoint is a minimal RESP3 server standing in for a Memorystore endpoint. It answers HELLO with the server
// information valkey-go reads and the other commands with +OK, or every command with errReply when it is set,
// and records the commands it receives.
type fakeEndpoint struct {
	ln       net.Listener
	errReply string
	mu       sync.Mutex
	cmds     [][]string
}

// newFakeEndpoint starts a fakeEndpoint on loopback; it stops when the test ends.
func newFakeEndpoint(t *testing.T, errReply string) *fakeEndpoint {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeEndpoint{ln: ln, errReply: errReply}
	t.Cleanup(func() { _ = ln.Close() })
	go f.serve()
	return f
}

func (f *fakeEndpoint) addr() string { return f.ln.Addr().String() }

// got reports whether the endpoint received the command cmd.
func (f *fakeEndpoint) got(cmd ...string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.ContainsFunc(f.cmds, func(c []string) bool { return slices.Equal(c, cmd) })
}

// gotCommand reports whether the endpoint received a command named name.
func (f *fakeEndpoint) gotCommand(name string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.ContainsFunc(f.cmds, func(c []string) bool { return strings.EqualFold(c[0], name) })
}

func (f *fakeEndpoint) serve() {
	for {
		c, err := f.ln.Accept()
		if err != nil {
			return
		}
		go f.handle(c)
	}
}

// fakeHelloReply is the RESP3 map valkey-go reads from HELLO: the server version and protocol.
const fakeHelloReply = "%3\r\n+server\r\n+valkey\r\n+version\r\n+8.0.0\r\n+proto\r\n:3\r\n"

func (f *fakeEndpoint) handle(c net.Conn) {
	defer func() { _ = c.Close() }()
	r := bufio.NewReader(c)
	for {
		cmd, err := readRESPCommand(r)
		if err != nil || len(cmd) == 0 {
			return
		}
		f.mu.Lock()
		f.cmds = append(f.cmds, cmd)
		f.mu.Unlock()
		reply := "+OK\r\n"
		switch {
		case f.errReply != "":
			reply = "-" + f.errReply + "\r\n"
		case strings.EqualFold(cmd[0], "HELLO"):
			reply = fakeHelloReply
		}
		if _, err := io.WriteString(c, reply); err != nil {
			return
		}
	}
}

// readRESPCommand reads one command: a RESP array of bulk strings.
func readRESPCommand(r *bufio.Reader) ([]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(line, "*")))
	if err != nil {
		return nil, err
	}
	cmd := make([]string, 0, n)
	for range n {
		header, err := r.ReadString('\n')
		if err != nil {
			return nil, err
		}
		size, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "$")))
		if err != nil {
			return nil, err
		}
		buf := make([]byte, size+2) // the value and its \r\n
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		cmd = append(cmd, string(buf[:size]))
	}
	return cmd, nil
}
