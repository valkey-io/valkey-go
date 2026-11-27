package valkeygcp

import (
	"context"
	"encoding/json"
	"math/rand/v2"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/internal/cmds"
)

// fakeClient stands in for a client that reads from replicas: like the real one, it asks sendToReplicas where each
// command goes, and recycles the commands once done, which clears their arguments. It records the answers in toReplica.
type fakeClient struct {
	valkey.Client
	sendToReplicas func(valkey.Completed) bool
	mu             sync.Mutex
	toReplica      []bool
}

func (c *fakeClient) route(cmd valkey.Completed) {
	toReplica := c.sendToReplicas(cmd)
	c.mu.Lock()
	c.toReplica = append(c.toReplica, toReplica)
	c.mu.Unlock()
}

func (c *fakeClient) Do(_ context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	c.route(cmd)
	cmds.PutCompleted(cmd)
	return valkey.ValkeyResult{}
}

func (c *fakeClient) DoMulti(_ context.Context, multi ...valkey.Completed) []valkey.ValkeyResult {
	for _, cmd := range multi {
		c.route(cmd)
	}
	for _, cmd := range multi {
		cmds.PutCompleted(cmd)
	}
	return make([]valkey.ValkeyResult, len(multi))
}

func newTestReadYourWrites(window time.Duration) (*readYourWrites, *fakeClient) {
	r := &readYourWrites{start: time.Now(), window: window}
	fake := &fakeClient{sendToReplicas: r.sendToReplicas(sendReadsToReplicas)}
	r.Client = fake
	return r, fake
}

// Without replica reads, every read goes to the primary already, so NewClient doesn't wrap the client.
func TestNewClientReadYourWritesWithoutReplicaReads(t *testing.T) {
	noMetadataServer(t)
	primary := newFakeEndpoint(t, "")
	ctx := context.Background()
	client, err := NewClient(ctx, valkey.ClientOption{InitAddress: []string{primary.addr()}, ForceSingleClient: true},
		WithReadYourWrites(time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	if _, ok := client.(*readYourWrites); ok {
		t.Fatal("the client should not be wrapped for read-your-writes without replica reads")
	}
	if err := client.Do(ctx, client.B().Get().Key("k").Build()).Error(); err != nil {
		t.Fatal(err)
	}
	if !primary.got("GET", "k") {
		t.Fatal("the read should go to the primary endpoint")
	}
}

func TestReadYourWritesWindow(t *testing.T) {
	r, fake := newTestReadYourWrites(500 * time.Millisecond)
	b := cmds.NewBuilder(cmds.InitSlot)
	r.Do(context.Background(), b.Set().Key("{a}").Value("v").Build())

	time.Sleep(50 * time.Millisecond)
	if fake.sendToReplicas(b.Get().Key("{a}").Build()) {
		t.Fatal("a read 50ms after a write to its slot should go to the primary")
	}
	if !fake.sendToReplicas(b.Get().Key("{b}").Build()) {
		t.Fatal("a read of another slot should go to a replica")
	}

	time.Sleep(550 * time.Millisecond)
	if !fake.sendToReplicas(b.Get().Key("{a}").Build()) {
		t.Fatal("a read 600ms after a write to its slot should go to a replica")
	}
}

func TestReadYourWritesDoMulti(t *testing.T) {
	r, fake := newTestReadYourWrites(time.Minute)
	b := cmds.NewBuilder(cmds.InitSlot)
	r.DoMulti(context.Background(),
		b.Get().Key("{a}").Build(),
		b.Set().Key("{a}").Value("v").Build(),
		b.Get().Key("{a}").Build(),
		b.Get().Key("{b}").Build(),
	)
	// The reads of {a} go to the primary along with the write, so they are ordered with it.
	if want := []bool{false, false, false, true}; !reflect.DeepEqual(fake.toReplica, want) {
		t.Fatalf("sent to replicas: got %v, want %v", fake.toReplica, want)
	}
	if fake.sendToReplicas(b.Get().Key("{a}").Build()) {
		t.Fatal("a read after the batch should go to the primary")
	}
}

func TestReadYourWritesIgnoresCommandsWithoutKeys(t *testing.T) {
	r, _ := newTestReadYourWrites(time.Minute)
	ctx := context.Background()
	r.Do(ctx, cmds.NewBuilder(cmds.InitSlot).Ping().Build())        // cluster client
	r.DoMulti(ctx, cmds.NewBuilder(cmds.NoSlot).Flushall().Build()) // other clients, such as with a reader endpoint
	for i := range r.until {
		if r.until[i].Load() != 0 {
			t.Fatalf("slot %d should not be pinned", i)
		}
	}
}

func TestReadYourWritesReaderEndpoint(t *testing.T) {
	r, fake := newTestReadYourWrites(500 * time.Millisecond)
	b := cmds.NewBuilder(cmds.NoSlot) // the builder of clients with a reader endpoint
	r.Do(context.Background(), b.Set().Key("{a}").Value("v").Build())

	time.Sleep(50 * time.Millisecond)
	if fake.sendToReplicas(b.Get().Key("{a}").Build()) {
		t.Fatal("a read 50ms after a write to its slot should go to the primary")
	}
	if !fake.sendToReplicas(b.Get().Key("{b}").Build()) {
		t.Fatal("a read of another slot should go to the reader endpoint")
	}

	time.Sleep(550 * time.Millisecond)
	if !fake.sendToReplicas(b.Get().Key("{a}").Build()) {
		t.Fatal("a read 600ms after a write to its slot should go to the reader endpoint")
	}
}

// A client with a reader endpoint builds a command with the slot of one of its keys only, so the other keys of a
// command that takes more than one key are found in its arguments.
func TestReadYourWritesReaderEndpointMultiKeyWrites(t *testing.T) {
	b := cmds.NewBuilder(cmds.NoSlot) // the builder of clients with a reader endpoint
	// Each write writes to {a} and {b}. Where it has other arguments, they are {c}, which should not be pinned.
	for _, tc := range []struct {
		name  string
		write func() valkey.Completed
	}{
		{"MSET", func() valkey.Completed {
			return b.Mset().KeyValue().KeyValue("{a}", "{c}").KeyValue("{b}", "{c}").Build()
		}},
		{"MSETNX", func() valkey.Completed {
			return b.Msetnx().KeyValue().KeyValue("{a}", "{c}").KeyValue("{b}", "{c}").Build()
		}},
		{"MSETEX", func() valkey.Completed {
			return b.Msetex().Numkeys(2).KeyValue().KeyValue("{a}", "{c}").KeyValue("{b}", "{c}").Build()
		}},
		{"JSON.MSET", func() valkey.Completed {
			return b.JsonMset().Key("{a}").Path("{c}").Value("{c}").Key("{b}").Path("{c}").Value("{c}").Build()
		}},
		{"DEL", func() valkey.Completed { return b.Del().Key("{a}", "{b}").Build() }},
		{"DEL with chained keys", func() valkey.Completed { return b.Del().Key("{a}").Key("{b}").Build() }},
		{"UNLINK", func() valkey.Completed { return b.Unlink().Key("{a}", "{b}").Build() }},
		{"RENAME", func() valkey.Completed { return b.Rename().Key("{a}").Newkey("{b}").Build() }},
		{"LMOVE", func() valkey.Completed { return b.Lmove().Source("{a}").Destination("{b}").Left().Right().Build() }},
		{"SUNIONSTORE", func() valkey.Completed { return b.Sunionstore().Destination("{a}").Key("{b}").Build() }},
		{"EVALSHA", func() valkey.Completed { return b.Evalsha().Sha1("{c}").Numkeys(2).Key("{a}", "{b}").Build() }},
		{"FCALL", func() valkey.Completed { return b.Fcall().Function("{c}").Numkeys(2).Key("{a}", "{b}").Build() }},
		{"Arbitrary in lower case", func() valkey.Completed { return b.Arbitrary("del").Keys("{a}", "{b}").Build() }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, fake := newTestReadYourWrites(time.Minute)
			r.Do(context.Background(), tc.write())
			for _, key := range []string{"{a}", "{b}"} {
				if fake.sendToReplicas(b.Get().Key(key).Build()) {
					t.Errorf("a read of %s after %s wrote to it should go to the primary", key, tc.name)
				}
			}
			if !fake.sendToReplicas(b.Get().Key("{c}").Build()) {
				t.Errorf("a read of {c}, which %s doesn't write to, should go to the reader endpoint", tc.name)
			}
		})
	}
}

func TestReadYourWritesReaderEndpointMultiKeyReads(t *testing.T) {
	r, fake := newTestReadYourWrites(time.Minute)
	b := cmds.NewBuilder(cmds.NoSlot) // the builder of clients with a reader endpoint
	r.Do(context.Background(), b.Set().Key("{a}").Value("v").Build())
	// The builder keeps the slot of {b} for each of these reads, not that of {a}.
	for _, tc := range []struct {
		name string
		read func() valkey.Completed
	}{
		{"MGET", func() valkey.Completed { return b.Mget().Key("{b}", "{a}").Build() }},
		{"EXISTS", func() valkey.Completed { return b.Exists().Key("{b}", "{a}").Build() }},
		{"SUNION", func() valkey.Completed { return b.Sunion().Key("{b}", "{a}").Build() }},
		{"JSON.MGET", func() valkey.Completed { return b.JsonMget().Key("{b}", "{a}").Path("$").Build() }},
		{"EVALSHA_RO", func() valkey.Completed {
			return b.EvalshaRo().Sha1("{c}").Numkeys(2).Key("{b}", "{a}").Build()
		}},
		{"Arbitrary in lower case", func() valkey.Completed {
			return b.Arbitrary("mget").Keys("{b}", "{a}").ReadOnly()
		}},
	} {
		if fake.sendToReplicas(tc.read()) {
			t.Errorf("%s of {b} and {a} after a write to {a} should go to the primary", tc.name)
		}
	}
	if !fake.sendToReplicas(b.Mget().Key("{b}", "{c}").Build()) {
		t.Error("MGET of {b} and {c} should go to the reader endpoint")
	}
	if !fake.sendToReplicas(b.EvalshaRo().Sha1("{a}").Numkeys(1).Key("{b}").Build()) {
		t.Error("EVALSHA_RO of {b} should go to the reader endpoint: its SHA1 is not a key")
	}
}

func TestReadYourWritesReaderEndpointMultiKeyDoMulti(t *testing.T) {
	r, fake := newTestReadYourWrites(time.Minute)
	b := cmds.NewBuilder(cmds.NoSlot) // the builder of clients with a reader endpoint
	r.DoMulti(context.Background(),
		b.Mset().KeyValue().KeyValue("{a}", "v").KeyValue("{b}", "v").Build(),
		b.Get().Key("{a}").Build(),
	)
	// The read of {a} goes to the primary along with the write, so it is ordered with it.
	if want := []bool{false, false}; !reflect.DeepEqual(fake.toReplica, want) {
		t.Fatalf("sent to replicas: got %v, want %v", fake.toReplica, want)
	}
	if fake.sendToReplicas(b.Get().Key("{a}").Build()) {
		t.Fatal("a read of {a} after the batch should go to the primary")
	}
}

// The other arguments of a command with one key, such as fields and values, are not keys.
func TestReadYourWritesReaderEndpointSingleKey(t *testing.T) {
	r, fake := newTestReadYourWrites(time.Minute)
	b := cmds.NewBuilder(cmds.NoSlot) // the builder of clients with a reader endpoint
	r.Do(context.Background(), b.Hset().Key("{a}").FieldValue().FieldValue("{c}", "{c}").Build())
	if fake.sendToReplicas(b.Hget().Key("{a}").Field("{b}").Build()) {
		t.Error("a read of {a} after a write to it should go to the primary")
	}
	if !fake.sendToReplicas(b.Hget().Key("{b}").Field("{c}").Build()) {
		t.Error("a read of {b} should go to the reader endpoint")
	}
	if !fake.sendToReplicas(b.Get().Key("{c}").Build()) {
		t.Error("a read of {c} should go to the reader endpoint")
	}
}

// A cluster client builds a command with the slot all its keys share, so only that slot is pinned.
func TestReadYourWritesClusterMultiKey(t *testing.T) {
	r, _ := newTestReadYourWrites(time.Minute)
	b := cmds.NewBuilder(cmds.InitSlot)
	r.Do(context.Background(), b.Mset().KeyValue().KeyValue("{a}1", "{b}").KeyValue("{a}2", "{c}").Build())
	for i := range r.until {
		if pinned := r.until[i].Load() != 0; pinned != (i == int(cmds.Slot("{a}"))) {
			t.Errorf("slot %d: pinned %v, want %v", i, pinned, !pinned)
		}
	}
}

// A client with a reader endpoint builds a command whose key is in slot 0 like one without keys, with cmds.NoSlot.
func TestReadYourWritesReaderEndpointSlot0(t *testing.T) {
	const key = "k596"
	if cmds.Slot(key) != 0 {
		t.Fatalf("%s should be in slot 0", key)
	}
	ctx := context.Background()
	b := cmds.NewBuilder(cmds.NoSlot) // the builder of clients with a reader endpoint
	for _, tc := range []struct {
		name  string
		write func(*readYourWrites)
	}{
		{"Do", func(r *readYourWrites) { r.Do(ctx, b.Set().Key(key).Value("v").Build()) }},
		{"DoMulti", func(r *readYourWrites) { r.DoMulti(ctx, b.Set().Key(key).Value("v").Build()) }},
		{"hash tag", func(r *readYourWrites) { r.Do(ctx, b.Set().Key("{"+key+"}x").Value("v").Build()) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r, fake := newTestReadYourWrites(time.Minute)
			if !fake.sendToReplicas(b.Get().Key(key).Build()) {
				t.Fatal("a read of slot 0 before a write to it should go to the reader endpoint")
			}
			tc.write(r)
			for _, read := range []valkey.Completed{
				b.Get().Key(key).Build(),
				b.Hget().Key(key).Field("f").Build(),
				b.ObjectEncoding().Key(key).Build(),
				b.Get().Key("{" + key + "}x").Build(),
			} {
				if fake.sendToReplicas(read) {
					t.Errorf("%q after a write to slot 0 should go to the primary", read.Commands())
				}
			}
			// Reads without keys, and of other slots, still go to the reader endpoint.
			for _, read := range []valkey.Completed{
				b.Scan().Cursor(0).Build(),
				b.Keys().Pattern("*").Build(),
				b.Dbsize().Build(),
				b.Get().Key("{c}").Build(),
			} {
				if !fake.sendToReplicas(read) {
					t.Errorf("%q should go to the reader endpoint", read.Commands())
				}
			}
		})
	}
}

// A write without keys looks like a write to a key in slot 0, so it pins slot 0, unless it has no arguments, such as
// the FLUSHALL in TestReadYourWritesIgnoresCommandsWithoutKeys.
func TestReadYourWritesReaderEndpointKeylessWrite(t *testing.T) {
	r, fake := newTestReadYourWrites(time.Minute)
	b := cmds.NewBuilder(cmds.NoSlot) // the builder of clients with a reader endpoint
	r.Do(context.Background(), b.Publish().Channel("ch").Message("m").Build())
	for i := range r.until {
		if pinned := r.until[i].Load() != 0; pinned != (i == 0) {
			t.Errorf("slot %d: pinned %v, want %v", i, pinned, !pinned)
		}
	}
	if !fake.sendToReplicas(b.Scan().Cursor(0).Build()) {
		t.Error("a read without keys should go to the reader endpoint")
	}
}

func TestReadYourWritesConcurrency(t *testing.T) {
	r, fake := newTestReadYourWrites(time.Minute)
	b := cmds.NewBuilder(cmds.InitSlot)
	ctx := context.Background()
	// 10,000 operations: the race detector allows at most 8128 live goroutines, so they share 100 goroutines.
	const goroutines, ops = 100, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				key := strconv.Itoa(rand.IntN(1 << 20))
				switch i % 3 {
				case 0:
					r.Do(ctx, b.Set().Key(key).Value("v").Build())
					if fake.sendToReplicas(b.Get().Key(key).Build()) {
						t.Error("a read after a write to its slot should go to the primary")
					}
				case 1:
					r.DoMulti(ctx, b.Set().Key(key).Value("v").Build(), b.Get().Key(key).Build())
				default:
					r.Do(ctx, b.Get().Key(key).Build())
				}
			}
		}()
	}
	wg.Wait()
}

func TestReadYourWritesReaderEndpointConcurrency(t *testing.T) {
	r, fake := newTestReadYourWrites(time.Minute)
	b := cmds.NewBuilder(cmds.NoSlot) // the builder of clients with a reader endpoint
	ctx := context.Background()
	const goroutines, ops = 100, 100
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				k1, k2 := strconv.Itoa(rand.IntN(1<<20)), strconv.Itoa(rand.IntN(1<<20))
				switch i % 3 {
				case 0:
					r.Do(ctx, b.Mset().KeyValue().KeyValue(k1, "v").KeyValue(k2, "v").Build())
					if fake.sendToReplicas(b.Mget().Key(k2, k1).Build()) {
						t.Error("a read after a write to its keys should go to the primary")
					}
				case 1:
					r.DoMulti(ctx, b.Del().Key(k1, k2).Build(), b.Mget().Key(k2, k1).Build())
				default:
					r.Do(ctx, b.Mget().Key(k1, k2).Build())
				}
			}
		}()
	}
	wg.Wait()
}

// otherKeys should know every command that can take more than one key in the specs valkey-go generates its command
// builders from, so that a command added there is not missed here.
func TestOtherKeysKnowsMultiKeyCommands(t *testing.T) {
	files, err := filepath.Glob("../hack/cmds/*.json")
	if err != nil || len(files) == 0 {
		t.Skip("the command specs of valkey-go are not in ../hack/cmds")
	}
	// Commands that take more than one key, but neither read nor write them.
	skip := map[string]bool{"WATCH": true, "SSUBSCRIBE": true, "SUNSUBSCRIBE": true, "PUBSUB SHARDNUMSUB": true}
	var multiKey []string
	for _, file := range files {
		data, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var specs map[string]struct{ Arguments []specArg }
		if err := json.Unmarshal(data, &specs); err != nil {
			t.Fatalf("%s: %v", file, err)
		}
		for name, spec := range specs {
			if keyCount(spec.Arguments, false) > 1 && !skip[name] {
				multiKey = append(multiKey, name)
			}
		}
	}
	for _, name := range []string{"MSET", "GEORADIUS"} { // GEORADIUS has its STORE key in an enum
		if !slices.Contains(multiKey, name) {
			t.Fatalf("keyCount should find that %s takes more than one key", name)
		}
	}
	b := cmds.NewBuilder(cmds.NoSlot)
	for _, name := range multiKey {
		cmd := b.Arbitrary(strings.Fields(name)...).Keys("k").Args("1", "2", "3", "4").Build()
		if keys, _ := otherKeys(cmd); len(keys) == 0 {
			t.Errorf("otherKeys should know %s, which takes more than one key", name)
		}
	}
}

// specArg is an argument of a command in the specs of valkey-go.
type specArg struct {
	Type          any       `json:"type"` // a string, or a list of strings for a group of arguments
	Enum          []string  `json:"enum"`
	Multiple      bool      `json:"multiple"`
	Variadic      bool      `json:"variadic"`
	MultipleToken bool      `json:"multiple_token"`
	Block         []specArg `json:"block"`
	Arguments     []specArg `json:"arguments"`
}

// keyCount returns the number of keys args take, or more if args can repeat a key.
func keyCount(args []specArg, repeated bool) int {
	n := 0
	for _, a := range args {
		repeated := repeated || a.Multiple || a.Variadic || a.MultipleToken
		keys := 0
		switch t := a.Type.(type) {
		case string:
			if t == "key" {
				keys = 1
			}
		case []any:
			for _, t := range t {
				if t == "key" {
					keys++
				}
			}
		}
		for _, e := range a.Enum {
			if strings.HasSuffix(e, " key") { // such as STORE key of GEORADIUS
				keys = 1
			}
		}
		if keys > 0 && repeated {
			keys = 2
		}
		n += keys + keyCount(a.Block, repeated) + keyCount(a.Arguments, repeated)
	}
	return n
}
