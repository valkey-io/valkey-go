package valkeygcp

import (
	"context"
	"math/rand/v2"
	"reflect"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/internal/cmds"
)

// fakeClient stands in for a client that reads from replicas: like the real one, it asks sendToReplicas where each
// command goes. It records the answers in toReplica.
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
	return valkey.ValkeyResult{}
}

func (c *fakeClient) DoMulti(_ context.Context, multi ...valkey.Completed) []valkey.ValkeyResult {
	for _, cmd := range multi {
		c.route(cmd)
	}
	return make([]valkey.ValkeyResult, len(multi))
}

func newTestReadYourWrites(window time.Duration) (*readYourWrites, *fakeClient) {
	r := &readYourWrites{start: time.Now(), window: window}
	fake := &fakeClient{sendToReplicas: r.sendToReplicas(valkey.DefaultSendToReplicas)}
	r.Client = fake
	return r, fake
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
