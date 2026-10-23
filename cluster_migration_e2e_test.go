package valkey

import (
	"context"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// End-to-end tests for Atomic Slot Migration awareness. They need the cluster9 (Valkey 9.0) and
// cluster91 (Valkey 9.1) services of docker-compose.yml. The measurements of routing during migrations are
// opt-in: VALKEY_ASM_BENCH=1 go test -run TestSlotMigrationAwarenessBenchmarkIntegration -v .

var asmClusters = []struct {
	name  string
	addrs []string
}{
	{"valkey-9.0", []string{"127.0.0.1:7013", "127.0.0.1:7014", "127.0.0.1:7015"}},
	{"valkey-9.1", []string{"127.0.0.1:7016", "127.0.0.1:7017", "127.0.0.1:7018"}},
}

type asmHarness struct {
	t     *testing.T
	nodes map[string]Client // addr -> node-local client
	ids   map[string]string // addr -> node id
}

func newASMHarness(t *testing.T, addrs []string) *asmHarness {
	t.Helper()
	h := &asmHarness{t: t, nodes: map[string]Client{}, ids: map[string]string{}}
	for _, addr := range addrs {
		c, err := NewClient(ClientOption{InitAddress: []string{addr}, ForceSingleClient: true})
		if err != nil {
			t.Fatalf("connect %s: %v", addr, err)
		}
		t.Cleanup(c.Close)
		id, err := c.Do(context.Background(), c.B().Arbitrary("CLUSTER", "MYID").Build()).ToString()
		if err != nil {
			t.Fatalf("CLUSTER MYID %s: %v", addr, err)
		}
		h.nodes[addr] = c
		h.ids[addr] = id
	}
	return h
}

func (h *asmHarness) do(addr string, args ...string) ValkeyResult {
	c := h.nodes[addr]
	return c.Do(context.Background(), c.B().Arbitrary(args[0]).Args(args[1:]...).Build())
}

func (h *asmHarness) owner(slot uint16) string {
	h.t.Helper()
	for addr := range h.nodes {
		shards, err := h.do(addr, "CLUSTER", "SHARDS").ToArray()
		if err != nil {
			continue
		}
		for _, shard := range shards {
			m, _ := shard.AsMap()
			slotsMsg, nodesMsg := m["slots"], m["nodes"]
			slots, _ := slotsMsg.AsIntSlice()
			nodes, _ := nodesMsg.ToArray()
			for i := 0; i+1 < len(slots); i += 2 {
				if int64(slot) >= slots[i] && int64(slot) <= slots[i+1] {
					for _, n := range nodes {
						nm, _ := n.AsMap()
						roleMsg, portMsg, ipMsg := nm["role"], nm["port"], nm["ip"]
						if role, _ := roleMsg.ToString(); role == "master" {
							port, _ := portMsg.AsInt64()
							ip, _ := ipMsg.ToString()
							return ip + ":" + strconv.FormatInt(port, 10)
						}
					}
				}
			}
		}
		break
	}
	h.t.Fatalf("no owner for slot %d", slot)
	return ""
}

// owners returns the primary address of every slot.
func (h *asmHarness) owners() map[uint16]string {
	h.t.Helper()
	out := make(map[uint16]string, 16384)
	for addr := range h.nodes {
		shards, err := h.do(addr, "CLUSTER", "SHARDS").ToArray()
		if err != nil {
			continue
		}
		for _, shard := range shards {
			m, _ := shard.AsMap()
			slotsMsg, nodesMsg := m["slots"], m["nodes"]
			slots, _ := slotsMsg.AsIntSlice()
			nodes, _ := nodesMsg.ToArray()
			primary := ""
			for _, n := range nodes {
				nm, _ := n.AsMap()
				roleMsg, portMsg, ipMsg := nm["role"], nm["port"], nm["ip"]
				if role, _ := roleMsg.ToString(); role == "master" {
					port, _ := portMsg.AsInt64()
					ip, _ := ipMsg.ToString()
					primary = ip + ":" + strconv.FormatInt(port, 10)
				}
			}
			for i := 0; i+1 < len(slots); i += 2 {
				for sl := slots[i]; sl <= slots[i+1]; sl++ {
					out[uint16(sl)] = primary
				}
			}
		}
		return out
	}
	h.t.Fatal("no CLUSTER SHARDS reply")
	return nil
}

func (h *asmHarness) otherThan(addr string) string {
	keys := make([]string, 0, len(h.nodes))
	for a := range h.nodes {
		if a != addr {
			keys = append(keys, a)
		}
	}
	sort.Strings(keys)
	return keys[0]
}

func (h *asmHarness) jobNames(addr string) map[string]bool {
	names := map[string]bool{}
	entries, _ := h.do(addr, "CLUSTER", "GETSLOTMIGRATIONS").ToArray()
	for _, e := range entries {
		m, _ := e.AsMap()
		nameMsg := m["name"]
		name, _ := nameMsg.ToString()
		names[name] = true
	}
	return names
}

// migrate moves slots from src to dst in one CLUSTER MIGRATESLOTS job and blocks until the source reports
// a terminal state. It returns the time at which the test observed success.
func (h *asmHarness) migrate(src string, slots []uint16, dst string) time.Time {
	h.t.Helper()
	before := h.jobNames(src)
	sorted := append([]uint16(nil), slots...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i] < sorted[j] })
	args := []string{"CLUSTER", "MIGRATESLOTS", "SLOTSRANGE"}
	for i := 0; i < len(sorted); {
		j := i
		for j+1 < len(sorted) && sorted[j+1] == sorted[j]+1 {
			j++
		}
		args = append(args, strconv.Itoa(int(sorted[i])), strconv.Itoa(int(sorted[j])))
		i = j + 1
	}
	args = append(args, "NODE", h.ids[dst])
	if err := h.do(src, args...).Error(); err != nil {
		h.t.Fatalf("MIGRATESLOTS: %v", err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		entries, _ := h.do(src, "CLUSTER", "GETSLOTMIGRATIONS").ToArray()
		for _, e := range entries {
			m, _ := e.AsMap()
			nameMsg, stateMsg, msgMsg := m["name"], m["state"], m["message"]
			name, _ := nameMsg.ToString()
			if before[name] {
				continue
			}
			switch state, _ := stateMsg.ToString(); state {
			case "success":
				return time.Now()
			case "failed", "cancelled":
				msg, _ := msgMsg.ToString()
				h.t.Fatalf("migration %s: %s", state, msg)
			}
		}
		time.Sleep(time.Millisecond)
	}
	h.t.Fatal("migration did not finish")
	return time.Time{}
}

// pickSlots chooses nslots slots owned by one primary, the source, and a hash tag for each of them, so keys
// can be placed in every slot. dst is the primary they will be migrated to.
func (h *asmHarness) pickSlots(c *clusterClient, nslots int, tag string) (src, dst string, slots []uint16, tags map[uint16]string) {
	owners := h.owners()
	probe := c.B().Get().Key("{" + tag + "}").Build()
	src = owners[probe.Slot()]
	dst = h.otherThan(src)
	start := int(probe.Slot())
	tags = map[uint16]string{}
	for i := 0; len(slots) < nslots && i < 16384; i++ {
		sl := uint16((start + i) % 16384)
		if owners[sl] == src {
			slots = append(slots, sl)
			tags[sl] = ""
		}
	}
	for i, missing := 0, len(slots); missing > 0; i++ {
		tg := tag + "-" + strconv.Itoa(i)
		kc := c.B().Get().Key("{" + tg + "}").Build()
		if v, ok := tags[kc.Slot()]; ok && v == "" {
			tags[kc.Slot()] = tg
			missing--
		}
	}
	return src, dst, slots, tags
}

// errorCount returns the number of errors of one kind, such as MOVED or ASK, replied by a node since its
// last CONFIG RESETSTAT.
func (h *asmHarness) errorCount(addr, kind string) int64 {
	info, _ := h.do(addr, "INFO", "errorstats").ToString()
	prefix := "errorstat_" + kind + ":count="
	for _, line := range strings.Split(info, "\n") {
		if strings.HasPrefix(line, prefix) {
			n, _ := strconv.ParseInt(strings.TrimSpace(strings.TrimPrefix(line, prefix)), 10, 64)
			return n
		}
	}
	return 0
}

// Monitor profiles used by the integration tests. The idle interval is shortened from its 30s default so a
// test does not wait for a migration to be noticed; the other intervals are the ones being measured.
var (
	asmOff     = ClusterOption{}
	asmDefault = ClusterOption{
		SlotMigrationAwareness:    true,
		SlotMigrationIdleInterval: time.Second, // active 250ms and finalizing 20ms are the defaults
	}
	// asmLargeFleet is the setting documented for fleets of hundreds of clients or more.
	asmLargeFleet = ClusterOption{
		SlotMigrationAwareness:          true,
		SlotMigrationIdleInterval:       time.Second,
		SlotMigrationActiveInterval:     time.Second,
		SlotMigrationFinalizingInterval: 100 * time.Millisecond,
	}
)

func newASMClient(t *testing.T, addrs []string, profile ClusterOption) *clusterClient {
	c, _ := newASMClientCountingPolls(t, addrs, profile)
	return c
}

// newASMClientCountingPolls also returns the number of CLUSTER GETSLOTMIGRATIONS the client has sent.
func newASMClientCountingPolls(t *testing.T, addrs []string, profile ClusterOption) (*clusterClient, *int64) {
	t.Helper()
	var polls int64
	opt := ClientOption{InitAddress: addrs, ClusterOption: profile}
	c, err := newClusterClient(&opt, func(dst string, opt *ClientOption) conn {
		return &pollCountingConn{conn: makeConn(dst, opt), polls: &polls}
	}, newRetryer(defaultRetryDelayFn))
	if err != nil {
		t.Fatalf("newClusterClient: %v", err)
	}
	t.Cleanup(c.Close)
	return c, &polls
}

// waitMonitorBaseline waits until the monitor has recorded its first poll of every primary, which only records the
// jobs already in the logs: a migration that ended before it would be ignored. Every primary is polled in the first
// round, and any later poll is sent by a later round, once the first one has been recorded.
func waitMonitorBaseline(t *testing.T, polls *int64, primaries int, idle time.Duration) {
	t.Helper()
	if idle <= 0 {
		idle = defaultSlotMigrationIdleInterval
	}
	for deadline := time.Now().Add(2*idle + 10*time.Second); atomic.LoadInt64(polls) <= int64(primaries); time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("the monitor did not record its first poll of the primaries")
		}
	}
}

func waitRoute(c *clusterClient, slot uint16, addr string, timeout time.Duration) (time.Duration, bool) {
	start := time.Now()
	for time.Since(start) < timeout {
		if p := c._pick(slot, false); p != nil && p.Addr() == addr {
			return time.Since(start), true
		}
		time.Sleep(time.Millisecond)
	}
	return timeout, false
}

// TestSlotMigrationAwarenessIdleClientIntegration shows the proactive part: a client that sends no commands
// still learns the new slot owner, while a client without the monitor keeps the stale route.
func TestSlotMigrationAwarenessIdleClientIntegration(t *testing.T) {
	for _, cl := range asmClusters {
		t.Run(cl.name, func(t *testing.T) {
			h := newASMHarness(t, cl.addrs)
			aware, polls := newASMClientCountingPolls(t, cl.addrs, asmDefault)
			unaware := newASMClient(t, cl.addrs, asmOff)

			key := "{asm-idle}k"
			cmd := aware.B().Set().Key(key).Value("v").Build()
			slot := cmd.Slot()
			src := h.owner(slot)
			dst := h.otherThan(src)
			if err := aware.Do(context.Background(), cmd).Error(); err != nil {
				t.Fatal(err)
			}
			waitMonitorBaseline(t, polls, len(cl.addrs), asmDefault.SlotMigrationIdleInterval)

			success := h.migrate(src, []uint16{slot}, dst)
			d, ok := waitRoute(aware, slot, dst, 5*time.Second)
			if !ok {
				t.Fatalf("aware client still routes slot %d to %s", slot, aware._pick(slot, false).Addr())
			}
			t.Logf("aware client routed slot %d to the new owner %v after the observed success (%v after waiting)", slot, time.Since(success), d)

			if p := unaware._pick(slot, false); p == nil || p.Addr() != src {
				t.Fatalf("expected the client without the monitor to keep the stale route")
			}
			// the first command of the stale client is still served correctly through MOVED
			if v, err := unaware.Do(context.Background(), unaware.B().Get().Key(key).Build()).ToString(); err != nil || v != "v" {
				t.Fatalf("unexpected %v %v", v, err)
			}
		})
	}
}

type asmLoadResult struct {
	ops, errs  int64
	moved, ask int64 // replied by the source
	polls      int64 // CLUSTER GETSLOTMIGRATIONS sent by the client from the start of the migration to the end of the run
	converge   time.Duration
	p50, p99   time.Duration
	pmax       time.Duration
	firstError error
}

// runASMLoad migrates nslots slots of one primary in a single job while 32 goroutines read and write keys
// spread over those slots. The slots also hold filler keys, so that the migration takes a realistic amount of
// time to replicate.
func runASMLoad(t *testing.T, h *asmHarness, addrs []string, profile ClusterOption, nslots, filler int, tag string) asmLoadResult {
	t.Helper()
	c, polls := newASMClientCountingPolls(t, addrs, profile)
	ctx := context.Background()

	src, dst, slots, tags := h.pickSlots(c, nslots, tag)
	var keys []string
	var fill, del []Completed
	perSlot := filler / len(slots)
	for _, sl := range slots {
		for n := 0; n < 10; n++ {
			keys = append(keys, "{"+tags[sl]+"}:k"+strconv.Itoa(n))
			del = append(del, c.B().Del().Key(keys[len(keys)-1]).Build())
		}
		for n := 0; n < perSlot; n++ {
			key := "{" + tags[sl] + "}:f" + strconv.Itoa(n)
			fill = append(fill, c.B().Set().Key(key).Value("filler-value-0123456789abcdef0123456789abcdef").Build())
			del = append(del, c.B().Del().Key(key).Build())
		}
	}
	// the keys are removed after the run, so that every run migrates the same amount of data
	defer func() {
		for i := 0; i < len(del); i += 1000 {
			c.DoMulti(ctx, del[i:min(i+1000, len(del))]...)
		}
	}()
	for i := 0; i < len(fill); i += 1000 {
		end := min(i+1000, len(fill))
		for _, r := range c.DoMulti(ctx, fill[i:end]...) {
			if err := r.Error(); err != nil {
				t.Fatal(err)
			}
		}
	}
	for i, k := range keys {
		if err := c.Do(ctx, c.B().Set().Key(k).Value(strconv.Itoa(i)).Build()).Error(); err != nil {
			t.Fatal(err)
		}
	}
	if profile.SlotMigrationAwareness {
		waitMonitorBaseline(t, polls, len(addrs), profile.SlotMigrationIdleInterval)
	}

	if err := h.do(src, "CONFIG", "RESETSTAT").Error(); err != nil {
		t.Fatal(err)
	}

	var res asmLoadResult
	var stop atomic.Bool
	var mu sync.Mutex
	var lat []time.Duration
	var wg sync.WaitGroup
	for w := 0; w < 32; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			local := make([]time.Duration, 0, 4096)
			for i := w; !stop.Load(); i += 32 {
				k := keys[i%len(keys)]
				var cmd Completed
				if i%2 == 0 {
					cmd = c.B().Set().Key(k).Value("x").Build()
				} else {
					cmd = c.B().Get().Key(k).Build()
				}
				cctx, cancel := context.WithTimeout(ctx, 3*time.Second)
				start := time.Now()
				err := c.Do(cctx, cmd).Error()
				local = append(local, time.Since(start))
				cancel()
				atomic.AddInt64(&res.ops, 1)
				if err != nil && !IsValkeyNil(err) {
					if atomic.AddInt64(&res.errs, 1) == 1 {
						mu.Lock()
						res.firstError = err
						mu.Unlock()
					}
				}
			}
			mu.Lock()
			lat = append(lat, local...)
			mu.Unlock()
		}(w)
	}

	time.Sleep(500 * time.Millisecond)
	polls0 := atomic.LoadInt64(polls)
	success := h.migrate(src, slots, dst)
	res.converge = -1
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		done := true
		for _, sl := range slots {
			if p := c._pick(sl, false); p == nil || p.Addr() != dst {
				done = false
				break
			}
		}
		if done {
			res.converge = time.Since(success)
			break
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(time.Second)
	stop.Store(true)
	wg.Wait()

	res.moved = h.errorCount(src, "MOVED")
	res.ask = h.errorCount(src, "ASK")
	res.polls = atomic.LoadInt64(polls) - polls0
	sort.Slice(lat, func(i, j int) bool { return lat[i] < lat[j] })
	if len(lat) > 0 {
		res.p50 = lat[len(lat)/2]
		res.p99 = lat[len(lat)*99/100]
		res.pmax = lat[len(lat)-1]
	}
	return res
}

// TestSlotMigrationAwarenessUnderLoadIntegration migrates 200 slots while 32 goroutines read and write them,
// with the default monitor settings, and checks that no command fails and that every migrated slot is routed
// to its new owner. It is the CI-sized version of TestSlotMigrationAwarenessBenchmarkIntegration.
func TestSlotMigrationAwarenessUnderLoadIntegration(t *testing.T) {
	for _, cl := range asmClusters {
		t.Run(cl.name, func(t *testing.T) {
			h := newASMHarness(t, cl.addrs)
			r := runASMLoad(t, h, cl.addrs, asmDefault, 200, 10000, fmt.Sprintf("asm-ci-%d", time.Now().UnixNano()))
			t.Logf("slots=200 ops=%d errors=%d MOVED=%d ASK=%d polls=%d route-converged=%v", r.ops, r.errs, r.moved, r.ask, r.polls, r.converge)
			if r.errs != 0 {
				t.Fatalf("commands failed during the migration: %d (%v)", r.errs, r.firstError)
			}
			if r.ask != 0 {
				t.Fatalf("an atomic slot migration should never reply ASK, got %d", r.ask)
			}
			if r.converge < 0 {
				t.Fatal("the migrated slots were not routed to the new owner")
			}
		})
	}
}

// TestSlotMigrationAwarenessBenchmarkIntegration measures routing during 1 and 200 slot migrations of 100,000
// keys under load, with the monitor off, at its defaults and at the large-fleet setting.
// MOVED replies are counted on the source node. It takes about a minute, so it only runs with VALKEY_ASM_BENCH=1.
func TestSlotMigrationAwarenessBenchmarkIntegration(t *testing.T) {
	if os.Getenv("VALKEY_ASM_BENCH") == "" {
		t.Skip("set VALKEY_ASM_BENCH=1 to run the slot migration measurements")
	}
	profiles := []struct {
		name string
		opt  ClusterOption
	}{{"off", asmOff}, {"default", asmDefault}, {"large-fleet", asmLargeFleet}}
	for _, cl := range asmClusters {
		for _, nslots := range []int{1, 200} {
			t.Run(fmt.Sprintf("%s/%d-slots", cl.name, nslots), func(t *testing.T) {
				h := newASMHarness(t, cl.addrs)
				for _, p := range profiles {
					r := runASMLoad(t, h, cl.addrs, p.opt, nslots, 100000, fmt.Sprintf("asm-%s-%d-%d", p.name, nslots, time.Now().UnixNano()))
					t.Logf("monitor=%-11s slots=%d ops=%d errors=%d MOVED=%d ASK=%d polls=%d route-converged=%v p50=%v p99=%v max=%v",
						p.name, nslots, r.ops, r.errs, r.moved, r.ask, r.polls, r.converge, r.p50, r.p99, r.pmax)
					if r.errs != 0 {
						t.Fatalf("commands failed during migration with monitor=%s: %d (%v)", p.name, r.errs, r.firstError)
					}
					if p.opt.SlotMigrationAwareness && r.converge < 0 {
						t.Fatalf("monitor=%s did not route the migrated slots to the new owner", p.name)
					}
				}
			})
		}
	}
}

// TestSlotMigrationAwarenessCommandTypesIntegration migrates slots while pipelines, MULTI/EXEC transactions,
// client-side cached reads, Lua scripts and transactions on dedicated connections run against them. None of them
// may fail or apply a write twice. Dedicated connections return MOVED to the caller by design, because their
// WATCH state cannot move with them; they must work again right after the handover.
func TestSlotMigrationAwarenessCommandTypesIntegration(t *testing.T) {
	for _, cl := range asmClusters {
		t.Run(cl.name, func(t *testing.T) {
			h := newASMHarness(t, cl.addrs)
			c, polls := newASMClientCountingPolls(t, cl.addrs, asmDefault)
			ctx := context.Background()
			src, dst, slots, tags := h.pickSlots(c, 20, fmt.Sprintf("asm-types-%d", time.Now().UnixNano()))
			key := func(w int, name string) string { return "{" + tags[slots[w%len(slots)]] + "}:" + name }
			const workers = 4
			for w := 0; w < workers; w++ {
				if err := c.Do(ctx, c.B().Set().Key(key(w, "cached")).Value("v").Build()).Error(); err != nil {
					t.Fatal(err)
				}
				if err := c.Do(ctx, c.B().Set().Key(key(w, "dedicated")).Value("0").Build()).Error(); err != nil {
					t.Fatal(err)
				}
			}
			waitMonitorBaseline(t, polls, len(cl.addrs), asmDefault.SlotMigrationIdleInterval)

			var stop atomic.Bool
			var mu sync.Mutex
			var failures []string
			fail := func(kind string, err error) {
				mu.Lock()
				if len(failures) < 10 {
					failures = append(failures, kind+": "+err.Error())
				}
				mu.Unlock()
			}
			txDone, luaDone := make([]int64, workers), make([]int64, workers)
			var dedicatedMoved int64
			var wg sync.WaitGroup
			run := func(f func(w, i int)) {
				for w := 0; w < workers; w++ {
					wg.Add(1)
					go func(w int) {
						defer wg.Done()
						for i := 0; !stop.Load(); i++ {
							f(w, i)
						}
					}(w)
				}
			}
			run(func(w, i int) { // pipeline: the GET must see the SET sent just before it
				k, v := key(w+i, "pipe"), strconv.Itoa(i)
				r := c.DoMulti(ctx, c.B().Set().Key(k).Value(v).Build(), c.B().Get().Key(k).Build())
				if err := r[0].Error(); err != nil {
					fail("pipeline SET", err)
				} else if got, err := r[1].ToString(); err != nil || got != v {
					fail("pipeline GET", fmt.Errorf("got %q, %v; want %q", got, err, v))
				}
			})
			run(func(w, i int) { // MULTI/EXEC: every committed INCR is applied exactly once
				r := c.DoMulti(ctx, c.B().Multi().Build(), c.B().Incr().Key(key(w, "tx")).Build(), c.B().Exec().Build())
				if err := r[2].Error(); err != nil {
					fail("MULTI/EXEC", err)
				} else {
					txDone[w]++
				}
			})
			run(func(w, i int) { // client-side caching
				if got, err := c.DoCache(ctx, c.B().Get().Key(key(w, "cached")).Cache(), time.Minute).ToString(); err != nil || got != "v" {
					fail("DoCache", fmt.Errorf("got %q, %v", got, err))
				}
			})
			run(func(w, i int) { // Lua: every successful INCR is applied exactly once
				script := c.B().Eval().Script("return redis.call('INCR', KEYS[1])").Numkeys(1).Key(key(w, "lua")).Build()
				if err := c.Do(ctx, script).Error(); err != nil {
					fail("EVAL", err)
				} else {
					luaDone[w]++
				}
			})
			dedicated := func(w int) error {
				return c.Dedicated(func(d DedicatedClient) error {
					k := key(w, "dedicated")
					if err := d.Do(ctx, d.B().Watch().Key(k).Build()).Error(); err != nil {
						return err
					}
					n, err := d.Do(ctx, d.B().Get().Key(k).Build()).AsInt64()
					if err != nil {
						return err
					}
					r := d.DoMulti(ctx, d.B().Multi().Build(), d.B().Set().Key(k).Value(strconv.FormatInt(n+1, 10)).Build(), d.B().Exec().Build())
					if err := r[1].Error(); err != nil {
						return err // a MOVED for the queued command makes the server discard the transaction (EXECABORT)
					}
					return r[2].Error()
				})
			}
			run(func(w, i int) { // WATCH/MULTI/EXEC on a dedicated connection
				if err := dedicated(w); err != nil {
					if verr, ok := IsValkeyErr(err); ok {
						if _, moved := verr.IsMoved(); moved {
							atomic.AddInt64(&dedicatedMoved, 1)
							return
						}
					}
					if !IsValkeyNil(err) { // EXEC returns nil when a concurrent worker changed the watched key
						fail("dedicated", err)
					}
				}
			})

			time.Sleep(300 * time.Millisecond)
			h.migrate(src, slots, dst)
			deadline := time.Now().Add(5 * time.Second)
			for _, sl := range slots {
				if _, ok := waitRoute(c, sl, dst, time.Until(deadline)); !ok {
					t.Fatalf("slot %d was not routed to the new owner", sl)
				}
			}
			time.Sleep(300 * time.Millisecond)
			stop.Store(true)
			wg.Wait()

			if len(failures) > 0 {
				t.Fatalf("commands failed during the migration: %v", failures)
			}
			for w := 0; w < workers; w++ {
				if n, err := c.Do(ctx, c.B().Get().Key(key(w, "tx")).Build()).AsInt64(); err != nil || n != txDone[w] {
					t.Fatalf("MULTI/EXEC counter %d = %d (%v), want %d", w, n, err, txDone[w])
				}
				if n, err := c.Do(ctx, c.B().Get().Key(key(w, "lua")).Build()).AsInt64(); err != nil || n != luaDone[w] {
					t.Fatalf("EVAL counter %d = %d (%v), want %d", w, n, err, luaDone[w])
				}
				if err := dedicated(w); err != nil {
					t.Fatalf("dedicated transaction after the handover: %v", err)
				}
			}
			t.Logf("MULTI/EXEC=%d EVAL=%d dedicated MOVED returned to the caller=%d", sumInt64(txDone), sumInt64(luaDone), dedicatedMoved)
		})
	}
}

func sumInt64(s []int64) (n int64) {
	for _, v := range s {
		n += v
	}
	return n
}

// ---------- compatibility with servers without ASM ----------

// pollCountingConn counts CLUSTER GETSLOTMIGRATIONS sent through a real connection.
type pollCountingConn struct {
	conn
	polls *int64
}

func (c *pollCountingConn) Do(ctx context.Context, cmd Completed) ValkeyResult {
	if cs := cmd.Commands(); len(cs) == 2 && cs[0] == "CLUSTER" && cs[1] == "GETSLOTMIGRATIONS" {
		atomic.AddInt64(c.polls, 1)
	}
	return c.conn.Do(ctx, cmd)
}

// TestSlotMigrationAwarenessCompatibilityIntegration enables the option against servers that do not support
// atomic slot migration and checks that the client keeps working and never sends the command to them.
// It needs the cluster (Valkey 8), cluster5 (Redis 5), cluster9 (Valkey 9.0) and valkey (standalone)
// services of docker-compose.yml.
func TestSlotMigrationAwarenessCompatibilityIntegration(t *testing.T) {
	ctx := context.Background()
	opt := func(addrs []string, resp2 bool) ClientOption {
		return ClientOption{
			InitAddress:  addrs,
			AlwaysRESP2:  resp2,
			DisableCache: resp2,
			ClusterOption: ClusterOption{
				SlotMigrationAwareness:      true,
				SlotMigrationIdleInterval:   20 * time.Millisecond,
				SlotMigrationActiveInterval: 20 * time.Millisecond,
			},
		}
	}
	clusterCase := func(t *testing.T, o ClientOption, wantPolls bool) {
		var polls int64
		c, err := newClusterClient(&o, func(dst string, opt *ClientOption) conn {
			return &pollCountingConn{conn: makeConn(dst, opt), polls: &polls}
		}, newRetryer(defaultRetryDelayFn))
		if err != nil {
			t.Fatalf("newClusterClient: %v", err)
		}
		defer c.Close()
		for i := 0; i < 50; i++ {
			k := "asm-compat-" + strconv.Itoa(i)
			if err := c.Do(ctx, c.B().Set().Key(k).Value("v").Build()).Error(); err != nil {
				t.Fatalf("SET: %v", err)
			}
			if v, err := c.Do(ctx, c.B().Get().Key(k).Build()).ToString(); err != nil || v != "v" {
				t.Fatalf("GET: %v %v", v, err)
			}
		}
		time.Sleep(300 * time.Millisecond) // about 15 idle intervals
		got := atomic.LoadInt64(&polls)
		if wantPolls && got == 0 {
			t.Fatal("expected CLUSTER GETSLOTMIGRATIONS to be polled")
		}
		if !wantPolls && got != 0 {
			t.Fatalf("CLUSTER GETSLOTMIGRATIONS sent %d times to a server without ASM", got)
		}
		t.Logf("CLUSTER GETSLOTMIGRATIONS polls: %d", got)
	}

	t.Run("valkey-8-cluster", func(t *testing.T) {
		clusterCase(t, opt([]string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}, false), false)
	})
	t.Run("valkey-8-cluster-resp2", func(t *testing.T) {
		clusterCase(t, opt([]string{"127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"}, true), false)
	})
	t.Run("redis-5-cluster", func(t *testing.T) {
		// Redis 5 has no RESP3, so valkey-go requires DisableCache (see TestCluster5ClientIntegration)
		clusterCase(t, opt([]string{"127.0.0.1:7004", "127.0.0.1:7005", "127.0.0.1:7006"}, true), false)
	})
	t.Run("valkey-9-cluster-resp2", func(t *testing.T) {
		clusterCase(t, opt([]string{"127.0.0.1:7013", "127.0.0.1:7014", "127.0.0.1:7015"}, true), true)
	})
	t.Run("standalone-valkey", func(t *testing.T) {
		defer ShouldNotLeak(SetupLeakDetection())
		// NewClient falls back to a single-node client when the server is not in cluster mode
		c, err := NewClient(opt([]string{"127.0.0.1:6379"}, false))
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		if c.Mode() != ClientModeStandalone {
			t.Fatalf("expected a standalone client, got %v", c.Mode())
		}
		if err := c.Do(ctx, c.B().Set().Key("asm-compat").Value("v").Build()).Error(); err != nil {
			t.Fatalf("SET: %v", err)
		}
		c.Close() // no monitor goroutine may outlive the client
	})
}
