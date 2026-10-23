package valkey

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// ---------- fixtures ----------

// migEntry builds one CLUSTER GETSLOTMIGRATIONS entry. kv alternates keys and values (string or int64).
// resp3 selects a RESP3 map, otherwise a RESP2 flat array is returned.
func migEntry(resp3 bool, kv ...any) ValkeyMessage {
	values := make([]ValkeyMessage, 0, len(kv))
	for _, v := range kv {
		switch vv := v.(type) {
		case string:
			values = append(values, strmsg(typeBlobString, vv))
		case int64:
			values = append(values, ValkeyMessage{typ: typeInteger, intlen: vv})
		case int:
			values = append(values, ValkeyMessage{typ: typeInteger, intlen: int64(vv)})
		}
	}
	if resp3 {
		return slicemsg(typeMap, values)
	}
	return slicemsg(typeArray, values)
}

func migReply(entries ...ValkeyMessage) ValkeyResult {
	return NewResult(slicemsg(typeArray, entries), nil)
}

func exportEntry(name, state, slots string) ValkeyMessage {
	return migEntry(true,
		"name", name,
		"operation", "EXPORT",
		"slot_ranges", slots,
		"target_node", "4b4f12fdfb58d5e30fef7b9ad3f1651dacbbaba9",
		"source_node", "93941e777e17fcbc92d4398cc957ffea888f472b",
		"state", state,
		"message", "",
		"cow_size", 0,
	)
}

type shardSpec struct {
	ip      string
	port    int64
	slots   []int64 // pairs of start, end
	replica string  // optional "ip:port" of a replica of the shard
}

// testShards builds a CLUSTER SHARDS reply with one primary, and optionally one replica, per shard.
func testShards(specs ...shardSpec) ValkeyResult {
	shards := make([]ValkeyMessage, 0, len(specs))
	for _, s := range specs {
		slots := make([]ValkeyMessage, 0, len(s.slots))
		for _, v := range s.slots {
			slots = append(slots, ValkeyMessage{typ: typeInteger, intlen: v})
		}
		nodes := []ValkeyMessage{shardNode(s.ip, s.port, "master")}
		if ip, port, ok := strings.Cut(s.replica, ":"); ok {
			p, _ := strconv.ParseInt(port, 10, 64)
			nodes = append(nodes, shardNode(ip, p, "replica"))
		}
		shards = append(shards, slicemsg(typeMap, []ValkeyMessage{
			strmsg(typeBlobString, "slots"),
			slicemsg(typeArray, slots),
			strmsg(typeBlobString, "nodes"),
			slicemsg(typeArray, nodes),
		}))
	}
	return NewResult(slicemsg(typeArray, shards), nil)
}

func shardNode(ip string, port int64, role string) ValkeyMessage {
	return slicemsg(typeMap, []ValkeyMessage{
		strmsg(typeBlobString, "id"), strmsg(typeBlobString, ip),
		strmsg(typeBlobString, "port"), {typ: typeInteger, intlen: port},
		strmsg(typeBlobString, "ip"), strmsg(typeBlobString, ip),
		strmsg(typeBlobString, "endpoint"), strmsg(typeBlobString, ip),
		strmsg(typeBlobString, "role"), strmsg(typeBlobString, role),
		strmsg(typeBlobString, "replication-offset"), {typ: typeInteger, intlen: 1},
		strmsg(typeBlobString, "health"), strmsg(typeBlobString, "online"),
	})
}

func isCmd(cmd Completed, parts ...string) bool {
	c := cmd.Commands()
	if len(c) < len(parts) {
		return false
	}
	for i, p := range parts {
		if !strings.EqualFold(c[i], p) {
			return false
		}
	}
	return true
}

// ---------- parsing ----------

func TestParseSlotMigrationRanges(t *testing.T) {
	tests := []struct {
		in   string
		want [][2]int64
	}{
		{"0-10", [][2]int64{{0, 10}}},
		{"100", [][2]int64{{100, 100}}},
		{"0-10 20-30 55", [][2]int64{{0, 10}, {20, 30}, {55, 55}}},
		{"  1-2   3-4 ", [][2]int64{{1, 2}, {3, 4}}},
		{"0-16383", [][2]int64{{0, 16383}}},
		{"", nil},
		{"abc 10-5 0-16384 -1 7-x 0-x", nil},
	}
	for _, tt := range tests {
		if got := parseSlotMigrationRanges(tt.in); !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("parseSlotMigrationRanges(%q) = %v, want %v", tt.in, got, tt.want)
		}
	}
}

func TestParseSlotMigrations(t *testing.T) {
	// fixtures follow the examples of https://valkey.io/commands/cluster-getslotmigrations/
	for _, resp3 := range []bool{false, true} {
		reply := slicemsg(typeArray, []ValkeyMessage{
			migEntry(resp3,
				"name", "5371b28997de6fd0bbe813ad8ebdfdf2faadb308",
				"operation", "EXPORT",
				"slot_ranges", "0-10 20-30",
				"target_node", "4b4f12fdfb58d5e30fef7b9ad3f1651dacbbaba9",
				"source_node", "93941e777e17fcbc92d4398cc957ffea888f472b",
				"create_time", 1754870400,
				"last_update_time", 1754870400,
				"last_ack_time", 1754870400,
				"state", "success",
				"message", "",
				"cow_size", 0,
				"remaining_repl_size", 0,
			),
			migEntry(resp3, // reported by the target node
				"name", "aaaa", "operation", "IMPORT", "slot_ranges", "1-1", "state", "receiving-snapshot",
			),
			migEntry(resp3, // unknown operation, ignored
				"name", "cccc", "operation", "OTHER", "slot_ranges", "1-1", "state", "snapshotting",
			),
			migEntry(resp3, // Valkey 9.0 entry without remaining_repl_size
				"name", "bbbb", "operation", "EXPORT", "slot_ranges", "5", "state", "snapshotting",
			),
			migEntry(resp3, // an entry without a name is ignored
				"operation", "EXPORT", "slot_ranges", "5", "state", "snapshotting",
			),
			migEntry(resp3, // an entry with a key but no value is ignored
				"name", "dddd", "operation", "EXPORT", "state",
			),
			strmsg(typeBlobString, "garbage"), // not a map, ignored
		})
		got, err := parseSlotMigrations(reply)
		if err != nil {
			t.Fatalf("resp3=%v unexpected err %v", resp3, err)
		}
		want := []slotMigration{
			{name: "5371b28997de6fd0bbe813ad8ebdfdf2faadb308", operation: "EXPORT", state: "success", slots: "0-10 20-30"},
			{name: "aaaa", operation: "IMPORT", state: "receiving-snapshot", slots: "1-1"},
			{name: "bbbb", operation: "EXPORT", state: "snapshotting", slots: "5"},
		}
		if !reflect.DeepEqual(got, want) {
			t.Fatalf("resp3=%v got %+v, want %+v", resp3, got, want)
		}
	}

	if _, err := parseSlotMigrations(strmsg(typeBlobString, "x")); err == nil {
		t.Fatal("expected an error for a non-array reply")
	}

}

func TestSlotMigrationPhase(t *testing.T) {
	tests := []struct {
		operation, state string
		want             slotMigrationPhase
	}{
		{"EXPORT", "success", slotMigrationPhaseIdle},
		{"EXPORT", "failed", slotMigrationPhaseIdle},
		{"EXPORT", "cancelled", slotMigrationPhaseIdle},
		{"EXPORT", "connecting", slotMigrationPhaseActive},
		{"EXPORT", "snapshotting", slotMigrationPhaseActive},
		{"EXPORT", "replicating", slotMigrationPhaseActive},
		{"EXPORT", "some-future-state", slotMigrationPhaseActive},
		{"EXPORT", "waiting-to-pause", slotMigrationPhaseFinalizing},
		{"EXPORT", "failover-paused", slotMigrationPhaseFinalizing},
		{"EXPORT", "failover-granted", slotMigrationPhaseFinalizing},
		{"EXPORT", "waiting-for-paused", slotMigrationPhaseActive}, // target states only count on the target
		{"IMPORT", "waiting-for-ack", slotMigrationPhaseActive},
		{"IMPORT", "receiving-snapshot", slotMigrationPhaseActive},
		{"IMPORT", "waiting-for-paused", slotMigrationPhaseFinalizing},
		{"IMPORT", "failover-requested", slotMigrationPhaseFinalizing},
		{"IMPORT", "failover-granted", slotMigrationPhaseFinalizing}, // both sides, right before the handover
		{"IMPORT", "success", slotMigrationPhaseIdle},
		{"IMPORT", "cancelled", slotMigrationPhaseIdle},
	}
	for _, tt := range tests {
		m := slotMigration{operation: tt.operation, state: tt.state}
		if got := m.phase(); got != tt.want {
			t.Fatalf("%s %s phase = %v, want %v", tt.operation, tt.state, got, tt.want)
		}
	}
}

// TestSlotMigrationNodeObserve checks the job tracking of one node: a job that starts and finishes between two polls
// is reported, failed and cancelled jobs never are, and jobs that drop out of the log are forgotten.
func TestSlotMigrationNodeObserve(t *testing.T) {
	n := &slotMigrationNode{}
	n.observe([]slotMigration{{name: "old", state: "success"}}, nil) // baseline
	if done := n.observe([]slotMigration{{name: "fast", state: "success"}}, nil); len(done) != 1 || done[0].name != "fast" {
		t.Fatalf("expected the completion of the job that ran between two polls, got %v", done)
	}
	if _, ok := n.seen["old"]; ok {
		t.Fatal("a job that dropped out of the log should be forgotten")
	}
	n.observe([]slotMigration{{name: "j2", state: "replicating"}}, nil)
	if done := n.observe([]slotMigration{{name: "j2", state: "failed"}, {name: "j3", state: "cancelled"}}, nil); len(done) != 0 {
		t.Fatalf("failed and cancelled jobs must not trigger a refresh, got %v", done)
	}
}

// ---------- monitor ----------

type scriptedConn struct {
	mockConn
	mu       sync.Mutex
	polls    int
	reply    ValkeyResult
	topology ValkeyResult
	shards   int
}

func newScriptedConn(addr string, version int) *scriptedConn {
	s := &scriptedConn{reply: migReply()}
	s.AddrFn = func() string { return addr }
	s.VersionFn = func() int { return version }
	s.DoFn = func(cmd Completed) ValkeyResult {
		s.mu.Lock()
		defer s.mu.Unlock()
		switch {
		case isCmd(cmd, "CLUSTER", "GETSLOTMIGRATIONS"):
			s.polls++
			return s.reply
		case isCmd(cmd, "CLUSTER", "SHARDS"), isCmd(cmd, "CLUSTER", "SLOTS"):
			s.shards++
			return s.topology
		}
		return NewResult(strmsg(typeSimpleString, "OK"), nil)
	}
	return s
}

func (s *scriptedConn) set(r ValkeyResult) {
	s.mu.Lock()
	s.reply = r
	s.mu.Unlock()
}

func (s *scriptedConn) setTopology(r ValkeyResult) {
	s.mu.Lock()
	s.topology = r
	s.mu.Unlock()
}

func (s *scriptedConn) counts() (polls, shards int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.polls, s.shards
}

type refreshCall struct {
	completed []slotMigrationCompletion
}

func newTestMonitor(t *testing.T, opt ClusterOption, owners ...conn) (*slotMigrationMonitor, *time.Time, *[]refreshCall) {
	t.Helper()
	c := &clusterClient{
		opt:    &ClientOption{ClusterOption: opt},
		conns:  map[string]connrole{},
		stopCh: make(chan struct{}),
	}
	per := 16384 / len(owners)
	for i := range c.wslots {
		idx := i / per
		if idx >= len(owners) {
			idx = len(owners) - 1
		}
		c.wslots[i] = owners[idx]
	}
	for _, o := range owners {
		c.conns[o.Addr()] = connrole{conn: o}
	}
	m := newSlotMigrationMonitor(c)
	m.ctx = context.Background() // set by run in production
	now := time.Unix(1_700_000_000, 0)
	m.now = func() time.Time { return now }
	m.jitter = func(d time.Duration) time.Duration { return d } // deterministic schedule
	var calls []refreshCall
	m.refresh = func(completed []slotMigrationCompletion) {
		calls = append(calls, refreshCall{completed: append([]slotMigrationCompletion(nil), completed...)})
	}
	return m, &now, &calls
}

// TestSlotMigrationAwarenessOptions checks the interval defaults and overrides, that the monitor is opt-in, and
// that it is not started when the client fails to build.
func TestSlotMigrationAwarenessOptions(t *testing.T) {
	m, _, _ := newTestMonitor(t, ClusterOption{}, newScriptedConn("a:1", 9))
	if m.idle != defaultSlotMigrationIdleInterval || m.active != defaultSlotMigrationActiveInterval || m.finalizing != defaultSlotMigrationFinalizingInterval {
		t.Fatalf("unexpected defaults %v %v %v", m.idle, m.active, m.finalizing)
	}
	m, _, _ = newTestMonitor(t, ClusterOption{
		SlotMigrationIdleInterval:       time.Second,
		SlotMigrationActiveInterval:     100 * time.Millisecond,
		SlotMigrationFinalizingInterval: time.Millisecond,
	}, newScriptedConn("a:1", 9))
	if m.idle != time.Second || m.active != 100*time.Millisecond || m.finalizing != time.Millisecond {
		t.Fatalf("unexpected intervals %v %v %v", m.idle, m.active, m.finalizing)
	}

	off := newScriptedConn("127.0.0.1:1", 9)
	dst := newScriptedConn("127.0.0.2:2", 9)
	off.setTopology(topologyBefore)
	dst.setTopology(topologyBefore)
	newRefreshTestClient(t, ClusterOption{}, off, dst)

	failed := newScriptedConn("127.0.0.3:3", 9)
	failed.setTopology(topologyBefore)
	_, err := newClusterClient(
		&ClientOption{InitAddress: []string{failed.Addr()}, ClusterOption: ClusterOption{SlotMigrationAwareness: true, ShardsRefreshInterval: -1}},
		func(addr string, _ *ClientOption) conn { return failed },
		newRetryer(defaultRetryDelayFn),
	)
	if err != ErrInvalidShardsRefreshInterval {
		t.Fatalf("unexpected err %v", err)
	}
	time.Sleep(50 * time.Millisecond)
	if p, _ := off.counts(); p != 0 {
		t.Fatalf("the monitor must be opt-in, got %d polls", p)
	}
	if p, _ := failed.counts(); p != 0 {
		t.Fatalf("the monitor must not start when the client fails to build, got %d polls", p)
	}
}

func TestSlotMigrationMonitorLifecycleOfAJob(t *testing.T) {
	src := newScriptedConn("127.0.0.1:1", 9)
	other := newScriptedConn("127.0.0.2:2", 9)
	m, now, calls := newTestMonitor(t, ClusterOption{}, src, other)
	start := *now

	step := func(reply ValkeyResult, wantDelay time.Duration) {
		t.Helper()
		src.set(reply)
		d, ok := m.tick()
		if !ok {
			t.Fatal("monitor stopped unexpectedly")
		}
		if d != wantDelay {
			t.Fatalf("delay = %v, want %v", d, wantDelay)
		}
		*now = now.Add(d)
	}

	step(migReply(exportEntry("old", "success", "0-1")), m.idle) // baseline: old finished job ignored
	step(migReply(exportEntry("old", "success", "0-1"), exportEntry("j1", "snapshotting", "0-99")), m.active)
	step(migReply(exportEntry("j1", "replicating", "0-99")), m.active)
	step(migReply(exportEntry("j1", "waiting-to-pause", "0-99")), m.finalizing)
	step(migReply(exportEntry("j1", "waiting-to-pause", "0-99")), m.finalizing) // a job can stay in this state for several polls
	step(migReply(exportEntry("j1", "failover-granted", "0-99")), m.finalizing)
	if len(*calls) != 0 {
		t.Fatalf("refresh before success: %v", *calls)
	}
	step(migReply(exportEntry("j1", "success", "0-99")), m.active) // cooldown keeps the source at the active cadence
	if len(*calls) != 1 || len((*calls)[0].completed) != 1 {
		t.Fatalf("expected one refresh, got %+v", *calls)
	}
	if got := (*calls)[0].completed[0]; got.reporter != src || got.migration.name != "j1" || got.migration.operation != "EXPORT" ||
		got.migration.slots != "0-99" {
		t.Fatalf("unexpected completion %+v", got)
	}

	// the finished job stays in the log but must not trigger another refresh
	for i := 0; i < 5; i++ {
		src.set(migReply(exportEntry("j1", "success", "0-99")))
		d, _ := m.tick()
		*now = now.Add(d)
	}
	if len(*calls) != 1 {
		t.Fatalf("refresh repeated for a finished job: %d", len(*calls))
	}

	// after the cooldown, the source returns to the idle cadence
	*now = now.Add(slotMigrationCooldown)
	if d, _ := m.tick(); d != m.idle {
		t.Fatalf("expected the idle cadence after the cooldown, got %v", d)
	}

	// the node without migrations was only polled at the idle cadence
	if polls, maxPolls := func() (int, int) { p, _ := other.counts(); return p, int(now.Sub(start)/m.idle) + 1 }(); polls > maxPolls {
		t.Fatalf("idle node polled too often: %d > %d", polls, maxPolls)
	}
}

func TestSlotMigrationMonitorTransientErrors(t *testing.T) {
	n := newScriptedConn("127.0.0.1:1", 9)
	m, now, _ := newTestMonitor(t, ClusterOption{}, n)
	n.set(migReply(exportEntry("j", "snapshotting", "0")))
	m.tick()
	*now = now.Add(m.active)
	n.set(NewErrorResult(errors.New("i/o timeout")))
	if d, ok := m.tick(); !ok || d != m.active {
		t.Fatalf("network errors keep the cadence, got %v %v", d, ok)
	}
	*now = now.Add(m.active)
	n.set(NewResult(strmsg(typeBlobString, "not an array"), nil))
	if d, ok := m.tick(); !ok || d != m.active {
		t.Fatalf("parse errors keep the cadence, got %v %v", d, ok)
	}

	// a node that stops answering during a handover falls back to the active cadence after slotMigrationMaxErrors
	// consecutive errors; the successful poll before them resets the count
	*now = now.Add(m.active)
	n.set(migReply(importEntry("j", "failover-granted", "0")))
	if d, _ := m.tick(); d != m.finalizing {
		t.Fatalf("delay = %v, want the finalizing interval", d)
	}
	n.set(NewErrorResult(errors.New("i/o timeout")))
	for i := 1; i <= slotMigrationMaxErrors; i++ {
		*now = now.Add(m.finalizing)
		want := m.finalizing
		if i == slotMigrationMaxErrors {
			want = m.active
		}
		if d, _ := m.tick(); d != want {
			t.Fatalf("delay after %d errors = %v, want %v", i, d, want)
		}
	}
}

func TestSlotMigrationMonitorTracksTopologyChanges(t *testing.T) {
	a := newScriptedConn("127.0.0.1:1", 9)
	b := newScriptedConn("127.0.0.2:2", 9)
	m, now, _ := newTestMonitor(t, ClusterOption{}, a, b)
	m.tick()
	if len(m.nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(m.nodes))
	}
	m.c.mu.Lock()
	for i := range m.c.wslots {
		m.c.wslots[i] = b
	}
	m.c.mu.Unlock()
	*now = now.Add(m.idle)
	m.tick()
	if _, ok := m.nodes[a]; ok || len(m.nodes) != 1 {
		t.Fatalf("node without slots should be dropped, nodes=%d", len(m.nodes))
	}

	// a refresh by the monitor gives slots to a new primary, which is tracked from the next tick on
	c := newScriptedConn("127.0.0.3:3", 9)
	m.refresh = func([]slotMigrationCompletion) {
		m.c.mu.Lock()
		for i := 0; i < 100; i++ {
			m.c.wslots[i] = c
		}
		m.c.mu.Unlock()
	}
	b.set(migReply(exportEntry("j", "success", "0-99")))
	*now = now.Add(m.idle)
	m.tick()
	*now = now.Add(m.finalizing)
	m.tick()
	if _, ok := m.nodes[c]; !ok {
		t.Fatal("the new owner should be tracked right after the monitor's refresh")
	}

	// each owner is listed once, also when its slots are not contiguous, and unassigned slots are skipped
	m.c.mu.Lock()
	for i := range m.c.wslots {
		m.c.wslots[i] = nil
	}
	m.c.wslots[0], m.c.wslots[1], m.c.wslots[3] = a, b, a
	m.c.mu.Unlock()
	if got := m.primaries(); len(got) != 2 {
		t.Fatalf("expected each of the 2 owners once, got %d entries", len(got))
	}
}

func TestSlotMigrationMonitorReplicaOnlyPollsAllNodes(t *testing.T) {
	a := newScriptedConn("127.0.0.1:1", 9)
	b := newScriptedConn("127.0.0.2:2", 9)
	m, _, _ := newTestMonitor(t, ClusterOption{}, a)
	m.c.opt.ReplicaOnly = true
	m.c.conns[b.Addr()] = connrole{conn: b}
	m.c.conns["hidden:3"] = connrole{conn: newScriptedConn("hidden:3", 9), hidden: true}
	if got := m.primaries(); len(got) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(got))
	}
	// the slot map points to replicas, so it cannot show whether a refresh routes the slots to their new owner
	if !m.routed([]slotMigrationCompletion{exportDone(a)}) {
		t.Fatal("with ReplicaOnly, a refresh should be taken as routed")
	}
}

// ---------- refresh path ----------

func newRefreshTestClient(t *testing.T, opt ClusterOption, src, dst *scriptedConn) *clusterClient {
	t.Helper()
	conns := map[string]*scriptedConn{src.Addr(): src, dst.Addr(): dst}
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{src.Addr()}, ClusterOption: opt},
		func(addr string, _ *ClientOption) conn {
			if c, ok := conns[addr]; ok {
				return c
			}
			return newScriptedConn(addr, 9)
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// newRefreshMonitor returns a monitor of client that is not running, so that its refresh path can be called directly.
func newRefreshMonitor(client *clusterClient) *slotMigrationMonitor {
	m := newSlotMigrationMonitor(client)
	m.ctx = context.Background()
	return m
}

var (
	topologyBefore = testShards(
		shardSpec{ip: "127.0.0.1", port: 1, slots: []int64{0, 8191}},
		shardSpec{ip: "127.0.0.2", port: 2, slots: []int64{8192, 16383}},
	)
	topologyAfter = testShards( // slots 0-99 moved from 127.0.0.1:1 to 127.0.0.2:2
		shardSpec{ip: "127.0.0.1", port: 1, slots: []int64{100, 8191}},
		shardSpec{ip: "127.0.0.2", port: 2, slots: []int64{0, 99, 8192, 16383}},
	)
)

// TestSlotMigrationRefreshFromReporter checks that a completion refreshes the topology from the node that reported
// it, which already knows the new owner, without asking the other node, whose view may still be stale. A stale
// answer is followed at once by another refresh, without waiting for the finalizing interval, and the refreshes
// stop as soon as every migrated slot routes to the new owner; an answer that stays stale leaves the update to
// the regular refresh.
func TestSlotMigrationRefreshFromReporter(t *testing.T) {
	for _, op := range []string{"EXPORT", "IMPORT"} {
		t.Run(op, func(t *testing.T) {
			src := newScriptedConn("127.0.0.1:1", 9)
			dst := newScriptedConn("127.0.0.2:2", 9)
			src.setTopology(topologyBefore)
			dst.setTopology(topologyBefore)
			client := newRefreshTestClient(t, ClusterOption{SlotMigrationFinalizingInterval: time.Hour}, src, dst)
			if got := client._pick(0, false); got != src {
				t.Fatalf("slot 0 should start on the source")
			}
			reporter, other := src, dst // the source reports EXPORT, the target IMPORT
			if op == "IMPORT" {
				reporter, other = dst, src
			}

			// the reporter's first answer is still stale, the second one is fresh
			var calls int32
			reporter.DoFn = func(cmd Completed) ValkeyResult {
				if isCmd(cmd, "CLUSTER", "SHARDS") {
					if atomic.AddInt32(&calls, 1) == 1 {
						return topologyBefore
					}
					return topologyAfter
				}
				return migReply()
			}
			_, otherShards := other.counts()
			start := time.Now()
			newRefreshMonitor(client).refreshFromReporters([]slotMigrationCompletion{{
				reporter:  reporter,
				migration: slotMigration{name: "j", operation: op, state: "success", slots: "0-99"},
			}})
			if d := time.Since(start); d > time.Second {
				t.Fatalf("the refresh should end as soon as the slots route to the new owner, it took %v", d)
			}
			if client._pick(0, false) != dst || client._pick(99, false) != dst {
				t.Fatal("slots 0-99 should move to the target")
			}
			if client._pick(100, false) != src {
				t.Fatal("slot 100 should stay on the source")
			}
			if n := atomic.LoadInt32(&calls); n != 2 {
				t.Fatalf("expected 2 refresh attempts, got %d", n)
			}
			if _, n := other.counts(); n != otherShards {
				t.Fatalf("the refresh should only ask the reporting node, the other node was asked %d times", n-otherShards)
			}
		})
	}

	t.Run("stale", func(t *testing.T) {
		src := newScriptedConn("127.0.0.1:1", 9)
		dst := newScriptedConn("127.0.0.2:2", 9)
		src.setTopology(topologyBefore)
		dst.setTopology(topologyBefore)
		client := newRefreshTestClient(t, ClusterOption{SlotMigrationFinalizingInterval: time.Millisecond}, src, dst)
		_, before := dst.counts()
		newRefreshMonitor(client).refreshFromReporters([]slotMigrationCompletion{exportDone(src)})
		if _, n := dst.counts(); n != before {
			t.Fatalf("the attempts should only ask the reporting node, the other node was asked %d times", n-before)
		}
		// the regular refresh follows after its random delay, and asks every node
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
			if _, n := dst.counts(); n > before {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("the regular refresh should follow when the reporter's answer stays stale")
			}
		}
	})

	t.Run("partial", func(t *testing.T) {
		src := newScriptedConn("127.0.0.1:1", 9)
		dst := newScriptedConn("127.0.0.2:2", 9)
		m, _, _ := newTestMonitor(t, ClusterOption{}, src, dst)
		for _, stale := range []int{0, 99} { // the first and the last migrated slot
			m.c.mu.Lock()
			for i := 0; i < 100; i++ {
				m.c.wslots[i] = dst
			}
			m.c.wslots[stale] = src
			m.c.mu.Unlock()
			if m.routed([]slotMigrationCompletion{exportDone(src)}) {
				t.Fatalf("slot %d still routes to the source, so the slots should not be taken as routed", stale)
			}
		}
	})
}

// TestSlotMigrationRefreshDoesNotWaitForTheRegularRefresh checks that the monitor refreshes from the reporter while a
// regular refresh, such as the one MOVED replies schedule at the handover, is still in flight.
func TestSlotMigrationRefreshDoesNotWaitForTheRegularRefresh(t *testing.T) {
	src := newScriptedConn("127.0.0.1:1", 9)
	dst := newScriptedConn("127.0.0.2:2", 9)
	src.setTopology(topologyBefore)
	dst.setTopology(topologyBefore)
	client := newRefreshTestClient(t, ClusterOption{SlotMigrationFinalizingInterval: time.Millisecond}, src, dst)
	src.setTopology(topologyAfter)
	dst.setTopology(topologyAfter)

	// the regular refresh asks both nodes; their first answers are held until the monitor's refresh is done
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	held := make(chan struct{}, 2)
	for _, c := range []*scriptedConn{src, dst} {
		var first atomic.Bool
		answer := c.DoFn
		c.DoFn = func(cmd Completed) ValkeyResult {
			if isCmd(cmd, "CLUSTER", "SHARDS") && first.CompareAndSwap(false, true) {
				held <- struct{}{}
				<-release
			}
			return answer(cmd)
		}
	}
	regular := make(chan error, 1)
	go func() { regular <- client.refresh(context.Background()) }()
	for i := 0; i < 2; i++ {
		select {
		case <-held:
		case <-time.After(5 * time.Second):
			t.Fatal("the regular refresh did not ask both nodes")
		}
	}

	start := time.Now()
	newRefreshMonitor(client).refreshFromReporters([]slotMigrationCompletion{exportDone(src)})
	if d := time.Since(start); d > time.Second {
		t.Fatalf("the refresh waited for the regular refresh: %v", d)
	}
	if client._pick(0, false) != dst {
		t.Fatal("slot 0 should move to the target")
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-regular; err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	if client._pick(0, false) != dst || client._pick(100, false) != src {
		t.Fatal("after both refreshes, slot 0 should route to the target and slot 100 to the source")
	}
}

// TestSlotMigrationRefreshesOverlapSafely runs two refreshes at the same time, as the monitor's refresh and a regular
// one can, with views of different nodes: one still lists a replica that the other no longer does. The refresh that
// drops the replica closes its connection a few seconds later, so the other one must not put that connection back.
func TestSlotMigrationRefreshesOverlapSafely(t *testing.T) {
	a := newScriptedConn("127.0.0.1:1", 9)
	b := newScriptedConn("127.0.0.2:2", 9)
	withReplica := testShards(
		shardSpec{ip: "127.0.0.1", port: 1, slots: []int64{0, 8191}, replica: "127.0.0.3:3"},
		shardSpec{ip: "127.0.0.2", port: 2, slots: []int64{8192, 16383}},
	)
	withoutReplica := testShards(
		shardSpec{ip: "127.0.0.1", port: 1, slots: []int64{0, 8191}},
		shardSpec{ip: "127.0.0.2", port: 2, slots: []int64{8192, 16383}},
	)
	a.setTopology(withReplica)
	b.setTopology(withoutReplica)

	// every connection to the replica is a new one, as in production; the first refresh that lists the replica
	// once the test is armed stops in the replica's AZ lookup, after it has read the client's connections
	var hold atomic.Bool
	held, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{a.Addr()}, SendToReplicas: func(Completed) bool { return false }, EnableReplicaAZInfo: true},
		func(addr string, _ *ClientOption) conn {
			switch addr {
			case a.Addr():
				return a
			case b.Addr():
				return b
			}
			replica := newScriptedConn(addr, 9)
			replica.AZFn = func() string {
				if hold.CompareAndSwap(true, false) {
					close(held)
					<-release
				}
				return ""
			}
			return replica
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	t.Cleanup(client.Close)
	client.mu.RLock()
	replica := client.conns["127.0.0.3:3"].conn
	client.mu.RUnlock()
	if replica == nil {
		t.Fatal("the replica should be known after the first refresh")
	}

	hold.Store(true)
	listing := make(chan error, 1)
	go func() { listing <- client._refreshFrom([]conn{a}) }()
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the refresh that lists the replica did not look up its AZ")
	}
	dropping := make(chan error, 1)
	go func() { dropping <- client._refreshFrom([]conn{b}) }()
	select { // give the refresh that drops the replica the chance to finish first
	case err := <-dropping:
		dropping <- err
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce.Do(func() { close(release) })
	for _, done := range []chan error{listing, dropping} {
		if err := <-done; err != nil {
			t.Fatalf("unexpected err %v", err)
		}
	}

	client.mu.RLock()
	defer client.mu.RUnlock()
	if cc, ok := client.conns["127.0.0.3:3"]; ok && cc.conn == replica {
		t.Fatal("a refresh put back the connection that the overlapping refresh removed and is about to close")
	}
	if client.wslots[0] != a || client.wslots[16383] != b {
		t.Fatal("the primaries should keep their slots")
	}
}

// ---------- compatibility ----------

func TestIsSlotMigrationUnsupported(t *testing.T) {
	for msg, want := range map[string]bool{
		"ERR unknown subcommand 'GETSLOTMIGRATIONS'. Try CLUSTER HELP.":                                  true, // Valkey 8 / Redis 7
		"ERR Unknown subcommand or wrong number of arguments for 'GETSLOTMIGRATIONS'. Try CLUSTER HELP.": true, // Redis 5
		"ERR unknown command 'CLUSTER', with args beginning with: 'GETSLOTMIGRATIONS'":                   true, // renamed / disabled
		"OOM command not allowed when used memory > 'maxmemory'.":                                        false,
		"ERR This instance has cluster support disabled":                                                 false,
		"LOADING Valkey is loading the dataset in memory":                                                false,
		"TRYAGAIN": false,
	} {
		if got := isSlotMigrationUnsupported(msg); got != want {
			t.Fatalf("%q: got %v, want %v", msg, got, want)
		}
	}
}

// TestSlotMigrationMonitorServerSupport checks which nodes are polled: only Valkey 9.0+ servers, including over
// RESP2 where the version comes from HELLO. A node that rejects the command, because it does not have it or the
// ACL user may not run it, is asked again after slotMigrationRecheck, not on MOVED replies in between, and polled
// as usual once it accepts it.
func TestSlotMigrationMonitorServerSupport(t *testing.T) {
	running := NewResult(slicemsg(typeArray, []ValkeyMessage{ // RESP2 flat array
		migEntry(false, "name", "j", "operation", "EXPORT", "slot_ranges", "0-10", "state", "snapshotting"),
	}), nil)
	noperm := NewResult(strmsg(typeSimpleErr, "NOPERM User app has no permissions to run the 'cluster|getslotmigrations' command"), nil)
	tests := []struct {
		name      string
		version   int    // conn.Version(), which is 5 on every RESP2 connection
		hello     string // server version reported by HELLO, if any
		reply     ValkeyResult
		wantPolls int           // over three ticks, slotMigrationRecheck/2 apart
		wantDelay time.Duration // after the first tick
	}{
		{name: "pre-9.0 server", version: 8, reply: migReply(), wantDelay: defaultSlotMigrationIdleInterval},
		{name: "unknown version", version: 0, reply: migReply(), wantDelay: defaultSlotMigrationIdleInterval},
		{name: "Valkey 9 over RESP2", version: 5, hello: "9.0.6", reply: running, wantPolls: 3, wantDelay: defaultSlotMigrationActiveInterval},
		{name: "command not available", version: 9, reply: NewResult(strmsg(typeSimpleErr, "ERR unknown subcommand 'GETSLOTMIGRATIONS'. Try CLUSTER HELP."), nil), wantPolls: 2, wantDelay: defaultSlotMigrationIdleInterval},
		{name: "NOPERM", version: 9, reply: noperm, wantPolls: 2, wantDelay: defaultSlotMigrationIdleInterval},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			n := newScriptedConn("127.0.0.1:1", tt.version)
			if tt.hello != "" {
				n.InfoFn = func() map[string]ValkeyMessage {
					return map[string]ValkeyMessage{"version": strmsg(typeBlobString, tt.hello)}
				}
			}
			n.set(tt.reply)
			m, now, _ := newTestMonitor(t, ClusterOption{}, n)
			for i := 0; i < 3; i++ {
				d, ok := m.tick()
				if !ok {
					t.Fatal("monitor stopped unexpectedly")
				}
				if i == 0 && d != tt.wantDelay {
					t.Fatalf("delay after the first poll = %v, want %v", d, tt.wantDelay)
				}
				*now = now.Add(slotMigrationRecheck / 2)
			}
			if p, _ := n.counts(); p != tt.wantPolls {
				t.Fatalf("polls = %d, want %d", p, tt.wantPolls)
			}
		})
	}

	// a node that rejected the command is not polled on MOVED either; once the ACL user may run the command, the
	// node is polled as usual
	n := newScriptedConn("127.0.0.1:1", 9)
	n.set(noperm)
	m, now, _ := newTestMonitor(t, ClusterOption{}, n)
	m.tick()
	if m.wake(n.Addr()) {
		t.Fatal("a MOVED reply should not poll a node that rejected the command before the recheck")
	}
	n.set(running)
	*now = now.Add(slotMigrationRecheck)
	if d, _ := m.tick(); d != m.active || m.nodes[n].unsupported {
		t.Fatalf("delay = %v, unsupported = %v; want the active cadence", d, m.nodes[n].unsupported)
	}
}

// ---------- target side, MOVED wake, deduplication, concurrency and shutdown ----------

func importEntry(name, state, slots string) ValkeyMessage {
	return migEntry(true, "name", name, "operation", "IMPORT", "slot_ranges", slots, "state", state)
}

func exportDone(src conn) slotMigrationCompletion {
	return slotMigrationCompletion{reporter: src, migration: slotMigration{name: "j", operation: "EXPORT", state: "success", slots: "0-99"}}
}

func TestSlotMigrationMonitorMovedWake(t *testing.T) {
	src := newScriptedConn("127.0.0.1:1", 9)
	dst := newScriptedConn("127.0.0.2:2", 9)
	m, now, calls := newTestMonitor(t, ClusterOption{}, src, dst)
	m.tick() // baseline; both nodes idle until now+idle
	if d := m.delay(*now); d != m.idle {
		t.Fatalf("delay = %v, want %v", d, m.idle)
	}
	if d := m.delay(now.Add(2 * m.idle)); d != 0 {
		t.Fatalf("an overdue poll should be due immediately, delay = %v", d)
	}

	// a job ran between two idle polls; the first MOVED names the target, which already reports success
	*now = now.Add(time.Second)
	dst.set(migReply(importEntry("j", "success", "0-99")))
	if !m.wake("127.0.0.2:2") {
		t.Fatal("the MOVED target should be scheduled")
	}
	if d := m.delay(*now); d != 0 {
		t.Fatalf("the MOVED target should be due now, delay = %v", d)
	}
	m.tick()
	if len(*calls) != 1 || (*calls)[0].completed[0].reporter != dst {
		t.Fatalf("expected a refresh from the target, got %+v", *calls)
	}
	if p, _ := src.counts(); p != 1 {
		t.Fatalf("only the MOVED target should be polled early, source polled %d times", p)
	}

	// nodes that are not monitored primaries are ignored: unknown addresses, and known nodes that own no slots
	if m.wake("10.0.0.9:9") {
		t.Fatal("unknown address should be ignored")
	}
	m.c.mu.Lock()
	m.c.conns["127.0.0.3:3"] = connrole{conn: newScriptedConn("127.0.0.3:3", 9)}
	m.c.mu.Unlock()
	if m.wake("127.0.0.3:3") {
		t.Fatal("a node that owns no slots should be ignored")
	}
	m.nodes[src].unsupported = true
	if m.wake("127.0.0.1:1") {
		t.Fatal("unsupported node should be ignored")
	}
	// a node whose poll is pending is checked at the finalizing interval; MOVED replies must not add ticks
	m.nodes[dst].pending = true
	*now = now.Add(time.Hour)
	if m.wake("127.0.0.2:2") {
		t.Fatal("a node with a pending poll should not be woken")
	}
}

func TestSlotMigrationMonitorMovedWakeRateLimit(t *testing.T) {
	src := newScriptedConn("127.0.0.1:1", 9)
	dst := newScriptedConn("127.0.0.2:2", 9)
	m, now, _ := newTestMonitor(t, ClusterOption{}, src, dst)
	m.tick() // baseline
	polled := *now

	// no known migration: a burst of MOVED replies polls the node at most once per active interval
	*now = now.Add(10 * time.Millisecond)
	if !m.wake("127.0.0.2:2") {
		t.Fatal("expected the node to be scheduled")
	}
	if got := m.nodes[dst].next; !got.Equal(polled.Add(m.active)) {
		t.Fatalf("next poll = %v after the previous one, want %v", got.Sub(polled), m.active)
	}
	if m.wake("127.0.0.2:2") {
		t.Fatal("a second MOVED must not move the poll earlier")
	}

	// while a migration is known to run, the finalizing interval applies
	m, now, _ = newTestMonitor(t, ClusterOption{}, src, dst)
	src.set(migReply(exportEntry("j", "snapshotting", "0-99")))
	m.tick() // first poll of both nodes; the source reports a running job
	polled = *now
	*now = now.Add(5 * time.Millisecond)
	if !m.wake("127.0.0.2:2") {
		t.Fatal("expected the node to be scheduled")
	}
	if got := m.nodes[dst].next; !got.Equal(polled.Add(m.finalizing)) {
		t.Fatalf("next poll = %v after the previous one, want %v", got.Sub(polled), m.finalizing)
	}
}

func TestClusterClientMovedNotifiesSlotMigrationMonitor(t *testing.T) {
	src := newScriptedConn("127.0.0.1:1", 9)
	dst := newScriptedConn("127.0.0.2:2", 9)
	src.setTopology(topologyBefore)
	dst.setTopology(topologyBefore)
	client := newRefreshTestClient(t, ClusterOption{}, src, dst)
	client.migMoved = make(chan string, 1) // as if the monitor were enabled, without its goroutine

	moved := NewResult(strmsg('-', "MOVED 0 127.0.0.2:2"), nil).Error()
	ask := NewResult(strmsg('-', "ASK 0 127.0.0.2:2"), nil).Error()
	if _, mode := client.shouldRefreshRetry(ask, context.Background()); mode != RedirectAsk || len(client.migMoved) != 0 {
		t.Fatalf("ASK must not wake the monitor, mode %v queued %d", mode, len(client.migMoved))
	}
	if _, mode := client.shouldRefreshRetry(moved, context.Background()); mode != RedirectMove {
		t.Fatalf("unexpected mode %v", mode)
	}
	if got := <-client.migMoved; got != "127.0.0.2:2" {
		t.Fatalf("monitor got %q, want the MOVED target", got)
	}
	// a full queue never blocks the command path
	done := make(chan struct{})
	go func() {
		for i := 0; i < 3; i++ {
			client.shouldRefreshRetry(moved, context.Background())
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("shouldRefreshRetry blocked on a full monitor queue")
	}
}

func TestSlotMigrationMonitorOneRefreshPerJob(t *testing.T) {
	src := newScriptedConn("127.0.0.1:1", 9)
	dst := newScriptedConn("127.0.0.2:2", 9)
	m, now, calls := newTestMonitor(t, ClusterOption{}, src, dst)
	m.tick() // baseline

	// both sides report the same job finishing in the same poll
	*now = now.Add(m.idle)
	src.set(migReply(exportEntry("j", "success", "0-99")))
	dst.set(migReply(importEntry("j", "success", "0-99")))
	m.tick()
	if len(*calls) != 1 || len((*calls)[0].completed) != 1 {
		t.Fatalf("expected one completion for the job, got %+v", *calls)
	}

	// the source reports a second job first, the target only on a later poll
	*now = now.Add(m.idle)
	src.set(migReply(exportEntry("j", "success", "0-99"), exportEntry("k", "success", "100")))
	m.tick()
	*now = now.Add(m.idle)
	dst.set(migReply(importEntry("j", "success", "0-99"), importEntry("k", "success", "100")))
	m.tick()
	if len(*calls) != 2 {
		t.Fatalf("expected exactly one refresh per job, got %d", len(*calls))
	}

	// once the target's success has been acted on, the source, still reporting the handover, leaves the
	// finalizing cadence
	*now = now.Add(m.idle)
	src.set(migReply(exportEntry("l", "failover-granted", "200")))
	dst.set(migReply(importEntry("l", "success", "200")))
	m.tick()
	*now = now.Add(m.finalizing)
	m.tick()
	if got := m.nodes[src].next.Sub(*now); len(*calls) != 3 || got != m.active {
		t.Fatalf("the source of a handled job should be polled at the active cadence, next poll in %v, %d refreshes", got, len(*calls))
	}
}

func TestJitterSlotMigrationInterval(t *testing.T) {
	for _, d := range []time.Duration{100 * time.Millisecond, time.Second, 30 * time.Second, 24 * time.Hour} {
		lo, hi := d-d/5, d+d/5
		var below, above bool
		for i := 0; i < 1000; i++ {
			got := jitterSlotMigrationInterval(d)
			if got < lo || got > hi {
				t.Fatalf("jitter(%v) = %v, want within [%v, %v]", d, got, lo, hi)
			}
			below, above = below || got < d, above || got > d
		}
		if !below || !above {
			t.Fatalf("jitter(%v) should spread the polls on both sides of the interval", d)
		}
	}
	if got := jitterSlotMigrationInterval(3); got != 3 {
		t.Fatalf("tiny intervals are returned unchanged, got %v", got)
	}
}

// TestSlotMigrationMonitorBoundedPolls checks that a tick sends at most slotMigrationMaxConcurrentPolls polls, the
// next tick following at once, and returns as soon as they have answered, and that a node that does not answer
// holds up the others for at most slotMigrationPollTimeout: it is not polled again before its poll answers, being
// checked at the finalizing interval in the meantime, its late answer is used, and a poll without an answer is
// cancelled.
func TestSlotMigrationMonitorBoundedPolls(t *testing.T) {
	var running, peak, total int32
	owners := make([]conn, 40)
	for i := range owners {
		c := newScriptedConn(fmt.Sprintf("127.0.%d.1:1", i), 9)
		c.DoFn = func(cmd Completed) ValkeyResult {
			atomic.AddInt32(&total, 1)
			n := atomic.AddInt32(&running, 1)
			for {
				p := atomic.LoadInt32(&peak)
				if n <= p || atomic.CompareAndSwapInt32(&peak, p, n) {
					break
				}
			}
			time.Sleep(5 * time.Millisecond)
			atomic.AddInt32(&running, -1)
			return migReply()
		}
		owners[i] = c
	}
	m, _, _ := newTestMonitor(t, ClusterOption{}, owners...)
	start := time.Now()
	if d, _ := m.tick(); d != 0 {
		t.Fatalf("the primaries left over by a tick should be polled at once, delay = %v", d)
	}
	if d := time.Since(start); d >= slotMigrationPollTimeout {
		t.Fatalf("a tick whose polls have all answered should return at once, it took %v", d)
	}
	m.tick()
	m.tick()
	if p := atomic.LoadInt32(&peak); p > slotMigrationMaxConcurrentPolls || p < 2 {
		t.Fatalf("expected concurrent polls capped at %d, peak %d", slotMigrationMaxConcurrentPolls, p)
	}
	if n := atomic.LoadInt32(&total); n != 40 || len(m.nodes) != 40 {
		t.Fatalf("expected each of the 40 primaries to be polled once in three ticks, got %d polls of %d nodes", n, len(m.nodes))
	}

	// reading the version of the slow node waits, as it does while its connection is being established
	release := make(chan struct{})
	var releaseOnce sync.Once
	defer releaseOnce.Do(func() { close(release) })
	var versionReads int32
	slow := newScriptedConn("127.0.0.1:1", 9)
	slow.InfoFn = func() map[string]ValkeyMessage {
		atomic.AddInt32(&versionReads, 1)
		<-release
		return nil
	}
	fast := newScriptedConn("127.0.0.2:2", 9)
	fast.set(migReply(exportEntry("j", "snapshotting", "0-99")))
	m, now, _ := newTestMonitor(t, ClusterOption{}, slow, fast)
	start = time.Now()
	m.tick()
	if d := time.Since(start); d > 4*slotMigrationPollTimeout {
		t.Fatalf("a node that does not answer held up the poll for %v", d)
	}
	if m.nodes[fast].phase != slotMigrationPhaseActive || m.nodes[slow].errors != 1 {
		t.Fatalf("the other node should be observed and the late one counted as failed, got %v and %d errors", m.nodes[fast].phase, m.nodes[slow].errors)
	}
	*now = now.Add(m.idle)
	if d, _ := m.tick(); d != m.finalizing {
		t.Fatalf("a node whose poll is pending should be checked again at the finalizing interval, delay = %v", d)
	}
	if n := atomic.LoadInt32(&versionReads); n != 1 || m.nodes[slow].errors != 1 {
		t.Fatalf("a node must not be polled again before its previous poll answers, got %d polls and %d errors", n, m.nodes[slow].errors)
	}
	releaseOnce.Do(func() { close(release) })
	waitAnswer := func(n *slotMigrationNode) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); len(n.answer) == 0; time.Sleep(time.Millisecond) {
			if time.Now().After(deadline) {
				t.Fatal("the late poll did not answer")
			}
		}
	}
	waitAnswer(m.nodes[slow])
	*now = now.Add(m.finalizing)
	m.tick()
	if n := atomic.LoadInt32(&versionReads); n != 1 || m.nodes[slow].errors != 0 || m.nodes[slow].pending {
		t.Fatalf("the late answer should be used instead of a new poll, got %d polls and %d errors", n, m.nodes[slow].errors)
	}
	*now = now.Add(m.idle)
	m.tick()
	if n := atomic.LoadInt32(&versionReads); n != 2 {
		t.Fatalf("the node should be polled as usual afterwards, got %d polls", n)
	}

	// a node that is slow to answer: its poll is cancelled after slotMigrationPollCancel, through a context without
	// a deadline, because a deadline would close the connection the poll shares with the application (see pollNode)
	busy := &stallConn{scriptedConn: newScriptedConn("127.0.0.3:3", 9), stall: "poll", started: make(chan struct{}), release: make(chan struct{})}
	busy.armed.Store(true)
	m, now, _ = newTestMonitor(t, ClusterOption{}, busy)
	start = time.Now()
	m.tick()
	if d := time.Since(start); d < slotMigrationPollTimeout || d > 4*slotMigrationPollTimeout {
		t.Fatalf("a slow node should be waited for the poll timeout, waited %v", d)
	}
	if busy.deadline.Load() {
		t.Fatal("the poll context must not have a deadline")
	}
	waitAnswer(m.nodes[busy])
	if d := time.Since(start); d < slotMigrationPollCancel {
		t.Fatalf("the late poll was cancelled after %v, before %v", d, slotMigrationPollCancel)
	}
	*now = now.Add(m.finalizing)
	m.tick()
	if m.nodes[busy].errors != 2 || m.nodes[busy].pending {
		t.Fatalf("the cancelled poll should count as the second failure, without a new poll, got %d errors", m.nodes[busy].errors)
	}
}

func TestSlotMigrationMonitorPrunesHandledJobs(t *testing.T) {
	src := newScriptedConn("127.0.0.1:1", 9)
	m, now, _ := newTestMonitor(t, ClusterOption{}, src)
	m.handled["old"] = *now
	m.handled["recent"] = now.Add(time.Minute)
	*now = now.Add(slotMigrationHandledRetention + time.Second)
	m.tick()
	if _, ok := m.handled["old"]; ok {
		t.Fatal("a job acted on should be forgotten after the retention period")
	}
	if _, ok := m.handled["recent"]; !ok {
		t.Fatal("a job acted on within the retention period must stay handled")
	}
}

// stallConn holds one kind of request of the monitor once armed: the poll until its context is done, or the
// version check or the topology request of a refresh until release is closed.
type stallConn struct {
	*scriptedConn
	stall    string // "poll", "version" or "refresh"
	armed    atomic.Bool
	deadline atomic.Bool // a held poll had a context deadline
	started  chan struct{}
	once     sync.Once
	release  chan struct{}
}

func (c *stallConn) held(kind string) bool {
	if c.stall != kind || !c.armed.Load() {
		return false
	}
	c.once.Do(func() { close(c.started) })
	return true
}

func (c *stallConn) Info() map[string]ValkeyMessage {
	if c.held("version") {
		<-c.release
	}
	return c.scriptedConn.Info()
}

func (c *stallConn) Do(ctx context.Context, cmd Completed) ValkeyResult {
	if isCmd(cmd, "CLUSTER", "GETSLOTMIGRATIONS") && c.held("poll") {
		_, ok := ctx.Deadline()
		c.deadline.Store(ok)
		<-ctx.Done()
		return NewErrorResult(ctx.Err())
	}
	if (isCmd(cmd, "CLUSTER", "SHARDS") || isCmd(cmd, "CLUSTER", "SLOTS")) && c.held("refresh") {
		<-c.release
	}
	return c.scriptedConn.Do(ctx, cmd)
}

// TestSlotMigrationCloseDoesNotWait checks that closing the client stops the monitor at once, whether it is waiting
// for a poll, for a version check that waits for a connection, or for a refresh.
func TestSlotMigrationCloseDoesNotWait(t *testing.T) {
	for _, stall := range []string{"poll", "version", "refresh"} {
		t.Run(stall, func(t *testing.T) {
			src := newScriptedConn("127.0.0.1:1", 9)
			dst := &stallConn{scriptedConn: newScriptedConn("127.0.0.2:2", 9), stall: stall, started: make(chan struct{}), release: make(chan struct{})}
			defer close(dst.release)
			src.setTopology(topologyBefore)
			dst.setTopology(topologyBefore)
			client, err := newClusterClient(
				&ClientOption{InitAddress: []string{src.Addr()}, ClusterOption: ClusterOption{
					SlotMigrationAwareness:          true,
					SlotMigrationIdleInterval:       5 * time.Millisecond,
					SlotMigrationActiveInterval:     2 * time.Millisecond,
					SlotMigrationFinalizingInterval: time.Millisecond,
				}},
				func(addr string, _ *ClientOption) conn {
					if addr == dst.Addr() {
						return dst
					}
					return src
				},
				newRetryer(defaultRetryDelayFn),
			)
			if err != nil {
				t.Fatalf("unexpected err %v", err)
			}
			deadline := time.Now().Add(5 * time.Second)
			for p, _ := dst.counts(); p < 1; p, _ = dst.counts() { // the first poll records the node's log
				if time.Now().After(deadline) {
					t.Fatal("the monitor did not poll")
				}
				time.Sleep(time.Millisecond)
			}
			if stall == "refresh" {
				dst.set(migReply(importEntry("j", "success", "0-99"))) // the next poll refreshes the topology from dst
			}
			dst.armed.Store(true)
			select {
			case <-dst.started:
			case <-time.After(5 * time.Second):
				t.Fatal("the monitor did not reach the held request")
			}
			start := time.Now()
			client.Close()
			if d := time.Since(start); d > slotMigrationPollTimeout/2 {
				t.Fatalf("Close waited %v for the monitor", d)
			}
			select {
			case <-client.migDone:
			default:
				t.Fatal("the monitor goroutine must have exited when Close returns")
			}
			client.Close() // a second Close must not block or panic
		})
	}
}

// TestSlotMigrationAwarenessNoGoroutineLeak runs the monitor through polls, a MOVED wake and a refresh. It checks
// that a MOVED reply makes the running monitor poll the node it names at once, and that closing the client leaves
// no goroutine behind.
func TestSlotMigrationAwarenessNoGoroutineLeak(t *testing.T) {
	defer ShouldNotLeak(SetupLeakDetection())
	src := newScriptedConn("127.0.0.1:1", 9)
	dst := newScriptedConn("127.0.0.2:2", 9)
	src.setTopology(topologyBefore)
	dst.setTopology(topologyBefore)
	client := newRefreshTestClient(t, ClusterOption{
		SlotMigrationAwareness:          true,
		SlotMigrationIdleInterval:       time.Hour, // after its first poll, only a MOVED wake polls the node again
		SlotMigrationActiveInterval:     2 * time.Millisecond,
		SlotMigrationFinalizingInterval: time.Millisecond,
	}, src, dst)
	defer client.Close() // runs before the deferred leak check, also when the test fails
	waitPolls := func(want int, msg string) {
		t.Helper()
		for deadline := time.Now().Add(5 * time.Second); ; time.Sleep(time.Millisecond) {
			if p, _ := dst.counts(); p >= want {
				return
			}
			if time.Now().After(deadline) {
				t.Fatal(msg)
			}
		}
	}
	waitPolls(1, "monitor did not poll")
	src.setTopology(topologyAfter) // after the handover both nodes report the new owner
	dst.setTopology(topologyAfter)
	dst.set(migReply(importEntry("j", "success", "0-99")))
	client.shouldRefreshRetry(NewResult(strmsg('-', "MOVED 0 127.0.0.2:2"), nil).Error(), context.Background())
	waitPolls(2, "the MOVED reply should make the monitor poll the node it names at once")
	for deadline := time.Now().Add(5 * time.Second); client._pick(0, false) != dst; time.Sleep(time.Millisecond) {
		if time.Now().After(deadline) {
			t.Fatal("slot map did not follow the migration")
		}
	}
}

// ---------- slot learning from MOVED ----------

func newMovedTestClient(t *testing.T, owner, target *mockConn, release chan struct{}) *clusterClient {
	t.Helper()
	var refreshed int32
	topology := func(cmd Completed) ValkeyResult {
		// the first refresh builds the slot map; later (lazy) refreshes are held until the test
		// is done so they cannot overwrite the slot map while the test inspects it.
		if atomic.AddInt32(&refreshed, 1) > 1 {
			<-release
		}
		return slotsMultiResp
	}
	owner.DoOverride = map[string]func(cmd Completed) ValkeyResult{"CLUSTER SLOTS": topology}
	target.DoOverride = map[string]func(cmd Completed) ValkeyResult{"CLUSTER SLOTS": topology}
	other := &mockConn{DoOverride: map[string]func(cmd Completed) ValkeyResult{"CLUSTER SLOTS": topology}}
	client, err := newClusterClient(
		&ClientOption{InitAddress: []string{"127.0.0.1:0"}},
		func(dst string, opt *ClientOption) conn {
			switch dst {
			case "127.0.0.1:0":
				return owner
			case "127.0.2.1:0":
				return target
			}
			return other
		},
		newRetryer(defaultRetryDelayFn),
	)
	if err != nil {
		t.Fatalf("unexpected err %v", err)
	}
	t.Cleanup(func() {
		close(release)
		client.Close()
	})
	return client
}

func TestClusterClientMovedToExistingConnUpdatesSlot(t *testing.T) {
	var ownerCalls, targetCalls int32
	owner := &mockConn{DoFn: func(cmd Completed) ValkeyResult {
		atomic.AddInt32(&ownerCalls, 1)
		return NewResult(strmsg('-', "MOVED 5061 127.0.2.1:0"), nil)
	}}
	target := &mockConn{DoFn: func(cmd Completed) ValkeyResult {
		atomic.AddInt32(&targetCalls, 1)
		return NewResult(strmsg('+', "OK"), nil)
	}}
	client := newMovedTestClient(t, owner, target, make(chan struct{}))

	get := client.B().Get().Key("bar").Build()
	if get.Slot() != 5061 {
		t.Fatalf("test key must hash to slot 5061, got %d", get.Slot())
	}
	if client._pick(5061, false) != owner {
		t.Fatal("slot 5061 should start on 127.0.0.1:0")
	}

	// the first command is redirected; the conn to the new owner already exists
	if v, err := client.Do(context.Background(), get).ToString(); err != nil || v != "OK" {
		t.Fatalf("unexpected %v %v", v, err)
	}
	if client._pick(5061, false) != target {
		t.Fatal("MOVED to an existing conn should update the slot owner")
	}
	if client._pick(5062, false) != owner {
		t.Fatal("other slots must keep their owner")
	}

	// the following commands go to the new owner directly
	for i := 0; i < 10; i++ {
		if v, err := client.Do(context.Background(), client.B().Get().Key("bar").Build()).ToString(); err != nil || v != "OK" {
			t.Fatalf("unexpected %v %v", v, err)
		}
	}
	if n := atomic.LoadInt32(&ownerCalls); n != 1 {
		t.Fatalf("old owner should be asked once, got %d", n)
	}
	if n := atomic.LoadInt32(&targetCalls); n != 11 {
		t.Fatalf("new owner should serve all 11 commands, got %d", n)
	}

	// with ReplicaOnly the slot map points to replicas, so MOVED, which names the primary, must not replace them
	client.opt.ReplicaOnly = true
	client.mu.Lock()
	client.wslots[5061] = owner
	client.mu.Unlock()
	if v, err := client.Do(context.Background(), client.B().Get().Key("bar").Build()).ToString(); err != nil || v != "OK" {
		t.Fatalf("unexpected %v %v", v, err)
	}
	if client._pick(5061, false) != owner {
		t.Fatal("with ReplicaOnly, MOVED must not replace the replica route of the slot")
	}
}

func TestClusterClientAskToExistingConnKeepsSlot(t *testing.T) {
	owner := &mockConn{DoFn: func(cmd Completed) ValkeyResult {
		return NewResult(strmsg('-', "ASK 5061 127.0.2.1:0"), nil)
	}}
	target := &mockConn{DoMultiFn: func(multi ...Completed) *valkeyresults {
		return &valkeyresults{s: []ValkeyResult{NewResult(strmsg('+', "OK"), nil), NewResult(strmsg('+', "OK"), nil)}}
	}}
	client := newMovedTestClient(t, owner, target, make(chan struct{}))

	if v, err := client.Do(context.Background(), client.B().Get().Key("bar").Build()).ToString(); err != nil || v != "OK" {
		t.Fatalf("unexpected %v %v", v, err)
	}
	if client._pick(5061, false) != owner {
		t.Fatal("ASK is temporary and must not change the slot owner")
	}
}
