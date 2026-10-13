package valkey

import (
	"context"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go/internal/cmds"
)

// Atomic Slot Migration (ASM) awareness for Valkey 9.0+.
//
// During an atomic slot migration the source primary keeps owning and serving the slots while
// they are replicated to the target. The job then enters its finalization phase: the source
// pauses writes, the target catches up and takes ownership, and both sides report success.
// Commands sent to the source after that point are answered with MOVED.
//
// slotMigrationMonitor follows those jobs through CLUSTER GETSLOTMIGRATIONS on the primaries that
// own slots. Each primary is polled at a cadence that matches the phase of its jobs, with jitter.
// Both sides of a job in its finalization phase are polled at the fastest cadence: the target
// takes ownership first and reports success before the source does. A MOVED reply also makes the
// monitor poll the node it points to, the new owner, without waiting for its next scheduled poll,
// so a finalization phase shorter than the polling interval is not missed under traffic. A target
// that owns no slots yet, such as a primary added to scale out, is followed only once it owns some.
// When a job reports success, the topology is refreshed from the node that reported it: the source
// (EXPORT) or the target (IMPORT). Both already know the new owner, so the refresh does not depend
// on gossip propagation, and the IMPORT side still reports success if the source primary fails
// right after the handover. Commands are never delayed: a MOVED reply only notifies the monitor.

const (
	defaultSlotMigrationIdleInterval       = 30 * time.Second
	defaultSlotMigrationActiveInterval     = 250 * time.Millisecond
	defaultSlotMigrationFinalizingInterval = 20 * time.Millisecond

	// slotMigrationCooldown keeps a primary at the active cadence after its last active job, because
	// rebalancing usually runs many jobs back to back.
	slotMigrationCooldown = time.Minute
	// slotMigrationJitter spreads polls of many clients by ±20% of the interval.
	slotMigrationJitter = 5 // interval / 5 = 20%

	// slotMigrationPollTimeout bounds the wait for the polls of a tick, and so how long a node that does not answer
	// can hold up the polls of the others. A late poll goes on in the background, and a later tick uses its answer.
	slotMigrationPollTimeout = 250 * time.Millisecond
	// slotMigrationPollCancel is when a poll that has not answered is cancelled. The node is not polled again
	// before its poll has answered or been cancelled.
	slotMigrationPollCancel = time.Second
	// slotMigrationMaxConcurrentPolls bounds the polls of a tick. Nodes that are due beyond it are polled by the
	// next tick, which follows at once.
	slotMigrationMaxConcurrentPolls = 16
	slotMigrationRefreshTimeout     = 3 * time.Second
	slotMigrationRefreshAttempts    = 3
	slotMigrationHandledRetention   = 10 * time.Minute
	slotMigrationMinVersion         = 9
	// slotMigrationMaxErrors consecutive failed polls move a node one phase down, from finalizing to active and then
	// to idle, so a primary that stops answering during a migration is not polled at a fast cadence while it stays in
	// the slot map. A poll that is late and then cancelled counts as two failures.
	slotMigrationMaxErrors = 3
	// slotMigrationRecheck is how long a node that rejects CLUSTER GETSLOTMIGRATIONS, because the server does
	// not have it or the ACL user may not run it, is left alone before it is asked again.
	slotMigrationRecheck = 10 * time.Minute
	// slotMigrationWakeBuffer bounds the MOVED targets queued for the monitor; more are dropped, because a
	// burst of MOVED replies names the same few nodes.
	slotMigrationWakeBuffer = 16
)

// CLUSTER GETSLOTMIGRATIONS values, see https://valkey.io/commands/cluster-getslotmigrations/
const (
	slotMigrationExport = "EXPORT"
	slotMigrationImport = "IMPORT"

	slotMigrationStateSuccess   = "success"
	slotMigrationStateFailed    = "failed"
	slotMigrationStateCancelled = "cancelled"

	// finalization phase on the source (EXPORT)
	slotMigrationStateWaitingToPause = "waiting-to-pause"
	slotMigrationStateFailoverPaused = "failover-paused"
	// finalization phase on the target (IMPORT)
	slotMigrationStateWaitingForPaused  = "waiting-for-paused"
	slotMigrationStateFailoverRequested = "failover-requested"
	// last finalization state on both sides, until the target takes over the slots
	slotMigrationStateFailoverGranted = "failover-granted"
)

type slotMigrationPhase int

const (
	slotMigrationPhaseIdle       slotMigrationPhase = iota // no running job
	slotMigrationPhaseActive                               // a job is running
	slotMigrationPhaseFinalizing                           // a job is about to hand over slot ownership
)

// slotMigration is an entry of CLUSTER GETSLOTMIGRATIONS.
type slotMigration struct {
	name      string
	operation string // EXPORT on the source primary, IMPORT on the target primary
	state     string
	slots     string // slot_ranges, for example "0-10 20-30"; see parseSlotMigrationRanges
}

// phase returns the polling phase of the job: finalizing while it is about to hand over its slots, active while it
// runs, and idle once it has finished.
func (s slotMigration) phase() slotMigrationPhase {
	switch s.state {
	case slotMigrationStateSuccess, slotMigrationStateFailed, slotMigrationStateCancelled:
		return slotMigrationPhaseIdle
	case slotMigrationStateFailoverGranted:
		return slotMigrationPhaseFinalizing
	case slotMigrationStateWaitingToPause, slotMigrationStateFailoverPaused:
		if s.operation == slotMigrationExport {
			return slotMigrationPhaseFinalizing
		}
	case slotMigrationStateWaitingForPaused, slotMigrationStateFailoverRequested:
		if s.operation == slotMigrationImport {
			return slotMigrationPhaseFinalizing
		}
	}
	return slotMigrationPhaseActive // includes unknown future states
}

// parseSlotMigrations extracts EXPORT and IMPORT entries from a RESP2 or RESP3 CLUSTER GETSLOTMIGRATIONS reply.
// Every reply holds the whole migration log, which keeps up to cluster-slot-migration-log-max-len finished jobs
// (1000 by default), so the entries are read in place, and fields other than the four used here, such as those
// newer servers add, are skipped.
func parseSlotMigrations(reply ValkeyMessage) ([]slotMigration, error) {
	entries, err := reply.ToArray()
	if err != nil {
		return nil, err
	}
	migrations := make([]slotMigration, 0, len(entries))
	for i := range entries {
		// keys and values alternate, in RESP3 maps and RESP2 arrays alike; an entry that is neither has no fields,
		// and so no name
		fields := entries[i].values()
		if len(fields)%2 != 0 {
			continue
		}
		var m slotMigration
		for j := 0; j < len(fields); j += 2 {
			switch fields[j].string() {
			case "name":
				m.name = fields[j+1].string()
			case "operation":
				m.operation = fields[j+1].string()
			case "state":
				m.state = fields[j+1].string()
			case "slot_ranges":
				m.slots = fields[j+1].string()
			}
		}
		if m.name == "" || (m.operation != slotMigrationExport && m.operation != slotMigrationImport) {
			continue
		}
		migrations = append(migrations, m)
	}
	return migrations, nil
}

// parseSlotMigrationRanges parses "start-end" ranges joined by spaces, for example "0-10 20-30".
func parseSlotMigrationRanges(s string) (ranges [][2]int64) {
	for _, part := range strings.Fields(s) {
		lo, hi, found := strings.Cut(part, "-")
		if !found {
			hi = lo
		}
		start, err1 := strconv.ParseInt(lo, 10, 64)
		end, err2 := strconv.ParseInt(hi, 10, 64)
		if err1 != nil || err2 != nil || start < 0 || end >= 16384 || start > end {
			continue
		}
		ranges = append(ranges, [2]int64{start, end})
	}
	return ranges
}

// serverMajorVersion returns the major server version reported by HELLO. conn.Version() is fixed to 5 on
// RESP2 connections (AlwaysRESP2 or servers without RESP3), so the HELLO reply is preferred when present.
// It returns 0 when the version is not known, for example while the connection is broken.
func serverMajorVersion(cc conn) int {
	if v, ok := cc.Info()["version"]; ok {
		if s, err := v.ToString(); err == nil {
			major, _, _ := strings.Cut(s, ".")
			if n, err := strconv.Atoi(major); err == nil {
				return n
			}
		}
	}
	return cc.Version()
}

// isSlotMigrationUnsupported reports whether an error means the node does not have CLUSTER GETSLOTMIGRATIONS:
// a server older than Valkey 9.0, or one where the command is renamed or disabled.
func isSlotMigrationUnsupported(msg string) bool {
	lower := strings.ToLower(msg)
	return strings.Contains(lower, "unknown subcommand") || strings.Contains(lower, "unknown command")
}

// slotMigrationNode is the monitor state of one primary.
type slotMigrationNode struct {
	conn        conn
	next        time.Time
	last        time.Time // last poll
	lastActive  time.Time
	seen        map[string]string // job name -> last observed state; nil until the first poll
	spare       map[string]string // the map seen replaced, emptied and reused by the next poll
	phase       slotMigrationPhase
	errors      int                    // consecutive failed polls
	unsupported bool                   // the node rejects the command; it is asked again after slotMigrationRecheck
	pending     bool                   // a poll did not answer in time; it delivers its answer to answer
	answer      chan slotMigrationPoll // receives the outcome of each poll; a node has at most one poll at a time
}

// observe records the jobs of one poll and returns the jobs that reached success since the previous poll. The
// node's phase becomes the most urgent phase among its jobs that are not in handled: once a job has been acted
// on, its source, which keeps reporting the handover for a few hundred milliseconds after the target's success,
// no longer needs the fastest cadence. Jobs found on the first poll of a node are only recorded, because
// CLUSTER GETSLOTMIGRATIONS keeps finished jobs in its log.
func (n *slotMigrationNode) observe(migrations []slotMigration, handled map[string]time.Time) (completed []slotMigration) {
	seen := n.spare // emptied after the previous poll
	if seen == nil {
		seen = make(map[string]string, len(migrations))
	}
	phase := slotMigrationPhaseIdle
	for _, m := range migrations {
		prev, known := n.seen[m.name]
		seen[m.name] = m.state
		if m.state == slotMigrationStateSuccess && n.seen != nil && (!known || prev != slotMigrationStateSuccess) {
			completed = append(completed, m)
		}
		if p := m.phase(); p > phase {
			if _, done := handled[m.name]; !done {
				phase = p
			}
		}
	}
	n.seen, n.spare = seen, n.seen // jobs that dropped out of the server log are forgotten
	clear(n.spare)
	n.phase = phase
	return completed
}

// cadence is the phase that decides the next poll interval, including the cooldown after recent activity.
func (n *slotMigrationNode) cadence(now time.Time) slotMigrationPhase {
	if n.phase == slotMigrationPhaseIdle && !n.lastActive.IsZero() && now.Sub(n.lastActive) < slotMigrationCooldown {
		return slotMigrationPhaseActive
	}
	return n.phase
}

// slotMigrationPoll is the outcome of polling one node.
type slotMigrationPoll struct {
	result   ValkeyResult
	skipped  bool // the node is older than Valkey 9.0, or its version is not known yet
	answered bool // false when the node did not answer within slotMigrationPollTimeout; see slotMigrationNode.pending
}

// slotMigrationCompletion is a job that reached success, with the node that reported it.
type slotMigrationCompletion struct {
	reporter  conn
	migration slotMigration
}

type slotMigrationMonitor struct {
	c          *clusterClient
	ctx        context.Context
	nodes      map[conn]*slotMigrationNode
	owners     []conn               // the primaries found by the last scan of the slot map, in slot order
	scanned    time.Time            // when the slot map was last scanned; zero to scan it on the next tick
	handled    map[string]time.Time // job name -> when it was acted on
	moved      <-chan string        // addresses named by MOVED replies
	now        func() time.Time
	jitter     func(time.Duration) time.Duration
	refresh    func(completed []slotMigrationCompletion)
	idle       time.Duration
	active     time.Duration
	finalizing time.Duration
}

// newSlotMigrationMonitor returns the monitor of c, with the intervals of c's options or their defaults. It starts
// polling when run is called.
func newSlotMigrationMonitor(c *clusterClient) *slotMigrationMonitor {
	opt := c.opt.ClusterOption
	m := &slotMigrationMonitor{
		c:          c,
		nodes:      make(map[conn]*slotMigrationNode),
		handled:    make(map[string]time.Time),
		moved:      c.migMoved,
		now:        time.Now,
		jitter:     jitterSlotMigrationInterval,
		idle:       opt.SlotMigrationIdleInterval,
		active:     opt.SlotMigrationActiveInterval,
		finalizing: opt.SlotMigrationFinalizingInterval,
	}
	if m.idle <= 0 {
		m.idle = defaultSlotMigrationIdleInterval
	}
	if m.active <= 0 {
		m.active = defaultSlotMigrationActiveInterval
	}
	if m.finalizing <= 0 {
		m.finalizing = defaultSlotMigrationFinalizingInterval
	}
	m.refresh = m.refreshFromReporters
	return m
}

// jitterSlotMigrationInterval returns d ± 20%, so that polls of many clients do not line up. The offset is drawn
// in 64 bits: twice the spread of a 30s interval overflows a 32-bit int.
func jitterSlotMigrationInterval(d time.Duration) time.Duration {
	spread := d / slotMigrationJitter
	if spread <= 0 {
		return d
	}
	return d - spread + time.Duration(rand.Int64N(int64(2*spread)+1))
}

// interval returns the polling interval of phase p.
func (m *slotMigrationMonitor) interval(p slotMigrationPhase) time.Duration {
	switch p {
	case slotMigrationPhaseFinalizing:
		return m.finalizing
	case slotMigrationPhaseActive:
		return m.active
	default:
		return m.idle
	}
}

// run polls until the client is closed, then closes done. Closing the client stops the monitor at once: it
// cancels in-flight polls and does not wait for a version check or a refresh that is still in progress.
func (m *slotMigrationMonitor) run(done chan struct{}) {
	defer close(done)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		select {
		case <-m.c.stopCh:
			cancel()
		case <-ctx.Done():
		}
	}()
	m.ctx = ctx

	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case addr := <-m.moved:
			if m.wake(addr) {
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				timer.Reset(m.delay(m.now()))
			}
			continue
		case <-timer.C:
		}
		delay, ok := m.tick()
		if !ok {
			return
		}
		timer.Reset(delay)
	}
}

// wake brings forward the next poll of the primary at addr, after a MOVED reply pointed to it. That node is
// the new owner of the slot: when the MOVED comes from an atomic slot migration, it already reports the job's
// success, usually before the source does. It is polled no sooner than the finalizing interval after its
// previous poll while a migration is known to run, or the active interval otherwise, so a burst of MOVED
// replies, including ones from failovers or legacy migrations, costs at most a few polls. A node whose poll is
// pending is left alone: it is checked at the finalizing interval until the poll answers. It reports whether the
// schedule changed.
func (m *slotMigrationMonitor) wake(addr string) bool {
	m.c.mu.RLock()
	cc := m.c.conns[addr].conn
	m.c.mu.RUnlock()
	n, ok := m.nodes[cc]
	if cc == nil || !ok || n.unsupported || n.pending {
		return false // not a monitored primary yet (the scheduled polls cover it once it owns slots), or not to be polled now
	}
	gap := m.active
	for _, o := range m.nodes {
		if o.phase != slotMigrationPhaseIdle {
			gap = m.finalizing
			break
		}
	}
	at := n.last.Add(gap)
	if now := m.now(); at.Before(now) {
		at = now
	}
	if !at.Before(n.next) {
		return false
	}
	n.next = at
	return true
}

// primaries returns the distinct connections that own slots. With ReplicaOnly, the slot map points to
// replicas, which do not report export jobs, so every known node is polled instead.
func (m *slotMigrationMonitor) primaries() []conn {
	m.c.mu.RLock()
	defer m.c.mu.RUnlock()
	var out []conn
	set := make(map[conn]struct{})
	if m.c.opt.ReplicaOnly {
		for _, cc := range m.c.conns {
			if cc.conn == nil || cc.hidden {
				continue
			}
			if _, ok := set[cc.conn]; !ok {
				set[cc.conn] = struct{}{}
				out = append(out, cc.conn)
			}
		}
		return out
	}
	var last conn
	for _, cc := range m.c.wslots {
		if cc == nil || cc == last {
			continue
		}
		last = cc
		if _, ok := set[cc]; !ok {
			set[cc] = struct{}{}
			out = append(out, cc)
		}
	}
	return out
}

// poll sends CLUSTER GETSLOTMIGRATIONS to the due nodes and waits at most slotMigrationPollTimeout for their
// answers. A node that is late does not hold up the others: its poll goes on in the background and delivers its
// answer to the node's answer channel, where a later tick finds it. Polls that have not answered are cancelled
// after slotMigrationPollCancel. poll returns at once when the client is closed; the results are not used then.
func (m *slotMigrationMonitor) poll(due []*slotMigrationNode) []slotMigrationPoll {
	polls := make([]slotMigrationPoll, len(due))
	if len(due) == 0 {
		return polls
	}
	ctx, cancel := context.WithCancel(m.ctx)
	stop := time.AfterFunc(slotMigrationPollCancel, cancel)
	all := make(chan struct{})
	var left atomic.Int32
	left.Store(int32(len(due)))
	for _, n := range due {
		n.pending = true
		go func(cc conn, answer chan<- slotMigrationPoll) {
			answer <- m.pollNode(ctx, cc)
			if left.Add(-1) == 0 {
				stop.Stop()
				cancel()
				close(all)
			}
		}(n.conn, n.answer)
	}
	wait := time.NewTimer(slotMigrationPollTimeout)
	defer wait.Stop()
	select {
	case <-all:
	case <-wait.C:
	case <-m.ctx.Done():
	}
	for i, n := range due {
		select {
		case polls[i] = <-n.answer:
		default: // late: the answer is left in the node's channel
		}
	}
	return polls
}

// pollNode polls one node. Reading the version may wait for the connection to be established, which no context
// interrupts. The poll itself is cancelled with ctx, and never given a deadline: it shares the node's connection
// with the application's commands, and on an idle connection a deadline is applied to the connection itself, which
// is closed when the deadline passes. That would fail the commands queued behind the poll, such as those redirected
// to the target at the handover. Cancelling only abandons the poll.
func (m *slotMigrationMonitor) pollNode(ctx context.Context, cc conn) slotMigrationPoll {
	if serverMajorVersion(cc) < slotMigrationMinVersion {
		return slotMigrationPoll{skipped: true, answered: true}
	}
	return slotMigrationPoll{result: cc.Do(ctx, cmds.GetSlotMigrationsCmd), answered: true}
}

// tick polls the primaries that are due and returns the delay until the next poll.
// It returns false when the client was closed.
func (m *slotMigrationMonitor) tick() (time.Duration, bool) {
	now := m.now()
	if m.scanned.IsZero() || now.Sub(m.scanned) >= m.active {
		m.track(now)
	}

	var due, late []*slotMigrationNode
	var answers []slotMigrationPoll // of the late nodes
	for _, cc := range m.owners {
		n := m.nodes[cc]
		if n.next.After(now) {
			continue
		}
		if n.pending { // the previous poll was late
			select {
			case p := <-n.answer:
				late = append(late, n)
				answers = append(answers, p)
			default: // and has not answered yet; it is checked again at the finalizing interval, without polling
				n.next = now.Add(m.finalizing)
			}
			continue
		}
		if len(due) < slotMigrationMaxConcurrentPolls {
			due = append(due, n)
		}
	}

	polls := m.poll(due)
	if m.ctx.Err() != nil {
		return 0, false // the client was closed during the poll
	}

	var completed []slotMigrationCompletion
	for i, n := range late {
		completed = m.record(completed, n, answers[i], now)
	}
	for i, n := range due {
		n.last = now
		completed = m.record(completed, n, polls[i], now)
	}

	if completed = m.claim(completed, now); len(completed) > 0 {
		m.refresh(completed)
		m.track(m.now()) // the slot map has changed: new owners are polled at once
	}
	return m.delay(m.now()), true
}

// track scans the slot map for the primaries to poll: it adds the nodes that own slots and drops those that no
// longer do. The slot map changes only when the topology is refreshed or a MOVED reply moves a slot, so tick scans
// it at most once per active interval, and again right after the monitor refreshes the topology.
func (m *slotMigrationMonitor) track(now time.Time) {
	m.owners, m.scanned = m.primaries(), now
	alive := make(map[conn]struct{}, len(m.owners))
	for _, cc := range m.owners {
		alive[cc] = struct{}{}
		if _, ok := m.nodes[cc]; !ok {
			m.nodes[cc] = &slotMigrationNode{conn: cc, answer: make(chan slotMigrationPoll, 1)}
		}
	}
	for cc := range m.nodes {
		if _, ok := alive[cc]; !ok {
			delete(m.nodes, cc)
		}
	}
}

// record applies the outcome of a poll of n and schedules its next poll. It appends the jobs that reached success
// to completed.
func (m *slotMigrationMonitor) record(completed []slotMigrationCompletion, n *slotMigrationNode, p slotMigrationPoll, now time.Time) []slotMigrationCompletion {
	if !p.answered {
		m.failed(n, now)
		if soon := now.Add(m.finalizing); soon.Before(n.next) {
			n.next = soon // to use the late answer as soon as it comes
		}
		return completed
	}
	n.pending = false
	if p.skipped {
		n.phase, n.errors = slotMigrationPhaseIdle, 0
		n.next = now.Add(m.jitter(m.idle))
		return completed
	}
	if err := p.result.Error(); err != nil {
		if verr, ok := IsValkeyErr(err); ok {
			if msg := verr.Error(); strings.HasPrefix(msg, "NOPERM") || isSlotMigrationUnsupported(msg) {
				n.unsupported = true // asked again later, in case the server or the ACL user changes
				n.phase, n.errors = slotMigrationPhaseIdle, 0
				n.next = now.Add(slotMigrationRecheck)
				return completed
			}
		}
		m.failed(n, now)
		return completed
	}
	migrations, err := parseSlotMigrations(p.result.val)
	if err != nil {
		m.failed(n, now)
		return completed
	}
	n.errors, n.unsupported = 0, false
	for _, mg := range n.observe(migrations, m.handled) {
		completed = append(completed, slotMigrationCompletion{reporter: n.conn, migration: mg})
	}
	if n.phase != slotMigrationPhaseIdle {
		n.lastActive = now
	}
	n.next = now.Add(m.jitter(m.interval(n.cadence(now))))
	return completed
}

// failed schedules the next poll of a node after a failed or late poll. The node keeps its cadence, except that
// every slotMigrationMaxErrors consecutive failures move it one phase down.
func (m *slotMigrationMonitor) failed(n *slotMigrationNode, now time.Time) {
	if n.errors++; n.errors%slotMigrationMaxErrors == 0 && n.phase > slotMigrationPhaseIdle {
		n.phase--
	}
	n.next = now.Add(m.jitter(m.interval(n.cadence(now))))
}

// delay returns the time until the next due poll, at most the idle interval.
func (m *slotMigrationMonitor) delay(now time.Time) time.Duration {
	delay := m.idle
	for _, n := range m.nodes {
		if d := n.next.Sub(now); d < delay {
			delay = d
		}
	}
	if delay < 0 {
		delay = 0
	}
	return delay
}

// claim keeps the first completion of each job. The source (EXPORT) and the target (IMPORT) both report the
// same job; whichever is seen first triggers the refresh. The other side reports it within a few polls, so a job
// acted on is remembered for slotMigrationHandledRetention.
func (m *slotMigrationMonitor) claim(completed []slotMigrationCompletion, now time.Time) []slotMigrationCompletion {
	for name, at := range m.handled {
		if now.Sub(at) > slotMigrationHandledRetention {
			delete(m.handled, name)
		}
	}
	out := completed[:0]
	for _, c := range completed {
		if _, ok := m.handled[c.migration.name]; ok {
			continue
		}
		m.handled[c.migration.name] = now
		out = append(out, c)
	}
	return out
}

// refreshFromReporters refreshes the topology from the nodes that reported the completed jobs, which already know
// the new owners. It calls _refreshFrom directly instead of going through c.sc, so it neither waits for nor
// replaces the jittered refresh that MOVED replies schedule at the handover; that refresh still runs at its own
// time. If the migrated slots do not route to their new owner afterwards, because the refresh failed or a regular
// refresh that asked a lagging node overwrote it, the refresh is repeated at once and then after the finalizing
// interval, before the update is left to the regular refresh. A stale regular refresh that lands after the last
// check is corrected as it would be without the monitor: by the MOVED replies to the commands sent to those slots,
// each of which updates its slot and schedules a refresh.
func (m *slotMigrationMonitor) refreshFromReporters(completed []slotMigrationCompletion) {
	var reporters []conn
	set := make(map[conn]struct{}, len(completed))
	for _, c := range completed {
		if _, ok := set[c.reporter]; !ok {
			set[c.reporter] = struct{}{}
			reporters = append(reporters, c.reporter)
		}
	}
	ctx, cancel := context.WithTimeout(m.ctx, slotMigrationRefreshTimeout)
	defer cancel()
	for attempt := 0; attempt < slotMigrationRefreshAttempts && ctx.Err() == nil; attempt++ {
		if attempt > 1 {
			select {
			case <-ctx.Done():
				continue // out of time: the regular refresh below takes over
			case <-time.After(m.finalizing):
			}
		}
		if err := m.refreshFrom(ctx, reporters); err == nil && m.routed(completed) {
			return
		}
	}
	if m.ctx.Err() == nil {
		m.c.lazyRefresh() // leave it to the regular refresh path
	}
}

// refreshFrom refreshes the topology from conns and gives up when ctx is done. ctx cannot interrupt _refreshFrom,
// so it runs in its own goroutine and, if ctx ends first, is left to finish on its own timeouts; like a regular
// refresh in flight, it may then still install its result, even after Close.
func (m *slotMigrationMonitor) refreshFrom(ctx context.Context, conns []conn) error {
	errc := make(chan error, 1)
	go func() { errc <- m.c._refreshFrom(conns) }()
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// routed reports whether the slots of the completed jobs point to their new owner: away from the source for
// EXPORT reports, and to the target for IMPORT reports. With ReplicaOnly the slot map points to replicas, so
// it cannot be checked this way.
func (m *slotMigrationMonitor) routed(completed []slotMigrationCompletion) bool {
	if m.c.opt.ReplicaOnly {
		return true
	}
	m.c.mu.RLock()
	defer m.c.mu.RUnlock()
	for _, c := range completed {
		for _, r := range parseSlotMigrationRanges(c.migration.slots) {
			for i := r[0]; i <= r[1]; i++ {
				owner := m.c.wslots[i]
				if c.migration.operation == slotMigrationExport && (owner == nil || owner == c.reporter) {
					return false
				}
				if c.migration.operation == slotMigrationImport && owner != c.reporter {
					return false
				}
			}
		}
	}
	return true
}
