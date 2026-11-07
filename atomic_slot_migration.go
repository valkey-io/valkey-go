package valkey

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"
)

// ErrAtomicSlotMigrationAwarenessRequiresCluster is returned when
// NewClientWithAtomicSlotMigrationAwareness is used with a non-cluster client.
var ErrAtomicSlotMigrationAwarenessRequiresCluster = errors.New("atomic slot migration awareness requires a cluster client")

const (
	asmDiscoveryInterval    = 2 * time.Second
	asmFinalizationInterval = 100 * time.Millisecond
	asmCommandTimeout       = 1500 * time.Millisecond
	asmRefreshTimeout       = 2 * time.Second
	asmMaxConcurrentNodes   = 8
	asmStateRetention       = 10 * time.Minute
)

// NewClientWithAtomicSlotMigrationAwareness constructs a normal valkey-go
// client and opts it into best-effort awareness of Valkey 9.0+ atomic slot
// migrations. Existing NewClient callers are unaffected.
//
// Awareness runs independently of application commands. It observes
// CLUSTER GETSLOTMIGRATIONS and asks the existing cluster client to refresh
// topology at migration phase transitions. It never changes slot ownership
// locally. The feature is intended for cluster clients only and requires ACL
// permission to run CLUSTER GETSLOTMIGRATIONS.
func NewClientWithAtomicSlotMigrationAwareness(option ClientOption) (Client, error) {
	client, err := NewClient(option)
	if err != nil {
		if client != nil {
			client.Close()
		}
		return nil, err
	}

	if client == nil {
		return nil, ErrAtomicSlotMigrationAwarenessRequiresCluster
	}

	cluster, ok := client.(*clusterClient)
	if !ok || client.Mode() != ClientModeCluster {
		client.Close()
		return nil, ErrAtomicSlotMigrationAwarenessRequiresCluster
	}

	ctx, cancel := context.WithCancel(context.Background())
	aware := &atomicSlotMigrationAwareClient{
		Client:  client,
		cluster: cluster,
		cancel:  cancel,
		done:    make(chan struct{}),
		monitor: atomicSlotMigrationMonitor{
			ctx:              ctx,
			cluster:          cluster,
			hotNodes:         make(map[string]struct{}),
			migrationStates:  make(map[string]asmMigrationObservation),
			pendingRefreshes: make(map[string]asmMigrationPhase),
		},
	}
	go aware.monitor.run(aware.done)
	return aware, nil
}

// atomicSlotMigrationAwareClient embeds Client so all normal command APIs
// continue to delegate to the existing client. Only Close is overridden to
// stop and join the optional monitor before returning.
type atomicSlotMigrationAwareClient struct {
	Client
	cluster   *clusterClient
	cancel    context.CancelFunc
	done      chan struct{}
	closeOnce sync.Once
	monitor   atomicSlotMigrationMonitor
}

func (c *atomicSlotMigrationAwareClient) Close() {
	c.closeOnce.Do(func() {
		c.cancel()
		c.Client.Close()
	})
	<-c.done
}

type asmMigrationPhase uint8

const (
	asmPhaseActive asmMigrationPhase = iota
	asmPhaseApproachingFinalization
	asmPhaseFinalizing
	asmPhaseTerminal
)

type asmMigrationRecord struct {
	name      string
	operation string
	state     string
}

type asmNodeResult struct {
	addr    string
	records []asmMigrationRecord
	err     error
}

type asmMigrationObservation struct {
	// phase is monotonic for the lifetime of a migration name. If source and
	// target report slightly different states, an older observation must not
	// move an already observed migration backward.
	phase    asmMigrationPhase
	lastSeen time.Time
}

type atomicSlotMigrationMonitor struct {
	ctx     context.Context
	cluster *clusterClient

	// The monitor loop is the only goroutine that mutates these maps.
	hotNodes         map[string]struct{}
	migrationStates  map[string]asmMigrationObservation
	pendingRefreshes map[string]asmMigrationPhase
}

func (m *atomicSlotMigrationMonitor) run(done chan<- struct{}) {
	defer close(done)

	if m.scanAllNodes() {
		// An old server or an account without command permission cannot provide
		// ASM awareness. Disable this monitor only; normal client traffic stays
		// available through the underlying client.
		return
	}

	discoveryTicker := time.NewTicker(asmDiscoveryInterval)
	defer discoveryTicker.Stop()
	fastTicker := time.NewTicker(asmFinalizationInterval)
	defer fastTicker.Stop()

	for {
		select {
		case <-m.ctx.Done():
			return
		case <-discoveryTicker.C:
			if m.scanAllNodes() {
				return
			}
		case <-fastTicker.C:
			if len(m.hotNodes) == 0 {
				continue
			}
			if m.scanHotNodes() {
				return
			}
		}
	}
}

// scanAllNodes performs the relatively infrequent discovery scan. Nodes()
// returns client handles backed by the cluster's current connections; those
// handles must not be closed here because their connections are shared.
func (m *atomicSlotMigrationMonitor) scanAllNodes() bool {
	nodes := m.cluster.Nodes()
	m.removeMissingHotNodes(nodes)
	return m.scan(nodes)
}

func (m *atomicSlotMigrationMonitor) scanHotNodes() bool {
	allNodes := m.cluster.Nodes()
	selected := make(map[string]Client, len(m.hotNodes))
	for addr := range m.hotNodes {
		if node, ok := allNodes[addr]; ok {
			selected[addr] = node
		} else {
			delete(m.hotNodes, addr)
		}
	}
	if len(selected) == 0 {
		return false
	}
	return m.scan(selected)
}

func (m *atomicSlotMigrationMonitor) removeMissingHotNodes(nodes map[string]Client) {
	for addr := range m.hotNodes {
		if _, ok := nodes[addr]; !ok {
			delete(m.hotNodes, addr)
		}
	}
}

// scan returns true only when the optional command is unavailable or denied.
// All other errors are treated as transient and retried on a later tick.
func (m *atomicSlotMigrationMonitor) scan(nodes map[string]Client) bool {
	if len(nodes) == 0 || m.ctx.Err() != nil {
		return false
	}

	// Give each scan its own child context. If one node tells us that the
	// command is unsupported/denied, cancel in-flight requests immediately
	// instead of waiting for every command timeout. Parent cancellation also
	// propagates here during client shutdown.
	scanCtx, cancelScan := context.WithCancel(m.ctx)
	defer cancelScan()

	type nodeJob struct {
		addr string
		node Client
	}
	jobs := make(chan nodeJob)
	results := make(chan asmNodeResult, len(nodes))
	workers := len(nodes)
	if workers > asmMaxConcurrentNodes {
		workers = asmMaxConcurrentNodes
	}

	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			for job := range jobs {
				results <- m.fetchNode(scanCtx, job.addr, job.node)
			}
		}()
	}
	go func() {
		defer close(results)
		for addr, node := range nodes {
			if scanCtx.Err() != nil {
				break
			}
			select {
			case <-scanCtx.Done():
				close(jobs)
				wg.Wait()
				return
			case jobs <- nodeJob{addr: addr, node: node}:
			}
		}
		close(jobs)
		wg.Wait()
	}()

	unsupported := false
	for result := range results {
		if result.err != nil {
			if isUnsupportedASMMigrationError(result.err) {
				unsupported = true
				// This feature is optional. Stop other commands in this scan now;
				// run() will exit after scan() drains its workers. No error is logged.
				cancelScan()
			}
			// Preserve the hot-node entry on transient failures. A timeout is not
			// evidence that a migration has finished.
			continue
		}

		hot := false
		for _, record := range result.records {
			phase := classifyASMMigrationPhase(record.operation, record.state)
			if phase == asmPhaseApproachingFinalization || phase == asmPhaseFinalizing {
				hot = true
			}
			m.observeMigration(record, time.Now())
		}
		if hot {
			m.hotNodes[result.addr] = struct{}{}
		} else {
			delete(m.hotNodes, result.addr)
		}
	}

	if unsupported || m.ctx.Err() != nil {
		return unsupported
	}

	// Keep pending events until refresh succeeds. State observations prevent an
	// already-seen terminal record from enqueueing another refresh each poll.
	if len(m.pendingRefreshes) > 0 {
		ctx, cancel := context.WithTimeout(m.ctx, asmRefreshTimeout)
		err := m.cluster.refresh(ctx)
		cancel()
		if err == nil {
			for name := range m.pendingRefreshes {
				delete(m.pendingRefreshes, name)
			}
		}
	}
	m.pruneMigrationStates(time.Now())
	return false
}

func (m *atomicSlotMigrationMonitor) fetchNode(parent context.Context, addr string, node Client) asmNodeResult {
	ctx, cancel := context.WithTimeout(parent, asmCommandTimeout)
	defer cancel()

	cmd := node.B().Arbitrary("CLUSTER", "GETSLOTMIGRATIONS").Build()
	result := node.Do(ctx, cmd)
	if err := result.Error(); err != nil {
		return asmNodeResult{addr: addr, err: err}
	}
	rows, err := result.ToArray()
	if err != nil {
		return asmNodeResult{addr: addr, err: err}
	}

	records := make([]asmMigrationRecord, 0, len(rows))
	for _, row := range rows {
		record, parseErr := parseASMMigrationRecord(row)
		if parseErr != nil {
			// Skip malformed/unrecognized rows rather than affecting ordinary
			// application commands or turning one row into a monitor outage.
			continue
		}
		if record.name != "" {
			records = append(records, record)
		}
	}
	return asmNodeResult{addr: addr, records: records}
}

func parseASMMigrationRecord(row ValkeyMessage) (asmMigrationRecord, error) {
	values, err := row.AsStrMap()
	if err != nil {
		return asmMigrationRecord{}, err
	}
	name := values["name"]
	if name == "" {
		return asmMigrationRecord{}, errors.New("ASM migration record has no name")
	}
	operation := values["operation"]
	state := values["state"]
	return asmMigrationRecord{
		name:      name,
		operation: strings.ToUpper(operation),
		state:     strings.ToLower(state),
	}, nil
}

func classifyASMMigrationPhase(operation, state string) asmMigrationPhase {
	operation = strings.ToUpper(operation)
	state = strings.ToLower(state)

	switch state {
	case "success", "failed", "cancelled":
		return asmPhaseTerminal
	}

	switch {
	case operation == "EXPORT" && state == "waiting-to-pause":
		return asmPhaseApproachingFinalization
	case operation == "IMPORT" && state == "waiting-for-paused":
		return asmPhaseApproachingFinalization
	case operation == "EXPORT" && (state == "failover-paused" || state == "failover-granted"):
		return asmPhaseFinalizing
	case operation == "IMPORT" && (state == "failover-requested" || state == "failover-granted" || state == "cleaning-up" || state == "occurring-on-primary"):
		return asmPhaseFinalizing
	default:
		// Unknown and other non-terminal states are active. Never infer local
		// slot ownership from a phase string.
		return asmPhaseActive
	}
}

func isUnsupportedASMMigrationError(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "noperm") ||
		strings.Contains(message, "unknown command") ||
		strings.Contains(message, "unknown subcommand") ||
		strings.Contains(message, "not allowed")
}

// observeMigration records the highest phase observed for a migration and
// returns whether this observation advanced it into a refresh-worthy phase. It
// deliberately does not compare server timestamps with the client's wall clock.
//
// A terminal record is actionable if it is first observed terminal (including
// when the migration completed between polling ticks) or if a tracked migration
// transitions into the terminal phase. Repeated observations of the same
// terminal record do not enqueue refreshes again. State is keyed by migration
// name, so seeing the corresponding record on both source and target does not
// produce duplicate refreshes for the same phase.
func (m *atomicSlotMigrationMonitor) observeMigration(record asmMigrationRecord, now time.Time) bool {
	phase := classifyASMMigrationPhase(record.operation, record.state)
	previous, seen := m.migrationStates[record.name]

	newPhase := !seen && phase >= asmPhaseApproachingFinalization
	if seen && phase > previous.phase && phase >= asmPhaseApproachingFinalization {
		newPhase = true
	}

	highestPhase := phase
	if seen && previous.phase > highestPhase {
		highestPhase = previous.phase
	}
	m.migrationStates[record.name] = asmMigrationObservation{
		phase:    highestPhase,
		lastSeen: now,
	}

	if newPhase {
		if pending, ok := m.pendingRefreshes[record.name]; !ok || phase > pending {
			m.pendingRefreshes[record.name] = phase
		}
	}
	return newPhase
}

func (m *atomicSlotMigrationMonitor) pruneMigrationStates(now time.Time) {
	cutoff := now.Add(-asmStateRetention)
	for key, observation := range m.migrationStates {
		if observation.lastSeen.Before(cutoff) {
			delete(m.migrationStates, key)
			// Do not retain a failed-refresh entry forever after the server stops
			// reporting that migration. Ordinary MOVED/ASK recovery remains the
			// fallback if this stale entry is eventually evicted.
			delete(m.pendingRefreshes, key)
		}
	}
}
