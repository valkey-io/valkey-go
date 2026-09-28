package valkey

import (
	"context"
	"errors"
	"maps"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go/internal/cmds"
)

const masterRole = "master"

var errNoPrimaryFound = errors.New("no primary found")

type standalone struct {
	retryer        retryHandler
	toReplicas     func(Completed) bool
	nodeSelector   func(uint16, []NodeInfo) int
	primary        atomic.Pointer[singleClient]
	state          atomic.Pointer[standaloneState]
	connFn         connFn
	opt            *ClientOption
	redirectCall   call
	reconcileCall  call
	stop           chan struct{}
	stopOnce       sync.Once
	enableRedirect bool
}

type standaloneState struct {
	masterAddr string // prioritized master address from ReplicaAddress
	clients    map[string]*singleClient
	nodes      []NodeInfo // length == len(replicas)+1; nodes[0] mirrors the current primary
	replicas   []*singleClient
}

func newStandaloneClient(opt *ClientOption, connFn connFn, retryer retryHandler) (*standalone, error) {
	if len(opt.InitAddress) == 0 {
		return nil, ErrNoAddr
	}

	p := connFn(opt.InitAddress[0], opt)
	if err := p.Dial(); err != nil {
		return nil, err
	}

	s := &standalone{
		toReplicas:     opt.SendToReplicas,
		nodeSelector:   opt.ReadNodeSelector,
		enableRedirect: opt.Standalone.EnableRedirect,
		connFn:         connFn,
		opt:            opt,
		retryer:        retryer,
	}
	primaryClient := newSingleClientWithConn(p, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry, opt.DisableCache, retryer, opt.ConnLifetime > 0)
	s.primary.Store(primaryClient)

	refreshEnabled := opt.Standalone.ReplicaRefreshInterval > 0
	replicas := make([]*singleClient, 0, len(opt.Standalone.ReplicaAddress))
	clients := make(map[string]*singleClient, len(opt.Standalone.ReplicaAddress)+1)

	masterAddr := p.Addr()
	clients[p.Addr()] = primaryClient

	// Dial each replica address. Entries are dropped when ROLE returns a
	// definitive negative signal: master (dedup with primary), or a slave that
	// is not currently connected to its master. Dial failures and ROLE errors
	// are tolerated so transient issues don't lose otherwise-usable connections;
	// the periodic monitor (if enabled) will re-evaluate them later.
	for _, addr := range opt.Standalone.ReplicaAddress {
		replicaConn := connFn(addr, opt)
		if err := replicaConn.Dial(); err != nil {
			if !refreshEnabled {
				return nil, err
			}
			continue
		}
		if role := replicaConn.Role(); role == masterRole {
			replicaConn.Close()
			if !refreshEnabled {
				return nil, errors.New("replica address points to a master node")
			}
			masterAddr = addr
			// do not add master to the replicas list
			continue
		}
		replicaClient := newSingleClientWithConn(replicaConn, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry, opt.DisableCache, retryer, opt.ConnLifetime > 0)
		clients[addr] = replicaClient
		replicas = append(replicas, replicaClient)
	}

	nodes := make([]NodeInfo, len(replicas)+1)

	if s.opt.EnableReplicaAZInfo && (s.opt.ReadNodeSelector != nil || len(replicas) > 1) {
		nodes[0] = NodeInfo{
			Addr: primaryClient.conn.Addr(),
			AZ:   primaryClient.conn.AZ(),
		}
		for i, replica := range replicas {
			nodes[i+1] = NodeInfo{
				Addr: replica.conn.Addr(),
				AZ:   replica.conn.AZ(),
			}
		}
	}

	s.state.Store(&standaloneState{
		masterAddr: masterAddr,
		clients:    clients,
		replicas:   replicas,
		nodes:      nodes,
	})

	if refreshEnabled {
		if err := s.reconcile(context.Background()); err != nil {
			return nil, err
		}

		s.stop = make(chan struct{})
		go s.runReplicaMonitor(opt.Standalone.ReplicaRefreshInterval)
	}

	return s, nil
}

// runRoleCmd returns the node's role and, for slaves, whether the replica is
// currently synced ("connected") to its master. Real Valkey/Redis returns a
// 5-element ROLE response for slaves; truncated responses are tolerated by
// defaulting connected=true so test mocks with a 1-element response still work.
func runRoleCmd(ctx context.Context, c conn) (role string, connected bool, err error) {
	resp, err := c.Do(ctx, cmds.RoleCmd).ToArray()
	if err != nil {
		return "", false, err
	}
	if len(resp) == 0 {
		return "", false, errors.New("empty ROLE response")
	}
	role = resp[0].string()
	// note that as oppossed to conn.Role() which returns "master" or "replica",
	// the ROLE response returns "master" or "slave"
	switch role {
	case masterRole:
		connected = true
	case "slave":
		connected = true
		if len(resp) >= 4 {
			connected = resp[3].string() == "connected"
		}
	}
	return role, connected, nil
}

func closeSingleClientDelayed(c *singleClient) {
	go func(c *singleClient) {
		time.Sleep(time.Second * 5)
		c.Close()
	}(c)
}

func (s *standalone) B() Builder {
	return s.primary.Load().B()
}

func (s *standalone) pick(slot uint16) *singleClient {
	st := s.state.Load()
	if s.nodeSelector != nil {
		idx := s.nodeSelector(slot, st.nodes)
		if idx <= 0 || idx > len(st.replicas) {
			return s.primary.Load()
		}
		return st.replicas[idx-1]
	}
	if n := len(st.replicas); n > 0 {
		return st.replicas[rand.IntN(n)]
	}
	return s.primary.Load()
}

func (s *standalone) recreatePrimaryConn(addr string) error {
	// Create a new connection to the redirect address
	redirectOpt := *s.opt
	redirectOpt.InitAddress = []string{addr}
	redirectConn := s.connFn(addr, &redirectOpt)
	if err := redirectConn.Dial(); err != nil {
		return err
	}

	// Create a new primary client with the redirect connection
	newPrimary := newSingleClientWithConn(redirectConn, cmds.NewBuilder(cmds.NoSlot), !s.opt.DisableRetry, s.opt.DisableCache, s.retryer, s.opt.ConnLifetime > 0)

	// Atomically swap the primary and close the old one
	oldPrimary := s.primary.Swap(newPrimary)
	closeSingleClientDelayed(oldPrimary)
	return nil
}

// handlePrimaryError handles REDIRECT (Valkey 8+ client redirect) and READONLY
// (primary failover detected by writing to a node that has become a replica).
// Returns true when the caller should retry the command on the updated primary.
func (s *standalone) handlePrimaryError(ctx context.Context, attempts int, cmd Completed, err error) bool {
	valkeyErr, ok := IsValkeyErr(err)
	if !ok {
		return false
	}
	if s.enableRedirect {
		if addr, ok := valkeyErr.IsRedirect(); ok {
			recErr := s.redirectCall.Do(ctx, func() error {
				return s.recreatePrimaryConn(addr)
			})
			return recErr == nil || s.retryer.WaitOrSkipRetry(ctx, attempts, cmd, err)
		}
	}
	if s.opt.Standalone.ReplicaRefreshInterval > 0 && valkeyErr.IsReadOnly() {
		prev := s.primary.Load()
		_ = s.reconcile(ctx)
		if s.primary.Load() != prev {
			return true
		}
		return s.retryer.WaitOrSkipRetry(ctx, attempts, cmd, err)
	}
	return false
}

// reconcile re-runs ROLE on the primary and every replica, atomically rebuilds
// the state.
// Safe to call concurrently; deduplicated through s.reconcileCall.
func (s *standalone) reconcile(ctx context.Context) error {
	return s.reconcileCall.Do(ctx, func() error {
		st := s.state.Load()
		withAZ := s.opt.EnableReplicaAZInfo
		masterAddr := st.masterAddr

		type entry struct {
			client    *singleClient
			node      NodeInfo
			role      string
			connected bool
			ok        bool
		}

		clients := map[string]*singleClient{}
		maps.Copy(clients, st.clients)

		fetchRole := func(c *singleClient) entry {
			role, connected, err := runRoleCmd(ctx, c.conn)
			n := NodeInfo{Addr: c.conn.Addr()}
			if withAZ {
				n.AZ = c.conn.AZ()
			}
			return entry{client: c, node: n, role: role, connected: connected, ok: err == nil}
		}

		// run ROLE against all nodes
		entries := make([]entry, 0, 1+len(s.opt.Standalone.ReplicaAddress))
		entries = append(entries, fetchRole(s.primary.Load()))

		for _, addr := range s.opt.Standalone.ReplicaAddress {
			client, ok := clients[addr]
			if !ok {
				conn := s.connFn(addr, s.opt)
				if err := conn.Dial(); err != nil {
					continue
				}
				client = newSingleClientWithConn(conn, cmds.NewBuilder(cmds.NoSlot), !s.opt.DisableRetry, s.opt.DisableCache, s.retryer, s.opt.ConnLifetime > 0)
				clients[addr] = client
			}
			entry := fetchRole(client)
			if entry.role == masterRole {
				masterAddr = addr
			}
			entries = append(entries, entry)
		}

		// filter out entries that are not ok or are not connected replicas
		newEntries := entries[:0]
		for _, e := range entries {
			if e.ok && e.connected {
				newEntries = append(newEntries, e)
			}
		}
		entries = newEntries

		// we might have two masters, one identified by primary address and one identified by a replica address
		numMasters := 0
		for _, e := range entries {
			if e.ok && e.role == masterRole {
				numMasters++
			}
		}

		if numMasters == 0 {
			// No master visible; leave state untouched and let the next pass
			// (or a real failover) sort it out.
			return errNoPrimaryFound
		}

		if len(entries) > 0 && entries[0].role != masterRole {
			// sort the entries by role, master first
			slices.SortStableFunc(entries, func(a, b entry) int {
				if a.role == b.role {
					return 0
				}
				if a.role == masterRole {
					return -1
				}
				if b.role == masterRole {
					return 1
				}
				return 0
			})
		}

		// filter out all masters except the first one
		newEntries = entries[:1]
		for _, e := range entries[1:] {
			if e.role == masterRole {
				continue
			}
			newEntries = append(newEntries, e)
		}
		entries = newEntries

		newReplicas := make([]*singleClient, 0, len(entries)-1)
		newNodes := make([]NodeInfo, 1, len(entries))
		newNodes[0] = entries[0].node

		for _, e := range entries[1:] {
			newReplicas = append(newReplicas, e.client)
			newNodes = append(newNodes, e.node)
		}

		// master address has changed, recreate the primary connection
		if st.masterAddr != masterAddr {
			initAddr0 := s.opt.InitAddress[0]
			if err := s.recreatePrimaryConn(initAddr0); err != nil {
				return err
			}
			delete(clients, initAddr0)
			clients[initAddr0] = s.primary.Load()
		}

		s.state.Store(&standaloneState{
			masterAddr: masterAddr,
			replicas:   newReplicas,
			nodes:      newNodes,
			clients:    clients,
		})
		return nil
	})
}

func (s *standalone) runReplicaMonitor(interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), interval)
			_ = s.reconcile(ctx)
			cancel()
		}
	}
}

func (s *standalone) Do(ctx context.Context, cmd Completed) (resp ValkeyResult) {
	attempts := 1
	cmd = cmd.Pin()

retry:
	if s.toReplicas != nil && s.toReplicas(cmd) {
		resp = s.pick(cmd.Slot()).Do(ctx, cmd)
	} else {
		resp = s.primary.Load().Do(ctx, cmd)
	}
	if s.handlePrimaryError(ctx, attempts, cmd, resp.Error()) {
		attempts++
		goto retry
	}

	if resp.NonValkeyError() == nil {
		cmds.PutCompletedForce(cmd)
	}
	return resp
}

func (s *standalone) DoMulti(ctx context.Context, multi ...Completed) (resp []ValkeyResult) {
	attempts := 1
	for i := range multi {
		multi[i] = multi[i].Pin()
	}

retry:
	toReplica := s.toReplicas != nil
	for i := 0; i < len(multi) && toReplica; i++ {
		toReplica = s.toReplicas(multi[i])
	}
	if toReplica && len(multi) > 0 {
		resp = s.pick(multi[0].Slot()).DoMulti(ctx, multi...)
	} else {
		resp = s.primary.Load().DoMulti(ctx, multi...)
	}
	if len(resp) > 0 && s.handlePrimaryError(ctx, attempts, multi[0], resp[0].Error()) {
		attempts++
		goto retry
	}
	for i, result := range resp {
		if result.NonValkeyError() == nil {
			cmds.PutCompletedForce(multi[i])
		}
	}
	return resp
}

func (s *standalone) Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) error {
	if s.toReplicas != nil && s.toReplicas(subscribe) {
		return s.pick(subscribe.Slot()).Receive(ctx, subscribe, fn)
	}
	return s.primary.Load().Receive(ctx, subscribe, fn)
}

func (s *standalone) Close() {
	if s.stop != nil {
		s.stopOnce.Do(func() { close(s.stop) })
	}
	s.primary.Load().Close()
	for _, replica := range s.state.Load().replicas {
		replica.Close()
	}
}

func (s *standalone) DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) (resp ValkeyResult) {
	attempts := 1
	cmd = cmd.Pin()

retry:
	resp = s.primary.Load().DoCache(ctx, cmd, ttl)
	if s.handlePrimaryError(ctx, attempts, Completed(cmd), resp.Error()) {
		attempts++
		goto retry
	}
	if resp.NonValkeyError() == nil {
		cmds.PutCacheableForce(cmd)
	}
	return
}

func (s *standalone) DoMultiCache(ctx context.Context, multi ...CacheableTTL) (resp []ValkeyResult) {
	attempts := 1
	for i := range multi {
		multi[i].Cmd = multi[i].Cmd.Pin()
	}

retry:
	resp = s.primary.Load().DoMultiCache(ctx, multi...)
	if len(resp) > 0 && s.handlePrimaryError(ctx, attempts, Completed(multi[0].Cmd), resp[0].Error()) {
		attempts++
		goto retry
	}
	for i, result := range resp {
		if result.NonValkeyError() == nil {
			cmds.PutCacheableForce(multi[i].Cmd)
		}
	}
	return
}

func (s *standalone) DoStream(ctx context.Context, cmd Completed) ValkeyResultStream {
	if s.toReplicas != nil && s.toReplicas(cmd) {
		return s.pick(cmd.Slot()).DoStream(ctx, cmd)
	}
	return s.primary.Load().DoStream(ctx, cmd)
}

func (s *standalone) DoMultiStream(ctx context.Context, multi ...Completed) MultiValkeyResultStream {
	toReplica := s.toReplicas != nil
	for i := 0; i < len(multi) && toReplica; i++ {
		toReplica = s.toReplicas(multi[i])
	}
	if toReplica && len(multi) > 0 {
		return s.pick(multi[0].Slot()).DoMultiStream(ctx, multi...)
	}
	return s.primary.Load().DoMultiStream(ctx, multi...)
}

func (s *standalone) Dedicated(fn func(DedicatedClient) error) (err error) {
	return s.primary.Load().Dedicated(fn)
}

func (s *standalone) Dedicate() (client DedicatedClient, cancel func()) {
	return s.primary.Load().Dedicate()
}

func (s *standalone) Nodes() map[string]Client {
	st := s.state.Load()
	nodes := make(map[string]Client, len(st.replicas)+1)
	maps.Copy(nodes, s.primary.Load().Nodes())
	for _, replica := range st.replicas {
		maps.Copy(nodes, replica.Nodes())
	}
	return nodes
}

func (s *standalone) Mode() ClientMode {
	return ClientModeStandalone
}
