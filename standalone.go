package valkey

import (
	"context"
	"math/rand/v2"
	"time"

	"github.com/valkey-io/valkey-go/internal/cmds"
)

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
		primary:        newSingleClientWithConn(p, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry, opt.DisableCache, retryer, false),
		replicas:       make([]*singleClient, len(opt.Standalone.ReplicaAddress)),
		enableRedirect: opt.Standalone.EnableRedirect,
		connFn:         connFn,
		opt:            opt,
		retryer:        retryer,
	}
	opt.ReplicaOnly = true
	for i := range s.replicas {
		replicaConn := connFn(opt.Standalone.ReplicaAddress[i], opt)
		if err := replicaConn.Dial(); err != nil {
			s.primary.Close() // close primary if any replica fails
			for j := 0; j < i; j++ {
				s.replicas[j].Close()
			}
			return nil, err
		}
		s.replicas[i] = newSingleClientWithConn(replicaConn, cmds.NewBuilder(cmds.NoSlot), !opt.DisableRetry, opt.DisableCache, retryer, false)
	}
	return s, nil
}

type standalone struct {
	toReplicas     func(Completed) bool
	primary        *singleClient
	replicas       []*singleClient
	enableRedirect bool
	connFn         connFn
	opt            *ClientOption
	retryer        retryHandler
	redirectCall   call
}

func (s *standalone) B() Builder {
	return s.primary.B()
}

func (s *standalone) pick() int {
	if len(s.replicas) == 1 {
		return 0
	}
	return rand.IntN(len(s.replicas))
}

func (s *standalone) redirectToPrimary(addr string) error {
	// Create a new connection to the redirect address
	redirectOpt := *s.opt
	redirectOpt.InitAddress = []string{addr}
	redirectConn := s.connFn(addr, &redirectOpt)
	if err := redirectConn.Dial(); err != nil {
		return err
	}

	// Create a new primary client with the redirect connection
	newPrimary := newSingleClientWithConn(redirectConn, cmds.NewBuilder(cmds.NoSlot), !s.opt.DisableRetry, s.opt.DisableCache, s.retryer, false)

	// Close the old primary and swap to the new one
	oldPrimary := s.primary
	s.primary = newPrimary
	oldPrimary.Close()

	return nil
}

func (s *standalone) Do(ctx context.Context, cmd Completed) (resp ValkeyResult) {
	attempts := 1
retry:
	if s.toReplicas != nil && s.toReplicas(cmd) {
		resp = s.replicas[s.pick()].Do(ctx, cmd)
	} else {
		resp = s.primary.Do(ctx, cmd)
	}

	// Handle redirects with retry until context deadline  
	if s.enableRedirect {
		if ret, yes := IsValkeyErr(resp.Error()); yes {
			if addr, ok := ret.IsRedirect(); ok {
				// Pin the command to prevent recycling during retries
				cmd = cmd.Pin()
				err := s.redirectCall.Do(ctx, func() error {
					return s.redirectToPrimary(addr)
				})
				// Use retryHandler to handle multiple redirects with context deadline
				if err == nil || s.retryer.WaitOrSkipRetry(ctx, attempts, cmd, resp.Error()) {
					attempts++
					goto retry
				}
			}
		}
	}

	return resp
}

func (s *standalone) DoMulti(ctx context.Context, multi ...Completed) (resp []ValkeyResult) {
	attempts := 1
retry:
	toReplica := true
	for _, cmd := range multi {
		if s.toReplicas == nil || !s.toReplicas(cmd) {
			toReplica = false
			break
		}
	}
	if toReplica {
		resp = s.replicas[s.pick()].DoMulti(ctx, multi...)
	} else {
		resp = s.primary.DoMulti(ctx, multi...)
	}

	// Handle redirects with retry until context deadline
	if s.enableRedirect {
		for i, result := range resp {
			if i < len(multi) {
				if ret, yes := IsValkeyErr(result.Error()); yes {
					if addr, ok := ret.IsRedirect(); ok {
						// Pin all commands to prevent recycling during retries
						for j := range multi {
							multi[j] = multi[j].Pin()
						}
						err := s.redirectCall.Do(ctx, func() error {
							return s.redirectToPrimary(addr)
						})
						// Use retryHandler to handle multiple redirects with context deadline
						if err == nil || s.retryer.WaitOrSkipRetry(ctx, attempts, multi[0], result.Error()) {
							attempts++
							goto retry
						}
						break // Exit the loop if redirect handling fails
					}
				}
			}
		}
	}

	return resp
}

func (s *standalone) Receive(ctx context.Context, subscribe Completed, fn func(msg PubSubMessage)) error {
	if s.toReplicas != nil && s.toReplicas(subscribe) {
		return s.replicas[s.pick()].Receive(ctx, subscribe, fn)
	}
	return s.primary.Receive(ctx, subscribe, fn)
}

func (s *standalone) Close() {
	s.primary.Close()
	for _, replica := range s.replicas {
		replica.Close()
	}
}

func (s *standalone) DoCache(ctx context.Context, cmd Cacheable, ttl time.Duration) (resp ValkeyResult) {
	return s.primary.DoCache(ctx, cmd, ttl)
}

func (s *standalone) DoMultiCache(ctx context.Context, multi ...CacheableTTL) (resp []ValkeyResult) {
	return s.primary.DoMultiCache(ctx, multi...)
}

func (s *standalone) DoStream(ctx context.Context, cmd Completed) ValkeyResultStream {
	var stream ValkeyResultStream
	if s.toReplicas != nil && s.toReplicas(cmd) {
		stream = s.replicas[s.pick()].DoStream(ctx, cmd)
	} else {
		stream = s.primary.DoStream(ctx, cmd)
	}

	// Handle redirect for stream
	if s.enableRedirect && stream.Error() != nil {
		if ret, yes := IsValkeyErr(stream.Error()); yes {
			if addr, ok := ret.IsRedirect(); ok {
				err := s.redirectCall.Do(ctx, func() error {
					return s.redirectToPrimary(addr)
				})
				if err == nil {
					// Execute the command on the updated primary
					return s.primary.DoStream(ctx, cmd)
				}
			}
		}
	}

	return stream
}

func (s *standalone) DoMultiStream(ctx context.Context, multi ...Completed) MultiValkeyResultStream {
	var stream MultiValkeyResultStream
	toReplica := true
	for _, cmd := range multi {
		if s.toReplicas == nil || !s.toReplicas(cmd) {
			toReplica = false
			break
		}
	}
	if toReplica {
		stream = s.replicas[s.pick()].DoMultiStream(ctx, multi...)
	} else {
		stream = s.primary.DoMultiStream(ctx, multi...)
	}

	// Handle redirect for stream
	if s.enableRedirect && stream.Error() != nil {
		if ret, yes := IsValkeyErr(stream.Error()); yes {
			if addr, ok := ret.IsRedirect(); ok {
				err := s.redirectCall.Do(ctx, func() error {
					return s.redirectToPrimary(addr)
				})
				if err == nil {
					// Execute the command on the updated primary
					return s.primary.DoMultiStream(ctx, multi...)
				}
			}
		}
	}

	return stream
}

func (s *standalone) Dedicated(fn func(DedicatedClient) error) (err error) {
	return s.primary.Dedicated(fn)
}

func (s *standalone) Dedicate() (client DedicatedClient, cancel func()) {
	return s.primary.Dedicate()
}

func (s *standalone) Nodes() map[string]Client {
	nodes := make(map[string]Client, len(s.replicas)+1)
	for addr, client := range s.primary.Nodes() {
		nodes[addr] = client
	}
	for _, replica := range s.replicas {
		for addr, client := range replica.Nodes() {
			nodes[addr] = client
		}
	}
	return nodes
}

func (s *standalone) Mode() ClientMode {
	return ClientModeStandalone
}
