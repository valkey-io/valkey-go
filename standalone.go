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
	toReplicas       func(Completed) bool
	primary          *singleClient
	replicas         []*singleClient
	enableRedirect   bool
	connFn           connFn
	opt              *ClientOption
	retryer          retryHandler
	redirectCall     call
	lastRedirectAddr string
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

func (s *standalone) handleRedirect(ctx context.Context, cmd Completed, result ValkeyResult) ValkeyResult {
	if !s.enableRedirect {
		return result
	}

	if ret, yes := IsValkeyErr(result.Error()); yes {
		if addr, ok := ret.IsRedirect(); ok {
			// Update the latest redirect address before singleflight
			s.lastRedirectAddr = addr
			
			// Use singleflight to ensure only one redirect operation happens at a time
			// Only proceed if the address matches the latest seen redirect address
			if err := s.redirectCall.Do(ctx, func() error {
				// Double-check that this is still the latest redirect address
				if s.lastRedirectAddr == addr {
					return s.redirectToPrimary(addr)
				}
				// If address doesn't match, another redirect happened, skip this one
				return nil
			}); err != nil {
				// If redirect fails, return the original result
				return result
			}

			// Execute the command on the updated primary
			return s.primary.Do(ctx, cmd)
		}
	}

	return result
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
	resp = s.handleRedirect(ctx, cmd, resp)
	if ret, yes := IsValkeyErr(resp.Error()); yes {
		if _, ok := ret.IsRedirect(); ok && s.enableRedirect {
			// Use retryHandler to handle multiple redirects with context deadline
			if s.retryer.WaitOrSkipRetry(ctx, attempts, cmd, resp.Error()) {
				attempts++
				goto retry
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

	// Handle redirects for each command in the multi
	hasRedirect := false
	if s.enableRedirect {
		for i, result := range resp {
			if i < len(multi) {
				resp[i] = s.handleRedirect(ctx, multi[i], result)
				// Check if we still have a redirect error after handling
				if ret, yes := IsValkeyErr(resp[i].Error()); yes {
					if _, ok := ret.IsRedirect(); ok {
						hasRedirect = true
					}
				}
			}
		}
		// If any command still has a redirect error, retry the entire multi
		if hasRedirect {
			if s.retryer.WaitOrSkipRetry(ctx, attempts, multi[0], resp[0].Error()) {
				attempts++
				goto retry
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

	// Check if there's a redirect error in the stream
	if s.enableRedirect && stream.Error() != nil {
		if ret, yes := IsValkeyErr(stream.Error()); yes {
			if addr, ok := ret.IsRedirect(); ok {
				// Update the latest redirect address before singleflight
				s.lastRedirectAddr = addr
				
				// Use singleflight to ensure only one redirect operation happens at a time
				// Only proceed if the address matches the latest seen redirect address
				if err := s.redirectCall.Do(ctx, func() error {
					// Double-check that this is still the latest redirect address
					if s.lastRedirectAddr == addr {
						return s.redirectToPrimary(addr)
					}
					// If address doesn't match, another redirect happened, skip this one
					return nil
				}); err != nil {
					// If redirect fails, return the original stream
					return stream
				}

				// Execute the command on the updated primary
				return s.primary.DoStream(ctx, cmd)
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

	// Check if there's a redirect error in the stream
	if s.enableRedirect && stream.Error() != nil {
		if ret, yes := IsValkeyErr(stream.Error()); yes {
			if addr, ok := ret.IsRedirect(); ok {
				// Update the latest redirect address before singleflight
				s.lastRedirectAddr = addr
				
				// Use singleflight to ensure only one redirect operation happens at a time
				// Only proceed if the address matches the latest seen redirect address
				if err := s.redirectCall.Do(ctx, func() error {
					// Double-check that this is still the latest redirect address
					if s.lastRedirectAddr == addr {
						return s.redirectToPrimary(addr)
					}
					// If address doesn't match, another redirect happened, skip this one
					return nil
				}); err != nil {
					// If redirect fails, return the original stream
					return stream
				}

				// Execute the command on the updated primary
				return s.primary.DoMultiStream(ctx, multi...)
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
