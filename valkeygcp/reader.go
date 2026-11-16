package valkeygcp

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
)

// noReplicas is in the error a Memorystore reader endpoint returns, before it closes the connection, when the
// instance has no replicas: "ERR no replicas found". There is no error code for it, so it is looked for in the text of
// server errors, ignoring case.
const noReplicas = "no replicas"

// The delays between the tries of a primaryUntilReplicas, variables so that tests can shorten them.
var (
	readerRetryMin = 10 * time.Second
	readerRetryMax = 5 * time.Minute
)

// connect creates the client for option, wrapped for read-your-writes if readYourWritesWindow is set and reads can go
// to replicas. Without SendToReplicas, every read goes to the primary already.
func connect(option valkey.ClientOption, readYourWritesWindow time.Duration) (valkey.Client, error) {
	if readYourWritesWindow == 0 || option.SendToReplicas == nil {
		return connectReader(option)
	}
	return newReadYourWrites(option, readYourWritesWindow, connectReader)
}

// connectReader creates the client for option. Memorystore keeps the reader endpoint of an instance without replicas
// but refuses its connections, and never sends them to the primary. So if option has a reader endpoint that refuses
// connections because the instance has no replicas, connectReader returns a primaryUntilReplicas.
func connectReader(option valkey.ClientOption) (valkey.Client, error) {
	client, err := valkey.NewClient(option)
	if err == nil || len(option.Standalone.ReplicaAddress) == 0 || !isNoReplicas(err) {
		return client, err
	}
	primary := option
	primary.Standalone.ReplicaAddress = nil
	primary.ForceSingleClient = true // the primary endpoint of a Cluster Mode Disabled instance is not a cluster
	if client, err = valkey.NewClient(primary); err != nil {
		return nil, err
	}
	return newPrimaryUntilReplicas(client, option), nil
}

// isNoReplicas reports whether err is the error of a reader endpoint without replicas.
func isNoReplicas(err error) bool {
	var verr *valkey.ValkeyError
	return errors.As(err, &verr) && strings.Contains(strings.ToLower(verr.Error()), noReplicas)
}

// primaryUntilReplicas is the client of an instance whose reader endpoint refused connections because the instance
// had no replicas. It sends commands to the primary endpoint, and creates the client with the reader endpoint again,
// readerRetryMin and then up to readerRetryMax apart, until it connects: new commands then go through that client.
// The client of the primary endpoint stays open until Close, for the dedicated clients, streams and subscriptions
// started on it.
type primaryUntilReplicas struct {
	current atomic.Pointer[valkey.Client] // primary, then reader
	primary valkey.Client
	stop    chan struct{} // closed by Close
	mu      sync.Mutex
	reader  valkey.Client // the client with the reader endpoint, once it connects
	closed  bool
}

var _ valkey.Client = (*primaryUntilReplicas)(nil)

func newPrimaryUntilReplicas(primary valkey.Client, option valkey.ClientOption) *primaryUntilReplicas {
	p := &primaryUntilReplicas{primary: primary, stop: make(chan struct{})}
	p.current.Store(&primary)
	go p.retry(option, readerRetryMin, readerRetryMax)
	return p
}

// retry creates the client for option until it connects or p is closed, waiting delay before each try, doubled after
// each try up to maxDelay.
func (p *primaryUntilReplicas) retry(option valkey.ClientOption, delay, maxDelay time.Duration) {
	for ; ; delay = min(2*delay, maxDelay) {
		select {
		case <-p.stop:
			return
		case <-time.After(delay):
		}
		client, err := valkey.NewClient(option)
		if err != nil {
			continue // still no replicas, or another error: try again later
		}
		if !p.use(client) {
			client.Close()
		}
		return
	}
}

// use makes new commands go through reader, unless p is closed.
func (p *primaryUntilReplicas) use(reader valkey.Client) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return false
	}
	p.reader = reader
	p.current.Store(&reader)
	return true
}

func (p *primaryUntilReplicas) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return
	}
	p.closed = true
	close(p.stop)
	if p.reader != nil {
		p.reader.Close()
	}
	p.primary.Close()
}

func (p *primaryUntilReplicas) client() valkey.Client { return *p.current.Load() }

func (p *primaryUntilReplicas) B() valkey.Builder { return p.client().B() }

func (p *primaryUntilReplicas) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	return p.client().Do(ctx, cmd)
}

func (p *primaryUntilReplicas) DoMulti(ctx context.Context, multi ...valkey.Completed) []valkey.ValkeyResult {
	return p.client().DoMulti(ctx, multi...)
}

func (p *primaryUntilReplicas) DoStream(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResultStream {
	return p.client().DoStream(ctx, cmd)
}

func (p *primaryUntilReplicas) DoMultiStream(ctx context.Context, multi ...valkey.Completed) valkey.MultiValkeyResultStream {
	return p.client().DoMultiStream(ctx, multi...)
}

func (p *primaryUntilReplicas) DoCache(ctx context.Context, cmd valkey.Cacheable, ttl time.Duration) valkey.ValkeyResult {
	return p.client().DoCache(ctx, cmd, ttl)
}

func (p *primaryUntilReplicas) DoMultiCache(ctx context.Context, multi ...valkey.CacheableTTL) []valkey.ValkeyResult {
	return p.client().DoMultiCache(ctx, multi...)
}

func (p *primaryUntilReplicas) Receive(ctx context.Context, subscribe valkey.Completed, fn func(valkey.PubSubMessage)) error {
	return p.client().Receive(ctx, subscribe, fn)
}

func (p *primaryUntilReplicas) Dedicated(fn func(valkey.DedicatedClient) error) error {
	return p.client().Dedicated(fn)
}

func (p *primaryUntilReplicas) Dedicate() (valkey.DedicatedClient, func()) {
	return p.client().Dedicate()
}

func (p *primaryUntilReplicas) Nodes() map[string]valkey.Client { return p.client().Nodes() }

func (p *primaryUntilReplicas) Mode() valkey.ClientMode { return p.client().Mode() }
