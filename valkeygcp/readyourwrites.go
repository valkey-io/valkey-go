package valkeygcp

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
)

// readYourWrites wraps a cluster valkey.Client so that, for a window after the client writes to a hash slot,
// reads of that slot go to the primary instead of to replicas that may not have the write yet.
type readYourWrites struct {
	valkey.Client
	start  time.Time
	window time.Duration
	// until holds, for each hash slot, the time since start until which reads of the slot go to the primary.
	until [16384]atomic.Int64
}

func newReadYourWrites(option valkey.ClientOption, window time.Duration) (valkey.Client, error) {
	r := &readYourWrites{start: time.Now(), window: window}
	option.SendToReplicas = r.sendToReplicas(option.SendToReplicas)
	client, err := valkey.NewClient(option)
	if err != nil {
		return nil, err
	}
	r.Client = client
	return r, nil
}

// sendToReplicas wraps next so that reads of recently written slots go to the primary.
func (r *readYourWrites) sendToReplicas(next func(valkey.Completed) bool) func(valkey.Completed) bool {
	return func(cmd valkey.Completed) bool {
		if !next(cmd) {
			return false
		}
		s := int(cmd.Slot())
		return s >= len(r.until) || r.until[s].Load() <= int64(time.Since(r.start))
	}
}

// pin sends reads of the slots written by multi to the primary for the window.
// Commands without a cluster hash slot are ignored.
func (r *readYourWrites) pin(multi ...valkey.Completed) {
	until := int64(time.Since(r.start) + r.window)
	for _, cmd := range multi {
		if s := int(cmd.Slot()); s < len(r.until) && cmd.IsWrite() {
			r.until[s].Store(until)
		}
	}
}

func (r *readYourWrites) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	resp := r.Client.Do(ctx, cmd)
	// Pin after the reply so that the window starts when the write is done.
	// Failed writes are pinned too, because they may have been applied.
	r.pin(cmd)
	return resp
}

func (r *readYourWrites) DoMulti(ctx context.Context, multi ...valkey.Completed) []valkey.ValkeyResult {
	// Pin before sending too, so that reads in the same batch also go to the primary and see the writes ahead of them.
	r.pin(multi...)
	resps := r.Client.DoMulti(ctx, multi...)
	r.pin(multi...)
	return resps
}

