package valkeygcp

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/internal/cmds"
)

// readYourWrites wraps a valkey.Client that reads from replicas, a cluster client or a client with a reader endpoint,

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
		s, ok := slot(cmd)
		return !ok || r.until[s].Load() <= int64(time.Since(r.start))
	}
}

// pin sends reads of the slots written by multi to the primary for the window.
// Commands without keys are ignored.
func (r *readYourWrites) pin(multi ...valkey.Completed) {
	until := int64(time.Since(r.start) + r.window)
	for _, cmd := range multi {
		if s, ok := slot(cmd); ok && cmd.IsWrite() {
			r.until[s].Store(until)
		}
	}
}

// slot returns the hash slot of the keys of cmd, or false if cmd has no keys.
// Cluster clients build commands with their slot; other clients, such as clients with a reader endpoint,
// build them with their slot and the cmds.NoSlot flag.
func slot(cmd valkey.Completed) (int, bool) {
	s := cmd.Slot()
	if s == cmds.InitSlot || s == cmds.NoSlot {
		return 0, false
	}
	return int(s &^ cmds.NoSlot), true
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
