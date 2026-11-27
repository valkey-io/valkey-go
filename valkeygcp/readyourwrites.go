package valkeygcp

import (
	"context"
	"strings"
	"sync/atomic"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/internal/cmds"
)

// defaultReadYourWritesWindow is the window used by WithReadYourWrites when the given window is not positive.
const defaultReadYourWritesWindow = 500 * time.Millisecond

// WithReadYourWrites makes the client send reads of a hash slot to its primary for window after the client
// writes to that slot, so that callers read their own writes despite asynchronous replication.
// A window <= 0 means 500ms. Only writes sent with Do and DoMulti are tracked.
// It takes effect with NewClient, not ResolveClientOption, and only when reads can go to replicas: with
// WithZoneAwareReads, a reader endpoint or your own ClientOption.SendToReplicas.
func WithReadYourWrites(window time.Duration) Option {
	if window <= 0 {
		window = defaultReadYourWritesWindow
	}
	return func(c *config) {
		c.readYourWritesWindow = window
	}
}

// readYourWrites wraps a valkey.Client that reads from replicas, a cluster client or a client with a reader endpoint,
// so that reads of the hash slots the client has just written to go to the primary.
type readYourWrites struct {
	valkey.Client
	start  time.Time
	window time.Duration
	// until holds, for each hash slot, the time since start until which reads of the slot go to the primary.
	until [16384]atomic.Int64
}

// newReadYourWrites creates the client for option with newClient, wrapped for read-your-writes. The clients newClient
// creates share the slots it pins.
func newReadYourWrites(option valkey.ClientOption, window time.Duration,
	newClient func(valkey.ClientOption) (valkey.Client, error)) (valkey.Client, error) {
	r := &readYourWrites{start: time.Now(), window: window}
	option.SendToReplicas = r.sendToReplicas(option.SendToReplicas)
	client, err := newClient(option)
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
		now := int64(time.Since(r.start))
		if s, ok := slot(cmd); ok && r.until[s].Load() > now {
			return false
		}
		keys, step := otherKeys(cmd)
		for i := 0; i < len(keys); i += step {
			if r.until[cmds.Slot(keys[i])].Load() > now {
				return false
			}
		}
		// A read built with cmds.NoSlot alone may be of a key in slot 0 (see slot). Look for the key only while slot 0
		// is pinned, so that reads without keys, such as SCAN, still go to replicas.
		if keys == nil && cmd.Slot() == cmds.NoSlot && r.until[0].Load() > now && !keyless(cmd) {
			return false
		}
		return true
	}
}

// pin sends reads of the slots written by multi, and of slots, to the primary for the window.
// Commands without keys are ignored.
func (r *readYourWrites) pin(slots []uint16, multi ...valkey.Completed) {
	until := int64(time.Since(r.start) + r.window)
	for _, cmd := range multi {
		if s, ok := slot(cmd); ok && cmd.IsWrite() {
			r.until[s].Store(until)
		}
	}
	for _, s := range slots {
		r.until[s].Store(until)
	}
}

// slot returns the hash slot of the keys of cmd, or false if cmd has no keys.
// Cluster clients build commands with their slot; other clients, such as clients with a reader endpoint,
// build them with the slot of one of their keys and the cmds.NoSlot flag: see otherKeys for the others.
// Those clients build a command whose key is in slot 0 like one without keys, with cmds.NoSlot alone, so slot returns
// false for both: appendOtherSlots and sendToReplicas handle slot 0 for them.
func slot(cmd valkey.Completed) (int, bool) {
	s := cmd.Slot()
	if s == cmds.InitSlot || s == cmds.NoSlot {
		return 0, false
	}
	return int(s &^ cmds.NoSlot), true
}

// appendOtherSlots appends to slots the hash slots of the keys of cmd that slot misses, if cmd is a write.
// It reads the arguments of cmd, so it must run before cmd is sent: valkey-go recycles them once cmd is done.
func appendOtherSlots(slots []uint16, cmd valkey.Completed) []uint16 {
	if !cmd.IsWrite() {
		return slots
	}
	keys, step := otherKeys(cmd)
	for i := 0; i < len(keys); i += step {
		slots = append(slots, cmds.Slot(keys[i]))
	}
	// A write built with cmds.NoSlot alone may be to a key in slot 0 (see slot), unless it has no arguments. It pins
	// slot 0 without looking for the key, as keyless does for reads: writes such as PUBLISH can have long arguments.
	if keys == nil && cmd.Slot() == cmds.NoSlot && len(cmd.Commands()) > 1 {
		slots = append(slots, 0)
	}
	return slots
}

// keyless reports whether cmd, which a client other than a cluster client built with cmds.NoSlot alone, has no keys
// rather than a key in slot 0 (see slot): none of its arguments is in slot 0.
// It reads the arguments of cmd, so it must run before cmd is sent.
func keyless(cmd valkey.Completed) bool {
	c := cmd.Commands()
	for i := 1; i < len(c); i++ {
		if cmds.Slot(c[i]) == 0 {
			return false
		}
	}
	return true
}

// otherKeys returns keys[0], keys[step], keys[2*step] and so on: the keys of cmd that slot misses, among other
// arguments of cmd. A cluster client builds a command with the slot all its keys share, so it returns none for them.
// Other clients build a command with the slot of one of its keys only, so for a command that takes more than one
// key, it returns the arguments that can be keys, skipping values and scripts.
func otherKeys(cmd valkey.Completed) (keys []string, step int) {
	if cmd.Slot()&cmds.NoSlot == 0 {
		return nil, 1
	}
	c := cmd.Commands()
	if len(c) == 0 {
		return nil, 1
	}
	switch strings.ToUpper(c[0]) {
	case "MSET", "MSETNX":
		return c[1:], 2 // key value [key value ...]
	case "MSETEX":
		return c[min(2, len(c)):], 2 // numkeys key value [key value ...] [options]
	case "JSON.MSET", "TS.MADD":
		return c[1:], 3 // key path value [...] and key timestamp value [...]
	case "EVAL", "EVALSHA", "EVAL_RO", "EVALSHA_RO", "FCALL", "FCALL_RO", "TFCALL", "TFCALLASYNC":
		return c[min(3, len(c)):], 1 // script numkeys [key ...] [arg ...]
	case "BITOP", "BLMOVE", "BLMPOP", "BLPOP", "BRPOP", "BRPOPLPUSH", "BZMPOP", "BZPOPMAX", "BZPOPMIN",
		"CMS.MERGE", "COPY", "DEL", "EXISTS", "GEORADIUS", "GEORADIUSBYMEMBER", "GEOSEARCHSTORE", "JSON.MGET",
		"LCS", "LMOVE", "LMPOP", "MGET", "MIGRATE", "PFCOUNT", "PFMERGE", "RENAME", "RENAMENX", "RPOPLPUSH",
		"SDIFF", "SDIFFSTORE", "SINTER", "SINTERCARD", "SINTERSTORE", "SMOVE", "SORT", "SUNION", "SUNIONSTORE",
		"TDIGEST.MERGE", "TOUCH", "TS.CREATERULE", "TS.DELETERULE", "UNLINK", "XREAD", "XREADGROUP",
		"ZDIFF", "ZDIFFSTORE", "ZINTER", "ZINTERCARD", "ZINTERSTORE", "ZMPOP", "ZRANGESTORE", "ZUNION", "ZUNIONSTORE":
		// Their arguments that are not keys are short, such as numbers and options.
		return c[1:], 1
	}
	return nil, 1
}

func (r *readYourWrites) Do(ctx context.Context, cmd valkey.Completed) valkey.ValkeyResult {
	var buf [8]uint16
	others := appendOtherSlots(buf[:0], cmd)
	resp := r.Client.Do(ctx, cmd)
	// Pin after the reply so that the window starts when the write is done.
	// Failed writes are pinned too, because they may have been applied.
	r.pin(others, cmd)
	return resp
}

func (r *readYourWrites) DoMulti(ctx context.Context, multi ...valkey.Completed) []valkey.ValkeyResult {
	var buf [16]uint16
	others := buf[:0]
	for _, cmd := range multi {
		others = appendOtherSlots(others, cmd)
	}
	// Pin before sending too, so that reads in the same batch also go to the primary and see the writes ahead of them.
	r.pin(others, multi...)
	resps := r.Client.DoMulti(ctx, multi...)
	r.pin(others, multi...)
	return resps
}
