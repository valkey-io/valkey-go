package valkey

import "context"

type staticClientTTLCtxKey struct{}

// WithStaticClientTTL returns a context that makes the ttl passed to
// DoCache / DoMultiCache authoritative for the L1 entry's lifetime.
//
// Without this flag, DoCache caps the L1 TTL on each miss against the
// server's remaining PTTL via a MULTI/PTTL/EXEC wrapper around the
// cacheable command. With this flag, the wrapper is skipped: the wire
// is reduced from [CLIENT CACHING YES, MULTI, PTTL, cmd, EXEC] to
// [CLIENT CACHING YES, cmd], and the L1 entry expires at
// populate_time + ttl. CLIENT TRACKING invalidations and LRU eviction
// still purge entries; nothing extends them.
//
// Intended for callers who have an active invalidation mechanism on
// Valkey — writes that explicitly invalidate the affected cached keys —
// which propagates to the client via CLIENT TRACKING and bounds
// staleness, so the per-read PTTL clamp is not needed.
//
// MGET / JSON.MGET fall through to the standard wire shape; this opt-in
// has no effect on them.
func WithStaticClientTTL(ctx context.Context) context.Context {
	return context.WithValue(ctx, staticClientTTLCtxKey{}, true)
}

func hasStaticClientTTL(ctx context.Context) bool {
	v, _ := ctx.Value(staticClientTTLCtxKey{}).(bool)
	return v
}
