package valkey

import (
	"sync"
	"testing"
	"time"
)

// The "stash" lets a caller attach an unmarshalled value to a cached
// ValkeyMessage (CachePut) and read it back on later cache hits (CacheGet),
// avoiding repeated parsing of the same response. These tests validate the two
// load-bearing properties:
//
//  1. the stash is shared across every copy returned for a live cache entry, so
//     a CachePut is visible to subsequent hits; and
//  2. a refill after invalidation/expiry installs a FRESH (empty) stash, so a
//     stale parse can never be served for a newer response.
//
// They run against both CacheStore implementations: the default *lru and the
// SimpleCache adapter.

// stashStores returns the CacheStore implementations to exercise. Each factory
// yields a fresh, empty store.
func stashStores() []struct {
	name string
	make func() CacheStore
} {
	return []struct {
		name string
		make func() CacheStore
	}{
		{"LRU", func() CacheStore {
			return newLRU(CacheStoreOption{CacheSizeEachConn: entryMinSize * 100})
		}},
		{"SimpleCache", func() CacheStore {
			return NewSimpleCacheAdapter(&simple{store: map[string]ValkeyMessage{}})
		}},
	}
}

// cacheMsg is a minimal, long-lived cacheable message (non-zero type, far-future
// expiry) so it stays a valid hit for the duration of a test.
func cacheMsg() ValkeyMessage {
	m := ValkeyMessage{typ: 1}
	m.setExpireAt(time.Now().Add(time.Minute).UnixMilli())
	return m
}

// fill mimics a cache miss followed by the server response: Flight prepares the
// pending entry, Update fills it (installing a fresh stash).
func fill(store CacheStore, key, cmd string) {
	store.Flight(key, cmd, TTL, time.Now())
	store.Update(key, cmd, cacheMsg())
}

func hit(t *testing.T, store CacheStore, key, cmd string) ValkeyMessage {
	t.Helper()
	// A hit is v.typ != 0; the caller (pipe.go) ignores the returned entry in
	// that case. The default *lru returns a non-nil entry alongside the value on
	// a hit, while the SimpleCache adapter returns nil — so only v.typ matters.
	v, _ := store.Flight(key, cmd, TTL, time.Now())
	if v.typ == 0 {
		t.Fatalf("expected a cache hit for %s/%s, got a miss", key, cmd)
	}
	return v
}

// TestCacheStash_SharedAcrossHits: a value put on one returned copy is visible
// from an independently-returned copy of the same live entry, and the entry is
// born with an empty stash.
func TestCacheStash_SharedAcrossHits(t *testing.T) {
	for _, s := range stashStores() {
		t.Run(s.name, func(t *testing.T) {
			store := s.make()
			fill(store, "k", "GET")

			first := hit(t, store, "k", "GET")
			if got := first.CacheGet(); got != nil {
				t.Fatalf("a freshly filled entry must have an empty stash, got %v", got)
			}

			want := []string{"tenant-a", "tenant-b"}
			first.CachePut(want)

			// A separately-returned copy of the same entry sees the put.
			second := hit(t, store, "k", "GET")
			got, ok := second.CacheGet().([]string)
			if !ok || len(got) != 2 || got[0] != "tenant-a" || got[1] != "tenant-b" {
				t.Fatalf("CacheGet from a second hit = %v, want %v", second.CacheGet(), want)
			}
		})
	}
}

// TestCacheStash_ResetsOnInvalidation is the critical correctness property: once
// an entry is invalidated and refilled (a new server response), the stash is
// empty again — so the caller re-parses and never serves the old parse for the
// new value.
func TestCacheStash_ResetsOnInvalidation(t *testing.T) {
	for _, s := range stashStores() {
		t.Run(s.name, func(t *testing.T) {
			store := s.make()
			fill(store, "k", "GET")

			v := hit(t, store, "k", "GET")
			v.CachePut("parse-of-old-value")
			if got := v.CacheGet(); got != "parse-of-old-value" {
				t.Fatalf("CacheGet before invalidation = %v, want the stored parse", got)
			}

			// Server pushes an invalidation for key "k".
			store.Delete([]ValkeyMessage{strmsg(0x0, "k")})

			// The entry is gone: the next Flight is a miss.
			if mv, _ := store.Flight("k", "GET", TTL, time.Now()); mv.typ != 0 {
				t.Fatalf("entry should be a miss after invalidation, got typ=%d", mv.typ)
			}
			// Refill with the new response.
			store.Update("k", "GET", cacheMsg())

			// The refilled entry must NOT carry the stale parse.
			fresh := hit(t, store, "k", "GET")
			if got := fresh.CacheGet(); got != nil {
				t.Fatalf("stash must reset on refill; got stale value %v", got)
			}
		})
	}
}

// TestCacheStash_PerKeyIsolation: each (key,cmd) entry has its own stash.
func TestCacheStash_PerKeyIsolation(t *testing.T) {
	for _, s := range stashStores() {
		t.Run(s.name, func(t *testing.T) {
			store := s.make()
			fill(store, "k1", "GET")
			fill(store, "k2", "GET")

			v1 := hit(t, store, "k1", "GET")
			v1.CachePut("one")
			v2 := hit(t, store, "k2", "GET")
			v2.CachePut("two")

			g1 := hit(t, store, "k1", "GET")
			if got := g1.CacheGet(); got != "one" {
				t.Fatalf("k1 stash = %v, want \"one\"", got)
			}
			g2 := hit(t, store, "k2", "GET")
			if got := g2.CacheGet(); got != "two" {
				t.Fatalf("k2 stash = %v, want \"two\"", got)
			}
		})
	}
}

// TestCacheStash_Concurrent exercises the atomic pointer under the race
// detector: many goroutines Put/Get the shared stash concurrently.
func TestCacheStash_Concurrent(t *testing.T) {
	for _, s := range stashStores() {
		t.Run(s.name, func(t *testing.T) {
			store := s.make()
			fill(store, "k", "GET")
			v := hit(t, store, "k", "GET")

			const n = 16
			var wg sync.WaitGroup
			for i := 0; i < n; i++ {
				wg.Add(2)
				go func(val int) { defer wg.Done(); v.CachePut(val) }(i)
				go func() { defer wg.Done(); _ = v.CacheGet() }()
			}
			wg.Wait()

			// After all puts settle, the stash holds one of the written values.
			got, ok := v.CacheGet().(int)
			if !ok || got < 0 || got >= n {
				t.Fatalf("after concurrent puts, CacheGet = %v, want an int in [0,%d)", v.CacheGet(), n)
			}
		})
	}
}

// TestCacheStash_NonCacheMessage: CachePut/CacheGet are safe no-ops on messages
// that didn't come from the client-side cache (no stash installed).
func TestCacheStash_NonCacheMessage(t *testing.T) {
	var m ValkeyMessage // no stash
	m.CachePut("ignored")
	if got := m.CacheGet(); got != nil {
		t.Fatalf("CacheGet on a non-cache message = %v, want nil", got)
	}

	// The same via the ValkeyResult delegation.
	r := ValkeyResult{val: ValkeyMessage{}}
	r.CachePut("ignored")
	if got := r.CacheGet(); got != nil {
		t.Fatalf("ValkeyResult.CacheGet on a non-cache message = %v, want nil", got)
	}
}
