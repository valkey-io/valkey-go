package valkey

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"testing"
	"time"
)

func test(t *testing.T, storeFn func() CacheStore) {
	t.Run("Flight and Update", func(t *testing.T) {
		var err error
		var now = time.Now()
		var store = storeFn()

		v, e := store.Flight("key", "cmd", time.Millisecond*100, now)
		if v.typ != 0 || e != nil {
			t.Fatal("first flight should return empty ValkeyMessage and nil CacheEntry")
		}

		v, e = store.Flight("key", "cmd", time.Millisecond*100, now)
		if v.typ != 0 || e == nil {
			t.Fatal("flights before Update should return empty ValkeyMessage and non-nil CacheEntry")
		}

		store.Delete([]ValkeyMessage{strmsg('+', "key")}) // Delete should not affect pending CacheEntry

		v2, e2 := store.Flight("key", "cmd", time.Millisecond*100, now)
		if v2.typ != 0 || e != e2 {
			t.Fatal("flights before Update should return empty ValkeyMessage and the same CacheEntry, not be affected by Delete")
		}

		v = strmsg('+', "val")
		v.setExpireAt(now.Add(time.Second).UnixMilli())
		if pttl := store.Update("key", "cmd", v); pttl < now.Add(90*time.Millisecond).UnixMilli() || pttl > now.Add(100*time.Millisecond).UnixMilli() {
			t.Fatal("Update should return a desired pttl")
		}

		v2, err = e.Wait(context.Background())
		if v2.typ != v.typ || v2.string() != v.string() || err != nil {
			t.Fatal("unexpected cache response")
		}
		if pttl := v2.CachePXAT(); pttl < now.Add(90*time.Millisecond).UnixMilli() || pttl > now.Add(100*time.Millisecond).UnixMilli() {
			t.Fatal("CachePXAT should return a desired pttl")
		}

		v2, _ = store.Flight("key", "cmd", time.Millisecond*100, now)
		if v2.typ != v.typ || v2.string() != v.string() {
			t.Fatal("flights after Update should return updated ValkeyMessage")
		}
		if pttl := v2.CachePXAT(); pttl < now.Add(90*time.Millisecond).UnixMilli() || pttl > now.Add(100*time.Millisecond).UnixMilli() {
			t.Fatal("CachePXAT should return a desired pttl")
		}

		store.Delete([]ValkeyMessage{strmsg('+', "key")})
		v, e = store.Flight("key", "cmd", time.Millisecond*100, now)
		if v.typ != 0 || e != nil {
			t.Fatal("flights after Delete should return empty ValkeyMessage and nil CacheEntry")
		}
	})

	t.Run("Flight and Cancel", func(t *testing.T) {
		var err error
		var now = time.Now()
		var store = storeFn()

		v, e := store.Flight("key", "cmd", time.Millisecond*100, now)
		if v.typ != 0 || e != nil {
			t.Fatal("first flight should return empty ValkeyMessage and nil CacheEntry")
		}

		v, e = store.Flight("key", "cmd", time.Millisecond*100, now)
		if v.typ != 0 || e == nil {
			t.Fatal("flights before Update should return empty ValkeyMessage and non-nil CacheEntry")
		}

		store.Delete([]ValkeyMessage{strmsg('+', "key")}) // Delete should not affect pending CacheEntry

		v2, e2 := store.Flight("key", "cmd", time.Millisecond*100, now)
		if v2.typ != 0 || e != e2 {
			t.Fatal("flights before Update should return empty ValkeyMessage and the same CacheEntry, not be affected by Delete")
		}

		store.Cancel("key", "cmd", errors.New("err"))

		v2, err = e.Wait(context.Background())
		if err.Error() != "err" {
			t.Fatal("unexpected cache response")
		}

		v, e = store.Flight("key", "cmd", time.Millisecond*100, now)
		if v.typ != 0 || e != nil {
			t.Fatal("flights after Cancel should return empty ValkeyMessage and nil CacheEntry")
		}
	})

	t.Run("Flight and Delete", func(t *testing.T) {
		var now = time.Now()
		var store = storeFn()

		for _, deletions := range [][]ValkeyMessage{
			{strmsg('+', "key")},
			nil,
		} {
			store.Flight("key", "cmd1", time.Millisecond*100, now)
			store.Flight("key", "cmd2", time.Millisecond*100, now)
			store.Update("key", "cmd1", strmsg('+', "val"))
			store.Update("key", "cmd2", strmsg('+', "val"))

			store.Delete(deletions)

			if v, e := store.Flight("key", "cmd1", time.Millisecond*100, now); v.typ != 0 || e != nil {
				t.Fatal("flight after delete should return empty ValkeyMessage and nil CacheEntry")
			}

			if v, e := store.Flight("key", "cmd2", time.Millisecond*100, now); v.typ != 0 || e != nil {
				t.Fatal("flight after delete should return empty ValkeyMessage and nil CacheEntry")
			}
		}
	})

	t.Run("Flight and TTL", func(t *testing.T) {
		var now = time.Now()
		var store = storeFn()

		v, e := store.Flight("key", "cmd", time.Second, now)
		if v.typ != 0 || e != nil {
			t.Fatal("first flight should return empty ValkeyMessage and nil CacheEntry")
		}

		v = strmsg('+', "val")
		v.setExpireAt(now.Add(time.Millisecond).UnixMilli())
		store.Update("key", "cmd", v)

		v, e = store.Flight("key", "cmd", time.Second, now.Add(time.Millisecond))
		if v.typ != 0 || e != nil {
			t.Fatal("flight after TTL should return empty ValkeyMessage and nil CacheEntry")
		}
	})

	t.Run("Flight and Close", func(t *testing.T) {
		var now = time.Now()
		var store = storeFn()

		_, _ = store.Flight("key", "cmd", time.Millisecond*100, now)
		_, e := store.Flight("key", "cmd", time.Millisecond*100, now)

		store.Close(errors.New("err"))

		if _, err := e.Wait(context.Background()); err.Error() != "err" {
			t.Fatal("unexpected cache response")
		}

		_, e = store.Flight("key", "cmd", time.Millisecond*100, now)
		if e != nil {
			t.Fatal("flight after Close should return empty ValkeyMessage and nil CacheEntry")
		}
	})

	t.Run("Flight timeout", func(t *testing.T) {
		var now = time.Now()
		var store = storeFn()

		_, _ = store.Flight("key", "cmd", time.Millisecond*100, now)
		_, e := store.Flight("key", "cmd", time.Millisecond*100, now)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if _, err := e.Wait(ctx); err != context.Canceled {
			t.Fatal("Wait should honor context")
		}
	})
}

func TestCacheStore(t *testing.T) {
	t.Run("LRUCacheStore", func(t *testing.T) {
		test(t, func() CacheStore {
			return newLRU(CacheStoreOption{CacheSizeEachConn: DefaultCacheBytes})
		})
	})
	t.Run("SimpleCache", func(t *testing.T) {
		test(t, func() CacheStore {
			return NewSimpleCacheAdapter(&simple{store: map[string]ValkeyMessage{}})
		})
	})
}

type simple struct {
	store map[string]ValkeyMessage
}

func (s *simple) Get(key string) ValkeyMessage {
	return s.store[key]
}

func (s *simple) Set(key string, val ValkeyMessage) {
	s.store[key] = val
}

func (s *simple) Del(key string) {
	delete(s.store, key)
}

func (s *simple) Flush() {
	s.store = nil
}

var (
	cacheDynamicKeys1000 = func() []string {
		ks := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			ks[i] = "ckey_" + strconv.Itoa(i)
		}
		return ks
	}()
	cacheDynamicCmds1000 = func() []string {
		cs := make([]string, 1000)
		for i := 0; i < 1000; i++ {
			cs[i] = "GET " + cacheDynamicKeys1000[i]
		}
		return cs
	}()
)

// Benchmark_Cache_DoCache_Hit measures in-memory LRU client cache hit latency with dynamic keys and 1KB cached value.
func Benchmark_Cache_DoCache_Hit(b *testing.B) {
	store := newLRU(CacheStoreOption{CacheSizeEachConn: DefaultCacheBytes})
	now := time.Now()
	val1KB := strings.Repeat("a", 1024)
	for i := 0; i < 1000; i++ {
		msg := strmsg('+', val1KB)
		msg.setExpireAt(now.Add(time.Hour).UnixMilli())
		store.Flight(cacheDynamicKeys1000[i], cacheDynamicCmds1000[i], time.Minute, now)
		store.Update(cacheDynamicKeys1000[i], cacheDynamicCmds1000[i], msg)
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % 1000
		v, _ := store.Flight(cacheDynamicKeys1000[idx], cacheDynamicCmds1000[idx], time.Minute, now)
		_ = v
	}
}

// Benchmark_Cache_DoCache_Miss measures flight miss and server store population using dynamic keys and payload gradient (64B, 1KB, 64KB).
func Benchmark_Cache_DoCache_Miss(b *testing.B) {
	store := newLRU(CacheStoreOption{CacheSizeEachConn: DefaultCacheBytes})
	now := time.Now()
	msgs := []ValkeyMessage{
		strmsg('+', strings.Repeat("a", 64)),
		strmsg('+', strings.Repeat("b", 1024)),
		strmsg('+', strings.Repeat("c", 64*1024)),
	}
	for i := range msgs {
		msgs[i].setExpireAt(now.Add(time.Hour).UnixMilli())
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		idx := i % 1000
		k := cacheDynamicKeys1000[idx]
		cmd := cacheDynamicCmds1000[idx]
		v, entry := store.Flight(k, cmd, time.Minute, now)
		if v.typ == 0 && entry == nil {
			store.Update(k, cmd, msgs[i%3])
		}
	}
}

// Benchmark_Cache_MGetCache measures mapping multi-key responses from cache across collection cardinality (10, 100 keys).
func Benchmark_Cache_MGetCache(b *testing.B) {
	store := newLRU(CacheStoreOption{CacheSizeEachConn: DefaultCacheBytes})
	now := time.Now()
	val1KB := strings.Repeat("a", 1024)
	for i := 0; i < 100; i++ {
		msg := strmsg('+', val1KB)
		msg.setExpireAt(now.Add(time.Hour).UnixMilli())
		store.Flight(cacheDynamicKeys1000[i], cacheDynamicCmds1000[i], time.Minute, now)
		store.Update(cacheDynamicKeys1000[i], cacheDynamicCmds1000[i], msg)
	}
	keys10 := cacheDynamicKeys1000[:10]
	keys100 := cacheDynamicKeys1000[:100]

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ks := keys10
		if i%2 == 1 {
			ks = keys100
		}
		for j, k := range ks {
			v, _ := store.Flight(k, cacheDynamicCmds1000[j], time.Minute, now)
			_ = v
		}
	}
}

// Benchmark_Cache_Invalidation measures server-assisted Pub/Sub cache invalidation batch across collection cardinality (10, 100, 1,000 dynamic keys).
func Benchmark_Cache_Invalidation(b *testing.B) {
	store := newLRU(CacheStoreOption{CacheSizeEachConn: DefaultCacheBytes})
	now := time.Now()
	delMsgs := make([]ValkeyMessage, 1000)
	for i := 0; i < 1000; i++ {
		msg := strmsg('+', "val")
		msg.setExpireAt(now.Add(time.Hour).UnixMilli())
		store.Flight(cacheDynamicKeys1000[i], "GET "+cacheDynamicKeys1000[i], time.Minute, now)
		store.Update(cacheDynamicKeys1000[i], "GET "+cacheDynamicKeys1000[i], msg)
		delMsgs[i] = strmsg('+', cacheDynamicKeys1000[i])
	}
	del10 := delMsgs[:10]
	del100 := delMsgs[:100]
	del1000 := delMsgs[:1000]

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		switch i % 3 {
		case 0:
			store.Delete(del10)
		case 1:
			store.Delete(del100)
		default:
			store.Delete(del1000)
		}
	}
}

func Benchmark_Cache_LocalHit_Latency(b *testing.B) {
	lru := newLRU(CacheStoreOption{CacheSizeEachConn: DefaultCacheBytes})
	lru.Flight("cached_key", "GET", 10*time.Second, time.Now())
	lru.Update("cached_key", "GET", strmsg('+', "OK"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if v, _ := lru.Flight("cached_key", "GET", 10*time.Second, time.Now()); v.typ == 0 {
			b.Fatal("cache miss")
		}
	}
}

func Benchmark_Cache_Miss_And_Server_Invalidate(b *testing.B) {
	lru := newLRU(CacheStoreOption{CacheSizeEachConn: DefaultCacheBytes})

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "miss_key_" + strconv.Itoa(i%100)
		lru.Flight(key, "GET", 10*time.Second, time.Now())
		lru.Update(key, "GET", strmsg('+', "OK"))
		if i%100 == 99 {
			var msgs []ValkeyMessage
			for j := 0; j < 100; j++ {
				msgs = append(msgs, strmsg('$', "miss_key_" + strconv.Itoa(j)))
			}
			lru.Delete(msgs)
		}
	}
}
