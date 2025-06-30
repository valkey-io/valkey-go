package valkey

import (
	"context"
	"errors"
	"iter"
	"time"

	intl "github.com/valkey-io/valkey-go/internal/cmds"
)

// MGetCache is a helper that consults the client-side caches with multiple keys by grouping keys within the same slot into multiple GETs
func MGetCache(client Client, ctx context.Context, ttl time.Duration, keys []string) (ret map[string]ValkeyMessage, err error) {
	if len(keys) == 0 {
		return make(map[string]ValkeyMessage), nil
	}
	if isCacheDisabled(client) {
		return MGet(client, ctx, keys)
	}
	cmds := mgetcachecmdsp.Get(len(keys), len(keys))
	defer mgetcachecmdsp.Put(cmds)
	for i := range cmds.s {
		cmds.s[i] = CT(client.B().Get().Key(keys[i]).Cache(), ttl)
	}
	return doMultiCache(client, ctx, cmds.s, keys)
}

func isCacheDisabled(client Client) bool {
	switch c := client.(type) {
	case *singleClient:
		return c.DisableCache
	case *standalone:
		return c.primary.DisableCache
	case *sentinelClient:
		return c.mOpt != nil && c.mOpt.DisableCache
	case *clusterClient:
		return c.opt != nil && c.opt.DisableCache
	}
	return false
}

// MGet is a helper that consults the valkey directly with multiple keys by grouping keys within the same slot into MGET or multiple GETs
func MGet(client Client, ctx context.Context, keys []string) (ret map[string]ValkeyMessage, err error) {
	if len(keys) == 0 {
		return make(map[string]ValkeyMessage), nil
	}

	switch client.(type) {
	case *singleClient, *standalone, *sentinelClient:
		return clientMGet(client, ctx, client.B().Mget().Key(keys...).Build(), keys)
	}

	cmds := mgetcmdsp.Get(len(keys), len(keys))
	defer mgetcmdsp.Put(cmds)
	for i := range cmds.s {
		cmds.s[i] = client.B().Get().Key(keys[i]).Build()
	}
	return doMultiGet(client, ctx, cmds.s, keys)
}

// MSet is a helper that consults the valkey directly with multiple keys by grouping keys within the same slot into MSETs or multiple SETs
func MSet(client Client, ctx context.Context, kvs map[string]string) map[string]error {
	if len(kvs) == 0 {
		return make(map[string]error)
	}

	switch client.(type) {
	case *singleClient, *standalone, *sentinelClient:
		return clientMSet(client, ctx, "MSET", kvs, make(map[string]error, len(kvs)))
	}

	cmds := mgetcmdsp.Get(0, len(kvs))
	defer mgetcmdsp.Put(cmds)
	for k, v := range kvs {
		cmds.s = append(cmds.s, client.B().Set().Key(k).Value(v).Build().Pin())
	}
	return doMultiSet(client, ctx, cmds.s)
}

// MDel is a helper that consults the valkey directly with multiple keys by grouping keys within the same slot into DELs
func MDel(client Client, ctx context.Context, keys []string) map[string]error {
	if len(keys) == 0 {
		return make(map[string]error)
	}

	switch client.(type) {
	case *singleClient, *standalone, *sentinelClient:
		return clientMDel(client, ctx, keys)
	}

	cmds := mgetcmdsp.Get(len(keys), len(keys))
	defer mgetcmdsp.Put(cmds)
	for i, k := range keys {
		cmds.s[i] = client.B().Del().Key(k).Build().Pin()
	}
	return doMultiSet(client, ctx, cmds.s)
}

// MSetNX is a helper that consults the valkey directly with multiple keys by grouping keys within the same slot into MSETNXs or multiple SETNXs
func MSetNX(client Client, ctx context.Context, kvs map[string]string) map[string]error {
	if len(kvs) == 0 {
		return make(map[string]error)
	}

	switch client.(type) {
	case *singleClient, *standalone, *sentinelClient:
		return clientMSet(client, ctx, "MSETNX", kvs, make(map[string]error, len(kvs)))
	}

	cmds := mgetcmdsp.Get(0, len(kvs))
	defer mgetcmdsp.Put(cmds)
	for k, v := range kvs {
		cmds.s = append(cmds.s, client.B().Set().Key(k).Value(v).Nx().Build().Pin())
	}
	return doMultiSet(client, ctx, cmds.s)
}

// JsonMGetCache is a helper that consults the client-side caches with multiple keys by grouping keys within the same slot into multiple JSON.GETs
func JsonMGetCache(client Client, ctx context.Context, ttl time.Duration, keys []string, path string) (ret map[string]ValkeyMessage, err error) {
	if len(keys) == 0 {
		return make(map[string]ValkeyMessage), nil
	}
	cmds := mgetcachecmdsp.Get(len(keys), len(keys))
	defer mgetcachecmdsp.Put(cmds)
	for i := range cmds.s {
		cmds.s[i] = CT(client.B().JsonGet().Key(keys[i]).Path(path).Cache(), ttl)
	}
	return doMultiCache(client, ctx, cmds.s, keys)
}

// JsonMGet is a helper that consults valkey directly with multiple keys by grouping keys within the same slot into JSON.MGETs or multiple JSON.GETs
func JsonMGet(client Client, ctx context.Context, keys []string, path string) (ret map[string]ValkeyMessage, err error) {
	if len(keys) == 0 {
		return make(map[string]ValkeyMessage), nil
	}

	switch client.(type) {
	case *singleClient, *standalone, *sentinelClient:
		return clientMGet(client, ctx, client.B().JsonMget().Key(keys...).Path(path).Build(), keys)
	}

	cmds := mgetcmdsp.Get(len(keys), len(keys))
	defer mgetcmdsp.Put(cmds)
	for i := range cmds.s {
		cmds.s[i] = client.B().JsonGet().Key(keys[i]).Path(path).Build()
	}
	return doMultiGet(client, ctx, cmds.s, keys)
}

// JsonMSet is a helper that consults valkey directly with multiple keys by grouping keys within the same slot into JSON.MSETs or multiple JSON.SETs
func JsonMSet(client Client, ctx context.Context, kvs map[string]string, path string) map[string]error {
	if len(kvs) == 0 {
		return make(map[string]error)
	}

	switch client.(type) {
	case *singleClient, *standalone, *sentinelClient:
		return clientJSONMSet(client, ctx, kvs, path, make(map[string]error, len(kvs)))
	}

	cmds := mgetcmdsp.Get(0, len(kvs))
	defer mgetcmdsp.Put(cmds)
	for k, v := range kvs {
		cmds.s = append(cmds.s, client.B().JsonSet().Key(k).Path(path).Value(v).Build().Pin())
	}
	return doMultiSet(client, ctx, cmds.s)
}

// DecodeSliceOfJSON is a helper that struct-scans each ValkeyMessage into dest, which must be a slice of the pointer.
func DecodeSliceOfJSON[T any](result ValkeyResult, dest *[]T) error {
	values, err := result.ToArray()
	if err != nil {
		return err
	}

	ts := make([]T, len(values))
	for i, v := range values {
		var t T
		if err = v.DecodeJSON(&t); err != nil {
			if IsValkeyNil(err) {
				continue
			}
			return err
		}
		ts[i] = t
	}
	*dest = ts
	return nil
}

func clientMGet(client Client, ctx context.Context, cmd Completed, keys []string) (ret map[string]ValkeyMessage, err error) {
	arr, err := client.Do(ctx, cmd).ToArray()
	if err != nil {
		return nil, err
	}
	return arrayToKV(make(map[string]ValkeyMessage, len(keys)), arr, keys), nil
}

func clientMSet(client Client, ctx context.Context, mset string, kvs map[string]string, ret map[string]error) map[string]error {
	cmd := client.B().Arbitrary(mset)
	for k, v := range kvs {
		cmd = cmd.Args(k, v)
	}
	ok, err := client.Do(ctx, cmd.Build()).AsBool()
	if err == nil && !ok {
		err = ErrMSetNXNotSet
	}
	for k := range kvs {
		ret[k] = err
	}
	return ret
}

func clientJSONMSet(client Client, ctx context.Context, kvs map[string]string, path string, ret map[string]error) map[string]error {
	cmd := intl.JsonMsetTripletValue(client.B().JsonMset())
	for k, v := range kvs {
		cmd = cmd.Key(k).Path(path).Value(v)
	}
	err := client.Do(ctx, cmd.Build()).Error()
	for k := range kvs {
		ret[k] = err
	}
	return ret
}

func clientMDel(client Client, ctx context.Context, keys []string) map[string]error {
	err := client.Do(ctx, client.B().Del().Key(keys...).Build()).Error()
	ret := make(map[string]error, len(keys))
	for _, k := range keys {
		ret[k] = err
	}
	return ret
}

func doMultiCache(cc Client, ctx context.Context, cmds []CacheableTTL, keys []string) (ret map[string]ValkeyMessage, err error) {
	ret = make(map[string]ValkeyMessage, len(keys))
	resps := cc.DoMultiCache(ctx, cmds...)
	defer resultsp.Put(&valkeyresults{s: resps})
	for i, resp := range resps {
		if err := resp.NonValkeyError(); err != nil {
			return nil, err
		}
		ret[keys[i]] = resp.val
	}
	return ret, nil
}

func doMultiGet(cc Client, ctx context.Context, cmds []Completed, keys []string) (ret map[string]ValkeyMessage, err error) {
	ret = make(map[string]ValkeyMessage, len(keys))
	resps := cc.DoMulti(ctx, cmds...)
	defer resultsp.Put(&valkeyresults{s: resps})
	for i, resp := range resps {
		if err := resp.NonValkeyError(); err != nil {
			return nil, err
		}
		ret[keys[i]] = resp.val
	}
	return ret, nil
}

func doMultiSet(cc Client, ctx context.Context, cmds []Completed) (ret map[string]error) {
	ret = make(map[string]error, len(cmds))
	resps := cc.DoMulti(ctx, cmds...)
	for i, resp := range resps {
		if ret[cmds[i].Commands()[1]] = resp.Error(); resp.NonValkeyError() == nil {
			intl.PutCompletedForce(cmds[i])
		}
	}
	resultsp.Put(&valkeyresults{s: resps})
	return ret
}

func arrayToKV(m map[string]ValkeyMessage, arr []ValkeyMessage, keys []string) map[string]ValkeyMessage {
	for i, resp := range arr {
		m[keys[i]] = resp
	}
	return m
}

// ErrMSetNXNotSet is used in the MSetNX helper when the underlying MSETNX response is 0.
// Ref: https://redis.io/commands/msetnx/
var ErrMSetNXNotSet = errors.New("MSETNX: no key was set")

type Scanner struct {
	next func(cursor uint64) (ScanEntry, error)
	err  error
}

func NewScanner(next func(cursor uint64) (ScanEntry, error)) *Scanner {
	return &Scanner{next: next}
}

func (s *Scanner) scan() iter.Seq[[]string] {
	return func(yield func([]string) bool) {
		var e ScanEntry
		for e, s.err = s.next(0); s.err == nil && yield(e.Elements) && e.Cursor != 0; {
			e, s.err = s.next(e.Cursor)
		}
	}
}

func (s *Scanner) Iter() iter.Seq[string] {
	return func(yield func(string) bool) {
		for vs := range s.scan() {
			for i := 0; i < len(vs) && yield(vs[i]); i++ {
			}
		}
	}
}

func (s *Scanner) Iter2() iter.Seq2[string, string] {
	return func(yield func(string, string) bool) {
		for vs := range s.scan() {
			for i := 0; i+1 < len(vs) && yield(vs[i], vs[i+1]); i += 2 {
			}
		}
	}
}

func (s *Scanner) Err() error {
	return s.err
}
