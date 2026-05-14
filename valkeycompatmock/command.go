package valkeycompatmock

import (
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"github.com/valkey-io/valkey-go/valkeycompat"
)

type ExpectedString struct{ exp *expectation }

func (e *ExpectedString) SetVal(v string) *ExpectedString {
	e.exp.result = mock.Result(mock.ValkeyString(v))
	return e
}

func (e *ExpectedString) SetErr(err error) *ExpectedString {
	e.exp.result = mock.ErrorResult(err)
	return e
}

func (e *ExpectedString) RedisNil() *ExpectedString {
	e.exp.result = mock.Result(mock.ValkeyNil())
	return e
}

type ExpectedStatus = ExpectedString

type ExpectedBool struct{ exp *expectation }

func (e *ExpectedBool) SetVal(v bool) *ExpectedBool {
	e.exp.result = mock.Result(mock.ValkeyBool(v))
	return e
}

func (e *ExpectedBool) SetErr(err error) *ExpectedBool {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedInt struct{ exp *expectation }

func (e *ExpectedInt) SetVal(v int64) *ExpectedInt {
	e.exp.result = mock.Result(mock.ValkeyInt64(v))
	return e
}

func (e *ExpectedInt) SetErr(err error) *ExpectedInt {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedDuration struct {
	exp       *expectation
	precision time.Duration
}

func (e *ExpectedDuration) SetVal(v time.Duration) *ExpectedDuration {
	e.exp.result = mock.Result(mock.ValkeyInt64(int64(v / e.precision)))
	return e
}

func (e *ExpectedDuration) SetErr(err error) *ExpectedDuration {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedFloat struct{ exp *expectation }

func (e *ExpectedFloat) SetVal(v float64) *ExpectedFloat {
	e.exp.result = mock.Result(mock.ValkeyFloat64(v))
	return e
}

func (e *ExpectedFloat) SetErr(err error) *ExpectedFloat {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedStringSlice struct{ exp *expectation }

func (e *ExpectedStringSlice) SetVal(v []string) *ExpectedStringSlice {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, s := range v {
		msgs = append(msgs, mock.ValkeyString(s))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
	return e
}

func (e *ExpectedStringSlice) SetErr(err error) *ExpectedStringSlice {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedIntSlice struct{ exp *expectation }

func (e *ExpectedIntSlice) SetVal(v []int64) *ExpectedIntSlice {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, n := range v {
		msgs = append(msgs, mock.ValkeyInt64(n))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
	return e
}

func (e *ExpectedIntSlice) SetErr(err error) *ExpectedIntSlice {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedBoolSlice struct{ exp *expectation }

func (e *ExpectedBoolSlice) SetVal(v []bool) *ExpectedBoolSlice {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, b := range v {
		msgs = append(msgs, mock.ValkeyBool(b))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
	return e
}

func (e *ExpectedBoolSlice) SetErr(err error) *ExpectedBoolSlice {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedFloatSlice struct{ exp *expectation }

func (e *ExpectedFloatSlice) SetVal(v []float64) *ExpectedFloatSlice {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, f := range v {
		msgs = append(msgs, mock.ValkeyFloat64(f))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
	return e
}

func (e *ExpectedFloatSlice) SetErr(err error) *ExpectedFloatSlice {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedSlice struct{ exp *expectation }

func (e *ExpectedSlice) SetVal(v []any) *ExpectedSlice {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, item := range v {
		switch x := item.(type) {
		case nil:
			msgs = append(msgs, mock.ValkeyNil())
		case string:
			msgs = append(msgs, mock.ValkeyString(x))
		case int64:
			msgs = append(msgs, mock.ValkeyInt64(x))
		case bool:
			msgs = append(msgs, mock.ValkeyBool(x))
		default:
			msgs = append(msgs, mock.ValkeyString(str(item)))
		}
	}
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
	return e
}

func (e *ExpectedSlice) SetErr(err error) *ExpectedSlice {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedStringStringMap struct{ exp *expectation }

func (e *ExpectedStringStringMap) SetVal(v map[string]string) *ExpectedStringStringMap {
	kv := make(map[string]valkey.ValkeyMessage, len(v))
	for k, val := range v {
		kv[k] = mock.ValkeyString(val)
	}
	e.exp.result = mock.Result(mock.ValkeyMap(kv))
	return e
}

func (e *ExpectedStringStringMap) SetErr(err error) *ExpectedStringStringMap {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedStringIntMap struct{ exp *expectation }

func (e *ExpectedStringIntMap) SetVal(v map[string]int64) *ExpectedStringIntMap {
	kv := make(map[string]valkey.ValkeyMessage, len(v))
	for k, val := range v {
		kv[k] = mock.ValkeyInt64(val)
	}
	e.exp.result = mock.Result(mock.ValkeyMap(kv))
	return e
}

func (e *ExpectedStringIntMap) SetErr(err error) *ExpectedStringIntMap {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedStringStructMap struct{ exp *expectation }

func (e *ExpectedStringStructMap) SetVal(v []string) *ExpectedStringStructMap {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, s := range v {
		msgs = append(msgs, mock.ValkeyString(s))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
	return e
}

func (e *ExpectedStringStructMap) SetErr(err error) *ExpectedStringStructMap {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedKeyValueSlice struct{ exp *expectation }

func (e *ExpectedKeyValueSlice) SetVal(v []valkeycompat.KeyValue) *ExpectedKeyValueSlice {
	pairs := make([]valkey.ValkeyMessage, 0, len(v)*2)
	for _, kv := range v {
		pairs = append(pairs, mock.ValkeyString(kv.Key), mock.ValkeyString(kv.Value))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(pairs...))
	return e
}

func (e *ExpectedKeyValueSlice) SetErr(err error) *ExpectedKeyValueSlice {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedKeyValues struct{ exp *expectation }

func (e *ExpectedKeyValues) SetVal(key string, vals []string) *ExpectedKeyValues {
	msgs := make([]valkey.ValkeyMessage, 0, len(vals))
	for _, v := range vals {
		msgs = append(msgs, mock.ValkeyString(v))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(mock.ValkeyString(key), mock.ValkeyArray(msgs...)))
	return e
}

func (e *ExpectedKeyValues) SetErr(err error) *ExpectedKeyValues {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedZSlice struct{ exp *expectation }

func (e *ExpectedZSlice) SetVal(v []valkeycompat.Z) *ExpectedZSlice {
	msgs := make([]valkey.ValkeyMessage, 0, len(v)*2)
	for _, z := range v {
		msgs = append(msgs, mock.ValkeyString(str(z.Member)), mock.ValkeyString(str(z.Score)))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
	return e
}

func (e *ExpectedZSlice) SetErr(err error) *ExpectedZSlice {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedZWithKey struct{ exp *expectation }

func (e *ExpectedZWithKey) SetVal(v valkeycompat.ZWithKey) *ExpectedZWithKey {
	e.exp.result = mock.Result(mock.ValkeyArray(
		mock.ValkeyString(v.Key),
		mock.ValkeyString(str(v.Member)),
		mock.ValkeyString(str(v.Score)),
	))
	return e
}

func (e *ExpectedZWithKey) SetErr(err error) *ExpectedZWithKey {
	e.exp.result = mock.ErrorResult(err)
	return e
}

func (e *ExpectedZWithKey) RedisNil() *ExpectedZWithKey {
	e.exp.result = mock.Result(mock.ValkeyNil())
	return e
}

type ExpectedRankWithScore struct{ exp *expectation }

func (e *ExpectedRankWithScore) SetVal(v valkeycompat.RankScore) *ExpectedRankWithScore {
	e.exp.result = mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(v.Rank),
		mock.ValkeyFloat64(v.Score),
	))
	return e
}

func (e *ExpectedRankWithScore) SetErr(err error) *ExpectedRankWithScore {
	e.exp.result = mock.ErrorResult(err)
	return e
}

func (e *ExpectedRankWithScore) RedisNil() *ExpectedRankWithScore {
	e.exp.result = mock.Result(mock.ValkeyNil())
	return e
}

type ExpectedTime struct{ exp *expectation }

func (e *ExpectedTime) SetVal(v time.Time) *ExpectedTime {
	e.exp.result = mock.Result(mock.ValkeyArray(
		mock.ValkeyString(itoa(v.Unix())),
		mock.ValkeyString(itoa(int64(v.Nanosecond()/1000))),
	))
	return e
}

func (e *ExpectedTime) SetErr(err error) *ExpectedTime {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedScan struct{ exp *expectation }

func (e *ExpectedScan) SetVal(cursor uint64, keys []string) *ExpectedScan {
	msgs := make([]valkey.ValkeyMessage, 0, len(keys))
	for _, k := range keys {
		msgs = append(msgs, mock.ValkeyString(k))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(
		mock.ValkeyString(itoa(int64(cursor))),
		mock.ValkeyArray(msgs...),
	))
	return e
}

func (e *ExpectedScan) SetErr(err error) *ExpectedScan {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedGeoPos struct{ exp *expectation }

func (e *ExpectedGeoPos) SetVal(v []*valkeycompat.GeoPos) *ExpectedGeoPos {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, p := range v {
		if p == nil {
			msgs = append(msgs, mock.ValkeyNil())
			continue
		}
		msgs = append(msgs, mock.ValkeyArray(
			mock.ValkeyFloat64(p.Longitude),
			mock.ValkeyFloat64(p.Latitude),
		))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
	return e
}

func (e *ExpectedGeoPos) SetErr(err error) *ExpectedGeoPos {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedGeoLocation struct{ exp *expectation }

func (e *ExpectedGeoLocation) SetVal(v []valkeycompat.GeoLocation) *ExpectedGeoLocation {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, l := range v {
		msgs = append(msgs, mock.ValkeyString(l.Name))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
	return e
}

func (e *ExpectedGeoLocation) SetErr(err error) *ExpectedGeoLocation {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedXMessageSlice struct{ exp *expectation }

func (e *ExpectedXMessageSlice) SetVal(v []valkeycompat.XMessage) *ExpectedXMessageSlice {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, m := range v {
		fields := make([]valkey.ValkeyMessage, 0, len(m.Values)*2)
		for k, val := range m.Values {
			fields = append(fields, mock.ValkeyString(k), mock.ValkeyString(str(val)))
		}
		msgs = append(msgs, mock.ValkeyArray(
			mock.ValkeyString(m.ID),
			mock.ValkeyArray(fields...),
		))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
	return e
}

func (e *ExpectedXMessageSlice) SetErr(err error) *ExpectedXMessageSlice {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedXStreamSlice struct{ exp *expectation }

func (e *ExpectedXStreamSlice) SetVal(v []valkeycompat.XStream) *ExpectedXStreamSlice {
	streams := make([]valkey.ValkeyMessage, 0, len(v))
	for _, s := range v {
		entries := make([]valkey.ValkeyMessage, 0, len(s.Messages))
		for _, m := range s.Messages {
			fields := make([]valkey.ValkeyMessage, 0, len(m.Values)*2)
			for k, val := range m.Values {
				fields = append(fields, mock.ValkeyString(k), mock.ValkeyString(str(val)))
			}
			entries = append(entries, mock.ValkeyArray(
				mock.ValkeyString(m.ID),
				mock.ValkeyArray(fields...),
			))
		}
		streams = append(streams, mock.ValkeyArray(
			mock.ValkeyString(s.Stream),
			mock.ValkeyArray(entries...),
		))
	}
	e.exp.result = mock.Result(mock.ValkeyArray(streams...))
	return e
}

func (e *ExpectedXStreamSlice) SetErr(err error) *ExpectedXStreamSlice {
	e.exp.result = mock.ErrorResult(err)
	return e
}

type ExpectedCmd struct{ exp *expectation }

func (e *ExpectedCmd) SetVal(v string) *ExpectedCmd {
	e.exp.result = mock.Result(mock.ValkeyString(v))
	return e
}

func (e *ExpectedCmd) SetValInt(v int64) *ExpectedCmd {
	e.exp.result = mock.Result(mock.ValkeyInt64(v))
	return e
}

func (e *ExpectedCmd) SetErr(err error) *ExpectedCmd {
	e.exp.result = mock.ErrorResult(err)
	return e
}

func (e *ExpectedCmd) RedisNil() *ExpectedCmd {
	e.exp.result = mock.Result(mock.ValkeyNil())
	return e
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
