package valkeycompatmock

import (
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
)

func defaultStringResult() valkey.ValkeyResult { return mock.Result(mock.ValkeyNil()) }
func defaultStatusResult() valkey.ValkeyResult { return mock.Result(mock.ValkeyString("OK")) }
func defaultIntResult() valkey.ValkeyResult    { return mock.Result(mock.ValkeyInt64(0)) }
func defaultBoolResult() valkey.ValkeyResult   { return mock.Result(mock.ValkeyBool(false)) }
func defaultFloatResult() valkey.ValkeyResult  { return mock.Result(mock.ValkeyFloat64(0)) }
func defaultArrayResult() valkey.ValkeyResult  { return mock.Result(mock.ValkeyArray()) }
func defaultMapResult() valkey.ValkeyResult {
	return mock.Result(mock.ValkeyMap(map[string]valkey.ValkeyMessage{}))
}

func (m *ClientMock) ExpectGetEx(key string, expiration time.Duration) *ExpectedString {
	var cmd valkey.Completed
	if expiration > 0 {
		if usePrecise(expiration) {
			cmd = m.raw.B().Getex().Key(key).PxMilliseconds(formatMs(expiration)).Build()
		} else {
			cmd = m.raw.B().Getex().Key(key).ExSeconds(formatSec(expiration)).Build()
		}
	} else {
		cmd = m.raw.B().Getex().Key(key).Build()
	}
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectGetDel(key string) *ExpectedString {
	cmd := m.raw.B().Getdel().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectGetRange(key string, start, end int64) *ExpectedString {
	cmd := m.raw.B().Getrange().Key(key).Start(start).End(end).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectSetEX(key string, value any, expiration time.Duration) *ExpectedStatus {
	cmd := m.raw.B().Setex().Key(key).Seconds(formatSec(expiration)).Value(str(value)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStatusResult())
	return &ExpectedStatus{exp: e}
}

func (m *ClientMock) ExpectSetXX(key string, value any, expiration time.Duration) *ExpectedBool {
	var cmd valkey.Completed
	if expiration > 0 {
		if usePrecise(expiration) {
			cmd = m.raw.B().Set().Key(key).Value(str(value)).Xx().PxMilliseconds(formatMs(expiration)).Build()
		} else {
			cmd = m.raw.B().Set().Key(key).Value(str(value)).Xx().ExSeconds(formatSec(expiration)).Build()
		}
	} else if expiration == keepTTL {
		cmd = m.raw.B().Set().Key(key).Value(str(value)).Xx().Keepttl().Build()
	} else {
		cmd = m.raw.B().Set().Key(key).Value(str(value)).Xx().Build()
	}
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectSetRange(key string, offset int64, value string) *ExpectedInt {
	cmd := m.raw.B().Setrange().Key(key).Offset(offset).Value(value).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectMSetNX(values ...any) *ExpectedBool {
	pairs := argsToSlice(values)
	args := append([]string{"MSETNX"}, pairs...)
	e := m.push(pairsMatcher("MSETNX", "", pairs), defaultBoolResult())
	_ = args
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectIncrByFloat(key string, increment float64) *ExpectedFloat {
	cmd := m.raw.B().Incrbyfloat().Key(key).Increment(increment).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultFloatResult())
	return &ExpectedFloat{exp: e}
}

func (m *ClientMock) ExpectGetBit(key string, offset int64) *ExpectedInt {
	cmd := m.raw.B().Getbit().Key(key).Offset(offset).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectSetBit(key string, offset, value int64) *ExpectedInt {
	cmd := m.raw.B().Setbit().Key(key).Offset(offset).Value(value).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectBitCount(key string, start, end int64) *ExpectedInt {
	cmd := m.raw.B().Bitcount().Key(key).Start(start).End(end).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectBitOpAnd(destKey string, keys ...string) *ExpectedInt {
	cmd := m.raw.B().Bitop().And().Destkey(destKey).Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectBitOpOr(destKey string, keys ...string) *ExpectedInt {
	cmd := m.raw.B().Bitop().Or().Destkey(destKey).Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectBitOpXor(destKey string, keys ...string) *ExpectedInt {
	cmd := m.raw.B().Bitop().Xor().Destkey(destKey).Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectBitOpNot(destKey, key string) *ExpectedInt {
	cmd := m.raw.B().Bitop().Not().Destkey(destKey).Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectUnlink(keys ...string) *ExpectedInt {
	cmd := m.raw.B().Unlink().Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectExpireAt(key string, tm time.Time) *ExpectedBool {
	cmd := m.raw.B().Expireat().Key(key).Timestamp(tm.Unix()).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectExpireNX(key string, expiration time.Duration) *ExpectedBool {
	cmd := m.raw.B().Expire().Key(key).Seconds(formatSec(expiration)).Nx().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectExpireXX(key string, expiration time.Duration) *ExpectedBool {
	cmd := m.raw.B().Expire().Key(key).Seconds(formatSec(expiration)).Xx().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectExpireGT(key string, expiration time.Duration) *ExpectedBool {
	cmd := m.raw.B().Expire().Key(key).Seconds(formatSec(expiration)).Gt().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectExpireLT(key string, expiration time.Duration) *ExpectedBool {
	cmd := m.raw.B().Expire().Key(key).Seconds(formatSec(expiration)).Lt().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectExpireTime(key string) *ExpectedDuration {
	cmd := m.raw.B().Expiretime().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedDuration{exp: e, precision: time.Second}
}

func (m *ClientMock) ExpectPExpire(key string, expiration time.Duration) *ExpectedBool {
	cmd := m.raw.B().Pexpire().Key(key).Milliseconds(formatMs(expiration)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectPExpireAt(key string, tm time.Time) *ExpectedBool {
	cmd := m.raw.B().Pexpireat().Key(key).MillisecondsTimestamp(tm.UnixMilli()).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectPExpireTime(key string) *ExpectedDuration {
	cmd := m.raw.B().Pexpiretime().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedDuration{exp: e, precision: time.Millisecond}
}

func (m *ClientMock) ExpectPTTL(key string) *ExpectedDuration {
	cmd := m.raw.B().Pttl().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedDuration{exp: e, precision: time.Millisecond}
}

func (m *ClientMock) ExpectPersist(key string) *ExpectedBool {
	cmd := m.raw.B().Persist().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectRandomKey() *ExpectedString {
	cmd := m.raw.B().Randomkey().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectRename(key, newkey string) *ExpectedStatus {
	cmd := m.raw.B().Rename().Key(key).Newkey(newkey).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStatusResult())
	return &ExpectedStatus{exp: e}
}

func (m *ClientMock) ExpectRenameNX(key, newkey string) *ExpectedBool {
	cmd := m.raw.B().Renamenx().Key(key).Newkey(newkey).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectMove(key string, db int64) *ExpectedBool {
	cmd := m.raw.B().Move().Key(key).Db(db).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectObjectEncoding(key string) *ExpectedString {
	cmd := m.raw.B().ObjectEncoding().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectObjectIdleTime(key string) *ExpectedDuration {
	cmd := m.raw.B().ObjectIdletime().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedDuration{exp: e, precision: time.Second}
}

func (m *ClientMock) ExpectObjectRefCount(key string) *ExpectedInt {
	cmd := m.raw.B().ObjectRefcount().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectTouch(keys ...string) *ExpectedInt {
	cmd := m.raw.B().Touch().Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectCopy(source, destination string, db int64, replace bool) *ExpectedInt {
	var cmd valkey.Completed
	if replace {
		cmd = m.raw.B().Copy().Source(source).Destination(destination).Db(db).Replace().Build()
	} else {
		cmd = m.raw.B().Copy().Source(source).Destination(destination).Db(db).Build()
	}
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectHExists(key, field string) *ExpectedBool {
	cmd := m.raw.B().Hexists().Key(key).Field(field).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectHKeys(key string) *ExpectedStringSlice {
	cmd := m.raw.B().Hkeys().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectHLen(key string) *ExpectedInt {
	cmd := m.raw.B().Hlen().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectHMGet(key string, fields ...string) *ExpectedSlice {
	cmd := m.raw.B().Hmget().Key(key).Field(fields...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedSlice{exp: e}
}

func (m *ClientMock) ExpectHMSet(key string, values ...any) *ExpectedBool {
	pairs := argsToSlice(values)
	e := m.push(pairsMatcher("HMSET", key, pairs), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectHSetNX(key, field string, value any) *ExpectedBool {
	cmd := m.raw.B().Hsetnx().Key(key).Field(field).Value(str(value)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectHVals(key string) *ExpectedStringSlice {
	cmd := m.raw.B().Hvals().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectHIncrBy(key, field string, incr int64) *ExpectedInt {
	cmd := m.raw.B().Hincrby().Key(key).Field(field).Increment(incr).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectHIncrByFloat(key, field string, incr float64) *ExpectedFloat {
	cmd := m.raw.B().Hincrbyfloat().Key(key).Field(field).Increment(incr).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultFloatResult())
	return &ExpectedFloat{exp: e}
}

func (m *ClientMock) ExpectHStrLen(key, field string) *ExpectedInt {
	cmd := m.raw.B().Hstrlen().Key(key).Field(field).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectHRandField(key string, count int64) *ExpectedStringSlice {
	cmd := m.raw.B().Hrandfield().Key(key).Count(count).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectLIndex(key string, index int64) *ExpectedString {
	cmd := m.raw.B().Lindex().Key(key).Index(index).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectLRange(key string, start, stop int64) *ExpectedStringSlice {
	cmd := m.raw.B().Lrange().Key(key).Start(start).Stop(stop).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectLRem(key string, count int64, value any) *ExpectedInt {
	cmd := m.raw.B().Lrem().Key(key).Count(count).Element(str(value)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectLSet(key string, index int64, value any) *ExpectedStatus {
	cmd := m.raw.B().Lset().Key(key).Index(index).Element(str(value)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStatusResult())
	return &ExpectedStatus{exp: e}
}

func (m *ClientMock) ExpectLTrim(key string, start, stop int64) *ExpectedStatus {
	cmd := m.raw.B().Ltrim().Key(key).Start(start).Stop(stop).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStatusResult())
	return &ExpectedStatus{exp: e}
}

func (m *ClientMock) ExpectLInsertBefore(key string, pivot, value any) *ExpectedInt {
	cmd := m.raw.B().Linsert().Key(key).Before().Pivot(str(pivot)).Element(str(value)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectLInsertAfter(key string, pivot, value any) *ExpectedInt {
	cmd := m.raw.B().Linsert().Key(key).After().Pivot(str(pivot)).Element(str(value)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectLPushX(key string, values ...any) *ExpectedInt {
	args := make([]string, 0, len(values))
	for _, v := range values {
		args = append(args, str(v))
	}
	cmd := m.raw.B().Lpushx().Key(key).Element(args...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectRPushX(key string, values ...any) *ExpectedInt {
	args := make([]string, 0, len(values))
	for _, v := range values {
		args = append(args, str(v))
	}
	cmd := m.raw.B().Rpushx().Key(key).Element(args...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectRPopLPush(source, destination string) *ExpectedString {
	cmd := m.raw.B().Rpoplpush().Source(source).Destination(destination).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectBLPop(timeout time.Duration, keys ...string) *ExpectedStringSlice {
	cmd := m.raw.B().Blpop().Key(keys...).Timeout(float64(formatSec(timeout))).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectBRPop(timeout time.Duration, keys ...string) *ExpectedStringSlice {
	cmd := m.raw.B().Brpop().Key(keys...).Timeout(float64(formatSec(timeout))).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectSCard(key string) *ExpectedInt {
	cmd := m.raw.B().Scard().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectSDiff(keys ...string) *ExpectedStringSlice {
	cmd := m.raw.B().Sdiff().Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectSDiffStore(destination string, keys ...string) *ExpectedInt {
	cmd := m.raw.B().Sdiffstore().Destination(destination).Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectSInter(keys ...string) *ExpectedStringSlice {
	cmd := m.raw.B().Sinter().Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectSInterStore(destination string, keys ...string) *ExpectedInt {
	cmd := m.raw.B().Sinterstore().Destination(destination).Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectSIsMember(key string, member any) *ExpectedBool {
	cmd := m.raw.B().Sismember().Key(key).Member(str(member)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectSMIsMember(key string, members ...any) *ExpectedBoolSlice {
	args := make([]string, 0, len(members))
	for _, mm := range members {
		args = append(args, str(mm))
	}
	cmd := m.raw.B().Smismember().Key(key).Member(args...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedBoolSlice{exp: e}
}

func (m *ClientMock) ExpectSMove(source, destination string, member any) *ExpectedBool {
	cmd := m.raw.B().Smove().Source(source).Destination(destination).Member(str(member)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultBoolResult())
	return &ExpectedBool{exp: e}
}

func (m *ClientMock) ExpectSPop(key string) *ExpectedString {
	cmd := m.raw.B().Spop().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectSPopN(key string, count int64) *ExpectedStringSlice {
	cmd := m.raw.B().Spop().Key(key).Count(count).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectSRandMember(key string) *ExpectedString {
	cmd := m.raw.B().Srandmember().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectSRandMemberN(key string, count int64) *ExpectedStringSlice {
	cmd := m.raw.B().Srandmember().Key(key).Count(count).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectSUnion(keys ...string) *ExpectedStringSlice {
	cmd := m.raw.B().Sunion().Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectSUnionStore(destination string, keys ...string) *ExpectedInt {
	cmd := m.raw.B().Sunionstore().Destination(destination).Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectZCard(key string) *ExpectedInt {
	cmd := m.raw.B().Zcard().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectZCount(key, min, max string) *ExpectedInt {
	cmd := m.raw.B().Zcount().Key(key).Min(min).Max(max).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectZIncrBy(key string, increment float64, member string) *ExpectedFloat {
	cmd := m.raw.B().Zincrby().Key(key).Increment(increment).Member(member).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultFloatResult())
	return &ExpectedFloat{exp: e}
}

func (m *ClientMock) ExpectZLexCount(key, min, max string) *ExpectedInt {
	cmd := m.raw.B().Zlexcount().Key(key).Min(min).Max(max).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectZRange(key string, start, stop int64) *ExpectedStringSlice {
	cmd := m.raw.B().Zrange().Key(key).Min(itoa(start)).Max(itoa(stop)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectZRevRange(key string, start, stop int64) *ExpectedStringSlice {
	cmd := m.raw.B().Zrevrange().Key(key).Start(start).Stop(stop).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectZRank(key, member string) *ExpectedInt {
	cmd := m.raw.B().Zrank().Key(key).Member(member).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectZRevRank(key, member string) *ExpectedInt {
	cmd := m.raw.B().Zrevrank().Key(key).Member(member).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectZRem(key string, members ...any) *ExpectedInt {
	args := make([]string, 0, len(members))
	for _, mm := range members {
		args = append(args, str(mm))
	}
	cmd := m.raw.B().Zrem().Key(key).Member(args...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectZRemRangeByLex(key, min, max string) *ExpectedInt {
	cmd := m.raw.B().Zremrangebylex().Key(key).Min(min).Max(max).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectZRemRangeByRank(key string, start, stop int64) *ExpectedInt {
	cmd := m.raw.B().Zremrangebyrank().Key(key).Start(start).Stop(stop).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectZRemRangeByScore(key, min, max string) *ExpectedInt {
	cmd := m.raw.B().Zremrangebyscore().Key(key).Min(min).Max(max).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectZScore(key, member string) *ExpectedFloat {
	cmd := m.raw.B().Zscore().Key(key).Member(member).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultFloatResult())
	return &ExpectedFloat{exp: e}
}

func (m *ClientMock) ExpectZMScore(key string, members ...string) *ExpectedFloatSlice {
	cmd := m.raw.B().Zmscore().Key(key).Member(members...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedFloatSlice{exp: e}
}

func (m *ClientMock) ExpectZPopMax(key string, count ...int64) *ExpectedZSlice {
	var cmd valkey.Completed
	if len(count) > 0 {
		cmd = m.raw.B().Zpopmax().Key(key).Count(count[0]).Build()
	} else {
		cmd = m.raw.B().Zpopmax().Key(key).Build()
	}
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedZSlice{exp: e}
}

func (m *ClientMock) ExpectZPopMin(key string, count ...int64) *ExpectedZSlice {
	var cmd valkey.Completed
	if len(count) > 0 {
		cmd = m.raw.B().Zpopmin().Key(key).Count(count[0]).Build()
	} else {
		cmd = m.raw.B().Zpopmin().Key(key).Build()
	}
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedZSlice{exp: e}
}

func (m *ClientMock) ExpectPublish(channel string, message any) *ExpectedInt {
	cmd := m.raw.B().Publish().Channel(channel).Message(str(message)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectSPublish(channel string, message any) *ExpectedInt {
	cmd := m.raw.B().Spublish().Channel(channel).Message(str(message)).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectPubSubChannels(pattern string) *ExpectedStringSlice {
	cmd := m.raw.B().PubsubChannels().Pattern(pattern).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectPubSubNumPat() *ExpectedInt {
	cmd := m.raw.B().PubsubNumpat().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectPFAdd(key string, els ...any) *ExpectedInt {
	args := make([]string, 0, len(els))
	for _, v := range els {
		args = append(args, str(v))
	}
	cmd := m.raw.B().Pfadd().Key(key).Element(args...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectPFCount(keys ...string) *ExpectedInt {
	cmd := m.raw.B().Pfcount().Key(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectPFMerge(dest string, keys ...string) *ExpectedStatus {
	cmd := m.raw.B().Pfmerge().Destkey(dest).Sourcekey(keys...).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStatusResult())
	return &ExpectedStatus{exp: e}
}

func (m *ClientMock) ExpectLastSave() *ExpectedInt {
	cmd := m.raw.B().Lastsave().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectTime() *ExpectedTime {
	cmd := m.raw.B().Time().Build()
	e := m.push(mock.Match(cmd.Commands()...), mock.Result(mock.ValkeyArray(mock.ValkeyString("0"), mock.ValkeyString("0"))))
	return &ExpectedTime{exp: e}
}

func (m *ClientMock) ExpectInfo(sections ...string) *ExpectedString {
	var cmd valkey.Completed
	if len(sections) > 0 {
		cmd = m.raw.B().Info().Section(sections...).Build()
	} else {
		cmd = m.raw.B().Info().Build()
	}
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectClientID() *ExpectedInt {
	cmd := m.raw.B().ClientId().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectClientInfo() *ExpectedString {
	cmd := m.raw.B().ClientInfo().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectClientGetName() *ExpectedString {
	cmd := m.raw.B().ClientGetname().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectReadOnly() *ExpectedStatus {
	cmd := m.raw.B().Readonly().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStatusResult())
	return &ExpectedStatus{exp: e}
}

func (m *ClientMock) ExpectReadWrite() *ExpectedStatus {
	cmd := m.raw.B().Readwrite().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStatusResult())
	return &ExpectedStatus{exp: e}
}

func (m *ClientMock) ExpectEvalSha(sha string, keys []string, args ...any) *ExpectedCmd {
	strArgs := make([]string, 0, len(args))
	for _, a := range args {
		strArgs = append(strArgs, str(a))
	}
	cmd := m.raw.B().Evalsha().Sha1(sha).Numkeys(int64(len(keys))).Key(keys...).Arg(strArgs...).Build()
	e := m.push(mock.Match(cmd.Commands()...), mock.Result(mock.ValkeyNil()))
	return &ExpectedCmd{exp: e}
}

func (m *ClientMock) ExpectEvalRO(script string, keys []string, args ...any) *ExpectedCmd {
	strArgs := make([]string, 0, len(args))
	for _, a := range args {
		strArgs = append(strArgs, str(a))
	}
	cmd := m.raw.B().EvalRo().Script(script).Numkeys(int64(len(keys))).Key(keys...).Arg(strArgs...).Build()
	e := m.push(mock.Match(cmd.Commands()...), mock.Result(mock.ValkeyNil()))
	return &ExpectedCmd{exp: e}
}

func (m *ClientMock) ExpectEvalShaRO(sha string, keys []string, args ...any) *ExpectedCmd {
	strArgs := make([]string, 0, len(args))
	for _, a := range args {
		strArgs = append(strArgs, str(a))
	}
	cmd := m.raw.B().EvalshaRo().Sha1(sha).Numkeys(int64(len(keys))).Key(keys...).Arg(strArgs...).Build()
	e := m.push(mock.Match(cmd.Commands()...), mock.Result(mock.ValkeyNil()))
	return &ExpectedCmd{exp: e}
}

func (m *ClientMock) ExpectClusterInfo() *ExpectedString {
	cmd := m.raw.B().ClusterInfo().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectClusterNodes() *ExpectedString {
	cmd := m.raw.B().ClusterNodes().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectClusterMyShardID() *ExpectedString {
	cmd := m.raw.B().ClusterMyshardid().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultStringResult())
	return &ExpectedString{exp: e}
}

func (m *ClientMock) ExpectClusterKeySlot(key string) *ExpectedInt {
	cmd := m.raw.B().ClusterKeyslot().Key(key).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}

func (m *ClientMock) ExpectACLList() *ExpectedStringSlice {
	cmd := m.raw.B().AclList().Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultArrayResult())
	return &ExpectedStringSlice{exp: e}
}

func (m *ClientMock) ExpectACLDelUser(username string) *ExpectedInt {
	cmd := m.raw.B().AclDeluser().Username(username).Build()
	e := m.push(mock.Match(cmd.Commands()...), defaultIntResult())
	return &ExpectedInt{exp: e}
}
