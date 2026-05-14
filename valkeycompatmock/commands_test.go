package valkeycompatmock

import (
	"context"
	"testing"
	"time"

	"github.com/valkey-io/valkey-go/mock"
	"github.com/valkey-io/valkey-go/valkeycompat"
	"go.uber.org/mock/gomock"
)

func TestStringCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectGetEx("k", time.Second).SetVal("v")
			_, err := rdb.GetEx(ctx, "k", time.Second).Result()
			return err
		},
		func() error {
			cm.ExpectGetEx("k", 0).SetVal("v")
			_, err := rdb.GetEx(ctx, "k", 0).Result()
			return err
		},
		func() error {
			cm.ExpectGetEx("k", 50*time.Millisecond).SetVal("v")
			_, err := rdb.GetEx(ctx, "k", 50*time.Millisecond).Result()
			return err
		},
		func() error {
			cm.ExpectGetDel("k").SetVal("v")
			_, err := rdb.GetDel(ctx, "k").Result()
			return err
		},
		func() error {
			cm.ExpectGetRange("k", 0, 5).SetVal("v")
			_, err := rdb.GetRange(ctx, "k", 0, 5).Result()
			return err
		},
		func() error {
			cm.ExpectSetEX("k", "v", time.Second).SetVal("OK")
			return rdb.SetEX(ctx, "k", "v", time.Second).Err()
		},
		func() error {
			cm.ExpectSetXX("k", "v", time.Second).SetVal(true)
			_, err := rdb.SetXX(ctx, "k", "v", time.Second).Result()
			return err
		},
		func() error {
			cm.ExpectSetXX("k", "v", 50*time.Millisecond).SetVal(true)
			_, err := rdb.SetXX(ctx, "k", "v", 50*time.Millisecond).Result()
			return err
		},
		func() error {
			cm.ExpectSetXX("k", "v", -1).SetVal(true)
			_, err := rdb.SetXX(ctx, "k", "v", -1).Result()
			return err
		},
		func() error {
			cm.ExpectSetXX("k", "v", 0).SetVal(true)
			_, err := rdb.SetXX(ctx, "k", "v", 0).Result()
			return err
		},
		func() error {
			cm.ExpectSetRange("k", 0, "v").SetVal(1)
			_, err := rdb.SetRange(ctx, "k", 0, "v").Result()
			return err
		},
		func() error {
			cm.ExpectMSetNX("k1", "v1", "k2", "v2").SetVal(true)
			_, err := rdb.MSetNX(ctx, "k1", "v1", "k2", "v2").Result()
			return err
		},
		func() error {
			cm.ExpectIncrByFloat("k", 1.5).SetVal(2.5)
			_, err := rdb.IncrByFloat(ctx, "k", 1.5).Result()
			return err
		},
		func() error {
			cm.ExpectGetBit("k", 7).SetVal(1)
			_, err := rdb.GetBit(ctx, "k", 7).Result()
			return err
		},
		func() error {
			cm.ExpectSetBit("k", 7, 1).SetVal(0)
			_, err := rdb.SetBit(ctx, "k", 7, 1).Result()
			return err
		},
		func() error {
			cm.ExpectBitCount("k", 0, 10).SetVal(5)
			_, err := rdb.BitCount(ctx, "k", &valkeycompat.BitCount{Start: 0, End: 10}).Result()
			return err
		},
		func() error {
			cm.ExpectBitOpAnd("dst", "k1", "k2").SetVal(3)
			_, err := rdb.BitOpAnd(ctx, "dst", "k1", "k2").Result()
			return err
		},
		func() error {
			cm.ExpectBitOpOr("dst", "k1", "k2").SetVal(3)
			_, err := rdb.BitOpOr(ctx, "dst", "k1", "k2").Result()
			return err
		},
		func() error {
			cm.ExpectBitOpXor("dst", "k1", "k2").SetVal(3)
			_, err := rdb.BitOpXor(ctx, "dst", "k1", "k2").Result()
			return err
		},
		func() error {
			cm.ExpectBitOpNot("dst", "k").SetVal(3)
			_, err := rdb.BitOpNot(ctx, "dst", "k").Result()
			return err
		},
	}
	runCases(t, cm, cases)
}

func TestGenericCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectUnlink("k1", "k2").SetVal(2)
			_, err := rdb.Unlink(ctx, "k1", "k2").Result()
			return err
		},
		func() error {
			now := time.Now()
			cm.ExpectExpireAt("k", now).SetVal(true)
			_, err := rdb.ExpireAt(ctx, "k", now).Result()
			return err
		},
		func() error {
			cm.ExpectExpireNX("k", time.Second).SetVal(true)
			_, err := rdb.ExpireNX(ctx, "k", time.Second).Result()
			return err
		},
		func() error {
			cm.ExpectExpireXX("k", time.Second).SetVal(true)
			_, err := rdb.ExpireXX(ctx, "k", time.Second).Result()
			return err
		},
		func() error {
			cm.ExpectExpireGT("k", time.Second).SetVal(true)
			_, err := rdb.ExpireGT(ctx, "k", time.Second).Result()
			return err
		},
		func() error {
			cm.ExpectExpireLT("k", time.Second).SetVal(true)
			_, err := rdb.ExpireLT(ctx, "k", time.Second).Result()
			return err
		},
		func() error {
			cm.ExpectExpireTime("k").SetVal(time.Hour)
			_, err := rdb.ExpireTime(ctx, "k").Result()
			return err
		},
		func() error {
			cm.ExpectPExpire("k", time.Second).SetVal(true)
			_, err := rdb.PExpire(ctx, "k", time.Second).Result()
			return err
		},
		func() error {
			now := time.Now()
			cm.ExpectPExpireAt("k", now).SetVal(true)
			_, err := rdb.PExpireAt(ctx, "k", now).Result()
			return err
		},
		func() error {
			cm.ExpectPExpireTime("k").SetVal(time.Hour)
			_, err := rdb.PExpireTime(ctx, "k").Result()
			return err
		},
		func() error {
			cm.ExpectPTTL("k").SetVal(time.Second)
			_, err := rdb.PTTL(ctx, "k").Result()
			return err
		},
		func() error {
			cm.ExpectPersist("k").SetVal(true)
			_, err := rdb.Persist(ctx, "k").Result()
			return err
		},
		func() error {
			cm.ExpectRandomKey().SetVal("k")
			_, err := rdb.RandomKey(ctx).Result()
			return err
		},
		func() error {
			cm.ExpectRename("k", "n").SetVal("OK")
			return rdb.Rename(ctx, "k", "n").Err()
		},
		func() error {
			cm.ExpectRenameNX("k", "n").SetVal(true)
			_, err := rdb.RenameNX(ctx, "k", "n").Result()
			return err
		},
		func() error {
			cm.ExpectMove("k", 1).SetVal(true)
			_, err := rdb.Move(ctx, "k", 1).Result()
			return err
		},
		func() error {
			cm.ExpectObjectEncoding("k").SetVal("raw")
			_, err := rdb.ObjectEncoding(ctx, "k").Result()
			return err
		},
		func() error {
			cm.ExpectObjectIdleTime("k").SetVal(time.Second)
			_, err := rdb.ObjectIdleTime(ctx, "k").Result()
			return err
		},
		func() error {
			cm.ExpectObjectRefCount("k").SetVal(1)
			_, err := rdb.ObjectRefCount(ctx, "k").Result()
			return err
		},
		func() error {
			cm.ExpectTouch("k1", "k2").SetVal(2)
			_, err := rdb.Touch(ctx, "k1", "k2").Result()
			return err
		},
		func() error {
			cm.ExpectCopy("s", "d", 0, false).SetVal(1)
			_, err := rdb.Copy(ctx, "s", "d", 0, false).Result()
			return err
		},
		func() error {
			cm.ExpectCopy("s", "d", 0, true).SetVal(1)
			_, err := rdb.Copy(ctx, "s", "d", 0, true).Result()
			return err
		},
	}
	runCases(t, cm, cases)
}

func TestHashCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectHExists("h", "f").SetVal(true)
			_, err := rdb.HExists(ctx, "h", "f").Result()
			return err
		},
		func() error {
			cm.ExpectHKeys("h").SetVal([]string{"f1", "f2"})
			_, err := rdb.HKeys(ctx, "h").Result()
			return err
		},
		func() error {
			cm.ExpectHLen("h").SetVal(2)
			_, err := rdb.HLen(ctx, "h").Result()
			return err
		},
		func() error {
			cm.ExpectHMGet("h", "f1", "f2").SetVal([]any{"v1", "v2"})
			_, err := rdb.HMGet(ctx, "h", "f1", "f2").Result()
			return err
		},
		func() error {
			cm.ExpectHMSet("h", "f1", "v1", "f2", "v2").SetVal(true)
			_, err := rdb.HMSet(ctx, "h", "f1", "v1", "f2", "v2").Result()
			return err
		},
		func() error {
			cm.ExpectHSetNX("h", "f", "v").SetVal(true)
			_, err := rdb.HSetNX(ctx, "h", "f", "v").Result()
			return err
		},
		func() error {
			cm.ExpectHVals("h").SetVal([]string{"v1", "v2"})
			_, err := rdb.HVals(ctx, "h").Result()
			return err
		},
		func() error {
			cm.ExpectHIncrBy("h", "f", 1).SetVal(2)
			_, err := rdb.HIncrBy(ctx, "h", "f", 1).Result()
			return err
		},
		func() error {
			cm.ExpectHIncrByFloat("h", "f", 1.5).SetVal(2.5)
			_, err := rdb.HIncrByFloat(ctx, "h", "f", 1.5).Result()
			return err
		},
		func() error {
			cm.ExpectHStrLen("h", "f").SetVal(3)
			_, err := rdb.HStrLen(ctx, "h", "f").Result()
			return err
		},
		func() error {
			cm.ExpectHRandField("h", 2).SetVal([]string{"f1", "f2"})
			_, err := rdb.HRandField(ctx, "h", 2).Result()
			return err
		},
	}
	runCases(t, cm, cases)
}

func TestListCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectLIndex("l", 0).SetVal("v")
			_, err := rdb.LIndex(ctx, "l", 0).Result()
			return err
		},
		func() error {
			cm.ExpectLRange("l", 0, -1).SetVal([]string{"v"})
			_, err := rdb.LRange(ctx, "l", 0, -1).Result()
			return err
		},
		func() error {
			cm.ExpectLRem("l", 0, "v").SetVal(1)
			_, err := rdb.LRem(ctx, "l", 0, "v").Result()
			return err
		},
		func() error {
			cm.ExpectLSet("l", 0, "v").SetVal("OK")
			return rdb.LSet(ctx, "l", 0, "v").Err()
		},
		func() error {
			cm.ExpectLTrim("l", 0, -1).SetVal("OK")
			return rdb.LTrim(ctx, "l", 0, -1).Err()
		},
		func() error {
			cm.ExpectLInsertBefore("l", "p", "v").SetVal(1)
			_, err := rdb.LInsertBefore(ctx, "l", "p", "v").Result()
			return err
		},
		func() error {
			cm.ExpectLInsertAfter("l", "p", "v").SetVal(1)
			_, err := rdb.LInsertAfter(ctx, "l", "p", "v").Result()
			return err
		},
		func() error {
			cm.ExpectLPushX("l", "v").SetVal(1)
			_, err := rdb.LPushX(ctx, "l", "v").Result()
			return err
		},
		func() error {
			cm.ExpectRPushX("l", "v").SetVal(1)
			_, err := rdb.RPushX(ctx, "l", "v").Result()
			return err
		},
		func() error {
			cm.ExpectRPopLPush("s", "d").SetVal("v")
			_, err := rdb.RPopLPush(ctx, "s", "d").Result()
			return err
		},
		func() error {
			cm.ExpectBLPop(time.Second, "k1").SetVal([]string{"k1", "v"})
			_, err := rdb.BLPop(ctx, time.Second, "k1").Result()
			return err
		},
		func() error {
			cm.ExpectBRPop(time.Second, "k1").SetVal([]string{"k1", "v"})
			_, err := rdb.BRPop(ctx, time.Second, "k1").Result()
			return err
		},
	}
	runCases(t, cm, cases)
}

func TestSetCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectSCard("s").SetVal(2)
			_, err := rdb.SCard(ctx, "s").Result()
			return err
		},
		func() error {
			cm.ExpectSDiff("s1", "s2").SetVal([]string{"a"})
			_, err := rdb.SDiff(ctx, "s1", "s2").Result()
			return err
		},
		func() error {
			cm.ExpectSDiffStore("d", "s1", "s2").SetVal(1)
			_, err := rdb.SDiffStore(ctx, "d", "s1", "s2").Result()
			return err
		},
		func() error {
			cm.ExpectSInter("s1", "s2").SetVal([]string{"a"})
			_, err := rdb.SInter(ctx, "s1", "s2").Result()
			return err
		},
		func() error {
			cm.ExpectSInterStore("d", "s1", "s2").SetVal(1)
			_, err := rdb.SInterStore(ctx, "d", "s1", "s2").Result()
			return err
		},
		func() error {
			cm.ExpectSIsMember("s", "m").SetVal(true)
			_, err := rdb.SIsMember(ctx, "s", "m").Result()
			return err
		},
		func() error {
			cm.ExpectSMIsMember("s", "m1", "m2").SetVal([]bool{true, false})
			_, err := rdb.SMIsMember(ctx, "s", "m1", "m2").Result()
			return err
		},
		func() error {
			cm.ExpectSMove("s", "d", "m").SetVal(true)
			_, err := rdb.SMove(ctx, "s", "d", "m").Result()
			return err
		},
		func() error {
			cm.ExpectSPop("s").SetVal("m")
			_, err := rdb.SPop(ctx, "s").Result()
			return err
		},
		func() error {
			cm.ExpectSPopN("s", 2).SetVal([]string{"a", "b"})
			_, err := rdb.SPopN(ctx, "s", 2).Result()
			return err
		},
		func() error {
			cm.ExpectSRandMember("s").SetVal("m")
			_, err := rdb.SRandMember(ctx, "s").Result()
			return err
		},
		func() error {
			cm.ExpectSRandMemberN("s", 2).SetVal([]string{"a", "b"})
			_, err := rdb.SRandMemberN(ctx, "s", 2).Result()
			return err
		},
		func() error {
			cm.ExpectSUnion("s1", "s2").SetVal([]string{"a", "b"})
			_, err := rdb.SUnion(ctx, "s1", "s2").Result()
			return err
		},
		func() error {
			cm.ExpectSUnionStore("d", "s1", "s2").SetVal(2)
			_, err := rdb.SUnionStore(ctx, "d", "s1", "s2").Result()
			return err
		},
	}
	runCases(t, cm, cases)
}

func TestSortedSetCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectZCard("z").SetVal(2)
			_, err := rdb.ZCard(ctx, "z").Result()
			return err
		},
		func() error {
			cm.ExpectZCount("z", "0", "10").SetVal(2)
			_, err := rdb.ZCount(ctx, "z", "0", "10").Result()
			return err
		},
		func() error {
			cm.ExpectZIncrBy("z", 1.0, "m").SetVal(2.0)
			_, err := rdb.ZIncrBy(ctx, "z", 1.0, "m").Result()
			return err
		},
		func() error {
			cm.ExpectZLexCount("z", "[a", "[z").SetVal(2)
			_, err := rdb.ZLexCount(ctx, "z", "[a", "[z").Result()
			return err
		},
		func() error {
			cm.ExpectZRange("z", 0, -1).SetVal([]string{"a", "b"})
			_, err := rdb.ZRange(ctx, "z", 0, -1).Result()
			return err
		},
		func() error {
			cm.ExpectZRevRange("z", 0, -1).SetVal([]string{"b", "a"})
			_, err := rdb.ZRevRange(ctx, "z", 0, -1).Result()
			return err
		},
		func() error {
			cm.ExpectZRank("z", "m").SetVal(0)
			_, err := rdb.ZRank(ctx, "z", "m").Result()
			return err
		},
		func() error {
			cm.ExpectZRevRank("z", "m").SetVal(0)
			_, err := rdb.ZRevRank(ctx, "z", "m").Result()
			return err
		},
		func() error {
			cm.ExpectZRem("z", "m1", "m2").SetVal(2)
			_, err := rdb.ZRem(ctx, "z", "m1", "m2").Result()
			return err
		},
		func() error {
			cm.ExpectZRemRangeByLex("z", "[a", "[z").SetVal(2)
			_, err := rdb.ZRemRangeByLex(ctx, "z", "[a", "[z").Result()
			return err
		},
		func() error {
			cm.ExpectZRemRangeByRank("z", 0, -1).SetVal(2)
			_, err := rdb.ZRemRangeByRank(ctx, "z", 0, -1).Result()
			return err
		},
		func() error {
			cm.ExpectZRemRangeByScore("z", "0", "10").SetVal(2)
			_, err := rdb.ZRemRangeByScore(ctx, "z", "0", "10").Result()
			return err
		},
		func() error {
			cm.ExpectZScore("z", "m").SetVal(1.5)
			_, err := rdb.ZScore(ctx, "z", "m").Result()
			return err
		},
		func() error {
			cm.ExpectZMScore("z", "m1", "m2").SetVal([]float64{1.0, 2.0})
			_, err := rdb.ZMScore(ctx, "z", "m1", "m2").Result()
			return err
		},
		func() error {
			cm.ExpectZPopMax("z").SetVal([]valkeycompat.Z{{Member: "m", Score: 1}})
			_, err := rdb.ZPopMax(ctx, "z").Result()
			return err
		},
		func() error {
			cm.ExpectZPopMax("z", 2).SetVal([]valkeycompat.Z{{Member: "m", Score: 1}})
			_, err := rdb.ZPopMax(ctx, "z", 2).Result()
			return err
		},
		func() error {
			cm.ExpectZPopMin("z").SetVal([]valkeycompat.Z{{Member: "m", Score: 1}})
			_, err := rdb.ZPopMin(ctx, "z").Result()
			return err
		},
		func() error {
			cm.ExpectZPopMin("z", 2).SetVal([]valkeycompat.Z{{Member: "m", Score: 1}})
			_, err := rdb.ZPopMin(ctx, "z", 2).Result()
			return err
		},
	}
	runCases(t, cm, cases)
}

func TestServerCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectLastSave().SetVal(1)
			_, err := rdb.LastSave(ctx).Result()
			return err
		},
		func() error {
			cm.ExpectTime().SetVal(time.Unix(1, 0))
			_, err := rdb.Time(ctx).Result()
			return err
		},
		func() error {
			cm.ExpectInfo().SetVal("info")
			_, err := rdb.Info(ctx).Result()
			return err
		},
		func() error {
			cm.ExpectInfo("server", "memory").SetVal("info")
			_, err := rdb.Info(ctx, "server", "memory").Result()
			return err
		},
		func() error {
			cm.ExpectClientID().SetVal(1)
			_, err := rdb.ClientID(ctx).Result()
			return err
		},
		func() error {
			cm.ExpectClientGetName().SetVal("name")
			_, err := rdb.ClientGetName(ctx).Result()
			return err
		},
		func() error {
			cm.ExpectReadOnly().SetVal("OK")
			return rdb.ReadOnly(ctx).Err()
		},
		func() error {
			cm.ExpectReadWrite().SetVal("OK")
			return rdb.ReadWrite(ctx).Err()
		},
	}
	runCases(t, cm, cases)
}

func TestPubSubCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectPublish("ch", "msg").SetVal(1)
			_, err := rdb.Publish(ctx, "ch", "msg").Result()
			return err
		},
		func() error {
			cm.ExpectSPublish("ch", "msg").SetVal(1)
			_, err := rdb.SPublish(ctx, "ch", "msg").Result()
			return err
		},
		func() error {
			cm.ExpectPubSubChannels("p*").SetVal([]string{"ch1"})
			_, err := rdb.PubSubChannels(ctx, "p*").Result()
			return err
		},
		func() error {
			cm.ExpectPubSubNumPat().SetVal(1)
			_, err := rdb.PubSubNumPat(ctx).Result()
			return err
		},
	}
	runCases(t, cm, cases)
}

func TestHyperLogLogCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectPFAdd("k", "v").SetVal(1)
			_, err := rdb.PFAdd(ctx, "k", "v").Result()
			return err
		},
		func() error {
			cm.ExpectPFCount("k1", "k2").SetVal(2)
			_, err := rdb.PFCount(ctx, "k1", "k2").Result()
			return err
		},
		func() error {
			cm.ExpectPFMerge("d", "k1", "k2").SetVal("OK")
			return rdb.PFMerge(ctx, "d", "k1", "k2").Err()
		},
	}
	runCases(t, cm, cases)
}

func TestScriptingCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectEvalSha("sha", []string{"k"}, "a").SetVal("ok")
			_, err := rdb.EvalSha(ctx, "sha", []string{"k"}, "a").Result()
			return err
		},
		func() error {
			cm.ExpectEvalRO("script", []string{"k"}, "a").SetVal("ok")
			_, err := rdb.EvalRO(ctx, "script", []string{"k"}, "a").Result()
			return err
		},
		func() error {
			cm.ExpectEvalShaRO("sha", []string{"k"}, "a").SetVal("ok")
			_, err := rdb.EvalShaRO(ctx, "sha", []string{"k"}, "a").Result()
			return err
		},
	}
	runCases(t, cm, cases)
}

func TestClusterCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectClusterInfo().SetVal("ok")
			_, err := rdb.ClusterInfo(ctx).Result()
			return err
		},
		func() error {
			cm.ExpectClusterNodes().SetVal("ok")
			_, err := rdb.ClusterNodes(ctx).Result()
			return err
		},
		func() error {
			cm.ExpectClusterMyShardID().SetVal("id")
			_, err := rdb.ClusterMyShardID(ctx).Result()
			return err
		},
		func() error {
			cm.ExpectClusterKeySlot("k").SetVal(123)
			_, err := rdb.ClusterKeySlot(ctx, "k").Result()
			return err
		},
	}
	runCases(t, cm, cases)
}

func TestACLCommands(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	ctx := context.Background()
	raw := mock.NewClient(ctrl)
	cm := NewAdapter(raw)
	rdb := valkeycompat.NewAdapter(raw)

	cases := []func() error{
		func() error {
			cm.ExpectACLList().SetVal([]string{"u1"})
			_, err := rdb.ACLList(ctx).Result()
			return err
		},
		func() error {
			cm.ExpectACLDelUser("u1").SetVal(1)
			_, err := rdb.ACLDelUser(ctx, "u1").Result()
			return err
		},
	}
	runCases(t, cm, cases)
}

func runCases(t *testing.T, cm *ClientMock, cases []func() error) {
	t.Helper()
	for i, c := range cases {
		if err := c(); err != nil {
			t.Fatalf("case %d unexpected err %v", i, err)
		}
	}
	if err := cm.ExpectationsWereMet(); err != nil {
		t.Fatalf("unexpected err %v", err)
	}
}
