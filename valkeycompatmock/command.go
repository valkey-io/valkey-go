package valkeycompatmock

import (
	"net"
	"sort"
	"strconv"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/mock"
	"github.com/valkey-io/valkey-go/valkeycompat"
)

type ExpectedString struct{ exp *expectation }

func (e *ExpectedString) SetVal(v string) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyString(v))
}

func (e *ExpectedString) SetErr(err error) {
	e.exp.setErr(err)
}

func (e *ExpectedString) RedisNil() {
	e.exp.setRedisNil()
}

type ExpectedStatus = ExpectedString

type ExpectedError = ExpectedStatus

type ExpectedBool struct {
	exp    *expectation
	encode func(bool) valkey.ValkeyMessage
}

func (e *ExpectedBool) SetVal(v bool) {
	if e.encode != nil {
		e.exp.resultSet = true
		e.exp.result = mock.Result(e.encode(v))
		return
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyBool(v))
}

func (e *ExpectedBool) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedInt struct{ exp *expectation }

func (e *ExpectedInt) SetVal(v int64) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyInt64(v))
}

func (e *ExpectedInt) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedDuration struct {
	exp       *expectation
	precision time.Duration
}

func (e *ExpectedDuration) SetVal(v time.Duration) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyInt64(int64(v / e.precision)))
}

func (e *ExpectedDuration) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedFloat struct{ exp *expectation }

func (e *ExpectedFloat) SetVal(v float64) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyFloat64(v))
}

func (e *ExpectedFloat) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedStringSlice struct{ exp *expectation }

func (e *ExpectedStringSlice) SetVal(v []string) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, s := range v {
		msgs = append(msgs, mock.ValkeyString(s))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedStringSlice) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedIntSlice struct{ exp *expectation }

func (e *ExpectedIntSlice) SetVal(v []int64) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, n := range v {
		msgs = append(msgs, mock.ValkeyInt64(n))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedIntSlice) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedBoolSlice struct{ exp *expectation }

func (e *ExpectedBoolSlice) SetVal(v []bool) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, b := range v {
		if b {
			msgs = append(msgs, mock.ValkeyInt64(1))
		} else {
			msgs = append(msgs, mock.ValkeyInt64(0))
		}
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedBoolSlice) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedFloatSlice struct{ exp *expectation }

func (e *ExpectedFloatSlice) SetVal(v []float64) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, f := range v {
		msgs = append(msgs, mock.ValkeyFloat64(f))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedFloatSlice) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedSlice struct{ exp *expectation }

func (e *ExpectedSlice) SetVal(v []any) {
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
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedSlice) SetErr(err error) {
	e.exp.setErr(err)
}

func valkeyMessage(v any) valkey.ValkeyMessage {
	switch x := v.(type) {
	case nil:
		return mock.ValkeyNil()
	case string:
		return mock.ValkeyString(x)
	case int:
		return mock.ValkeyInt64(int64(x))
	case int64:
		return mock.ValkeyInt64(x)
	case uint64:
		return mock.ValkeyString(itoa(int64(x)))
	case bool:
		return mock.ValkeyBool(x)
	case float64:
		return mock.ValkeyFloat64(x)
	case []any:
		msgs := make([]valkey.ValkeyMessage, 0, len(x))
		for _, item := range x {
			msgs = append(msgs, valkeyMessage(item))
		}
		return mock.ValkeyArray(msgs...)
	case []string:
		msgs := make([]valkey.ValkeyMessage, 0, len(x))
		for _, item := range x {
			msgs = append(msgs, mock.ValkeyString(item))
		}
		return mock.ValkeyArray(msgs...)
	default:
		return mock.ValkeyString(str(v))
	}
}

type ExpectedStringStringMap struct{ exp *expectation }

type ExpectedMapStringString = ExpectedStringStringMap

func (e *ExpectedStringStringMap) SetVal(v map[string]string) {
	kv := make(map[string]valkey.ValkeyMessage, len(v))
	for k, val := range v {
		kv[k] = mock.ValkeyString(val)
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyMap(kv))
}

func (e *ExpectedStringStringMap) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedStringIntMap struct{ exp *expectation }

type ExpectedMapStringInt = ExpectedStringIntMap

func (e *ExpectedStringIntMap) SetVal(v map[string]int64) {
	kv := make(map[string]valkey.ValkeyMessage, len(v))
	for k, val := range v {
		kv[k] = mock.ValkeyInt64(val)
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyMap(kv))
}

func (e *ExpectedStringIntMap) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedStringStructMap struct{ exp *expectation }

func (e *ExpectedStringStructMap) SetVal(v []string) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, s := range v {
		msgs = append(msgs, mock.ValkeyString(s))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedStringStructMap) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedKeyValueSlice struct{ exp *expectation }

func (e *ExpectedKeyValueSlice) SetVal(v []valkeycompat.KeyValue) {
	pairs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, kv := range v {
		pairs = append(pairs, mock.ValkeyArray(mock.ValkeyString(kv.Key), mock.ValkeyString(kv.Value)))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(pairs...))
}

func (e *ExpectedKeyValueSlice) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedKeyValues struct{ exp *expectation }

func (e *ExpectedKeyValues) SetVal(key string, vals []string) {
	msgs := make([]valkey.ValkeyMessage, 0, len(vals))
	for _, v := range vals {
		msgs = append(msgs, mock.ValkeyString(v))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(mock.ValkeyString(key), mock.ValkeyArray(msgs...)))
}

func (e *ExpectedKeyValues) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedZSlice struct{ exp *expectation }

func (e *ExpectedZSlice) SetVal(v []valkeycompat.Z) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v)*2)
	for _, z := range v {
		msgs = append(msgs, mock.ValkeyString(str(z.Member)), mock.ValkeyString(str(z.Score)))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedZSlice) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedZWithKey struct{ exp *expectation }

func (e *ExpectedZWithKey) SetVal(v *valkeycompat.ZWithKey) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(
		mock.ValkeyString(v.Key),
		mock.ValkeyString(str(v.Member)),
		mock.ValkeyString(str(v.Score)),
	))
}

func (e *ExpectedZWithKey) SetErr(err error) {
	e.exp.setErr(err)
}

func (e *ExpectedZWithKey) RedisNil() {
	e.exp.setRedisNil()
}

type ExpectedRankWithScore struct{ exp *expectation }

func (e *ExpectedRankWithScore) SetVal(v valkeycompat.RankScore) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(
		mock.ValkeyInt64(v.Rank),
		mock.ValkeyFloat64(v.Score),
	))
}

func (e *ExpectedRankWithScore) SetErr(err error) {
	e.exp.setErr(err)
}

func (e *ExpectedRankWithScore) RedisNil() {
	e.exp.setRedisNil()
}

type ExpectedTime struct{ exp *expectation }

func (e *ExpectedTime) SetVal(v time.Time) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(
		mock.ValkeyString(itoa(v.Unix())),
		mock.ValkeyString(itoa(int64(v.Nanosecond()/1000))),
	))
}

func (e *ExpectedTime) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedScan struct{ exp *expectation }

func (e *ExpectedScan) SetVal(keys []string, cursor uint64) {
	msgs := make([]valkey.ValkeyMessage, 0, len(keys))
	for _, k := range keys {
		msgs = append(msgs, mock.ValkeyString(k))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(
		mock.ValkeyString(itoa(int64(cursor))),
		mock.ValkeyArray(msgs...),
	))
}

func (e *ExpectedScan) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedGeoPos struct{ exp *expectation }

func (e *ExpectedGeoPos) SetVal(v []*valkeycompat.GeoPos) {
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
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedGeoPos) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedGeoLocation struct{ exp *expectation }

type ExpectedGeoSearchLocation = ExpectedGeoLocation

func (e *ExpectedGeoLocation) SetVal(v []valkeycompat.GeoLocation) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, l := range v {
		parts := []valkey.ValkeyMessage{mock.ValkeyString(l.Name)}
		if l.Dist != 0 {
			parts = append(parts, mock.ValkeyString(str(l.Dist)))
		}
		if l.GeoHash != 0 {
			parts = append(parts, mock.ValkeyInt64(l.GeoHash))
		}
		if l.Longitude != 0 || l.Latitude != 0 {
			parts = append(parts, mock.ValkeyArray(
				mock.ValkeyFloat64(l.Longitude),
				mock.ValkeyFloat64(l.Latitude),
			))
		}
		if len(parts) == 1 {
			msgs = append(msgs, parts[0])
			continue
		}
		msgs = append(msgs, mock.ValkeyArray(parts...))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedGeoLocation) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedXMessageSlice struct{ exp *expectation }

func (e *ExpectedXMessageSlice) SetVal(v []valkeycompat.XMessage) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, m := range v {
		msgs = append(msgs, xMessageEntry(m))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedXMessageSlice) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedXStreamSlice struct{ exp *expectation }

func (e *ExpectedXStreamSlice) SetVal(v []valkeycompat.XStream) {
	streams := make([]valkey.ValkeyMessage, 0, len(v))
	for _, s := range v {
		entries := make([]valkey.ValkeyMessage, 0, len(s.Messages))
		for _, m := range s.Messages {
			entries = append(entries, xMessageEntry(m))
		}
		streams = append(streams, mock.ValkeyArray(
			mock.ValkeyString(s.Stream),
			mock.ValkeyArray(entries...),
		))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(streams...))
}

func (e *ExpectedXStreamSlice) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedCmd struct{ exp *expectation }

func (e *ExpectedCmd) SetVal(v any) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(valkeyMessage(v))
	e.exp.rawSet = true
	e.exp.rawResult = v
}

func (e *ExpectedCmd) SetValInt(v int64) *ExpectedCmd {
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyInt64(v))
	return e
}

func (e *ExpectedCmd) SetErr(err error) {
	e.exp.setErr(err)
}

func (e *ExpectedCmd) RedisNil() {
	e.exp.setRedisNil()
}

type ExpectedCommandsInfo struct{ exp *expectation }

func (e *ExpectedCommandsInfo) SetVal(v []*valkeycompat.CommandInfo) {
	entries := make([]valkey.ValkeyMessage, 0, len(v))
	for _, info := range v {
		flags := info.Flags
		if flags == nil {
			flags = []string{}
		}
		flagMsgs := make([]valkey.ValkeyMessage, len(flags))
		for i, f := range flags {
			flagMsgs[i] = mock.ValkeyString(f)
		}
		inner := []valkey.ValkeyMessage{
			mock.ValkeyString(info.Name),
			mock.ValkeyInt64(info.Arity),
			mock.ValkeyArray(flagMsgs...),
			mock.ValkeyInt64(info.FirstKeyPos),
			mock.ValkeyInt64(info.LastKeyPos),
			mock.ValkeyInt64(info.StepCount),
		}
		if info.ACLFlags != nil {
			aclMsgs := make([]valkey.ValkeyMessage, len(info.ACLFlags))
			for i, f := range info.ACLFlags {
				aclMsgs[i] = mock.ValkeyString(f)
			}
			inner = append(inner, mock.ValkeyArray(aclMsgs...))
		}
		entries = append(entries, mock.ValkeyArray(inner...))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(entries...))
}

func (e *ExpectedCommandsInfo) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedKeyFlags struct{ exp *expectation }

func (e *ExpectedKeyFlags) SetVal(v []valkeycompat.KeyFlags) {
	entries := make([]valkey.ValkeyMessage, 0, len(v))
	for _, kf := range v {
		flagMsgs := make([]valkey.ValkeyMessage, len(kf.Flags))
		for i, f := range kf.Flags {
			flagMsgs[i] = mock.ValkeyString(f)
		}
		entries = append(entries, mock.ValkeyArray(
			mock.ValkeyString(kf.Key),
			mock.ValkeyArray(flagMsgs...),
		))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(entries...))
}

func (e *ExpectedKeyFlags) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedSlowLog struct{ exp *expectation }

func (e *ExpectedSlowLog) SetVal(v []valkeycompat.SlowLog) {
	entries := make([]valkey.ValkeyMessage, 0, len(v))
	for _, entry := range v {
		argMsgs := make([]valkey.ValkeyMessage, len(entry.Args))
		for i, a := range entry.Args {
			argMsgs[i] = mock.ValkeyString(a)
		}
		inner := []valkey.ValkeyMessage{
			mock.ValkeyInt64(entry.ID),
			mock.ValkeyInt64(entry.Time.Unix()),
			mock.ValkeyInt64(int64(entry.Duration / time.Microsecond)),
			mock.ValkeyArray(argMsgs...),
		}
		if entry.ClientAddr != "" {
			inner = append(inner, mock.ValkeyString(entry.ClientAddr))
		}
		if entry.ClientName != "" {
			if entry.ClientAddr == "" {
				inner = append(inner, mock.ValkeyString(""))
			}
			inner = append(inner, mock.ValkeyString(entry.ClientName))
		}
		entries = append(entries, mock.ValkeyArray(inner...))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(entries...))
}

func (e *ExpectedSlowLog) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedOKBool struct{ exp *expectation }

func (e *ExpectedOKBool) SetVal(v bool) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(okBoolMessage(v))
}

func (e *ExpectedOKBool) SetErr(err error) {
	e.exp.setErr(err)
}

func okBoolMessage(v bool) valkey.ValkeyMessage {
	if v {
		return mock.ValkeyString("OK")
	}
	return mock.ValkeyString("")
}

type ExpectedClusterSlots struct{ exp *expectation }

func (e *ExpectedClusterSlots) SetVal(v []valkeycompat.ClusterSlot) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(clusterSlotMessages(v)...))
}

func (e *ExpectedClusterSlots) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedClusterShards struct{ exp *expectation }

func (e *ExpectedClusterShards) SetVal(v []valkeycompat.ClusterShard) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, shard := range v {
		msgs = append(msgs, clusterShardMessage(shard))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedClusterShards) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedClusterLinks struct{ exp *expectation }

func (e *ExpectedClusterLinks) SetVal(v []valkeycompat.ClusterLink) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, link := range v {
		msgs = append(msgs, clusterLinkMessage(link))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedClusterLinks) SetErr(err error) {
	e.exp.setErr(err)
}

func clusterSlotMessages(slots []valkeycompat.ClusterSlot) []valkey.ValkeyMessage {
	out := make([]valkey.ValkeyMessage, 0, len(slots))
	for _, slot := range slots {
		elems := []valkey.ValkeyMessage{
			mock.ValkeyInt64(slot.Start),
			mock.ValkeyInt64(slot.End),
		}
		for _, node := range slot.Nodes {
			elems = append(elems, clusterNodeMessage(node))
		}
		out = append(out, mock.ValkeyArray(elems...))
	}
	return out
}

func clusterNodeMessage(node valkeycompat.ClusterNode) valkey.ValkeyMessage {
	host, portStr, err := net.SplitHostPort(node.Addr)
	if err != nil {
		host = node.Addr
		portStr = "0"
	}
	port, _ := strconv.ParseInt(portStr, 10, 64)
	if node.ID != "" {
		return mock.ValkeyArray(
			mock.ValkeyString(host),
			mock.ValkeyInt64(port),
			mock.ValkeyString(node.ID),
		)
	}
	return mock.ValkeyArray(
		mock.ValkeyString(host),
		mock.ValkeyInt64(port),
	)
}

func clusterShardMessage(shard valkeycompat.ClusterShard) valkey.ValkeyMessage {
	slotPairs := make([]valkey.ValkeyMessage, 0, len(shard.Slots)*2)
	for _, sr := range shard.Slots {
		slotPairs = append(slotPairs, mock.ValkeyInt64(sr.Start), mock.ValkeyInt64(sr.End))
	}
	nodes := make([]valkey.ValkeyMessage, 0, len(shard.Nodes))
	for _, n := range shard.Nodes {
		kv := map[string]valkey.ValkeyMessage{}
		if n.ID != "" {
			kv["id"] = mock.ValkeyString(n.ID)
		}
		if n.Endpoint != "" {
			kv["endpoint"] = mock.ValkeyString(n.Endpoint)
		}
		if n.IP != "" {
			kv["ip"] = mock.ValkeyString(n.IP)
		}
		if n.Hostname != "" {
			kv["hostname"] = mock.ValkeyString(n.Hostname)
		}
		if n.Port != 0 {
			kv["port"] = mock.ValkeyInt64(n.Port)
		}
		if n.TLSPort != 0 {
			kv["tls-port"] = mock.ValkeyInt64(n.TLSPort)
		}
		if n.Role != "" {
			kv["role"] = mock.ValkeyString(n.Role)
		}
		if n.ReplicationOffset != 0 {
			kv["replication-offset"] = mock.ValkeyInt64(n.ReplicationOffset)
		}
		if n.Health != "" {
			kv["health"] = mock.ValkeyString(n.Health)
		}
		nodes = append(nodes, mock.ValkeyMap(kv))
	}
	return mock.ValkeyMap(map[string]valkey.ValkeyMessage{
		"slots": mock.ValkeyArray(slotPairs...),
		"nodes": mock.ValkeyArray(nodes...),
	})
}

func clusterLinkMessage(link valkeycompat.ClusterLink) valkey.ValkeyMessage {
	return mock.ValkeyMap(map[string]valkey.ValkeyMessage{
		"direction":             mock.ValkeyString(link.Direction),
		"node":                  mock.ValkeyString(link.Node),
		"create-time":           mock.ValkeyInt64(link.CreateTime),
		"events":                mock.ValkeyString(link.Events),
		"send-buffer-allocated": mock.ValkeyInt64(link.SendBufferAllocated),
		"send-buffer-used":      mock.ValkeyInt64(link.SendBufferUsed),
	})
}

type ExpectedLCS struct {
	exp          *expectation
	readType     uint8
	withMatchLen bool
}

func (e *ExpectedLCS) SetVal(v *valkeycompat.LCSMatch) {
	switch e.readType {
	case 2:
		e.exp.resultSet = true
		e.exp.result = mock.Result(mock.ValkeyInt64(v.Len))
	case 3:
		matches := make([]valkey.ValkeyMessage, len(v.Matches))
		for i, mp := range v.Matches {
			inner := []valkey.ValkeyMessage{
				mock.ValkeyArray(mock.ValkeyInt64(mp.Key1.Start), mock.ValkeyInt64(mp.Key1.End)),
				mock.ValkeyArray(mock.ValkeyInt64(mp.Key2.Start), mock.ValkeyInt64(mp.Key2.End)),
			}
			if e.withMatchLen {
				inner = append(inner, mock.ValkeyInt64(mp.MatchLen))
			}
			matches[i] = mock.ValkeyArray(inner...)
		}
		e.exp.resultSet = true
		e.exp.result = mock.Result(mock.ValkeyMap(map[string]valkey.ValkeyMessage{
			"matches": mock.ValkeyArray(matches...),
			"len":     mock.ValkeyInt64(v.Len),
		}))
	default:
		e.exp.resultSet = true
		e.exp.result = mock.Result(mock.ValkeyString(v.MatchString))
	}
}

func (e *ExpectedLCS) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedZSliceWithKey struct{ exp *expectation }

func (e *ExpectedZSliceWithKey) SetVal(key string, vals []valkeycompat.Z) {
	msgs := make([]valkey.ValkeyMessage, 0, len(vals)*2)
	for _, z := range vals {
		msgs = append(msgs, mock.ValkeyString(str(z.Member)), mock.ValkeyString(str(z.Score)))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(mock.ValkeyString(key), mock.ValkeyArray(msgs...)))
}

func (e *ExpectedZSliceWithKey) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedScriptExists struct{ exp *expectation }

func (e *ExpectedScriptExists) SetVal(v []bool) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, b := range v {
		if b {
			msgs = append(msgs, mock.ValkeyInt64(1))
		} else {
			msgs = append(msgs, mock.ValkeyInt64(0))
		}
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedScriptExists) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedFunctionList struct{ exp *expectation }

func (e *ExpectedFunctionList) SetVal(v []valkeycompat.Library) {
	entries := make([]valkey.ValkeyMessage, 0, len(v))
	for _, lib := range v {
		kv := map[string]valkey.ValkeyMessage{
			"library_name": mock.ValkeyString(lib.Name),
			"engine":       mock.ValkeyString(lib.Engine),
		}
		if lib.Code != "" {
			kv["library_code"] = mock.ValkeyString(lib.Code)
		}
		fns := make([]valkey.ValkeyMessage, 0, len(lib.Functions))
		for _, fn := range lib.Functions {
			flagMsgs := make([]valkey.ValkeyMessage, len(fn.Flags))
			for i, f := range fn.Flags {
				flagMsgs[i] = mock.ValkeyString(f)
			}
			fkv := map[string]valkey.ValkeyMessage{
				"name":        mock.ValkeyString(fn.Name),
				"description": mock.ValkeyString(fn.Description),
				"flags":       mock.ValkeyArray(flagMsgs...),
			}
			fns = append(fns, mock.ValkeyMap(fkv))
		}
		kv["functions"] = mock.ValkeyArray(fns...)
		entries = append(entries, mock.ValkeyMap(kv))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(entries...))
}

func (e *ExpectedFunctionList) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedXAutoClaim struct{ exp *expectation }

func (e *ExpectedXAutoClaim) SetVal(messages []valkeycompat.XMessage, start string) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(
		mock.ValkeyString(start),
		xMessagesValkeyArray(messages),
	))
}

func (e *ExpectedXAutoClaim) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedXAutoClaimJustID struct{ exp *expectation }

func (e *ExpectedXAutoClaimJustID) SetVal(ids []string, start string) {
	msgs := make([]valkey.ValkeyMessage, 0, len(ids))
	for _, id := range ids {
		msgs = append(msgs, mock.ValkeyString(id))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(
		mock.ValkeyString(start),
		mock.ValkeyArray(msgs...),
	))
}

func (e *ExpectedXAutoClaimJustID) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedXInfoConsumers struct{ exp *expectation }

func (e *ExpectedXInfoConsumers) SetVal(v []valkeycompat.XInfoConsumer) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, c := range v {
		kv := map[string]valkey.ValkeyMessage{}
		if c.Name != "" {
			kv["name"] = mock.ValkeyString(c.Name)
		}
		if c.Pending != 0 {
			kv["pending"] = mock.ValkeyInt64(c.Pending)
		}
		if c.Idle != 0 {
			kv["idle"] = mock.ValkeyInt64(int64(c.Idle / time.Millisecond))
		}
		msgs = append(msgs, mock.ValkeyMap(kv))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedXInfoConsumers) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedXInfoGroups struct{ exp *expectation }

func (e *ExpectedXInfoGroups) SetVal(v []valkeycompat.XInfoGroup) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, g := range v {
		msgs = append(msgs, xInfoGroupMessage(g))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedXInfoGroups) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedXInfoStream struct{ exp *expectation }

func (e *ExpectedXInfoStream) SetVal(v *valkeycompat.XInfoStream) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(xInfoStreamMessage(*v))
}

func (e *ExpectedXInfoStream) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedXInfoStreamFull struct{ exp *expectation }

func (e *ExpectedXInfoStreamFull) SetVal(v *valkeycompat.XInfoStreamFull) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(xInfoStreamFullMessage(*v))
}

func (e *ExpectedXInfoStreamFull) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedXPending struct{ exp *expectation }

func (e *ExpectedXPending) SetVal(v *valkeycompat.XPending) {
	e.exp.resultSet = true
	e.exp.result = mock.Result(xPendingMessage(*v))
}

func (e *ExpectedXPending) SetErr(err error) {
	e.exp.setErr(err)
}

type ExpectedXPendingExt struct{ exp *expectation }

func (e *ExpectedXPendingExt) SetVal(v []valkeycompat.XPendingExt) {
	msgs := make([]valkey.ValkeyMessage, 0, len(v))
	for _, p := range v {
		msgs = append(msgs, mock.ValkeyArray(
			mock.ValkeyString(p.ID),
			mock.ValkeyString(p.Consumer),
			mock.ValkeyInt64(int64(p.Idle/time.Millisecond)),
			mock.ValkeyInt64(p.RetryCount),
		))
	}
	e.exp.resultSet = true
	e.exp.result = mock.Result(mock.ValkeyArray(msgs...))
}

func (e *ExpectedXPendingExt) SetErr(err error) {
	e.exp.setErr(err)
}

func xMessagesValkeyArray(msgs []valkeycompat.XMessage) valkey.ValkeyMessage {
	entries := make([]valkey.ValkeyMessage, 0, len(msgs))
	for _, m := range msgs {
		entries = append(entries, xMessageEntry(m))
	}
	return mock.ValkeyArray(entries...)
}

func xMessageEntry(m valkeycompat.XMessage) valkey.ValkeyMessage {
	keys := make([]string, 0, len(m.Values))
	for k := range m.Values {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	fields := make([]valkey.ValkeyMessage, 0, len(keys)*2)
	for _, k := range keys {
		fields = append(fields, mock.ValkeyString(k), mock.ValkeyString(str(m.Values[k])))
	}
	return mock.ValkeyArray(mock.ValkeyString(m.ID), mock.ValkeyArray(fields...))
}

func xPendingMessage(v valkeycompat.XPending) valkey.ValkeyMessage {
	names := make([]string, 0, len(v.Consumers))
	for name := range v.Consumers {
		names = append(names, name)
	}
	sort.Strings(names)
	consumers := make([]valkey.ValkeyMessage, 0, len(names))
	for _, name := range names {
		consumers = append(consumers, mock.ValkeyArray(
			mock.ValkeyString(name),
			mock.ValkeyInt64(v.Consumers[name]),
		))
	}
	return mock.ValkeyArray(
		mock.ValkeyInt64(v.Count),
		mock.ValkeyString(v.Lower),
		mock.ValkeyString(v.Higher),
		mock.ValkeyArray(consumers...),
	)
}

func xInfoGroupMessage(g valkeycompat.XInfoGroup) valkey.ValkeyMessage {
	kv := map[string]valkey.ValkeyMessage{}
	if g.Name != "" {
		kv["name"] = mock.ValkeyString(g.Name)
	}
	if g.Consumers != 0 {
		kv["consumers"] = mock.ValkeyInt64(g.Consumers)
	}
	if g.Pending != 0 {
		kv["pending"] = mock.ValkeyInt64(g.Pending)
	}
	if g.EntriesRead != 0 {
		kv["entries-read"] = mock.ValkeyInt64(g.EntriesRead)
	}
	if g.Lag != 0 {
		kv["lag"] = mock.ValkeyInt64(g.Lag)
	}
	if g.LastDeliveredID != "" {
		kv["last-delivered-id"] = mock.ValkeyString(g.LastDeliveredID)
	}
	return mock.ValkeyMap(kv)
}

func xInfoStreamMessage(v valkeycompat.XInfoStream) valkey.ValkeyMessage {
	kv := map[string]valkey.ValkeyMessage{}
	if v.Length != 0 {
		kv["length"] = mock.ValkeyInt64(v.Length)
	}
	if v.RadixTreeKeys != 0 {
		kv["radix-tree-keys"] = mock.ValkeyInt64(v.RadixTreeKeys)
	}
	if v.RadixTreeNodes != 0 {
		kv["radix-tree-nodes"] = mock.ValkeyInt64(v.RadixTreeNodes)
	}
	if v.Groups != 0 {
		kv["groups"] = mock.ValkeyInt64(v.Groups)
	}
	if v.EntriesAdded != 0 {
		kv["entries-added"] = mock.ValkeyInt64(v.EntriesAdded)
	}
	if v.IDMPDuration != 0 {
		kv["idmp-duration"] = mock.ValkeyInt64(v.IDMPDuration)
	}
	if v.IDMPMaxSize != 0 {
		kv["idmp-maxsize"] = mock.ValkeyInt64(v.IDMPMaxSize)
	}
	if v.PIDsTracked != 0 {
		kv["pids-tracked"] = mock.ValkeyInt64(v.PIDsTracked)
	}
	if v.IIDsTracked != 0 {
		kv["iids-tracked"] = mock.ValkeyInt64(v.IIDsTracked)
	}
	if v.IIDsAdded != 0 {
		kv["iids-added"] = mock.ValkeyInt64(v.IIDsAdded)
	}
	if v.IIDsDuplicates != 0 {
		kv["iids-duplicates"] = mock.ValkeyInt64(v.IIDsDuplicates)
	}
	if v.LastGeneratedID != "" {
		kv["last-generated-id"] = mock.ValkeyString(v.LastGeneratedID)
	}
	if v.MaxDeletedEntryID != "" {
		kv["max-deleted-entry-id"] = mock.ValkeyString(v.MaxDeletedEntryID)
	}
	if v.RecordedFirstEntryID != "" {
		kv["recorded-first-entry-id"] = mock.ValkeyString(v.RecordedFirstEntryID)
	}
	if v.FirstEntry.ID != "" || len(v.FirstEntry.Values) > 0 {
		kv["first-entry"] = xMessageEntry(v.FirstEntry)
	}
	if v.LastEntry.ID != "" || len(v.LastEntry.Values) > 0 {
		kv["last-entry"] = xMessageEntry(v.LastEntry)
	}
	return mock.ValkeyMap(kv)
}

func xInfoStreamFullMessage(v valkeycompat.XInfoStreamFull) valkey.ValkeyMessage {
	kv := map[string]valkey.ValkeyMessage{}
	if v.Length != 0 {
		kv["length"] = mock.ValkeyInt64(v.Length)
	}
	if v.RadixTreeKeys != 0 {
		kv["radix-tree-keys"] = mock.ValkeyInt64(v.RadixTreeKeys)
	}
	if v.RadixTreeNodes != 0 {
		kv["radix-tree-nodes"] = mock.ValkeyInt64(v.RadixTreeNodes)
	}
	if v.EntriesAdded != 0 {
		kv["entries-added"] = mock.ValkeyInt64(v.EntriesAdded)
	}
	if v.IDMPDuration != 0 {
		kv["idmp-duration"] = mock.ValkeyInt64(v.IDMPDuration)
	}
	if v.IDMPMaxSize != 0 {
		kv["idmp-maxsize"] = mock.ValkeyInt64(v.IDMPMaxSize)
	}
	if v.PIDsTracked != 0 {
		kv["pids-tracked"] = mock.ValkeyInt64(v.PIDsTracked)
	}
	if v.IIDsTracked != 0 {
		kv["iids-tracked"] = mock.ValkeyInt64(v.IIDsTracked)
	}
	if v.IIDsAdded != 0 {
		kv["iids-added"] = mock.ValkeyInt64(v.IIDsAdded)
	}
	if v.IIDsDuplicates != 0 {
		kv["iids-duplicates"] = mock.ValkeyInt64(v.IIDsDuplicates)
	}
	if v.LastGeneratedID != "" {
		kv["last-generated-id"] = mock.ValkeyString(v.LastGeneratedID)
	}
	if v.MaxDeletedEntryID != "" {
		kv["max-deleted-entry-id"] = mock.ValkeyString(v.MaxDeletedEntryID)
	}
	if v.RecordedFirstEntryID != "" {
		kv["recorded-first-entry-id"] = mock.ValkeyString(v.RecordedFirstEntryID)
	}
	if len(v.Entries) > 0 {
		kv["entries"] = xMessagesValkeyArray(v.Entries)
	}
	if len(v.Groups) > 0 {
		groupMsgs := make([]valkey.ValkeyMessage, 0, len(v.Groups))
		for _, g := range v.Groups {
			groupMsgs = append(groupMsgs, xInfoStreamFullGroupMessage(g))
		}
		kv["groups"] = mock.ValkeyArray(groupMsgs...)
	}
	return mock.ValkeyMap(kv)
}

func xInfoStreamFullGroupMessage(g valkeycompat.XInfoStreamGroup) valkey.ValkeyMessage {
	kv := map[string]valkey.ValkeyMessage{}
	if g.Name != "" {
		kv["name"] = mock.ValkeyString(g.Name)
	}
	if g.LastDeliveredID != "" {
		kv["last-delivered-id"] = mock.ValkeyString(g.LastDeliveredID)
	}
	if g.EntriesRead != 0 {
		kv["entries-read"] = mock.ValkeyInt64(g.EntriesRead)
	}
	if g.Lag != 0 {
		kv["lag"] = mock.ValkeyInt64(g.Lag)
	}
	if g.PelCount != 0 {
		kv["pel-count"] = mock.ValkeyInt64(g.PelCount)
	}
	if len(g.Pending) > 0 {
		pending := make([]valkey.ValkeyMessage, 0, len(g.Pending))
		for _, p := range g.Pending {
			pending = append(pending, mock.ValkeyArray(
				mock.ValkeyString(p.ID),
				mock.ValkeyString(p.Consumer),
				mock.ValkeyInt64(p.DeliveryTime.UnixMilli()),
				mock.ValkeyInt64(p.DeliveryCount),
			))
		}
		kv["pending"] = mock.ValkeyArray(pending...)
	}
	if len(g.Consumers) > 0 {
		consumers := make([]valkey.ValkeyMessage, 0, len(g.Consumers))
		for _, c := range g.Consumers {
			consumerKV := map[string]valkey.ValkeyMessage{}
			if c.Name != "" {
				consumerKV["name"] = mock.ValkeyString(c.Name)
			}
			if !c.SeenTime.IsZero() {
				consumerKV["seen-time"] = mock.ValkeyInt64(c.SeenTime.UnixMilli())
			}
			if c.PelCount != 0 {
				consumerKV["pel-count"] = mock.ValkeyInt64(c.PelCount)
			}
			if len(c.Pending) > 0 {
				pending := make([]valkey.ValkeyMessage, 0, len(c.Pending))
				for _, p := range c.Pending {
					pending = append(pending, mock.ValkeyArray(
						mock.ValkeyString(p.ID),
						mock.ValkeyInt64(p.DeliveryTime.UnixMilli()),
						mock.ValkeyInt64(p.DeliveryCount),
					))
				}
				consumerKV["pending"] = mock.ValkeyArray(pending...)
			}
			consumers = append(consumers, mock.ValkeyMap(consumerKV))
		}
		kv["consumers"] = mock.ValkeyArray(consumers...)
	}
	return mock.ValkeyMap(kv)
}

func itoa(n int64) string {
	return strconv.FormatInt(n, 10)
}
