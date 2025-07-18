// Copyright (c) 2013 The github.com/go-redis/redis Authors.
// All rights reserved.
//
// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:
//
// * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
// * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//
// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

package valkeycompat

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/valkey-io/valkey-go"
	"github.com/valkey-io/valkey-go/internal/util"
)

type Cmder interface {
	SetErr(error)
	Err() error
	from(result valkey.ValkeyResult)
}

type baseCmd[T any] struct {
	err    error
	val    T
	rawVal any
}

func (cmd *baseCmd[T]) SetVal(val T) {
	cmd.val = val
}

func (cmd *baseCmd[T]) Val() T {
	return cmd.val
}

func (cmd *baseCmd[T]) SetRawVal(rawVal any) {
	cmd.rawVal = rawVal
}

func (cmd *baseCmd[T]) RawVal() any {
	return cmd.rawVal
}

func (cmd *baseCmd[T]) SetErr(err error) {
	cmd.err = err
}

func (cmd *baseCmd[T]) Err() error {
	return cmd.err
}

func (cmd *baseCmd[T]) Result() (T, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *baseCmd[T]) RawResult() (any, error) {
	return cmd.RawVal(), cmd.Err()
}

type Cmd struct {
	baseCmd[any]
}

func (cmd *Cmd) from(res valkey.ValkeyResult) {
	val, err := res.ToAny()
	if err != nil {
		cmd.err = err
		return
	}
	cmd.SetVal(val)
}

func newCmd(res valkey.ValkeyResult) *Cmd {
	cmd := &Cmd{}
	cmd.from(res)
	return cmd
}

func (cmd *Cmd) Text() (string, error) {
	if cmd.err != nil {
		return "", cmd.err
	}
	return toString(cmd.val)
}

func toString(val any) (string, error) {
	switch val := val.(type) {
	case string:
		return val, nil
	default:
		err := fmt.Errorf("valkey: unexpected type=%T for String", val)
		return "", err
	}
}

func (cmd *Cmd) Int() (int, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	switch val := cmd.val.(type) {
	case int64:
		return int(val), nil
	case string:
		return strconv.Atoi(val)
	default:
		err := fmt.Errorf("valkey: unexpected type=%T for Int", val)
		return 0, err
	}
}

func (cmd *Cmd) Int64() (int64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return toInt64(cmd.val)
}

func toInt64(val any) (int64, error) {
	switch val := val.(type) {
	case int64:
		return val, nil
	case string:
		return strconv.ParseInt(val, 10, 64)
	default:
		err := fmt.Errorf("valkey: unexpected type=%T for Int64", val)
		return 0, err
	}
}

func (cmd *Cmd) Uint64() (uint64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return toUint64(cmd.val)
}

func toUint64(val any) (uint64, error) {
	switch val := val.(type) {
	case int64:
		return uint64(val), nil
	case string:
		return strconv.ParseUint(val, 10, 64)
	default:
		err := fmt.Errorf("valkey: unexpected type=%T for Uint64", val)
		return 0, err
	}
}

func (cmd *Cmd) Float32() (float32, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return toFloat32(cmd.val)
}

func toFloat32(val any) (float32, error) {
	switch val := val.(type) {
	case int64:
		return float32(val), nil
	case string:
		f, err := util.ToFloat32(val)
		if err != nil {
			return 0, err
		}
		return f, nil
	default:
		err := fmt.Errorf("valkey: unexpected type=%T for Float32", val)
		return 0, err
	}
}

func (cmd *Cmd) Float64() (float64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return toFloat64(cmd.val)
}

func toFloat64(val any) (float64, error) {
	switch val := val.(type) {
	case int64:
		return float64(val), nil
	case string:
		return util.ToFloat64(val)
	default:
		err := fmt.Errorf("valkey: unexpected type=%T for Float64", val)
		return 0, err
	}
}

func (cmd *Cmd) Bool() (bool, error) {
	if cmd.err != nil {
		return false, cmd.err
	}
	return toBool(cmd.val)
}

func toBool(val any) (bool, error) {
	switch val := val.(type) {
	case int64:
		return val != 0, nil
	case string:
		return strconv.ParseBool(val)
	default:
		err := fmt.Errorf("valkey: unexpected type=%T for Bool", val)
		return false, err
	}
}

func (cmd *Cmd) Slice() ([]any, error) {
	if cmd.err != nil {
		return nil, cmd.err
	}
	switch val := cmd.val.(type) {
	case []any:
		return val, nil
	default:
		return nil, fmt.Errorf("valkey: unexpected type=%T for Slice", val)
	}
}

func (cmd *Cmd) StringSlice() ([]string, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	ss := make([]string, len(slice))
	for i, iface := range slice {
		val, err := toString(iface)
		if err != nil {
			return nil, err
		}
		ss[i] = val
	}
	return ss, nil
}

func (cmd *Cmd) Int64Slice() ([]int64, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	nums := make([]int64, len(slice))
	for i, iface := range slice {
		val, err := toInt64(iface)
		if err != nil {
			return nil, err
		}
		nums[i] = val
	}
	return nums, nil
}

func (cmd *Cmd) Uint64Slice() ([]uint64, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	nums := make([]uint64, len(slice))
	for i, iface := range slice {
		val, err := toUint64(iface)
		if err != nil {
			return nil, err
		}
		nums[i] = val
	}
	return nums, nil
}

func (cmd *Cmd) Float32Slice() ([]float32, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	floats := make([]float32, len(slice))
	for i, iface := range slice {
		val, err := toFloat32(iface)
		if err != nil {
			return nil, err
		}
		floats[i] = val
	}
	return floats, nil
}

func (cmd *Cmd) Float64Slice() ([]float64, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	floats := make([]float64, len(slice))
	for i, iface := range slice {
		val, err := toFloat64(iface)
		if err != nil {
			return nil, err
		}
		floats[i] = val
	}
	return floats, nil
}

func (cmd *Cmd) BoolSlice() ([]bool, error) {
	slice, err := cmd.Slice()
	if err != nil {
		return nil, err
	}

	bools := make([]bool, len(slice))
	for i, iface := range slice {
		val, err := toBool(iface)
		if err != nil {
			return nil, err
		}
		bools[i] = val
	}
	return bools, nil
}

type StringCmd struct {
	baseCmd[string]
}

func (cmd *StringCmd) from(res valkey.ValkeyResult) {
	val, err := res.ToString()
	cmd.SetErr(err)
	cmd.SetVal(val)
}

func newStringCmd(res valkey.ValkeyResult) *StringCmd {
	cmd := &StringCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *StringCmd) Bytes() ([]byte, error) {
	return []byte(cmd.val), cmd.err
}

func (cmd *StringCmd) Bool() (bool, error) {
	return cmd.val != "", cmd.err
}

func (cmd *StringCmd) Int() (int, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.Atoi(cmd.Val())
}

func (cmd *StringCmd) Int64() (int64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.ParseInt(cmd.Val(), 10, 64)
}

func (cmd *StringCmd) Uint64() (uint64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return strconv.ParseUint(cmd.Val(), 10, 64)
}

func (cmd *StringCmd) Float32() (float32, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	v, err := util.ToFloat32(cmd.Val())
	if err != nil {
		return 0, err
	}
	return v, nil
}

func (cmd *StringCmd) Float64() (float64, error) {
	if cmd.err != nil {
		return 0, cmd.err
	}
	return util.ToFloat64(cmd.Val())
}

func (cmd *StringCmd) Time() (time.Time, error) {
	if cmd.err != nil {
		return time.Time{}, cmd.err
	}
	return time.Parse(time.RFC3339Nano, cmd.Val())
}

func (cmd *StringCmd) String() string {
	return cmd.val
}

type BoolCmd struct {
	baseCmd[bool]
}

func (cmd *BoolCmd) from(res valkey.ValkeyResult) {
	val, err := res.AsBool()
	if valkey.IsValkeyNil(err) {
		val = false
		err = nil
	}
	cmd.SetVal(val)
	cmd.SetErr(err)
}

func newBoolCmd(res valkey.ValkeyResult) *BoolCmd {
	cmd := &BoolCmd{}
	cmd.from(res)
	return cmd
}

type IntCmd struct {
	baseCmd[int64]
}

func (cmd *IntCmd) from(res valkey.ValkeyResult) {
	val, err := res.AsInt64()
	cmd.SetErr(err)
	cmd.SetVal(val)
}

func newIntCmd(res valkey.ValkeyResult) *IntCmd {
	cmd := &IntCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *IntCmd) Uint64() (uint64, error) {
	return uint64(cmd.val), cmd.err
}

type DurationCmd struct {
	baseCmd[time.Duration]
	precision time.Duration
}

func (cmd *DurationCmd) from(res valkey.ValkeyResult) {
	val, err := res.AsInt64()
	cmd.SetErr(err)
	if val > 0 {
		cmd.SetVal(time.Duration(val) * cmd.precision)
		return
	}
	cmd.SetVal(time.Duration(val))
}

func newDurationCmd(res valkey.ValkeyResult, precision time.Duration) *DurationCmd {
	cmd := &DurationCmd{precision: precision}
	cmd.from(res)
	return cmd
}

type StatusCmd = StringCmd

func newStatusCmd(res valkey.ValkeyResult) *StatusCmd {
	cmd := &StatusCmd{}
	val, err := res.ToString()
	cmd.SetErr(err)
	cmd.SetVal(val)
	return cmd
}

type SliceCmd struct {
	baseCmd[[]any]
	keys []string
	json bool
}

func (cmd *SliceCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	vals := make([]any, len(arr))
	if cmd.json {
		for i, v := range arr {
			// for JSON.OBJKEYS
			if v.IsNil() {
				continue
			}
			// convert to any which underlying type is []any
			arr, err := v.ToAny()
			if err != nil {
				cmd.SetErr(err)
				return
			}
			vals[i] = arr
		}
		cmd.SetVal(vals)
		return
	}
	for i, v := range arr {
		// keep the old behavior the same as before (don't handle error while parsing v as string)
		if s, err := v.ToString(); err == nil {
			vals[i] = s
		}
	}
	cmd.SetVal(vals)
}

// newSliceCmd returns SliceCmd according to input arguments, if the caller is JSONObjKeys,
// set isJSONObjKeys to true.
func newSliceCmd(res valkey.ValkeyResult, isJSONObjKeys bool, keys ...string) *SliceCmd {
	cmd := &SliceCmd{keys: keys, json: isJSONObjKeys}
	cmd.from(res)
	return cmd
}

// Scan scans the results from the map into a destination struct. The map keys
// are matched in the Valkey struct fields by the `valkey:"field"` tag.
// NOTE: result from JSON.ObjKeys should not call this.
func (cmd *SliceCmd) Scan(dst any) error {
	if cmd.err != nil {
		return cmd.err
	}
	return Scan(dst, cmd.keys, cmd.val)
}

type StringSliceCmd struct {
	baseCmd[[]string]
}

func (cmd *StringSliceCmd) from(res valkey.ValkeyResult) {
	val, err := res.AsStrSlice()
	cmd.SetVal(val)
	cmd.SetErr(err)
}

func newStringSliceCmd(res valkey.ValkeyResult) *StringSliceCmd {
	cmd := &StringSliceCmd{}
	cmd.from(res)
	return cmd
}

type IntSliceCmd struct {
	err error
	val []int64
}

func (cmd *IntSliceCmd) from(res valkey.ValkeyResult) {
	cmd.val, cmd.err = res.AsIntSlice()

}

func newIntSliceCmd(res valkey.ValkeyResult) *IntSliceCmd {
	cmd := &IntSliceCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *IntSliceCmd) SetVal(val []int64) {
	cmd.val = val
}

func (cmd *IntSliceCmd) SetErr(err error) {
	cmd.err = err
}

func (cmd *IntSliceCmd) Val() []int64 {
	return cmd.val
}

func (cmd *IntSliceCmd) Err() error {
	return cmd.err
}

func (cmd *IntSliceCmd) Result() ([]int64, error) {
	return cmd.val, cmd.err
}

type BoolSliceCmd struct {
	baseCmd[[]bool]
}

func (cmd *BoolSliceCmd) from(res valkey.ValkeyResult) {
	ints, err := res.AsIntSlice()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make([]bool, 0, len(ints))
	for _, i := range ints {
		val = append(val, i == 1)
	}
	cmd.SetVal(val)
}

func newBoolSliceCmd(res valkey.ValkeyResult) *BoolSliceCmd {
	cmd := &BoolSliceCmd{}
	cmd.from(res)
	return cmd
}

type FloatSliceCmd struct {
	baseCmd[[]float64]
}

func (cmd *FloatSliceCmd) from(res valkey.ValkeyResult) {
	val, err := res.AsFloatSlice()
	cmd.SetErr(err)
	cmd.SetVal(val)
}

func newFloatSliceCmd(res valkey.ValkeyResult) *FloatSliceCmd {
	cmd := &FloatSliceCmd{}
	cmd.from(res)
	return cmd
}

type ZSliceCmd struct {
	baseCmd[[]Z]
	single bool
}

func (cmd *ZSliceCmd) from(res valkey.ValkeyResult) {
	if cmd.single {
		s, err := res.AsZScore()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		cmd.SetVal([]Z{{Member: s.Member, Score: s.Score}})
	} else {
		scores, err := res.AsZScores()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		val := make([]Z, 0, len(scores))
		for _, s := range scores {
			val = append(val, Z{Member: s.Member, Score: s.Score})
		}
		cmd.SetVal(val)
	}
}

func newZSliceCmd(res valkey.ValkeyResult) *ZSliceCmd {
	cmd := &ZSliceCmd{}
	cmd.from(res)
	return cmd
}

func newZSliceSingleCmd(res valkey.ValkeyResult) *ZSliceCmd {
	cmd := &ZSliceCmd{single: true}
	cmd.from(res)
	return cmd
}

type FloatCmd struct {
	baseCmd[float64]
}

func (cmd *FloatCmd) from(res valkey.ValkeyResult) {
	val, err := res.AsFloat64()
	cmd.SetErr(err)
	cmd.SetVal(val)
}

func newFloatCmd(res valkey.ValkeyResult) *FloatCmd {
	cmd := &FloatCmd{}
	cmd.from(res)
	return cmd
}

type ScanCmd struct {
	err    error
	keys   []string
	cursor uint64
}

func (cmd *ScanCmd) from(res valkey.ValkeyResult) {
	e, err := res.AsScanEntry()
	if err != nil {
		cmd.err = err
		return
	}
	cmd.cursor, cmd.keys = e.Cursor, e.Elements
}

func newScanCmd(res valkey.ValkeyResult) *ScanCmd {
	cmd := &ScanCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *ScanCmd) SetVal(keys []string, cursor uint64) {
	cmd.keys = keys
	cmd.cursor = cursor
}

func (cmd *ScanCmd) Val() (keys []string, cursor uint64) {
	return cmd.keys, cmd.cursor
}

func (cmd *ScanCmd) SetErr(err error) {
	cmd.err = err
}

func (cmd *ScanCmd) Err() error {
	return cmd.err
}

func (cmd *ScanCmd) Result() (keys []string, cursor uint64, err error) {
	return cmd.keys, cmd.cursor, cmd.err
}

type KeyValue struct {
	Key   string
	Value string
}

type KeyValueSliceCmd struct {
	baseCmd[[]KeyValue]
}

func (cmd *KeyValueSliceCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	for _, a := range arr {
		kv, _ := a.AsStrSlice()
		for i := 0; i < len(kv); i += 2 {
			cmd.val = append(cmd.val, KeyValue{Key: kv[i], Value: kv[i+1]})
		}
	}
	cmd.SetErr(err)
}

func newKeyValueSliceCmd(res valkey.ValkeyResult) *KeyValueSliceCmd {
	cmd := &KeyValueSliceCmd{}
	cmd.from(res)
	return cmd
}

type KeyValuesCmd struct {
	err error
	val valkey.KeyValues
}

func (cmd *KeyValuesCmd) from(res valkey.ValkeyResult) {
	cmd.val, cmd.err = res.AsLMPop()
}

func newKeyValuesCmd(res valkey.ValkeyResult) *KeyValuesCmd {
	cmd := &KeyValuesCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *KeyValuesCmd) SetVal(key string, val []string) {
	cmd.val.Key = key
	cmd.val.Values = val
}

func (cmd *KeyValuesCmd) SetErr(err error) {
	cmd.err = err
}

func (cmd *KeyValuesCmd) Val() (string, []string) {
	return cmd.val.Key, cmd.val.Values
}

func (cmd *KeyValuesCmd) Err() error {
	return cmd.err
}

func (cmd *KeyValuesCmd) Result() (string, []string, error) {
	return cmd.val.Key, cmd.val.Values, cmd.err
}

type KeyFlags struct {
	Key   string
	Flags []string
}

type KeyFlagsCmd struct {
	baseCmd[[]KeyFlags]
}

func (cmd *KeyFlagsCmd) from(res valkey.ValkeyResult) {
	if cmd.err = res.Error(); cmd.err == nil {
		kfs, _ := res.ToArray()
		cmd.val = make([]KeyFlags, len(kfs))
		for i := 0; i < len(kfs); i++ {
			if kf, _ := kfs[i].ToArray(); len(kf) >= 2 {
				cmd.val[i].Key, _ = kf[0].ToString()
				cmd.val[i].Flags, _ = kf[1].AsStrSlice()
			}
		}
	}
}

func newKeyFlagsCmd(res valkey.ValkeyResult) *KeyFlagsCmd {
	cmd := &KeyFlagsCmd{}
	cmd.from(res)
	return cmd
}

type ZSliceWithKeyCmd struct {
	err error
	key string
	val []Z
}

func (cmd *ZSliceWithKeyCmd) from(res valkey.ValkeyResult) {
	v, err := res.AsZMPop()
	if err != nil {
		cmd.err = err
		return
	}
	val := make([]Z, 0, len(v.Values))
	for _, s := range v.Values {
		val = append(val, Z{Member: s.Member, Score: s.Score})
	}
	cmd.key, cmd.val = v.Key, val
}

func newZSliceWithKeyCmd(res valkey.ValkeyResult) *ZSliceWithKeyCmd {
	cmd := &ZSliceWithKeyCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *ZSliceWithKeyCmd) SetVal(key string, val []Z) {
	cmd.key = key
	cmd.val = val
}

func (cmd *ZSliceWithKeyCmd) SetErr(err error) {
	cmd.err = err
}

func (cmd *ZSliceWithKeyCmd) Val() (string, []Z) {
	return cmd.key, cmd.val
}

func (cmd *ZSliceWithKeyCmd) Err() error {
	return cmd.err
}

func (cmd *ZSliceWithKeyCmd) Result() (string, []Z, error) {
	return cmd.key, cmd.val, cmd.err
}

type StringStringMapCmd struct {
	baseCmd[map[string]string]
}

func (cmd *StringStringMapCmd) from(res valkey.ValkeyResult) {
	val, err := res.AsStrMap()
	cmd.SetErr(err)
	cmd.SetVal(val)
}

func newStringStringMapCmd(res valkey.ValkeyResult) *StringStringMapCmd {
	cmd := &StringStringMapCmd{}
	cmd.from(res)
	return cmd
}

// Scan scans the results from the map into a destination struct. The map keys
// are matched in the Valkey struct fields by the `valkey:"field"` tag.
func (cmd *StringStringMapCmd) Scan(dest interface{}) error {
	if cmd.Err() != nil {
		return cmd.Err()
	}

	strct, err := Struct(dest)
	if err != nil {
		return err
	}

	for k, v := range cmd.val {
		if err := strct.Scan(k, v); err != nil {
			return err
		}
	}

	return nil
}

type StringIntMapCmd struct {
	baseCmd[map[string]int64]
}

func (cmd *StringIntMapCmd) from(res valkey.ValkeyResult) {
	val, err := res.AsIntMap()
	cmd.SetErr(err)
	cmd.SetVal(val)
}

func newStringIntMapCmd(res valkey.ValkeyResult) *StringIntMapCmd {
	cmd := &StringIntMapCmd{}
	cmd.from(res)
	return cmd
}

type StringStructMapCmd struct {
	baseCmd[map[string]struct{}]
}

func (cmd *StringStructMapCmd) from(res valkey.ValkeyResult) {
	strSlice, err := res.AsStrSlice()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make(map[string]struct{}, len(strSlice))
	for _, v := range strSlice {
		val[v] = struct{}{}
	}
	cmd.SetVal(val)
}

func newStringStructMapCmd(res valkey.ValkeyResult) *StringStructMapCmd {
	cmd := &StringStructMapCmd{}
	cmd.from(res)
	return cmd
}

type XMessageSliceCmd struct {
	baseCmd[[]XMessage]
}

func (cmd *XMessageSliceCmd) from(res valkey.ValkeyResult) {
	val, err := res.AsXRange()
	cmd.SetErr(err)
	cmd.val = make([]XMessage, len(val))
	for i, r := range val {
		cmd.val[i] = newXMessage(r)
	}
}

func newXMessageSliceCmd(res valkey.ValkeyResult) *XMessageSliceCmd {
	cmd := &XMessageSliceCmd{}
	cmd.from(res)
	return cmd
}

func newXMessage(r valkey.XRangeEntry) XMessage {
	if r.FieldValues == nil {
		return XMessage{ID: r.ID, Values: nil}
	}
	m := XMessage{ID: r.ID, Values: make(map[string]any, len(r.FieldValues))}
	for k, v := range r.FieldValues {
		m.Values[k] = v
	}
	return m
}

type XStream struct {
	Stream   string
	Messages []XMessage
}

type XStreamSliceCmd struct {
	baseCmd[[]XStream]
}

func (cmd *XStreamSliceCmd) from(res valkey.ValkeyResult) {
	streams, err := res.AsXRead()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make([]XStream, 0, len(streams))
	for name, messages := range streams {
		msgs := make([]XMessage, 0, len(messages))
		for _, r := range messages {
			msgs = append(msgs, newXMessage(r))
		}
		val = append(val, XStream{Stream: name, Messages: msgs})
	}
	cmd.SetVal(val)
}

func newXStreamSliceCmd(res valkey.ValkeyResult) *XStreamSliceCmd {
	cmd := &XStreamSliceCmd{}
	cmd.from(res)
	return cmd
}

type XPending struct {
	Consumers map[string]int64
	Lower     string
	Higher    string
	Count     int64
}

type XPendingCmd struct {
	baseCmd[XPending]
}

func (cmd *XPendingCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	if len(arr) < 4 {
		cmd.SetErr(fmt.Errorf("got %d, wanted 4", len(arr)))
		return
	}
	count, err := arr[0].AsInt64()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	lower, err := arr[1].ToString()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	higher, err := arr[2].ToString()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := XPending{
		Count:  count,
		Lower:  lower,
		Higher: higher,
	}
	consumerArr, err := arr[3].ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	for _, v := range consumerArr {
		consumer, err := v.ToArray()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		if len(consumer) < 2 {
			cmd.SetErr(fmt.Errorf("got %d, wanted 2", len(arr)))
			return
		}
		consumerName, err := consumer[0].ToString()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		consumerPending, err := consumer[1].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		if val.Consumers == nil {
			val.Consumers = make(map[string]int64)
		}
		val.Consumers[consumerName] = consumerPending
	}
	cmd.SetVal(val)
}

func newXPendingCmd(res valkey.ValkeyResult) *XPendingCmd {
	cmd := &XPendingCmd{}
	cmd.from(res)
	return cmd
}

type XPendingExt struct {
	ID         string
	Consumer   string
	Idle       time.Duration
	RetryCount int64
}

type XPendingExtCmd struct {
	baseCmd[[]XPendingExt]
}

func (cmd *XPendingExtCmd) from(res valkey.ValkeyResult) {
	arrs, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make([]XPendingExt, 0, len(arrs))
	for _, v := range arrs {
		arr, err := v.ToArray()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		if len(arr) < 4 {
			cmd.SetErr(fmt.Errorf("got %d, wanted 4", len(arr)))
			return
		}
		id, err := arr[0].ToString()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		consumer, err := arr[1].ToString()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		idle, err := arr[2].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		retryCount, err := arr[3].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		val = append(val, XPendingExt{
			ID:         id,
			Consumer:   consumer,
			Idle:       time.Duration(idle) * time.Millisecond,
			RetryCount: retryCount,
		})
	}
	cmd.SetVal(val)
}

func newXPendingExtCmd(res valkey.ValkeyResult) *XPendingExtCmd {
	cmd := &XPendingExtCmd{}
	cmd.from(res)
	return cmd
}

type XAutoClaimCmd struct {
	err   error
	start string
	val   []XMessage
}

func (cmd *XAutoClaimCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	if len(arr) < 2 {
		cmd.SetErr(fmt.Errorf("got %d, wanted 2", len(arr)))
		return
	}
	start, err := arr[0].ToString()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	ranges, err := arr[1].AsXRange()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make([]XMessage, 0, len(ranges))
	for _, r := range ranges {
		val = append(val, newXMessage(r))
	}
	cmd.val, cmd.start = val, start
}

func newXAutoClaimCmd(res valkey.ValkeyResult) *XAutoClaimCmd {
	cmd := &XAutoClaimCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *XAutoClaimCmd) SetVal(val []XMessage, start string) {
	cmd.val = val
	cmd.start = start
}

func (cmd *XAutoClaimCmd) SetErr(err error) {
	cmd.err = err
}

func (cmd *XAutoClaimCmd) Val() (messages []XMessage, start string) {
	return cmd.val, cmd.start
}

func (cmd *XAutoClaimCmd) Err() error {
	return cmd.err
}

func (cmd *XAutoClaimCmd) Result() (messages []XMessage, start string, err error) {
	return cmd.val, cmd.start, cmd.err
}

type XAutoClaimJustIDCmd struct {
	err   error
	start string
	val   []string
}

func (cmd *XAutoClaimJustIDCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	if len(arr) < 2 {
		cmd.SetErr(fmt.Errorf("got %d, wanted 2", len(arr)))
		return
	}
	start, err := arr[0].ToString()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val, err := arr[1].AsStrSlice()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.val, cmd.start = val, start
}

func newXAutoClaimJustIDCmd(res valkey.ValkeyResult) *XAutoClaimJustIDCmd {
	cmd := &XAutoClaimJustIDCmd{}
	cmd.from(res)
	return cmd

}

func (cmd *XAutoClaimJustIDCmd) SetVal(val []string, start string) {
	cmd.val = val
	cmd.start = start
}

func (cmd *XAutoClaimJustIDCmd) SetErr(err error) {
	cmd.err = err
}

func (cmd *XAutoClaimJustIDCmd) Val() (ids []string, start string) {
	return cmd.val, cmd.start
}

func (cmd *XAutoClaimJustIDCmd) Err() error {
	return cmd.err
}

func (cmd *XAutoClaimJustIDCmd) Result() (ids []string, start string, err error) {
	return cmd.val, cmd.start, cmd.err
}

type XInfoGroup struct {
	Name            string
	LastDeliveredID string
	Consumers       int64
	Pending         int64
	EntriesRead     int64
	Lag             int64
}

type XInfoGroupsCmd struct {
	baseCmd[[]XInfoGroup]
}

func (cmd *XInfoGroupsCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	groupInfos := make([]XInfoGroup, 0, len(arr))
	for _, v := range arr {
		info, err := v.AsMap()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		var group XInfoGroup
		if attr, ok := info["name"]; ok {
			group.Name, _ = attr.ToString()
		}
		if attr, ok := info["consumers"]; ok {
			group.Consumers, _ = attr.AsInt64()
		}
		if attr, ok := info["pending"]; ok {
			group.Pending, _ = attr.AsInt64()
		}
		if attr, ok := info["entries-read"]; ok {
			group.EntriesRead, _ = attr.AsInt64()
		}
		if attr, ok := info["lag"]; ok {
			group.Lag, _ = attr.AsInt64()
		}
		if attr, ok := info["last-delivered-id"]; ok {
			group.LastDeliveredID, _ = attr.ToString()
		}
		groupInfos = append(groupInfos, group)
	}
	cmd.SetVal(groupInfos)
}

func newXInfoGroupsCmd(res valkey.ValkeyResult) *XInfoGroupsCmd {
	cmd := &XInfoGroupsCmd{}
	cmd.from(res)
	return cmd
}

type XInfoStream struct {
	FirstEntry           XMessage
	LastEntry            XMessage
	LastGeneratedID      string
	MaxDeletedEntryID    string
	RecordedFirstEntryID string
	Length               int64
	RadixTreeKeys        int64
	RadixTreeNodes       int64
	Groups               int64
	EntriesAdded         int64
}
type XInfoStreamCmd struct {
	baseCmd[XInfoStream]
}

func (cmd *XInfoStreamCmd) from(res valkey.ValkeyResult) {
	kv, err := res.AsMap()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	var val XInfoStream
	if v, ok := kv["length"]; ok {
		val.Length, _ = v.AsInt64()
	}
	if v, ok := kv["radix-tree-keys"]; ok {
		val.RadixTreeKeys, _ = v.AsInt64()
	}
	if v, ok := kv["radix-tree-nodes"]; ok {
		val.RadixTreeNodes, _ = v.AsInt64()
	}
	if v, ok := kv["groups"]; ok {
		val.Groups, _ = v.AsInt64()
	}
	if v, ok := kv["last-generated-id"]; ok {
		val.LastGeneratedID, _ = v.ToString()
	}
	if v, ok := kv["max-deleted-entry-id"]; ok {
		val.MaxDeletedEntryID, _ = v.ToString()
	}
	if v, ok := kv["recorded-first-entry-id"]; ok {
		val.RecordedFirstEntryID, _ = v.ToString()
	}
	if v, ok := kv["entries-added"]; ok {
		val.EntriesAdded, _ = v.AsInt64()
	}
	if v, ok := kv["first-entry"]; ok {
		if r, err := v.AsXRangeEntry(); err == nil {
			val.FirstEntry = newXMessage(r)
		}
	}
	if v, ok := kv["last-entry"]; ok {
		if r, err := v.AsXRangeEntry(); err == nil {
			val.LastEntry = newXMessage(r)
		}
	}
	cmd.SetVal(val)
}

func newXInfoStreamCmd(res valkey.ValkeyResult) *XInfoStreamCmd {
	cmd := &XInfoStreamCmd{}
	cmd.from(res)
	return cmd
}

type XInfoStreamConsumerPending struct {
	DeliveryTime  time.Time
	ID            string
	DeliveryCount int64
}

type XInfoStreamGroupPending struct {
	DeliveryTime  time.Time
	ID            string
	Consumer      string
	DeliveryCount int64
}

type XInfoStreamConsumer struct {
	SeenTime time.Time
	Name     string
	Pending  []XInfoStreamConsumerPending
	PelCount int64
}

type XInfoStreamGroup struct {
	Name            string
	LastDeliveredID string
	Pending         []XInfoStreamGroupPending
	Consumers       []XInfoStreamConsumer
	EntriesRead     int64
	Lag             int64
	PelCount        int64
}

type XInfoStreamFull struct {
	LastGeneratedID      string
	MaxDeletedEntryID    string
	RecordedFirstEntryID string
	Entries              []XMessage
	Groups               []XInfoStreamGroup
	Length               int64
	RadixTreeKeys        int64
	RadixTreeNodes       int64
	EntriesAdded         int64
}

type XInfoStreamFullCmd struct {
	baseCmd[XInfoStreamFull]
}

func (cmd *XInfoStreamFullCmd) from(res valkey.ValkeyResult) {
	kv, err := res.AsMap()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	var val XInfoStreamFull
	if v, ok := kv["length"]; ok {
		val.Length, _ = v.AsInt64()
	}
	if v, ok := kv["radix-tree-keys"]; ok {
		val.RadixTreeKeys, _ = v.AsInt64()
	}
	if v, ok := kv["radix-tree-nodes"]; ok {
		val.RadixTreeNodes, _ = v.AsInt64()
	}
	if v, ok := kv["last-generated-id"]; ok {
		val.LastGeneratedID, _ = v.ToString()
	}
	if v, ok := kv["entries-added"]; ok {
		val.EntriesAdded, _ = v.AsInt64()
	}
	if v, ok := kv["max-deleted-entry-id"]; ok {
		val.MaxDeletedEntryID, _ = v.ToString()
	}
	if v, ok := kv["recorded-first-entry-id"]; ok {
		val.RecordedFirstEntryID, _ = v.ToString()
	}
	if v, ok := kv["groups"]; ok {
		val.Groups, err = readStreamGroups(v)
		if err != nil {
			cmd.SetErr(err)
			return
		}
	}
	if v, ok := kv["entries"]; ok {
		ranges, err := v.AsXRange()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		val.Entries = make([]XMessage, 0, len(ranges))
		for _, r := range ranges {
			val.Entries = append(val.Entries, newXMessage(r))
		}
	}
	cmd.SetVal(val)
}

func newXInfoStreamFullCmd(res valkey.ValkeyResult) *XInfoStreamFullCmd {
	cmd := &XInfoStreamFullCmd{}
	cmd.from(res)
	return cmd
}

func readStreamGroups(res valkey.ValkeyMessage) ([]XInfoStreamGroup, error) {
	arr, err := res.ToArray()
	if err != nil {
		return nil, err
	}
	groups := make([]XInfoStreamGroup, 0, len(arr))
	for _, v := range arr {
		info, err := v.AsMap()
		if err != nil {
			return nil, err
		}
		var group XInfoStreamGroup
		if attr, ok := info["name"]; ok {
			group.Name, _ = attr.ToString()
		}
		if attr, ok := info["last-delivered-id"]; ok {
			group.LastDeliveredID, _ = attr.ToString()
		}
		if attr, ok := info["entries-read"]; ok {
			group.EntriesRead, _ = attr.AsInt64()
		}
		if attr, ok := info["lag"]; ok {
			group.Lag, _ = attr.AsInt64()
		}
		if attr, ok := info["pel-count"]; ok {
			group.PelCount, _ = attr.AsInt64()
		}
		if attr, ok := info["pending"]; ok {
			group.Pending, err = readXInfoStreamGroupPending(attr)
			if err != nil {
				return nil, err
			}
		}
		if attr, ok := info["consumers"]; ok {
			group.Consumers, err = readXInfoStreamConsumers(attr)
			if err != nil {
				return nil, err
			}
		}
		groups = append(groups, group)
	}
	return groups, nil
}

func readXInfoStreamGroupPending(res valkey.ValkeyMessage) ([]XInfoStreamGroupPending, error) {
	arr, err := res.ToArray()
	if err != nil {
		return nil, err
	}
	pending := make([]XInfoStreamGroupPending, 0, len(arr))
	for _, v := range arr {
		info, err := v.ToArray()
		if err != nil {
			return nil, err
		}
		if len(info) < 4 {
			return nil, fmt.Errorf("got %d, wanted 4", len(arr))
		}
		var p XInfoStreamGroupPending
		p.ID, err = info[0].ToString()
		if err != nil {
			return nil, err
		}
		p.Consumer, err = info[1].ToString()
		if err != nil {
			return nil, err
		}
		delivery, err := info[2].AsInt64()
		if err != nil {
			return nil, err
		}
		p.DeliveryTime = time.Unix(delivery/1000, delivery%1000*int64(time.Millisecond))
		p.DeliveryCount, err = info[3].AsInt64()
		if err != nil {
			return nil, err
		}
		pending = append(pending, p)
	}
	return pending, nil
}

func readXInfoStreamConsumers(res valkey.ValkeyMessage) ([]XInfoStreamConsumer, error) {
	arr, err := res.ToArray()
	if err != nil {
		return nil, err
	}
	consumer := make([]XInfoStreamConsumer, 0, len(arr))
	for _, v := range arr {
		info, err := v.AsMap()
		if err != nil {
			return nil, err
		}
		var c XInfoStreamConsumer
		if attr, ok := info["name"]; ok {
			c.Name, _ = attr.ToString()
		}
		if attr, ok := info["seen-time"]; ok {
			seen, _ := attr.AsInt64()
			c.SeenTime = time.Unix(seen/1000, seen%1000*int64(time.Millisecond))
		}
		if attr, ok := info["pel-count"]; ok {
			c.PelCount, _ = attr.AsInt64()
		}
		if attr, ok := info["pending"]; ok {
			pending, err := attr.ToArray()
			if err != nil {
				return nil, err
			}
			c.Pending = make([]XInfoStreamConsumerPending, 0, len(pending))
			for _, v := range pending {
				pendingInfo, err := v.ToArray()
				if err != nil {
					return nil, err
				}
				if len(pendingInfo) < 3 {
					return nil, fmt.Errorf("got %d, wanted 3", len(arr))
				}
				var p XInfoStreamConsumerPending
				p.ID, err = pendingInfo[0].ToString()
				if err != nil {
					return nil, err
				}
				delivery, err := pendingInfo[1].AsInt64()
				if err != nil {
					return nil, err
				}
				p.DeliveryTime = time.Unix(delivery/1000, delivery%1000*int64(time.Millisecond))
				p.DeliveryCount, err = pendingInfo[2].AsInt64()
				if err != nil {
					return nil, err
				}
				c.Pending = append(c.Pending, p)
			}
		}
		consumer = append(consumer, c)
	}
	return consumer, nil
}

type XInfoConsumer struct {
	Name    string
	Pending int64
	Idle    time.Duration
}
type XInfoConsumersCmd struct {
	baseCmd[[]XInfoConsumer]
}

func (cmd *XInfoConsumersCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make([]XInfoConsumer, 0, len(arr))
	for _, v := range arr {
		info, err := v.AsMap()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		var consumer XInfoConsumer
		if attr, ok := info["name"]; ok {
			consumer.Name, _ = attr.ToString()
		}
		if attr, ok := info["pending"]; ok {
			consumer.Pending, _ = attr.AsInt64()
		}
		if attr, ok := info["idle"]; ok {
			idle, _ := attr.AsInt64()
			consumer.Idle = time.Duration(idle) * time.Millisecond
		}
		val = append(val, consumer)
	}
	cmd.SetVal(val)
}

func newXInfoConsumersCmd(res valkey.ValkeyResult) *XInfoConsumersCmd {
	cmd := &XInfoConsumersCmd{}
	cmd.from(res)
	return cmd
}

// Z represents sorted set member.
type Z struct {
	Member string
	Score  float64
}

// ZWithKey represents a sorted set member including the name of the key where it was popped.
type ZWithKey struct {
	Key string
	Z
}

// ZStore is used as an arg to ZInter/ZInterStore and ZUnion/ZUnionStore.
type ZStore struct {
	Aggregate string
	Keys      []string
	Weights   []int64
}

type ZWithKeyCmd struct {
	baseCmd[ZWithKey]
}

func (cmd *ZWithKeyCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	if len(arr) < 3 {
		cmd.SetErr(fmt.Errorf("got %d, wanted 3", len(arr)))
		return
	}
	val := ZWithKey{}
	val.Key, err = arr[0].ToString()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val.Member, err = arr[1].ToString()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val.Score, err = arr[2].AsFloat64()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.SetVal(val)
}

func newZWithKeyCmd(res valkey.ValkeyResult) *ZWithKeyCmd {
	cmd := &ZWithKeyCmd{}
	cmd.from(res)
	return cmd
}

type RankScore struct {
	Rank  int64
	Score float64
}

type RankWithScoreCmd struct {
	baseCmd[RankScore]
}

func (cmd *RankWithScoreCmd) from(res valkey.ValkeyResult) {
	if cmd.err = res.Error(); cmd.err == nil {
		vs, _ := res.ToArray()
		if len(vs) >= 2 {
			cmd.val.Rank, _ = vs[0].AsInt64()
			cmd.val.Score, _ = vs[1].AsFloat64()
		}
	}
}

func newRankWithScoreCmd(res valkey.ValkeyResult) *RankWithScoreCmd {
	cmd := &RankWithScoreCmd{}
	cmd.from(res)
	return cmd
}

type TimeCmd struct {
	baseCmd[time.Time]
}

func (cmd *TimeCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	if len(arr) < 2 {
		cmd.SetErr(fmt.Errorf("got %d, wanted 2", len(arr)))
		return
	}
	sec, err := arr[0].AsInt64()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	microSec, err := arr[1].AsInt64()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.SetVal(time.Unix(sec, microSec*1000))
}

func newTimeCmd(res valkey.ValkeyResult) *TimeCmd {
	cmd := &TimeCmd{}
	cmd.from(res)
	return cmd
}

type ClusterNode struct {
	ID   string
	Addr string
}

type ClusterSlot struct {
	Nodes []ClusterNode
	Start int64
	End   int64
}

type ClusterSlotsCmd struct {
	baseCmd[[]ClusterSlot]
}

func (cmd *ClusterSlotsCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make([]ClusterSlot, 0, len(arr))
	for _, v := range arr {
		slot, err := v.ToArray()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		if len(slot) < 2 {
			cmd.SetErr(fmt.Errorf("got %d, expected at least 2", len(slot)))
			return
		}
		start, err := slot[0].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		end, err := slot[1].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		nodes := make([]ClusterNode, len(slot)-2)
		for i, j := 2, 0; i < len(slot); i, j = i+1, j+1 {
			node, err := slot[i].ToArray()
			if err != nil {
				cmd.SetErr(err)
				return
			}
			if len(node) < 2 {
				cmd.SetErr(fmt.Errorf("got %d, expected 2 or 3", len(node)))
				return
			}
			ip, err := node[0].ToString()
			if err != nil {
				cmd.SetErr(err)
				return
			}
			port, err := node[1].AsInt64()
			if err != nil {
				cmd.SetErr(err)
				return
			}
			nodes[j].Addr = net.JoinHostPort(ip, strconv.FormatInt(port, 10))
			if len(node) > 2 {
				id, err := node[2].ToString()
				if err != nil {
					cmd.SetErr(err)
					return
				}
				nodes[j].ID = id
			}
		}
		val = append(val, ClusterSlot{
			Start: start,
			End:   end,
			Nodes: nodes,
		})
	}
	cmd.SetVal(val)
}

func newClusterSlotsCmd(res valkey.ValkeyResult) *ClusterSlotsCmd {
	cmd := &ClusterSlotsCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *ClusterShardsCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make([]ClusterShard, 0, len(arr))
	for _, v := range arr {
		dict, err := v.ToMap()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		shard := ClusterShard{}
		{
			slots := dict["slots"]
			arr, _ := slots.ToArray()
			for i := 0; i+1 < len(arr); i += 2 {
				start, _ := arr[i].AsInt64()
				end, _ := arr[i+1].AsInt64()
				shard.Slots = append(shard.Slots, SlotRange{Start: start, End: end})
			}
		}
		{
			nodes, ok := dict["nodes"]
			if !ok {
				cmd.SetErr(errors.New("nodes not found"))
				return
			}
			arr, err := nodes.ToArray()
			if err != nil {
				cmd.SetErr(err)
				return
			}
			shard.Nodes = make([]Node, len(arr))
			for i := 0; i < len(arr); i++ {
				nodeMap, err := arr[i].ToMap()
				if err != nil {
					cmd.SetErr(err)
					return
				}
				for k, v := range nodeMap {
					switch k {
					case "id":
						shard.Nodes[i].ID, _ = v.ToString()
					case "endpoint":
						shard.Nodes[i].Endpoint, _ = v.ToString()
					case "ip":
						shard.Nodes[i].IP, _ = v.ToString()
					case "hostname":
						shard.Nodes[i].Hostname, _ = v.ToString()
					case "port":
						shard.Nodes[i].Port, _ = v.ToInt64()
					case "tls-port":
						shard.Nodes[i].TLSPort, _ = v.ToInt64()
					case "role":
						shard.Nodes[i].Role, _ = v.ToString()
					case "replication-offset":
						shard.Nodes[i].ReplicationOffset, _ = v.ToInt64()
					case "health":
						shard.Nodes[i].Health, _ = v.ToString()
					}
				}
			}
		}
		val = append(val, shard)
	}
	cmd.SetVal(val)
}

func newClusterShardsCmd(res valkey.ValkeyResult) *ClusterShardsCmd {
	cmd := &ClusterShardsCmd{}
	cmd.from(res)
	return cmd
}

type SlotRange struct {
	Start int64
	End   int64
}
type Node struct {
	ID                string
	Endpoint          string
	IP                string
	Hostname          string
	Role              string
	Health            string
	Port              int64
	TLSPort           int64
	ReplicationOffset int64
}
type ClusterShard struct {
	Slots []SlotRange
	Nodes []Node
}

type ClusterShardsCmd struct {
	baseCmd[[]ClusterShard]
}

type GeoPos struct {
	Longitude, Latitude float64
}

type GeoPosCmd struct {
	baseCmd[[]*GeoPos]
}

func (cmd *GeoPosCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make([]*GeoPos, 0, len(arr))
	for _, v := range arr {
		loc, err := v.ToArray()
		if err != nil {
			if valkey.IsValkeyNil(err) {
				val = append(val, nil)
				continue
			}
			cmd.SetErr(err)
			return
		}
		if len(loc) != 2 {
			cmd.SetErr(fmt.Errorf("got %d, expected 2", len(loc)))
			return
		}
		long, err := loc[0].AsFloat64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		lat, err := loc[1].AsFloat64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		val = append(val, &GeoPos{
			Longitude: long,
			Latitude:  lat,
		})
	}
	cmd.SetVal(val)
}

func newGeoPosCmd(res valkey.ValkeyResult) *GeoPosCmd {
	cmd := &GeoPosCmd{}
	cmd.from(res)
	return cmd
}

type GeoLocationCmd struct {
	baseCmd[[]valkey.GeoLocation]
}

func (cmd *GeoLocationCmd) from(res valkey.ValkeyResult) {
	cmd.val, cmd.err = res.AsGeosearch()
}

func newGeoLocationCmd(res valkey.ValkeyResult) *GeoLocationCmd {
	cmd := &GeoLocationCmd{}
	cmd.from(res)
	return cmd
}

type CommandInfo struct {
	Name        string
	Flags       []string
	ACLFlags    []string
	Arity       int64
	FirstKeyPos int64
	LastKeyPos  int64
	StepCount   int64
	ReadOnly    bool
}

type CommandsInfoCmd struct {
	baseCmd[map[string]CommandInfo]
}

func (cmd *CommandsInfoCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make(map[string]CommandInfo, len(arr))
	for _, v := range arr {
		info, err := v.ToArray()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		if len(info) < 6 {
			cmd.SetErr(fmt.Errorf("got %d, wanted at least 6", len(info)))
			return
		}
		var _cmd CommandInfo
		_cmd.Name, err = info[0].ToString()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		_cmd.Arity, err = info[1].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		_cmd.Flags, err = info[2].AsStrSlice()
		if err != nil {
			if valkey.IsValkeyNil(err) {
				_cmd.Flags = []string{}
			} else {
				cmd.SetErr(err)
				return
			}
		}
		_cmd.FirstKeyPos, err = info[3].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		_cmd.LastKeyPos, err = info[4].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		_cmd.StepCount, err = info[5].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		for _, flag := range _cmd.Flags {
			if flag == "readonly" {
				_cmd.ReadOnly = true
				break
			}
		}
		if len(info) == 6 {
			val[_cmd.Name] = _cmd
			continue
		}
		_cmd.ACLFlags, err = info[6].AsStrSlice()
		if err != nil {
			if valkey.IsValkeyNil(err) {
				_cmd.ACLFlags = []string{}
			} else {
				cmd.SetErr(err)
				return
			}
		}
		val[_cmd.Name] = _cmd
	}
	cmd.SetVal(val)
}

func newCommandsInfoCmd(res valkey.ValkeyResult) *CommandsInfoCmd {
	cmd := &CommandsInfoCmd{}
	cmd.from(res)
	return cmd
}

type HExpireArgs struct {
	NX bool
	XX bool
	GT bool
	LT bool
}

// ExpirationType represents an expiration option for the HGETEX command.
type HGetEXExpirationType string

const (
	HGetEXExpirationEX      HGetEXExpirationType = "EX"
	HGetEXExpirationPX      HGetEXExpirationType = "PX"
	HGetEXExpirationEXAT    HGetEXExpirationType = "EXAT"
	HGetEXExpirationPXAT    HGetEXExpirationType = "PXAT"
	HGetEXExpirationPERSIST HGetEXExpirationType = "PERSIST"
)

type HSetEXCondition string

const (
	HSetEXFNX HSetEXCondition = "FNX"
	HSetEXFXX HSetEXCondition = "FXX"
)

type HGetEXOptions struct {
	ExpirationType HGetEXExpirationType
	ExpirationVal  int64
}

type HSetEXExpirationType string

const (
	HSetEXExpirationEX      HSetEXExpirationType = "EX"
	HSetEXExpirationPX      HSetEXExpirationType = "PX"
	HSetEXExpirationEXAT    HSetEXExpirationType = "EXAT"
	HSetEXExpirationPXAT    HSetEXExpirationType = "PXAT"
	HSetEXExpirationKEEPTTL HSetEXExpirationType = "KEEPTTL"
)

type HSetEXOptions struct {
	Condition      HSetEXCondition
	ExpirationType HSetEXExpirationType
	ExpirationVal  int64
}

type Sort struct {
	By     string
	Order  string
	Get    []string
	Offset int64
	Count  int64
	Alpha  bool
}

// SetArgs provides arguments for the SetArgs function.
type SetArgs struct {
	ExpireAt time.Time
	Mode     string
	TTL      time.Duration
	Get      bool
	KeepTTL  bool
}

type BitCount struct {
	Unit       string // Stores BIT or BYTE
	Start, End int64
}

//type BitPos struct {
//	BitCount
//	Byte bool
//}

type BitFieldArg struct {
	Encoding string
	Offset   int64
}

type BitField struct {
	Get       *BitFieldArg
	Set       *BitFieldArg
	IncrBy    *BitFieldArg
	Overflow  string
	Increment int64
}

type LPosArgs struct {
	Rank, MaxLen int64
}

// Note: MaxLen/MaxLenApprox and MinID are in conflict, only one of them can be used.
type XAddArgs struct {
	Values     any
	Stream     string
	MinID      string
	ID         string
	MaxLen     int64
	Limit      int64
	NoMkStream bool
	Approx     bool
}

type XReadArgs struct {
	Streams []string // list of streams
	Count   int64
	Block   time.Duration
}

type XReadGroupArgs struct {
	Group    string
	Consumer string
	Streams  []string // list of streams
	Count    int64
	Block    time.Duration
	NoAck    bool
}

type XPendingExtArgs struct {
	Stream   string
	Group    string
	Start    string
	End      string
	Consumer string
	Idle     time.Duration
	Count    int64
}

type XClaimArgs struct {
	Stream   string
	Group    string
	Consumer string
	Messages []string
	MinIdle  time.Duration
}

type XAutoClaimArgs struct {
	Stream   string
	Group    string
	Start    string
	Consumer string
	MinIdle  time.Duration
	Count    int64
}

type XMessage struct {
	Values map[string]any
	ID     string
}

// Note: The GT, LT and NX options are mutually exclusive.
type ZAddArgs struct {
	Members []Z
	NX      bool
	XX      bool
	LT      bool
	GT      bool
	Ch      bool
}

// ZRangeArgs is all the options of the ZRange command.
// In version> 6.2.0, you can replace the(cmd):
//
//	ZREVRANGE,
//	ZRANGEBYSCORE,
//	ZREVRANGEBYSCORE,
//	ZRANGEBYLEX,
//	ZREVRANGEBYLEX.
//
// Please pay attention to your valkey-server version.
//
// Rev, ByScore, ByLex and Offset+Count options require valkey-server 6.2.0 and higher.
type ZRangeArgs struct {
	Start   any
	Stop    any
	Key     string
	Offset  int64
	Count   int64
	ByScore bool
	ByLex   bool
	Rev     bool
}

type ZRangeBy struct {
	Min, Max      string
	Offset, Count int64
}

type GeoLocation = valkey.GeoLocation

// GeoRadiusQuery is used with GeoRadius to query geospatial index.
type GeoRadiusQuery struct {
	Unit        string
	Sort        string
	Store       string
	StoreDist   string
	Radius      float64
	Count       int64
	WithCoord   bool
	WithDist    bool
	WithGeoHash bool
}

// GeoSearchQuery is used for GEOSearch/GEOSearchStore command query.
type GeoSearchQuery struct {
	Member     string
	RadiusUnit string
	BoxUnit    string
	Sort       string
	Longitude  float64
	Latitude   float64
	Radius     float64
	BoxWidth   float64
	BoxHeight  float64
	Count      int64
	CountAny   bool
}

type GeoSearchLocationQuery struct {
	GeoSearchQuery

	WithCoord bool
	WithDist  bool
	WithHash  bool
}

type GeoSearchStoreQuery struct {
	GeoSearchQuery

	// When using the StoreDist option, the command stores the items in a
	// sorted set populated with their distance from the center of the circle or box,
	// as a floating-point number, in the same unit specified for that shape.
	StoreDist bool
}

func (q *GeoRadiusQuery) args() []string {
	args := make([]string, 0, 2)
	args = append(args, strconv.FormatFloat(q.Radius, 'f', -1, 64))
	if q.Unit != "" {
		args = append(args, q.Unit)
	} else {
		args = append(args, "km")
	}
	if q.WithCoord {
		args = append(args, "WITHCOORD")
	}
	if q.WithDist {
		args = append(args, "WITHDIST")
	}
	if q.WithGeoHash {
		args = append(args, "WITHHASH")
	}
	if q.Count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(q.Count, 10))
	}
	if q.Sort != "" {
		args = append(args, q.Sort)
	}
	if q.Store != "" {
		args = append(args, "STORE")
		args = append(args, q.Store)
	}
	if q.StoreDist != "" {
		args = append(args, "STOREDIST")
		args = append(args, q.StoreDist)
	}
	return args
}

func (q *GeoSearchQuery) args() []string {
	args := make([]string, 0, 2)
	if q.Member != "" {
		args = append(args, "FROMMEMBER", q.Member)
	} else {
		args = append(args, "FROMLONLAT", strconv.FormatFloat(q.Longitude, 'f', -1, 64), strconv.FormatFloat(q.Latitude, 'f', -1, 64))
	}
	if q.Radius > 0 {
		if q.RadiusUnit == "" {
			q.RadiusUnit = "KM"
		}
		args = append(args, "BYRADIUS", strconv.FormatFloat(q.Radius, 'f', -1, 64), q.RadiusUnit)
	} else {
		if q.BoxUnit == "" {
			q.BoxUnit = "KM"
		}
		args = append(args, "BYBOX", strconv.FormatFloat(q.BoxWidth, 'f', -1, 64), strconv.FormatFloat(q.BoxHeight, 'f', -1, 64), q.BoxUnit)
	}
	if q.Sort != "" {
		args = append(args, q.Sort)
	}
	if q.Count > 0 {
		args = append(args, "COUNT", strconv.FormatInt(q.Count, 10))
		if q.CountAny {
			args = append(args, "ANY")
		}
	}
	return args
}

func (q *GeoSearchLocationQuery) args() []string {
	args := q.GeoSearchQuery.args()
	if q.WithCoord {
		args = append(args, "WITHCOORD")
	}
	if q.WithDist {
		args = append(args, "WITHDIST")
	}
	if q.WithHash {
		args = append(args, "WITHHASH")
	}
	return args
}

type Function struct {
	Name        string
	Description string
	Flags       []string
}

type Library struct {
	Name      string
	Engine    string
	Code      string
	Functions []Function
}

type FunctionListQuery struct {
	LibraryNamePattern string
	WithCode           bool
}

type FunctionListCmd struct {
	baseCmd[[]Library]
}

func (cmd *FunctionListCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	val := make([]Library, len(arr))
	for i := 0; i < len(arr); i++ {
		kv, _ := arr[i].AsMap()
		for k, v := range kv {
			switch k {
			case "library_name":
				val[i].Name, _ = v.ToString()
			case "engine":
				val[i].Engine, _ = v.ToString()
			case "library_code":
				val[i].Code, _ = v.ToString()
			case "functions":
				fns, _ := v.ToArray()
				val[i].Functions = make([]Function, len(fns))
				for j := 0; j < len(fns); j++ {
					fkv, _ := fns[j].AsMap()
					for k, v := range fkv {
						switch k {
						case "name":
							val[i].Functions[j].Name, _ = v.ToString()
						case "description":
							val[i].Functions[j].Description, _ = v.ToString()
						case "flags":
							val[i].Functions[j].Flags, _ = v.AsStrSlice()
						}
					}
				}
			}
		}
	}
	cmd.SetVal(val)
}

func newFunctionListCmd(res valkey.ValkeyResult) *FunctionListCmd {
	cmd := &FunctionListCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *FunctionListCmd) First() (*Library, error) {
	if cmd.err != nil {
		return nil, cmd.err
	}
	if len(cmd.val) > 0 {
		return &cmd.val[0], nil
	}
	return nil, valkey.Nil
}

func usePrecise(dur time.Duration) bool {
	return dur < time.Second || dur%time.Second != 0
}

func formatMs(dur time.Duration) int64 {
	if dur > 0 && dur < time.Millisecond {
		// too small, truncate too 1ms
		return 1
	}
	return int64(dur / time.Millisecond)
}

func formatSec(dur time.Duration) int64 {
	if dur > 0 && dur < time.Second {
		// too small, truncate too 1s
		return 1
	}
	return int64(dur / time.Second)
}

// https://github.com/redis/go-redis/blob/f994ff1cd96299a5c8029ae3403af7b17ef06e8a/gears_commands.go#L21C1-L35C2
type TFunctionLoadOptions struct {
	Config  string
	Replace bool
}

type TFunctionListOptions struct {
	Library  string
	Verbose  int
	Withcode bool
}

type TFCallOptions struct {
	Keys      []string
	Arguments []string
}

type MapStringInterfaceSliceCmd struct {
	baseCmd[[]map[string]any]
}

func (cmd *MapStringInterfaceSliceCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.val = make([]map[string]any, 0, len(arr))
	for _, ele := range arr {
		m, err := ele.AsMap()
		eleMap := make(map[string]any, len(m))
		if err != nil {
			cmd.SetErr(err)
			return
		}
		for k, v := range m {
			var val any
			if !v.IsNil() {
				var err error
				val, err = v.ToAny()
				if err != nil {
					cmd.SetErr(err)
					return
				}
			}
			eleMap[k] = val
		}
		cmd.val = append(cmd.val, eleMap)
	}
}

func newMapStringInterfaceSliceCmd(res valkey.ValkeyResult) *MapStringInterfaceSliceCmd {
	cmd := &MapStringInterfaceSliceCmd{}
	cmd.from(res)
	return cmd
}

type BFInsertOptions struct {
	Capacity   int64
	Error      float64
	Expansion  int64
	NonScaling bool
	NoCreate   bool
}

type BFReserveOptions struct {
	Capacity   int64
	Error      float64
	Expansion  int64
	NonScaling bool
}

type CFReserveOptions struct {
	Capacity      int64
	BucketSize    int64
	MaxIterations int64
	Expansion     int64
}

type CFInsertOptions struct {
	Capacity int64
	NoCreate bool
}

type BFInfo struct {
	Capacity      int64 `valkey:"Capacity"`
	Size          int64 `valkey:"Size"`
	Filters       int64 `valkey:"Number of filters"`
	ItemsInserted int64 `valkey:"Number of items inserted"`
	ExpansionRate int64 `valkey:"Expansion rate"`
}

type BFInfoCmd struct {
	baseCmd[BFInfo]
}

func (cmd *BFInfoCmd) from(res valkey.ValkeyResult) {
	info := BFInfo{}
	if err := res.Error(); err != nil {
		cmd.err = err
		return
	}
	m, err := res.AsIntMap()
	if err != nil {
		cmd.err = err
		return
	}
	keys := make([]string, 0, len(m))
	values := make([]any, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		values = append(values, strconv.FormatInt(v, 10))
	}
	if err := Scan(&info, keys, values); err != nil {
		cmd.err = err
		return
	}
	cmd.SetVal(info)
}

func newBFInfoCmd(res valkey.ValkeyResult) *BFInfoCmd {
	cmd := &BFInfoCmd{}
	cmd.from(res)
	return cmd
}

type ScanDump struct {
	Data string
	Iter int64
}

type ScanDumpCmd struct {
	baseCmd[ScanDump]
}

func (cmd *ScanDumpCmd) from(res valkey.ValkeyResult) {
	scanDump := ScanDump{}
	if err := res.Error(); err != nil {
		cmd.err = err
		return
	}
	arr, err := res.ToArray()
	if err != nil {
		cmd.err = err
		return
	}
	if len(arr) != 2 {
		panic(fmt.Sprintf("wrong length of valkey message, got %v, want %v", len(arr), 2))
	}
	iter, err := arr[0].AsInt64()
	if err != nil {
		cmd.err = err
		return
	}
	data, err := arr[1].ToString()
	if err != nil {
		cmd.err = err
		return
	}
	scanDump.Iter = iter
	scanDump.Data = data
	cmd.SetVal(scanDump)
}

func newScanDumpCmd(res valkey.ValkeyResult) *ScanDumpCmd {
	cmd := &ScanDumpCmd{}
	cmd.from(res)
	return cmd
}

type CFInfo struct {
	Size             int64 `valkey:"Size"`
	NumBuckets       int64 `valkey:"Number of buckets"`
	NumFilters       int64 `valkey:"Number of filters"`
	NumItemsInserted int64 `valkey:"Number of items inserted"`
	NumItemsDeleted  int64 `valkey:"Number of items deleted"`
	BucketSize       int64 `valkey:"Bucket size"`
	ExpansionRate    int64 `valkey:"Expansion rate"`
	MaxIteration     int64 `valkey:"Max iterations"`
}

type CFInfoCmd struct {
	baseCmd[CFInfo]
}

func (cmd *CFInfoCmd) from(res valkey.ValkeyResult) {
	info := CFInfo{}
	m, err := res.AsMap()
	if err != nil {
		cmd.err = err
		return
	}
	keys := make([]string, 0, len(m))
	values := make([]any, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		val, err := v.AsInt64()
		if err != nil {
			cmd.err = err
			return
		}
		values = append(values, strconv.FormatInt(val, 10))
	}
	if err := Scan(&info, keys, values); err != nil {
		cmd.err = err
		return
	}
	cmd.SetVal(info)
}

func newCFInfoCmd(res valkey.ValkeyResult) *CFInfoCmd {
	cmd := &CFInfoCmd{}
	cmd.from(res)
	return cmd
}

type CMSInfo struct {
	Width int64 `valkey:"width"`
	Depth int64 `valkey:"depth"`
	Count int64 `valkey:"count"`
}

type CMSInfoCmd struct {
	baseCmd[CMSInfo]
}

func (cmd *CMSInfoCmd) from(res valkey.ValkeyResult) {
	info := CMSInfo{}
	m, err := res.AsIntMap()
	if err != nil {
		cmd.err = err
		return
	}
	keys := make([]string, 0, len(m))
	values := make([]any, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		values = append(values, strconv.FormatInt(v, 10))
	}
	if err := Scan(&info, keys, values); err != nil {
		cmd.err = err
		return
	}
	cmd.SetVal(info)
}

func newCMSInfoCmd(res valkey.ValkeyResult) *CMSInfoCmd {
	cmd := &CMSInfoCmd{}
	cmd.from(res)
	return cmd
}

type TopKInfo struct {
	K     int64   `valkey:"k"`
	Width int64   `valkey:"width"`
	Depth int64   `valkey:"depth"`
	Decay float64 `valkey:"decay"`
}

type TopKInfoCmd struct {
	baseCmd[TopKInfo]
}

func (cmd *TopKInfoCmd) from(res valkey.ValkeyResult) {
	info := TopKInfo{}
	m, err := res.ToMap()
	if err != nil {
		cmd.err = err
		return
	}
	keys := make([]string, 0, len(m))
	values := make([]any, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		switch k {
		case "k", "width", "depth":
			intVal, err := v.AsInt64()
			if err != nil {
				cmd.err = err
				return
			}
			values = append(values, strconv.FormatInt(intVal, 10))
		case "decay":
			decay, err := v.AsFloat64()
			if err != nil {
				cmd.err = err
				return
			}
			// args of strconv.FormatFloat is copied from cmds.TopkReserveParamsDepth.Decay
			values = append(values, strconv.FormatFloat(decay, 'f', -1, 64))
		default:
			panic("unexpected key")
		}
	}
	if err := Scan(&info, keys, values); err != nil {
		cmd.err = err
		return
	}
	cmd.SetVal(info)
}

func newTopKInfoCmd(res valkey.ValkeyResult) *TopKInfoCmd {
	cmd := &TopKInfoCmd{}
	cmd.from(res)
	return cmd
}

type MapStringIntCmd struct {
	baseCmd[map[string]int64]
}

func (cmd *MapStringIntCmd) from(res valkey.ValkeyResult) {
	m, err := res.AsIntMap()
	if err != nil {
		cmd.err = err
		return
	}
	cmd.SetVal(m)
}

func newMapStringIntCmd(res valkey.ValkeyResult) *MapStringIntCmd {
	cmd := &MapStringIntCmd{}
	cmd.from(res)
	return cmd
}

// Ref: https://redis.io/commands/tdigest.info/
type TDigestInfo struct {
	Compression       int64 `valkey:"Compression"`
	Capacity          int64 `valkey:"Capacity"`
	MergedNodes       int64 `valkey:"Merged nodes"`
	UnmergedNodes     int64 `valkey:"UnmergedNodes"`
	MergedWeight      int64 `valkey:"MergedWeight"`
	UnmergedWeight    int64 `valkey:"Unmerged weight"`
	Observations      int64 `valkey:"Observations"`
	TotalCompressions int64 `valkey:"Total compressions"`
	MemoryUsage       int64 `valkey:"Memory usage"`
}

type TDigestInfoCmd struct {
	baseCmd[TDigestInfo]
}

func (cmd *TDigestInfoCmd) from(res valkey.ValkeyResult) {
	info := TDigestInfo{}
	m, err := res.AsIntMap()
	if err != nil {
		cmd.err = err
		return
	}
	keys := make([]string, 0, len(m))
	values := make([]any, 0, len(m))
	for k, v := range m {
		keys = append(keys, k)
		values = append(values, strconv.FormatInt(v, 10))
	}
	if err := Scan(&info, keys, values); err != nil {
		cmd.err = err
		return
	}
	cmd.SetVal(info)
}

func newTDigestInfoCmd(res valkey.ValkeyResult) *TDigestInfoCmd {
	cmd := &TDigestInfoCmd{}
	cmd.from(res)
	return cmd
}

type TDigestMergeOptions struct {
	Compression int64
	Override    bool
}

type TSOptions struct {
	Labels          map[string]string
	Encoding        string
	DuplicatePolicy string
	Retention       int
	ChunkSize       int
}
type TSIncrDecrOptions struct {
	Labels       map[string]string
	Timestamp    int64
	Retention    int
	ChunkSize    int
	Uncompressed bool
}

type TSAlterOptions struct {
	Labels          map[string]string
	DuplicatePolicy string
	Retention       int
	ChunkSize       int
}

type TSCreateRuleOptions struct {
	AlignTimestamp int64
}

type TSGetOptions struct {
	Latest bool
}

type TSInfoOptions struct {
	Debug bool
}
type Aggregator int

const (
	Invalid = Aggregator(iota)
	Avg
	Sum
	Min
	Max
	Range
	Count
	First
	Last
	StdP
	StdS
	VarP
	VarS
	Twa
)

func (a Aggregator) String() string {
	switch a {
	case Invalid:
		return ""
	case Avg:
		return "AVG"
	case Sum:
		return "SUM"
	case Min:
		return "MIN"
	case Max:
		return "MAX"
	case Range:
		return "RANGE"
	case Count:
		return "COUNT"
	case First:
		return "FIRST"
	case Last:
		return "LAST"
	case StdP:
		return "STD.P"
	case StdS:
		return "STD.S"
	case VarP:
		return "VAR.P"
	case VarS:
		return "VAR.S"
	case Twa:
		return "TWA"
	default:
		return ""
	}
}

type TSTimestampValue struct {
	Timestamp int64
	Value     float64
}
type TSTimestampValueCmd struct {
	baseCmd[TSTimestampValue]
}

func (cmd *TSTimestampValueCmd) from(res valkey.ValkeyResult) {
	val := TSTimestampValue{}
	if err := res.Error(); err != nil {
		cmd.err = err
		return
	}
	arr, err := res.ToArray()
	if err != nil {
		cmd.err = err
		return
	}
	if len(arr) != 2 {
		panic(fmt.Sprintf("wrong len of array reply, should be 2, got %v", len(arr)))
	}
	val.Timestamp, err = arr[0].AsInt64()
	if err != nil {
		cmd.err = err
		return
	}
	val.Value, err = arr[1].AsFloat64()
	if err != nil {
		cmd.err = err
		return
	}
	cmd.SetVal(val)
}

func newTSTimestampValueCmd(res valkey.ValkeyResult) *TSTimestampValueCmd {
	cmd := &TSTimestampValueCmd{}
	cmd.from(res)
	return cmd
}

type MapStringInterfaceCmd struct {
	baseCmd[map[string]any]
}

func (cmd *MapStringInterfaceCmd) from(res valkey.ValkeyResult) {
	m, err := res.AsMap()
	if err != nil {
		cmd.err = err
		return
	}
	strIntMap := make(map[string]any, len(m))
	for k, ele := range m {
		var v any
		var err error
		if !ele.IsNil() {
			v, err = ele.ToAny()
			if err != nil {
				cmd.err = err
				return
			}
		}
		strIntMap[k] = v
	}
	cmd.SetVal(strIntMap)
}

func newMapStringInterfaceCmd(res valkey.ValkeyResult) *MapStringInterfaceCmd {
	cmd := &MapStringInterfaceCmd{}
	cmd.from(res)
	return cmd
}

type TSTimestampValueSliceCmd struct {
	baseCmd[[]TSTimestampValue]
}

func (cmd *TSTimestampValueSliceCmd) from(res valkey.ValkeyResult) {
	msgSlice, err := res.ToArray()
	if err != nil {
		cmd.err = err
		return
	}
	tsValSlice := make([]TSTimestampValue, 0, len(msgSlice))
	for i := 0; i < len(msgSlice); i++ {
		msgArray, err := msgSlice[i].ToArray()
		if err != nil {
			cmd.err = err
			return
		}
		tstmp, err := msgArray[0].AsInt64()
		if err != nil {
			cmd.err = err
			return
		}
		val, err := msgArray[1].AsFloat64()
		if err != nil {
			cmd.err = err
			return
		}
		tsValSlice = append(tsValSlice, TSTimestampValue{Timestamp: tstmp, Value: val})
	}
	cmd.SetVal(tsValSlice)
}

func newTSTimestampValueSliceCmd(res valkey.ValkeyResult) *TSTimestampValueSliceCmd {
	cmd := &TSTimestampValueSliceCmd{}
	cmd.from(res)
	return cmd
}

type MapStringSliceInterfaceCmd struct {
	baseCmd[map[string][]any]
}

func (cmd *MapStringSliceInterfaceCmd) from(res valkey.ValkeyResult) {
	m, err := res.ToMap()
	if err != nil {
		cmd.err = err
		return
	}
	mapStrSliceInt := make(map[string][]any, len(m))
	for k, entry := range m {
		vals, err := entry.ToArray()
		if err != nil {
			cmd.err = err
			return
		}
		anySlice := make([]any, 0, len(vals))
		for _, v := range vals {
			var err error
			ele, err := v.ToAny()
			if err != nil {
				cmd.err = err
				return
			}
			anySlice = append(anySlice, ele)
		}
		mapStrSliceInt[k] = anySlice
	}
	cmd.SetVal(mapStrSliceInt)
}

func newMapStringSliceInterfaceCmd(res valkey.ValkeyResult) *MapStringSliceInterfaceCmd {
	cmd := &MapStringSliceInterfaceCmd{}
	cmd.from(res)
	return cmd
}

type TSRangeOptions struct {
	Align           interface{}
	BucketTimestamp interface{}
	FilterByTS      []int
	FilterByValue   []int
	Count           int
	Aggregator      Aggregator
	BucketDuration  int
	Latest          bool
	Empty           bool
}

type TSRevRangeOptions struct {
	Align           interface{}
	BucketTimestamp interface{}
	FilterByTS      []int
	FilterByValue   []int
	Count           int
	Aggregator      Aggregator
	BucketDuration  int
	Latest          bool
	Empty           bool
}

type TSMRangeOptions struct {
	Align           interface{}
	BucketTimestamp interface{}
	GroupByLabel    interface{}
	Reducer         interface{}
	FilterByTS      []int
	FilterByValue   []int
	SelectedLabels  []interface{}
	Count           int
	Aggregator      Aggregator
	BucketDuration  int
	Latest          bool
	WithLabels      bool
	Empty           bool
}

type TSMRevRangeOptions struct {
	Align           interface{}
	BucketTimestamp interface{}
	GroupByLabel    interface{}
	Reducer         interface{}
	FilterByTS      []int
	FilterByValue   []int
	SelectedLabels  []interface{}
	Count           int
	Aggregator      Aggregator
	BucketDuration  int
	Latest          bool
	WithLabels      bool
	Empty           bool
}

type TSMGetOptions struct {
	SelectedLabels []interface{}
	Latest         bool
	WithLabels     bool
}

type JSONSetArgs struct {
	Value interface{}
	Key   string
	Path  string
}

type JSONArrIndexArgs struct {
	Stop  *int
	Start int
}

type JSONArrTrimArgs struct {
	Stop  *int
	Start int
}

type JSONCmd struct {
	baseCmd[string]
	expanded []any // expanded will be used at JSONCmd.Expanded
	typ      jsonCmdTyp
}

type jsonCmdTyp int

const (
	TYP_STRING jsonCmdTyp = iota
	TYP_ARRAY
)

// https://github.com/redis/go-redis/blob/v9.3.0/json.go#L86
func (cmd *JSONCmd) Val() string {
	return cmd.val
}

// https://github.com/redis/go-redis/blob/v9.3.0/json.go#L105
func (cmd *JSONCmd) Expanded() (any, error) {
	if cmd.typ == TYP_STRING {
		return cmd.Val(), nil
	}
	// TYP_ARRAY
	return cmd.expanded, nil
}

func (cmd *JSONCmd) Result() (string, error) {
	return cmd.Val(), nil
}

func (cmd *JSONCmd) from(res valkey.ValkeyResult) {
	msg, err := res.ToMessage()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	switch {
	// JSON.GET
	case msg.IsString():
		cmd.typ = TYP_STRING
		str, err := res.ToString()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		cmd.SetVal(str)
	// JSON.NUMINCRBY
	case msg.IsArray():
		// we set the marshaled string to cmd.val
		// which will be used at cmd.Val()
		// and also stored parsed result to cmd.expanded,
		// which will be used at cmd.Expanded()
		cmd.typ = TYP_ARRAY
		arr, err := res.ToArray()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		expanded := make([]any, len(arr))
		for i, e := range arr {
			anyE, err := e.ToAny()
			if err != nil {
				if err == valkey.Nil {
					continue
				}
				cmd.SetErr(err)
				return
			}
			expanded[i] = anyE
		}
		cmd.expanded = expanded
		val, err := json.Marshal(cmd.expanded)
		if err != nil {
			cmd.SetErr(err)
			return
		}
		cmd.SetVal(string(val))
	default:
		panic("invalid type, expect array or string")
	}
}

func newJSONCmd(res valkey.ValkeyResult) *JSONCmd {
	cmd := &JSONCmd{}
	cmd.from(res)
	return cmd
}

type JSONGetArgs struct {
	Indent  string
	Newline string
	Space   string
}

type IntPointerSliceCmd struct {
	baseCmd[[]*int64]
}

func (cmd *IntPointerSliceCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	intPtrSlice := make([]*int64, len(arr))
	for i, e := range arr {
		if e.IsNil() {
			continue
		}
		length, err := e.ToInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		intPtrSlice[i] = &length
	}
	cmd.SetVal(intPtrSlice)
}

// newIntPointerSliceCmd initialises an IntPointerSliceCmd
func newIntPointerSliceCmd(res valkey.ValkeyResult) *IntPointerSliceCmd {
	cmd := &IntPointerSliceCmd{}
	cmd.from(res)
	return cmd
}

type JSONSliceCmd struct {
	baseCmd[[]any]
}

func (cmd *JSONSliceCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	anySlice := make([]any, len(arr))
	for i, e := range arr {
		if e.IsNil() {
			continue
		}
		anyE, err := e.ToAny()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		anySlice[i] = anyE
	}
	cmd.SetVal(anySlice)
}

func newJSONSliceCmd(res valkey.ValkeyResult) *JSONSliceCmd {
	cmd := &JSONSliceCmd{}
	cmd.from(res)
	return cmd
}

type MapMapStringInterfaceCmd struct {
	baseCmd[map[string]any]
}

func (cmd *MapMapStringInterfaceCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	data := make(map[string]any, len(arr)/2)
	for i := 0; i < len(arr); i++ {
		arr1, err := arr[i].ToArray()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		for _i := 0; _i < len(arr1); _i += 2 {
			key, err := arr1[_i].ToString()
			if err != nil {
				cmd.SetErr(err)
				return
			}
			if !arr1[_i+1].IsNil() {
				value, err := arr1[_i+1].ToAny()
				if err != nil {
					cmd.SetErr(err)
					return
				}
				data[key] = value
			} else {
				data[key] = nil
			}
		}
	}
	cmd.SetVal(data)
}

func newMapMapStringInterfaceCmd(res valkey.ValkeyResult) *MapMapStringInterfaceCmd {
	cmd := &MapMapStringInterfaceCmd{}
	cmd.from(res)
	return cmd
}

type FTAggregateResult struct {
	Rows  []AggregateRow
	Total int
}

type AggregateRow struct {
	Fields map[string]any
}

// Each AggregateReducer have different args.
// Please follow https://redis.io/docs/interact/search-and-query/search/aggregations/#supported-groupby-reducers for more information.
type FTAggregateReducer struct {
	As      string
	Args    []interface{}
	Reducer SearchAggregator
}

type FTAggregateGroupBy struct {
	Fields []interface{}
	Reduce []FTAggregateReducer
}

type FTAggregateSortBy struct {
	FieldName string
	Asc       bool
	Desc      bool
}

type FTAggregateApply struct {
	Field string
	As    string
}

type FTAggregateLoad struct {
	Field string
	As    string
}

type FTAggregateWithCursor struct {
	Count   int
	MaxIdle int
}

type FTAggregateOptions struct {
	WithCursorOptions *FTAggregateWithCursor
	Params            map[string]interface{}
	Filter            string
	Scorer            string
	Load              []FTAggregateLoad
	GroupBy           []FTAggregateGroupBy
	SortBy            []FTAggregateSortBy
	Apply             []FTAggregateApply
	Timeout           int
	SortByMax         int
	LimitOffset       int
	Limit             int
	DialectVersion    int
	Verbatim          bool
	LoadAll           bool
	WithCursor        bool
	AddScores         bool
}

type AggregateCmd struct {
	baseCmd[*FTAggregateResult]
}

func (cmd *AggregateCmd) from(res valkey.ValkeyResult) {
	if err := res.Error(); err != nil {
		cmd.SetErr(err)
		return
	}
	anyRes, err := res.ToAny()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.SetRawVal(anyRes)
	msg, err := res.ToMessage()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	if !(msg.IsMap() || msg.IsArray()) {
		panic("res should be either map(RESP3) or array(RESP2)")
	}
	if msg.IsMap() {
		total, docs, err := msg.AsFtAggregate()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		aggResult := &FTAggregateResult{Total: int(total)}
		for _, doc := range docs {
			anyMap := make(map[string]any, len(doc))
			for k, v := range doc {
				anyMap[k] = v
			}
			aggResult.Rows = append(aggResult.Rows, AggregateRow{anyMap})
		}
		cmd.SetVal(aggResult)
		return
	}
	// is RESP2 array
	rows, err := msg.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	anyArr := make([]any, 0, len(rows))
	for _, e := range rows {
		anyE, err := e.ToAny()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		anyArr = append(anyArr, anyE)
	}
	result, err := processAggregateResult(anyArr)
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.SetVal(result)
}

// Ref: https://github.com/redis/go-redis/blob/v9.7.0/search_commands.go#L584
func processAggregateResult(data []interface{}) (*FTAggregateResult, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("no data returned")
	}

	total, ok := data[0].(int64)
	if !ok {
		return nil, fmt.Errorf("invalid total format")
	}

	rows := make([]AggregateRow, 0, len(data)-1)
	for _, row := range data[1:] {
		fields, ok := row.([]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid row format")
		}

		rowMap := make(map[string]interface{})
		for i := 0; i < len(fields); i += 2 {
			key, ok := fields[i].(string)
			if !ok {
				return nil, fmt.Errorf("invalid field key format")
			}
			value := fields[i+1]
			rowMap[key] = value
		}
		rows = append(rows, AggregateRow{Fields: rowMap})
	}

	result := &FTAggregateResult{
		Total: int(total),
		Rows:  rows,
	}
	return result, nil
}

func newAggregateCmd(res valkey.ValkeyResult) *AggregateCmd {
	cmd := &AggregateCmd{}
	cmd.from(res)
	return cmd
}

type FTCreateOptions struct {
	Filter          string
	DefaultLanguage string
	LanguageField   string
	ScoreField      string
	PayloadField    string
	Prefix          []any
	StopWords       []any
	Score           float64
	MaxTextFields   int
	Temporary       int
	OnHash          bool
	OnJSON          bool
	NoOffsets       bool
	NoHL            bool
	NoFields        bool
	NoFreqs         bool
	SkipInitialScan bool
}

type SearchAggregator int

const (
	SearchInvalid = SearchAggregator(iota)
	SearchAvg
	SearchSum
	SearchMin
	SearchMax
	SearchCount
	SearchCountDistinct
	SearchCountDistinctish
	SearchStdDev
	SearchQuantile
	SearchToList
	SearchFirstValue
	SearchRandomSample
)

func (a SearchAggregator) String() string {
	switch a {
	case SearchInvalid:
		return ""
	case SearchAvg:
		return "AVG"
	case SearchSum:
		return "SUM"
	case SearchMin:
		return "MIN"
	case SearchMax:
		return "MAX"
	case SearchCount:
		return "COUNT"
	case SearchCountDistinct:
		return "COUNT_DISTINCT"
	case SearchCountDistinctish:
		return "COUNT_DISTINCTISH"
	case SearchStdDev:
		return "STDDEV"
	case SearchQuantile:
		return "QUANTILE"
	case SearchToList:
		return "TOLIST"
	case SearchFirstValue:
		return "FIRST_VALUE"
	case SearchRandomSample:
		return "RANDOM_SAMPLE"
	default:
		return ""
	}
}

type SearchFieldType int

const (
	SearchFieldTypeInvalid = SearchFieldType(iota)
	SearchFieldTypeNumeric
	SearchFieldTypeTag
	SearchFieldTypeText
	SearchFieldTypeGeo
	SearchFieldTypeVector
	SearchFieldTypeGeoShape
)

func (t SearchFieldType) String() string {
	switch t {
	case SearchFieldTypeInvalid:
		return ""
	case SearchFieldTypeNumeric:
		return "NUMERIC"
	case SearchFieldTypeTag:
		return "TAG"
	case SearchFieldTypeText:
		return "TEXT"
	case SearchFieldTypeGeo:
		return "GEO"
	case SearchFieldTypeVector:
		return "VECTOR"
	case SearchFieldTypeGeoShape:
		return "GEOSHAPE"
	default:
		return "TEXT"
	}
}

type FieldSchema struct {
	VectorArgs        *FTVectorArgs
	FieldName         string
	As                string
	PhoneticMatcher   string
	Separator         string
	GeoShapeFieldType string
	FieldType         SearchFieldType
	Weight            float64
	Sortable          bool
	UNF               bool
	NoStem            bool
	NoIndex           bool
	CaseSensitive     bool
	WithSuffixtrie    bool
	IndexEmpty        bool
	IndexMissing      bool
}

type FTVectorArgs struct {
	FlatOptions *FTFlatOptions
	HNSWOptions *FTHNSWOptions
}

type FTFlatOptions struct {
	Type            string
	DistanceMetric  string
	Dim             int
	InitialCapacity int
	BlockSize       int
}

type FTHNSWOptions struct {
	Type                   string
	DistanceMetric         string
	Dim                    int
	InitialCapacity        int
	MaxEdgesPerNode        int
	MaxAllowedEdgesPerNode int
	EFRunTime              int
	Epsilon                float64
}

type SpellCheckTerms struct {
	Dictionary string
	Include    bool
	Exclude    bool
}

type FTSearchFilter struct {
	FieldName any
	Min       any
	Max       any
}

type FTSearchGeoFilter struct {
	FieldName string
	Unit      string
	Longitude float64
	Latitude  float64
	Radius    float64
}

type FTSearchReturn struct {
	FieldName string
	As        string
}

type FTSearchSortBy struct {
	FieldName string
	Asc       bool
	Desc      bool
}

type FTDropIndexOptions struct {
	DeleteDocs bool
}

type FTExplainOptions struct {
	Dialect string
}

type IndexErrors struct {
	LastIndexingError    string
	LastIndexingErrorKey string
	IndexingFailures     int `redis:"indexing failures"`
}

type FTAttribute struct {
	Identifier      string
	Attribute       string
	Type            string
	PhoneticMatcher string
	Weight          float64
	Sortable        bool
	NoStem          bool
	NoIndex         bool
	UNF             bool
	CaseSensitive   bool
	WithSuffixtrie  bool
}

type CursorStats struct {
	GlobalIdle    int
	GlobalTotal   int
	IndexCapacity int
	IndexTotal    int
}

type FieldStatistic struct {
	Identifier  string
	Attribute   string
	IndexErrors IndexErrors
}

type GCStats struct {
	AverageCycleTimeMs   string `redis:"average_cycle_time_ms"`
	BytesCollected       int    `redis:"bytes_collected"`
	TotalMsRun           int    `redis:"total_ms_run"`
	TotalCycles          int    `redis:"total_cycles"`
	LastRunTimeMs        int    `redis:"last_run_time_ms"`
	GCNumericTreesMissed int    `redis:"gc_numeric_trees_missed"`
	GCBlocksDenied       int    `redis:"gc_blocks_denied"`
}

type IndexDefinition struct {
	KeyType      string
	Prefixes     []string
	DefaultScore float64
}

type FTInfoResult struct {
	DialectStats             map[string]int   `redis:"dialect_stats"`
	IndexErrors              IndexErrors      `redis:"Index Errors"`
	BytesPerRecordAvg        string           `redis:"bytes_per_record_avg"`
	IndexName                string           `redis:"index_name"`
	OffsetBitsPerRecordAvg   string           `redis:"offset_bits_per_record_avg"`
	OffsetsPerTermAvg        string           `redis:"offsets_per_term_avg"`
	RecordsPerDocAvg         string           `redis:"records_per_doc_avg"`
	Attributes               []FTAttribute    `redis:"attributes"`
	FieldStatistics          []FieldStatistic `redis:"field statistics"`
	IndexOptions             []string         `redis:"index_options"`
	IndexDefinition          IndexDefinition  `redis:"index_definition"`
	GCStats                  GCStats          `redis:"gc_stats"`
	CursorStats              CursorStats      `redis:"cursor_stats"`
	Cleaning                 int              `redis:"cleaning"`
	DocTableSizeMB           float64          `redis:"doc_table_size_mb"`
	GeoshapesSzMB            float64          `redis:"geoshapes_sz_mb"`
	HashIndexingFailures     int              `redis:"hash_indexing_failures"`
	Indexing                 int              `redis:"indexing"`
	InvertedSzMB             float64          `redis:"inverted_sz_mb"`
	KeyTableSizeMB           float64          `redis:"key_table_size_mb"`
	MaxDocID                 int              `redis:"max_doc_id"`
	NumDocs                  int              `redis:"num_docs"`
	NumRecords               int              `redis:"num_records"`
	NumTerms                 int              `redis:"num_terms"`
	NumberOfUses             int              `redis:"number_of_uses"`
	OffsetVectorsSzMB        float64          `redis:"offset_vectors_sz_mb"`
	PercentIndexed           float64          `redis:"percent_indexed"`
	SortableValuesSizeMB     float64          `redis:"sortable_values_size_mb"`
	TagOverheadSzMB          float64          `redis:"tag_overhead_sz_mb"`
	TextOverheadSzMB         float64          `redis:"text_overhead_sz_mb"`
	TotalIndexMemorySzMB     float64          `redis:"total_index_memory_sz_mb"`
	TotalIndexingTime        int              `redis:"total_indexing_time"`
	TotalInvertedIndexBlocks int              `redis:"total_inverted_index_blocks"`
	VectorIndexSzMB          float64          `redis:"vector_index_sz_mb"`
}

type FTInfoCmd struct {
	baseCmd[FTInfoResult]
}

// Ref: https://github.com/redis/go-redis/blob/v9.7.0/search_commands.go#L1143
func parseFTInfo(data map[string]interface{}) (FTInfoResult, error) {
	var ftInfo FTInfoResult
	// Manually parse each field from the map
	if indexErrors, ok := data["Index Errors"].([]interface{}); ok {
		ftInfo.IndexErrors = IndexErrors{
			IndexingFailures:     ToInteger(indexErrors[1]),
			LastIndexingError:    ToString(indexErrors[3]),
			LastIndexingErrorKey: ToString(indexErrors[5]),
		}
	}

	if attributes, ok := data["attributes"].([]interface{}); ok {
		for _, attr := range attributes {
			if attrMap, ok := attr.([]interface{}); ok {
				att := FTAttribute{}
				for i := 0; i < len(attrMap); i++ {
					if ToLower(ToString(attrMap[i])) == "attribute" {
						att.Attribute = ToString(attrMap[i+1])
						continue
					}
					if ToLower(ToString(attrMap[i])) == "identifier" {
						att.Identifier = ToString(attrMap[i+1])
						continue
					}
					if ToLower(ToString(attrMap[i])) == "type" {
						att.Type = ToString(attrMap[i+1])
						continue
					}
					if ToLower(ToString(attrMap[i])) == "weight" {
						att.Weight = ToFloat(attrMap[i+1])
						continue
					}
					if ToLower(ToString(attrMap[i])) == "nostem" {
						att.NoStem = true
						continue
					}
					if ToLower(ToString(attrMap[i])) == "sortable" {
						att.Sortable = true
						continue
					}
					if ToLower(ToString(attrMap[i])) == "noindex" {
						att.NoIndex = true
						continue
					}
					if ToLower(ToString(attrMap[i])) == "unf" {
						att.UNF = true
						continue
					}
					if ToLower(ToString(attrMap[i])) == "phonetic" {
						att.PhoneticMatcher = ToString(attrMap[i+1])
						continue
					}
					if ToLower(ToString(attrMap[i])) == "case_sensitive" {
						att.CaseSensitive = true
						continue
					}
					if ToLower(ToString(attrMap[i])) == "withsuffixtrie" {
						att.WithSuffixtrie = true
						continue
					}

				}
				ftInfo.Attributes = append(ftInfo.Attributes, att)
			}
		}
	}

	ftInfo.BytesPerRecordAvg = ToString(data["bytes_per_record_avg"])
	ftInfo.Cleaning = ToInteger(data["cleaning"])

	if cursorStats, ok := data["cursor_stats"].([]interface{}); ok {
		ftInfo.CursorStats = CursorStats{
			GlobalIdle:    ToInteger(cursorStats[1]),
			GlobalTotal:   ToInteger(cursorStats[3]),
			IndexCapacity: ToInteger(cursorStats[5]),
			IndexTotal:    ToInteger(cursorStats[7]),
		}
	}

	if dialectStats, ok := data["dialect_stats"].([]interface{}); ok {
		ftInfo.DialectStats = make(map[string]int)
		for i := 0; i < len(dialectStats); i += 2 {
			ftInfo.DialectStats[ToString(dialectStats[i])] = ToInteger(dialectStats[i+1])
		}
	}

	ftInfo.DocTableSizeMB = ToFloat(data["doc_table_size_mb"])

	if fieldStats, ok := data["field statistics"].([]interface{}); ok {
		for _, stat := range fieldStats {
			if statMap, ok := stat.([]interface{}); ok {
				ftInfo.FieldStatistics = append(ftInfo.FieldStatistics, FieldStatistic{
					Identifier: ToString(statMap[1]),
					Attribute:  ToString(statMap[3]),
					IndexErrors: IndexErrors{
						IndexingFailures:     ToInteger(statMap[5].([]interface{})[1]),
						LastIndexingError:    ToString(statMap[5].([]interface{})[3]),
						LastIndexingErrorKey: ToString(statMap[5].([]interface{})[5]),
					},
				})
			}
		}
	}

	if gcStats, ok := data["gc_stats"].([]interface{}); ok {
		ftInfo.GCStats = GCStats{}
		for i := 0; i < len(gcStats); i += 2 {
			if ToLower(ToString(gcStats[i])) == "bytes_collected" {
				ftInfo.GCStats.BytesCollected = ToInteger(gcStats[i+1])
				continue
			}
			if ToLower(ToString(gcStats[i])) == "total_ms_run" {
				ftInfo.GCStats.TotalMsRun = ToInteger(gcStats[i+1])
				continue
			}
			if ToLower(ToString(gcStats[i])) == "total_cycles" {
				ftInfo.GCStats.TotalCycles = ToInteger(gcStats[i+1])
				continue
			}
			if ToLower(ToString(gcStats[i])) == "average_cycle_time_ms" {
				ftInfo.GCStats.AverageCycleTimeMs = ToString(gcStats[i+1])
				continue
			}
			if ToLower(ToString(gcStats[i])) == "last_run_time_ms" {
				ftInfo.GCStats.LastRunTimeMs = ToInteger(gcStats[i+1])
				continue
			}
			if ToLower(ToString(gcStats[i])) == "gc_numeric_trees_missed" {
				ftInfo.GCStats.GCNumericTreesMissed = ToInteger(gcStats[i+1])
				continue
			}
			if ToLower(ToString(gcStats[i])) == "gc_blocks_denied" {
				ftInfo.GCStats.GCBlocksDenied = ToInteger(gcStats[i+1])
				continue
			}
		}
	}

	ftInfo.GeoshapesSzMB = ToFloat(data["geoshapes_sz_mb"])
	ftInfo.HashIndexingFailures = ToInteger(data["hash_indexing_failures"])

	if indexDef, ok := data["index_definition"].([]interface{}); ok {
		ftInfo.IndexDefinition = IndexDefinition{
			KeyType:      ToString(indexDef[1]),
			Prefixes:     ToStringSlice(indexDef[3]),
			DefaultScore: ToFloat(indexDef[5]),
		}
	}

	ftInfo.IndexName = ToString(data["index_name"])
	ftInfo.IndexOptions = ToStringSlice(data["index_options"].([]interface{}))
	ftInfo.Indexing = ToInteger(data["indexing"])
	ftInfo.InvertedSzMB = ToFloat(data["inverted_sz_mb"])
	ftInfo.KeyTableSizeMB = ToFloat(data["key_table_size_mb"])
	ftInfo.MaxDocID = ToInteger(data["max_doc_id"])
	ftInfo.NumDocs = ToInteger(data["num_docs"])
	ftInfo.NumRecords = ToInteger(data["num_records"])
	ftInfo.NumTerms = ToInteger(data["num_terms"])
	ftInfo.NumberOfUses = ToInteger(data["number_of_uses"])
	ftInfo.OffsetBitsPerRecordAvg = ToString(data["offset_bits_per_record_avg"])
	ftInfo.OffsetVectorsSzMB = ToFloat(data["offset_vectors_sz_mb"])
	ftInfo.OffsetsPerTermAvg = ToString(data["offsets_per_term_avg"])
	ftInfo.PercentIndexed = ToFloat(data["percent_indexed"])
	ftInfo.RecordsPerDocAvg = ToString(data["records_per_doc_avg"])
	ftInfo.SortableValuesSizeMB = ToFloat(data["sortable_values_size_mb"])
	ftInfo.TagOverheadSzMB = ToFloat(data["tag_overhead_sz_mb"])
	ftInfo.TextOverheadSzMB = ToFloat(data["text_overhead_sz_mb"])
	ftInfo.TotalIndexMemorySzMB = ToFloat(data["total_index_memory_sz_mb"])
	ftInfo.TotalIndexingTime = ToInteger(data["total_indexing_time"])
	ftInfo.TotalInvertedIndexBlocks = ToInteger(data["total_inverted_index_blocks"])
	ftInfo.VectorIndexSzMB = ToFloat(data["vector_index_sz_mb"])

	return ftInfo, nil
}

func (cmd *FTInfoCmd) from(res valkey.ValkeyResult) {
	if err := res.Error(); err != nil {
		cmd.SetErr(err)
		return
	}
	m, err := res.AsMap()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	anyM, err := res.ToAny()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.SetRawVal(anyM)
	anyMap := make(map[string]any, len(m))
	for k, v := range m {
		anyMap[k], err = v.ToAny()
		if err != nil {
			cmd.SetErr(err)
			return
		}
	}
	ftInfoResult, err := parseFTInfo(anyMap)
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.SetVal(ftInfoResult)
}

func newFTInfoCmd(res valkey.ValkeyResult) *FTInfoCmd {
	cmd := &FTInfoCmd{}
	cmd.from(res)
	return cmd
}

type FTSpellCheckOptions struct {
	Terms    *FTSpellCheckTerms
	Distance int
	Dialect  int
}

type FTSpellCheckTerms struct {
	Inclusion  string // Either "INCLUDE" or "EXCLUDE"
	Dictionary string
	Terms      []interface{}
}

type SpellCheckResult struct {
	Term        string
	Suggestions []SpellCheckSuggestion
}

type SpellCheckSuggestion struct {
	Suggestion string
	Score      float64
}

type FTSpellCheckCmd struct{ baseCmd[[]SpellCheckResult] }

func (cmd *FTSpellCheckCmd) Val() []SpellCheckResult {
	return cmd.val
}

func (cmd *FTSpellCheckCmd) Result() ([]SpellCheckResult, error) {
	return cmd.Val(), cmd.Err()
}

func (cmd *FTSpellCheckCmd) from(res valkey.ValkeyResult) {
	if err := res.Error(); err != nil {
		cmd.SetErr(err)
		return
	}
	msg, err := res.ToMessage()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	if !(msg.IsMap() || msg.IsArray()) {
		panic("res should be either map(RESP3) or array(RESP2)")
	}
	if msg.IsMap() {
		// is RESP3 map
		m, err := msg.ToMap()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		anyM, err := msg.ToAny()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		cmd.SetRawVal(anyM)
		spellCheckResults := []SpellCheckResult{}
		result := m["results"]
		resultMap, err := result.ToMap()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		for k, v := range resultMap {
			result := SpellCheckResult{}
			result.Term = k
			suggestions, err := v.ToArray()
			if err != nil {
				cmd.SetErr(err)
				return
			}
			for _, suggestion := range suggestions {
				// map key: suggestion, score
				sugMap, err := suggestion.ToMap()
				if err != nil {
					cmd.SetErr(err)
					return
				}
				for _k, _v := range sugMap {
					score, err := _v.ToFloat64()
					if err != nil {
						cmd.SetErr(err)
						return
					}
					result.Suggestions = append(result.Suggestions, SpellCheckSuggestion{Suggestion: _k, Score: score})
				}
			}
			spellCheckResults = append(spellCheckResults, result)
		}
		cmd.SetVal(spellCheckResults)
		return
	}
	// is RESP2 array
	arr, err := msg.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	anyRes, err := msg.ToAny()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.SetRawVal(anyRes)
	AnyArr := make([]any, 0, len(arr))
	for _, e := range arr {
		anyE, err := e.ToAny()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		AnyArr = append(AnyArr, anyE)
	}
	result, err := parseFTSpellCheck(AnyArr)
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.SetVal(result)
}

func newFTSpellCheckCmd(res valkey.ValkeyResult) *FTSpellCheckCmd {
	cmd := &FTSpellCheckCmd{}
	cmd.from(res)
	return cmd
}

func parseFTSpellCheck(data []interface{}) ([]SpellCheckResult, error) {
	results := make([]SpellCheckResult, 0, len(data))

	for _, termData := range data {
		termInfo, ok := termData.([]interface{})
		if !ok || len(termInfo) != 3 {
			return nil, fmt.Errorf("invalid term format")
		}

		term, ok := termInfo[1].(string)
		if !ok {
			return nil, fmt.Errorf("invalid term format")
		}

		suggestionsData, ok := termInfo[2].([]interface{})
		if !ok {
			return nil, fmt.Errorf("invalid suggestions format")
		}

		suggestions := make([]SpellCheckSuggestion, 0, len(suggestionsData))
		for _, suggestionData := range suggestionsData {
			suggestionInfo, ok := suggestionData.([]interface{})
			if !ok || len(suggestionInfo) != 2 {
				return nil, fmt.Errorf("invalid suggestion format")
			}

			scoreStr, ok := suggestionInfo[0].(string)
			if !ok {
				return nil, fmt.Errorf("invalid suggestion score format")
			}
			score, err := strconv.ParseFloat(scoreStr, 64)
			if err != nil {
				return nil, fmt.Errorf("invalid suggestion score value")
			}

			suggestion, ok := suggestionInfo[1].(string)
			if !ok {
				return nil, fmt.Errorf("invalid suggestion format")
			}

			suggestions = append(suggestions, SpellCheckSuggestion{
				Score:      score,
				Suggestion: suggestion,
			})
		}

		results = append(results, SpellCheckResult{
			Term:        term,
			Suggestions: suggestions,
		})
	}

	return results, nil
}

type Document struct {
	Score   *float64
	Payload *string
	SortKey *string
	Fields  map[string]string
	ID      string
}

type FTSearchResult struct {
	Docs  []Document
	Total int64
}

type FTSearchOptions struct {
	Params          map[string]interface{}
	Language        string
	Expander        string
	Scorer          string
	Payload         string
	Filters         []FTSearchFilter
	GeoFilter       []FTSearchGeoFilter
	InKeys          []interface{}
	InFields        []interface{}
	Return          []FTSearchReturn
	SortBy          []FTSearchSortBy
	Slop            int
	Timeout         int
	LimitOffset     int
	Limit           int
	DialectVersion  int
	NoContent       bool
	Verbatim        bool
	NoStopWords     bool
	WithScores      bool
	WithPayloads    bool
	WithSortKeys    bool
	InOrder         bool
	ExplainScore    bool
	SortByWithCount bool
}

type FTSearchCmd struct {
	baseCmd[FTSearchResult]
	options *FTSearchOptions
}

// Ref: https://github.com/redis/go-redis/blob/v9.7.0/search_commands.go#L1541
func (cmd *FTSearchCmd) from(res valkey.ValkeyResult) {
	if err := res.Error(); err != nil {
		cmd.SetErr(err)
		return
	}
	anyRes, err := res.ToAny()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.SetRawVal(anyRes)
	msg, err := res.ToMessage()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	if !(msg.IsMap() || msg.IsArray()) {
		panic("res should be either map(RESP3) or array(RESP2)")
	}
	if msg.IsMap() {
		// is RESP3 map
		m, err := msg.ToMap()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		totalResultsMsg, ok := m["total_results"]
		if !ok {
			cmd.SetErr(fmt.Errorf(`result map should contain key "total_results"`))
		}
		totalResults, err := totalResultsMsg.AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		resultsMsg, ok := m["results"]
		if !ok {
			cmd.SetErr(fmt.Errorf(`result map should contain key "results"`))
		}
		resultsArr, err := resultsMsg.ToArray()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		ftSearchResult := FTSearchResult{Total: totalResults, Docs: make([]Document, 0, len(resultsArr))}
		for _, result := range resultsArr {
			resultMap, err := result.ToMap()
			if err != nil {
				cmd.SetErr(err)
				return
			}
			doc := Document{}
			for k, v := range resultMap {
				switch k {
				case "id":
					idStr, err := v.ToString()
					if err != nil {
						cmd.SetErr(err)
						return
					}
					doc.ID = idStr
				case "extra_attributes":
					// doc.ID = resultArr[i+1].String()
					strMap, err := v.AsStrMap()
					if err != nil {
						cmd.SetErr(err)
						return
					}
					doc.Fields = strMap
				case "score":
					score, err := v.AsFloat64()
					if err != nil {
						cmd.SetErr(err)
						return
					}
					doc.Score = &score
				case "payload":
					if !v.IsNil() {
						payload, err := v.ToString()
						if err != nil {
							cmd.SetErr(err)
							return
						}
						doc.Payload = &payload
					}
				case "sortkey":
					if !v.IsNil() {
						sortKey, err := v.ToString()
						if err != nil {
							cmd.SetErr(err)
							return
						}
						doc.SortKey = &sortKey
					}
				}
			}

			ftSearchResult.Docs = append(ftSearchResult.Docs, doc)
		}
		cmd.SetVal(ftSearchResult)
		return
	}
	// is RESP2 array
	data, err := msg.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	if len(data) < 1 {
		cmd.SetErr(fmt.Errorf("unexpected search result format"))
		return
	}
	total, err := data[0].AsInt64()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	var results []Document
	for i := 1; i < len(data); {
		docID, err := data[i].ToString()
		if err != nil {
			cmd.SetErr(fmt.Errorf("invalid total results format: %w", err))
			return
		}
		doc := Document{
			ID:     docID,
			Fields: make(map[string]string),
		}
		i++
		if cmd.options != nil {
			if cmd.options.NoContent {
				results = append(results, doc)
				continue
			}
			if cmd.options.WithScores && i < len(data) {
				scoreStr, err := data[i].ToString()
				if err != nil {
					cmd.SetErr(fmt.Errorf("invalid score format: %w", err))
					return
				}
				score, err := strconv.ParseFloat(scoreStr, 64)
				if err != nil {
					cmd.SetErr(fmt.Errorf("invalid score format: %w", err))
					return
				}
				doc.Score = &score
				i++
			}
			if cmd.options.WithPayloads && i < len(data) {
				payload, err := data[i].ToString()
				if err != nil {
					cmd.SetErr(fmt.Errorf("invalid payload format: %w", err))
					return
				}
				doc.Payload = &payload
				i++
			}
			if cmd.options.WithSortKeys && i < len(data) {
				sortKey, err := data[i].ToString()
				if err != nil {
					cmd.SetErr(fmt.Errorf("invalid payload format: %w", err))
					return
				}
				doc.SortKey = &sortKey
				i++
			}
		}
		if i < len(data) {
			fields, err := data[i].ToArray()
			if err != nil {
				cmd.SetErr(fmt.Errorf("invalid document fields format: %w", err))
				return
			}
			for j := 0; j < len(fields); j += 2 {
				key, err := fields[j].ToString()
				if err != nil {
					cmd.SetErr(fmt.Errorf("invalid field key format: %w", err))
					return
				}
				value, err := fields[j+1].ToString()
				if err != nil {
					cmd.SetErr(fmt.Errorf("invalid field value format: %w", err))
					return
				}
				doc.Fields[key] = value
			}
			i++
		}
		results = append(results, doc)
	}
	cmd.SetVal(FTSearchResult{
		Total: total,
		Docs:  results,
	})
}

func newFTSearchCmd(res valkey.ValkeyResult, options *FTSearchOptions) *FTSearchCmd {
	cmd := &FTSearchCmd{options: options}
	cmd.from(res)
	return cmd
}

type FTSynUpdateOptions struct {
	SkipInitialScan bool
}

type FTSynDumpCmd struct {
	baseCmd[[]FTSynDumpResult]
}

func (cmd *FTSynDumpCmd) from(res valkey.ValkeyResult) {
	if err := res.Error(); err != nil {
		cmd.SetErr(err)
		return
	}
	anyRes, err := res.ToAny()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	cmd.SetRawVal(anyRes)
	msg, err := res.ToMessage()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	if !(msg.IsMap() || msg.IsArray()) {
		panic("res should be either map(RESP3) or array(RESP2)")
	}
	if msg.IsMap() {
		// is RESP3 map
		m, err := msg.ToMap()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		results := make([]FTSynDumpResult, 0, len(m))
		for term, synMsg := range m {
			synonyms, err := synMsg.AsStrSlice()
			if err != nil {
				cmd.SetErr(err)
				return
			}
			results = append(results, FTSynDumpResult{Term: term, Synonyms: synonyms})
		}
		cmd.SetVal(results)
		return
	}
	// is RESP2 array
	arr, err := msg.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	results := make([]FTSynDumpResult, 0, len(arr)/2)
	for i := 0; i < len(arr); i += 2 {
		term, err := arr[i].ToString()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		synonyms, err := arr[i+1].AsStrSlice()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		results = append(results, FTSynDumpResult{
			Term:     term,
			Synonyms: synonyms,
		})
	}
	cmd.SetVal(results)
}

func newFTSynDumpCmd(res valkey.ValkeyResult) *FTSynDumpCmd {
	cmd := &FTSynDumpCmd{}
	cmd.from(res)
	return cmd
}

type FTSynDumpResult struct {
	Term     string
	Synonyms []string
}

// ClientFlags is redis-server client flags, copy from redis/src/server.h (redis 7.0)
type ClientFlags uint64

const (
	ClientSlave            ClientFlags = 1 << 0  /* This client is a replica */
	ClientMaster           ClientFlags = 1 << 1  /* This client is a master */
	ClientMonitor          ClientFlags = 1 << 2  /* This client is a slave monitor, see MONITOR */
	ClientMulti            ClientFlags = 1 << 3  /* This client is in a MULTI context */
	ClientBlocked          ClientFlags = 1 << 4  /* The client is waiting in a blocking operation */
	ClientDirtyCAS         ClientFlags = 1 << 5  /* Watched keys modified. EXEC will fail. */
	ClientCloseAfterReply  ClientFlags = 1 << 6  /* Close after writing the entire reply. */
	ClientUnBlocked        ClientFlags = 1 << 7  /* This client was unblocked and is stored in server.unblocked_clients */
	ClientScript           ClientFlags = 1 << 8  /* This is a non-connected client used by Lua */
	ClientAsking           ClientFlags = 1 << 9  /* Client issued the ASKING command */
	ClientCloseASAP        ClientFlags = 1 << 10 /* Close this client ASAP */
	ClientUnixSocket       ClientFlags = 1 << 11 /* Client connected via Unix domain socket */
	ClientDirtyExec        ClientFlags = 1 << 12 /* EXEC will fail for errors while queueing */
	ClientMasterForceReply ClientFlags = 1 << 13 /* Queue replies even if is master */
	ClientForceAOF         ClientFlags = 1 << 14 /* Force AOF propagation of current cmd. */
	ClientForceRepl        ClientFlags = 1 << 15 /* Force replication of the current cmd. */
	ClientPrePSync         ClientFlags = 1 << 16 /* Instance don't understand PSYNC. */
	ClientReadOnly         ClientFlags = 1 << 17 /* Cluster client is in the read-only state. */
	ClientPubSub           ClientFlags = 1 << 18 /* Client is in Pub/Sub mode. */
	ClientPreventAOFProp   ClientFlags = 1 << 19 /* Don't propagate to AOF. */
	ClientPreventReplProp  ClientFlags = 1 << 20 /* Don't propagate to slaves. */
	ClientPreventProp      ClientFlags = ClientPreventAOFProp | ClientPreventReplProp
	ClientPendingWrite     ClientFlags = 1 << 21 /* Client has output to send, but a-write handler is yet not installed. */
	ClientReplyOff         ClientFlags = 1 << 22 /* Don't send replies to a client. */
	ClientReplySkipNext    ClientFlags = 1 << 23 /* Set ClientREPLY_SKIP for the next cmd */
	ClientReplySkip        ClientFlags = 1 << 24 /* Don't send just this reply. */
	ClientLuaDebug         ClientFlags = 1 << 25 /* Run EVAL in debug mode. */
	ClientLuaDebugSync     ClientFlags = 1 << 26 /* EVAL debugging without fork() */
	ClientModule           ClientFlags = 1 << 27 /* Non-connected client used by some module. */
	ClientProtected        ClientFlags = 1 << 28 /* Client should not be freed for now. */
	ClientExecutingCommand ClientFlags = 1 << 29 /* Indicates that the client is currently in the process of handling
	   a command. usually this will be marked only during call()
	   however, blocked clients might have this flag kept until they
	   will try to reprocess the command. */
	ClientPendingCommand      ClientFlags = 1 << 30 /* Indicates the client has a fully * parsed command ready for execution. */
	ClientTracking            ClientFlags = 1 << 31 /* Client enabled key tracking in order to perform client side caching. */
	ClientTrackingBrokenRedir ClientFlags = 1 << 32 /* Target client is invalid. */
	ClientTrackingBCAST       ClientFlags = 1 << 33 /* Tracking in BCAST mode. */
	ClientTrackingOptIn       ClientFlags = 1 << 34 /* Tracking in opt-in mode. */
	ClientTrackingOptOut      ClientFlags = 1 << 35 /* Tracking in opt-out mode. */
	ClientTrackingCaching     ClientFlags = 1 << 36 /* CACHING yes/no was given, depending on opt-in/opt-out mode. */
	ClientTrackingNoLoop      ClientFlags = 1 << 37 /* Don't send invalidation messages about writes performed by myself.*/
	ClientInTimeoutTable      ClientFlags = 1 << 38 /* This client is in the timeout table. */
	ClientProtocolError       ClientFlags = 1 << 39 /* Protocol error chatting with it. */
	ClientCloseAfterCommand   ClientFlags = 1 << 40 /* Close after executing commands * and writing the entire reply. */
	ClientDenyBlocking        ClientFlags = 1 << 41 /* Indicate that the client should not be blocked. currently, turned on inside MULTI, Lua, RM_Call, and AOF client */
	ClientReplRDBOnly         ClientFlags = 1 << 42 /* This client is a replica that only wants RDB without a replication buffer. */
	ClientNoEvict             ClientFlags = 1 << 43 /* This client is protected against client memory eviction. */
	ClientAllowOOM            ClientFlags = 1 << 44 /* Client used by RM_Call is allowed to fully execute scripts even when in OOM */
	ClientNoTouch             ClientFlags = 1 << 45 /* This client will not touch LFU/LRU stats. */
	ClientPushing             ClientFlags = 1 << 46 /* This client is pushing notifications. */
)

// ClientInfo is valkey-server ClientInfo
type ClientInfo struct {
	Addr               string        // address/port of the client
	LAddr              string        // address/port of a local address client connected to (bind address)
	Name               string        // the name set by the client with CLIENT SETNAME
	Events             string        // file descriptor events (see below)
	LastCmd            string        // cmd, last command played
	User               string        // the authenticated username of the client
	LibName            string        // valkey version 7.2, client library name
	LibVer             string        // valkey version 7.2, client library version
	ID                 int64         // valkey version 2.8.12, a unique 64-bit client ID
	FD                 int64         // file descriptor corresponding to the socket
	Age                time.Duration // total duration of the connection in seconds
	Idle               time.Duration // idle time of the connection in seconds
	Flags              ClientFlags   // client flags (see below)
	DB                 int           // current database ID
	Sub                int           // number of channel subscriptions
	PSub               int           // number of pattern matching subscriptions
	SSub               int           // valkey version 7.0.3, number of shard channel subscriptions
	Multi              int           // number of commands in a MULTI/EXEC context
	Watch              int           // valkey version 7.4 RC1, the number of keys this client is currently watching.
	QueryBuf           int           // qbuf, query buffer length (0 means no query pending)
	QueryBufFree       int           // qbuf-free, free space of the query buffer (0 means the buffer is full)
	ArgvMem            int           // incomplete arguments for the next command (already extracted from query buffer)
	MultiMem           int           // valkey version 7.0, memory is used up by buffered multi commands
	BufferSize         int           // rbs, usable size of buffer
	BufferPeak         int           // rbp, peak used size of buffer in the last 5 sec interval
	OutputBufferLength int           // obl, output buffer length
	OutputListLength   int           // oll, output list length (replies are queued in this list when the buffer is full)
	OutputMemory       int           // omem, output buffer memory usage
	TotalMemory        int           // tot-mem, total memory consumed by this client in its various buffers
	Redir              int64         // client id of current client tracking redirection
	Resp               int           // valkey version 7.0, client RESP protocol version
	TotalNetIn         int64         // tot-net-in, total network bytes read from the client connection
	TotalNetOut        int64         // tot-net-out, total network bytes sent to the client connection
	TotalCmds          int64         // tot-cmds, number of commands executed by the client connection
}

type ClientInfoCmd struct {
	baseCmd[*ClientInfo]
}

func newClientInfoCmd(res valkey.ValkeyResult) *ClientInfoCmd {
	cmd := &ClientInfoCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *ClientInfoCmd) SetVal(val *ClientInfo) {
	cmd.val = val
}

func (cmd *ClientInfoCmd) Val() *ClientInfo {
	return cmd.val
}

func (cmd *ClientInfoCmd) Result() (*ClientInfo, error) {
	return cmd.val, cmd.err
}

func stringToClientInfo(txt string) (*ClientInfo, error) {
	info := &ClientInfo{}
	var err error
	for _, s := range strings.Split(strings.TrimPrefix(strings.TrimSpace(txt), "txt:"), " ") {
		kv := strings.Split(s, "=")
		if len(kv) != 2 {
			return nil, fmt.Errorf("valkey: unexpected client info data (%s)", s)
		}
		key, val := kv[0], kv[1]

		switch key {
		case "id":
			info.ID, err = strconv.ParseInt(val, 10, 64)
		case "addr":
			info.Addr = val
		case "laddr":
			info.LAddr = val
		case "fd":
			info.FD, err = strconv.ParseInt(val, 10, 64)
		case "name":
			info.Name = val
		case "age":
			var age int
			if age, err = strconv.Atoi(val); err == nil {
				info.Age = time.Duration(age) * time.Second
			}
		case "idle":
			var idle int
			if idle, err = strconv.Atoi(val); err == nil {
				info.Idle = time.Duration(idle) * time.Second
			}
		case "flags":
			if val == "N" {
				break
			}

			for i := 0; i < len(val); i++ {
				switch val[i] {
				case 'S':
					info.Flags |= ClientSlave
				case 'O':
					info.Flags |= ClientSlave | ClientMonitor
				case 'M':
					info.Flags |= ClientMaster
				case 'P':
					info.Flags |= ClientPubSub
				case 'x':
					info.Flags |= ClientMulti
				case 'b':
					info.Flags |= ClientBlocked
				case 't':
					info.Flags |= ClientTracking
				case 'R':
					info.Flags |= ClientTrackingBrokenRedir
				case 'B':
					info.Flags |= ClientTrackingBCAST
				case 'd':
					info.Flags |= ClientDirtyCAS
				case 'c':
					info.Flags |= ClientCloseAfterCommand
				case 'u':
					info.Flags |= ClientUnBlocked
				case 'A':
					info.Flags |= ClientCloseASAP
				case 'U':
					info.Flags |= ClientUnixSocket
				case 'r':
					info.Flags |= ClientReadOnly
				case 'e':
					info.Flags |= ClientNoEvict
				case 'T':
					info.Flags |= ClientNoTouch
				default:
					return nil, fmt.Errorf("valkey: unexpected client info flags(%s)", string(val[i]))
				}
			}
		case "db":
			info.DB, err = strconv.Atoi(val)
		case "sub":
			info.Sub, err = strconv.Atoi(val)
		case "psub":
			info.PSub, err = strconv.Atoi(val)
		case "ssub":
			info.SSub, err = strconv.Atoi(val)
		case "multi":
			info.Multi, err = strconv.Atoi(val)
		case "watch":
			info.Watch, err = strconv.Atoi(val)
		case "qbuf":
			info.QueryBuf, err = strconv.Atoi(val)
		case "qbuf-free":
			info.QueryBufFree, err = strconv.Atoi(val)
		case "argv-mem":
			info.ArgvMem, err = strconv.Atoi(val)
		case "multi-mem":
			info.MultiMem, err = strconv.Atoi(val)
		case "rbs":
			info.BufferSize, err = strconv.Atoi(val)
		case "rbp":
			info.BufferPeak, err = strconv.Atoi(val)
		case "obl":
			info.OutputBufferLength, err = strconv.Atoi(val)
		case "oll":
			info.OutputListLength, err = strconv.Atoi(val)
		case "omem":
			info.OutputMemory, err = strconv.Atoi(val)
		case "tot-mem":
			info.TotalMemory, err = strconv.Atoi(val)
		case "events":
			info.Events = val
		case "cmd":
			info.LastCmd = val
		case "user":
			info.User = val
		case "redir":
			info.Redir, err = strconv.ParseInt(val, 10, 64)
		case "resp":
			info.Resp, err = strconv.Atoi(val)
		case "lib-name":
			info.LibName = val
		case "lib-ver":
			info.LibVer = val
		case "tot-net-in":
			info.TotalNetIn, err = strconv.ParseInt(val, 10, 64)
		case "tot-net-out":
			info.TotalNetOut, err = strconv.ParseInt(val, 10, 64)
		case "tot-cmds":
			info.TotalCmds, err = strconv.ParseInt(val, 10, 64)
		}

		if err != nil {
			return nil, err
		}
	}
	return info, nil
}

// fmt.Sscanf() cannot handle null values
func (cmd *ClientInfoCmd) from(res valkey.ValkeyResult) {
	txt, err := res.ToString()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	info, err := stringToClientInfo(txt)
	if err != nil {
		cmd.SetErr(err)
		return
	}

	cmd.SetVal(info)
}

type ACLLogEntry struct {
	Count                int64
	Reason               string
	Context              string
	Object               string
	Username             string
	AgeSeconds           float64
	ClientInfo           *ClientInfo
	EntryID              int64
	TimestampCreated     int64
	TimestampLastUpdated int64
}

type ACLLogCmd struct {
	baseCmd[[]*ACLLogEntry]
}

func (cmd *ACLLogCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}

	logEntries := make([]*ACLLogEntry, 0, len(arr))
	for _, msg := range arr {
		log, err := msg.AsMap()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		entry := ACLLogEntry{}

		for key, attr := range log {
			switch key {
			case "count":
				entry.Count, err = attr.AsInt64()
			case "reason":
				entry.Reason, err = attr.ToString()
			case "context":
				entry.Context, err = attr.ToString()
			case "object":
				entry.Object, err = attr.ToString()
			case "username":
				entry.Username, err = attr.ToString()
			case "age-seconds":
				entry.AgeSeconds, err = attr.AsFloat64()
			case "client-info":
				txt, txtErr := attr.ToString()
				if txtErr == nil {
					entry.ClientInfo, err = stringToClientInfo(txt)
				} else {
					err = txtErr
				}
			case "entry-id":
				entry.EntryID, err = attr.AsInt64()
			case "timestamp-created":
				entry.TimestampCreated, err = attr.AsInt64()
			case "timestamp-last-updated":
				entry.TimestampLastUpdated, err = attr.AsInt64()
			}
		}

		if err != nil {
			cmd.SetErr(err)
			return
		}
		logEntries = append(logEntries, &entry)
	}
	cmd.SetVal(logEntries)
}

func newACLLogCmd(res valkey.ValkeyResult) *ACLLogCmd {
	cmd := &ACLLogCmd{}
	cmd.from(res)
	return cmd
}

// ModuleLoadexConfig struct is used to specify the arguments for the MODULE LOADEX command of valkey.
// `MODULE LOADEX path [CONFIG name value [CONFIG name value ...]] [ARGS args [args ...]]`
type ModuleLoadexConfig struct {
	Path string
	Conf map[string]interface{}
	Args []interface{}
}

type ClusterLink struct {
	Direction           string
	Node                string
	CreateTime          int64
	Events              string
	SendBufferAllocated int64
	SendBufferUsed      int64
}

// ClusterLinksCmd represents the response structure for ClusterLinks.
type ClusterLinksCmd struct {
	val []ClusterLink
	err error
}

func (c *ClusterLinksCmd) SetErr(err error) {
	c.err = err
}

func (c *ClusterLinksCmd) Err() error {
	return c.err
}

func (cmd *ClusterLinksCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}

	val := make([]ClusterLink, 0, len(arr))
	for _, v := range arr {
		dict, err := v.AsMap()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		link := ClusterLink{}

		for k, v := range dict {
			switch k {
			case "direction":
				link.Direction, _ = v.ToString()
			case "node":
				link.Node, _ = v.ToString()
			case "create-time":
				link.CreateTime, _ = v.ToInt64()
			case "events":
				link.Events, _ = v.ToString()
			case "send-buffer-allocated":
				link.SendBufferAllocated, _ = v.ToInt64()
			case "send-buffer-used":
				link.SendBufferUsed, _ = v.ToInt64()
			default:
				cmd.SetErr(fmt.Errorf("unexpected key %q in CLUSTER LINKS reply", k))
				return
			}
		}
		val = append(val, link)
	}

	cmd.val = val
}

func newClusterLinksCmd(resp valkey.ValkeyResult) *ClusterLinksCmd {
	cmd := &ClusterLinksCmd{}
	cmd.from(resp)
	return cmd
}

func (cmd *ClusterLinksCmd) SetVal(val []ClusterLink) {
	cmd.val = val
}

func (cmd *ClusterLinksCmd) Val() []ClusterLink {
	return cmd.val
}

func (cmd *ClusterLinksCmd) Result() ([]ClusterLink, error) {
	return cmd.Val(), cmd.Err()
}

type SlowLog struct {
	ID         int64
	Time       time.Time
	Duration   time.Duration
	Args       []string
	ClientAddr string
	ClientName string
}

type SlowLogCmd struct {
	baseCmd[[]*SlowLog]
}

func (cmd *SlowLogCmd) from(res valkey.ValkeyResult) {
	arr, err := res.ToArray()
	if err != nil {
		cmd.SetErr(err)
		return
	}

	logEntries := make([]*SlowLog, 0, len(arr))
	for _, msg := range arr {
		log, err := msg.ToArray()

		if err != nil {
			cmd.SetErr(err)
			return
		}

		if len(log) < 4 {
			cmd.SetErr(fmt.Errorf("valkey: got %d elements in slowlog get, expected at least 4", len(log)))
			return
		}

		entry := SlowLog{}

		entry.ID, err = log[0].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}

		createdAt, err := log[1].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		entry.Time = time.Unix(createdAt, 0)

		duration, err := log[2].AsInt64()
		if err != nil {
			cmd.SetErr(err)
			return
		}
		entry.Duration = time.Duration(duration) * time.Microsecond

		argsArr, err := log[3].ToArray()
		if err != nil {
			cmd.SetErr(err)
			return
		}

		entry.Args = make([]string, len(argsArr))

		for i, arg := range argsArr {
			entry.Args[i], err = arg.ToString()
			if err != nil {
				cmd.SetErr(err)
				return
			}
		}

		if len(log) >= 5 {
			entry.ClientAddr, err = log[4].ToString()
			if err != nil {
				cmd.SetErr(err)
				return
			}
		}

		if len(log) >= 6 {
			entry.ClientName, err = log[5].ToString()
			if err != nil {
				cmd.SetErr(err)
				return
			}
		}

		logEntries = append(logEntries, &entry)
	}
	cmd.SetVal(logEntries)
}

func newSlowLogCmd(res valkey.ValkeyResult) *SlowLogCmd {
	cmd := &SlowLogCmd{}
	cmd.from(res)
	return cmd
}

// LCSQuery is a parameter used for the LCS command
type LCSQuery struct {
	Key1         string
	Key2         string
	Len          bool
	Idx          bool
	MinMatchLen  int
	WithMatchLen bool
}

// LCSMatch is the result set of the LCS command
type LCSMatch struct {
	MatchString string
	Matches     []LCSMatchedPosition
	Len         int64
}

type LCSMatchedPosition struct {
	Key1 LCSPosition
	Key2 LCSPosition

	// only for withMatchLen is true
	MatchLen int64
}

type LCSPosition struct {
	Start int64
	End   int64
}

type LCSCmd struct {
	baseCmd[*LCSMatch]

	// 1: match string
	// 2: match len
	// 3: match idx LCSMatch
	readType uint8
}

func newLCSCmd(res valkey.ValkeyResult, readType uint8) *LCSCmd {
	cmd := &LCSCmd{readType: readType}
	cmd.from(res)
	return cmd
}

func (cmd *LCSCmd) SetVal(val *LCSMatch) {
	cmd.val = val
}

func (cmd *LCSCmd) SetErr(err error) {
	cmd.err = err
}

func (cmd *LCSCmd) Val() *LCSMatch {
	return cmd.val
}

func (cmd *LCSCmd) Err() error {
	return cmd.err
}

func (cmd *LCSCmd) Result() (*LCSMatch, error) {
	return cmd.val, cmd.err
}

func (cmd *LCSCmd) from(res valkey.ValkeyResult) {
	lcs := &LCSMatch{}
	var err error

	switch cmd.readType {
	case 1:
		// match string
		if lcs.MatchString, err = res.ToString(); err != nil {
			cmd.SetErr(err)
			return
		}
	case 2:
		// match len
		if lcs.Len, err = res.AsInt64(); err != nil {
			cmd.SetErr(err)
			return
		}
	case 3:
		// read LCSMatch
		if msgMap, err := res.AsMap(); err != nil {
			cmd.SetErr(err)
			return
		} else {
			// Validate length (should have exactly 2 keys: "matches" and "len")
			if len(msgMap) != 2 {
				cmd.SetErr(fmt.Errorf("valkey: got %d elements in the map, wanted %d", len(msgMap), 2))
				return
			}

			// read matches or len field
			for key, value := range msgMap {
				switch key {
				case "matches":
					// read an array of matched positions
					matches, err := cmd.readMatchedPositions(value)
					if err != nil {
						cmd.SetErr(err)
						return
					}
					lcs.Matches = matches

				case "len":
					// read match length
					matchLen, err := value.AsInt64()
					if err != nil {
						cmd.SetErr(err)
						return
					}
					lcs.Len = matchLen
				}
			}
		}
	}

	cmd.val = lcs
}

func (cmd *LCSCmd) readMatchedPositions(res valkey.ValkeyMessage) ([]LCSMatchedPosition, error) {
	val, err := res.ToArray()
	if err != nil {
		return nil, err
	}

	n := len(val)
	positions := make([]LCSMatchedPosition, n)

	for i := 0; i < n; i++ {
		pn, err := val[i].ToArray()
		if err != nil {
			return nil, err
		}

		if len(pn) < 2 {
			return nil, fmt.Errorf("invalid position format")
		}

		key1, err := cmd.readPosition(pn[0])
		if err != nil {
			return nil, err
		}

		key2, err := cmd.readPosition(pn[1])
		if err != nil {
			return nil, err
		}

		pos := LCSMatchedPosition{
			Key1: key1,
			Key2: key2,
		}

		// Read match length if WithMatchLen is true
		if len(pn) > 2 {
			if pos.MatchLen, err = pn[2].AsInt64(); err != nil {
				return nil, err
			}
		}

		positions[i] = pos
	}

	return positions, nil
}

func (cmd *LCSCmd) readPosition(res valkey.ValkeyMessage) (LCSPosition, error) {
	posArray, err := res.ToArray()
	if err != nil {
		return LCSPosition{}, err
	}
	if len(posArray) != 2 {
		return LCSPosition{}, fmt.Errorf("valkey: got %d elements in the array, wanted %d", len(posArray), 2)
	}

	start, err := posArray[0].AsInt64()
	if err != nil {
		return LCSPosition{}, err
	}

	end, err := posArray[1].AsInt64()
	if err != nil {
		return LCSPosition{}, err
	}

	return LCSPosition{Start: start, End: end}, nil
}

type FunctionStats struct {
	Engines   []Engine
	isRunning bool
	rs        RunningScript
	allrs     []RunningScript
}

func (fs *FunctionStats) Running() bool {
	return fs.isRunning
}

func (fs *FunctionStats) RunningScript() (RunningScript, bool) {
	return fs.rs, fs.isRunning
}

func (fs *FunctionStats) AllRunningScripts() []RunningScript {
	return fs.allrs
}

type FunctionStatsCmd struct {
	baseCmd[FunctionStats]
}

func newFunctionStatsCmd(res valkey.ValkeyResult) *FunctionStatsCmd {
	cmd := &FunctionStatsCmd{}
	cmd.from(res)
	return cmd
}

func (cmd *FunctionStatsCmd) from(res valkey.ValkeyResult) {
	var fstats FunctionStats
	mp, err := res.AsMap()
	if err != nil {
		cmd.SetErr(err)
		return
	}
	for key, val := range mp {
		switch key {
		case "running_script":
			fstats.rs, fstats.isRunning, err = cmd.parseRunningScript(val)
			if err != nil {
				cmd.SetErr(err)
				return
			}
		case "engines":
			fstats.Engines, err = cmd.parseEngines(val)
			if err != nil {
				cmd.SetErr(err)
				return
			}
		case "all_running_scripts":
			fstats.allrs, err = cmd.parseRunningScripts(val)
			if err != nil {
				cmd.SetErr(err)
				return
			}
		}

	}
	cmd.SetVal(fstats)
}

type RunningScript struct {
	Name     string
	Command  []string
	Duration time.Duration
}

func (cmd *FunctionStatsCmd) parseRunningScript(msg valkey.ValkeyMessage) (RunningScript, bool, error) {
	rsMap, err := msg.AsMap()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return RunningScript{}, false, nil
		}
		return RunningScript{}, false, err
	}

	if len(rsMap) == 0 {
		return RunningScript{}, false, nil
	}

	val := RunningScript{}
	for key, attr := range rsMap {
		switch key {
		case "name":
			val.Name, err = attr.ToString()
		case "command":
			val.Command, err = attr.AsStrSlice()
		case "duration_ms":
			ms, err := attr.AsInt64()
			if err != nil {
				return RunningScript{}, false, err
			}
			val.Duration = time.Duration(ms) * time.Millisecond
		}
		if err != nil {
			return RunningScript{}, false, err
		}
	}
	return val, true, nil
}

type Engine struct {
	Language       string
	LibrariesCount int64
	FunctionsCount int64
}

func (cmd *FunctionStatsCmd) parseEngines(msg valkey.ValkeyMessage) ([]Engine, error) {

	engineMap, err := msg.AsMap()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return []Engine{}, nil
		}
		return []Engine{}, err
	}
	vals := make([]Engine, 0, len(engineMap))
	for key, attr := range engineMap {
		engine := Engine{}
		engine.Language = key
		emap, err := attr.AsMap()
		if err != nil {
			return []Engine{}, err
		}
		for k, v := range emap {
			switch k {
			case "libraries_count":
				engine.LibrariesCount, err = v.AsInt64()
			case "functions_count":
				engine.FunctionsCount, err = v.AsInt64()
			}
			if err != nil {
				return []Engine{}, err
			}
		}
		vals = append(vals, engine)
	}
	return vals, nil
}

func (cmd *FunctionStatsCmd) parseRunningScripts(msg valkey.ValkeyMessage) ([]RunningScript, error) {
	rScriptMap, err := msg.AsMap()
	if err != nil {
		if valkey.IsValkeyNil(err) {
			return []RunningScript{}, nil
		}
		return []RunningScript{}, err
	}
	vals := make([]RunningScript, 0, len(rScriptMap))
	for _, attr := range rScriptMap {
		var val RunningScript
		attrMap, err := attr.AsMap()
		for k, v := range attrMap {
			switch k {
			case "name":
				val.Name, err = v.ToString()
			case "duration_ms":
				ms, err := v.AsInt64()
				if err != nil {
					return []RunningScript{}, err
				}
				val.Duration = time.Duration(ms) * time.Millisecond
			case "command":
				val.Command, err = v.AsStrSlice()
			}
			if err != nil {
				return []RunningScript{}, err
			}
		}
		vals = append(vals, val)

	}
	return vals, err
}
