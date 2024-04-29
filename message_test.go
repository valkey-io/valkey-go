package valkey

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

type wrapped struct {
	msg string
	err error
}

func (e wrapped) Error() string { return e.msg }
func (e wrapped) Unwrap() error { return e.err }

func TestIsValkeyNil(t *testing.T) {
	err := Nil
	if !IsValkeyNil(err) {
		t.Fatal("IsValkeyNil fail")
	}
	if IsValkeyNil(errors.New("other")) {
		t.Fatal("IsValkeyNil fail")
	}
	if err.Error() != "valkey nil message" {
		t.Fatal("IsValkeyNil fail")
	}
	wrappedErr := wrapped{msg: "wrapped", err: Nil}
	if IsValkeyNil(wrappedErr) {
		t.Fatal("IsValkeyNil fail : wrapped error")
	}
}

func TestIsValkeyErr(t *testing.T) {
	err := Nil
	if ret, ok := IsValkeyErr(err); ok || ret != Nil {
		t.Fatal("TestIsValkeyErr fail")
	}
	if ret, ok := IsValkeyErr(nil); ok || ret != nil {
		t.Fatal("TestIsValkeyErr fail")
	}
	if ret, ok := IsValkeyErr(errors.New("other")); ok || ret != nil {
		t.Fatal("TestIsValkeyErr fail")
	}
	if ret, ok := IsValkeyErr(&ValkeyError{typ: '-'}); !ok || ret.typ != '-' {
		t.Fatal("TestIsValkeyErr fail")
	}
	wrappedErr := wrapped{msg: "wrapped", err: Nil}
	if ret, ok := IsValkeyErr(wrappedErr); ok || ret == Nil {
		t.Fatal("TestIsValkeyErr fail : wrapped error")
	}
}

func TestValkeyErrorIsMoved(t *testing.T) {
	for _, c := range []struct {
		err  string
		addr string
	}{
		{err: "MOVED 1 127.0.0.1:1", addr: "127.0.0.1:1"},
		{err: "MOVED 1 [::1]:1", addr: "[::1]:1"},
		{err: "MOVED 1 ::1:1", addr: "[::1]:1"},
	} {
		e := ValkeyError{typ: '-', string: c.err}
		if addr, ok := e.IsMoved(); !ok || addr != c.addr {
			t.Fail()
		}
	}
}

func TestValkeyErrorIsAsk(t *testing.T) {
	for _, c := range []struct {
		err  string
		addr string
	}{
		{err: "ASK 1 127.0.0.1:1", addr: "127.0.0.1:1"},
		{err: "ASK 1 [::1]:1", addr: "[::1]:1"},
		{err: "ASK 1 ::1:1", addr: "[::1]:1"},
	} {
		e := ValkeyError{typ: '-', string: c.err}
		if addr, ok := e.IsAsk(); !ok || addr != c.addr {
			t.Fail()
		}
	}
}

func TestIsValkeyBusyGroup(t *testing.T) {
	err := errors.New("other")
	if IsValkeyBusyGroup(err) {
		t.Fatal("TestIsValkeyBusyGroup fail")
	}

	err = &ValkeyError{string: "BUSYGROUP Consumer Group name already exists"}
	if !IsValkeyBusyGroup(err) {
		t.Fatal("TestIsValkeyBusyGroup fail")
	}
}

//gocyclo:ignore
func TestValkeyResult(t *testing.T) {
	//Add erroneous type
	typeNames['t'] = "t"

	t.Run("ToInt64", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).ToInt64(); err == nil {
			t.Fatal("ToInt64 not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).ToInt64(); err == nil {
			t.Fatal("ToInt64 not failed as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: ':', integer: 1}}).ToInt64(); v != 1 {
			t.Fatal("ToInt64 not get value as expected")
		}
	})

	t.Run("ToBool", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).ToBool(); err == nil {
			t.Fatal("ToBool not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).ToBool(); err == nil {
			t.Fatal("ToBool not failed as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: '#', integer: 1}}).ToBool(); !v {
			t.Fatal("ToBool not get value as expected")
		}
	})

	t.Run("AsBool", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsBool(); err == nil {
			t.Fatal("ToBool not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsBool(); err == nil {
			t.Fatal("ToBool not failed as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: '#', integer: 1}}).AsBool(); !v {
			t.Fatal("ToBool not get value as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: ':', integer: 1}}).AsBool(); !v {
			t.Fatal("ToBool not get value as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: '+', string: "OK"}}).AsBool(); !v {
			t.Fatal("ToBool not get value as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: '$', string: "OK"}}).AsBool(); !v {
			t.Fatal("ToBool not get value as expected")
		}
	})

	t.Run("ToFloat64", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).ToFloat64(); err == nil {
			t.Fatal("ToFloat64 not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).ToFloat64(); err == nil {
			t.Fatal("ToFloat64 not failed as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: ',', string: "0.1"}}).ToFloat64(); v != 0.1 {
			t.Fatal("ToFloat64 not get value as expected")
		}
	})

	t.Run("ToString", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).ToString(); err == nil {
			t.Fatal("ToString not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).ToString(); err == nil {
			t.Fatal("ToString not failed as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: '+', string: "0.1"}}).ToString(); v != "0.1" {
			t.Fatal("ToString not get value as expected")
		}
	})

	t.Run("AsReader", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsReader(); err == nil {
			t.Fatal("AsReader not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsReader(); err == nil {
			t.Fatal("AsReader not failed as expected")
		}
		r, _ := (ValkeyResult{val: ValkeyMessage{typ: '+', string: "0.1"}}).AsReader()
		bs, _ := io.ReadAll(r)
		if !bytes.Equal(bs, []byte("0.1")) {
			t.Fatalf("AsReader not get value as expected %v", bs)
		}
	})

	t.Run("AsBytes", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsBytes(); err == nil {
			t.Fatal("AsBytes not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsBytes(); err == nil {
			t.Fatal("AsBytes not failed as expected")
		}
		bs, _ := (ValkeyResult{val: ValkeyMessage{typ: '+', string: "0.1"}}).AsBytes()
		if !bytes.Equal(bs, []byte("0.1")) {
			t.Fatalf("AsBytes not get value as expected %v", bs)
		}
	})

	t.Run("DecodeJSON", func(t *testing.T) {
		v := map[string]string{}
		if err := (ValkeyResult{err: errors.New("other")}).DecodeJSON(&v); err == nil {
			t.Fatal("DecodeJSON not failed as expected")
		}
		if err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).DecodeJSON(&v); err == nil {
			t.Fatal("DecodeJSON not failed as expected")
		}
		if _ = (ValkeyResult{val: ValkeyMessage{typ: '+', string: `{"k":"v"}`}}).DecodeJSON(&v); v["k"] != "v" {
			t.Fatalf("DecodeJSON not get value as expected %v", v)
		}
	})

	t.Run("AsInt64", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsInt64(); err == nil {
			t.Fatal("AsInt64 not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsInt64(); err == nil {
			t.Fatal("AsInt64 not failed as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: '+', string: "1"}}).AsInt64(); v != 1 {
			t.Fatal("AsInt64 not get value as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: ':', integer: 2}}).AsInt64(); v != 2 {
			t.Fatal("AsInt64 not get value as expected")
		}
	})

	t.Run("AsUint64", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsUint64(); err == nil {
			t.Fatal("AsUint64 not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsUint64(); err == nil {
			t.Fatal("AsUint64 not failed as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: '+', string: "1"}}).AsUint64(); v != 1 {
			t.Fatal("AsUint64 not get value as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: ':', integer: 2}}).AsUint64(); v != 2 {
			t.Fatal("AsUint64 not get value as expected")
		}
	})

	t.Run("AsFloat64", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsFloat64(); err == nil {
			t.Fatal("AsFloat64 not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsFloat64(); err == nil {
			t.Fatal("AsFloat64 not failed as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: '+', string: "1.1"}}).AsFloat64(); v != 1.1 {
			t.Fatal("AsFloat64 not get value as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: ',', string: "2.2"}}).AsFloat64(); v != 2.2 {
			t.Fatal("AsFloat64 not get value as expected")
		}
	})

	t.Run("ToArray", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).ToArray(); err == nil {
			t.Fatal("ToArray not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).ToArray(); err == nil {
			t.Fatal("ToArray not failed as expected")
		}
		values := []ValkeyMessage{{string: "item", typ: '+'}}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: values}}).ToArray(); !reflect.DeepEqual(ret, values) {
			t.Fatal("ToArray not get value as expected")
		}
	})

	t.Run("AsStrSlice", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsStrSlice(); err == nil {
			t.Fatal("AsStrSlice not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsStrSlice(); err == nil {
			t.Fatal("AsStrSlice not failed as expected")
		}
		values := []ValkeyMessage{{string: "item", typ: '+'}}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: values}}).AsStrSlice(); !reflect.DeepEqual(ret, []string{"item"}) {
			t.Fatal("AsStrSlice not get value as expected")
		}
	})

	t.Run("AsIntSlice", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsIntSlice(); err == nil {
			t.Fatal("AsIntSlice not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsIntSlice(); err == nil {
			t.Fatal("AsIntSlice not failed as expected")
		}
		values := []ValkeyMessage{{integer: 2, typ: ':'}}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: values}}).AsIntSlice(); !reflect.DeepEqual(ret, []int64{2}) {
			t.Fatal("AsIntSlice not get value as expected")
		}
		values = []ValkeyMessage{{string: "3", typ: '+'}}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: values}}).AsIntSlice(); !reflect.DeepEqual(ret, []int64{3}) {
			t.Fatal("AsIntSlice not get value as expected")
		}
		values = []ValkeyMessage{{string: "ab", typ: '+'}}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '*', values: values}}).AsIntSlice(); err == nil {
			t.Fatal("AsIntSlice not failed as expected")
		}
	})

	t.Run("AsFloatSlice", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsFloatSlice(); err == nil {
			t.Fatal("AsFloatSlice not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsFloatSlice(); err == nil {
			t.Fatal("AsFloatSlice not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{{string: "fff", typ: ','}}}}).AsFloatSlice(); err == nil {
			t.Fatal("AsFloatSlice not failed as expected")
		}
		values := []ValkeyMessage{{integer: 1, typ: ':'}, {string: "2", typ: '+'}, {string: "3", typ: '$'}, {string: "4", typ: ','}}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: values}}).AsFloatSlice(); !reflect.DeepEqual(ret, []float64{1, 2, 3, 4}) {
			t.Fatal("AsFloatSlice not get value as expected")
		}
	})

	t.Run("AsBoolSlice", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsBoolSlice(); err == nil {
			t.Fatal("AsBoolSlice not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsBoolSlice(); err == nil {
			t.Fatal("AsBoolSlice not failed as expected")
		}
		values := []ValkeyMessage{{integer: 1, typ: ':'}, {string: "0", typ: '+'}, {integer: 1, typ: typeBool}}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: values}}).AsBoolSlice(); !reflect.DeepEqual(ret, []bool{true, false, true}) {
			t.Fatal("AsBoolSlice not get value as expected")
		}
	})

	t.Run("AsMap", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsMap(); err == nil {
			t.Fatal("AsMap not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsMap(); err == nil {
			t.Fatal("AsMap not failed as expected")
		}
		values := []ValkeyMessage{{string: "key", typ: '+'}, {string: "value", typ: '+'}}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: values}}).AsMap(); !reflect.DeepEqual(map[string]ValkeyMessage{
			values[0].string: values[1],
		}, ret) {
			t.Fatal("AsMap not get value as expected")
		}
	})

	t.Run("AsStrMap", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsStrMap(); err == nil {
			t.Fatal("AsStrMap not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsStrMap(); err == nil {
			t.Fatal("AsStrMap not failed as expected")
		}
		values := []ValkeyMessage{{string: "key", typ: '+'}, {string: "value", typ: '+'}}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: values}}).AsStrMap(); !reflect.DeepEqual(map[string]string{
			values[0].string: values[1].string,
		}, ret) {
			t.Fatal("AsStrMap not get value as expected")
		}
	})

	t.Run("AsIntMap", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsIntMap(); err == nil {
			t.Fatal("AsIntMap not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsIntMap(); err == nil {
			t.Fatal("AsIntMap not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{{string: "key", typ: '+'}, {string: "value", typ: '+'}}}}).AsIntMap(); err == nil {
			t.Fatal("AsIntMap not failed as expected")
		}
		values := []ValkeyMessage{{string: "k1", typ: '+'}, {string: "1", typ: '+'}, {string: "k2", typ: '+'}, {integer: 2, typ: ':'}}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: values}}).AsIntMap(); !reflect.DeepEqual(map[string]int64{
			"k1": 1,
			"k2": 2,
		}, ret) {
			t.Fatal("AsIntMap not get value as expected")
		}
	})

	t.Run("ToMap", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).ToMap(); err == nil {
			t.Fatal("ToMap not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).ToMap(); err == nil {
			t.Fatal("ToMap not failed as expected")
		}
		values := []ValkeyMessage{{string: "key", typ: '+'}, {string: "value", typ: '+'}}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '%', values: values}}).ToMap(); !reflect.DeepEqual(map[string]ValkeyMessage{
			values[0].string: values[1],
		}, ret) {
			t.Fatal("ToMap not get value as expected")
		}
	})

	t.Run("ToAny", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).ToAny(); err == nil {
			t.Fatal("ToAny not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).ToAny(); err == nil {
			t.Fatal("ToAny not failed as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '%', values: []ValkeyMessage{{typ: '+', string: "key"}, {typ: ':', integer: 1}}},
			{typ: '%', values: []ValkeyMessage{{typ: '+', string: "nil"}, {typ: '_'}}},
			{typ: '%', values: []ValkeyMessage{{typ: '+', string: "err"}, {typ: '-', string: "err"}}},
			{typ: ',', string: "1.2"},
			{typ: '+', string: "str"},
			{typ: '#', integer: 0},
			{typ: '-', string: "err"},
			{typ: '_'},
		}}}).ToAny(); !reflect.DeepEqual([]any{
			map[string]any{"key": int64(1)},
			map[string]any{"nil": nil},
			map[string]any{"err": &ValkeyError{typ: '-', string: "err"}},
			1.2,
			"str",
			false,
			&ValkeyError{typ: '-', string: "err"},
			nil,
		}, ret) {
			t.Fatal("ToAny not get value as expected")
		}
	})

	t.Run("AsXRangeEntry", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry not failed as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{{string: "id", typ: '+'}, {typ: '*', values: []ValkeyMessage{{typ: '+', string: "a"}, {typ: '+', string: "b"}}}}}}).AsXRangeEntry(); !reflect.DeepEqual(XRangeEntry{
			ID:          "id",
			FieldValues: map[string]string{"a": "b"},
		}, ret) {
			t.Fatal("AsXRangeEntry not get value as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{{string: "id", typ: '+'}, {typ: '_'}}}}).AsXRangeEntry(); !reflect.DeepEqual(XRangeEntry{
			ID:          "id",
			FieldValues: nil,
		}, ret) {
			t.Fatal("AsXRangeEntry not get value as expected")
		}
	})

	t.Run("AsXRange", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsXRange(); err == nil {
			t.Fatal("AsXRange not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsXRange(); err == nil {
			t.Fatal("AsXRange not failed as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{{string: "id1", typ: '+'}, {typ: '*', values: []ValkeyMessage{{typ: '+', string: "a"}, {typ: '+', string: "b"}}}}},
			{typ: '*', values: []ValkeyMessage{{string: "id2", typ: '+'}, {typ: '_'}}},
		}}}).AsXRange(); !reflect.DeepEqual([]XRangeEntry{{
			ID:          "id1",
			FieldValues: map[string]string{"a": "b"},
		}, {
			ID:          "id2",
			FieldValues: nil,
		}}, ret) {
			t.Fatal("AsXRange not get value as expected")
		}
	})

	t.Run("AsXRead", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsXRead(); err == nil {
			t.Fatal("AsXRead not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsXRead(); err == nil {
			t.Fatal("AsXRead not failed as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '%', values: []ValkeyMessage{
			{typ: '+', string: "stream1"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '*', values: []ValkeyMessage{{string: "id1", typ: '+'}, {typ: '*', values: []ValkeyMessage{{typ: '+', string: "a"}, {typ: '+', string: "b"}}}}},
				{typ: '*', values: []ValkeyMessage{{string: "id2", typ: '+'}, {typ: '_'}}},
			}},
			{typ: '+', string: "stream2"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '*', values: []ValkeyMessage{{string: "id3", typ: '+'}, {typ: '*', values: []ValkeyMessage{{typ: '+', string: "c"}, {typ: '+', string: "d"}}}}},
			}},
		}}}).AsXRead(); !reflect.DeepEqual(map[string][]XRangeEntry{
			"stream1": {
				{ID: "id1", FieldValues: map[string]string{"a": "b"}},
				{ID: "id2", FieldValues: nil}},
			"stream2": {
				{ID: "id3", FieldValues: map[string]string{"c": "d"}},
			},
		}, ret) {
			t.Fatal("AsXRead not get value as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "stream1"},
				{typ: '*', values: []ValkeyMessage{
					{typ: '*', values: []ValkeyMessage{{string: "id1", typ: '+'}, {typ: '*', values: []ValkeyMessage{{typ: '+', string: "a"}, {typ: '+', string: "b"}}}}},
					{typ: '*', values: []ValkeyMessage{{string: "id2", typ: '+'}, {typ: '_'}}},
				}},
			}},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "stream2"},
				{typ: '*', values: []ValkeyMessage{
					{typ: '*', values: []ValkeyMessage{{string: "id3", typ: '+'}, {typ: '*', values: []ValkeyMessage{{typ: '+', string: "c"}, {typ: '+', string: "d"}}}}},
				}},
			}},
		}}}).AsXRead(); !reflect.DeepEqual(map[string][]XRangeEntry{
			"stream1": {
				{ID: "id1", FieldValues: map[string]string{"a": "b"}},
				{ID: "id2", FieldValues: nil}},
			"stream2": {
				{ID: "id3", FieldValues: map[string]string{"c": "d"}},
			},
		}, ret) {
			t.Fatal("AsXRead not get value as expected")
		}
	})

	t.Run("AsZScore", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsZScore(); err == nil {
			t.Fatal("AsZScore not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsZScore(); err == nil {
			t.Fatal("AsZScore not failed as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '+', string: "m1"},
			{typ: '+', string: "1"},
		}}}).AsZScore(); !reflect.DeepEqual(ZScore{Member: "m1", Score: 1}, ret) {
			t.Fatal("AsZScore not get value as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '+', string: "m1"},
			{typ: ',', string: "1"},
		}}}).AsZScore(); !reflect.DeepEqual(ZScore{Member: "m1", Score: 1}, ret) {
			t.Fatal("AsZScore not get value as expected")
		}
	})

	t.Run("AsZScores", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsZScores(); err == nil {
			t.Fatal("AsZScores not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsZScores(); err == nil {
			t.Fatal("AsZScores not failed as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '+', string: "m1"},
			{typ: '+', string: "1"},
			{typ: '+', string: "m2"},
			{typ: '+', string: "2"},
		}}}).AsZScores(); !reflect.DeepEqual([]ZScore{
			{Member: "m1", Score: 1},
			{Member: "m2", Score: 2},
		}, ret) {
			t.Fatal("AsZScores not get value as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "m1"},
				{typ: ',', string: "1"},
			}},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "m2"},
				{typ: ',', string: "2"},
			}},
		}}}).AsZScores(); !reflect.DeepEqual([]ZScore{
			{Member: "m1", Score: 1},
			{Member: "m2", Score: 2},
		}, ret) {
			t.Fatal("AsZScores not get value as expected")
		}
	})

	t.Run("AsLMPop", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsLMPop(); err == nil {
			t.Fatal("AsLMPop not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsLMPop(); err == nil {
			t.Fatal("AsLMPop not failed as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '+', string: "k"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "1"},
				{typ: '+', string: "2"},
			}},
		}}}).AsLMPop(); !reflect.DeepEqual(KeyValues{
			Key:    "k",
			Values: []string{"1", "2"},
		}, ret) {
			t.Fatal("AsZScores not get value as expected")
		}
	})

	t.Run("AsZMPop", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsZMPop(); err == nil {
			t.Fatal("AsZMPop not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsZMPop(); err == nil {
			t.Fatal("AsZMPop not failed as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '+', string: "k"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '*', values: []ValkeyMessage{
					{typ: '+', string: "1"},
					{typ: ',', string: "1"},
				}},
				{typ: '*', values: []ValkeyMessage{
					{typ: '+', string: "2"},
					{typ: ',', string: "2"},
				}},
			}},
		}}}).AsZMPop(); !reflect.DeepEqual(KeyZScores{
			Key: "k",
			Values: []ZScore{
				{Member: "1", Score: 1},
				{Member: "2", Score: 2},
			},
		}, ret) {
			t.Fatal("AsZMPop not get value as expected")
		}
	})

	t.Run("AsFtSearch", func(t *testing.T) {
		if _, _, err := (ValkeyResult{err: errors.New("other")}).AsFtSearch(); err == nil {
			t.Fatal("AsFtSearch not failed as expected")
		}
		if _, _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsFtSearch(); err == nil {
			t.Fatal("AsFtSearch not failed as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
			{typ: '+', string: "a"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "k1"},
				{typ: '+', string: "v1"},
				{typ: '+', string: "kk"},
				{typ: '+', string: "vv"},
			}},
			{typ: '+', string: "b"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "k2"},
				{typ: '+', string: "v2"},
				{typ: '+', string: "kk"},
				{typ: '+', string: "vv"},
			}},
		}}}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: map[string]string{"k1": "v1", "kk": "vv"}},
			{Key: "b", Doc: map[string]string{"k2": "v2", "kk": "vv"}},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
			{typ: '+', string: "a"},
			{typ: '+', string: "1"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "k1"},
				{typ: '+', string: "v1"},
			}},
			{typ: '+', string: "b"},
			{typ: '+', string: "2"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "k2"},
				{typ: '+', string: "v2"},
			}},
		}}}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: map[string]string{"k1": "v1"}, Score: 1},
			{Key: "b", Doc: map[string]string{"k2": "v2"}, Score: 2},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
			{typ: '+', string: "a"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "k1"},
				{typ: '+', string: "v1"},
				{typ: '+', string: "kk"},
				{typ: '+', string: "vv"},
			}},
		}}}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: map[string]string{"k1": "v1", "kk": "vv"}},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
			{typ: '+', string: "a"},
			{typ: '+', string: "b"},
		}}}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: nil},
			{Key: "b", Doc: nil},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
			{typ: '+', string: "a"},
			{typ: '+', string: "1"},
			{typ: '+', string: "b"},
			{typ: '+', string: "2"},
		}}}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: nil, Score: 1},
			{Key: "b", Doc: nil, Score: 2},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
			{typ: '+', string: "1"},
			{typ: '+', string: "2"},
		}}}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "1", Doc: nil},
			{Key: "2", Doc: nil},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
			{typ: '+', string: "a"},
		}}}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: nil},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
		}}}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
	})

	t.Run("AsFtSearch RESP3", func(t *testing.T) {
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '%', values: []ValkeyMessage{
			{typ: '+', string: "total_results"},
			{typ: ':', integer: 3},
			{typ: '+', string: "results"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '%', values: []ValkeyMessage{
					{typ: '+', string: "id"},
					{typ: '+', string: "1"},
					{typ: '+', string: "extra_attributes"},
					{typ: '%', values: []ValkeyMessage{
						{typ: '+', string: "$"},
						{typ: '+', string: "1"},
					}},
					{typ: '+', string: "score"},
					{typ: ',', string: "1"},
				}},
				{typ: '%', values: []ValkeyMessage{
					{typ: '+', string: "id"},
					{typ: '+', string: "2"},
					{typ: '+', string: "extra_attributes"},
					{typ: '%', values: []ValkeyMessage{
						{typ: '+', string: "$"},
						{typ: '+', string: "2"},
					}},
					{typ: '+', string: "score"},
					{typ: ',', string: "2"},
				}},
			}},
			{typ: '+', string: "error"},
			{typ: '*', values: []ValkeyMessage{}},
		}}}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "1", Doc: map[string]string{"$": "1"}, Score: 1},
			{Key: "2", Doc: map[string]string{"$": "2"}, Score: 2},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if _, _, err := (ValkeyResult{val: ValkeyMessage{typ: '%', values: []ValkeyMessage{
			{typ: '+', string: "total_results"},
			{typ: ':', integer: 3},
			{typ: '+', string: "results"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '%', values: []ValkeyMessage{
					{typ: '+', string: "id"},
					{typ: '+', string: "1"},
					{typ: '+', string: "extra_attributes"},
					{typ: '%', values: []ValkeyMessage{
						{typ: '+', string: "$"},
						{typ: '+', string: "1"},
					}},
					{typ: '+', string: "score"},
					{typ: ',', string: "1"},
				}},
			}},
			{typ: '+', string: "error"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "mytimeout"},
			}},
		}}}).AsFtSearch(); err == nil || err.Error() != "mytimeout" {
			t.Fatal("AsFtSearch not get value as expected")
		}
	})

	t.Run("AsFtAggregate", func(t *testing.T) {
		if _, _, err := (ValkeyResult{err: errors.New("other")}).AsFtAggregate(); err == nil {
			t.Fatal("AsFtAggregate not failed as expected")
		}
		if _, _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsFtAggregate(); err == nil {
			t.Fatal("AsFtAggregate not failed as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "k1"},
				{typ: '+', string: "v1"},
				{typ: '+', string: "kk"},
				{typ: '+', string: "vv"},
			}},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "k2"},
				{typ: '+', string: "v2"},
				{typ: '+', string: "kk"},
				{typ: '+', string: "vv"},
			}},
		}}}).AsFtAggregate(); n != 3 || !reflect.DeepEqual([]map[string]string{
			{"k1": "v1", "kk": "vv"},
			{"k2": "v2", "kk": "vv"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "k1"},
				{typ: '+', string: "v1"},
				{typ: '+', string: "kk"},
				{typ: '+', string: "vv"},
			}},
		}}}).AsFtAggregate(); n != 3 || !reflect.DeepEqual([]map[string]string{
			{"k1": "v1", "kk": "vv"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: ':', integer: 3},
		}}}).AsFtAggregate(); n != 3 || !reflect.DeepEqual([]map[string]string{}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
	})

	t.Run("AsFtAggregate RESP3", func(t *testing.T) {
		if n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '%', values: []ValkeyMessage{
			{typ: '+', string: "total_results"},
			{typ: ':', integer: 3},
			{typ: '+', string: "results"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '%', values: []ValkeyMessage{
					{typ: '+', string: "extra_attributes"},
					{typ: '%', values: []ValkeyMessage{
						{typ: '+', string: "$"},
						{typ: '+', string: "1"},
					}},
				}},
				{typ: '%', values: []ValkeyMessage{
					{typ: '+', string: "extra_attributes"},
					{typ: '%', values: []ValkeyMessage{
						{typ: '+', string: "$"},
						{typ: '+', string: "2"},
					}},
				}},
			}},
			{typ: '+', string: "error"},
			{typ: '*', values: []ValkeyMessage{}},
		}}}).AsFtAggregate(); n != 3 || !reflect.DeepEqual([]map[string]string{
			{"$": "1"},
			{"$": "2"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if _, _, err := (ValkeyResult{val: ValkeyMessage{typ: '%', values: []ValkeyMessage{
			{typ: '+', string: "total_results"},
			{typ: ':', integer: 3},
			{typ: '+', string: "results"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '%', values: []ValkeyMessage{
					{typ: '+', string: "extra_attributes"},
					{typ: '%', values: []ValkeyMessage{
						{typ: '+', string: "$"},
						{typ: '+', string: "1"},
					}},
				}},
			}},
			{typ: '+', string: "error"},
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "mytimeout"},
			}},
		}}}).AsFtAggregate(); err == nil || err.Error() != "mytimeout" {
			t.Fatal("AsFtAggregate not get value as expected")
		}
	})

	t.Run("AsFtAggregate Cursor", func(t *testing.T) {
		if _, _, _, err := (ValkeyResult{err: errors.New("other")}).AsFtAggregateCursor(); err == nil {
			t.Fatal("AsFtAggregate not failed as expected")
		}
		if _, _, _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsFtAggregateCursor(); err == nil {
			t.Fatal("AsFtAggregate not failed as expected")
		}
		if c, n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: ':', integer: 3},
				{typ: '*', values: []ValkeyMessage{
					{typ: '+', string: "k1"},
					{typ: '+', string: "v1"},
					{typ: '+', string: "kk"},
					{typ: '+', string: "vv"},
				}},
				{typ: '*', values: []ValkeyMessage{
					{typ: '+', string: "k2"},
					{typ: '+', string: "v2"},
					{typ: '+', string: "kk"},
					{typ: '+', string: "vv"},
				}},
			}},
			{typ: ':', integer: 1},
		}}}).AsFtAggregateCursor(); c != 1 || n != 3 || !reflect.DeepEqual([]map[string]string{
			{"k1": "v1", "kk": "vv"},
			{"k2": "v2", "kk": "vv"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if c, n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: ':', integer: 3},
				{typ: '*', values: []ValkeyMessage{
					{typ: '+', string: "k1"},
					{typ: '+', string: "v1"},
					{typ: '+', string: "kk"},
					{typ: '+', string: "vv"},
				}},
			}},
			{typ: ':', integer: 1},
		}}}).AsFtAggregateCursor(); c != 1 || n != 3 || !reflect.DeepEqual([]map[string]string{
			{"k1": "v1", "kk": "vv"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if c, n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: ':', integer: 3},
			}},
			{typ: ':', integer: 1},
		}}}).AsFtAggregateCursor(); c != 1 || n != 3 || !reflect.DeepEqual([]map[string]string{}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
	})

	t.Run("AsFtAggregate Cursor RESP3", func(t *testing.T) {
		if c, n, ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '%', values: []ValkeyMessage{
				{typ: '+', string: "total_results"},
				{typ: ':', integer: 3},
				{typ: '+', string: "results"},
				{typ: '*', values: []ValkeyMessage{
					{typ: '%', values: []ValkeyMessage{
						{typ: '+', string: "extra_attributes"},
						{typ: '%', values: []ValkeyMessage{
							{typ: '+', string: "$"},
							{typ: '+', string: "1"},
						}},
					}},
					{typ: '%', values: []ValkeyMessage{
						{typ: '+', string: "extra_attributes"},
						{typ: '%', values: []ValkeyMessage{
							{typ: '+', string: "$"},
							{typ: '+', string: "2"},
						}},
					}},
				}},
				{typ: '+', string: "error"},
				{typ: '*', values: []ValkeyMessage{}},
			}},
			{typ: ':', integer: 1},
		}}}).AsFtAggregateCursor(); c != 1 || n != 3 || !reflect.DeepEqual([]map[string]string{
			{"$": "1"},
			{"$": "2"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if _, _, _, err := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '%', values: []ValkeyMessage{
				{typ: '+', string: "total_results"},
				{typ: ':', integer: 3},
				{typ: '+', string: "results"},
				{typ: '*', values: []ValkeyMessage{
					{typ: '%', values: []ValkeyMessage{
						{typ: '+', string: "extra_attributes"},
						{typ: '%', values: []ValkeyMessage{
							{typ: '+', string: "$"},
							{typ: '+', string: "1"},
						}},
					}},
				}},
				{typ: '+', string: "error"},
				{typ: '*', values: []ValkeyMessage{
					{typ: '+', string: "mytimeout"},
				}},
			}},
			{typ: ':', integer: 1},
		}}}).AsFtAggregateCursor(); err == nil || err.Error() != "mytimeout" {
			t.Fatal("AsFtAggregate not get value as expected")
		}
	})

	t.Run("AsGeosearch", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsGeosearch(); err == nil {
			t.Fatal("AsGeosearch not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsGeosearch(); err == nil {
			t.Fatal("AsGeosearch not failed as expected")
		}
		//WithDist, WithHash, WithCoord
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k1"},
				{typ: ',', string: "2.5"},
				{typ: ':', integer: 1},
				{typ: '*', values: []ValkeyMessage{
					{typ: ',', string: "28.0473"},
					{typ: ',', string: "26.2041"},
				}},
			}},
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k2"},
				{typ: ',', string: "4.5"},
				{typ: ':', integer: 4},
				{typ: '*', values: []ValkeyMessage{
					{typ: ',', string: "72.4973"},
					{typ: ',', string: "13.2263"},
				}},
			}},
		}}}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", Dist: 2.5, GeoHash: 1, Longitude: 28.0473, Latitude: 26.2041},
			{Name: "k2", Dist: 4.5, GeoHash: 4, Longitude: 72.4973, Latitude: 13.2263},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//WithHash, WithCoord
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k1"},
				{typ: ':', integer: 1},
				{typ: '*', values: []ValkeyMessage{
					{typ: ',', string: "84.3877"},
					{typ: ',', string: "33.7488"},
				}},
			}},
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k2"},
				{typ: ':', integer: 4},
				{typ: '*', values: []ValkeyMessage{
					{typ: ',', string: "115.8613"},
					{typ: ',', string: "31.9523"},
				}},
			}},
		}}}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", GeoHash: 1, Longitude: 84.3877, Latitude: 33.7488},
			{Name: "k2", GeoHash: 4, Longitude: 115.8613, Latitude: 31.9523},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//WithDist, WithCoord
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k1"},
				{typ: ',', string: "2.50076"},
				{typ: '*', values: []ValkeyMessage{
					{typ: ',', string: "84.3877"},
					{typ: ',', string: "33.7488"},
				}},
			}},
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k2"},
				{typ: ',', string: "1024.96"},
				{typ: '*', values: []ValkeyMessage{
					{typ: ',', string: "115.8613"},
					{typ: ',', string: "31.9523"},
				}},
			}},
		}}}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", Dist: 2.50076, Longitude: 84.3877, Latitude: 33.7488},
			{Name: "k2", Dist: 1024.96, Longitude: 115.8613, Latitude: 31.9523},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//WithCoord
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k1"},
				{typ: '*', values: []ValkeyMessage{
					{typ: ',', string: "122.4194"},
					{typ: ',', string: "37.7749"},
				}},
			}},
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k2"},
				{typ: '*', values: []ValkeyMessage{
					{typ: ',', string: "35.6762"},
					{typ: ',', string: "139.6503"},
				}},
			}},
		}}}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", Longitude: 122.4194, Latitude: 37.7749},
			{Name: "k2", Longitude: 35.6762, Latitude: 139.6503},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//WithDist
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k1"},
				{typ: ',', string: "2.50076"},
			}},
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k2"},
				{typ: ',', string: strconv.FormatFloat(math.MaxFloat64, 'E', -1, 64)},
			}},
		}}}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", Dist: 2.50076},
			{Name: "k2", Dist: math.MaxFloat64},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//WithHash
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k1"},
				{typ: ':', integer: math.MaxInt64},
			}},
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k2"},
				{typ: ':', integer: 22296},
			}},
		}}}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", GeoHash: math.MaxInt64},
			{Name: "k2", GeoHash: 22296},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//With no additional options
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '$', string: "k1"},
			{typ: '$', string: "k2"},
		}}}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1"},
			{Name: "k2"},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//With wrong distance
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k1"},
				{typ: ',', string: "wrong distance"},
			}},
		}}}).AsGeosearch(); err == nil {
			t.Fatal("AsGeosearch not failed as expected")
		}
		//With wrong coordinates
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '$', string: "k2"},
				{typ: '*', values: []ValkeyMessage{
					{typ: ',', string: "35.6762"},
				}},
			}},
		}}}).AsGeosearch(); err == nil {
			t.Fatal("AsGeosearch not failed as expected")
		}
	})

	t.Run("IsCacheHit", func(t *testing.T) {
		if (ValkeyResult{err: errors.New("other")}).IsCacheHit() {
			t.Fatal("IsCacheHit not as expected")
		}
		if !(ValkeyResult{val: ValkeyMessage{attrs: cacheMark}}).IsCacheHit() {
			t.Fatal("IsCacheHit not as expected")
		}
	})

	t.Run("CacheTTL", func(t *testing.T) {
		if (ValkeyResult{err: errors.New("other")}).CacheTTL() != -1 {
			t.Fatal("CacheTTL != -1")
		}
		m := ValkeyMessage{}
		m.setExpireAt(time.Now().Add(time.Millisecond * 100).UnixMilli())
		if (ValkeyResult{val: m}).CacheTTL() <= 0 {
			t.Fatal("CacheTTL <= 0")
		}
		time.Sleep(150 * time.Millisecond)
		if (ValkeyResult{val: m}).CacheTTL() != 0 {
			t.Fatal("CacheTTL != 0")
		}
	})

	t.Run("CachePTTL", func(t *testing.T) {
		if (ValkeyResult{err: errors.New("other")}).CachePTTL() != -1 {
			t.Fatal("CachePTTL != -1")
		}
		m := ValkeyMessage{}
		m.setExpireAt(time.Now().Add(time.Millisecond * 100).UnixMilli())
		if (ValkeyResult{val: m}).CachePTTL() <= 0 {
			t.Fatal("CachePTTL <= 0")
		}
		time.Sleep(150 * time.Millisecond)
		if (ValkeyResult{val: m}).CachePTTL() != 0 {
			t.Fatal("CachePTTL != 0")
		}
	})

	t.Run("CachePXAT", func(t *testing.T) {
		if (ValkeyResult{err: errors.New("other")}).CachePXAT() != -1 {
			t.Fatal("CachePTTL != -1")
		}
		m := ValkeyMessage{}
		m.setExpireAt(time.Now().Add(time.Millisecond * 100).UnixMilli())
		if (ValkeyResult{val: m}).CachePXAT() <= 0 {
			t.Fatal("CachePXAT <= 0")
		}
	})

	t.Run("Stringer", func(t *testing.T) {
		tests := []struct {
			input    ValkeyResult
			expected string
		}{
			{
				input: ValkeyResult{
					val: ValkeyMessage{typ: '*', values: []ValkeyMessage{
						{typ: '*', values: []ValkeyMessage{
							{typ: ':', integer: 0},
							{typ: ':', integer: 0},
							{typ: '*', values: []ValkeyMessage{ // master
								{typ: '+', string: "127.0.3.1"},
								{typ: ':', integer: 3},
								{typ: '+', string: ""},
							}},
						}},
					}},
				},
				expected: `{"Message":{"Value":[{"Value":[{"Value":0,"Type":"int64"},{"Value":0,"Type":"int64"},{"Value":[{"Value":"127.0.3.1","Type":"simple string"},{"Value":3,"Type":"int64"},{"Value":"","Type":"simple string"}],"Type":"array"}],"Type":"array"}],"Type":"array"}}`,
			},
			{
				input:    ValkeyResult{err: errors.New("foo")},
				expected: `{"Error":"foo"}`,
			},
		}
		for _, test := range tests {
			msg := test.input.String()
			if msg != test.expected {
				t.Fatalf("unexpected string. got %v expected %v", msg, test.expected)
			}
		}
	})
}

//gocyclo:ignore
func TestValkeyMessage(t *testing.T) {
	//Add erroneous type
	typeNames['t'] = "t"

	t.Run("IsNil", func(t *testing.T) {
		if !(&ValkeyMessage{typ: '_'}).IsNil() {
			t.Fatal("IsNil fail")
		}
	})
	t.Run("Trim ERR prefix", func(t *testing.T) {
		// kvrocks: https://github.com/redis/rueidis/issues/152#issuecomment-1333923750
		if (&ValkeyMessage{typ: '-', string: "ERR no_prefix"}).Error().Error() != "no_prefix" {
			t.Fatal("fail to trim ERR")
		}
	})
	t.Run("ToInt64", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).ToInt64(); err == nil {
			t.Fatal("ToInt64 not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a RESP3 int64") {
				t.Fatal("ToInt64 not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).ToInt64()
	})

	t.Run("ToBool", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).ToBool(); err == nil {
			t.Fatal("ToBool not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a RESP3 bool") {
				t.Fatal("ToBool not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).ToBool()
	})

	t.Run("AsBool", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsBool(); err == nil {
			t.Fatal("AsBool not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a int, string or bool") {
				t.Fatal("AsBool not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).AsBool()
	})

	t.Run("ToFloat64", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).ToFloat64(); err == nil {
			t.Fatal("ToFloat64 not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a RESP3 float64") {
				t.Fatal("ToFloat64 not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).ToFloat64()
	})

	t.Run("ToString", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).ToString(); err == nil {
			t.Fatal("ToString not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
				t.Fatal("ToString not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: ':'}).ToString()
	})

	t.Run("AsReader", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsReader(); err == nil {
			t.Fatal("AsReader not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
				t.Fatal("AsReader not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: ':'}).AsReader()
	})

	t.Run("AsBytes", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsBytes(); err == nil {
			t.Fatal("AsBytes not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
				t.Fatal("AsBytes not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: ':'}).AsBytes()
	})

	t.Run("DecodeJSON", func(t *testing.T) {
		if err := (&ValkeyMessage{typ: '_'}).DecodeJSON(nil); err == nil {
			t.Fatal("DecodeJSON not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
				t.Fatal("DecodeJSON not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: ':'}).DecodeJSON(nil)
	})

	t.Run("AsInt64", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsInt64(); err == nil {
			t.Fatal("AsInt64 not failed as expected")
		}
		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a string", typeNames['*'])) {
				t.Fatal("AsInt64 not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*', values: []ValkeyMessage{{}}}).AsInt64()
	})

	t.Run("AsUint64", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsUint64(); err == nil {
			t.Fatal("AsUint64 not failed as expected")
		}
		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a string", typeNames['*'])) {
				t.Fatal("AsUint64 not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*', values: []ValkeyMessage{{}}}).AsUint64()
	})

	t.Run("AsFloat64", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsFloat64(); err == nil {
			t.Fatal("AsFloat64 not failed as expected")
		}
		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
				t.Fatal("AsFloat64 not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: ':'}).AsFloat64()
	})

	t.Run("ToArray", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).ToArray(); err == nil {
			t.Fatal("ToArray not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a array") {
				t.Fatal("ToArray not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).ToArray()
	})

	t.Run("AsStrSlice", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsStrSlice(); err == nil {
			t.Fatal("AsStrSlice not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a array") {
				t.Fatal("AsStrSlice not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).AsStrSlice()
	})

	t.Run("AsIntSlice", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsIntSlice(); err == nil {
			t.Fatal("AsIntSlice not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a array") {
				t.Fatal("AsIntSlice not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).AsIntSlice()
	})

	t.Run("AsFloatSlice", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsFloatSlice(); err == nil {
			t.Fatal("AsFloatSlice not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a array") {
				t.Fatal("AsFloatSlice not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).AsFloatSlice()
	})

	t.Run("AsBoolSlice", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsBoolSlice(); err == nil {
			t.Fatal("AsBoolSlice not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a array") {
				t.Fatal("AsBoolSlice not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).AsBoolSlice()
	})

	t.Run("AsMap", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsMap(); err == nil {
			t.Fatal("AsMap not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a map/array/set or its length is not even") {
				t.Fatal("AsMap not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).AsMap()
	})

	t.Run("AsStrMap", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsStrMap(); err == nil {
			t.Fatal("AsStrMap not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a map/array/set") {
				t.Fatal("AsMap not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).AsStrMap()
	})

	t.Run("AsIntMap", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsIntMap(); err == nil {
			t.Fatal("AsIntMap not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a map/array/set") {
				t.Fatal("AsMap not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).AsIntMap()
	})

	t.Run("ToMap", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).ToMap(); err == nil {
			t.Fatal("ToMap not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a RESP3 map") {
				t.Fatal("ToMap not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).ToMap()
	})

	t.Run("ToAny", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).ToAny(); err == nil {
			t.Fatal("ToAny not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a supported in ToAny") {
				t.Fatal("ToAny not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).ToAny()
	})

	t.Run("AsXRangeEntry - no range id", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry not failed as expected")
		}

		if _, err := (&ValkeyMessage{typ: '*'}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry not failed as expected")
		}

		if _, err := (&ValkeyMessage{typ: '*', values: []ValkeyMessage{{typ: '_'}, {typ: '%'}}}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
				t.Fatal("AsXRangeEntry not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*', values: []ValkeyMessage{{typ: ':'}, {typ: '%'}}}).AsXRangeEntry()
	})

	t.Run("AsXRangeEntry - no range field values", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry not failed as expected")
		}

		if _, err := (&ValkeyMessage{typ: '*'}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry not failed as expected")
		}

		if _, err := (&ValkeyMessage{typ: '*', values: []ValkeyMessage{{typ: '+'}, {typ: '-'}}}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a map/array/set") {
				t.Fatal("AsXRangeEntry not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*', values: []ValkeyMessage{{typ: '+'}, {typ: 't'}}}).AsXRangeEntry()
	})

	t.Run("AsXRange", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsXRange(); err == nil {
			t.Fatal("AsXRange not failed as expected")
		}

		if _, err := (&ValkeyMessage{typ: '*', values: []ValkeyMessage{{typ: '_'}}}).AsXRange(); err == nil {
			t.Fatal("AsXRange not failed as expected")
		}
	})

	t.Run("AsXRead", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsXRead(); err == nil {
			t.Fatal("AsXRead not failed as expected")
		}
		if _, err := (&ValkeyMessage{typ: '%', values: []ValkeyMessage{
			{typ: '+', string: "stream1"},
			{typ: '*', values: []ValkeyMessage{{typ: '*', values: []ValkeyMessage{{string: "id1", typ: '+'}}}}},
		}}).AsXRead(); err == nil {
			t.Fatal("AsXRead not failed as expected")
		}
		if _, err := (&ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "stream1"},
			}},
		}}).AsXRead(); err == nil {
			t.Fatal("AsXRead not failed as expected")
		}
		if _, err := (&ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "stream1"},
				{typ: '*', values: []ValkeyMessage{{typ: '*', values: []ValkeyMessage{{string: "id1", typ: '+'}}}}},
			}},
		}}).AsXRead(); err == nil {
			t.Fatal("AsXRead not failed as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), "valkey message type t is not a map/array/set") {
				t.Fatal("AsXRangeEntry not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: 't'}).AsXRead()
	})

	t.Run("AsZScore", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsZScore(); err == nil {
			t.Fatal("AsZScore not failed as expected")
		}
		defer func() {
			if !strings.Contains(recover().(string), "valkey message is not a map/array/set or its length is not 2") {
				t.Fatal("AsZScore not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*'}).AsZScore()
	})

	t.Run("AsZScores", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsZScores(); err == nil {
			t.Fatal("AsZScore not failed as expected")
		}
		if _, err := (&ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '+', string: "m1"},
			{typ: '+', string: "m2"},
		}}).AsZScores(); err == nil {
			t.Fatal("AsZScores not fails as expected")
		}
		if _, err := (&ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '*', values: []ValkeyMessage{
				{typ: '+', string: "m1"},
				{typ: '+', string: "m2"},
			}},
		}}).AsZScores(); err == nil {
			t.Fatal("AsZScores not fails as expected")
		}
	})

	t.Run("AsLMPop", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsLMPop(); err == nil {
			t.Fatal("AsLMPop not failed as expected")
		}
		if _, err := (&ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '+', string: "k"},
			{typ: '_'},
		}}).AsLMPop(); err == nil {
			t.Fatal("AsLMPop not fails as expected")
		}
		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a LMPOP response", typeNames['*'])) {
				t.Fatal("AsLMPop not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '+', string: "k"},
		}}).AsLMPop()
	})

	t.Run("AsZMPop", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsZMPop(); err == nil {
			t.Fatal("AsZMPop not failed as expected")
		}
		if _, err := (&ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '+', string: "k"},
			{typ: '_'},
		}}).AsZMPop(); err == nil {
			t.Fatal("AsZMPop not fails as expected")
		}
		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a ZMPOP response", typeNames['*'])) {
				t.Fatal("AsZMPop not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*', values: []ValkeyMessage{
			{typ: '+', string: "k"},
		}}).AsZMPop()
	})

	t.Run("AsFtSearch", func(t *testing.T) {
		if _, _, err := (&ValkeyMessage{typ: '_'}).AsFtSearch(); err == nil {
			t.Fatal("AsFtSearch not failed as expected")
		}
		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a FT.SEARCH response", typeNames['*'])) {
				t.Fatal("AsFtSearch not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*', values: []ValkeyMessage{}}).AsFtSearch()
	})

	t.Run("AsFtAggregate", func(t *testing.T) {
		if _, _, err := (&ValkeyMessage{typ: '_'}).AsFtAggregate(); err == nil {
			t.Fatal("AsFtAggregate not failed as expected")
		}
		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a FT.AGGREGATE response", typeNames['*'])) {
				t.Fatal("AsFtAggregate not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*', values: []ValkeyMessage{}}).AsFtAggregate()
	})

	t.Run("AsFtAggregateCursor", func(t *testing.T) {
		if _, _, _, err := (&ValkeyMessage{typ: '_'}).AsFtAggregateCursor(); err == nil {
			t.Fatal("AsFtAggregate not failed as expected")
		}
		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a FT.AGGREGATE response", typeNames['*'])) {
				t.Fatal("AsFtAggregate not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*', values: []ValkeyMessage{}}).AsFtAggregateCursor()
	})

	t.Run("AsScanEntry", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsScanEntry(); err == nil {
			t.Fatal("AsScanEntry not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsScanEntry(); err == nil {
			t.Fatal("AsScanEntry not failed as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{{string: "1", typ: '+'}, {typ: '*', values: []ValkeyMessage{{typ: '+', string: "a"}, {typ: '+', string: "b"}}}}}}).AsScanEntry(); !reflect.DeepEqual(ScanEntry{
			Cursor:   1,
			Elements: []string{"a", "b"},
		}, ret) {
			t.Fatal("AsScanEntry not get value as expected")
		}
		if ret, _ := (ValkeyResult{val: ValkeyMessage{typ: '*', values: []ValkeyMessage{{string: "0", typ: '+'}, {typ: '_'}}}}).AsScanEntry(); !reflect.DeepEqual(ScanEntry{}, ret) {
			t.Fatal("AsScanEntry not get value as expected")
		}

		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s is not a scan response or its length is not at least 2", typeNames['*'])) {
				t.Fatal("AsScanEntry not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '*', values: []ValkeyMessage{{typ: ':'}}}).AsScanEntry()
	})

	t.Run("ToMap with non string key", func(t *testing.T) {
		defer func() {
			if !strings.Contains(recover().(string), fmt.Sprintf("valkey message type %s as map key is not supported", typeNames[':'])) {
				t.Fatal("ToMap not panic as expected")
			}
		}()
		(&ValkeyMessage{typ: '%', values: []ValkeyMessage{{typ: ':'}, {typ: ':'}}}).ToMap()
	})

	t.Run("IsCacheHit", func(t *testing.T) {
		if (&ValkeyMessage{typ: '_'}).IsCacheHit() {
			t.Fatal("IsCacheHit not as expected")
		}
		if !(&ValkeyMessage{typ: '_', attrs: cacheMark}).IsCacheHit() {
			t.Fatal("IsCacheHit not as expected")
		}
	})

	t.Run("CacheTTL", func(t *testing.T) {
		if (&ValkeyMessage{typ: '_'}).CacheTTL() != -1 {
			t.Fatal("CacheTTL != -1")
		}
		m := &ValkeyMessage{typ: '_'}
		m.setExpireAt(time.Now().Add(time.Millisecond * 100).UnixMilli())
		if m.CacheTTL() <= 0 {
			t.Fatal("CacheTTL <= 0")
		}
		time.Sleep(100 * time.Millisecond)
		if m.CachePTTL() > 0 {
			t.Fatal("CachePTTL > 0")
		}
	})

	t.Run("CachePTTL", func(t *testing.T) {
		if (&ValkeyMessage{typ: '_'}).CachePTTL() != -1 {
			t.Fatal("CachePTTL != -1")
		}
		m := &ValkeyMessage{typ: '_'}
		m.setExpireAt(time.Now().Add(time.Millisecond * 100).UnixMilli())
		if m.CachePTTL() <= 0 {
			t.Fatal("CachePTTL <= 0")
		}
		time.Sleep(100 * time.Millisecond)
		if m.CachePTTL() > 0 {
			t.Fatal("CachePTTL > 0")
		}
	})

	t.Run("CachePXAT", func(t *testing.T) {
		if (&ValkeyMessage{typ: '_'}).CachePXAT() != -1 {
			t.Fatal("CachePXAT != -1")
		}
		m := &ValkeyMessage{typ: '_'}
		m.setExpireAt(time.Now().Add(time.Millisecond * 100).UnixMilli())
		if m.CachePXAT() <= 0 {
			t.Fatal("CachePXAT <= 0")
		}
	})

	t.Run("Stringer", func(t *testing.T) {
		tests := []struct {
			input    ValkeyMessage
			expected string
		}{
			{
				input: ValkeyMessage{typ: '*', values: []ValkeyMessage{
					{typ: '*', values: []ValkeyMessage{
						{typ: ':', integer: 0},
						{typ: ':', integer: 0},
						{typ: '*', values: []ValkeyMessage{
							{typ: '+', string: "127.0.3.1"},
							{typ: ':', integer: 3},
							{typ: '+', string: ""},
						}},
					}},
				}},
				expected: `{"Value":[{"Value":[{"Value":0,"Type":"int64"},{"Value":0,"Type":"int64"},{"Value":[{"Value":"127.0.3.1","Type":"simple string"},{"Value":3,"Type":"int64"},{"Value":"","Type":"simple string"}],"Type":"array"}],"Type":"array"}],"Type":"array"}`,
			},
			{
				input:    ValkeyMessage{typ: '+', string: "127.0.3.1", ttl: [7]byte{97, 77, 74, 61, 138, 1, 0}},
				expected: `{"Value":"127.0.3.1","Type":"simple string","TTL":"2023-08-28 17:56:34.273 +0000 UTC"}`,
			},
			{
				input:    ValkeyMessage{typ: '0'},
				expected: `{"Type":"unknown"}`,
			},
			{
				input:    ValkeyMessage{typ: typeBool, integer: 1},
				expected: `{"Value":true,"Type":"boolean"}`,
			},
			{
				input:    ValkeyMessage{typ: typeNull},
				expected: `{"Type":"null","Error":"valkey nil message"}`,
			},
			{
				input:    ValkeyMessage{typ: typeSimpleErr, string: "ERR foo"},
				expected: `{"Type":"simple error","Error":"foo"}`,
			},
			{
				input:    ValkeyMessage{typ: typeBlobErr, string: "ERR foo"},
				expected: `{"Type":"blob error","Error":"foo"}`,
			},
		}
		for _, test := range tests {
			msg := test.input.String()
			if msg != test.expected {
				t.Fatalf("unexpected string. got %v expected %v", msg, test.expected)
			}
		}
	})
}
