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
	"unsafe"
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

func TestIsParseErr(t *testing.T) {
	err := errParse
	if !IsParseErr(err) {
		t.Fatal("IsParseErr fail")
	}
	if IsParseErr(errors.New("other")) {
		t.Fatal("IsParseErr fail")
	}
	if err.Error() != "valkey: parse error" {
		t.Fatal("IsValkeyNil fail")
	}
	wrappedErr := wrapped{msg: "wrapped", err: errParse}
	wrappedNonParseErr := wrapped{msg: "wrapped", err: errors.New("other")}
	if !IsParseErr(wrappedErr) || IsParseErr(wrappedNonParseErr) {
		t.Fatal("IsParseErr fail : wrapped error")
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
		e := ValkeyError(strmsg('-', c.err))
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
		e := ValkeyError(strmsg('-', c.err))
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

	valkeyErr := ValkeyError(strmsg('-', "BUSYGROUP Consumer Group name already exists"))
	err = &valkeyErr
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
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: ':', intlen: 1}}).ToInt64(); v != 1 {
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
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: '#', intlen: 1}}).ToBool(); !v {
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
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: '#', intlen: 1}}).AsBool(); !v {
			t.Fatal("ToBool not get value as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: ':', intlen: 1}}).AsBool(); !v {
			t.Fatal("ToBool not get value as expected")
		}
		if v, _ := (ValkeyResult{val: strmsg('+', "OK")}).AsBool(); !v {
			t.Fatal("ToBool not get value as expected")
		}
		if v, _ := (ValkeyResult{val: strmsg('$', "OK")}).AsBool(); !v {
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
		if v, _ := (ValkeyResult{val: strmsg(',', "0.1")}).ToFloat64(); v != 0.1 {
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
		if v, _ := (ValkeyResult{val: strmsg('+', "0.1")}).ToString(); v != "0.1" {
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
		r, _ := (ValkeyResult{val: strmsg('+', "0.1")}).AsReader()
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
		bs, _ := (ValkeyResult{val: strmsg('+', "0.1")}).AsBytes()
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
		if _ = (ValkeyResult{val: strmsg('+', `{"k":"v"}`)}).DecodeJSON(&v); v["k"] != "v" {
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
		if v, _ := (ValkeyResult{val: strmsg('+', "1")}).AsInt64(); v != 1 {
			t.Fatal("AsInt64 not get value as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: ':', intlen: 2}}).AsInt64(); v != 2 {
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
		if v, _ := (ValkeyResult{val: strmsg('+', "1")}).AsUint64(); v != 1 {
			t.Fatal("AsUint64 not get value as expected")
		}
		if v, _ := (ValkeyResult{val: ValkeyMessage{typ: ':', intlen: 2}}).AsUint64(); v != 2 {
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
		if v, _ := (ValkeyResult{val: strmsg('+', "1.1")}).AsFloat64(); v != 1.1 {
			t.Fatal("AsFloat64 not get value as expected")
		}
		if v, _ := (ValkeyResult{val: strmsg(',', "2.2")}).AsFloat64(); v != 2.2 {
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
		values := []ValkeyMessage{strmsg('+', "item")}
		if ret, _ := (ValkeyResult{val: slicemsg('*', values)}).ToArray(); !reflect.DeepEqual(ret, values) {
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
		values := []ValkeyMessage{strmsg('+', "item")}
		if ret, _ := (ValkeyResult{val: slicemsg('*', values)}).AsStrSlice(); !reflect.DeepEqual(ret, []string{"item"}) {
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
		values := []ValkeyMessage{{intlen: 2, typ: ':'}}
		if ret, _ := (ValkeyResult{val: slicemsg('*', values)}).AsIntSlice(); !reflect.DeepEqual(ret, []int64{2}) {
			t.Fatal("AsIntSlice not get value as expected")
		}
		values = []ValkeyMessage{strmsg('+', "3")}
		if ret, _ := (ValkeyResult{val: slicemsg('*', values)}).AsIntSlice(); !reflect.DeepEqual(ret, []int64{3}) {
			t.Fatal("AsIntSlice not get value as expected")
		}
		values = []ValkeyMessage{strmsg('+', "ab")}
		if _, err := (ValkeyResult{val: slicemsg('*', values)}).AsIntSlice(); err == nil {
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
		if _, err := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg(',', "fff")})}).AsFloatSlice(); err == nil {
			t.Fatal("AsFloatSlice not failed as expected")
		}
		values := []ValkeyMessage{{intlen: 1, typ: ':'}, strmsg('+', "2"), strmsg('$', "3"), strmsg(',', "4")}
		if ret, _ := (ValkeyResult{val: slicemsg('*', values)}).AsFloatSlice(); !reflect.DeepEqual(ret, []float64{1, 2, 3, 4}) {
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
		values := []ValkeyMessage{{intlen: 1, typ: ':'}, strmsg('+', "0"), {intlen: 1, typ: typeBool}}
		if ret, _ := (ValkeyResult{val: slicemsg('*', values)}).AsBoolSlice(); !reflect.DeepEqual(ret, []bool{true, false, true}) {
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
		values := []ValkeyMessage{strmsg('+', "key"), strmsg('+', "value")}
		if ret, _ := (ValkeyResult{val: slicemsg('*', values)}).AsMap(); !reflect.DeepEqual(map[string]ValkeyMessage{
			values[0].string(): values[1],
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
		values := []ValkeyMessage{strmsg('+', "key"), strmsg('+', "value")}
		if ret, _ := (ValkeyResult{val: slicemsg('*', values)}).AsStrMap(); !reflect.DeepEqual(map[string]string{
			values[0].string(): values[1].string(),
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
		if _, err := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "key"), strmsg('+', "value")})}).AsIntMap(); err == nil {
			t.Fatal("AsIntMap not failed as expected")
		}
		values := []ValkeyMessage{strmsg('+', "k1"), strmsg('+', "1"), strmsg('+', "k2"), {intlen: 2, typ: ':'}}
		if ret, _ := (ValkeyResult{val: slicemsg('*', values)}).AsIntMap(); !reflect.DeepEqual(map[string]int64{
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
		values := []ValkeyMessage{strmsg('+', "key"), strmsg('+', "value")}
		if ret, _ := (ValkeyResult{val: slicemsg('%', values)}).ToMap(); !reflect.DeepEqual(map[string]ValkeyMessage{
			values[0].string(): values[1],
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
		valkeyErr := ValkeyError(strmsg('-', "err"))
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('%', []ValkeyMessage{strmsg('+', "key"), {typ: ':', intlen: 1}}),
			slicemsg('%', []ValkeyMessage{strmsg('+', "nil"), {typ: '_'}}),
			slicemsg('%', []ValkeyMessage{strmsg('+', "err"), strmsg('-', "err")}),
			strmsg(',', "1.2"),
			strmsg('+', "str"),
			{typ: '#', intlen: 0},
			strmsg('-', "err"),
			{typ: '_'},
		})}).ToAny(); !reflect.DeepEqual([]any{
			map[string]any{"key": int64(1)},
			map[string]any{"nil": nil},
			map[string]any{"err": &valkeyErr},
			1.2,
			"str",
			false,
			&valkeyErr,
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
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "id"), slicemsg('*', []ValkeyMessage{strmsg('+', "a"), strmsg('+', "b")})})}).AsXRangeEntry(); !reflect.DeepEqual(XRangeEntry{
			ID:          "id",
			FieldValues: map[string]string{"a": "b"},
		}, ret) {
			t.Fatal("AsXRangeEntry not get value as expected")
		}
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "id"), {typ: '_'}})}).AsXRangeEntry(); !reflect.DeepEqual(XRangeEntry{
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
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{strmsg('+', "id1"), slicemsg('*', []ValkeyMessage{strmsg('+', "a"), strmsg('+', "b")})}),
			slicemsg('*', []ValkeyMessage{strmsg('+', "id2"), {typ: '_'}}),
		})}).AsXRange(); !reflect.DeepEqual([]XRangeEntry{{
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
		if ret, _ := (ValkeyResult{val: slicemsg('%', []ValkeyMessage{
			strmsg('+', "stream1"),
			slicemsg('*', []ValkeyMessage{
				slicemsg('*', []ValkeyMessage{strmsg('+', "id1"), slicemsg('*', []ValkeyMessage{strmsg('+', "a"), strmsg('+', "b")})}),
				slicemsg('*', []ValkeyMessage{strmsg('+', "id2"), {typ: '_'}}),
			}),
			strmsg('+', "stream2"),
			slicemsg('*', []ValkeyMessage{
				slicemsg('*', []ValkeyMessage{strmsg('+', "id3"), slicemsg('*', []ValkeyMessage{strmsg('+', "c"), strmsg('+', "d")})}),
			}),
		})}).AsXRead(); !reflect.DeepEqual(map[string][]XRangeEntry{
			"stream1": {
				{ID: "id1", FieldValues: map[string]string{"a": "b"}},
				{ID: "id2", FieldValues: nil}},
			"stream2": {
				{ID: "id3", FieldValues: map[string]string{"c": "d"}},
			},
		}, ret) {
			t.Fatal("AsXRead not get value as expected")
		}
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "stream1"),
				slicemsg('*', []ValkeyMessage{
					slicemsg('*', []ValkeyMessage{strmsg('+', "id1"), slicemsg('*', []ValkeyMessage{strmsg('+', "a"), strmsg('+', "b")})}),
					slicemsg('*', []ValkeyMessage{strmsg('+', "id2"), {typ: '_'}}),
				}),
			}),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "stream2"),
				slicemsg('*', []ValkeyMessage{
					slicemsg('*', []ValkeyMessage{strmsg('+', "id3"), slicemsg('*', []ValkeyMessage{strmsg('+', "c"), strmsg('+', "d")})}),
				}),
			}),
		})}).AsXRead(); !reflect.DeepEqual(map[string][]XRangeEntry{
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
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			strmsg('+', "m1"),
			strmsg('+', "1"),
		})}).AsZScore(); !reflect.DeepEqual(ZScore{Member: "m1", Score: 1}, ret) {
			t.Fatal("AsZScore not get value as expected")
		}
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			strmsg('+', "m1"),
			strmsg(',', "1"),
		})}).AsZScore(); !reflect.DeepEqual(ZScore{Member: "m1", Score: 1}, ret) {
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
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			strmsg('+', "m1"),
			strmsg('+', "1"),
			strmsg('+', "m2"),
			strmsg('+', "2"),
		})}).AsZScores(); !reflect.DeepEqual([]ZScore{
			{Member: "m1", Score: 1},
			{Member: "m2", Score: 2},
		}, ret) {
			t.Fatal("AsZScores not get value as expected")
		}
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "m1"),
				strmsg(',', "1"),
			}),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "m2"),
				strmsg(',', "2"),
			}),
		})}).AsZScores(); !reflect.DeepEqual([]ZScore{
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
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			strmsg('+', "k"),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "1"),
				strmsg('+', "2"),
			}),
		})}).AsLMPop(); !reflect.DeepEqual(KeyValues{
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
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			strmsg('+', "k"),
			slicemsg('*', []ValkeyMessage{
				slicemsg('*', []ValkeyMessage{
					strmsg('+', "1"),
					strmsg(',', "1"),
				}),
				slicemsg('*', []ValkeyMessage{
					strmsg('+', "2"),
					strmsg(',', "2"),
				}),
			}),
		})}).AsZMPop(); !reflect.DeepEqual(KeyZScores{
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
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
			strmsg('+', "a"),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "k1"),
				strmsg('+', "v1"),
				strmsg('+', "kk"),
				strmsg('+', "vv"),
			}),
			strmsg('+', "b"),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "k2"),
				strmsg('+', "v2"),
				strmsg('+', "kk"),
				strmsg('+', "vv"),
			}),
		})}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: map[string]string{"k1": "v1", "kk": "vv"}},
			{Key: "b", Doc: map[string]string{"k2": "v2", "kk": "vv"}},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
			strmsg('+', "a"),
			strmsg('+', "1"),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "k1"),
				strmsg('+', "v1"),
			}),
			strmsg('+', "b"),
			strmsg('+', "2"),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "k2"),
				strmsg('+', "v2"),
			}),
		})}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: map[string]string{"k1": "v1"}, Score: 1},
			{Key: "b", Doc: map[string]string{"k2": "v2"}, Score: 2},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
			strmsg('+', "a"),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "k1"),
				strmsg('+', "v1"),
				strmsg('+', "kk"),
				strmsg('+', "vv"),
			}),
		})}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: map[string]string{"k1": "v1", "kk": "vv"}},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
			strmsg('+', "a"),
			strmsg('+', "b"),
		})}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: nil},
			{Key: "b", Doc: nil},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
			strmsg('+', "a"),
			strmsg('+', "1"),
			strmsg('+', "b"),
			strmsg('+', "2"),
		})}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: nil, Score: 1},
			{Key: "b", Doc: nil, Score: 2},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
			strmsg('+', "1"),
			strmsg('+', "2"),
		})}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "1", Doc: nil},
			{Key: "2", Doc: nil},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
			strmsg('+', "a"),
		})}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "a", Doc: nil},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
		})}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
	})

	t.Run("AsFtSearch RESP3", func(t *testing.T) {
		if n, ret, _ := (ValkeyResult{val: slicemsg('%', []ValkeyMessage{
			strmsg('+', "total_results"),
			{typ: ':', intlen: 3},
			strmsg('+', "results"),
			slicemsg('*', []ValkeyMessage{
				slicemsg('%', []ValkeyMessage{
					strmsg('+', "id"),
					strmsg('+', "1"),
					strmsg('+', "extra_attributes"),
					slicemsg('%', []ValkeyMessage{
						strmsg('+', "$"),
						strmsg('+', "1"),
					}),
					strmsg('+', "score"),
					strmsg(',', "1"),
				}),
				slicemsg('%', []ValkeyMessage{
					strmsg('+', "id"),
					strmsg('+', "2"),
					strmsg('+', "extra_attributes"),
					slicemsg('%', []ValkeyMessage{
						strmsg('+', "$"),
						strmsg('+', "2"),
					}),
					strmsg('+', "score"),
					strmsg(',', "2"),
				}),
			}),
			strmsg('+', "error"),
			slicemsg('*', []ValkeyMessage{}),
		})}).AsFtSearch(); n != 3 || !reflect.DeepEqual([]FtSearchDoc{
			{Key: "1", Doc: map[string]string{"$": "1"}, Score: 1},
			{Key: "2", Doc: map[string]string{"$": "2"}, Score: 2},
		}, ret) {
			t.Fatal("AsFtSearch not get value as expected")
		}
		if _, _, err := (ValkeyResult{val: slicemsg('%', []ValkeyMessage{
			strmsg('+', "total_results"),
			{typ: ':', intlen: 3},
			strmsg('+', "results"),
			slicemsg('*', []ValkeyMessage{
				slicemsg('%', []ValkeyMessage{
					strmsg('+', "id"),
					strmsg('+', "1"),
					strmsg('+', "extra_attributes"),
					slicemsg('%', []ValkeyMessage{
						strmsg('+', "$"),
						strmsg('+', "1"),
					}),
					strmsg('+', "score"),
					strmsg(',', "1"),
				}),
			}),
			strmsg('+', "error"),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "mytimeout"),
			}),
		})}).AsFtSearch(); err == nil || err.Error() != "mytimeout" {
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
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "k1"),
				strmsg('+', "v1"),
				strmsg('+', "kk"),
				strmsg('+', "vv"),
			}),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "k2"),
				strmsg('+', "v2"),
				strmsg('+', "kk"),
				strmsg('+', "vv"),
			}),
		})}).AsFtAggregate(); n != 3 || !reflect.DeepEqual([]map[string]string{
			{"k1": "v1", "kk": "vv"},
			{"k2": "v2", "kk": "vv"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "k1"),
				strmsg('+', "v1"),
				strmsg('+', "kk"),
				strmsg('+', "vv"),
			}),
		})}).AsFtAggregate(); n != 3 || !reflect.DeepEqual([]map[string]string{
			{"k1": "v1", "kk": "vv"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			{typ: ':', intlen: 3},
		})}).AsFtAggregate(); n != 3 || !reflect.DeepEqual([]map[string]string{}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
	})

	t.Run("AsFtAggregate RESP3", func(t *testing.T) {
		if n, ret, _ := (ValkeyResult{val: slicemsg('%', []ValkeyMessage{
			strmsg('+', "total_results"),
			{typ: ':', intlen: 3},
			strmsg('+', "results"),
			slicemsg('*', []ValkeyMessage{
				slicemsg('%', []ValkeyMessage{
					strmsg('+', "extra_attributes"),
					slicemsg('%', []ValkeyMessage{
						strmsg('+', "$"),
						strmsg('+', "1"),
					}),
				}),
				slicemsg('%', []ValkeyMessage{
					strmsg('+', "extra_attributes"),
					slicemsg('%', []ValkeyMessage{
						strmsg('+', "$"),
						strmsg('+', "2"),
					}),
				}),
			}),
			strmsg('+', "error"),
			slicemsg('*', []ValkeyMessage{}),
		})}).AsFtAggregate(); n != 3 || !reflect.DeepEqual([]map[string]string{
			{"$": "1"},
			{"$": "2"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if _, _, err := (ValkeyResult{val: slicemsg('%', []ValkeyMessage{
			strmsg('+', "total_results"),
			{typ: ':', intlen: 3},
			strmsg('+', "results"),
			slicemsg('*', []ValkeyMessage{
				slicemsg('%', []ValkeyMessage{
					strmsg('+', "extra_attributes"),
					slicemsg('%', []ValkeyMessage{
						strmsg('+', "$"),
						strmsg('+', "1"),
					}),
				}),
			}),
			strmsg('+', "error"),
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "mytimeout"),
			}),
		})}).AsFtAggregate(); err == nil || err.Error() != "mytimeout" {
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
		if c, n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				{typ: ':', intlen: 3},
				slicemsg('*', []ValkeyMessage{
					strmsg('+', "k1"),
					strmsg('+', "v1"),
					strmsg('+', "kk"),
					strmsg('+', "vv"),
				}),
				slicemsg('*', []ValkeyMessage{
					strmsg('+', "k2"),
					strmsg('+', "v2"),
					strmsg('+', "kk"),
					strmsg('+', "vv"),
				}),
			}),
			{typ: ':', intlen: 1},
		})}).AsFtAggregateCursor(); c != 1 || n != 3 || !reflect.DeepEqual([]map[string]string{
			{"k1": "v1", "kk": "vv"},
			{"k2": "v2", "kk": "vv"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if c, n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				{typ: ':', intlen: 3},
				slicemsg('*', []ValkeyMessage{
					strmsg('+', "k1"),
					strmsg('+', "v1"),
					strmsg('+', "kk"),
					strmsg('+', "vv"),
				}),
			}),
			{typ: ':', intlen: 1},
		})}).AsFtAggregateCursor(); c != 1 || n != 3 || !reflect.DeepEqual([]map[string]string{
			{"k1": "v1", "kk": "vv"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if c, n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				{typ: ':', intlen: 3},
			}),
			{typ: ':', intlen: 1},
		})}).AsFtAggregateCursor(); c != 1 || n != 3 || !reflect.DeepEqual([]map[string]string{}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
	})

	t.Run("AsFtAggregate Cursor RESP3", func(t *testing.T) {
		if c, n, ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('%', []ValkeyMessage{
				strmsg('+', "total_results"),
				{typ: ':', intlen: 3},
				strmsg('+', "results"),
				slicemsg('*', []ValkeyMessage{
					slicemsg('%', []ValkeyMessage{
						strmsg('+', "extra_attributes"),
						slicemsg('%', []ValkeyMessage{
							strmsg('+', "$"),
							strmsg('+', "1"),
						}),
					}),
					slicemsg('%', []ValkeyMessage{
						strmsg('+', "extra_attributes"),
						slicemsg('%', []ValkeyMessage{
							strmsg('+', "$"),
							strmsg('+', "2"),
						}),
					}),
				}),
				strmsg('+', "error"),
				slicemsg('*', []ValkeyMessage{}),
			}),
			{typ: ':', intlen: 1},
		})}).AsFtAggregateCursor(); c != 1 || n != 3 || !reflect.DeepEqual([]map[string]string{
			{"$": "1"},
			{"$": "2"},
		}, ret) {
			t.Fatal("AsFtAggregate not get value as expected")
		}
		if _, _, _, err := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('%', []ValkeyMessage{
				strmsg('+', "total_results"),
				{typ: ':', intlen: 3},
				strmsg('+', "results"),
				slicemsg('*', []ValkeyMessage{
					slicemsg('%', []ValkeyMessage{
						strmsg('+', "extra_attributes"),
						slicemsg('%', []ValkeyMessage{
							strmsg('+', "$"),
							strmsg('+', "1"),
						}),
					}),
				}),
				strmsg('+', "error"),
				slicemsg('*', []ValkeyMessage{
					strmsg('+', "mytimeout"),
				}),
			}),
			{typ: ':', intlen: 1},
		})}).AsFtAggregateCursor(); err == nil || err.Error() != "mytimeout" {
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
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k1"),
				strmsg(',', "2.5"),
				{typ: ':', intlen: 1},
				slicemsg('*', []ValkeyMessage{
					strmsg(',', "28.0473"),
					strmsg(',', "26.2041"),
				}),
			}),
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k2"),
				strmsg(',', "4.5"),
				{typ: ':', intlen: 4},
				slicemsg('*', []ValkeyMessage{
					strmsg(',', "72.4973"),
					strmsg(',', "13.2263"),
				}),
			}),
		})}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", Dist: 2.5, GeoHash: 1, Longitude: 28.0473, Latitude: 26.2041},
			{Name: "k2", Dist: 4.5, GeoHash: 4, Longitude: 72.4973, Latitude: 13.2263},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//WithHash, WithCoord
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k1"),
				{typ: ':', intlen: 1},
				slicemsg('*', []ValkeyMessage{
					strmsg(',', "84.3877"),
					strmsg(',', "33.7488"),
				}),
			}),
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k2"),
				{typ: ':', intlen: 4},
				slicemsg('*', []ValkeyMessage{
					strmsg(',', "115.8613"),
					strmsg(',', "31.9523"),
				}),
			}),
		})}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", GeoHash: 1, Longitude: 84.3877, Latitude: 33.7488},
			{Name: "k2", GeoHash: 4, Longitude: 115.8613, Latitude: 31.9523},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//WithDist, WithCoord
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k1"),
				strmsg(',', "2.50076"),
				slicemsg('*', []ValkeyMessage{
					strmsg(',', "84.3877"),
					strmsg(',', "33.7488"),
				}),
			}),
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k2"),
				strmsg(',', "1024.96"),
				slicemsg('*', []ValkeyMessage{
					strmsg(',', "115.8613"),
					strmsg(',', "31.9523"),
				}),
			}),
		})}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", Dist: 2.50076, Longitude: 84.3877, Latitude: 33.7488},
			{Name: "k2", Dist: 1024.96, Longitude: 115.8613, Latitude: 31.9523},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//WithCoord
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k1"),
				slicemsg('*', []ValkeyMessage{
					strmsg(',', "122.4194"),
					strmsg(',', "37.7749"),
				}),
			}),
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k2"),
				slicemsg('*', []ValkeyMessage{
					strmsg(',', "35.6762"),
					strmsg(',', "139.6503"),
				}),
			}),
		})}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", Longitude: 122.4194, Latitude: 37.7749},
			{Name: "k2", Longitude: 35.6762, Latitude: 139.6503},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//WithDist
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k1"),
				strmsg(',', "2.50076"),
			}),
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k2"),
				strmsg(',', strconv.FormatFloat(math.MaxFloat64, 'E', -1, 64)),
			}),
		})}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", Dist: 2.50076},
			{Name: "k2", Dist: math.MaxFloat64},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//WithHash
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k1"),
				{typ: ':', intlen: math.MaxInt64},
			}),
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k2"),
				{typ: ':', intlen: 22296},
			}),
		})}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1", GeoHash: math.MaxInt64},
			{Name: "k2", GeoHash: 22296},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//With no additional options
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			strmsg('$', "k1"),
			strmsg('$', "k2"),
		})}).AsGeosearch(); !reflect.DeepEqual([]GeoLocation{
			{Name: "k1"},
			{Name: "k2"},
		}, ret) {
			t.Fatal("AsGeosearch not get value as expected")
		}
		//With wrong distance
		if _, err := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k1"),
				strmsg(',', "wrong distance"),
			}),
		})}).AsGeosearch(); err == nil {
			t.Fatal("AsGeosearch not failed as expected")
		}
		//With wrong coordinates
		if _, err := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('$', "k2"),
				slicemsg('*', []ValkeyMessage{
					strmsg(',', "35.6762"),
				}),
			}),
		})}).AsGeosearch(); err == nil {
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
					val: slicemsg('*', []ValkeyMessage{
						slicemsg('*', []ValkeyMessage{
							{typ: ':', intlen: 0},
							{typ: ':', intlen: 0},
							slicemsg('*', []ValkeyMessage{ // master
								strmsg('+', "127.0.3.1"),
								{typ: ':', intlen: 3},
								strmsg('+', ""),
							}),
						}),
					}),
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
		valkeyMessageError := strmsg('-', "ERR no_prefix")
		if (&valkeyMessageError).Error().Error() != "no_prefix" {
			t.Fatal("fail to trim ERR")
		}
	})
	t.Run("ToInt64", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 int64
		if val, err := (&ValkeyMessage{typ: '_'}).ToInt64(); err == nil {
			t.Fatal("ToInt64 did not fail as expected")
		} else if val != 0 {
			t.Fatalf("expected 0, got %d", val)
		}

		// Test case where the message type is 't', which is not a RESP3 int64
		if val, err := (&ValkeyMessage{typ: 't'}).ToInt64(); err == nil {
			t.Fatal("ToInt64 did not fail as expected")
		} else if val != 0 {
			t.Fatalf("expected 0, got %d", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a RESP3 int64") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ToBool", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 bool
		if val, err := (&ValkeyMessage{typ: '_'}).ToBool(); err == nil {
			t.Fatal("ToBool did not fail as expected")
		} else if val != false {
			t.Fatalf("expected false, got %v", val)
		}

		// Test case where the message type is 't', which is not a RESP3 bool
		if val, err := (&ValkeyMessage{typ: 't'}).ToBool(); err == nil {
			t.Fatal("ToBool did not fail as expected")
		} else if val != false {
			t.Fatalf("expected false, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a RESP3 bool") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsBool", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 int, string, or bool
		if val, err := (&ValkeyMessage{typ: '_'}).AsBool(); err == nil {
			t.Fatal("AsBool did not fail as expected")
		} else if val != false {
			t.Fatalf("expected false, got %v", val)
		}

		// Test case where the message type is 't', which is not a RESP3 int, string, or bool
		if val, err := (&ValkeyMessage{typ: 't'}).AsBool(); err == nil {
			t.Fatal("AsBool did not fail as expected")
		} else if val != false {
			t.Fatalf("expected false, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a int, string or bool") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ToFloat64", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 float64
		if val, err := (&ValkeyMessage{typ: '_'}).ToFloat64(); err == nil {
			t.Fatal("ToFloat64 did not fail as expected")
		} else if val != 0 {
			t.Fatalf("expected 0, got %f", val)
		}

		// Test case where the message type is 't', which is not a RESP3 float64
		if val, err := (&ValkeyMessage{typ: 't'}).ToFloat64(); err == nil {
			t.Fatal("ToFloat64 did not fail as expected")
		} else if val != 0 {
			t.Fatalf("expected 0, got %f", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a RESP3 float64") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ToString", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 string
		if val, err := (&ValkeyMessage{typ: '_'}).ToString(); err == nil {
			t.Fatal("ToString did not fail as expected")
		} else if val != "" {
			t.Fatalf("expected empty string, got %v", val)
		}

		// Test case where the message type is ':', which is not a RESP3 string
		if val, err := (&ValkeyMessage{typ: ':'}).ToString(); err == nil {
			t.Fatal("ToString did not fail as expected")
		} else if val != "" {
			t.Fatalf("expected empty string, got %v", val)
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsReader", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 string
		if val, err := (&ValkeyMessage{typ: '_'}).AsReader(); err == nil {
			t.Fatal("AsReader did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		// Test case where the message type is ':', which is not a RESP3 string
		if val, err := (&ValkeyMessage{typ: ':'}).AsReader(); err == nil {
			t.Fatal("AsReader did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsBytes", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 string
		if val, err := (&ValkeyMessage{typ: '_'}).AsBytes(); err == nil {
			t.Fatal("AsBytes did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		// Test case where the message type is ':', which is not a RESP3 string
		if val, err := (&ValkeyMessage{typ: ':'}).AsBytes(); err == nil {
			t.Fatal("AsBytes did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("DecodeJSON", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 string
		if err := (&ValkeyMessage{typ: '_'}).DecodeJSON(nil); err == nil {
			t.Fatal("DecodeJSON did not fail as expected")
		}

		// Test case where the message type is ':', which is not a RESP3 string
		if err := (&ValkeyMessage{typ: ':'}).DecodeJSON(nil); err == nil {
			t.Fatal("DecodeJSON did not fail as expected")
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsInt64", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 string
		if val, err := (&ValkeyMessage{typ: '_'}).AsInt64(); err == nil {
			t.Fatal("AsInt64 did not fail as expected")
		} else if val != 0 {
			t.Fatalf("expected 0, got %d", val)
		}

		// Test case where the message type is '*', which is not a RESP3 string
		valkeyMessageArrayWithEmptyMessage := slicemsg('*', []ValkeyMessage{{}})
		if val, err := (&valkeyMessageArrayWithEmptyMessage).AsInt64(); err == nil {
			t.Fatal("AsInt64 did not fail as expected")
		} else if val != 0 {
			t.Fatalf("expected 0, got %d", val)
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a string", typeNames['*'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsUint64", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 string
		if val, err := (&ValkeyMessage{typ: '_'}).AsUint64(); err == nil {
			t.Fatal("AsUint64 did not fail as expected")
		} else if val != 0 {
			t.Fatalf("expected 0, got %d", val)
		}

		// Test case where the message type is '*', which is not a RESP3 string
		valkeyMessageArrayWithEmptyMessage := slicemsg('*', []ValkeyMessage{{}})
		if val, err := (&valkeyMessageArrayWithEmptyMessage).AsUint64(); err == nil {
			t.Fatal("AsUint64 did not fail as expected")
		} else if val != 0 {
			t.Fatalf("expected 0, got %d", val)
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a string", typeNames['*'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsFloat64", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 string
		if val, err := (&ValkeyMessage{typ: '_'}).AsFloat64(); err == nil {
			t.Fatal("AsFloat64 did not fail as expected")
		} else if val != 0 {
			t.Fatalf("expected 0, got %f", val)
		}

		// Test case where the message type is ':', which is not a RESP3 string
		if val, err := (&ValkeyMessage{typ: ':'}).AsFloat64(); err == nil {
			t.Fatal("AsFloat64 did not fail as expected")
		} else if val != 0 {
			t.Fatalf("expected 0, got %f", val)
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ToArray", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 array
		if val, err := (&ValkeyMessage{typ: '_'}).ToArray(); err == nil {
			t.Fatal("ToArray did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		// Test case where the message type is 't', which is not a RESP3 array
		if val, err := (&ValkeyMessage{typ: 't'}).ToArray(); err == nil {
			t.Fatal("ToArray did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a array") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsStrSlice", func(t *testing.T) {
		// Test case where the message type is '_', which is not a RESP3 array
		if val, err := (&ValkeyMessage{typ: '_'}).AsStrSlice(); err == nil {
			t.Fatal("AsStrSlice did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		// Test case where the message type is 't', which is not a RESP3 array
		if val, err := (&ValkeyMessage{typ: 't'}).AsStrSlice(); err == nil {
			t.Fatal("AsStrSlice did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a array") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsIntSlice", func(t *testing.T) {
		if val, err := (&ValkeyMessage{typ: '_'}).AsIntSlice(); err == nil {
			t.Fatal("AsIntSlice did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		if val, err := (&ValkeyMessage{typ: 't'}).AsIntSlice(); err == nil {
			t.Fatal("AsIntSlice did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a array") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsFloatSlice", func(t *testing.T) {
		if val, err := (&ValkeyMessage{typ: '_'}).AsFloatSlice(); err == nil {
			t.Fatal("AsFloatSlice did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		if val, err := (&ValkeyMessage{typ: 't'}).AsFloatSlice(); err == nil {
			t.Fatal("AsFloatSlice did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a array") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsBoolSlice", func(t *testing.T) {
		if val, err := (&ValkeyMessage{typ: '_'}).AsBoolSlice(); err == nil {
			t.Fatal("AsBoolSlice did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		if val, err := (&ValkeyMessage{typ: 't'}).AsBoolSlice(); err == nil {
			t.Fatal("AsBoolSlice did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a array") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsMap", func(t *testing.T) {
		if val, err := (&ValkeyMessage{typ: '_'}).AsMap(); err == nil {
			t.Fatal("AsMap did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		if val, err := (&ValkeyMessage{typ: 't'}).AsMap(); err == nil {
			t.Fatal("AsMap did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a map/array/set or its length is not even") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsStrMap", func(t *testing.T) {
		if val, err := (&ValkeyMessage{typ: '_'}).AsStrMap(); err == nil {
			t.Fatal("AsStrMap did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		if val, err := (&ValkeyMessage{typ: 't'}).AsStrMap(); err == nil {
			t.Fatal("AsStrMap did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a map/array/set") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsIntMap", func(t *testing.T) {
		if val, err := (&ValkeyMessage{typ: '_'}).AsIntMap(); err == nil {
			t.Fatal("AsIntMap did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		if val, err := (&ValkeyMessage{typ: 't'}).AsIntMap(); err == nil {
			t.Fatal("AsIntMap did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a map/array/set") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ToMap", func(t *testing.T) {
		if val, err := (&ValkeyMessage{typ: '_'}).ToMap(); err == nil {
			t.Fatal("ToMap did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		if val, err := (&ValkeyMessage{typ: 't'}).ToMap(); err == nil {
			t.Fatal("ToMap did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a RESP3 map") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("ToAny", func(t *testing.T) {
		if val, err := (&ValkeyMessage{typ: '_'}).ToAny(); err == nil {
			t.Fatal("ToAny did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		}

		if val, err := (&ValkeyMessage{typ: 't'}).ToAny(); err == nil {
			t.Fatal("ToAny did not fail as expected")
		} else if val != nil {
			t.Fatalf("expected nil, got %v", val)
		} else if !strings.Contains(err.Error(), "valkey message type t is not a supported in ToAny") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsXRangeEntry - no range id", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry did not fail as expected")
		}

		if _, err := (&ValkeyMessage{typ: '*'}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry did not fail as expected")
		}

		valkeyMessageNullAndMap := slicemsg('*', []ValkeyMessage{{typ: '_'}, {typ: '%'}})
		if _, err := (&valkeyMessageNullAndMap).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry did not fail as expected")
		}

		valkeyMessageIntAndMap := slicemsg('*', []ValkeyMessage{{typ: ':'}, {typ: '%'}})
		if _, err := (&valkeyMessageIntAndMap).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry did not fail as expected")
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a string", typeNames[':'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsXRangeEntry - no range field values", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry did not fail as expected")
		}

		if _, err := (&ValkeyMessage{typ: '*'}).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry did not fail as expected")
		}

		valkeyMessageStringAndErr := slicemsg('*', []ValkeyMessage{{typ: '+'}, {typ: '-'}})
		if _, err := (&valkeyMessageStringAndErr).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry did not fail as expected")
		}

		valkeyMessageStringAndUnknown := slicemsg('*', []ValkeyMessage{{typ: '+'}, {typ: 't'}})
		if _, err := (&valkeyMessageStringAndUnknown).AsXRangeEntry(); err == nil {
			t.Fatal("AsXRangeEntry did not fail as expected")
		} else if !strings.Contains(err.Error(), "valkey message type t is not a map/array/set") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsXRange", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsXRange(); err == nil {
			t.Fatal("AsXRange not failed as expected")
		}

		valkeyMessageArrayWithNull := slicemsg('*', []ValkeyMessage{{typ: '_'}})
		if _, err := (&valkeyMessageArrayWithNull).AsXRange(); err == nil {
			t.Fatal("AsXRange not failed as expected")
		}
	})

	t.Run("AsXRead", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsXRead(); err == nil {
			t.Fatal("AsXRead did not fail as expected")
		}

		valkeyMessageMapIncorrectLen := slicemsg('%', []ValkeyMessage{
			strmsg('+', "stream1"),
			slicemsg('*', []ValkeyMessage{slicemsg('*', []ValkeyMessage{strmsg('+', "id1")})}),
		})
		if _, err := (&valkeyMessageMapIncorrectLen).AsXRead(); err == nil {
			t.Fatal("AsXRead did not fail as expected")
		}

		valkeyMessageArrayIncorrectLen := slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "stream1"),
			}),
		})
		if _, err := (&valkeyMessageArrayIncorrectLen).AsXRead(); err == nil {
			t.Fatal("AsXRead did not fail as expected")
		}

		valkeyMessageArrayIncorrectLen2 := slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "stream1"),
				slicemsg('*', []ValkeyMessage{slicemsg('*', []ValkeyMessage{strmsg('+', "id1")})}),
			}),
		})
		if _, err := (&valkeyMessageArrayIncorrectLen2).AsXRead(); err == nil {
			t.Fatal("AsXRead did not fail as expected")
		}

		if _, err := (&ValkeyMessage{typ: 't'}).AsXRead(); err == nil {
			t.Fatal("AsXRead did not fail as expected")
		} else if !strings.Contains(err.Error(), "valkey message type t is not a map/array/set") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsZScore", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsZScore(); err == nil {
			t.Fatal("AsZScore did not fail as expected")
		}

		if _, err := (&ValkeyMessage{typ: '*'}).AsZScore(); err == nil {
			t.Fatal("AsZScore did not fail as expected")
		} else if !strings.Contains(err.Error(), "valkey message is not a map/array/set or its length is not 2") {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsZScores", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsZScores(); err == nil {
			t.Fatal("AsZScore not failed as expected")
		}
		valkeyMessageStringArray := slicemsg('*', []ValkeyMessage{
			strmsg('+', "m1"),
			strmsg('+', "m2"),
		})
		if _, err := (&valkeyMessageStringArray).AsZScores(); err == nil {
			t.Fatal("AsZScores not fails as expected")
		}
		valkeyMessageNestedStringArray := slicemsg('*', []ValkeyMessage{
			slicemsg('*', []ValkeyMessage{
				strmsg('+', "m1"),
				strmsg('+', "m2"),
			}),
		})
		if _, err := (&valkeyMessageNestedStringArray).AsZScores(); err == nil {
			t.Fatal("AsZScores not fails as expected")
		}
	})

	t.Run("AsLMPop", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsLMPop(); err == nil {
			t.Fatal("AsLMPop did not fail as expected")
		}

		valkeyMessageStringAndNull := slicemsg('*', []ValkeyMessage{
			strmsg('+', "k"),
			{typ: '_'},
		})
		if _, err := (&valkeyMessageStringAndNull).AsLMPop(); err == nil {
			t.Fatal("AsLMPop did not fail as expected")
		}

		valkeyMessageStringArray := slicemsg('*', []ValkeyMessage{
			strmsg('+', "k"),
		})
		if _, err := (&valkeyMessageStringArray).AsLMPop(); err == nil {
			t.Fatal("AsLMPop did not fail as expected")
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a LMPOP response", typeNames['*'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsZMPop", func(t *testing.T) {
		if _, err := (&ValkeyMessage{typ: '_'}).AsZMPop(); err == nil {
			t.Fatal("AsZMPop did not fail as expected")
		}

		valkeyMessageStringAndNull := slicemsg('*', []ValkeyMessage{
			strmsg('+', "k"),
			{typ: '_'},
		})
		if _, err := (&valkeyMessageStringAndNull).AsZMPop(); err == nil {
			t.Fatal("AsZMPop did not fail as expected")
		}

		valkeyMessageStringArray := slicemsg('*', []ValkeyMessage{
			strmsg('+', "k"),
		})
		if _, err := (&valkeyMessageStringArray).AsZMPop(); err == nil {
			t.Fatal("AsZMPop did not fail as expected")
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a ZMPOP response", typeNames['*'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsFtSearch", func(t *testing.T) {
		if _, _, err := (&ValkeyMessage{typ: '_'}).AsFtSearch(); err == nil {
			t.Fatal("AsFtSearch did not fail as expected")
		}

		if _, _, err := (&ValkeyMessage{typ: '*'}).AsFtSearch(); err == nil {
			t.Fatal("AsFtSearch did not fail as expected")
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a FT.SEARCH response", typeNames['*'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsFtAggregate", func(t *testing.T) {
		if _, _, err := (&ValkeyMessage{typ: '_'}).AsFtAggregate(); err == nil {
			t.Fatal("AsFtAggregate did not fail as expected")
		}

		if _, _, err := (&ValkeyMessage{typ: '*'}).AsFtAggregate(); err == nil {
			t.Fatal("AsFtAggregate did not fail as expected")
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a FT.AGGREGATE response", typeNames['*'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsFtAggregateCursor", func(t *testing.T) {
		if _, _, _, err := (&ValkeyMessage{typ: '_'}).AsFtAggregateCursor(); err == nil {
			t.Fatal("AsFtAggregateCursor did not fail as expected")
		}

		if _, _, _, err := (&ValkeyMessage{typ: '*'}).AsFtAggregateCursor(); err == nil {
			t.Fatal("AsFtAggregateCursor did not fail as expected")
		} else if !strings.Contains(err.Error(), fmt.Sprintf("valkey message type %s is not a FT.AGGREGATE response", typeNames['*'])) {
			t.Fatalf("unexpected error: %v", err)
		}
	})

	t.Run("AsScanEntry", func(t *testing.T) {
		if _, err := (ValkeyResult{err: errors.New("other")}).AsScanEntry(); err == nil {
			t.Fatal("AsScanEntry not failed as expected")
		}
		if _, err := (ValkeyResult{val: ValkeyMessage{typ: '-'}}).AsScanEntry(); err == nil {
			t.Fatal("AsScanEntry not failed as expected")
		}
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "1"), slicemsg('*', []ValkeyMessage{strmsg('+', "a"), strmsg('+', "b")})})}).AsScanEntry(); !reflect.DeepEqual(ScanEntry{
			Cursor:   1,
			Elements: []string{"a", "b"},
		}, ret) {
			t.Fatal("AsScanEntry not get value as expected")
		}
		if ret, _ := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "0"), {typ: '_'}})}).AsScanEntry(); !reflect.DeepEqual(ScanEntry{}, ret) {
			t.Fatal("AsScanEntry not get value as expected")
		}
		if _, err := (ValkeyResult{val: slicemsg('*', []ValkeyMessage{strmsg('+', "0")})}).AsScanEntry(); err == nil || !strings.Contains(err.Error(), "a scan response or its length is not at least 2") {
			t.Fatal("AsScanEntry not get value as expected")
		}
	})

	t.Run("ToMap with non-string key", func(t *testing.T) {
		valkeyMessageSet := slicemsg('~', []ValkeyMessage{{typ: ':'}, {typ: ':'}})
		_, err := (&valkeyMessageSet).ToMap()
		if err == nil {
			t.Fatal("ToMap did not fail as expected")
		}
		if !strings.Contains(err.Error(), "valkey message type set is not a RESP3 map") {
			t.Fatalf("ToMap failed with unexpected error: %v", err)
		}
		valkeyMessageMap := slicemsg('%', []ValkeyMessage{{typ: ':'}, {typ: ':'}})
		_, err = (&valkeyMessageMap).ToMap()
		if err == nil {
			t.Fatal("ToMap did not fail as expected")
		}
		if !strings.Contains(err.Error(), "int64 as map key is not supported") {
			t.Fatalf("ToMap failed with unexpected error: %v", err)
		}
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
				input: slicemsg('*', []ValkeyMessage{
					slicemsg('*', []ValkeyMessage{
						{typ: ':', intlen: 0},
						{typ: ':', intlen: 0},
						slicemsg('*', []ValkeyMessage{
							strmsg('+', "127.0.3.1"),
							{typ: ':', intlen: 3},
							strmsg('+', ""),
						}),
					}),
				}),
				expected: `{"Value":[{"Value":[{"Value":0,"Type":"int64"},{"Value":0,"Type":"int64"},{"Value":[{"Value":"127.0.3.1","Type":"simple string"},{"Value":3,"Type":"int64"},{"Value":"","Type":"simple string"}],"Type":"array"}],"Type":"array"}],"Type":"array"}`,
			},
			{
				input: ValkeyMessage{
					typ:    '+',
					bytes:  unsafe.StringData("127.0.3.1"),
					intlen: int64(len("127.0.3.1")),
					ttl:    [7]byte{97, 77, 74, 61, 138, 1, 0},
				},
				expected: `{"Value":"127.0.3.1","Type":"simple string","TTL":"2023-08-28 17:56:34.273 +0000 UTC"}`,
			},
			{
				input:    ValkeyMessage{typ: '0'},
				expected: `{"Type":"unknown"}`,
			},
			{
				input:    ValkeyMessage{typ: typeBool, intlen: 1},
				expected: `{"Value":true,"Type":"boolean"}`,
			},
			{
				input:    ValkeyMessage{typ: typeNull},
				expected: `{"Type":"null","Error":"valkey nil message"}`,
			},
			{
				input:    strmsg(typeSimpleErr, "ERR foo"),
				expected: `{"Type":"simple error","Error":"foo"}`,
			},
			{
				input:    strmsg(typeBlobErr, "ERR foo"),
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

	t.Run("CacheMarshal and CacheUnmarshalView", func(t *testing.T) {
		m1 := ValkeyMessage{typ: '_'}
		m2 := strmsg('+', "random")
		m3 := ValkeyMessage{typ: '#', intlen: 1}
		m4 := ValkeyMessage{typ: ':', intlen: -1234}
		m5 := strmsg(',', "-1.5")
		m6 := slicemsg('%', nil)
		m7 := slicemsg('~', []ValkeyMessage{m1, m2, m3, m4, m5, m6})
		m8 := slicemsg('*', []ValkeyMessage{m1, m2, m3, m4, m5, m6, m7})
		msgs := []ValkeyMessage{m1, m2, m3, m4, m5, m6, m7, m8}
		now := time.Now()
		for i := range msgs {
			msgs[i].setExpireAt(now.Add(time.Second * time.Duration(i)).UnixMilli())
		}
		for i, m1 := range msgs {
			siz := m1.CacheSize()
			bs1 := m1.CacheMarshal(nil)
			if len(bs1) != siz {
				t.Fatal("size not match")
			}
			bs2 := m1.CacheMarshal(bs1)
			if !bytes.Equal(bs2[:siz], bs2[siz:]) {
				t.Fatal("byte not match")
			}
			var m2 ValkeyMessage
			if err := m2.CacheUnmarshalView(bs1); err != nil {
				t.Fatal(err)
			}
			if m1.String() != m2.String() {
				t.Fatal("content not match")
			}
			if !m2.IsCacheHit() {
				t.Fatal("should be cache hit")
			}
			if m2.CachePXAT() != now.Add(time.Second*time.Duration(i)).UnixMilli() {
				t.Fatal("should have the same ttl")
			}
			for l := 0; l < siz; l++ {
				var m3 ValkeyMessage
				if err := m3.CacheUnmarshalView(bs2[:l]); err != ErrCacheUnmarshal {
					t.Fatal("should fail as expected")
				}
			}
		}
	})
}
