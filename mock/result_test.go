package mock

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/valkey-io/valkey-go"
)

func TestValkeyString(t *testing.T) {
	m := ValkeyString("s")
	if v, err := m.ToString(); err != nil || v != "s" {
		t.Fatalf("unexpected value %v", v)
	}
}

func TestValkeyError(t *testing.T) {
	m := ValkeyError("s")
	if err := m.Error(); err == nil || err.Error() != "s" {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestValkeyInt64(t *testing.T) {
	m := ValkeyInt64(1)
	if v, err := m.ToInt64(); err != nil || v != int64(1) {
		t.Fatalf("unexpected value %v", v)
	}
	if v, err := m.AsInt64(); err != nil || v != int64(1) {
		t.Fatalf("unexpected value %v", v)
	}
}

func TestValkeyFloat64(t *testing.T) {
	m := ValkeyFloat64(1)
	if v, err := m.ToFloat64(); err != nil || v != float64(1) {
		t.Fatalf("unexpected value %v", v)
	}
	if v, err := m.AsFloat64(); err != nil || v != float64(1) {
		t.Fatalf("unexpected value %v", v)
	}
}

func TestValkeyBool(t *testing.T) {
	m := ValkeyBool(true)
	if v, err := m.ToBool(); err != nil || v != true {
		t.Fatalf("unexpected value %v", v)
	}
	if v, err := m.AsBool(); err != nil || v != true {
		t.Fatalf("unexpected value %v", v)
	}
}

func TestValkeyNil(t *testing.T) {
	m := ValkeyNil()
	if err := m.Error(); err == nil {
		t.Fatalf("unexpected value %v", err)
	}
	if v := m.IsNil(); v != true {
		t.Fatalf("unexpected value %v", v)
	}
}

func TestValkeyArray(t *testing.T) {
	m := ValkeyArray(ValkeyString("0"), ValkeyString("1"), ValkeyString("2"))
	if arr, err := m.AsStrSlice(); err != nil || !reflect.DeepEqual(arr, []string{"0", "1", "2"}) {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestValkeyMap(t *testing.T) {
	m := ValkeyMap(map[string]valkey.ValkeyMessage{
		"a": ValkeyString("0"),
		"b": ValkeyString("1"),
	})
	if arr, err := m.AsStrMap(); err != nil || !reflect.DeepEqual(arr, map[string]string{
		"a": "0",
		"b": "1",
	}) {
		t.Fatalf("unexpected value %v", err)
	}
	if arr, err := m.ToMap(); err != nil || !reflect.DeepEqual(arr, map[string]valkey.ValkeyMessage{
		"a": ValkeyString("0"),
		"b": ValkeyString("1"),
	}) {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestValkeyResult(t *testing.T) {
	r := Result(ValkeyNil())
	if err := r.Error(); !valkey.IsValkeyNil(err) {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestErrorResult(t *testing.T) {
	r := ErrorResult(errors.New("any"))
	if err := r.Error(); err.Error() != "any" {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestErrorResultStream(t *testing.T) {
	s := ValkeyResultStreamError(errors.New("any"))
	if err := s.Error(); err.Error() != "any" {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestErrorMultiResultStream(t *testing.T) {
	s := MultiValkeyResultStreamError(errors.New("any"))
	if err := s.Error(); err.Error() != "any" {
		t.Fatalf("unexpected value %v", err)
	}
}

func TestResultStream(t *testing.T) {
	type test struct {
		msg []valkey.ValkeyMessage
		out []string
		err []string
	}
	tests := []test{
		{msg: []valkey.ValkeyMessage{ValkeyString("")}, out: []string{""}, err: []string{""}},
		{msg: []valkey.ValkeyMessage{ValkeyString("0"), ValkeyBlobString("12345")}, out: []string{"0", "12345"}, err: []string{"", ""}},
		{msg: []valkey.ValkeyMessage{ValkeyInt64(123), ValkeyInt64(-456)}, out: []string{"123", "-456"}, err: []string{"", ""}},
		{msg: []valkey.ValkeyMessage{ValkeyString(""), ValkeyNil()}, out: []string{"", ""}, err: []string{"", "nil"}},
		{msg: []valkey.ValkeyMessage{ValkeyArray(ValkeyString("n")), ValkeyString("ok"), ValkeyNil(), ValkeyMap(map[string]valkey.ValkeyMessage{"b": ValkeyBlobString("b")})}, out: []string{"", "ok", "", ""}, err: []string{"unsupported", "", "nil", "unsupported"}},
	}
	for _, tc := range tests {
		s := ValkeyResultStream(tc.msg...)
		if err := s.Error(); err != nil {
			t.Fatalf("unexpected value %v", err)
		}
		if !s.HasNext() {
			t.Fatalf("unexpected value %v", s.HasNext())
		}
		buf := bytes.NewBuffer(nil)
		for i := 0; s.HasNext(); i++ {
			n, err := s.WriteTo(buf)
			if tc.err[i] != "" {
				if err == nil {
					t.Fatalf("unexpected value %v", err)
				} else if !strings.Contains(err.Error(), tc.err[i]) {
					t.Fatalf("unexpected value %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected value %v", err)
			}
			if n != int64(len(tc.out[i])) {
				t.Fatalf("unexpected value %v", n)
			}
		}
		if buf.String() != strings.Join(tc.out, "") {
			t.Fatalf("unexpected value %v", buf.String())
		}
		if s.HasNext() {
			t.Fatalf("unexpected value %v", s.HasNext())
		}
		if err := s.Error(); err != io.EOF {
			t.Fatalf("unexpected value %v", err)
		}
	}
}

func TestMultiResultStream(t *testing.T) {
	type test struct {
		msg []valkey.ValkeyMessage
		out []string
		err []string
	}
	tests := []test{
		{msg: []valkey.ValkeyMessage{ValkeyString("")}, out: []string{""}, err: []string{""}},
		{msg: []valkey.ValkeyMessage{ValkeyString("0"), ValkeyBlobString("12345")}, out: []string{"0", "12345"}, err: []string{"", ""}},
		{msg: []valkey.ValkeyMessage{ValkeyInt64(123), ValkeyInt64(-456)}, out: []string{"123", "-456"}, err: []string{"", ""}},
		{msg: []valkey.ValkeyMessage{ValkeyString(""), ValkeyNil()}, out: []string{"", ""}, err: []string{"", "nil"}},
		{msg: []valkey.ValkeyMessage{ValkeyArray(ValkeyString("n")), ValkeyString("ok"), ValkeyNil(), ValkeyMap(map[string]valkey.ValkeyMessage{"b": ValkeyBlobString("b")})}, out: []string{"", "ok", "", ""}, err: []string{"unsupported", "", "nil", "unsupported"}},
	}
	for _, tc := range tests {
		s := MultiValkeyResultStream(tc.msg...)
		if err := s.Error(); err != nil {
			t.Fatalf("unexpected value %v", err)
		}
		if !s.HasNext() {
			t.Fatalf("unexpected value %v", s.HasNext())
		}
		buf := bytes.NewBuffer(nil)
		for i := 0; s.HasNext(); i++ {
			n, err := s.WriteTo(buf)
			if tc.err[i] != "" {
				if err == nil {
					t.Fatalf("unexpected value %v", err)
				} else if !strings.Contains(err.Error(), tc.err[i]) {
					t.Fatalf("unexpected value %v", err)
				}
			} else if err != nil {
				t.Fatalf("unexpected value %v", err)
			}
			if n != int64(len(tc.out[i])) {
				t.Fatalf("unexpected value %v", n)
			}
		}
		if buf.String() != strings.Join(tc.out, "") {
			t.Fatalf("unexpected value %v", buf.String())
		}
		if s.HasNext() {
			t.Fatalf("unexpected value %v", s.HasNext())
		}
		if err := s.Error(); err != io.EOF {
			t.Fatalf("unexpected value %v", err)
		}
	}
}
