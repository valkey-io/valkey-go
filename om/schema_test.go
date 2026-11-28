package om

import (
	"reflect"
	"strings"
	"testing"
)

type s1 struct {
	A int `valkey:",key"`
}

type s2 struct {
	A string `valkey:",ver"`
}

type s3 struct {
	A       string `json:"-" valkey:",key"`
	B       int64  `valkey:",ver"`
	private int64
}

type s4 struct {
	A       string `valkey:",key"`
	B       int64  `valkey:",ver"`
	private int64
}

type s5 struct {
	A       string `valkey:",key"`
	private int64
}

type s6 struct {
	A string `valkey:",key"`
	B int64  `valkey:",ver"`
	C int64  `valkey:",exat"`
}

func TestSchema(t *testing.T) {
	t.Run("non struct", func(t *testing.T) {
		if v := recovered(func() {
			newSchema(reflect.TypeOf(map[string]string{}))
		}); !strings.Contains(v, "should be a struct") {
			t.Fatalf("unexpected msg %v", v)
		}
	})
	t.Run("non string `valkey:\",key\"`", func(t *testing.T) {
		if v := recovered(func() {
			newSchema(reflect.TypeOf(s1{}))
		}); !strings.Contains(v, "should be a string") {
			t.Fatalf("unexpected msg %v", v)
		}
	})
	t.Run("non int64 `valkey:\",ver\"`", func(t *testing.T) {
		if v := recovered(func() {
			newSchema(reflect.TypeOf(s2{}))
		}); !strings.Contains(v, "should be a int64") {
			t.Fatalf("unexpected msg %v", v)
		}
	})
	t.Run("missing `valkey:\",key\"`", func(t *testing.T) {
		if v := recovered(func() {
			newSchema(reflect.TypeOf(s3{}))
		}); !strings.Contains(v, "should have one field with `valkey:\",key\"` tag") {
			t.Fatalf("unexpected msg %v", v)
		}
	})
	t.Run("ver is not verless", func(t *testing.T) {
		v := newSchema(reflect.TypeOf(s4{}))
		if v.verless {
			t.Fatal("schema should not be verless")
		}
	})
	t.Run("missing `valkey:\",ver\"` should be verless", func(t *testing.T) {
		v := newSchema(reflect.TypeOf(s5{}))
		if !v.verless {
			t.Fatal("schema should be verless")
		}
	})
	t.Run("non time.Time `valkey:\",exat\"`", func(t *testing.T) {
		if v := recovered(func() {
			newSchema(reflect.TypeOf(s6{}))
		}); !strings.Contains(v, "should be a time.Time") {
			t.Fatalf("unexpected msg %v", v)
		}
	})
}

func TestSchemaJSONTagOptions(t *testing.T) {
	type entity struct {
		Key       string `json:"id,omitempty" valkey:",key"`
		Ver       int64  `json:"ver,omitempty" valkey:",ver"`
		Title     string `json:",omitempty"`
		Count     int64  `json:"count,string"`
		Ignored   string `json:"-"`
		DashNamed string `json:"-,"`
	}

	s := newSchema(reflect.TypeOf(entity{}))

	if s.key == nil || s.key.name != "id" {
		t.Fatalf("expected key field name %q, got %#v", "id", s.key)
	}
	if s.ver == nil || s.ver.name != "ver" {
		t.Fatalf("expected ver field name %q, got %#v", "ver", s.ver)
	}
	if _, ok := s.fields["Ignored"]; ok {
		t.Fatal("expected field tagged json:\"-\" to be ignored")
	}
	for _, unexpected := range []string{"id,omitempty", "ver,omitempty", ",omitempty", "count,string", "-,"} {
		if _, ok := s.fields[unexpected]; ok {
			t.Fatalf("unexpected unstripped json tag name %q in schema fields", unexpected)
		}
	}
	for _, want := range []string{"id", "ver", "Title", "count", "-"} {
		if _, ok := s.fields[want]; !ok {
			t.Fatalf("expected field %q in schema fields, got keys %v", want, s.fields)
		}
	}

	t.Run("hash converter round-trip uses stripped names", func(t *testing.T) {
		type hashEntity struct {
			Key   string `json:"id,omitempty" valkey:",key"`
			Ver   int64  `json:"ver,omitempty" valkey:",ver"`
			Title string `json:",omitempty"`
			Dash  string `json:"-,"`
		}
		repo := NewHashRepository("test", hashEntity{}, nil).(*HashRepository[hashEntity])
		in := &hashEntity{Key: "k1", Ver: 3, Title: "hello", Dash: "literal-dash"}
		m := repo.factory.NewConverter(reflect.ValueOf(in).Elem()).ToHash()
		if m["id"] != "k1" || m["ver"] != "3" || m["Title"] != "hello" || m["-"] != "literal-dash" {
			t.Fatalf("unexpected hash map: %v", m)
		}
		var out hashEntity
		if err := repo.factory.NewConverter(reflect.ValueOf(&out).Elem()).FromHash(m); err != nil {
			t.Fatalf("FromHash failed: %v", err)
		}
		if out != *in {
			t.Fatalf("round-trip mismatch: got %+v, want %+v", out, *in)
		}
	})

	t.Run("json repository uses stripped ver field name", func(t *testing.T) {
		type jsonEntity struct {
			Key string `json:"id,omitempty" valkey:",key"`
			Ver int64  `json:"ver,omitempty" valkey:",ver"`
		}
		repo := NewJSONRepository("test", jsonEntity{}, nil).(*JSONRepository[jsonEntity])
		_, exec := repo.toExec(&jsonEntity{Key: "k1", Ver: 2})
		if len(exec.Args) < 2 || exec.Args[0] != "ver" {
			t.Fatalf("expected jsonSaveScript ARGV[1] version field %q, got %v", "ver", exec.Args)
		}
	})
}

func recovered(fn func()) (msg string) {
	defer func() {
		msg = recover().(string)
	}()
	fn()
	return msg
}
