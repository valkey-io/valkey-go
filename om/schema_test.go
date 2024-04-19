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
	B       int64  `json:"-" valkey:",ver"`
	private int64
}

type s5 struct {
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
	t.Run("non string `valkey:\",ver\"`", func(t *testing.T) {
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
	t.Run("missing `valkey:\",ver\"`", func(t *testing.T) {
		if v := recovered(func() {
			newSchema(reflect.TypeOf(s4{}))
		}); !strings.Contains(v, "should have one field with `valkey:\",ver\"` tag") {
			t.Fatalf("unexpected msg %v", v)
		}
	})
	t.Run("non time.Time `valkey:\",exat\"`", func(t *testing.T) {
		if v := recovered(func() {
			newSchema(reflect.TypeOf(s5{}))
		}); !strings.Contains(v, "should be a time.Time") {
			t.Fatalf("unexpected msg %v", v)
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
