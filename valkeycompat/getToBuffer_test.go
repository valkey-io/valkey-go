package valkeycompat

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/valkey-io/valkey-go"
)

func TestGetToBuffer(t *testing.T) {
	client, err := valkey.NewClient(valkey.ClientOption{
		InitAddress: []string{"127.0.0.1:6378"},
		ClientName:  "get-to-buffer-test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	adapter := NewAdapter(client)
	ctx := context.Background()

	t.Run("basic GET", func(t *testing.T) {
		if err := adapter.Set(ctx, "gtb-basic", "hello", 0).Err(); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 32)

		cmd := adapter.GetToBuffer(ctx, "gtb-basic", buf)

		n, err := cmd.Result()
		if err != nil {
			t.Fatal(err)
		}

		if n != 5 {
			t.Fatalf("expected n=5, got %d", n)
		}

		if got := string(cmd.Bytes()); got != "hello" {
			t.Fatalf("expected %q, got %q", "hello", got)
		}
	})

	t.Run("exact size buffer", func(t *testing.T) {
		if err := adapter.Set(ctx, "gtb-exact", "hello", 0).Err(); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 5)

		cmd := adapter.GetToBuffer(ctx, "gtb-exact", buf)

		n, err := cmd.Result()
		if err != nil {
			t.Fatal(err)
		}

		if n != 5 {
			t.Fatalf("expected n=5, got %d", n)
		}

		if got := string(cmd.Bytes()); got != "hello" {
			t.Fatalf("expected %q, got %q", "hello", got)
		}
	})

	t.Run("larger buffer", func(t *testing.T) {
		if err := adapter.Set(ctx, "gtb-large-buffer", "hello", 0).Err(); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 100)

		cmd := adapter.GetToBuffer(ctx, "gtb-large-buffer", buf)

		n, err := cmd.Result()
		if err != nil {
			t.Fatal(err)
		}

		if n != 5 {
			t.Fatalf("expected n=5, got %d", n)
		}

		if len(cmd.Bytes()) != 5 {
			t.Fatalf("expected len(Bytes())=5, got %d", len(cmd.Bytes()))
		}
	})

	t.Run("too small buffer", func(t *testing.T) {
		if err := adapter.Set(ctx, "gtb-small", "hello world", 0).Err(); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 5)

		cmd := adapter.GetToBuffer(ctx, "gtb-small", buf)

		_, err := cmd.Result()

		if !errors.Is(err, io.ErrShortBuffer) {
			t.Fatalf("expected io.ErrShortBuffer, got %v", err)
		}

		if got := string(cmd.Bytes()); got != "hello" {
			t.Fatalf("expected partial buffer %q, got %q", "hello", got)
		}
	})

	t.Run("connection reuse after short buffer", func(t *testing.T) {
		if err := adapter.Set(ctx, "gtb-large", "hello world", 0).Err(); err != nil {
			t.Fatal(err)
		}

		if err := adapter.Set(ctx, "gtb-next", "second", 0).Err(); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 5)

		cmd := adapter.GetToBuffer(ctx, "gtb-large", buf)

		if _, err := cmd.Result(); !errors.Is(err, io.ErrShortBuffer) {
			t.Fatalf("expected io.ErrShortBuffer, got %v", err)
		}

		value, err := adapter.Get(ctx, "gtb-next").Result()
		if err != nil {
			t.Fatal(err)
		}

		if value != "second" {
			t.Fatalf("expected %q, got %q", "second", value)
		}
	})

	t.Run("missing key", func(t *testing.T) {
		buf := make([]byte, 32)

		cmd := adapter.GetToBuffer(ctx, "gtb-missing", buf)

		n, err := cmd.Result()
		if err != nil {
			t.Fatal(err)
		}

		if n != 0 {
			t.Fatalf("expected n=0, got %d", n)
		}

		if len(cmd.Bytes()) != 0 {
			t.Fatalf("expected empty bytes, got %d", len(cmd.Bytes()))
		}
	})

	t.Run("empty value", func(t *testing.T) {
		if err := adapter.Set(ctx, "gtb-empty", "", 0).Err(); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 32)

		cmd := adapter.GetToBuffer(ctx, "gtb-empty", buf)

		n, err := cmd.Result()
		if err != nil {
			t.Fatal(err)
		}

		if n != 0 {
			t.Fatalf("expected n=0, got %d", n)
		}
	})

	t.Run("binary payload", func(t *testing.T) {
		value := string([]byte{0x00, 0x01, 0xff, 0x7f, 0x80})

		if err := adapter.Set(ctx, "gtb-binary", value, 0).Err(); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, len(value))

		cmd := adapter.GetToBuffer(ctx, "gtb-binary", buf)

		_, err := cmd.Result()
		if err != nil {
			t.Fatal(err)
		}

		if string(cmd.Bytes()) != value {
			t.Fatalf("binary payload mismatch")
		}
	})

	t.Run("large payload", func(t *testing.T) {
		value := strings.Repeat("x", 1024*1024)

		if err := adapter.Set(ctx, "gtb-large-payload", value, 0).Err(); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, len(value))

		cmd := adapter.GetToBuffer(ctx, "gtb-large-payload", buf)

		n, err := cmd.Result()
		if err != nil {
			t.Fatal(err)
		}

		if n != len(value) {
			t.Fatalf("expected n=%d, got %d", len(value), n)
		}

		if string(cmd.Bytes()) != value {
			t.Fatalf("large payload mismatch")
		}
	})

	t.Run("buffer reuse", func(t *testing.T) {
		if err := adapter.Set(ctx, "gtb-reuse-1", "first", 0).Err(); err != nil {
			t.Fatal(err)
		}

		if err := adapter.Set(ctx, "gtb-reuse-2", "second", 0).Err(); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 32)

		cmd := adapter.GetToBuffer(ctx, "gtb-reuse-1", buf)
		if _, err := cmd.Result(); err != nil {
			t.Fatal(err)
		}

		if got := string(cmd.Bytes()); got != "first" {
			t.Fatalf("expected %q, got %q", "first", got)
		}

		cmd = adapter.GetToBuffer(ctx, "gtb-reuse-2", buf)
		if _, err := cmd.Result(); err != nil {
			t.Fatal(err)
		}

		if got := string(cmd.Bytes()); got != "second" {
			t.Fatalf("expected %q, got %q", "second", got)
		}
	})
}
