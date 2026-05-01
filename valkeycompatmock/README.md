## Go-redis like API Mock Adapter

`valkeycompat` exists as a very close interface to go-redis's `Cmdable` interface,  
but there is not a test helper that mirrors go-redis's `ClientMock`  
interface. This package aims to do that.

### Usage example

```golang
package main

import (
	"context"
	"testing"

	"github.com/valkey-io/valkey-go/mock"
	"github.com/valkey-io/valkey-go/valkeycompat"
	"github.com/valkey-io/valkey-go/valkeycompatmock"
	"go.uber.org/mock/gomock"
)

func TestExample(t *testing.T) {
	ctrl := gomock.NewController(t)
	m := mock.NewClient(ctrl)
	compatmock := valkeycompatmock.NewAdapter(m)

	compatmock.ExpectSet("key", "val", 0).SetVal("OK")
	compatmock.ExpectGet("key").SetVal("val")

	rdb := valkeycompat.NewAdapter(m)
	rdb.Set(context.Background(), "key", "val", 0)
	rdb.Get(context.Background(), "key")
}
```

### Pipeline example

Pipelined commands use the same `Expect*` calls as non-pipelined commands.
Expectations are matched in the order they are queued, mirroring `go-redis/redismock`.

```golang
compatmock.ExpectGet("k1").SetVal("v1")
compatmock.ExpectSet("k2", "v2", 0).SetVal("OK")

p := rdb.Pipeline()
p.Get(ctx, "k1")
p.Set(ctx, "k2", "v2", 0)
p.Exec(ctx)
```