# valkey mock

Due to the design of the command builder, it is impossible for users to mock `valkey.Client` for testing.

Therefore, valkey provides an implemented one, based on the `gomock`, with some helpers
to make user writing tests more easily, including command matcher `mock.Match`, `mock.MatchFn` and `mock.Result` for faking valkey responses.

## Examples

### Mock `client.Do`

```go
package main

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"
	"github.com/rueian/valkey-go/mock"
)

func TestWithValkey(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	client := mock.NewClient(ctrl)

	client.EXPECT().Do(ctx, mock.Match("GET", "key")).Return(mock.Result(mock.ValkeyString("val")))
	if v, _ := client.Do(ctx, client.B().Get().Key("key").Build()).ToString(); v != "val" {
		t.Fatalf("unexpected val %v", v)
	}
	client.EXPECT().DoMulti(ctx, mock.Match("GET", "c"), mock.Match("GET", "d")).Return([]valkey.ValkeyResult{
		mock.Result(mock.ValkeyNil()),
		mock.Result(mock.ValkeyNil()),
	})
	for _, resp := range client.DoMulti(ctx, client.B().Get().Key("c").Build(), client.B().Get().Key("d").Build()) {
		if err := resp.Error(); !valkey.IsValkeyNil(err) {
			t.Fatalf("unexpected err %v", err)
		}
	}
}
```

### Mock `client.Receive`

```go
package main

import (
	"context"
	"testing"

	"go.uber.org/mock/gomock"
	"github.com/rueian/valkey-go/mock"
)

func TestWithValkeyReceive(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ctx := context.Background()
	client := mock.NewClient(ctrl)

	client.EXPECT().Receive(ctx, mock.Match("SUBSCRIBE", "ch"), gomock.Any()).Do(func(_, _ any, fn func(message valkey.PubSubMessage)) {
		fn(valkey.PubSubMessage{Message: "msg"})
	})

	client.Receive(ctx, client.B().Subscribe().Channel("ch").Build(), func(msg valkey.PubSubMessage) {
		if msg.Message != "msg" {
			t.Fatalf("unexpected val %v", msg.Message)
		}
	})
}
```

