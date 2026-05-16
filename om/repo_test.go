package om

import (
	"testing"

	"github.com/valkey-io/valkey-go"
)

type option func(*valkey.ClientOption)

func withRedis86(c *valkey.ClientOption) {
	c.InitAddress = []string{"127.0.0.1:6382"}
}

func setup(t *testing.T, opts ...option) valkey.Client {
	clientOption := &valkey.ClientOption{InitAddress: []string{"127.0.0.1:6377"}}
	for _, opt := range opts {
		opt(clientOption)
	}
	client, err := valkey.NewClient(*clientOption)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

type TestStruct struct {
	Key string `valkey:",key"`
	Ver int64  `valkey:",ver"`
}

func TestWithIndexName(t *testing.T) {
	client := setup(t)
	defer client.Close()

	for _, repo := range []Repository[TestStruct]{
		NewHashRepository("custom_prefix", TestStruct{}, client, WithIndexName("custom_index")),
		NewJSONRepository("custom_prefix", TestStruct{}, client, WithIndexName("custom_index")),
	} {
		if repo.IndexName() != "custom_index" {
			t.Fail()
		}
	}
}
