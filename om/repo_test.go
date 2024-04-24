package om

import (
	"testing"

	"github.com/valkey-io/valkey-go"
)

func setup(t *testing.T) valkey.Client {
	client, err := valkey.NewClient(valkey.ClientOption{InitAddress: []string{"127.0.0.1:6377"}})
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
