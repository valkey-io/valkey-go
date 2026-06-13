module github.com/valkey-io/valkey-go/valkeycompatmock

go 1.25.0

replace github.com/valkey-io/valkey-go => ../

replace github.com/valkey-io/valkey-go/mock => ../mock

replace github.com/valkey-io/valkey-go/valkeycompat => ../valkeycompat

require (
	github.com/valkey-io/valkey-go v1.0.75
	github.com/valkey-io/valkey-go/mock v1.0.75
	github.com/valkey-io/valkey-go/valkeycompat v1.0.75
	go.uber.org/mock v0.6.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/dgryski/go-rendezvous v0.0.0-20200823014737-9f7001d12a5f // indirect
	golang.org/x/sys v0.43.0 // indirect
)
