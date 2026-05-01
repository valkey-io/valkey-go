module github.com/valkey-io/valkey-go/valkeycompatmock

go 1.25.0

replace github.com/valkey-io/valkey-go => ../

replace github.com/valkey-io/valkey-go/mock => ../mock

replace github.com/valkey-io/valkey-go/valkeycompat => ../valkeycompat

require (
	github.com/valkey-io/valkey-go v1.0.75
	github.com/valkey-io/valkey-go/mock v1.0.75
	github.com/valkey-io/valkey-go/valkeycompat v0.0.0-00010101000000-000000000000
	go.uber.org/mock v0.6.0
)

require golang.org/x/sys v0.43.0 // indirect
