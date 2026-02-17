module github.com/valkey-io/valkey-go/valkeylimiter

go 1.24.9

toolchain go1.24.11

replace github.com/valkey-io/valkey-go => ../

replace github.com/valkey-io/valkey-go/mock => ../mock

require (
	github.com/valkey-io/valkey-go v1.0.72
	github.com/valkey-io/valkey-go/mock v1.0.72
	go.uber.org/mock v0.6.0
)

require golang.org/x/sys v0.39.0 // indirect
