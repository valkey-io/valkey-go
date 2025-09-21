module github.com/valkey-io/valkey-go/mock

go 1.23.0

toolchain go1.23.4

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/valkey-io/valkey-go v1.0.65
	go.uber.org/mock v0.5.0
)

require golang.org/x/sys v0.31.0 // indirect
