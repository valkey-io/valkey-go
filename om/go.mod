module github.com/valkey-io/valkey-go/om

go 1.23.0

toolchain go1.23.4

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/oklog/ulid/v2 v2.1.0
	github.com/valkey-io/valkey-go v1.0.64
)

require golang.org/x/sys v0.31.0 // indirect
