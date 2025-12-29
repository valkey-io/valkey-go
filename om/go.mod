module github.com/valkey-io/valkey-go/om

go 1.24.9

toolchain go1.24.11

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/oklog/ulid/v2 v2.1.1
	github.com/valkey-io/valkey-go v1.0.70
)

require golang.org/x/sys v0.39.0 // indirect
