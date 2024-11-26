module github.com/valkey-io/valkey-go/om

go 1.21

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/oklog/ulid/v2 v2.1.0
	github.com/valkey-io/valkey-go v1.0.50
)

require golang.org/x/sys v0.24.0 // indirect
