module github.com/valkey-io/valkey-go/om

go 1.25.0

replace github.com/valkey-io/valkey-go => ../

require (
	github.com/oklog/ulid/v2 v2.1.2
	github.com/valkey-io/valkey-go v1.0.78
)

require golang.org/x/sys v0.47.0 // indirect
